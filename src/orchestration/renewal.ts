import * as Cause from "effect/Cause";
import * as Clock from "effect/Clock";
import * as Crypto from "effect/Crypto";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Option from "effect/Option";
import * as Queue from "effect/Queue";
import * as Result from "effect/Result";
import * as Scope from "effect/Scope";
import * as Semaphore from "effect/Semaphore";
import * as Stream from "effect/Stream";
import { ReactorError, errorOf } from "../errors.js";
import { Observations } from "../observation.js";
import * as Sequence from "../Sequence.js";
import * as Submission from "../Submission.js";
import { CommandFailure } from "../session/commands.js";
import { AcquisitionFailure } from "../session/index.js";
import type { AudioFrame, VideoFrame, MediaPressure } from "../session/media.js";
import { PolicyFailure, captureRequest } from "./request.js";
import type { ClipId } from "./request.js";
import { emptyState, isIdle } from "./queries.js";
import { activeIds, generation, resolve } from "./routing.js";
import type { Candidate } from "./routing.js";
import type {
  CleanupReport,
  EngineEvent,
  EngineShape,
  EngineState,
  HandleShape,
  MediaSource,
  MediaState,
  Source,
  SourceCleanup,
} from "./types.js";

export interface Options<R = never> {
  readonly open: Effect.Effect<
    { readonly source: Source; readonly maxSeconds: number },
    unknown,
    Scope.Scope | R
  >;
  readonly leadSeconds?: number;
  readonly reconnectTimeoutMs?: number;
  readonly maxSessions?: number;
  readonly log?: (message: string) => void;
  readonly onRenewal?: (event: Renewal) => Effect.Effect<void>;
}

export interface MediaTail {
  readonly video: {
    readonly framesPerSecond: number;
    readonly expectedFrames: number;
    readonly receivedFrames: number;
    readonly status: "not-started" | "count-complete" | "incomplete";
  };
  readonly audio: { readonly receivedSamples: number; readonly status: "unverified" };
  readonly sourceDrops: { readonly video: bigint | null; readonly audio: bigint | null };
  readonly forwarded: { readonly queuedVideoFrames: number; readonly queuedAudioSamples: number };
}

export type Renewal =
  | { readonly _tag: "Opened"; readonly sessionId: string; readonly maxSeconds: number }
  | { readonly _tag: "Prepared" }
  | { readonly _tag: "SetupFailed"; readonly reason: string; readonly consecutive: number }
  | { readonly _tag: "Recovering"; readonly sessionId: string; readonly reason: string }
  | { readonly _tag: "Reconnected"; readonly sessionId: string; readonly generation: bigint }
  | {
      readonly _tag: "Switched";
      readonly sessionId: string;
      readonly ageSeconds: number;
      readonly tail: MediaTail;
    }
  | {
      readonly _tag: "Replaced";
      readonly reason: string;
      readonly lostClips: number;
      readonly sessionId: string;
      readonly ageSeconds: number;
      readonly tail: MediaTail;
    }
  | { readonly _tag: "Failed"; readonly reason: string };

interface Slot {
  readonly source: Source;
  readonly scope: Scope.Closeable;
  readonly openedAt: number;
  readonly maxSeconds: number;
  readonly accepted: Set<ClipId>;
  readonly started: Set<ClipId>;
  readonly submissions: Set<Submission.Submission<ClipId, CommandFailure>>;
  readonly closeGate: Semaphore.Semaphore;
  media: MediaSource;
  mediaScope: Scope.Closeable | undefined;
  closed: boolean;
  recovering: boolean;
  indeterminate: boolean;
  requiresReplacement: boolean;
  inFlight: number;
  settled: Deferred.Deferred<void>;
  expectedFrames: number;
  receivedFrames: number;
  receivedAudioSamples: number;
  /** Completed receiver generations contribute to the whole source's drop evidence. */
  retiredDrops: { readonly video: bigint | null; readonly audio: bigint | null };
}

/**
 * Explicit application policy for renewal, sequence affinity and continuous
 * recovering media. A physical source still owns each connection generation.
 */
export const make = <R>(
  options: Options<R>,
): Effect.Effect<HandleShape, ReactorError, Scope.Scope | Crypto.Crypto | R> =>
  Effect.gen(function* () {
    const scope = yield* Effect.scope;
    const context = yield* Effect.context<R>();
    const clock = yield* Clock.Clock;
    const random = yield* (yield* Crypto.Crypto)
      .randomBytes(16)
      .pipe(Effect.mapError((cause) => errorOf(cause, "InvalidState")));
    const namespace = Array.from(random, (byte) => byte.toString(16).padStart(2, "0")).join("");
    const commands = yield* Semaphore.make(1);
    const closeGate = yield* Semaphore.make(1);
    const affinity = yield* Sequence.makeAffinity<string>().pipe(
      Effect.mapError((cause) => errorOf(cause, "InvalidInput")),
    );
    const events = new Observations<EngineEvent>();
    const video = yield* Queue.unbounded<VideoFrame, ReactorError | Cause.Done>();
    const audio = yield* Queue.unbounded<AudioFrame, ReactorError | Cause.Done>();
    const fatal = yield* Deferred.make<ReactorError>();
    const slots = new Map<string, Slot>();
    const cleanups: SourceCleanup[] = [];
    const seenCleanups = new Set<SourceCleanup["lease"]>();
    const maxSessions = options.maxSessions ?? 64;
    const reconnectTimeout = options.reconnectTimeoutMs ?? 10_000;
    const leadSeconds = options.leadSeconds ?? 30;
    if (
      !Number.isSafeInteger(maxSessions) ||
      maxSessions < 1 ||
      maxSessions > 4096 ||
      !Number.isFinite(reconnectTimeout) ||
      reconnectTimeout <= 0 ||
      !Number.isFinite(leadSeconds) ||
      leadSeconds < 0
    ) {
      return yield* Effect.fail(new ReactorError("InvalidInput", "Invalid orchestration bounds"));
    }
    let current: Slot | undefined;
    let next: Slot | undefined;
    let opening: Fiber.Fiber<Slot, ReactorError> | undefined;
    let opened = 0;
    let openFailures = 0;
    let retryAt = 0;
    let autoplay = true;
    let closing = false;
    let finalReport: CleanupReport | undefined;
    let mediaState: MediaState = { _tag: "Closed" };
    let terminalFailure: ReactorError | undefined;
    let queuedFrames = 0;
    let queuedSamples = 0;
    let queuedAudioFrames = 0;
    let queuedVideoBytes = 0;
    let submissionSequence = 0n;

    const observe = (event: Renewal) => options.onRenewal?.(event) ?? Effect.void;
    const log = (message: string) => Effect.sync(() => options.log?.(message));
    const emit = (event: EngineEvent): void => events.emit(event, 256);
    const fail = (cause: ReactorError): Effect.Effect<void> =>
      Effect.suspend(() => {
        if (terminalFailure !== undefined) return Effect.void;
        terminalFailure = cause;
        // Publish the failed state and queues before waking a failure waiter.
        mediaState = { _tag: "Failed", cause };
        emit({ _tag: "SessionFailed", failure: cause });
        Queue.failCauseUnsafe(video, Cause.fail(cause));
        Queue.failCauseUnsafe(audio, Cause.fail(cause));
        Deferred.doneUnsafe(fatal, Effect.succeed(cause));
        return observe({ _tag: "Failed", reason: cause.message });
      });
    const recordCleanup = (cleanup: SourceCleanup): void => {
      if (seenCleanups.has(cleanup.lease)) return;
      seenCleanups.add(cleanup.lease);
      cleanups.push(cleanup);
    };
    const openSequences = (slot: Slot) =>
      affinity.snapshots.pipe(
        Effect.map((entries) =>
          entries.some((entry) => entry.owner === slot.source.id && entry.status === "open"),
        ),
      );
    const expired = (slot: Slot): boolean =>
      clock.currentTimeMillisUnsafe() - slot.openedAt >= slot.maxSeconds * 1000;
    const recoveryBudget = (slot: Slot): number =>
      Math.min(
        reconnectTimeout,
        Math.max(1, slot.maxSeconds * 1000 - (clock.currentTimeMillisUnsafe() - slot.openedAt)),
      );
    const sumDrops = (retained: bigint | null, latest: bigint | null): bigint | null =>
      retained === null || latest === null ? null : retained + latest;
    const retired = (slot: Slot) =>
      Effect.gen(function* () {
        const pressure = yield* Effect.result(
          slot.media.pressure.pipe(Effect.timeout(recoveryBudget(slot))),
        );
        return {
          sessionId: slot.source.id,
          ageSeconds: Math.max(0, (clock.currentTimeMillisUnsafe() - slot.openedAt) / 1000),
          tail: {
            video: {
              framesPerSecond: slot.media.videoFramesPerSecond,
              expectedFrames: slot.expectedFrames,
              receivedFrames: slot.receivedFrames,
              status:
                slot.expectedFrames === 0
                  ? ("not-started" as const)
                  : slot.receivedFrames >= slot.expectedFrames
                    ? ("count-complete" as const)
                    : ("incomplete" as const),
            },
            audio: { receivedSamples: slot.receivedAudioSamples, status: "unverified" as const },
            sourceDrops: Result.isSuccess(pressure)
              ? {
                  video: sumDrops(slot.retiredDrops.video, pressure.success.droppedVideo),
                  audio: sumDrops(slot.retiredDrops.audio, pressure.success.droppedAudio),
                }
              : { video: null, audio: null },
            forwarded: { queuedVideoFrames: queuedFrames, queuedAudioSamples: queuedSamples },
          },
        };
      });

    const awaitCommitted = (slot: Slot, budget = reconnectTimeout): Effect.Effect<void> =>
      Effect.suspend(() =>
        Effect.all(
          [
            Deferred.await(slot.settled),
            // The result hook runs inside physical execution. Its signal alone
            // cannot prove that execution has returned its immutable outcome yet.
            // Every retained handle was registered at commit, so submit only joins
            // its existing owner; inert preparation is never dispatched here.
            Effect.forEach([...slot.submissions], (submission) => Effect.exit(submission.submit), {
              concurrency: "unbounded",
              discard: true,
            }),
          ],
          { concurrency: "unbounded", discard: true },
        ),
      ).pipe(
        Effect.interruptible,
        Effect.timeoutOrElse({
          duration: budget,
          orElse: () =>
            Effect.sync(() => {
              slot.indeterminate = true;
            }),
        }),
      );

    const closeSlot = (slot: Slot): Effect.Effect<void> =>
      slot.closeGate.withPermit(
        Effect.uninterruptible(
          Effect.gen(function* () {
            if (slot.closed) return;
            slot.closed = true;
            if (slot.mediaScope !== undefined) yield* Scope.close(slot.mediaScope, Exit.void);
            // Closing the lease wakes protocol waiters with their established dispatch
            // evidence. Give the committed owner time to record those outcomes before
            // retiring its surrounding application scope. The remote lifetime has
            // ended; local bookkeeping retains its own bounded cleanup budget.
            recordCleanup(yield* slot.source.close);
            yield* awaitCommitted(slot);
            yield* affinity.retire(slot.source.id);
            yield* Scope.close(slot.scope, Exit.void);
            slot.submissions.clear();
          }),
        ),
      );

    const replace = (slot: Slot, cause: ReactorError): Effect.Effect<void> =>
      Effect.gen(function* () {
        if (slot.closed || closing || terminalFailure !== undefined) return;
        const state = yield* slot.source.state;
        const lost = activeIds(state);
        const tail = yield* retired(slot);
        for (const clipId of lost)
          emit({
            _tag: "Failed",
            clipId,
            reason: `Session lost: ${cause.message}`,
            sessionId: slot.source.id,
          });
        yield* closeSlot(slot);
        yield* observe({
          _tag: "Replaced",
          reason: cause.message,
          lostClips: lost.length,
          ...tail,
        });
        if (slot === next) next = undefined;
        if (slot !== current) return;
        if (next === undefined) {
          next = yield* opening === undefined ? acquire : Fiber.join(opening);
          opening = undefined;
        }
        current = next;
        next = undefined;
        yield* current.source.setAutoplay(autoplay);
        if (terminalFailure === undefined)
          mediaState = {
            _tag: "Ready",
            sessionId: current.source.id,
            generation: current.media.generation,
          };
        yield* log("Replaced lost session; local queue resumes on the new connection");
      }).pipe(Effect.catch((failure) => fail(errorOf(failure, "Disconnected"))));

    const recover = (
      slot: Slot,
      cause: ReactorError,
      mode: "reconnect" | "replace",
    ): Effect.Effect<void> =>
      Effect.gen(function* () {
        if (slot.closed || closing || terminalFailure !== undefined) return;
        if (slot === current) mediaState = { _tag: "Recovering", sessionId: slot.source.id, cause };
        yield* observe({ _tag: "Recovering", sessionId: slot.source.id, reason: cause.message });
        yield* awaitCommitted(slot, recoveryBudget(slot));
        if (mode === "replace" || slot.requiresReplacement || slot.indeterminate || expired(slot)) {
          yield* replace(slot, cause);
          return;
        }
        const before = activeIds(yield* slot.source.state);
        // Read the retiring receiver before reconnect makes its counters unavailable.
        const previousPressure = yield* Effect.result(
          slot.media.pressure.pipe(Effect.timeout(recoveryBudget(slot))),
        );
        if (expired(slot)) {
          yield* replace(
            slot,
            new ReactorError("TerminalSession", "Source expired during recovery", {
              operation: "renewal",
            }),
          );
          return;
        }
        if (slot.mediaScope !== undefined) yield* Scope.close(slot.mediaScope, Exit.void);
        const connected = yield* Effect.result(
          slot.source.reconnect.pipe(
            Effect.timeoutOrElse({
              duration: recoveryBudget(slot),
              orElse: () =>
                Effect.fail(
                  new ReactorError("Disconnected", "Explicit source reconnect timed out"),
                ),
            }),
          ),
        );
        if (Result.isFailure(connected)) {
          yield* replace(slot, connected.failure);
          return;
        }
        if (slot.requiresReplacement || slot.indeterminate || expired(slot)) {
          yield* replace(slot, cause);
          return;
        }
        const after = new Set(activeIds(yield* slot.source.state));
        for (const clipId of before)
          if (!after.has(clipId))
            emit({
              _tag: "Failed",
              clipId,
              reason: "Clip left the provider's active queues during the observation gap",
              sessionId: slot.source.id,
            });
        const restarted = yield* Effect.result(
          Effect.gen(function* () {
            const media = yield* slot.source.media;
            if (media.generation <= slot.media.generation)
              return yield* Effect.fail(
                new ReactorError("Protocol", "Reconnect did not acquire a new media generation"),
              );
            slot.retiredDrops = {
              video: sumDrops(
                slot.retiredDrops.video,
                Result.isSuccess(previousPressure) ? previousPressure.success.droppedVideo : null,
              ),
              audio: sumDrops(
                slot.retiredDrops.audio,
                Result.isSuccess(previousPressure) ? previousPressure.success.droppedAudio : null,
              ),
            };
            return yield* startMedia(slot, media);
          }),
        );
        if (Result.isFailure(restarted)) {
          yield* replace(slot, restarted.failure);
          return;
        }
        slot.recovering = false;
        if (slot === current && terminalFailure === undefined)
          mediaState = {
            _tag: "Ready",
            sessionId: slot.source.id,
            generation: slot.media.generation,
          };
        yield* observe({
          _tag: "Reconnected",
          sessionId: slot.source.id,
          generation: slot.media.generation,
        });
      });

    const scheduleRecovery = (
      slot: Slot,
      cause: ReactorError,
      mode: "reconnect" | "replace",
    ): Effect.Effect<void> =>
      Effect.gen(function* () {
        if (slot.closed || closing || terminalFailure !== undefined) return;
        if (mode === "replace") slot.requiresReplacement = true;
        if (slot.recovering) return;
        slot.recovering = true;
        yield* recover(slot, cause, mode).pipe(commands.withPermit, Effect.forkIn(scope));
      });

    const startMedia = (slot: Slot, acquired?: MediaSource): Effect.Effect<void, ReactorError> =>
      Effect.gen(function* () {
        slot.media = acquired ?? (yield* slot.source.media);
        const mediaScope = yield* Scope.fork(slot.scope);
        slot.mediaScope = mediaScope;
        const generation = slot.media.generation;
        const lost = (cause: ReactorError) =>
          slot.closed || slot.media.generation !== generation || closing
            ? Effect.void
            : scheduleRecovery(slot, cause, "reconnect");
        yield* slot.media.video.pipe(
          Stream.runForEach((frame) =>
            Effect.gen(function* () {
              if (
                slot !== current ||
                slot.closed ||
                slot.media.generation !== generation ||
                terminalFailure !== undefined
              )
                return;
              slot.receivedFrames++;
              if (queuedFrames >= 96)
                return yield* fail(
                  new ReactorError("Overflow", "Orchestration video receiver overflow"),
                );
              queuedFrames++;
              queuedVideoBytes += frame.data.byteLength + frame.metadata.byteLength;
              yield* Queue.offer(video, frame);
            }),
          ),
          Effect.andThen(lost(new ReactorError("Disconnected", "Video generation ended"))),
          Effect.catch(lost),
          Effect.forkIn(mediaScope),
        );
        yield* slot.media.audio.pipe(
          Stream.runForEach((frame) =>
            Effect.gen(function* () {
              if (
                slot !== current ||
                slot.closed ||
                slot.media.generation !== generation ||
                terminalFailure !== undefined
              )
                return;
              if (queuedSamples + frame.samples.length > 48_000 * 4)
                return yield* fail(
                  new ReactorError("Overflow", "Orchestration audio receiver overflow"),
                );
              slot.receivedAudioSamples += frame.samples.length;
              queuedSamples += frame.samples.length;
              queuedAudioFrames++;
              yield* Queue.offer(audio, frame);
            }),
          ),
          Effect.andThen(lost(new ReactorError("Disconnected", "Audio generation ended"))),
          Effect.catch(lost),
          Effect.forkIn(mediaScope),
        );
      });

    const acquire: Effect.Effect<Slot, ReactorError> = Effect.uninterruptibleMask((restore) =>
      Effect.gen(function* () {
        if (opened >= maxSessions)
          return yield* Effect.fail(
            new ReactorError("Overflow", "Orchestration session and cleanup-history bound reached"),
          );
        const owned = yield* Scope.make();
        let acquired: Slot | undefined;
        let acquiredSource: Source | undefined;
        return yield* restore(
          Effect.gen(function* () {
            // Captured contexts contain the caller's Scope at runtime even when R
            // omits it statically. Install the child inside that captured environment.
            const value = yield* options.open.pipe(
              Scope.provide(owned),
              Effect.provideContext(context),
              Effect.timeoutOrElse({
                duration: "30 seconds",
                orElse: () =>
                  Effect.fail(new ReactorError("InvalidState", "Source acquisition timed out")),
              }),
              Effect.mapError((cause) => errorOf(cause, "InvalidState")),
            );
            acquiredSource = value.source;
            if (
              !(value.maxSeconds > 0) ||
              (value.maxSeconds !== Infinity && !Number.isFinite(value.maxSeconds))
            ) {
              return yield* Effect.fail(
                new ReactorError("InvalidInput", "Source lifetime must be positive or infinite"),
              );
            }
            if (slots.has(value.source.id))
              return yield* Effect.fail(
                new ReactorError(
                  "InvalidInput",
                  "Orchestration sources must have distinct session identities",
                ),
              );
            const media = yield* value.source.media;
            const settled = yield* Deferred.make<void>();
            yield* Deferred.succeed(settled, undefined);
            const slot: Slot = {
              source: value.source,
              scope: owned,
              maxSeconds: value.maxSeconds,
              openedAt: clock.currentTimeMillisUnsafe(),
              accepted: new Set(),
              submissions: new Set(),
              started: new Set(),
              closeGate: yield* Semaphore.make(1),
              media,
              mediaScope: undefined,
              closed: false,
              recovering: false,
              indeterminate: false,
              requiresReplacement: false,
              inFlight: 0,
              settled,
              expectedFrames: 0,
              receivedFrames: 0,
              receivedAudioSamples: 0,
              retiredDrops: { video: 0n, audio: 0n },
            };
            acquired = slot;
            slots.set(slot.source.id, slot);
            opened++;
            yield* slot.source.setAutoplay(false);
            yield* slot.source.events.pipe(
              Stream.runForEach((event) =>
                Effect.gen(function* () {
                  if (slot.closed) return;
                  if (event._tag === "SessionFailed")
                    return yield* scheduleRecovery(slot, event.failure, "replace");
                  if (event._tag === "Started" && !slot.started.has(event.clipId)) {
                    slot.started.add(event.clipId);
                    slot.expectedFrames += Math.round(
                      event.durationSeconds * slot.media.videoFramesPerSecond,
                    );
                  }
                  if (event._tag !== "Starved" || slot === current) emit(event);
                }),
              ),
              Effect.catch((cause) => scheduleRecovery(slot, cause, "replace")),
              Effect.forkIn(owned),
            );
            yield* startMedia(slot);
            yield* observe({
              _tag: "Opened",
              sessionId: slot.source.id,
              maxSeconds: slot.maxSeconds,
            });
            return slot;
          }),
        ).pipe(
          Effect.onExit((exit) =>
            Exit.isSuccess(exit)
              ? Effect.void
              : Effect.gen(function* () {
                  if (acquired !== undefined) yield* closeSlot(acquired);
                  else {
                    if (acquiredSource !== undefined) recordCleanup(yield* acquiredSource.close);
                    yield* Scope.close(owned, exit);
                    if (Exit.isFailure(exit)) {
                      const failure = Cause.findErrorOption(exit.cause);
                      if (Option.isSome(failure) && failure.value instanceof AcquisitionFailure)
                        recordCleanup({ lease: failure.value.cleanup, policy: [] });
                    }
                  }
                }),
          ),
        );
      }),
    );

    const close: Effect.Effect<CleanupReport> = closeGate.withPermit(
      Effect.uninterruptible(
        Effect.gen(function* () {
          if (finalReport !== undefined) return finalReport;
          closing = true;
          if (opening !== undefined) yield* Fiber.interrupt(opening);
          yield* Effect.forEach([...slots.values()], closeSlot, { discard: true });
          Queue.endUnsafe(video);
          Queue.endUnsafe(audio);
          events.end();
          mediaState = { _tag: "Closed" };
          finalReport = Object.freeze({ sessions: Object.freeze([...cleanups]) });
          return finalReport;
        }),
      ),
    );
    yield* Effect.addFinalizer(() => close);
    current = yield* acquire.pipe(
      Effect.tap((slot) => slot.source.setAutoplay(true)),
      Effect.catch((cause) =>
        close.pipe(
          Effect.flatMap((report) => {
            const lease = report.sessions[0]?.lease;
            return Effect.fail(lease === undefined ? cause : new AcquisitionFailure(cause, lease));
          }),
        ),
      ),
    );
    mediaState = {
      _tag: "Ready",
      sessionId: current.source.id,
      generation: current.media.generation,
    };

    const guard = <A, E>(
      operation: string,
      effect: Effect.Effect<A, E>,
    ): Effect.Effect<A, E | CommandFailure> =>
      Effect.gen(function* () {
        if (closing)
          return yield* Effect.fail(
            new PolicyFailure("session_closed", "Orchestration is closed", operation),
          );
        if (yield* Deferred.isDone(fatal))
          return yield* Effect.fail(
            new CommandFailure(yield* Deferred.await(fatal), {
              operation,
              outcome: "not-submitted",
            }),
          );
        return yield* effect;
      });

    const route = (request: Parameters<EngineShape["prepare"]>[0]) =>
      Effect.gen(function* () {
        const values = yield* Effect.suspend(() =>
          Effect.forEach([...slots.values()], (slot) =>
            Effect.map(slot.source.state, (state): Candidate<string> => ({
              owner: slot.source.id,
              state,
              accepted: slot.accepted,
              closed: slot.closed,
              recovering: slot.recovering,
            })),
          ),
        );
        const binding =
          request.sequence === undefined ? undefined : yield* affinity.get(request.sequence.id);
        const preferred =
          next !== undefined && current !== undefined && !(yield* openSequences(current))
            ? next
            : current;
        if (preferred === undefined)
          return yield* Effect.fail(new PolicyFailure("session_recovering", "No source is ready"));
        return yield* resolve(request, values, preferred.source.id, binding);
      });
    const sequenceError = (error: Sequence.SequenceError) =>
      new PolicyFailure(
        `sequence_${error.reason}`,
        `Sequence ${error.sequenceId}: ${error.reason}`,
      );

    const prepare: EngineShape["prepare"] = (input) =>
      Effect.gen(function* () {
        const request = yield* captureRequest(input);
        const id = `${namespace}:${++submissionSequence}`;
        const gate = yield* Semaphore.make(1);
        let active: Submission.Submission<ClipId, CommandFailure> | undefined;
        let activeOwner: Slot | undefined;
        const submit = gate.withPermit(
          Effect.gen(function* () {
            if (active === undefined) {
              // A committed submission remains a readable outcome after the handle
              // closes. Only selecting or committing fresh work needs a live owner.
              yield* guard("enqueue", Effect.void);
              const decision = yield* route(request).pipe(commands.withPermit);
              const target = slots.get(decision.owner);
              if (target === undefined)
                return yield* Effect.fail(
                  new PolicyFailure("session_retired", "Selected source was retired"),
                );
              const sequence = request.sequence;
              active = yield* target.source.prepareRouted(
                { request, position: decision.position },
                {
                  commit: (submissionId) =>
                    commands.withPermit(
                      guard(
                        "enqueue",
                        Effect.gen(function* () {
                          const checked = yield* route(request);
                          if (
                            checked.owner !== decision.owner ||
                            checked.position !== decision.position
                          ) {
                            return yield* Effect.fail(
                              new PolicyFailure(
                                "route_changed",
                                "Request ownership or insertion position changed during preparation",
                              ),
                            );
                          }
                          if (active === undefined)
                            return yield* Effect.die(
                              new Error("Source committed before returning its inert submission"),
                            );
                          for (const previous of target.submissions) {
                            if ((yield* previous.state)._tag === "Completed")
                              target.submissions.delete(previous);
                          }
                          if (target.submissions.size >= 4096)
                            return yield* Effect.fail(
                              new PolicyFailure(
                                "submission_capacity",
                                "Committed source submissions reached their bound",
                              ),
                            );
                          if (sequence !== undefined) {
                            yield* affinity
                              .bind(sequence.id, target.source.id)
                              .pipe(Effect.mapError(sequenceError));
                            yield* affinity
                              .begin(sequence.id, sequence.memberId ?? submissionId)
                              .pipe(Effect.mapError(sequenceError));
                          }
                          if (target.inFlight++ === 0) target.settled = Deferred.makeUnsafe<void>();
                          target.submissions.add(active);
                        }),
                      ),
                    ),
                  result: (submissionId, result) =>
                    Effect.gen(function* () {
                      if (Result.isSuccess(result)) target.accepted.add(result.success);
                      else if (result.failure.context.outcome === "unknown")
                        target.indeterminate = true;
                      if (sequence !== undefined) {
                        const memberId = sequence.memberId ?? submissionId;
                        const account = Result.isSuccess(result)
                          ? affinity.accepted(sequence.id, memberId, result.success, sequence.final)
                          : result.failure.context.outcome === "unknown"
                            ? affinity.uncertain(sequence.id, memberId, result.failure.message)
                            : affinity.rejected(
                                sequence.id,
                                memberId,
                                result.failure.message,
                                sequence.final,
                              );
                        yield* account.pipe(
                          Effect.catch((cause) =>
                            Effect.gen(function* () {
                              target.indeterminate = true;
                              yield* affinity.retire(target.source.id);
                              yield* fail(
                                new ReactorError(
                                  "InvalidState",
                                  "Committed sequence accounting failed",
                                  { detail: cause },
                                ),
                              );
                            }),
                          ),
                        );
                      }
                      if (--target.inFlight === 0)
                        yield* Deferred.succeed(target.settled, undefined);
                      if (
                        Result.isFailure(result) &&
                        result.failure.context.outcome === "unknown"
                      ) {
                        yield* scheduleRecovery(target, result.failure, "replace");
                      }
                    }),
                },
              );
              activeOwner = target;
            }
            const selected = active;
            return yield* selected.submit.pipe(
              Effect.onExit(() =>
                selected.state.pipe(
                  Effect.flatMap((state) =>
                    Effect.sync(() => {
                      if (state._tag === "Completed") activeOwner?.submissions.delete(selected);
                      if (state._tag === "Prepared" && active === selected) {
                        active = undefined;
                        activeOwner = undefined;
                      }
                    }),
                  ),
                ),
              ),
            );
          }),
        );
        return {
          id,
          submit,
          state: Effect.suspend(() =>
            active === undefined
              ? Effect.succeed<Submission.State<ClipId, CommandFailure>>({ _tag: "Prepared" })
              : active.state,
          ),
        };
      });

    const state: Effect.Effect<EngineState> = Effect.gen(function* () {
      const live = [...slots.values()].filter((slot) => !slot.closed);
      const values = yield* Effect.forEach(live, (slot) => slot.source.state);
      const primary = values[live.indexOf(current!)] ?? emptyState();
      const builds = values.flatMap((value) =>
        Option.isSome(value.building) ? [value.building.value] : [],
      );
      return Object.freeze({
        ...primary,
        availability: values.some((value) => value.availability === "Unavailable")
          ? "Unavailable"
          : values.some((value) => value.availability === "Synchronizing")
            ? "Synchronizing"
            : primary.availability,
        queued: Object.freeze([
          ...values.flatMap((value) => value.queued),
          ...builds.slice(1).map((build) => build.record),
        ]),
        generationOrder: Object.freeze(values.flatMap((value) => value.generationOrder)),
        building: Option.fromUndefinedOr(builds[0]),
        ready: Object.freeze(values.flatMap((value) => value.ready)),
        continuable: Object.freeze(values.flatMap((value) => value.continuable)),
        failed: Object.freeze(values.flatMap((value) => value.failed)),
        capacities: {
          generation: Math.min(
            primary.capacities.generation,
            ...values.map((value) => value.capacities.generation),
          ),
          playout: Math.min(
            primary.capacities.playout,
            ...values.map((value) => value.capacities.playout),
          ),
        },
      });
    });

    const owner = (id: ClipId, operation: string) =>
      Effect.gen(function* () {
        const matches = yield* Effect.forEach(
          [...slots.values()].filter((slot) => !slot.closed),
          (slot) => slot.source.state.pipe(Effect.map((state) => ({ slot, state }))),
        );
        const found = matches.filter(({ state }) => activeIds(state).includes(id));
        if (found.length !== 1)
          return yield* Effect.fail(
            new PolicyFailure(
              found.length === 0 ? "not_found" : "owner_conflict",
              "Clip has no unique active owning session",
              operation,
            ),
          );
        if (found[0]!.slot.recovering)
          return yield* Effect.fail(
            new PolicyFailure("session_recovering", "Owning session is recovering", operation),
          );
        return found[0]!;
      });

    const engine: EngineShape = {
      prepare,
      enqueue: (request) =>
        prepare(request).pipe(Effect.flatMap((submission) => submission.submit)),
      state,
      events: events.stream(),
      failure: Deferred.await(fatal),
      setAutoplay: (enabled) =>
        commands.withPermit(
          guard(
            "set_autoplay",
            Effect.gen(function* () {
              autoplay = enabled;
              if (current !== undefined && !current.closed)
                yield* current.source.setAutoplay(enabled);
            }),
          ),
        ),
      pauseAndStop: commands.withPermit(
        guard(
          "pauseAndStop",
          Effect.gen(function* () {
            autoplay = false;
            for (const slot of slots.values())
              if (!slot.closed) {
                yield* slot.source.setAutoplay(false);
                yield* slot.source.stop;
              }
          }),
        ),
      ),
      remove: (id) =>
        commands.withPermit(
          guard("pop", owner(id, "pop").pipe(Effect.flatMap(({ slot }) => slot.source.remove(id)))),
        ),
      move: (id, position, queue) =>
        commands.withPermit(
          guard(
            "move",
            Effect.gen(function* () {
              if (!Number.isSafeInteger(position) || position < 0)
                return yield* Effect.fail(
                  new PolicyFailure(
                    "invalid_request",
                    "Move position must be a nonnegative integer",
                    "move",
                  ),
                );
              const selected = yield* owner(id, "move");
              const own =
                queue === "generation" ? generation(selected.state) : selected.state.ready;
              if (!own.some((clip) => clip.clipId === id))
                return yield* Effect.fail(
                  new PolicyFailure(
                    "queue_changed",
                    "Clip is no longer in the selected application queue",
                    "move",
                  ),
                );
              let preceding = 0;
              for (const slot of slots.values()) {
                if (slot === selected.slot) break;
                if (slot.closed) continue;
                const value = yield* slot.source.state;
                preceding += queue === "generation" ? generation(value).length : value.ready.length;
              }
              yield* selected.slot.source.move(id, Math.max(0, position - preceding));
            }),
          ),
        ),
      setCanvas: (canvas) =>
        commands.withPermit(
          guard(
            "set_canvas",
            Effect.gen(function* () {
              if (!isIdle(yield* state))
                return yield* Effect.fail(
                  new PolicyFailure(
                    "busy",
                    "Canvas can only change while every source is idle",
                    "set_canvas",
                  ),
                );
              for (const slot of slots.values())
                if (!slot.closed) yield* slot.source.setCanvas(canvas);
            }),
          ),
        ),
    };

    const tick = commands
      .withPermit(
        Effect.gen(function* () {
          if (
            closing ||
            current === undefined ||
            current.closed ||
            current.recovering ||
            (yield* Deferred.isDone(fatal))
          )
            return;
          if (opening !== undefined) {
            const result = opening.pollUnsafe();
            if (result !== undefined) {
              opening = undefined;
              if (Exit.isSuccess(result)) {
                next = result.value;
                openFailures = 0;
                yield* log("Prepared the next session for renewal");
                yield* observe({ _tag: "Prepared" });
              } else {
                openFailures++;
                retryAt = clock.currentTimeMillisUnsafe() + 5000;
                yield* observe({
                  _tag: "SetupFailed",
                  reason: "Could not prepare the next source",
                  consecutive: openFailures,
                });
                if (openFailures >= 3)
                  return yield* fail(
                    new ReactorError("Disconnected", "Repeated source renewal acquisition failed"),
                  );
              }
            }
          }
          const age = (clock.currentTimeMillisUnsafe() - current.openedAt) / 1000;
          // The remote lifetime is a hard boundary even with an open sequence or
          // incomplete media. Closing first lets the physical commit owner retain
          // unknown outcomes before sequence retirement; no work is replayed.
          if (age >= current.maxSeconds) {
            yield* replace(
              current,
              new ReactorError("TerminalSession", "Source lifetime limit reached", {
                operation: "renewal",
              }),
            );
            return;
          }
          if (
            next === undefined &&
            opening === undefined &&
            age >= current.maxSeconds - leadSeconds &&
            clock.currentTimeMillisUnsafe() >= retryAt
          ) {
            opening = yield* acquire.pipe(Effect.forkIn(scope));
          }
          if (
            next === undefined ||
            next.closed ||
            next.recovering ||
            (yield* openSequences(current))
          )
            return;
          const oldState = yield* current.source.state;
          const nextState = yield* next.source.state;
          if (
            !isIdle(oldState) ||
            nextState.availability !== "Ready" ||
            nextState.ready.length === 0
          )
            return;
          const tail = yield* retired(current);
          if (
            tail.tail.video.status === "incomplete" ||
            tail.tail.sourceDrops.video !== 0n ||
            tail.tail.sourceDrops.audio !== 0n
          )
            return;
          const old = current;
          current = next;
          next = undefined;
          yield* current.source.setAutoplay(autoplay);
          if (terminalFailure === undefined)
            mediaState = {
              _tag: "Ready",
              sessionId: current.source.id,
              generation: current.media.generation,
            };
          yield* closeSlot(old);
          yield* observe({ _tag: "Switched", ...tail });
          yield* log("Switched prepared sessions at a sequence boundary");
        }),
      )
      .pipe(Effect.catch((cause) => fail(errorOf(cause, "Disconnected"))));
    yield* Effect.forever(Effect.sleep(100).pipe(Effect.andThen(tick))).pipe(Effect.forkIn(scope));

    const pressure: Effect.Effect<MediaPressure, ReactorError> = Effect.suspend(() => {
      if (current === undefined)
        return Effect.fail(new ReactorError("InvalidState", "No active media source"));
      const owner = current;
      return owner.media.pressure.pipe(
        Effect.flatMap((source) => {
          const droppedVideo = sumDrops(owner.retiredDrops.video, source.droppedVideo);
          const droppedAudio = sumDrops(owner.retiredDrops.audio, source.droppedAudio);
          if (droppedVideo === null || droppedAudio === null)
            return Effect.fail(
              new ReactorError("InvalidState", "Retired media generation drop totals are unknown"),
            );
          return Effect.succeed({
            ...source,
            closed: closing,
            queuedVideo: source.queuedVideo + queuedFrames,
            queuedAudio: source.queuedAudio + queuedAudioFrames,
            queuedBytes: source.queuedBytes + queuedVideoBytes + queuedSamples * 2,
            droppedVideo,
            droppedAudio,
          });
        }),
      );
    });
    return {
      engine,
      media: {
        video: Stream.fromQueue(video).pipe(
          Stream.tap((frame) =>
            Effect.sync(() => {
              queuedFrames--;
              queuedVideoBytes -= frame.data.byteLength + frame.metadata.byteLength;
            }),
          ),
        ),
        audio: Stream.fromQueue(audio).pipe(
          Stream.tap((frame) =>
            Effect.sync(() => {
              queuedSamples -= frame.samples.length;
              queuedAudioFrames--;
            }),
          ),
        ),
        pressure,
        videoFramesPerSecond: current.media.videoFramesPerSecond,
      },
      mediaState: Effect.sync(() => mediaState),
      sessionId: Effect.sync(() =>
        current === undefined || current.closed ? Option.none() : Option.some(current.source.id),
      ),
      close,
      cleanup: Effect.sync(() =>
        finalReport === undefined ? Option.none() : Option.some(finalReport),
      ),
      sequences: {
        get: affinity.get,
        snapshots: affinity.snapshots,
        seal: affinity.seal,
        acknowledgeIndeterminate: affinity.acknowledgeIndeterminate,
        release: affinity.release,
      },
    } satisfies HandleShape;
  });
