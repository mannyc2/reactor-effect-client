import * as Cause from "effect/Cause";
import * as Clock from "effect/Clock";
import * as Context from "effect/Context";
import * as Crypto from "effect/Crypto";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Option from "effect/Option";
import * as Result from "effect/Result";
import * as Scope from "effect/Scope";
import * as Semaphore from "effect/Semaphore";
import { ReactorError, errorOf } from "../errors.js";
import { Observations } from "../observation.js";
import * as Sequence from "../Sequence.js";
import * as Submission from "../Submission.js";
import { CommandFailure } from "../session/commands.js";
import { AcquisitionFailure } from "../session/index.js";
import type { MediaPressure } from "../session/media.js";
import * as MediaBuffer from "./media-buffer.js";
import * as SourceSlot from "./source-slot.js";
import type { SourceSlot as Slot } from "./source-slot.js";
import { canHandoff, decideRenewal } from "./renewal-state.js";
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

type Replacement =
  | { readonly _tag: "Absent" }
  | { readonly _tag: "Opening"; readonly fiber: Fiber.Fiber<Slot, ReactorError> }
  | { readonly _tag: "Ready"; readonly slot: Slot };

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
    const buffer = yield* MediaBuffer.make;
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
      return yield* new ReactorError({
        code: "InvalidInput",
        message: "Invalid orchestration bounds",
      });
    }
    let current: Slot | undefined;
    let replacement: Replacement = { _tag: "Absent" };
    let opened = 0;
    let openFailures = 0;
    let retryAt = 0;
    let autoplay = true;
    let closing = false;
    let finalReport: CleanupReport | undefined;
    let mediaState: MediaState = { _tag: "Closed" };
    let terminalFailure: ReactorError | undefined;
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
        buffer.fail(cause);
        Deferred.doneUnsafe(fatal, Effect.succeed(cause));
        return observe({ _tag: "Failed", reason: cause.message });
      });
    // Every asynchronous activation uses this final fence. Closed/failed state
    // cannot be replaced by a reconnect or autoplay operation that finished late.
    const publishReady = (slot: Slot): boolean => {
      if (closing || slot.closed || slot !== current || terminalFailure !== undefined) return false;
      mediaState = { _tag: "Ready", sessionId: slot.source.id, generation: slot.media.generation };
      return true;
    };
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
    const expired = (slot: Slot) => slot.expired();
    const recoveryBudget = (slot: Slot) => slot.recoveryBudget();
    const retired = (slot: Slot) => slot.retired(buffer.forwarded);
    const closeSlot = (slot: Slot) => slot.close;

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
        if (replacement._tag === "Ready" && replacement.slot === slot)
          replacement = { _tag: "Absent" };
        if (slot !== current) return;
        const pending = replacement;
        const selected =
          pending._tag === "Ready"
            ? pending.slot
            : yield* pending._tag === "Opening" ? Fiber.join(pending.fiber) : acquire;
        replacement = { _tag: "Absent" };
        if (closing || terminalFailure !== undefined) {
          yield* selected.close;
          return;
        }
        current = selected;
        yield* current.source.setAutoplay(autoplay);
        if (!publishReady(current)) return;
        yield* log("Replaced lost session; local queue resumes on the new connection");
      }).pipe(Effect.catch((failure) => fail(errorOf(failure, "Disconnected"))));

    const recover = (slot: Slot, cause: ReactorError): Effect.Effect<void> =>
      Effect.gen(function* () {
        if (slot.closed || closing || terminalFailure !== undefined) return;
        if (slot === current) mediaState = { _tag: "Recovering", sessionId: slot.source.id, cause };
        yield* observe({ _tag: "Recovering", sessionId: slot.source.id, reason: cause.message });
        yield* slot.joinCommitted(recoveryBudget(slot));
        if (slot.closed || closing || terminalFailure !== undefined) return;
        if (slot.needsReplacement()) {
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
            new ReactorError({
              code: "TerminalSession",
              message: "Source expired during recovery",
              context: { operation: "renewal" },
            }),
          );
          return;
        }
        yield* slot.closeMedia;
        if (slot.closed || closing || terminalFailure !== undefined) return;
        const connected = yield* Effect.result(
          slot.source.reconnect.pipe(
            Effect.timeoutOrElse({
              duration: recoveryBudget(slot),
              orElse: () =>
                Effect.fail(
                  new ReactorError({
                    code: "Disconnected",
                    message: "Explicit source reconnect timed out",
                  }),
                ),
            }),
          ),
        );
        if (slot.closed || closing || terminalFailure !== undefined) return;
        if (Result.isFailure(connected)) {
          yield* replace(slot, connected.failure);
          return;
        }
        if (slot.needsReplacement()) {
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
            yield* slot.replaceMedia(media, previousPressure);
            return yield* startMedia(slot);
          }),
        );
        if (slot.closed || closing || terminalFailure !== undefined) return;
        if (Result.isFailure(restarted)) {
          yield* replace(slot, restarted.failure);
          return;
        }
        if (slot.needsReplacement() || !slot.recovered()) {
          yield* replace(slot, cause);
          return;
        }
        if (slot === current && !publishReady(slot)) return;
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
        if (!slot.beginRecovery(mode)) return;
        yield* recover(slot, cause).pipe(commands.withPermits(1), Effect.forkIn(scope));
      });

    const startMedia = (slot: Slot): Effect.Effect<void> =>
      slot.startMedia({
        video: (frame) =>
          Effect.suspend(() => {
            if (slot !== current || terminalFailure !== undefined) return Effect.void;
            slot.recordVideo();
            return buffer.offerVideo(frame).pipe(Effect.catch(fail));
          }),
        audio: (frame) =>
          Effect.suspend(() => {
            if (slot !== current || terminalFailure !== undefined) return Effect.void;
            return buffer.offerAudio(frame).pipe(
              Effect.tap(() =>
                Effect.sync(() => {
                  slot.recordAudio(frame.samples.length);
                }),
              ),
              Effect.catch(fail),
            );
          }),
        lost: (cause) => (closing ? Effect.void : scheduleRecovery(slot, cause, "reconnect")),
      });

    const acquire: Effect.Effect<Slot, ReactorError> = Effect.uninterruptibleMask((restore) =>
      Effect.gen(function* () {
        if (opened >= maxSessions)
          return yield* new ReactorError({
            code: "Overflow",
            message: "Orchestration session and cleanup-history bound reached",
          });
        const owned = yield* Scope.make();
        let acquired: Slot | undefined;
        let acquiredSource: Source | undefined;
        return yield* restore(
          Effect.gen(function* () {
            // Replace the captured Scope explicitly in one context operation.
            // Dependency provision order cannot move resources to the caller's owner.
            const value = yield* options.open.pipe(
              Effect.provideContext(Context.add(context, Scope.Scope, owned)),
              Effect.timeoutOrElse({
                duration: "30 seconds",
                orElse: () =>
                  Effect.fail(
                    new ReactorError({
                      code: "InvalidState",
                      message: "Source acquisition timed out",
                    }),
                  ),
              }),
              Effect.mapError((cause) => errorOf(cause, "InvalidState")),
            );
            acquiredSource = value.source;
            if (
              !(value.maxSeconds > 0) ||
              (value.maxSeconds !== Infinity && !Number.isFinite(value.maxSeconds))
            ) {
              return yield* new ReactorError({
                code: "InvalidInput",
                message: "Source lifetime must be positive or infinite",
              });
            }
            if (slots.has(value.source.id))
              return yield* new ReactorError({
                code: "InvalidInput",
                message: "Orchestration sources must have distinct session identities",
              });
            const media = yield* value.source.media;
            const slot = yield* SourceSlot.make({
              source: value.source,
              scope: owned,
              media,
              openedAt: clock.currentTimeMillisUnsafe(),
              maxSeconds: value.maxSeconds,
              cleanupBudgetMs: reconnectTimeout,
              recordCleanup,
              retireSequences: affinity.retire(value.source.id).pipe(Effect.asVoid),
            });
            acquired = slot;
            slots.set(slot.source.id, slot);
            opened++;
            yield* slot.source.setAutoplay(false);
            yield* slot.observe(
              (event) =>
                Effect.suspend(() => {
                  if (event._tag === "SessionFailed")
                    return scheduleRecovery(slot, event.failure, "replace");
                  if (event._tag !== "Starved" || slot === current) emit(event);
                  return Effect.void;
                }),
              (cause) => scheduleRecovery(slot, cause, "replace"),
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
          if (replacement._tag === "Opening") yield* Fiber.interrupt(replacement.fiber);
          yield* Effect.forEach([...slots.values()], closeSlot, { discard: true });
          buffer.end();
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
            return Effect.fail(lease === undefined ? cause : AcquisitionFailure.from(cause, lease));
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
          return yield* PolicyFailure.refuse(
            "session_closed",
            "Orchestration is closed",
            operation,
          );
        if (yield* Deferred.isDone(fatal))
          return yield* CommandFailure.from(yield* Deferred.await(fatal), {
            operation,
            outcome: "not-submitted",
          });
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
        const next = replacement._tag === "Ready" ? replacement.slot : undefined;
        const preferred =
          next !== undefined && current !== undefined && !(yield* openSequences(current))
            ? next
            : current;
        if (preferred === undefined)
          return yield* PolicyFailure.refuse("session_recovering", "No source is ready");
        return yield* resolve(request, values, preferred.source.id, binding);
      });
    const sequenceError = (error: Sequence.SequenceError) =>
      PolicyFailure.refuse(
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
              const decision = yield* route(request).pipe(commands.withPermits(1));
              const target = slots.get(decision.owner);
              if (target === undefined)
                return yield* PolicyFailure.refuse(
                  "session_retired",
                  "Selected source was retired",
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
                            return yield* PolicyFailure.refuse(
                              "route_changed",
                              "Request ownership or insertion position changed during preparation",
                            );
                          }
                          if (active === undefined)
                            return yield* Effect.die(
                              new Error("Source committed before returning its inert submission"),
                            );
                          yield* target.register(
                            active,
                            Effect.gen(function* () {
                              if (sequence !== undefined) {
                                yield* affinity.bind(sequence.id, target.source.id);
                                yield* affinity.begin(
                                  sequence.id,
                                  sequence.memberId ?? submissionId,
                                );
                              }
                            }).pipe(Effect.mapError(sequenceError)),
                          );
                        }),
                      ),
                    ),
                  result: (submissionId, result) =>
                    Effect.gen(function* () {
                      target.recordResult(result);
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
                              target.markIndeterminate();
                              yield* affinity.retire(target.source.id);
                              yield* fail(
                                new ReactorError({
                                  code: "InvalidState",
                                  message: "Committed sequence accounting failed",
                                  context: { detail: cause },
                                }),
                              );
                            }),
                          ),
                        );
                      }
                      target.finishAccounting();
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
                      if (state._tag === "Completed") activeOwner?.forgetCompleted(selected);
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
          return yield* PolicyFailure.refuse(
            found.length === 0 ? "not_found" : "owner_conflict",
            "Clip has no unique active owning session",
            operation,
          );
        if (found[0]!.slot.recovering)
          return yield* PolicyFailure.refuse(
            "session_recovering",
            "Owning session is recovering",
            operation,
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
                return yield* PolicyFailure.refuse(
                  "invalid_request",
                  "Move position must be a nonnegative integer",
                  "move",
                );
              const selected = yield* owner(id, "move");
              const own =
                queue === "generation" ? generation(selected.state) : selected.state.ready;
              if (!own.some((clip) => clip.clipId === id))
                return yield* PolicyFailure.refuse(
                  "queue_changed",
                  "Clip is no longer in the selected application queue",
                  "move",
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
                return yield* PolicyFailure.refuse(
                  "busy",
                  "Canvas can only change while every source is idle",
                  "set_canvas",
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
          if (replacement._tag === "Opening") {
            const result = replacement.fiber.pollUnsafe();
            if (result !== undefined) {
              replacement = { _tag: "Absent" };
              if (Exit.isSuccess(result)) {
                replacement = { _tag: "Ready", slot: result.value };
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
                    new ReactorError({
                      code: "Disconnected",
                      message: "Repeated source renewal acquisition failed",
                    }),
                  );
              }
            }
          }
          const decision = decideRenewal({
            running: !closing && terminalFailure === undefined,
            now: clock.currentTimeMillisUnsafe(),
            current,
            replacement: replacement._tag === "Ready" ? replacement.slot.phase : replacement._tag,
            leadSeconds,
            retryAt,
          });
          switch (decision) {
            case "Retain":
              return;
            case "Expire":
              yield* replace(
                current,
                new ReactorError({
                  code: "TerminalSession",
                  message: "Source lifetime limit reached",
                  context: { operation: "renewal" },
                }),
              );
              return;
            case "Prepare":
              replacement = { _tag: "Opening", fiber: yield* acquire.pipe(Effect.forkIn(scope)) };
              return;
            case "InspectHandoff":
              break;
          }
          const next = replacement._tag === "Ready" ? replacement.slot : undefined;
          if (next === undefined || (yield* openSequences(current))) return;
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
            !canHandoff({
              sequenceOpen: false,
              currentIdle: isIdle(oldState),
              replacementReady: nextState.availability === "Ready" && nextState.ready.length > 0,
              video: tail.tail.video.status,
              droppedVideo: tail.tail.sourceDrops.video,
              droppedAudio: tail.tail.sourceDrops.audio,
            })
          )
            return;
          const old = current;
          current = next;
          replacement = { _tag: "Absent" };
          yield* current.source.setAutoplay(autoplay);
          const activated = publishReady(current);
          yield* closeSlot(old);
          if (!activated) return;
          yield* observe({ _tag: "Switched", ...tail });
          yield* log("Switched prepared sessions at a sequence boundary");
        }),
      )
      .pipe(Effect.catch((cause) => fail(errorOf(cause, "Disconnected"))));
    yield* Effect.forever(Effect.sleep(100).pipe(Effect.andThen(tick))).pipe(Effect.forkIn(scope));

    const pressure: Effect.Effect<MediaPressure, ReactorError> = Effect.suspend(() => {
      if (current === undefined)
        return Effect.fail(
          new ReactorError({ code: "InvalidState", message: "No active media source" }),
        );
      const owner = current;
      return owner.pressure.pipe(
        Effect.map((source) => {
          const queued = buffer.pressure();
          return {
            ...source,
            closed: closing,
            queuedVideo: source.queuedVideo + queued.queuedVideo,
            queuedAudio: source.queuedAudio + queued.queuedAudio,
            queuedBytes: source.queuedBytes + queued.queuedBytes,
          };
        }),
      );
    });
    return {
      engine,
      media: {
        video: buffer.video,
        audio: buffer.audio,
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
