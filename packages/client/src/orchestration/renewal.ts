import * as Cause from "effect/Cause";
import * as Clock from "effect/Clock";
import * as Context from "effect/Context";
import * as Crypto from "effect/Crypto";
import * as Deferred from "effect/Deferred";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Result from "effect/Result";
import * as Scope from "effect/Scope";
import * as Semaphore from "effect/Semaphore";
import * as Stream from "effect/Stream";
import { duration } from "../duration.js";
import {
  AcquisitionFailure,
  CommandFailure,
  ReactorError,
  errorOf,
  parsed,
  positiveLimit,
} from "../errors.js";
import type { ReactorFailure } from "../errors.js";
import { Observations } from "../observation.js";
import { noAcquisition } from "../session/_internal/acquire.js";
import * as Sequence from "../Sequence.js";
import * as Submission from "../Submission.js";
import type { MediaPressure } from "../session/media.js";
import * as MediaBuffer from "./media-buffer.js";
import * as SourceSlot from "./source-slot.js";
import type { SourceSlot as Slot } from "./source-slot.js";
import { monotonicMillis } from "./elapsed.js";
import { canHandoff, decideRenewal } from "./renewal-state.js";
import { PolicyFailure, captureRequest } from "./request.js";
import type { ClipId } from "./request.js";
import { emptyState, isIdle } from "./queries.js";
import { activeIds, generation, resolve } from "./routing.js";
import type { Candidate } from "./routing.js";
import type {
  ClipRecord,
  CleanupReport,
  EngineError,
  EngineEvent,
  EngineShape,
  EngineState,
  HandleEvent,
  HandleShape,
  MediaState,
  Renewal,
  Source,
  SourceCleanup,
} from "./types.js";
import { handleContext } from "./types.js";
import type { Engine, Handle, Media } from "./types.js";

export type { MediaTail, Renewal } from "./types.js";

/** An opened source and how long its remote session may live. */
export interface Opened {
  readonly source: Source;
  /**
   * The source's remote session lifetime, positive, or `"Infinity"` for a
   * source that never expires. A bare number is milliseconds, so build it with
   * a unit from a grant in seconds: `` `${seconds} seconds` ``.
   */
  readonly lifetime: Duration.Input;
}

export interface Options<R = never> {
  readonly open: Effect.Effect<Opened, ReactorError | AcquisitionFailure, Scope.Scope | R>;
  /**
   * How long before a source's lifetime ends to prepare its replacement; 30
   * seconds by default, and zero prepares at expiry. A bare number is
   * milliseconds, so `lead: 30` is 30 milliseconds.
   */
  readonly lead?: Duration.Input | undefined;
  /**
   * How long recovering a source's connection may take, and the local cleanup
   * budget of a retired source; 10 seconds by default. A bare number is
   * milliseconds.
   */
  readonly reconnectTimeout?: Duration.Input | undefined;
  readonly maxSessions?: number;
  /**
   * The video frames the media output keeps for a reader that has not taken
   * them, 96 by default (4 seconds at 24 fps). Past it the orchestration fails
   * with `Overflow`.
   */
  readonly maxQueuedVideoFrames?: number;
  /**
   * The interleaved audio samples the media output keeps for a reader that has
   * not taken them, 192,000 by default (4 seconds at 48 kHz). Past it the
   * orchestration fails with `Overflow`.
   */
  readonly maxQueuedAudioSamples?: number;
  /**
   * Runs for each renewal event, in order, on a reader the handle forks: it
   * never holds the handle's command permit, so a slow observer delays no
   * enqueue or control. A failure is logged and the next event still runs.
   * Events that queue past 4096 end the reader with an `Overflow` warning; use
   * `observe` to read renewals together with the engine events they follow.
   */
  readonly onRenewal?: (event: Renewal) => Effect.Effect<void>;
}

const engineOnly = (event: HandleEvent): Result.Result<EngineEvent, HandleEvent> =>
  event._tag === "Engine" ? Result.succeed(event.event) : Result.fail(event);
const renewalsOnly = (event: HandleEvent): Result.Result<Renewal, HandleEvent> =>
  event._tag === "Renewal" ? Result.succeed(event.event) : Result.fail(event);

/** A physical source supplies facts; the renewing handle assigns their owner. */
const withSessionId = (state: EngineState, sessionId: string): EngineState => {
  const record = (clip: ClipRecord): ClipRecord => Object.freeze({ ...clip, sessionId });
  return Object.freeze({
    ...state,
    queued: state.queued.map(record),
    building: Option.map(state.building, (build) => ({ ...build, record: record(build.record) })),
    ready: state.ready.map(record),
    playing: Option.map(state.playing, (playing) => ({
      ...playing,
      record: Option.map(playing.record, record),
    })),
  });
};

type Replacement =
  | { readonly _tag: "Absent" }
  | { readonly _tag: "Opening"; readonly fiber: Fiber.Fiber<Slot, ReactorFailure> }
  | { readonly _tag: "Ready"; readonly slot: Slot };

/**
 * Explicit application policy for renewal, sequence affinity and continuous
 * recovering media. A physical source still owns each connection generation.
 */
export const make = <R>(
  options: Options<R>,
): Effect.Effect<HandleShape, ReactorError | AcquisitionFailure, Scope.Scope | Crypto.Crypto | R> =>
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
    // One ordered stream: engine events, renewals and media transitions.
    const observations = new Observations<HandleEvent>();
    const buffer = yield* MediaBuffer.make(
      yield* parsed(() => ({
        videoFrames: positiveLimit(
          options.maxQueuedVideoFrames ?? MediaBuffer.defaultLimits.videoFrames,
          "orchestration maxQueuedVideoFrames",
        ),
        audioSamples: positiveLimit(
          options.maxQueuedAudioSamples ?? MediaBuffer.defaultLimits.audioSamples,
          "orchestration maxQueuedAudioSamples",
        ),
      })),
    );
    const fatal = yield* Deferred.make<ReactorFailure>();
    const slots = new Map<string, Slot>();
    const cleanups: SourceCleanup[] = [];
    const seenCleanups = new Set<SourceCleanup["lease"]>();
    const maxSessions = options.maxSessions ?? 64;
    if (!Number.isSafeInteger(maxSessions) || maxSessions < 1 || maxSessions > 4096) {
      return yield* ReactorError.fromCode("InvalidInput", "Invalid orchestration bounds");
    }
    const reconnectTimeout = Duration.toMillis(
      yield* parsed(() =>
        duration(options.reconnectTimeout ?? "10 seconds", "orchestration reconnectTimeout"),
      ),
    );
    const leadSeconds = Duration.toSeconds(
      yield* parsed(() =>
        duration(options.lead ?? "30 seconds", "orchestration lead", { allowZero: true }),
      ),
    );
    let current: Slot | undefined;
    let replacement: Replacement = { _tag: "Absent" };
    let opened = 0;
    let openFailures = 0;
    let retryAt = 0;
    let autoplay = true;
    let closing = false;
    let finalReport: CleanupReport | undefined;
    let mediaState: MediaState = { _tag: "Closed" };
    let terminalFailure: ReactorFailure | undefined;
    let submissionSequence = 0n;

    const announce = (event: Renewal): Effect.Effect<void> =>
      Effect.sync(() => observations.emit({ _tag: "Renewal", event }, 256));
    const log = (message: string): Effect.Effect<void> =>
      Effect.logDebug(message).pipe(Effect.annotateLogs({ module: "reactor.orchestration" }));
    const emit = (event: EngineEvent): void => observations.emit({ _tag: "Engine", event }, 256);
    const setMedia = (state: MediaState): void => {
      mediaState = state;
      observations.emit({ _tag: "Media", state }, 256);
    };
    const onRenewal = options.onRenewal;
    // Subscribed before the first source opens, so the reader sees its Opened.
    const renewalReader =
      onRenewal === undefined
        ? undefined
        : yield* observations.subscribe({ capacity: 4096 }).pipe(
            Effect.flatMap((stream) =>
              stream.pipe(
                Stream.filterMap(renewalsOnly),
                Stream.runForEach((event) =>
                  Effect.suspend(() => onRenewal(event)).pipe(
                    Effect.catchCause((cause) =>
                      Effect.logWarning("onRenewal failed", cause).pipe(
                        Effect.annotateLogs({ module: "reactor.orchestration" }),
                      ),
                    ),
                  ),
                ),
                Effect.catchCause((cause) =>
                  Effect.logWarning("onRenewal reader stopped", cause).pipe(
                    Effect.annotateLogs({ module: "reactor.orchestration" }),
                  ),
                ),
                Effect.forkIn(scope),
              ),
            ),
          );
    const fail = (cause: ReactorFailure): Effect.Effect<void> =>
      Effect.suspend(() => {
        if (terminalFailure !== undefined) return Effect.void;
        terminalFailure = cause;
        // Publish the failed state and queues before waking a failure waiter.
        setMedia({ _tag: "Failed", cause });
        emit({ _tag: "SessionFailed", failure: cause });
        buffer.fail(cause);
        Deferred.doneUnsafe(fatal, Effect.succeed(cause));
        return announce({ _tag: "Failed", reason: cause.message });
      });
    // Every asynchronous activation uses this final fence. Closed/failed state
    // cannot be replaced by a reconnect or autoplay operation that finished late.
    const publishReady = (slot: Slot): boolean => {
      if (closing || slot.closed || slot !== current || terminalFailure !== undefined) return false;
      setMedia({ _tag: "Ready", sessionId: slot.source.id, generation: slot.media.generation });
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
    // The logical output's loss: every former owner's loss while it fed the
    // output, plus the current owner's since it became the owner. A prepared
    // replacement's loss before it fed the output is not the output's.
    let outputLoss: SourceSlot.Loss = SourceSlot.noLoss;
    let ownerBaseline: SourceSlot.Loss = SourceSlot.noLoss;
    const ownerLoss = (slot: Slot) =>
      Effect.result(slot.pressure.pipe(Effect.timeout(recoveryBudget(slot)))).pipe(
        Effect.map(SourceSlot.lossOf),
      );
    const activateOwner = (slot: Slot) =>
      ownerLoss(slot).pipe(
        Effect.map((loss) => {
          ownerBaseline = loss;
        }),
      );
    const retireOwner = (slot: Slot) =>
      ownerLoss(slot).pipe(
        Effect.map((loss) => {
          outputLoss = SourceSlot.addLoss(outputLoss, SourceSlot.subtractLoss(loss, ownerBaseline));
          ownerBaseline = SourceSlot.noLoss;
        }),
      );
    const retired = (slot: Slot) => slot.retired(buffer.forwarded);
    const closeSlot = (slot: Slot) => slot.close;

    const replace = (slot: Slot, cause: ReactorFailure): Effect.Effect<void> =>
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
        if (slot === current) yield* retireOwner(slot);
        yield* closeSlot(slot);
        yield* announce({
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
        yield* activateOwner(selected);
        yield* current.source.setAutoplay(autoplay);
        if (!publishReady(current)) return;
        yield* log("Replaced lost session; local queue resumes on the new connection");
      }).pipe(
        Effect.withSpan(
          "reactor.orchestration.renewal.replace",
          { attributes: { "reactor.session.id": slot.source.id } },
          { captureStackTrace: false },
        ),
        Effect.catch(fail),
      );

    const recover = (slot: Slot, cause: ReactorFailure): Effect.Effect<void> =>
      Effect.gen(function* () {
        if (slot.closed || closing || terminalFailure !== undefined) return;
        if (slot === current) setMedia({ _tag: "Recovering", sessionId: slot.source.id, cause });
        yield* announce({ _tag: "Recovering", sessionId: slot.source.id, reason: cause.message });
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
            ReactorError.fromCode("TerminalSession", "Source expired during recovery", {
              operation: "renewal",
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
                  ReactorError.fromCode("Disconnected", "Explicit source reconnect timed out"),
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
        yield* announce({
          _tag: "Reconnected",
          sessionId: slot.source.id,
          generation: slot.media.generation,
        });
      }).pipe(
        Effect.withSpan(
          "reactor.orchestration.renewal.recover",
          { attributes: { "reactor.session.id": slot.source.id } },
          { captureStackTrace: false },
        ),
      );

    const scheduleRecovery = (
      slot: Slot,
      cause: ReactorFailure,
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

    const acquire: Effect.Effect<Slot, ReactorFailure> = Effect.uninterruptibleMask((restore) =>
      Effect.gen(function* () {
        if (opened >= maxSessions)
          return yield* ReactorError.fromCode(
            "Overflow",
            "Orchestration session and cleanup-history bound reached",
          );
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
                    ReactorError.fromCode("InvalidState", "Source acquisition timed out"),
                  ),
              }),
            );
            acquiredSource = value.source;
            const lifetime = yield* parsed(() =>
              duration(value.lifetime, "source lifetime", { allowInfinite: true }),
            );
            // A lead as long as the lifetime would prepare a replacement at once,
            // after every open, bounded only by maxSessions.
            if (Duration.isFinite(lifetime) && leadSeconds >= Duration.toSeconds(lifetime))
              return yield* ReactorError.fromCode(
                "InvalidInput",
                "renewal lead must be shorter than the source lifetime",
                { operation: "renewal", outcome: "not-submitted" },
              );
            if (slots.has(value.source.id))
              return yield* ReactorError.fromCode(
                "InvalidInput",
                "Orchestration sources must have distinct session identities",
              );
            const media = yield* value.source.media;
            const slot = yield* SourceSlot.make({
              source: value.source,
              scope: owned,
              media,
              openedAt: monotonicMillis(clock),
              maxSeconds: Duration.toSeconds(lifetime),
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
            yield* announce({
              _tag: "Opened",
              sessionId: slot.source.id,
              lifetime: Duration.seconds(slot.maxSeconds),
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
                      if (Option.isSome(failure) && AcquisitionFailure.is(failure.value))
                        recordCleanup({ lease: failure.value.cleanup, policy: [] });
                    }
                  }
                }),
          ),
        );
      }),
    ).pipe(
      Effect.tap((slot) => Effect.annotateCurrentSpan("reactor.session.id", slot.source.id)),
      Effect.withSpan("reactor.orchestration.renewal.open", {}, { captureStackTrace: false }),
    );

    const close: Effect.Effect<CleanupReport> = closeGate.withPermit(
      Effect.uninterruptible(
        Effect.gen(function* () {
          if (finalReport !== undefined) return finalReport;
          closing = true;
          if (replacement._tag === "Opening") yield* Fiber.interrupt(replacement.fiber);
          yield* Effect.forEach([...slots.values()], closeSlot, { discard: true });
          buffer.end();
          setMedia({ _tag: "Closed" });
          observations.end();
          // The reader delivers what was announced before close, within a bound.
          if (renewalReader !== undefined)
            yield* Fiber.await(renewalReader).pipe(
              Effect.interruptible,
              Effect.timeout("1 second"),
              Effect.ignore,
            );
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
            // An AcquisitionFailure from `open` already carries the lease it
            // recorded, by reference; any later failure takes the recorded lease.
            const lease = report.sessions[0]?.lease;
            return Effect.fail(
              AcquisitionFailure.is(cause) || (lease === undefined && ReactorError.is(cause))
                ? cause
                : AcquisitionFailure.from(cause, lease ?? noAcquisition),
            );
          }),
        ),
      ),
    );
    // The first owner's loss from its start is the output's: no baseline.
    setMedia({
      _tag: "Ready",
      sessionId: current.source.id,
      generation: current.media.generation,
    });

    const guard = <A, E>(
      operation: string,
      effect: Effect.Effect<A, E>,
    ): Effect.Effect<A, E | EngineError> =>
      Effect.gen(function* () {
        if (closing)
          return yield* PolicyFailure.refuse("SessionClosed", "Orchestration is closed", operation);
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
          return yield* PolicyFailure.refuse("SessionRecovering", "No source is ready");
        return yield* resolve(request, values, preferred.source.id, binding);
      });
    const sequenceError = (error: Sequence.SequenceError) =>
      PolicyFailure.sequence(error.sequenceId, error.code);

    const prepare: EngineShape["prepare"] = (input) =>
      Effect.gen(function* () {
        const request = yield* captureRequest(input);
        const id = `${namespace}:${++submissionSequence}`;
        const gate = yield* Semaphore.make(1);
        let active: Submission.Submission<ClipId, EngineError> | undefined;
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
                return yield* PolicyFailure.refuse("SessionRetired", "Selected source was retired");
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
                              "RouteChanged",
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
                                ReactorError.fromCode(
                                  "InvalidState",
                                  "Committed sequence accounting failed",
                                  { detail: cause },
                                ),
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
              ? Effect.succeed<Submission.State<ClipId, EngineError>>({ _tag: "Prepared" })
              : active.state,
          ),
        };
      });

    const state: Effect.Effect<EngineState> = Effect.gen(function* () {
      const live = [...slots.values()].filter((slot) => !slot.closed);
      const values = (yield* Effect.forEach(live, (slot) => slot.source.state)).map(
        (value, index) => withSessionId(value, live[index]!.source.id),
      );
      const primary = values[live.indexOf(current!)] ?? emptyState();
      const next = replacement._tag === "Ready" ? replacement.slot : undefined;
      const preferred =
        next !== undefined && current !== undefined && !(yield* openSequences(current))
          ? next
          : current;
      const builds = values.flatMap((value) =>
        Option.isSome(value.building) ? [value.building.value] : [],
      );
      return Object.freeze({
        ...primary,
        sessions: Object.freeze(
          values.map((value, index) => ({
            sessionId: live[index]!.source.id,
            availability: value.availability,
          })),
        ),
        preferredSessionId: Option.fromUndefinedOr(preferred?.source.id),
        retiringSessionId:
          next !== undefined && current !== undefined && !current.closed
            ? Option.some(current.source.id)
            : Option.none(),
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
            found.length === 0 ? "NotFound" : "OwnerConflict",
            "Clip has no unique active owning session",
            operation,
          );
        if (found[0]!.slot.recovering)
          return yield* PolicyFailure.refuse(
            "SessionRecovering",
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
      events: Stream.filterMap(observations.stream(), engineOnly),
      observe: (options) =>
        observations.observeWith(state, options).pipe(
          Effect.map(({ initial, events }) => ({
            initial,
            events: Stream.filterMap(events, engineOnly),
          })),
        ),
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
                  "InvalidRequest",
                  "Move position must be a nonnegative integer",
                  "move",
                );
              const selected = yield* owner(id, "move");
              const own =
                queue === "generation" ? generation(selected.state) : selected.state.ready;
              if (!own.some((clip) => clip.clipId === id))
                return yield* PolicyFailure.refuse(
                  "QueueChanged",
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
              if (position < preceding || position >= preceding + own.length)
                return yield* PolicyFailure.refuse(
                  "InvalidRequest",
                  "Move position is outside the clip's session range",
                  "move",
                );
              yield* selected.slot.source.move(id, position - preceding);
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
                  "Busy",
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
                yield* announce({ _tag: "Prepared" });
              } else {
                openFailures++;
                retryAt = monotonicMillis(clock) + 5000;
                yield* announce({
                  _tag: "SetupFailed",
                  reason: "Could not prepare the next source",
                  consecutive: openFailures,
                });
                if (openFailures >= 3)
                  return yield* fail(
                    ReactorError.fromCode(
                      "Disconnected",
                      "Repeated source renewal acquisition failed",
                    ),
                  );
              }
            }
          }
          const decision = decideRenewal({
            running: !closing && terminalFailure === undefined,
            now: monotonicMillis(clock),
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
                ReactorError.fromCode("TerminalSession", "Source lifetime limit reached", {
                  operation: "renewal",
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
          // The switch is traced; the tick that found it is not.
          yield* Effect.gen(function* () {
            yield* retireOwner(old);
            current = next;
            yield* activateOwner(next);
            replacement = { _tag: "Absent" };
            yield* next.source.setAutoplay(autoplay);
            const activated = publishReady(next);
            yield* closeSlot(old);
            if (!activated) return;
            yield* announce({ _tag: "Switched", ...tail });
            yield* log("Switched prepared sessions at a sequence boundary");
          }).pipe(
            Effect.withSpan(
              "reactor.orchestration.renewal.switch",
              {
                attributes: {
                  "reactor.session.id": next.source.id,
                  "reactor.session.previous_id": old.source.id,
                },
              },
              { captureStackTrace: false },
            ),
          );
        }),
      )
      .pipe(Effect.catch(fail));
    // Spaced polling, not a fixed rate: each tick runs 100 ms after the previous one
    // finished, so a slow tick delays the next instead of overlapping it.
    yield* Effect.forever(Effect.sleep(100).pipe(Effect.andThen(tick))).pipe(Effect.forkIn(scope));

    const pressure: Effect.Effect<MediaPressure, ReactorError> = Effect.suspend(() => {
      if (current === undefined)
        return Effect.fail(ReactorError.fromCode("InvalidState", "No active media source"));
      const owner = current;
      return owner.pressure.pipe(
        Effect.flatMap((source) => {
          const queued = buffer.pressure();
          const loss = SourceSlot.addLoss(
            outputLoss,
            SourceSlot.subtractLoss(SourceSlot.lossOf(Result.succeed(source)), ownerBaseline),
          );
          if (loss.video === null || loss.audio === null || loss.readers === null)
            return Effect.fail(
              ReactorError.fromCode(
                "InvalidState",
                "A former media owner's loss totals are unknown",
              ),
            );
          return Effect.succeed({
            ...source,
            closed: closing,
            queuedVideo: source.queuedVideo + queued.queuedVideo,
            queuedAudio: source.queuedAudio + queued.queuedAudio,
            queuedBytes: source.queuedBytes + queued.queuedBytes,
            droppedVideo: loss.video,
            droppedAudio: loss.audio,
            readerOverflows: loss.readers,
          });
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
      observe: (options) =>
        observations.observeWith(
          Effect.map(state, (engine) => ({ engine, media: mediaState })),
          options,
        ),
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

/**
 * The handle's `Engine`, `Media` and `Handle` services from one orchestration,
 * so the three can never come from two paid session chains. Each build of the
 * layer opens its own chain: bind it to a `const` and provide that one value.
 */
export const layer = <R>(
  options: Options<R>,
): Layer.Layer<
  Engine | Media | Handle,
  ReactorError | AcquisitionFailure,
  Crypto.Crypto | Exclude<R, Scope.Scope>
> => Layer.effectContext(Effect.map(make(options), handleContext));
