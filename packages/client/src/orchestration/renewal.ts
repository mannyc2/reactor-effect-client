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
import { AcquisitionFailure, CommandFailure, ReactorError } from "../ReactorError.js";
import { errorOf, parsed, positiveLimit } from "../internal/validation.js";

import { Observations } from "../observation.js";
import { noAcquisition } from "../Reactor.js";
import * as Sequence from "../Sequence.js";
import * as Submission from "../Submission.js";
import type { MediaPressure } from "../Media.js";
import * as MediaBuffer from "./media-buffer.js";
import * as SourceSlot from "./source-slot.js";
import * as Retention from "./retention.js";
import { monotonicMillis } from "./elapsed.js";
import { handoffEvidence, decideRenewal } from "./renewal-state.js";
import { PolicyFailure, captureRequest } from "./request.js";
import type { ClipId } from "./request.js";
import { emptyState, isIdle } from "./queries.js";
import { activeIds, generation, resolve } from "./routing.js";
import type { Candidate } from "./routing.js";
import type {
  ClipRecord,
  CleanupReport,
  CleanupSummary,
  ContinuousHandleShape,
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
import { asReactorFailure } from "./policy.js";
import type { OrchestrationFailure } from "./policy.js";
import { summarize } from "../ReactorError.js";

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
   * How long past the retiring source's final clip a planned switch waits for
   * that clip's missing video frames; 250 milliseconds by default. Frames can
   * land after the clip's Ended, but one the provider never sent never does,
   * so the switch then proceeds and `Switched.tail` reports the shortfall. The
   * grace also starts when the source reports nothing playing, in case Ended
   * was lost. At most 5 seconds, since the retiring source running dry within
   * it is not reported as `Starved`. Renewal checks every 100 milliseconds, so
   * the switch can come up to one check after the grace. A bare number is
   * milliseconds.
   */
  readonly handoffGrace?: Duration.Input | undefined;
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

/**
 * Each open must supply a globally unique physical source ID; clip IDs must
 * never be reassigned during this handle's lifetime. Continuous mode checks live
 * collisions, but cannot detect historical reuse after eviction with bounded memory.
 * Source/clip IDs are limited to 1024 UTF-16 code units; each source admits at most
 * 4096 accepted IDs and 4096 distinct Started IDs. Sequence history still requires
 * explicit acknowledgement/release and keeps its independent bounds.
 */
export interface ContinuousOptions<R = never> extends Options<R> {
  /** Supply globally unique source IDs and never reassign clip IDs; historical reuse cannot be checked after eviction. */
  readonly open: Options<R>["open"];
  /** No cumulative successful-open cap when omitted; explicit values retain legacy semantics. */
  readonly maxSessions?: number;
  /** Newest complete cleanup details retained, 0..4096; defaults to 64. */
  readonly retainedSuccessfulCleanups?: number;
  /**
   * Incomplete cleanup or unknown-submission records plus outstanding reservations,
   * 2..4096; defaults to 16. Unknown outcomes can exhaust it despite confirmed termination.
   */
  readonly maxUnresolvedCleanups?: number;
}

interface Owner {
  readonly namespace: string;
  readonly incarnation: bigint;
  readonly sessionId: string;
}
type Slot = SourceSlot.SourceSlot & { readonly owner: Owner };

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

/** A completed caller handle keeps only its Exit, not the routing or source closures. */
const logicalSubmission = (id: string) => {
  let execute: Effect.Effect<ClipId, EngineError> | undefined;
  let inspect: Effect.Effect<Submission.State<ClipId, EngineError>> | undefined;
  let outcome: Exit.Exit<ClipId, EngineError> | undefined;
  return {
    attach: (
      submit: Effect.Effect<ClipId, EngineError>,
      state: Effect.Effect<Submission.State<ClipId, EngineError>>,
    ) => {
      execute = submit;
      inspect = state;
    },
    complete: (exit: Exit.Exit<ClipId, EngineError>) => {
      outcome ??= exit;
      execute = undefined;
      inspect = undefined;
    },
    outcome: () => outcome,
    submission: {
      id,
      submit: Effect.suspend(
        () => outcome ?? execute ?? Effect.die(new Error("Logical submission has no execution")),
      ),
      state: Effect.suspend(() =>
        outcome === undefined
          ? (inspect ?? Effect.succeed<Submission.State<ClipId, EngineError>>({ _tag: "Prepared" }))
          : Effect.succeed<Submission.State<ClipId, EngineError>>({
              _tag: "Completed",
              exit: outcome,
            }),
      ),
    } satisfies Submission.Submission<ClipId, EngineError>,
  };
};

type Replacement =
  | { readonly _tag: "Absent" }
  | { readonly _tag: "Opening"; readonly fiber: Fiber.Fiber<Slot, OrchestrationFailure> }
  | { readonly _tag: "Ready"; readonly slot: Slot };

/**
 * Explicit application policy for renewal, sequence affinity and continuous
 * recovering media. A physical source still owns each connection generation.
 */
const makeOwner = <R>(options: ContinuousOptions<R>, continuous: boolean) =>
  Effect.gen(function* () {
    const scope = yield* Effect.scope;
    const context = yield* Effect.context<R>();
    const clock = yield* Clock.Clock;
    const random = yield* (yield* Crypto.Crypto)
      .randomBytes(16)
      .pipe(Effect.mapError((cause) => errorOf(cause, "InvalidState")));
    const namespace = Array.from(random, (byte) => byte.toString(16).padStart(2, "0")).join("");
    const commands = yield* Semaphore.make(1);
    const affinity = yield* Sequence.makeAffinity<Owner>().pipe(
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
    const fatal = yield* Deferred.make<OrchestrationFailure>();
    let renewing = true;
    const slots = new Map<Owner, Slot>();
    const physicalOwners = new Map<string, Owner>();
    const retention = continuous ? yield* Retention.make(options) : undefined;
    let incarnation = 0n;
    const cleanups: SourceCleanup[] = [];
    const seenCleanups = new Set<SourceCleanup["lease"]>();
    const maxSessions = options.maxSessions ?? (continuous ? Infinity : 64);
    if (
      (options.maxSessions !== undefined || !continuous) &&
      (!Number.isSafeInteger(maxSessions) || maxSessions < 1 || maxSessions > 4096)
    ) {
      return yield* ReactorError.fromCode("InvalidInput", "Invalid orchestration bounds");
    }
    const reconnectTimeout = Duration.toMillis(
      yield* parsed(() =>
        duration(options.reconnectTimeout ?? "10 seconds", "orchestration reconnectTimeout"),
      ),
    );
    const handoffGraceMs = Duration.toMillis(
      yield* parsed(() =>
        duration(options.handoffGrace ?? "250 millis", "orchestration handoffGrace", {
          allowZero: true,
          // Starvation during the grace is not reported, so it stays short.
          maximum: "5 seconds",
        }),
      ),
    );
    const leadSeconds = Duration.toSeconds(
      yield* parsed(() =>
        duration(options.lead ?? "30 seconds", "orchestration lead", { allowZero: true }),
      ),
    );
    let current: Slot | undefined;
    let replacement: Replacement = { _tag: "Absent" };
    let opened = 0n;
    let openFailures = 0;
    let retryAt = 0;
    let autoplay = true;
    let closing = false;
    let finalReport: CleanupReport | undefined;
    let finalSummary: CleanupSummary | undefined;
    let firstCleanup: SourceCleanup | undefined;
    let initializing = true;
    // Close reports each ownership attempt's failures once. They are recorded
    // where the attempt and its retirement end: every waiter of a cached close
    // sees its own Cause object, and cancellation by close is not a failure.
    const attemptFailures = new Map<object, Cause.Reason<never>[]>();
    const recordFailure = (attempt: object, reasons: readonly Cause.Reason<never>[]): void => {
      const failed = reasons.filter((reason) => !Cause.isInterruptReason(reason));
      if (failed.length > 0)
        attemptFailures.set(attempt, [...(attemptFailures.get(attempt) ?? []), ...failed]);
    };
    // Every acquisition runs on a fiber this owner forks into its scope, never
    // on a caller's fiber. Close interrupts and joins the registered ones before
    // retiring owned slots, so no source opens after close and every attempt's
    // cleanup is reported, without waiting on a fiber that may be waiting on it.
    const acquisitions = new Set<Fiber.Fiber<unknown, unknown>>();
    const admissionClosed = () =>
      ReactorError.fromCode("Closed", "Orchestration no longer admits sources");
    // The owner stops renewing when close begins or its scope starts closing,
    // whichever is first: that scope's finalizers can stop an attempt, or wait
    // on an application, before close runs. Commands stay admitted until close
    // begins, so an application finalizer can still issue its last commands.
    const stopping = () => closing || scope.state._tag === "Closed";
    let mediaState: MediaState = { _tag: "Closed" };
    let terminal: Exit.Exit<OrchestrationFailure> | undefined;
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
    const fail = (cause: OrchestrationFailure): Effect.Effect<void> =>
      Effect.suspend(() => {
        if (terminal !== undefined) return Effect.void;
        terminal = Exit.succeed(cause);
        // Publish the failed state and queues before waking a failure waiter.
        setMedia({ _tag: "Failed", cause });
        emit({ _tag: "SessionFailed", failure: cause });
        buffer.failCause(Cause.fail(cause));
        Deferred.doneUnsafe(fatal, Effect.succeed(cause));
        return announce({ _tag: "Failed", reason: cause.message });
      });
    // A failed worker must wake observers as well as failure waiters: the
    // scheduler consumes observations, and cannot recover a vanished worker.
    // Claim and publish synchronously before a waiter can close this owner.
    const supervise = (effect: Effect.Effect<void>): Effect.Effect<void> =>
      effect.pipe(
        Effect.onExit((exit) =>
          Effect.sync(() => {
            if (
              Exit.isSuccess(exit) ||
              terminal !== undefined ||
              (stopping() && Cause.hasInterruptsOnly(exit.cause))
            )
              return;
            terminal = Exit.failCause(exit.cause);
            buffer.failCause(exit.cause);
            observations.failCause(exit.cause);
            Deferred.doneUnsafe(fatal, terminal);
          }),
        ),
      );
    // Every asynchronous activation uses this final fence. Closed/failed state
    // cannot be replaced by a reconnect or autoplay operation that finished late.
    const publishReady = (slot: Slot): boolean => {
      if (stopping() || slot.closed || slot !== current || terminal !== undefined) return false;
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
          entries.some((entry) => entry.owner === slot.owner && entry.status === "open"),
        ),
      );
    const expired = (slot: Slot) => slot.expired();
    const recoveryBudget = (slot: Slot) => slot.recoveryBudget();
    // The logical output's loss: every former owner's loss while it fed the
    // output, plus the current owner's since it became the owner. A prepared
    // replacement's loss before it fed the output is not the output's.
    let outputLoss: SourceSlot.Loss = SourceSlot.noLoss;
    let ownerBaseline: SourceSlot.Loss = SourceSlot.noLoss;
    let outputOwner: Owner | undefined;
    const ownerLoss = (slot: Slot) =>
      Effect.result(slot.pressure.pipe(Effect.timeout(recoveryBudget(slot)))).pipe(
        Effect.map(SourceSlot.lossOf),
      );
    const activateOwner = (slot: Slot) =>
      ownerLoss(slot).pipe(
        Effect.map((loss) => {
          ownerBaseline = loss;
          outputOwner = slot.owner;
        }),
      );
    const retireOwner = (slot: Slot): Effect.Effect<void> =>
      Effect.suspend(() =>
        outputOwner !== slot.owner
          ? Effect.void
          : ownerLoss(slot).pipe(
              Effect.map((loss) => {
                outputLoss = SourceSlot.addLoss(
                  outputLoss,
                  SourceSlot.subtractLoss(loss, ownerBaseline),
                );
                ownerBaseline = SourceSlot.noLoss;
                outputOwner = undefined;
              }),
            ),
      );
    const retired = (slot: Slot) => slot.retired(buffer.forwarded);
    const closeSlot = (slot: Slot) => slot.close;

    const replace = (slot: Slot, cause: OrchestrationFailure): Effect.Effect<void> =>
      Effect.gen(function* () {
        if (slot.closed || stopping() || terminal !== undefined) return;
        const state = yield* slot.source.state;
        const lost = activeIds(state);
        const tail = yield* retired(slot);
        for (const clipId of lost)
          emit({
            _tag: "Failed",
            clipId,
            reason: `Session lost: ${cause.message}`,
            sessionId: slot.source.id,
            lost: true,
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
        // Close retires what remains; a replacement it refuses is no failure.
        if (slot !== current || stopping()) return;
        const pending = replacement;
        if (!renewing && pending._tag === "Absent") {
          yield* fail(cause);
          return;
        }
        const selected =
          pending._tag === "Ready"
            ? pending.slot
            : yield* pending._tag === "Opening"
                ? Effect.uninterruptibleMask((restore) => joinAcquisition(pending.fiber, restore))
                : acquire;
        replacement = { _tag: "Absent" };
        if (stopping() || terminal !== undefined) {
          yield* closeSlot(selected);
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
        Effect.catch((cause) => (stopping() ? Effect.void : fail(cause))),
      );

    const recover = (slot: Slot, cause: OrchestrationFailure): Effect.Effect<void> =>
      Effect.gen(function* () {
        if (slot.closed || stopping() || terminal !== undefined) return;
        if (slot === current) setMedia({ _tag: "Recovering", sessionId: slot.source.id, cause });
        yield* announce({ _tag: "Recovering", sessionId: slot.source.id, reason: cause.message });
        yield* slot.joinCommitted(recoveryBudget(slot));
        if (slot.closed || stopping() || terminal !== undefined) return;
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
        if (slot.closed || stopping() || terminal !== undefined) return;
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
        if (slot.closed || stopping() || terminal !== undefined) return;
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
        if (slot.closed || stopping() || terminal !== undefined) return;
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
      cause: OrchestrationFailure,
      mode: "reconnect" | "replace",
    ): Effect.Effect<void> =>
      Effect.gen(function* () {
        if (slot.closed || stopping() || terminal !== undefined) return;
        if (!slot.beginRecovery(mode)) return;
        yield* recover(slot, cause).pipe(commands.withPermits(1), supervise, Effect.forkIn(scope));
      });

    const startMedia = (slot: Slot): Effect.Effect<void> =>
      slot.startMedia({
        video: (frame) =>
          Effect.suspend(() => {
            if (slot !== current || terminal !== undefined) return Effect.void;
            if (slot.recordVideo() && replacement._tag === "Ready")
              emit({ _tag: "HandoffReady", sessionId: slot.source.id });
            return buffer.offerVideo(frame).pipe(Effect.catch(fail));
          }),
        audio: (frame) =>
          Effect.suspend(() => {
            if (slot !== current || terminal !== undefined) return Effect.void;
            return buffer.offerAudio(frame).pipe(
              Effect.tap(() =>
                Effect.sync(() => {
                  slot.recordAudio(frame.samples.length);
                }),
              ),
              Effect.catch(fail),
            );
          }),
        lost: (cause) => (stopping() ? Effect.void : scheduleRecovery(slot, cause, "reconnect")),
      });

    // Runs only on a fiber of its own, forked into the owner's scope.
    const acquisition: Effect.Effect<Slot, OrchestrationFailure> = Effect.uninterruptibleMask(
      (restore) =>
        Effect.gen(function* () {
          const fiber = yield* Effect.fiber;
          if (opened >= maxSessions)
            return yield* ReactorError.fromCode(
              "Overflow",
              "Orchestration session and cleanup-history bound reached",
            );
          if (stopping() || (continuous && (terminal !== undefined || !renewing)))
            return yield* admissionClosed();
          // Registered in the same synchronous step as the stopping check, so
          // either close sees this attempt or this attempt sees close.
          acquisitions.add(fiber);
          const reservation = retention === undefined ? undefined : yield* retention.reserve;
          const attemptIncarnation = reservation?.incarnation ?? ++incarnation;
          const recordAttempt = (cleanup: SourceCleanup): Effect.Effect<void> => {
            if (initializing && firstCleanup === undefined) firstCleanup = cleanup;
            return retention !== undefined && reservation !== undefined
              ? retention.record(reservation, cleanup)
              : Effect.sync(() => recordCleanup(cleanup));
          };
          const owned = yield* Scope.make();
          const attempt = {};
          let acquired: Slot | undefined;
          let acquiredSource: Source | undefined;
          let acquisitionCleanup: SourceCleanup | undefined;
          let acquisitionDefect: ReactorError | undefined;
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
              if (retention !== undefined && reservation !== undefined)
                yield* retention.identify(reservation, value.source.id);
              const owner: Owner = Object.freeze({
                namespace,
                incarnation: attemptIncarnation,
                sessionId: value.source.id,
              });
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
              if (physicalOwners.has(value.source.id))
                return yield* ReactorError.fromCode(
                  "InvalidInput",
                  "Orchestration sources must have distinct session identities",
                );
              const media = yield* value.source.media;
              const local = yield* SourceSlot.make({
                source: value.source,
                scope: owned,
                media,
                openedAt: monotonicMillis(clock),
                maxSeconds: Duration.toSeconds(lifetime),
                cleanupBudgetMs: reconnectTimeout,
                recordCleanup: recordAttempt,
                boundedHistory: continuous,
                retired: (facts, retirementExit) =>
                  Effect.gen(function* () {
                    const bookkeeping = yield* Effect.exit(
                      Effect.gen(function* () {
                        if (acquisitionCleanup !== undefined)
                          yield* recordAttempt(acquisitionCleanup);
                        if (retention === undefined || reservation === undefined) return;
                        const lossExit =
                          acquired === undefined
                            ? Exit.void
                            : yield* Effect.exit(retireOwner(acquired));
                        yield* retention.finish(reservation, {
                          ...facts,
                          errors: [
                            ...facts.errors,
                            ...(acquisitionDefect === undefined
                              ? []
                              : [summarize(acquisitionDefect)]),
                            ...(Exit.isFailure(lossExit)
                              ? [
                                  summarize(
                                    errorOf(
                                      lossExit.cause,
                                      "InvalidState",
                                      "media loss accounting",
                                    ),
                                  ),
                                ]
                              : []),
                          ],
                        });
                        slots.delete(owner);
                        if (physicalOwners.get(owner.sessionId) === owner)
                          physicalOwners.delete(owner.sessionId);
                        return yield* lossExit;
                      }),
                    );
                    // The slot calls this once, however many callers wait on its close.
                    const exit = Exit.asVoidAll([retirementExit, bookkeeping]);
                    if (Exit.isFailure(exit)) recordFailure(attempt, exit.cause.reasons);
                    return yield* bookkeeping;
                  }),
                retireSequences: affinity.retire(owner).pipe(Effect.asVoid),
              });
              const slot: Slot = Object.assign(local, { owner });
              acquired = slot;
              slots.set(owner, slot);
              physicalOwners.set(owner.sessionId, owner);
              opened++;
              yield* slot.source.setAutoplay(false);
              yield* slot.observe(
                (event) =>
                  Effect.gen(function* () {
                    if (event._tag === "SessionFailed")
                      return yield* scheduleRecovery(slot, event.failure, "replace");
                    // Once the final clip has ended or arrived in full, a planned
                    // switch follows within its bounded grace, so the retiring
                    // source running dry is not starvation. Before its Ended, no
                    // switch is due yet, and that starvation is reported.
                    if (
                      event._tag === "Starved" &&
                      slot === current &&
                      replacement._tag === "Ready" &&
                      (slot.finalClip().video !== "incomplete" ||
                        slot.finalClip().endedAgoMs !== undefined)
                    ) {
                      const next = yield* replacement.slot.source.state;
                      if (
                        next.availability === "Ready" &&
                        next.ready.length > 0 &&
                        isIdle(yield* slot.source.state) &&
                        !(yield* openSequences(slot))
                      )
                        return;
                    }
                    if (event._tag !== "Starved" || slot === current) emit(event);
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
                    // Capture an acquisition failure even if the source also
                    // returns cleanup. Conflicting reports belong to this attempt.
                    // A typed failure is the attempt's result, and cancellation
                    // is not a defect: only a Die is a failure close must report.
                    const defects = exit.cause.reasons.filter(Cause.isDieReason);
                    if (defects.length > 0) {
                      recordFailure(attempt, defects);
                      acquisitionDefect = errorOf(
                        Cause.fromReasons(defects),
                        "InvalidState",
                        "source acquisition",
                      );
                    }
                    const failure = Cause.findErrorOption(exit.cause);
                    if (Option.isSome(failure) && AcquisitionFailure.is(failure.value))
                      acquisitionCleanup = { lease: failure.value.cleanup, policy: [] };
                    if (acquired !== undefined) yield* closeSlot(acquired);
                    else {
                      const sourceExit =
                        acquiredSource === undefined
                          ? Exit.void
                          : yield* Effect.exit(
                              acquiredSource.close.pipe(Effect.flatMap(recordAttempt)),
                            );
                      const scopeExit = yield* Effect.exit(Scope.close(owned, exit));
                      if (acquisitionCleanup !== undefined)
                        yield* recordAttempt(acquisitionCleanup);
                      if (retention !== undefined && reservation !== undefined) {
                        yield* retention.finish(reservation, {
                          accounting: "not-applicable",
                          unknownSubmissions: 0n,
                          affinity: "not-applicable",
                          scope: Exit.isSuccess(scopeExit) ? "closed" : "failed",
                          errors: [
                            ...(acquisitionDefect === undefined
                              ? []
                              : [summarize(acquisitionDefect)]),
                            ...[sourceExit, scopeExit].flatMap((result) =>
                              Exit.isFailure(result)
                                ? [
                                    summarize(
                                      errorOf(
                                        result.cause,
                                        "InvalidState",
                                        "acquisition retirement",
                                      ),
                                    ),
                                  ]
                                : [],
                            ),
                          ],
                        });
                      }
                      const retirementExit = Exit.asVoidAll([sourceExit, scopeExit]);
                      if (Exit.isFailure(retirementExit))
                        recordFailure(attempt, retirementExit.cause.reasons);
                      yield* retirementExit;
                    }
                  }),
            ),
          );
        }),
    ).pipe(
      // The attempt leaves the registry only after its own cleanup has run.
      Effect.onExit(() =>
        Effect.withFiber((fiber) => Effect.sync(() => void acquisitions.delete(fiber))),
      ),
      Effect.tap((slot) => Effect.annotateCurrentSpan("reactor.session.id", slot.source.id)),
      Effect.withSpan("reactor.orchestration.renewal.open", {}, { captureStackTrace: false }),
    );
    // A forked attempt is interruptible whatever its caller is, so close and
    // the owner's scope can stop an open that an uninterruptible caller waits on.
    // A prepared replacement opens on its own after the tick that forks it.
    const forkAcquisition = Effect.forkIn(acquisition, scope);
    // To its caller, an attempt that close, stopRenewal or the owner's scope
    // stopped is refused, not failed. Interrupted while the owner still admits
    // sources, as by its own open, the attempt failed. A caller interrupted
    // while it waits stops the attempt too: the handler is in place before the
    // wait is restored, so an interruption that arrives first still reaches it.
    const joinAcquisition = (
      fiber: Fiber.Fiber<Slot, OrchestrationFailure>,
      restore: <A, E, R>(effect: Effect.Effect<A, E, R>) => Effect.Effect<A, E, R>,
    ): Effect.Effect<Slot, OrchestrationFailure> =>
      restore(Fiber.await(fiber)).pipe(
        Effect.onInterrupt(() => Fiber.interrupt(fiber)),
        Effect.flatMap((exit): Effect.Effect<Slot, OrchestrationFailure> => {
          if (Exit.isSuccess(exit)) return exit;
          const failures = exit.cause.reasons.filter((reason) => !Cause.isInterruptReason(reason));
          if (failures.length > 0) return Effect.failCause(Cause.fromReasons(failures));
          return stopping()
            ? admissionClosed()
            : ReactorError.fromCode("InvalidState", "Source acquisition was interrupted");
        }),
      );
    // The constructor and a replacement start their attempt at once, so an open
    // that completes without waiting is published in the same turn.
    const acquire = Effect.uninterruptibleMask((restore) =>
      Effect.forkIn(acquisition, scope, { startImmediately: true }).pipe(
        Effect.flatMap((fiber) => joinAcquisition(fiber, restore)),
      ),
    );

    const close = yield* SourceSlot.once(
      Effect.uninterruptible(
        Effect.gen(function* () {
          closing = true;
          // Every registered fiber is an attempt this owner forked, never a
          // caller's. Interrupting joins each attempt's cleanup, which records
          // its own defects; cancelling it is not a close failure, and an
          // Opening fiber that has not started never runs. Close does not wait
          // for an attempt it runs on, but one issued from inside `open` cannot
          // finish: `open` runs on a race fiber that stopping its attempt awaits.
          const pending = new Set(acquisitions);
          if (replacement._tag === "Opening") pending.add(replacement.fiber);
          yield* Effect.withFiber((self) => {
            pending.delete(self);
            return Fiber.interruptAll(pending);
          });
          for (const slot of [...slots.values()]) yield* Effect.exit(closeSlot(slot));
          const exits = [...attemptFailures.values()].map((reasons) =>
            Exit.failCause(Cause.fromReasons(reasons)),
          );
          buffer.end();
          setMedia({ _tag: "Closed" });
          observations.end();
          if (renewalReader !== undefined)
            yield* Fiber.await(renewalReader).pipe(
              Effect.interruptible,
              Effect.timeout("1 second"),
              Effect.ignore,
            );
          if (retention === undefined)
            finalReport = Object.freeze({ sessions: Object.freeze([...cleanups]) });
          else finalSummary = yield* retention.conclude;
          // The closed owner stays current, so final loss totals remain readable.
          replacement = { _tag: "Absent" };
          return yield* Exit.asVoidAll(exits);
        }),
      ),
    );
    yield* Effect.addFinalizer(() => close);
    current = yield* acquire.pipe(
      Effect.tap((slot) => slot.source.setAutoplay(true)),
      // Fenced as publishReady is: construction returns no handle whose owner
      // stopped while its first source was activated, and close retires it.
      Effect.tap(() =>
        Effect.gen(function* () {
          if (stopping()) return yield* admissionClosed();
        }),
      ),
      Effect.catch((cause) =>
        Effect.exit(close).pipe(
          Effect.flatMap((closed) => {
            // An AcquisitionFailure from `open` already carries the lease it
            // recorded, by reference; any later failure takes the recorded lease.
            const lease = firstCleanup?.lease;
            const failure =
              AcquisitionFailure.is(cause) || (lease === undefined && ReactorError.is(cause))
                ? cause
                : AcquisitionFailure.from(asReactorFailure(cause), lease ?? noAcquisition);
            // A defect of the close that retired the attempt stays beside the
            // typed failure, which the caller still handles.
            return Exit.isSuccess(closed)
              ? Effect.fail(failure)
              : Effect.failCause(Cause.combine(Cause.fail(failure), closed.cause));
          }),
        ),
      ),
    );
    initializing = false;
    firstCleanup = undefined;
    // The first owner's loss from its start is the output's: no baseline.
    outputOwner = current.owner;
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
          return yield* CommandFailure.from(asReactorFailure(yield* Deferred.await(fatal)), {
            operation,
            outcome: "not-submitted",
          });
        return yield* effect;
      });

    const route = (request: Parameters<EngineShape["prepare"]>[0]) =>
      Effect.gen(function* () {
        const values = yield* Effect.suspend(() =>
          Effect.forEach([...slots.values()], (slot) =>
            Effect.map(slot.source.state, (state): Candidate<Owner> => ({
              owner: slot.owner,
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
        return yield* resolve(request, values, preferred.owner, binding);
      });
    const sequenceError = (error: Sequence.SequenceError) =>
      PolicyFailure.sequence(error.sequenceId, error.code);

    const prepare = (input: Parameters<EngineShape["prepare"]>[0], expectedSessionId?: string) =>
      Effect.gen(function* () {
        const expectedOwner =
          expectedSessionId === undefined ? undefined : physicalOwners.get(expectedSessionId);
        const request = yield* captureRequest(input);
        const id = `${namespace}:${++submissionSequence}`;
        const gate = yield* Semaphore.make(1);
        const logical = logicalSubmission(id);
        let selectedOwner: Owner | undefined;
        let active: Submission.Submission<ClipId, EngineError> | undefined;
        let activeOwner: Slot | undefined;
        const submit = gate.withPermit(
          Effect.gen(function* () {
            const completed = logical.outcome();
            if (completed !== undefined) return yield* completed;
            if (active === undefined) {
              // A committed submission remains a readable outcome after the handle
              // closes. Only selecting or committing fresh work needs a live owner.
              yield* guard("enqueue", Effect.void);
              const decision = yield* route(request).pipe(commands.withPermits(1));
              if (expectedSessionId !== undefined && decision.owner !== expectedOwner)
                return yield* PolicyFailure.refuse(
                  "RouteChanged",
                  "The selected source changed before dispatch",
                );
              if (continuous && selectedOwner !== undefined && selectedOwner !== decision.owner) {
                // The selection stays fenced to its incarnation. While that
                // source is live, only the route moved; it has not retired.
                const selected = slots.get(selectedOwner);
                return yield* selected !== undefined && !selected.closed
                  ? PolicyFailure.refuse(
                      "RouteChanged",
                      "The selected source changed before dispatch",
                    )
                  : PolicyFailure.refuse(
                      "SessionRetired",
                      "Selected source incarnation was retired",
                    );
              }
              selectedOwner = decision.owner;
              const target = slots.get(decision.owner);
              if (target === undefined)
                return yield* PolicyFailure.refuse("SessionRetired", "Selected source was retired");
              const sequence = request.sequence;
              let resultAccounted = false;
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
                                yield* affinity.bind(sequence.id, target.owner);
                                yield* affinity.begin(
                                  sequence.id,
                                  sequence.memberId ?? submissionId,
                                );
                              }
                            }).pipe(Effect.mapError(sequenceError)),
                            (exit) => {
                              logical.complete(exit);
                              active = undefined;
                              activeOwner = undefined;
                            },
                          );
                        }),
                      ),
                    ),
                  result: (submissionId, result) =>
                    Effect.gen(function* () {
                      if (resultAccounted) return;
                      resultAccounted = true;
                      // An accepted identity past the continuous bound is a
                      // source-contract failure, as for Started: it is never
                      // retained, its member cannot be attributed and the source
                      // is replaced.
                      const recorded = yield* Effect.result(target.recordResult(result));
                      if (sequence !== undefined) {
                        const memberId = sequence.memberId ?? submissionId;
                        const account = Result.isFailure(recorded)
                          ? affinity.uncertain(sequence.id, memberId, recorded.failure.message)
                          : Result.isSuccess(result)
                            ? affinity.accepted(
                                sequence.id,
                                memberId,
                                result.success,
                                sequence.final,
                              )
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
                              yield* affinity.retire(target.owner);
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
                      if (Result.isFailure(recorded))
                        yield* scheduleRecovery(target, recorded.failure, "replace");
                      else if (
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
                      if (state._tag === "Completed") {
                        logical.complete(state.exit);
                        activeOwner?.forgetCompleted(selected);
                        active = undefined;
                        activeOwner = undefined;
                      }
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
        logical.attach(
          submit,
          Effect.suspend(() =>
            active === undefined
              ? Effect.succeed<Submission.State<ClipId, EngineError>>({ _tag: "Prepared" })
              : active.state,
          ),
        );
        return logical.submission;
      });

    const state: Effect.Effect<EngineState> = Effect.gen(function* () {
      if (!closing && terminal !== undefined && Exit.isFailure(terminal))
        return yield* Effect.failCause(terminal.cause);
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
        // No media condition: a short final clip holds the switch only for the
        // grace past its Ended, or past the source first reported idle. Gating
        // on the playing clip's frames instead could hold only in the instant
        // between its last frame and Ended, so a lossy clip let the retiring
        // source roll into its next filler.
        handoffReady:
          next !== undefined &&
          preferred === next &&
          current !== undefined &&
          !current.closed &&
          !current.recovering &&
          primary.availability === "Ready",
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
      enqueueOnSource: (request, expectedSessionId) =>
        prepare(request, expectedSessionId).pipe(Effect.flatMap((submission) => submission.submit)),
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
      stopRenewal: Effect.gen(function* () {
        // Fence allocation before waiting on a command already in progress.
        renewing = false;
        yield* commands.withPermit(
          Effect.gen(function* () {
            if (replacement._tag !== "Opening") return;
            const pending = replacement.fiber;
            // It stays Opening until joined, so a concurrent close joins it too.
            yield* Fiber.interrupt(pending);
            const result = yield* Fiber.await(pending);
            if (replacement._tag === "Opening" && replacement.fiber === pending)
              replacement = { _tag: "Absent" };
            if (Exit.isSuccess(result)) yield* closeSlot(result.value);
          }),
        );
      }),
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
      cut: (id) =>
        commands.withPermit(
          guard(
            "stop",
            owner(id, "stop").pipe(
              Effect.flatMap(({ slot, state }) =>
                Option.getOrUndefined(state.playing)?.clipId === id
                  ? slot.source.stop
                  : PolicyFailure.refuse("NotFound", "The clip is no longer playing", "stop"),
              ),
            ),
          ),
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
            stopping() ||
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
              // Close or the owner's scope stopped this attempt, and close
              // retires whatever it opened: that is no failed setup.
              if (stopping()) return;
              if (Exit.isSuccess(result)) {
                replacement = { _tag: "Ready", slot: result.value };
                openFailures = 0;
                yield* log("Prepared the next session for renewal");
                yield* announce({ _tag: "Prepared" });
              } else {
                if (continuous) {
                  const error = Cause.findErrorOption(result.cause);
                  if (
                    Option.isSome(error) &&
                    ReactorError.is(error.value) &&
                    error.value.reason._tag === "Overflow"
                  )
                    return yield* fail(error.value);
                }
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
            running: !stopping() && terminal === undefined,
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
              if (renewing) replacement = { _tag: "Opening", fiber: yield* forkAcquisition };
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
          // The provider reports nothing playing: a lost Ended must not hold
          // the switch until expiry, so the grace also starts here.
          current.observedIdle();
          const evidence = handoffEvidence({
            sequenceOpen: false,
            currentIdle: isIdle(oldState),
            replacementReady: nextState.availability === "Ready" && nextState.ready.length > 0,
            finalClip: current.finalClip(),
            graceMs: handoffGraceMs,
          });
          if (evidence === undefined) return;
          const handoff = Object.freeze({ ...evidence, replacementSessionId: next.source.id });
          // Read only for a switch that happens: it can wait on source pressure.
          const tail = yield* retired(current);
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
            yield* announce({ _tag: "Switched", ...tail, handoff });
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
    yield* Effect.forever(Effect.sleep(100).pipe(Effect.andThen(tick))).pipe(
      supervise,
      Effect.forkIn(scope),
    );

    const pressure: Effect.Effect<MediaPressure, ReactorError> = Effect.suspend(() => {
      if (current === undefined)
        return Effect.fail(ReactorError.fromCode("InvalidState", "No active media source"));
      const owner = current;
      return owner.pressure.pipe(
        Effect.flatMap((source) => {
          const queued = buffer.pressure();
          // A retired owner's loss is already in the output's totals, as the
          // closed continuous owner's is once its retirement completes.
          const loss =
            outputOwner === owner.owner
              ? SourceSlot.addLoss(
                  outputLoss,
                  SourceSlot.subtractLoss(SourceSlot.lossOf(Result.succeed(source)), ownerBaseline),
                )
              : outputLoss;
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
    const facets = {
      engine,
      media: {
        video: buffer.video,
        audio: buffer.audio,
        pressure,
        videoFramesPerSecond: current.media.videoFramesPerSecond,
      },
      mediaState: Effect.suspend(() =>
        !closing && terminal !== undefined && Exit.isFailure(terminal)
          ? Effect.failCause(terminal.cause)
          : Effect.succeed(mediaState),
      ),
      observe: (options) =>
        observations.observeWith(
          Effect.map(state, (engine) => ({ engine, media: mediaState })),
          options,
        ),
      sessionId: Effect.sync(() =>
        current === undefined || current.closed ? Option.none() : Option.some(current.source.id),
      ),
      sequences: {
        get: (id: string) =>
          affinity
            .get(id)
            .pipe(
              Effect.map((entry) =>
                entry === undefined
                  ? undefined
                  : Object.freeze({ ...entry, owner: entry.owner.sessionId }),
              ),
            ),
        snapshots: affinity.snapshots.pipe(
          Effect.map((entries) =>
            Object.freeze(
              entries.map((entry) => Object.freeze({ ...entry, owner: entry.owner.sessionId })),
            ),
          ),
        ),
        seal: affinity.seal,
        acknowledgeIndeterminate: affinity.acknowledgeIndeterminate,
        release: affinity.release,
      },
    } satisfies Omit<HandleShape, "close" | "cleanup">;
    return {
      legacy: {
        ...facets,
        close: close.pipe(
          Effect.andThen(
            Effect.sync(() => {
              if (finalReport === undefined) throw new Error("Missing legacy cleanup report");
              return finalReport;
            }),
          ),
        ),
        cleanup: Effect.sync(() => Option.fromUndefinedOr(finalReport)),
      } satisfies HandleShape,
      continuous: {
        ...facets,
        close: close.pipe(
          Effect.andThen(
            Effect.sync(() => {
              if (finalSummary === undefined) throw new Error("Missing continuous cleanup summary");
              return finalSummary;
            }),
          ),
        ),
        cleanup: Effect.sync(() => Option.fromUndefinedOr(finalSummary)),
      } satisfies ContinuousHandleShape,
    };
  });

/** Legacy renewal retains complete cleanup history and defaults to 64 successful opens. */
export const make = <R>(
  options: Options<R>,
): Effect.Effect<HandleShape, ReactorError | AcquisitionFailure, Scope.Scope | Crypto.Crypto | R> =>
  makeOwner(options, false).pipe(Effect.map((owner) => owner.legacy));

/**
 * Bounded continuous renewal. Adapters must meet ContinuousOptions' unique-ID
 * precondition. Retired anchors no longer retained can refuse as missing rather
 * than SessionRetired; captured operations remain fenced to their exact incarnation.
 */
export const makeContinuous = <R>(
  options: ContinuousOptions<R>,
): Effect.Effect<
  ContinuousHandleShape,
  ReactorError | AcquisitionFailure,
  Scope.Scope | Crypto.Crypto | R
> => makeOwner(options, true).pipe(Effect.map((owner) => owner.continuous));

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
