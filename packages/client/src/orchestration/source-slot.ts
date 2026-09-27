import * as Clock from "effect/Clock";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Result from "effect/Result";
import * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import { errorOf } from "../internal/validation.js";
import { ReactorError } from "../ReactorError.js";
import type { Submission } from "../Submission.js";
import type { AudioFrame, VideoFrame, MediaPressure } from "../Media.js";
import { PolicyFailure, type ClipId } from "./request.js";
import { monotonicMillis } from "./elapsed.js";
import * as Lifecycle from "./renewal-state.js";
import type { Retirement } from "./retention.js";
import type { EngineError, EngineEvent, MediaSource, Source, SourceCleanup } from "./types.js";
import { summarize } from "../ReactorError.js";

interface Options extends Lifecycle.Lifetime {
  readonly source: Source;
  readonly scope: Scope.Closeable;
  readonly media: MediaSource;
  readonly cleanupBudgetMs: number;
  readonly recordCleanup: (cleanup: SourceCleanup) => Effect.Effect<void>;
  readonly retired: (retirement: Retirement, exit: Exit.Exit<void>) => Effect.Effect<void>;
  readonly boundedHistory: boolean;
  readonly retireSequences: Effect.Effect<void>;
}

interface MediaReaders {
  readonly video: (frame: VideoFrame) => Effect.Effect<void>;
  readonly audio: (frame: AudioFrame) => Effect.Effect<void>;
  readonly lost: (cause: ReactorError) => Effect.Effect<void>;
}

const sumDrops = (retained: bigint | null, latest: bigint | null): bigint | null =>
  retained === null || latest === null ? null : retained + latest;

/** Loss counters; `null` when a retired generation's counters could not be read. */
export interface Loss {
  readonly video: bigint | null;
  readonly audio: bigint | null;
  readonly readers: bigint | null;
}
export const noLoss: Loss = { video: 0n, audio: 0n, readers: 0n };
export const lossOf = (pressure: Result.Result<MediaPressure, unknown>): Loss =>
  Result.isSuccess(pressure)
    ? {
        video: pressure.success.droppedVideo,
        audio: pressure.success.droppedAudio,
        readers: pressure.success.readerOverflows,
      }
    : { video: null, audio: null, readers: null };
export const addLoss = (a: Loss, b: Loss): Loss => ({
  video: sumDrops(a.video, b.video),
  audio: sumDrops(a.audio, b.audio),
  readers: sumDrops(a.readers, b.readers),
});
const minus = (a: bigint | null, b: bigint | null): bigint | null =>
  a === null || b === null ? null : a - b;
export const subtractLoss = (a: Loss, b: Loss): Loss => ({
  video: minus(a.video, b.video),
  audio: minus(a.audio, b.audio),
  readers: minus(a.readers, b.readers),
});

/**
 * Runs a close at most once and gives every caller its Exit. Unlike
 * `Effect.cached`, the first caller claims the run and enters it in one
 * uninterruptible step: an interruption between the two would otherwise be
 * cached as the close's result, or strand later callers on a run never
 * entered. Later callers still wait interruptibly.
 */
export const once = <A, E>(self: Effect.Effect<A, E>) =>
  Effect.sync(() => {
    const done = Deferred.makeUnsafe<void>();
    let exit: Exit.Exit<A, E> | undefined;
    let started = false;
    return Effect.uninterruptibleMask((restore) =>
      Effect.suspend(() => {
        if (exit !== undefined) return exit;
        if (started)
          return restore(Deferred.await(done)).pipe(
            Effect.andThen(
              Effect.suspend(() => exit ?? Effect.die(new Error("Joined close has no Exit"))),
            ),
          );
        started = true;
        return Effect.onExit(self, (result) =>
          Effect.sync(() => {
            exit = result;
            Deferred.doneUnsafe(done, Effect.void);
          }),
        );
      }),
    );
  });

/**
 * One physical source's local owner. The source closes its remote lease; this
 * owner then joins its registered committed executions before closing their scope.
 * Logical preparation is never registered here, and this module never dispatches it.
 */
export const make = (options: Options) =>
  Effect.gen(function* () {
    const clock = yield* Clock.Clock;
    let phase: Lifecycle.SourcePhase = { _tag: "Active" };
    let indeterminate = false;
    let unknownSubmissions = 0n;
    let contractFailure: ReactorError | undefined;
    let inFlight = 0;
    let settled = yield* Deferred.make<void>();
    yield* Deferred.succeed(settled, undefined);
    const submissions = new Map<
      Submission<ClipId, EngineError>,
      (exit: Exit.Exit<ClipId, EngineError>) => void
    >();
    const accepted = new Set<ClipId>();
    const started = new Set<ClipId>();
    let media = options.media;
    let mediaScope: Scope.Closeable | undefined;
    let expectedFrames = 0;
    let receivedFrames = 0;
    let finalClipFrames = 0;
    let finalClipReceived = 0;
    let framesAtBoundary = 0;
    let finalClipId: ClipId | undefined;
    let finalClipEndedAt: number | undefined;
    let finalClipGraceOrigin: "Ended" | "Idle" | undefined;
    let receivedAudioSamples = 0;
    let retiredDrops: Loss = noLoss;

    const closed = () => Lifecycle.isClosed(phase);
    const recoveryBudget = () =>
      Lifecycle.recoveryBudget(options, monotonicMillis(clock), options.cleanupBudgetMs);
    const closeMedia = Effect.suspend(() =>
      mediaScope === undefined ? Effect.void : Scope.close(mediaScope, Exit.void),
    );

    const completed = (submission: Submission<ClipId, EngineError>) =>
      submission.state.pipe(
        Effect.map((state) => {
          if (state._tag !== "Completed") return;
          submissions.get(submission)?.(state.exit);
          submissions.delete(submission);
        }),
      );

    // Whether committed accounting settled within this wait. A recovery wait
    // that gives up marks the source indeterminate for replacement; only the
    // retirement's own wait decides its reported accounting.
    const joinCommitted = (budget: number): Effect.Effect<boolean> =>
      Effect.suspend(() =>
        Effect.all(
          [
            Deferred.await(settled),
            // A result hook can signal accounting before execution returns. Join
            // the original handles as well so closing the scope cannot erase a known result.
            Effect.forEach(
              [...submissions.keys()],
              (submission) =>
                Effect.exit(submission.submit).pipe(Effect.andThen(completed(submission))),
              {
                concurrency: "unbounded",
                discard: true,
              },
            ),
          ],
          { concurrency: "unbounded", discard: true },
        ),
      ).pipe(
        Effect.as(true),
        Effect.interruptible,
        Effect.timeoutOrElse({
          duration: budget,
          orElse: () =>
            Effect.sync(() => {
              indeterminate = true;
              return false;
            }),
        }),
      );

    const close = yield* once(
      Effect.uninterruptible(
        Effect.gen(function* () {
          phase = Lifecycle.transitionSource(phase, { _tag: "Close" });
          // Each stage owns an independent obligation. A defect is retained while
          // later stages still run; a stalled finalizer keeps this close pending.
          const mediaExit = yield* Effect.exit(closeMedia);
          const sourceExit = yield* Effect.exit(
            options.source.close.pipe(Effect.flatMap(options.recordCleanup)),
          );
          const accountingExit = yield* Effect.exit(joinCommitted(options.cleanupBudgetMs));
          const affinityExit = yield* Effect.exit(options.retireSequences);
          const scopeExit = yield* Effect.exit(Scope.close(options.scope, Exit.void));
          const completionExit = yield* Effect.exit(
            Effect.forEach([...submissions.keys()], completed, { discard: true }),
          );
          const stages: Exit.Exit<unknown>[] = [
            mediaExit,
            sourceExit,
            accountingExit,
            affinityExit,
            scopeExit,
            completionExit,
          ];
          const errors = stages.flatMap((exit) =>
            Exit.isFailure(exit)
              ? [summarize(errorOf(exit.cause, "InvalidState", "source retirement"))]
              : [],
          );
          submissions.clear();
          if (contractFailure !== undefined) errors.push(summarize(contractFailure));
          phase = Lifecycle.transitionSource(phase, { _tag: "Closed" });
          const exit = Exit.asVoidAll(stages);
          // Completion bookkeeping cannot hide a stage's failure, or its own.
          const retiredExit = yield* Effect.exit(
            options.retired(
              {
                accounting:
                  Exit.isSuccess(accountingExit) && accountingExit.value ? "settled" : "timed-out",
                scope: Exit.isSuccess(scopeExit) ? "closed" : "failed",
                affinity: Exit.isSuccess(affinityExit) ? "retired" : "failed",
                errors,
                unknownSubmissions,
              },
              exit,
            ),
          );
          return yield* Exit.asVoidAll([exit, retiredExit]);
        }),
      ).pipe(
        Effect.withSpan(
          "reactor.orchestration.source.close",
          { attributes: { "reactor.session.id": options.source.id } },
          { captureStackTrace: false },
        ),
      ),
    );

    return {
      source: options.source,
      openedAt: options.openedAt,
      maxSeconds: options.maxSeconds,
      get phase() {
        return phase;
      },
      get closed() {
        return closed();
      },
      get recovering() {
        return phase._tag === "Recovering";
      },
      get media() {
        return media;
      },
      finalClip: (): Lifecycle.FinalClip => ({
        clipId: finalClipId,
        expectedVideoFrames: finalClipFrames,
        receivedVideoFrames: finalClipReceived,
        video:
          finalClipFrames === 0
            ? "not-started"
            : finalClipReceived >= finalClipFrames
              ? "count-complete"
              : "incomplete",
        endedAgoMs:
          finalClipEndedAt === undefined ? undefined : monotonicMillis(clock) - finalClipEndedAt,
        graceOrigin: finalClipGraceOrigin,
      }),
      /** The source reports nothing playing; starts the grace if no Ended was seen. */
      observedIdle: (): void => {
        if (finalClipFrames > 0 && finalClipEndedAt === undefined) {
          finalClipEndedAt = monotonicMillis(clock);
          finalClipGraceOrigin = "Idle";
        }
      },
      get accepted(): ReadonlySet<ClipId> {
        return accepted;
      },
      recoveryBudget,
      expired: () => Lifecycle.expired(options, monotonicMillis(clock)),
      needsReplacement: () =>
        Lifecycle.needsReplacement(
          phase,
          indeterminate,
          Lifecycle.expired(options, monotonicMillis(clock)),
        ),
      beginRecovery: (mode: Lifecycle.RecoveryMode): boolean => {
        const previous = phase;
        phase = Lifecycle.transitionSource(phase, { _tag: "Recover", mode });
        return previous._tag === "Active" && phase._tag === "Recovering";
      },
      recovered: (): boolean => {
        phase = Lifecycle.transitionSource(phase, { _tag: "Recovered" });
        return phase._tag === "Active";
      },
      markIndeterminate: (): void => {
        indeterminate = true;
      },
      joinCommitted,
      close,
      closeMedia,
      /** Called only from the physical Submission's existing commit hook, under admission serialization. */
      register: (
        submission: Submission<ClipId, EngineError>,
        admit: Effect.Effect<void, EngineError>,
        onCompleted: (exit: Exit.Exit<ClipId, EngineError>) => void,
      ) =>
        Effect.gen(function* () {
          if (closed())
            return yield* PolicyFailure.refuse(
              "SessionRetired",
              "Selected source incarnation was retired",
            );
          for (const previous of submissions.keys()) yield* completed(previous);
          if (submissions.size >= 4096)
            return yield* PolicyFailure.refuse(
              "SubmissionCapacity",
              "Committed source submissions reached their bound",
            );
          if (options.boundedHistory && accepted.size + inFlight >= 4096)
            return yield* PolicyFailure.refuse(
              "SubmissionCapacity",
              "Source accepted-history capacity reached",
            );
          yield* admit;
          if (inFlight++ === 0) settled = Deferred.makeUnsafe<void>();
          submissions.set(submission, onCompleted);
          // The source scope is closed only after committed joins. This observer
          // releases logical references even if the caller abandoned its wait.
          yield* Effect.exit(submission.submit).pipe(
            Effect.andThen(completed(submission)),
            Effect.forkIn(options.scope),
          );
        }),
      recordResult: (result: Result.Result<ClipId, EngineError>) =>
        Effect.suspend(() => {
          if (Result.isSuccess(result)) {
            if (
              options.boundedHistory &&
              (typeof result.success !== "string" ||
                result.success.length === 0 ||
                result.success.length > 1024)
            ) {
              indeterminate = true;
              contractFailure ??= ReactorError.fromCode(
                "Protocol",
                "Source accepted identity exceeds its bound",
              );
              return Effect.fail(contractFailure);
            }
            accepted.add(result.success);
          } else if (result.failure.context.outcome === "unknown") {
            indeterminate = true;
            unknownSubmissions++;
          }
          return Effect.void;
        }),
      finishAccounting: (): void => {
        if (--inFlight === 0) Deferred.doneUnsafe(settled, Effect.void);
      },
      forgetCompleted: (submission: Submission<ClipId, EngineError>): void => {
        submissions.delete(submission);
      },
      // Subscribed in the slot's scope when the slot starts observing, before
      // anything else runs, so a source event is never emitted between the two.
      observe: (
        receive: (event: EngineEvent) => Effect.Effect<void>,
        failed: (cause: ReactorError) => Effect.Effect<void>,
      ) =>
        options.source.observe().pipe(
          Scope.provide(options.scope),
          Effect.flatMap(({ events }) =>
            events.pipe(
              Stream.runForEach((event) =>
                Effect.suspend(() => {
                  if (closed()) return Effect.void;
                  // A repeated Started, as after a reconnect, is still playing.
                  if (event._tag === "Started") {
                    finalClipEndedAt = undefined;
                    finalClipGraceOrigin = undefined;
                  }
                  if (event._tag === "Started" && !started.has(event.clipId)) {
                    if (
                      options.boundedHistory &&
                      (typeof event.clipId !== "string" ||
                        event.clipId.length === 0 ||
                        event.clipId.length > 1024)
                    ) {
                      contractFailure ??= ReactorError.fromCode(
                        "Protocol",
                        "Source Started identity exceeds its bound",
                      );
                      return Effect.fail(contractFailure);
                    }
                    if (options.boundedHistory && started.size >= 4096) {
                      contractFailure ??= ReactorError.fromCode(
                        "Overflow",
                        "Source Started history capacity reached",
                      );
                      return Effect.fail(contractFailure);
                    }
                    started.add(event.clipId);
                    finalClipId = event.clipId;
                    finalClipFrames = Math.round(
                      event.durationSeconds * media.videoFramesPerSecond,
                    );
                    // Media and provider events use separate readers. Include
                    // frames that overtook Started after the preceding Ended.
                    finalClipReceived = receivedFrames - framesAtBoundary;
                    expectedFrames += finalClipFrames;
                  }
                  if (event._tag === "Ended") {
                    framesAtBoundary = receivedFrames;
                    finalClipEndedAt = monotonicMillis(clock);
                    finalClipGraceOrigin = "Ended";
                  }
                  return receive(event);
                }),
              ),
              Effect.catch(failed),
              Effect.forkIn(options.scope),
            ),
          ),
          Effect.catch(failed),
          Effect.asVoid,
        ),
      startMedia: (readers: MediaReaders) =>
        Effect.gen(function* () {
          if (closed()) return;
          const owned = yield* Scope.fork(options.scope);
          mediaScope = owned;
          const generation = media.generation;
          const lost = (cause: ReactorError) =>
            closed() || media.generation !== generation ? Effect.void : readers.lost(cause);
          yield* media.video.pipe(
            Stream.runForEach((frame) =>
              closed() || media.generation !== generation ? Effect.void : readers.video(frame),
            ),
            Effect.andThen(lost(ReactorError.fromCode("Disconnected", "Video generation ended"))),
            Effect.catch(lost),
            Effect.forkIn(owned),
          );
          yield* media.audio.pipe(
            Stream.runForEach((frame) =>
              closed() || media.generation !== generation ? Effect.void : readers.audio(frame),
            ),
            Effect.andThen(lost(ReactorError.fromCode("Disconnected", "Audio generation ended"))),
            Effect.catch(lost),
            Effect.forkIn(owned),
          );
        }),
      replaceMedia: (
        next: MediaSource,
        previous: Result.Result<MediaPressure, unknown>,
      ): Effect.Effect<void, ReactorError> =>
        Effect.suspend(() => {
          if (closed())
            return Effect.fail(
              ReactorError.fromCode("Closed", "Retired source cannot acquire a media generation"),
            );
          if (next.generation <= media.generation)
            return Effect.fail(
              ReactorError.fromCode("Protocol", "Reconnect did not acquire a new media generation"),
            );
          retiredDrops = addLoss(retiredDrops, lossOf(previous));
          media = next;
          return Effect.void;
        }),
      recordVideo: (): boolean => {
        receivedFrames++;
        finalClipReceived++;
        return finalClipFrames > 0 && finalClipReceived === finalClipFrames;
      },
      recordAudio: (samples: number): void => {
        receivedAudioSamples += samples;
      },
      pressure: Effect.suspend(() =>
        media.pressure.pipe(
          Effect.flatMap((pressure) => {
            const loss = addLoss(retiredDrops, lossOf(Result.succeed(pressure)));
            return loss.video === null || loss.audio === null || loss.readers === null
              ? Effect.fail(
                  ReactorError.fromCode(
                    "InvalidState",
                    "Retired media generation drop totals are unknown",
                  ),
                )
              : Effect.succeed({
                  ...pressure,
                  droppedVideo: loss.video,
                  droppedAudio: loss.audio,
                  readerOverflows: loss.readers,
                });
          }),
        ),
      ),
      retired: (
        forwarded: () => {
          readonly queuedVideoFrames: number;
          readonly queuedAudioSamples: number;
        },
      ) =>
        Effect.gen(function* () {
          const pressure = yield* Effect.result(
            media.pressure.pipe(Effect.timeout(recoveryBudget())),
          );
          return {
            sessionId: options.source.id,
            ageSeconds: Math.max(0, Lifecycle.ageMillis(options, monotonicMillis(clock)) / 1000),
            tail: {
              video: {
                framesPerSecond: media.videoFramesPerSecond,
                expectedFrames,
                receivedFrames,
                status:
                  expectedFrames === 0
                    ? ("not-started" as const)
                    : receivedFrames >= expectedFrames
                      ? ("count-complete" as const)
                      : ("incomplete" as const),
              },
              audio: { receivedSamples: receivedAudioSamples, status: "unverified" as const },
              sourceDrops: Result.isSuccess(pressure)
                ? {
                    video: sumDrops(retiredDrops.video, pressure.success.droppedVideo),
                    audio: sumDrops(retiredDrops.audio, pressure.success.droppedAudio),
                  }
                : { video: null, audio: null },
              forwarded: forwarded(),
            },
          };
        }),
    };
  });

export type SourceSlot = Effect.Success<ReturnType<typeof make>>;
