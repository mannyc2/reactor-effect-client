import * as Clock from "effect/Clock";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Result from "effect/Result";
import * as Scope from "effect/Scope";
import * as Semaphore from "effect/Semaphore";
import * as Stream from "effect/Stream";
import { ReactorError } from "../errors.js";
import type { Submission } from "../Submission.js";
import type { AudioFrame, VideoFrame, MediaPressure } from "../session/media.js";
import { PolicyFailure, type ClipId } from "./request.js";
import * as Lifecycle from "./renewal-state.js";
import type { EngineError, EngineEvent, MediaSource, Source, SourceCleanup } from "./types.js";

interface Options extends Lifecycle.Lifetime {
  readonly source: Source;
  readonly scope: Scope.Closeable;
  readonly media: MediaSource;
  readonly cleanupBudgetMs: number;
  readonly recordCleanup: (cleanup: SourceCleanup) => void;
  readonly retireSequences: Effect.Effect<void>;
}

interface MediaReaders {
  readonly video: (frame: VideoFrame) => Effect.Effect<void>;
  readonly audio: (frame: AudioFrame) => Effect.Effect<void>;
  readonly lost: (cause: ReactorError) => Effect.Effect<void>;
}

const sumDrops = (retained: bigint | null, latest: bigint | null): bigint | null =>
  retained === null || latest === null ? null : retained + latest;

/**
 * One physical source's local owner. The source closes its remote lease; this
 * owner then joins its registered committed executions before closing their scope.
 * Logical preparation is never registered here, and this module never dispatches it.
 */
export const make = (options: Options) =>
  Effect.gen(function* () {
    const clock = yield* Clock.Clock;
    const closeGate = yield* Semaphore.make(1);
    let phase: Lifecycle.SourcePhase = { _tag: "Active" };
    let indeterminate = false;
    let inFlight = 0;
    let settled = yield* Deferred.make<void>();
    yield* Deferred.succeed(settled, undefined);
    const submissions = new Set<Submission<ClipId, EngineError>>();
    const accepted = new Set<ClipId>();
    const started = new Set<ClipId>();
    let media = options.media;
    let mediaScope: Scope.Closeable | undefined;
    let expectedFrames = 0;
    let receivedFrames = 0;
    let receivedAudioSamples = 0;
    let retiredDrops: { readonly video: bigint | null; readonly audio: bigint | null } = {
      video: 0n,
      audio: 0n,
    };

    const closed = () => Lifecycle.isClosed(phase);
    const recoveryBudget = () =>
      Lifecycle.recoveryBudget(options, clock.currentTimeMillisUnsafe(), options.cleanupBudgetMs);
    const closeMedia = Effect.suspend(() =>
      mediaScope === undefined ? Effect.void : Scope.close(mediaScope, Exit.void),
    );

    const joinCommitted = (budget: number): Effect.Effect<void> =>
      Effect.suspend(() =>
        Effect.all(
          [
            Deferred.await(settled),
            // A result hook can signal accounting before execution returns. Join
            // the original handles as well so closing the scope cannot erase a known result.
            Effect.forEach([...submissions], (submission) => Effect.exit(submission.submit), {
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
              indeterminate = true;
            }),
        }),
      );

    const close = closeGate
      .withPermit(
        Effect.uninterruptible(
          Effect.gen(function* () {
            if (closed()) return;
            phase = Lifecycle.transitionSource(phase, { _tag: "Close" });
            yield* closeMedia;
            options.recordCleanup(yield* options.source.close);
            // Remote retirement must not borrow time from this local accounting budget.
            yield* joinCommitted(options.cleanupBudgetMs);
            yield* options.retireSequences;
            yield* Scope.close(options.scope, Exit.void);
            submissions.clear();
            phase = Lifecycle.transitionSource(phase, { _tag: "Closed" });
          }),
        ),
      )
      .pipe(Effect.withSpan("reactor.orchestration.source.close"));

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
      get accepted(): ReadonlySet<ClipId> {
        return accepted;
      },
      recoveryBudget,
      expired: () => Lifecycle.expired(options, clock.currentTimeMillisUnsafe()),
      needsReplacement: () =>
        Lifecycle.needsReplacement(
          phase,
          indeterminate,
          Lifecycle.expired(options, clock.currentTimeMillisUnsafe()),
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
      ) =>
        Effect.gen(function* () {
          for (const previous of submissions) {
            if ((yield* previous.state)._tag === "Completed") submissions.delete(previous);
          }
          if (submissions.size >= 4096)
            return yield* PolicyFailure.refuse(
              "SubmissionCapacity",
              "Committed source submissions reached their bound",
            );
          yield* admit;
          if (inFlight++ === 0) settled = Deferred.makeUnsafe<void>();
          submissions.add(submission);
        }),
      recordResult: (result: Result.Result<ClipId, EngineError>): void => {
        if (Result.isSuccess(result)) accepted.add(result.success);
        else if (result.failure.context.outcome === "unknown") indeterminate = true;
      },
      finishAccounting: (): void => {
        if (--inFlight === 0) Deferred.doneUnsafe(settled, Effect.void);
      },
      forgetCompleted: (submission: Submission<ClipId, EngineError>): void => {
        submissions.delete(submission);
      },
      observe: (
        receive: (event: EngineEvent) => Effect.Effect<void>,
        failed: (cause: ReactorError) => Effect.Effect<void>,
      ) =>
        options.source.events.pipe(
          Stream.runForEach((event) =>
            Effect.suspend(() => {
              if (closed()) return Effect.void;
              if (event._tag === "Started" && !started.has(event.clipId)) {
                started.add(event.clipId);
                expectedFrames += Math.round(event.durationSeconds * media.videoFramesPerSecond);
              }
              return receive(event);
            }),
          ),
          Effect.catch(failed),
          Effect.forkIn(options.scope),
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
          retiredDrops = {
            video: sumDrops(
              retiredDrops.video,
              Result.isSuccess(previous) ? previous.success.droppedVideo : null,
            ),
            audio: sumDrops(
              retiredDrops.audio,
              Result.isSuccess(previous) ? previous.success.droppedAudio : null,
            ),
          };
          media = next;
          return Effect.void;
        }),
      recordVideo: (): void => {
        receivedFrames++;
      },
      recordAudio: (samples: number): void => {
        receivedAudioSamples += samples;
      },
      pressure: Effect.suspend(() =>
        media.pressure.pipe(
          Effect.flatMap((pressure) => {
            const droppedVideo = sumDrops(retiredDrops.video, pressure.droppedVideo);
            const droppedAudio = sumDrops(retiredDrops.audio, pressure.droppedAudio);
            return droppedVideo === null || droppedAudio === null
              ? Effect.fail(
                  ReactorError.fromCode(
                    "InvalidState",
                    "Retired media generation drop totals are unknown",
                  ),
                )
              : Effect.succeed({ ...pressure, droppedVideo, droppedAudio });
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
            ageSeconds: Math.max(
              0,
              Lifecycle.ageMillis(options, clock.currentTimeMillisUnsafe()) / 1000,
            ),
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
