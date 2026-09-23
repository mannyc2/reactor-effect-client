import * as Cause from "effect/Cause";
import * as Clock from "effect/Clock";
import * as Crypto from "effect/Crypto";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Option from "effect/Option";
import * as Queue from "effect/Queue";
import * as Result from "effect/Result";
import * as Scope from "effect/Scope";
import * as Semaphore from "effect/Semaphore";
import * as Stream from "effect/Stream";
import { ReactorError, errorOf } from "../../errors.js";
import {
  alignFrames,
  alignSecondsTo,
  h3ReferenceTurboRealtime,
  isRequestableSeconds,
} from "../../h3/profile.js";
import { Observations } from "../../observation.js";
import { PolicyFailure, captureRequest } from "../../orchestration/request.js";
import type { Canvas, ClipId } from "../../orchestration/request.js";
import type {
  EngineEvent,
  EngineState,
  LocalClipRecord,
  Source,
  SourceCleanup,
} from "../../orchestration/types.js";
import { CommandFailure } from "../../session/commands.js";
import type { AudioFrame, MediaPressure, VideoFrame } from "../../session/media.js";
import * as Submission from "../../Submission.js";
import type { SimOptions, SimulatedMediaSink } from "../types.js";

interface Clip {
  record: LocalClipRecord;
  popped: boolean;
  disposed: boolean;
  buildStartedAt?: number;
}

/**
 * Production unpaid source. It implements the public orchestration contract
 * directly: no fake schema, wire messages, or private H3 acceptance metadata.
 */
export const source = (
  options: SimOptions = {},
): Effect.Effect<Source, ReactorError, Scope.Scope | Crypto.Crypto> =>
  Effect.gen(function* () {
    const scope = yield* Effect.scope;
    const workers = yield* Scope.fork(scope);
    const clock = yield* Clock.Clock;
    const random = yield* (yield* Crypto.Crypto)
      .randomBytes(16)
      .pipe(Effect.mapError((cause) => errorOf(cause, "InvalidState")));
    const namespace = Array.from(random, (byte) => byte.toString(16).padStart(2, "0")).join("");
    const id = `simulated-${namespace}`;
    const profile = options.profile ?? h3ReferenceTurboRealtime;
    const generationCapacity = options.queueLimit ?? profile.expectedCapacities.generation;
    const playoutCapacity = options.playoutLimit ?? profile.expectedCapacities.playout;
    const buildRatio = options.buildRatio ?? 0.1;
    const buildFixedMs = options.buildFixedMs ?? 0;
    const gap = options.playoutGapMs ?? 0;
    if (
      !Number.isSafeInteger(generationCapacity) ||
      generationCapacity <= 0 ||
      generationCapacity > 1024 ||
      !Number.isSafeInteger(playoutCapacity) ||
      playoutCapacity <= 0 ||
      playoutCapacity > 1024 ||
      !Number.isFinite(buildRatio) ||
      buildRatio < 0 ||
      !Number.isFinite(buildFixedMs) ||
      buildFixedMs < 0 ||
      !Number.isFinite(gap) ||
      gap < 0 ||
      !Number.isFinite(profile.fps) ||
      profile.fps <= 0
    ) {
      return yield* ReactorError.fromCode(
        "InvalidInput",
        "Invalid simulation timing or queue bounds",
      );
    }
    const events = new Observations<EngineEvent>();
    const video = yield* Queue.bounded<VideoFrame, ReactorError | Cause.Done>(4);
    const audio = yield* Queue.bounded<AudioFrame, ReactorError | Cause.Done>(8);
    const buildSignal = yield* Queue.dropping<void>(1);
    const playSignal = yield* Queue.dropping<void>(1);
    const admission = yield* Semaphore.make(1);
    const closeGate = yield* Semaphore.make(1);
    let generation: Clip[] = [];
    let playout: Clip[] = [];
    let building: Clip | undefined;
    let playing: { readonly clip: Clip; readonly startedAt: number } | undefined;
    let stopSignal: Deferred.Deferred<void> | undefined;
    let sequence = 0;
    let prepared = 0n;
    let autoplay = false;
    let canvas: Canvas = "16:9";
    let started = false;
    let closed = false;
    let failed: ReactorError | undefined;
    let report: SourceCleanup | undefined;
    let pending = 0;
    let deliveredVideo = 0n;
    let deliveredAudio = 0n;
    let droppedVideo = 0n;
    let droppedAudio = 0n;
    const retained: ClipId[] = [];
    const failures: ClipId[] = [];
    const active = new Set<Clip>();

    const emit = (event: EngineEvent): void => events.emit(Object.freeze(event), 256);
    const signalBuild = Queue.offer(buildSignal, undefined).pipe(Effect.asVoid);
    const signalPlay = Queue.offer(playSignal, undefined).pipe(Effect.asVoid);
    const validate = (operation: string): Effect.Effect<void, CommandFailure | PolicyFailure> =>
      Effect.suspend((): Effect.Effect<void, CommandFailure | PolicyFailure> => {
        if (closed)
          return Effect.fail(
            PolicyFailure.refuse("SessionClosed", "Simulation is closed", operation),
          );
        if (failed !== undefined)
          return Effect.fail(CommandFailure.from(failed, { operation, outcome: "not-submitted" }));
        return Effect.void;
      });
    const terminate = (cause: ReactorError): Effect.Effect<void> =>
      Effect.sync(() => {
        if (failed !== undefined || closed) return;
        failed = cause;
        events.fail(cause);
        Queue.failCauseUnsafe(video, Cause.fail(cause));
        Queue.failCauseUnsafe(audio, Cause.fail(cause));
      });
    const discard = (clip: Clip): Effect.Effect<void> =>
      Effect.suspend(() => {
        if (clip.disposed) return Effect.void;
        clip.disposed = true;
        active.delete(clip);
        return options.discard?.(clip.record) ?? Effect.void;
      });
    const sink: SimulatedMediaSink = {
      video: (frame) =>
        Effect.suspend(() =>
          closed
            ? Effect.void
            : Queue.offer(video, {
                ...frame,
                data: new Uint8Array(frame.data),
                metadata: new Uint8Array(frame.metadata),
              }).pipe(Effect.asVoid),
        ),
      audio: (frame) =>
        Effect.suspend(() =>
          closed
            ? Effect.void
            : Queue.offer(audio, {
                ...frame,
                samples: new Int16Array(frame.samples),
              }).pipe(Effect.asVoid),
        ),
    };

    const state: Effect.Effect<EngineState> = Effect.sync(() =>
      Object.freeze({
        availability: closed || failed !== undefined ? "Unavailable" : "Ready",
        queued: Object.freeze(
          generation.filter((clip) => !clip.popped && clip !== building).map((clip) => clip.record),
        ),
        generationOrder: Object.freeze(
          generation.filter((clip) => !clip.popped).map((clip) => clip.record.clipId),
        ),
        building:
          building === undefined || building.popped
            ? Option.none()
            : Option.some({
                record: building.record,
                startedAt:
                  options.timing === "unknown"
                    ? Option.none()
                    : Option.fromUndefinedOr(building.buildStartedAt),
              }),
        ready: Object.freeze(playout.filter((clip) => !clip.popped).map((clip) => clip.record)),
        playing:
          playing === undefined
            ? Option.none()
            : Option.some({
                clipId: playing.clip.record.clipId,
                record: Option.some(playing.clip.record),
                startedAt: Option.some(playing.startedAt),
              }),
        continuable: Object.freeze([...retained]),
        failed: Object.freeze([...failures]),
        started,
        canvas: Option.some(canvas),
        capacities: Object.freeze({ generation: generationCapacity, playout: playoutCapacity }),
      }),
    );

    const builder = Effect.gen(function* () {
      while (!closed && failed === undefined) {
        const next = generation.find((clip) => !clip.popped);
        if (next === undefined || playout.length >= playoutCapacity) {
          yield* Queue.take(buildSignal);
          continue;
        }
        building = next;
        next.buildStartedAt = clock.currentTimeMillisUnsafe();
        emit({
          _tag: "Building",
          clipId: next.record.clipId,
          startedAt:
            options.timing === "unknown" ? Option.none() : Option.some(next.buildStartedAt),
        });
        const built = yield* Effect.result(
          options.build === undefined
            ? Effect.sleep(buildFixedMs + buildRatio * next.record.durationSeconds * 1000).pipe(
                Effect.as(next.record.durationSeconds),
              )
            : options.build(next.record),
        );
        const buildMs = Math.max(0, clock.currentTimeMillisUnsafe() - next.buildStartedAt);
        generation = generation.filter((clip) => clip !== next);
        building = undefined;
        if (next.popped || closed) {
          yield* discard(next);
          continue;
        }
        if (
          Result.isFailure(built) ||
          options.faults?.buildFails?.(next.record.seq) === true ||
          !Number.isFinite(built.success) ||
          built.success <= 0
        ) {
          const reason = Result.isFailure(built)
            ? built.failure.message
            : "Simulated build failed or returned an invalid duration";
          failures.push(next.record.clipId);
          if (failures.length > 256) failures.shift();
          emit({ _tag: "Failed", clipId: next.record.clipId, reason });
          yield* discard(next);
          continue;
        }
        // A local renderer may extend a clip beyond the provider grid to fit
        // speech. This is a simulation capability, not a claim about H3 input.
        const seconds =
          Math.ceil(Math.max(profile.requestSeconds.min, built.success) * profile.fps) /
          profile.fps;
        next.record = Object.freeze({
          ...next.record,
          durationSeconds: seconds,
          provider: Object.freeze({
            ...next.record.provider,
            seconds,
            frames: Math.round(seconds * profile.fps),
            ready: true,
          }),
        });
        playout.push(next);
        retained.push(next.record.clipId);
        if (retained.length > profile.continuationWindow) retained.shift();
        emit({
          _tag: "Ready",
          clipId: next.record.clipId,
          durationSeconds: seconds,
          timing:
            options.timing === "unknown" ? { _tag: "Unknown" } : { _tag: "Measured", buildMs },
        });
        yield* signalPlay;
      }
    });

    const player = Effect.gen(function* () {
      while (!closed && failed === undefined) {
        const next = autoplay ? playout.find((clip) => !clip.popped) : undefined;
        if (next === undefined) {
          yield* Queue.take(playSignal);
          continue;
        }
        playout = playout.filter((clip) => clip !== next);
        const at = clock.currentTimeMillisUnsafe();
        playing = { clip: next, startedAt: at };
        started = true;
        const stopped = yield* Deferred.make<void>();
        stopSignal = stopped;
        emit({
          _tag: "Started",
          clipId: next.record.clipId,
          durationSeconds: next.record.durationSeconds,
          at,
        });
        yield* signalBuild;
        const presentation =
          options.present === undefined
            ? Effect.sleep(next.record.durationSeconds * 1000)
            : options.present(next.record, at, sink);
        const result = yield* Effect.result(
          Effect.raceFirst(presentation, Deferred.await(stopped)),
        );
        const wasStopped = yield* Deferred.isDone(stopped);
        stopSignal = undefined;
        playing = undefined;
        next.disposed = true;
        active.delete(next);
        if (Result.isFailure(result)) {
          failures.push(next.record.clipId);
          if (failures.length > 256) failures.shift();
          emit({ _tag: "Failed", clipId: next.record.clipId, reason: result.failure.message });
        } else
          emit({
            _tag: "Ended",
            clipId: next.record.clipId,
            termination: wasStopped ? "stopped" : "finished",
          });
        if (!wasStopped && autoplay && playout.length === 0)
          emit({ _tag: "Starved", at: clock.currentTimeMillisUnsafe() });
        yield* signalBuild;
        if (gap > 0) yield* Effect.sleep(gap);
      }
    });

    yield* builder.pipe(
      Effect.catchCause((cause) =>
        terminate(
          ReactorError.fromCode("InvalidState", "Simulation builder failed", { detail: cause }),
        ),
      ),
      Effect.forkIn(workers),
    );
    yield* player.pipe(
      Effect.catchCause((cause) =>
        terminate(
          ReactorError.fromCode("InvalidState", "Simulation presentation failed", {
            detail: cause,
          }),
        ),
      ),
      Effect.forkIn(workers),
    );

    const prepareRouted: Source["prepareRouted"] = (plan, hooks = {}) =>
      Effect.gen(function* () {
        yield* validate("enqueue");
        const request = yield* captureRequest(plan.request);
        const submissionId = `${namespace}:${++prepared}`;
        return yield* Submission.make({
          id: submissionId,
          prepare: Effect.gen(function* () {
            yield* validate("enqueue");
            if (
              !isRequestableSeconds(profile, request.durationSeconds) ||
              request.prompt.length > profile.prompt.maxChars ||
              request.references.length > profile.references.max
            ) {
              return yield* PolicyFailure.refuse(
                "InvalidRequest",
                "Request exceeds the simulation profile limits",
              );
            }
            if (
              plan.position !== undefined &&
              (!Number.isSafeInteger(plan.position) || plan.position < 0)
            ) {
              return yield* PolicyFailure.refuse(
                "InvalidRequest",
                "Insertion position must be a nonnegative integer",
              );
            }
            yield* Effect.acquireRelease(admission.take(1), () => admission.release(1));
            if (generation.filter((clip) => !clip.popped).length >= generationCapacity) {
              return yield* PolicyFailure.refuse(
                "QueueFull",
                "Simulation generation queue is full",
              );
            }
            if (request.continueFrom !== undefined && !retained.includes(request.continueFrom)) {
              return yield* PolicyFailure.refuse(
                "ContinuationUnavailable",
                "Simulation no longer retains the continuation target",
              );
            }
            return request;
          }),
          commit: () =>
            Effect.gen(function* () {
              yield* validate("enqueue");
              if (hooks.commit !== undefined) yield* hooks.commit(submissionId);
              pending++;
            }),
          execute: (request) =>
            Effect.gen(function* () {
              const result = yield* Effect.result(
                Effect.gen(function* () {
                  const seq = ++sequence;
                  if (options.faults?.sessionFails?.(seq) === true) {
                    // The session is lost after this enqueue was dispatched: the
                    // source fails with the loss, the command with its evidence.
                    const loss = ReactorError.fromCode(
                      "Disconnected",
                      "Simulated loss after enqueue dispatch",
                    );
                    failed = loss;
                    return yield* CommandFailure.from(loss, {
                      operation: "enqueue",
                      outcome: "unknown",
                      requestId: submissionId,
                      generation: 1n,
                    });
                  }
                  const clipId =
                    `${namespace.slice(0, 8)}-${namespace.slice(8, 12)}-4${namespace.slice(13, 16)}-8${namespace.slice(17, 20)}-${seq.toString(16).padStart(12, "0")}` as ClipId;
                  const seconds = alignSecondsTo(profile, request.durationSeconds);
                  const record: LocalClipRecord = Object.freeze({
                    clipId,
                    seq,
                    durationSeconds: seconds,
                    request,
                    enqueuedAt: clock.currentTimeMillisUnsafe(),
                    provider: Object.freeze({
                      clip_id: clipId,
                      prompt: request.prompt,
                      metadata: JSON.stringify(request.metadata),
                      seconds,
                      frames: alignFrames(profile, seconds),
                      seed: request.seed ?? 1,
                      ready: false,
                      has_reference_image: request.references.length > 0,
                      reference_image_count: request.references.length,
                    }),
                  });
                  const clip: Clip = { record, popped: false, disposed: false };
                  active.add(clip);
                  generation.splice(
                    Math.min(plan.position ?? generation.length, generation.length),
                    0,
                    clip,
                  );
                  emit({ _tag: "Queued", clipId, durationSeconds: seconds });
                  return clipId;
                }),
              );
              pending--;
              if (hooks.result !== undefined)
                yield* hooks.result(submissionId, result).pipe(
                  Effect.timeoutOrElse({
                    duration: "1 second",
                    orElse: () =>
                      terminate(
                        ReactorError.fromCode(
                          "InvalidState",
                          "Simulation result accounting timed out",
                        ),
                      ),
                  }),
                  Effect.catchCause((cause) =>
                    terminate(
                      ReactorError.fromCode("InvalidState", "Simulation result accounting failed", {
                        detail: cause,
                      }),
                    ),
                  ),
                );
              if (Result.isSuccess(result)) {
                yield* signalBuild;
                return result.success;
              }
              if (failed !== undefined) {
                events.fail(failed);
                Queue.failCauseUnsafe(video, Cause.fail(failed));
                Queue.failCauseUnsafe(audio, Cause.fail(failed));
              }
              return yield* result.failure;
            }),
        }).pipe(Scope.provide(scope));
      });

    const close = closeGate.withPermit(
      Effect.uninterruptible(
        Effect.gen(function* () {
          if (report !== undefined) return report;
          closed = true;
          const errors: ReactorError[] = [];
          if (stopSignal !== undefined) yield* Deferred.succeed(stopSignal, undefined);
          yield* Scope.close(workers, Exit.void);
          for (const clip of [...active]) {
            const disposed = yield* Effect.exit(discard(clip).pipe(Effect.timeout("5 seconds")));
            if (Exit.isFailure(disposed))
              errors.push(
                ReactorError.fromCode("Shutdown", "Simulation renderer cleanup failed", {
                  detail: disposed.cause,
                }),
              );
          }
          droppedVideo += BigInt(Queue.sizeUnsafe(video));
          droppedAudio += BigInt(Queue.sizeUnsafe(audio));
          Queue.failCauseUnsafe(
            video,
            Cause.fail(ReactorError.fromCode("Closed", "Simulation is closed")),
          );
          Queue.failCauseUnsafe(
            audio,
            Cause.fail(ReactorError.fromCode("Closed", "Simulation is closed")),
          );
          generation = [];
          playout = [];
          building = undefined;
          playing = undefined;
          events.end();
          report = Object.freeze({
            lease: Object.freeze({
              localClosed: errors.length === 0,
              allocation: "none",
              sessionId: id,
              remote: Object.freeze({
                attempted: false,
                responseReceived: false,
                confirmed: false,
                evidence: null,
                deleteStatus: null,
                state: null,
              }),
              unpublishSubmitted: Object.freeze([]),
              unresolvedPublications: Object.freeze([]),
              localErrors: Object.freeze(errors),
            }),
            policy: Object.freeze([]),
          });
          return report;
        }),
      ),
    );
    yield* Effect.addFinalizer(() => close);

    const pressure = Effect.sync((): MediaPressure => ({
      closed,
      queuedControl: 0,
      queuedVideo: Queue.sizeUnsafe(video),
      queuedAudio: Queue.sizeUnsafe(audio),
      queuedBytes: 0,
      droppedVideo,
      droppedAudio,
      pendingRequests: pending,
      deliveredVideo,
      deliveredAudio,
      // One bounded queue with no per-reader subscriptions: no reader is ever failed for lag.
      readerOverflows: 0n,
    }));
    return {
      id,
      state,
      events: events.stream(),
      observe: (options) => events.observeWith(state, options),
      prepareRouted,
      media: Effect.succeed({
        generation: 1n,
        video: Stream.fromQueue(video).pipe(
          Stream.tap(() =>
            Effect.sync(() => {
              deliveredVideo++;
            }),
          ),
        ),
        audio: Stream.fromQueue(audio).pipe(
          Stream.tap(() =>
            Effect.sync(() => {
              deliveredAudio++;
            }),
          ),
        ),
        pressure,
        videoFramesPerSecond: profile.fps,
      }),
      reconnect: Effect.fail(
        ReactorError.fromCode(
          "Disconnected",
          "A failed simulated source requires explicit replacement",
        ),
      ),
      refresh: validate("refresh"),
      setAutoplay: (enabled) =>
        validate("set_autoplay").pipe(
          Effect.andThen(
            Effect.sync(() => {
              autoplay = enabled;
            }),
          ),
          Effect.andThen(signalPlay),
        ),
      stop: validate("stop").pipe(
        Effect.andThen(
          Effect.suspend(() =>
            stopSignal === undefined
              ? Effect.void
              : Deferred.succeed(stopSignal, undefined).pipe(Effect.asVoid),
          ),
        ),
      ),
      remove: (clipId) =>
        validate("pop").pipe(
          Effect.andThen(
            Effect.gen(function* () {
              const clip = [...generation, ...playout].find(
                (value) => !value.popped && value.record.clipId === clipId,
              );
              if (clip === undefined)
                return yield* PolicyFailure.refuse(
                  "NotFound",
                  "Clip is not in a simulated queue",
                  "pop",
                );
              const outcome =
                clip === building ? "in_flight" : playout.includes(clip) ? "ready" : "unstarted";
              clip.popped = true;
              if (clip !== building) generation = generation.filter((value) => value !== clip);
              playout = playout.filter((value) => value !== clip);
              if (clip !== building) yield* discard(clip);
              yield* signalBuild;
              return outcome;
            }),
          ),
        ),
      move: (clipId, position) =>
        validate("move").pipe(
          Effect.andThen(
            Effect.suspend((): Effect.Effect<void, PolicyFailure> => {
              if (!Number.isSafeInteger(position) || position < 0)
                return Effect.fail(
                  PolicyFailure.refuse(
                    "InvalidRequest",
                    "Move position must be a nonnegative integer",
                    "move",
                  ),
                );
              const queue = generation.some(
                (value) => value.record.clipId === clipId && !value.popped,
              )
                ? generation
                : playout;
              const index = queue.findIndex(
                (value) => value.record.clipId === clipId && !value.popped,
              );
              if (index < 0)
                return Effect.fail(
                  PolicyFailure.refuse("NotFound", "Clip is not in a simulated queue", "move"),
                );
              const [clip] = queue.splice(index, 1);
              queue.splice(Math.min(position, queue.length), 0, clip!);
              return Effect.void;
            }),
          ),
        ),
      setCanvas: (aspect) =>
        validate("set_canvas").pipe(
          Effect.andThen(
            Effect.suspend(() => {
              if (generation.length > 0 || playout.length > 0 || playing !== undefined)
                return Effect.fail(
                  PolicyFailure.refuse(
                    "Busy",
                    "Simulation must be idle to change canvas",
                    "set_canvas",
                  ),
                );
              if (!profile.canvases.some((value) => value.aspect === aspect))
                return Effect.fail(
                  PolicyFailure.refuse(
                    "InvalidRequest",
                    "Unsupported simulation canvas",
                    "set_canvas",
                  ),
                );
              canvas = aspect;
              return Effect.void;
            }),
          ),
        ),
      close,
    } satisfies Source;
  });
