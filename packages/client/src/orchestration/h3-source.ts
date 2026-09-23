import * as Clock from "effect/Clock";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import type * as FileSystem from "effect/FileSystem";
import * as Option from "effect/Option";
import type * as Path from "effect/Path";
import * as Result from "effect/Result";
import * as Scope from "effect/Scope";
import * as Semaphore from "effect/Semaphore";
import * as Stream from "effect/Stream";
import type * as Http from "effect/unstable/http/HttpClient";
import { ReactorError } from "../errors.js";
import type { Clip as ProviderClip } from "../h3/messages.js";
import { h3ReferenceTurboRealtime } from "../h3/profile.js";
import type {
  Provider,
  ProviderEvent,
  ProviderSnapshot,
  Request as ProviderRequest,
} from "../h3/types.js";
import { Observations } from "../observation.js";
import type { Session } from "../session/index.js";
import type { MediaGeneration } from "../session/media.js";
import * as Submission from "../Submission.js";
import { PolicyFailure, captureRequest, preworkFailure } from "./request.js";
import type { Canvas, ClipId, ClipRequest } from "./request.js";
import { emptyState, isIdle } from "./queries.js";
import { loadReferenceBytes } from "./references.js";
import type { LoadLimits } from "./references.js";
import type {
  ClipRecord,
  EngineEvent,
  EngineState,
  LocalClipRecord,
  MediaSource,
  PolicyCleanup,
  Source,
  SourceCleanup,
} from "./types.js";

interface Annotation {
  readonly request: ClipRequest;
  readonly seq: number;
  readonly enqueuedAt: number;
}
interface Times {
  readonly generatedAt?: number;
  readonly startedAt?: number;
}

export interface H3SourceOptions {
  readonly media: Effect.Effect<MediaGeneration, ReactorError>;
  /** Explicit broadcast policy; absent settings leave the provider unchanged. */
  readonly canvas?: Canvas;
  readonly holdLastFrame?: boolean;
  readonly resetOnClose?: boolean;
  readonly references?: Partial<LoadLimits>;
  readonly maxAnnotations?: number;
}

const record = (clip: ProviderClip, annotations: ReadonlyMap<string, Annotation>): ClipRecord => {
  const annotation = annotations.get(clip.clip_id);
  return Object.freeze({
    clipId: clip.clip_id as ClipId,
    durationSeconds: clip.seconds,
    provider: clip,
    ...(annotation === undefined ? {} : annotation),
  });
};

/** This projection owns no provider queue or playback state. */
const project = (
  snapshot: ProviderSnapshot,
  annotations: ReadonlyMap<string, Annotation>,
  times: ReadonlyMap<string, Times>,
): EngineState => {
  const facts = snapshot._tag === "Ready" ? snapshot : snapshot.lastFacts;
  const known = new Map(snapshot.clips.map((entry) => [entry.clip.clip_id, entry.clip]));
  const playingId = facts?.state.playing_clip_id ?? null;
  const playingClip = playingId === null ? undefined : known.get(playingId);
  const playingTime = playingId === null ? undefined : times.get(playingId)?.startedAt;
  const aspect = facts?.state.aspect;
  const canvas: Option.Option<Canvas> =
    aspect === "16:9" || aspect === "1:1" || aspect === "9:16" || aspect === "4:3"
      ? Option.some<Canvas>(aspect)
      : Option.none();
  const continuable = snapshot.clips
    .filter((entry) => entry.clip.ready && entry.lifecycle !== "clip_failed")
    .sort((left, right) =>
      left.source.sequence < right.source.sequence
        ? -1
        : left.source.sequence > right.source.sequence
          ? 1
          : 0,
    )
    .slice(-h3ReferenceTurboRealtime.continuationWindow)
    .map((entry) => entry.clip.clip_id as ClipId);
  return Object.freeze({
    ...emptyState(),
    availability: snapshot._tag,
    queued: Object.freeze((facts?.queue.generation ?? []).map((clip) => record(clip, annotations))),
    generationOrder: Object.freeze(
      (facts?.queue.generation ?? []).map((clip) => clip.clip_id as ClipId),
    ),
    // H3 does not report a build start. Queue-head estimates remain estimates in
    // an application's horizon calculation, never observed Building records.
    building: Option.none(),
    ready: Object.freeze((facts?.queue.playout ?? []).map((clip) => record(clip, annotations))),
    playing:
      playingId === null
        ? Option.none()
        : Option.some({
            clipId: playingId as ClipId,
            record:
              playingClip === undefined
                ? Option.none()
                : Option.some(record(playingClip, annotations)),
            startedAt: playingTime === undefined ? Option.none() : Option.some(playingTime),
          }),
    continuable: Object.freeze(continuable),
    failed: Object.freeze(
      snapshot.clips
        .filter((entry) => entry.lifecycle === "clip_failed")
        .map((entry) => entry.clip.clip_id as ClipId),
    ),
    started: (facts?.state.clips_played ?? 0) > 0 || facts?.state.playing === true,
    canvas,
    capacities:
      facts === null
        ? emptyState().capacities
        : {
            generation: facts.state.generation_capacity,
            playout: facts.state.playout_capacity,
          },
  });
};

/**
 * Bind explicit broadcast policy and host reference loading to an H3 view.
 * Acquisition, provider facts, dispatch evidence, and media remain with their
 * existing owners; this source only annotates and projects them.
 */
export const fromH3 = (
  session: Session,
  provider: Provider,
  options: H3SourceOptions,
): Effect.Effect<
  Source,
  ReactorError,
  Scope.Scope | FileSystem.FileSystem | Path.Path | Http.HttpClient
> =>
  Effect.gen(function* () {
    if (session.id !== provider.sessionId)
      return yield* Effect.fail(
        new ReactorError({
          code: "InvalidInput",
          message: "H3 provider and session identities differ",
        }),
      );
    const environment = yield* Effect.context<
      FileSystem.FileSystem | Path.Path | Http.HttpClient
    >();
    const clock = yield* Clock.Clock;
    const annotations = new Map<string, Annotation>();
    const times = new Map<string, Times>();
    const observations = new Observations<EngineEvent>();
    const limit = options.maxAnnotations ?? 2048;
    if (!Number.isSafeInteger(limit) || limit < 1 || limit > 16384)
      return yield* Effect.fail(
        new ReactorError({ code: "InvalidInput", message: "Invalid annotation bound" }),
      );
    let reservations = 0;
    let sequence = 0;
    let closed = false;
    let report: SourceCleanup | undefined;
    let starvationArmed = false;
    const closeGate = yield* Semaphore.make(1);
    const state = provider.current.pipe(
      Effect.map((snapshot) => project(snapshot, annotations, times)),
    );
    const emit = (event: EngineEvent): void => observations.emit(event, 256);

    const receive = (event: ProviderEvent): Effect.Effect<void> =>
      Effect.gen(function* () {
        if (closed) return;
        if (event._tag === "Diagnostic") {
          observations.fail(event.error);
          return;
        }
        if (
          event._tag !== "Message" ||
          event.disposition !== "applied" ||
          event.message.type === "unknown"
        )
          return;
        const message = event.message;
        const at = clock.currentTimeMillisUnsafe();
        if ("clip" in message.data) {
          const clip = message.data.clip;
          const clipId = clip.clip_id as ClipId;
          const previous = times.get(clipId) ?? {};
          if (!times.has(clipId) && times.size >= limit * 2) {
            observations.fail(
              new ReactorError({
                code: "Overflow",
                message: "Orchestration timing observation bound exceeded",
              }),
            );
            return;
          }
          switch (message.type) {
            case "clip_queued":
              emit({ _tag: "Queued", clipId, durationSeconds: clip.seconds });
              break;
            case "clip_generated": {
              times.set(clipId, { ...previous, generatedAt: previous.generatedAt ?? at });
              const annotation = annotations.get(clipId);
              emit({
                _tag: "Ready",
                clipId,
                durationSeconds: clip.seconds,
                timing:
                  annotation === undefined
                    ? { _tag: "Unknown" }
                    : {
                        _tag: "Bounded",
                        admissionToReadyMs: Math.max(0, at - annotation.enqueuedAt),
                      },
              });
              break;
            }
            case "clip_started":
              times.set(clipId, { ...previous, startedAt: previous.startedAt ?? at });
              starvationArmed = false;
              emit({ _tag: "Started", clipId, durationSeconds: clip.seconds, at });
              break;
            case "clip_finished":
            case "clip_stopped":
              starvationArmed = message.type === "clip_finished";
              emit({
                _tag: "Ended",
                clipId,
                termination: message.type === "clip_finished" ? "finished" : "stopped",
              });
              break;
            case "clip_failed":
              emit({ _tag: "Failed", clipId, reason: message.data.reason });
              break;
          }
        }
        if (
          starvationArmed &&
          (message.type === "state_update" || message.type === "queue_update")
        ) {
          const snapshot = yield* provider.current;
          if (
            snapshot._tag === "Ready" &&
            snapshot.state.autoplay &&
            !snapshot.state.playing &&
            snapshot.queue.playout.length === 0
          ) {
            starvationArmed = false;
            emit({ _tag: "Starved", at });
          }
        }
      });
    const observation = yield* provider.observe();
    yield* observation.events.pipe(
      Stream.runForEach(receive),
      Effect.catch((error) => Effect.sync(() => observations.fail(error))),
      Effect.forkScoped,
    );

    const requireReady = (operation: string) =>
      Effect.gen(function* () {
        if (closed)
          return yield* Effect.fail(
            PolicyFailure.refuse("session_closed", "Source is closed", operation),
          );
        let snapshot = yield* provider.current;
        if (snapshot._tag === "Synchronizing") {
          yield* provider.refresh;
          snapshot = yield* provider.current;
        }
        if (snapshot._tag !== "Ready")
          return yield* Effect.fail(
            PolicyFailure.refuse("session_recovering", "Provider state is unavailable", operation),
          );
        return snapshot;
      });

    const prepareRouted: Source["prepareRouted"] = (plan, hooks = {}) =>
      Effect.gen(function* () {
        const request = yield* captureRequest(plan.request);
        let annotation: Annotation | undefined;
        const input: Effect.Effect<ProviderRequest, ReactorError, Scope.Scope> = Effect.gen(
          function* () {
            yield* requireReady("enqueue");
            const references = yield* Effect.forEach(request.references, ({ uri }) =>
              loadReferenceBytes(uri, {
                maxBytes:
                  options.references?.maxBytes ?? h3ReferenceTurboRealtime.references.maxBytes,
                timeoutMs: options.references?.timeoutMs ?? 5000,
              }).pipe(
                Effect.provideContext(environment),
                Effect.map((bytes) => ({ _tag: "Bytes" as const, bytes })),
              ),
            );
            return {
              prompt: request.prompt,
              references,
              seconds: request.durationSeconds,
              metadata: JSON.stringify(request.metadata),
              ...(request.seed === undefined ? {} : { seed: request.seed }),
              ...(request.continueFrom === undefined ? {} : { continueFrom: request.continueFrom }),
              ...(plan.position === undefined ? {} : { position: plan.position }),
            };
          },
        );
        const prepared = yield* provider
          .prepareFrom(input, {
            commit: (id) =>
              Effect.gen(function* () {
                if (annotations.size + reservations >= limit)
                  return yield* Effect.fail(
                    PolicyFailure.refuse(
                      "annotation_capacity",
                      "Local submission annotations are full",
                    ),
                  );
                if (sequence >= Number.MAX_SAFE_INTEGER)
                  return yield* Effect.fail(
                    PolicyFailure.refuse(
                      "identity_exhausted",
                      "Local submission sequence is exhausted",
                    ),
                  );
                reservations++;
                yield* (hooks.commit?.(id) ?? Effect.void).pipe(
                  Effect.onExit((exit) =>
                    Exit.isFailure(exit)
                      ? Effect.sync(() => {
                          reservations--;
                        })
                      : Effect.void,
                  ),
                );
                annotation = Object.freeze({
                  request,
                  seq: ++sequence,
                  enqueuedAt: clock.currentTimeMillisUnsafe(),
                });
              }),
            result: (id, result) =>
              Effect.gen(function* () {
                if (annotation === undefined)
                  return yield* Effect.die(
                    new Error("H3 result has no committed application annotation"),
                  );
                reservations--;
                if (Result.isSuccess(result))
                  annotations.set(result.success.clip.clip_id, annotation);
                if (hooks.result !== undefined)
                  yield* hooks.result(
                    id,
                    Result.isSuccess(result)
                      ? Result.succeed(result.success.clip.clip_id as ClipId)
                      : Result.fail(result.failure),
                  );
              }),
          })
          .pipe(Effect.mapError((cause) => preworkFailure("enqueue", cause)));
        return Submission.map(prepared, (acceptance) => acceptance.clip.clip_id as ClipId);
      });

    const close = closeGate.withPermit(
      Effect.uninterruptible(
        Effect.gen(function* () {
          if (report !== undefined) return report;
          const policy: PolicyCleanup[] = [];
          if (session.ownership === "owned" && options.resetOnClose === true) {
            const result = yield* Effect.result(provider.reset.pipe(Effect.asVoid));
            policy.push(Object.freeze({ operation: "reset", result }));
          }
          closed = true;
          const lease = yield* session.close;
          observations.end();
          report = Object.freeze({ lease, policy: Object.freeze(policy) });
          return report;
        }),
      ),
    );
    yield* Effect.addFinalizer(() => close);

    if (options.canvas !== undefined) {
      if (!isIdle(yield* state))
        return yield* Effect.fail(
          PolicyFailure.refuse(
            "busy",
            "Canvas can only change while the provider is idle",
            "set_canvas",
          ),
        );
      yield* provider.setCanvas(options.canvas);
      yield* provider.refresh;
    }
    if (options.holdLastFrame !== undefined) {
      yield* provider.setFlushOnClipEnd(!options.holdLastFrame);
      yield* provider.refresh;
    }

    const media = options.media.pipe(
      Effect.map((generation): MediaSource => ({
        generation: generation.generation,
        video: generation.video(h3ReferenceTurboRealtime.tracks.video),
        audio: generation.audio(h3ReferenceTurboRealtime.tracks.audio),
        pressure: generation.snapshot,
        videoFramesPerSecond: h3ReferenceTurboRealtime.fps,
      })),
    );
    return {
      id: session.id,
      state,
      events: observations.stream(),
      prepareRouted,
      media,
      reconnect: session.reconnect.pipe(Effect.andThen(provider.refresh)),
      refresh: provider.refresh,
      setAutoplay: (enabled) => provider.setAutoplay(enabled).pipe(Effect.asVoid),
      stop: provider.stop.pipe(Effect.asVoid),
      remove: (id) =>
        Effect.gen(function* () {
          const snapshot = yield* requireReady("pop");
          const queued = snapshot.queue.generation.some((clip) => clip.clip_id === id);
          const ready = snapshot.queue.playout.some((clip) => clip.clip_id === id);
          if (!queued && !ready)
            return yield* Effect.fail(
              PolicyFailure.refuse("not_found", "Clip is not present in a provider queue", "pop"),
            );
          yield* provider.pop(id);
          return ready ? "ready" : "generation";
        }),
      move: (id, position) => provider.move(id, position).pipe(Effect.asVoid),
      setCanvas: (canvas) =>
        Effect.gen(function* () {
          yield* requireReady("set_canvas");
          if (!isIdle(yield* state))
            return yield* Effect.fail(
              PolicyFailure.refuse(
                "busy",
                "Canvas can only change while the provider is idle",
                "set_canvas",
              ),
            );
          yield* provider.setCanvas(canvas);
        }),
      close,
    } satisfies Source;
  });

/** A consumer can narrow its optional annotation without assuming ownership. */
export const isLocalClip = (value: ClipRecord): value is LocalClipRecord =>
  value.request !== undefined && value.seq !== undefined && value.enqueuedAt !== undefined;
