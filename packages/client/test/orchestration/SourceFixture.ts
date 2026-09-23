import {
  Cause,
  Crypto,
  Deferred,
  Effect,
  FileSystem,
  Option,
  Path,
  Queue,
  Result,
  Scope,
  Stream,
} from "effect";
import { HttpClient } from "effect/unstable/http";
import { TestClock } from "effect/testing";
import { ReactorError } from "../../src/errors.js";
import { Observations } from "../../src/observation.js";
import { ClipId, ClipRequest, PolicyFailure } from "../../src/orchestration/request.js";
import { emptyState } from "../../src/orchestration/queries.js";
import type {
  ClipRecord,
  EngineEvent,
  EngineState,
  MediaSource,
  RoutedRequest,
  Source,
  SourceCleanup,
} from "../../src/orchestration/types.js";
import { CommandFailure } from "../../src/session/commands.js";
import type { AudioFrame, MediaPressure, VideoFrame } from "../../src/session/media.js";
import * as Submission from "../../src/Submission.js";
import * as TestPlatform from "../Platform.js";
import { fixtureClip } from "../h3/ProviderSession.js";
import { signals } from "./Signals.js";

export type Services =
  | Scope.Scope
  | Crypto.Crypto
  | FileSystem.FileSystem
  | Path.Path
  | HttpClient.HttpClient;
export const run = <A, E>(effect: Effect.Effect<A, E, Services>) =>
  Effect.runPromise(Effect.scoped(effect.pipe(Effect.provide(TestPlatform.layer))));
export const runClock = <A, E>(effect: Effect.Effect<A, E, Services>) =>
  run(effect.pipe(Effect.provide(TestClock.layer())));
export const request = (fields: Partial<ClipRequest> = {}): ClipRequest =>
  new ClipRequest({
    prompt: "A host speaks at a desk.",
    references: [],
    durationSeconds: 7,
    metadata: { fixture: "orchestration" },
    ...fields,
  });
export const member = (
  id: string,
  final = true,
  memberId = final ? "final" : "first",
  fields: Partial<ClipRequest> = {},
) => request({ sequence: { id, final, memberId }, ...fields });
export const record = (id: string, seconds = 1): ClipRecord => ({
  clipId: ClipId.make(id),
  durationSeconds: seconds,
  provider: fixtureClip({ clip_id: id, seconds, frames: Math.round(seconds * 24) }),
});
export const readyState = (fields: Partial<EngineState> = {}): EngineState => ({
  ...emptyState(),
  availability: "Ready",
  ...fields,
});
export const cleanPressure: MediaPressure = {
  closed: false,
  queuedControl: 0,
  queuedVideo: 0,
  queuedAudio: 0,
  queuedBytes: 0,
  droppedVideo: 0n,
  droppedAudio: 0n,
  pendingRequests: 0,
  deliveredVideo: 0n,
  deliveredAudio: 0n,
};
export const videoFrame = (value = 1): VideoFrame => ({
  _tag: "VideoFrame",
  track: "video",
  width: 1,
  height: 1,
  data: new Uint8Array([value, 0, 0, 0]),
  metadata: new Uint8Array(),
  frameId: 0n,
  timestampMicros: 0n,
});
export const audioFrame = (length = 480, value = 1): AudioFrame => ({
  _tag: "AudioFrame",
  track: "audio",
  sampleRate: 48000,
  channels: 1,
  samples: new Int16Array(length).fill(value),
});
export const gate = Effect.gen(function* () {
  const signal = yield* Deferred.make<void>();
  return {
    wait: Deferred.await(signal),
    release: Deferred.succeed(signal, undefined).pipe(Effect.asVoid),
  };
});

/** Bounded real scheduler turns also work while TestClock is deliberately stationary. */
export const until = (
  predicate: () => boolean,
  advance: Effect.Effect<void> = Effect.void,
  label = "fixture did not settle",
) =>
  Effect.gen(function* () {
    for (let count = 0; !predicate(); count++) {
      if (count >= 1000) return yield* Effect.die(new Error(label));
      yield* advance;
      yield* Effect.promise(() => new Promise<void>((resolve) => setTimeout(resolve, 1)));
    }
  });
export const untilEffect = <E, R>(
  predicate: Effect.Effect<boolean, E, R>,
  label = "effect did not settle",
) =>
  Effect.gen(function* () {
    for (let count = 0; !(yield* predicate); count++) {
      if (count >= 1000) return yield* Effect.die(new Error(label));
      yield* Effect.promise(() => new Promise<void>((resolve) => setTimeout(resolve, 1)));
    }
  });
/**
 * A local refusal's reason as one comparable label: its tag, with a Missing
 * reason's purpose or a Sequence reason's code; undefined for any other value.
 */
export const refusal = (failure: unknown): string | undefined => {
  if (!PolicyFailure.is(failure)) return undefined;
  const reason = failure.reason;
  return reason._tag === "Missing"
    ? `Missing:${reason.purpose}`
    : reason._tag === "Sequence"
      ? `Sequence:${reason.code}`
      : reason._tag;
};
export const failure = (
  outcome: "unknown" | "replied" | "not-submitted",
  message = "fixture outcome",
  operation = "enqueue",
) =>
  CommandFailure.from(
    ReactorError.fromCode("Remote", message),
    outcome === "not-submitted"
      ? { operation, outcome }
      : { operation, outcome, requestId: "fixture-dispatch", generation: 1n },
  );

export interface FixtureGeneration {
  readonly generation: bigint;
  readonly video: Queue.Queue<VideoFrame, ReactorError | Cause.Done>;
  readonly audio: Queue.Queue<AudioFrame, ReactorError | Cause.Done>;
}
export interface SourceScript {
  readonly initial?: EngineState;
  readonly prework?: (plan: RoutedRequest) => Effect.Effect<void, CommandFailure, Scope.Scope>;
  readonly execute?: (
    plan: RoutedRequest,
    accept: Effect.Effect<ClipId>,
  ) => Effect.Effect<ClipId, CommandFailure>;
  /** Hold local result bookkeeping after the physical outcome is already known. */
  readonly result?: (
    plan: RoutedRequest,
    result: Result.Result<ClipId, CommandFailure>,
  ) => Effect.Effect<void>;
  readonly reconnect?: Effect.Effect<void, ReactorError>;
  readonly autoplay?: (enabled: boolean) => Effect.Effect<void, CommandFailure>;
  readonly close?: Effect.Effect<void>;
  readonly framesPerSecond?: number;
  readonly pressure?: (generation: bigint) => Effect.Effect<MediaPressure, ReactorError>;
}

type LifecycleEvent =
  | { readonly _tag: "Dispatched"; readonly index: number }
  | { readonly _tag: "ResultKnown"; readonly index: number }
  | { readonly _tag: "Accounted"; readonly index: number }
  | { readonly _tag: "Reconnecting"; readonly attempt: number }
  | { readonly _tag: "Closed" }
  | { readonly _tag: "Finalized" };

/** A physical Source fixture, with no router, renewal policy or H3 reducer. */
export const sourceFixture = (id: string, script: SourceScript = {}) =>
  Effect.gen(function* () {
    const scope = yield* Effect.scope;
    const events = new Observations<EngineEvent>();
    const closedSignal = yield* Deferred.make<void>();
    const lifecycle = signals<LifecycleEvent>();
    const generations: FixtureGeneration[] = [];
    let state = script.initial ?? readyState();
    let pressure = { ...cleanPressure };
    let closed = false,
      finalized = false,
      closes = 0,
      reconnects = 0,
      results = 0,
      sequence = 0;
    const plans: RoutedRequest[] = [],
      sends: RoutedRequest[] = [];
    const controls: { command: string; value?: unknown }[] = [];
    const accepted: ClipRecord[] = [];
    const cleanup: SourceCleanup = Object.freeze({
      lease: Object.freeze({
        localClosed: true,
        allocation: "known",
        ownership: "attached",
        sessionId: id,
        remote: {
          attempted: false,
          responseReceived: false,
          confirmed: false,
          evidence: null,
          deleteStatus: null,
          state: null,
        },
        unpublishSubmitted: [],
        unresolvedPublications: [],
        localErrors: [],
      }),
      policy: Object.freeze([]),
    });
    const nextGeneration = Effect.gen(function* () {
      const entry: FixtureGeneration = {
        generation: BigInt(generations.length + 1),
        video: yield* Queue.unbounded<VideoFrame, ReactorError | Cause.Done>(),
        audio: yield* Queue.unbounded<AudioFrame, ReactorError | Cause.Done>(),
      };
      generations.push(entry);
      return entry;
    });
    let active = yield* nextGeneration;
    const media: Effect.Effect<MediaSource> = Effect.sync(() => ({
      generation: active.generation,
      video: Stream.fromQueue(active.video),
      audio: Stream.fromQueue(active.audio),
      pressure: script.pressure?.(active.generation) ?? Effect.sync(() => ({ ...pressure })),
      videoFramesPerSecond: script.framesPerSecond ?? 24,
    }));
    const source: Source = {
      id,
      state: Effect.sync(() => state),
      events: events.stream(),
      media,
      prepareRouted: (plan, hooks = {}) =>
        Effect.gen(function* () {
          plans.push(plan);
          const submissionId = `${id}/submission/${++sequence}`;
          return yield* Submission.make({
            id: submissionId,
            prepare: script.prework?.(plan) ?? Effect.void,
            commit: () => hooks.commit?.(submissionId) ?? Effect.void,
            execute: () =>
              Effect.gen(function* () {
                sends.push(plan);
                const dispatchIndex = sends.length;
                lifecycle.record({ _tag: "Dispatched", index: dispatchIndex });
                const accept = Effect.sync(() => {
                  const clip = {
                    ...record(`${id}/clip/${dispatchIndex}`, plan.request.durationSeconds),
                    request: plan.request,
                    seq: dispatchIndex,
                    enqueuedAt: 0,
                  };
                  accepted.push(clip);
                  const ordered = [...state.queued];
                  ordered.splice(plan.position ?? ordered.length, 0, clip);
                  state = {
                    ...state,
                    queued: ordered,
                    generationOrder: ordered.map((entry) => entry.clipId),
                  };
                  return clip.clipId;
                });
                const result = yield* Effect.result(
                  (script.execute?.(plan, accept) ?? accept).pipe(
                    Effect.raceFirst(
                      Deferred.await(closedSignal).pipe(
                        Effect.andThen(
                          Effect.fail(failure("unknown", "source closed after commit")),
                        ),
                      ),
                    ),
                  ),
                );
                results++;
                lifecycle.record({ _tag: "ResultKnown", index: dispatchIndex });
                if (script.result !== undefined) yield* script.result(plan, result);
                if (hooks.result !== undefined) yield* hooks.result(submissionId, result);
                lifecycle.record({ _tag: "Accounted", index: dispatchIndex });
                return yield* Result.isSuccess(result)
                  ? Effect.succeed(result.success)
                  : Effect.fail(result.failure);
              }),
          }).pipe(Scope.provide(scope));
        }),
      reconnect: Effect.gen(function* () {
        reconnects++;
        lifecycle.record({ _tag: "Reconnecting", attempt: reconnects });
        yield* script.reconnect ?? Effect.void;
        active = yield* nextGeneration;
      }),
      refresh: Effect.void,
      setAutoplay: (enabled) =>
        Effect.sync(() => {
          controls.push({ command: "autoplay", value: enabled });
        }).pipe(Effect.andThen(script.autoplay?.(enabled) ?? Effect.void)),
      stop: Effect.sync(() => {
        controls.push({ command: "stop" });
      }),
      remove: (clipId) =>
        Effect.sync(() => {
          controls.push({ command: "remove", value: clipId });
          const wasReady = state.ready.some((clip) => clip.clipId === clipId);
          state = {
            ...state,
            queued: state.queued.filter((clip) => clip.clipId !== clipId),
            generationOrder: state.generationOrder.filter((id) => id !== clipId),
            ready: state.ready.filter((clip) => clip.clipId !== clipId),
          };
          return wasReady ? ("ready" as const) : ("generation" as const);
        }),
      move: (clipId, position) =>
        Effect.sync(() => {
          controls.push({ command: "move", value: { clipId, position } });
        }),
      setCanvas: (canvas) =>
        Effect.sync(() => {
          controls.push({ command: "canvas", value: canvas });
          state = { ...state, canvas: Option.some(canvas) };
        }),
      close: Effect.gen(function* () {
        if (!closed) {
          closed = true;
          closes++;
          yield* Deferred.succeed(closedSignal, undefined);
          lifecycle.record({ _tag: "Closed" });
          yield* script.close ?? Effect.void;
          events.end();
        }
        return cleanup;
      }),
    };
    yield* Effect.addFinalizer(() =>
      Effect.sync(() => {
        finalized = true;
        lifecycle.record({ _tag: "Finalized" });
      }),
    );
    return {
      source,
      lifecycle,
      plans,
      sends,
      controls,
      accepted,
      generations,
      cleanup,
      setState: (value: EngineState) =>
        Effect.sync(() => {
          state = value;
        }),
      update: (fields: Partial<EngineState>) =>
        Effect.sync(() => {
          state = { ...state, ...fields };
        }),
      setPressure: (fields: Partial<MediaPressure>) =>
        Effect.sync(() => {
          pressure = { ...pressure, ...fields };
        }),
      emit: (event: EngineEvent) =>
        Effect.sync(() => {
          events.emit(event, 256);
        }),
      failEvents: (error: ReactorError) =>
        Effect.sync(() => {
          events.fail(error);
        }),
      video: (frame: VideoFrame) => Queue.offer(active.video, frame).pipe(Effect.asVoid),
      audio: (frame: AudioFrame) => Queue.offer(active.audio, frame).pipe(Effect.asVoid),
      failVideo: (error: ReactorError) => Queue.fail(active.video, error).pipe(Effect.asVoid),
      endVideo: Effect.suspend(() => Queue.end(active.video).pipe(Effect.asVoid)),
      status: () => ({ closed, finalized, closes, reconnects, results }),
    };
  });
export type SourceFixture = Effect.Success<ReturnType<typeof sourceFixture>>;
