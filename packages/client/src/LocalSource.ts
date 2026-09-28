/**
 * A playout `Source` rendered in this process instead of by Reactor: one build
 * slot and a playout queue with H3's autoplay semantics, driven by the
 * application's renderer hooks. It never expires and costs nothing, so it
 * serves locally rendered material (speech, stills) and demos.
 */
import * as Deferred from "effect/Deferred";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as PubSub from "effect/PubSub";
import * as Ref from "effect/Ref";
import type * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import * as SubscriptionRef from "effect/SubscriptionRef";
import type { Request } from "./H3.js";
import { requestSeconds } from "./internal/h3/profile.js";
import type { AudioFrame, VideoFrame } from "./Media.js";
import type { ClipTag, Source, SourceClip, SourceEvent, SourceState } from "./Playout.js";
import { noAcquisition } from "./Reactor.js";
import { CommandFailure, ReactorError } from "./ReactorError.js";

export interface LocalClip {
  readonly clipId: string;
  readonly request: Request;
  readonly tag: ClipTag;
  /** The requested length, in seconds. */
  readonly seconds: number;
}

/** Where a presentation sends its media; the source's `video` and `audio` streams read it. */
export interface Sink {
  readonly video: (frame: VideoFrame) => Effect.Effect<void>;
  readonly audio: (frame: AudioFrame) => Effect.Effect<void>;
}

export interface Options {
  /** Renders a clip before it is Ready; its result, when given, is the clip's actual length in seconds. */
  readonly build?: ((clip: LocalClip) => Effect.Effect<number | void, ReactorError>) | undefined;
  /** Plays a clip; the clip ends when it completes. Without it a clip plays for its length. */
  readonly present?:
    | ((clip: LocalClip, sink: Sink) => Effect.Effect<void, ReactorError>)
    | undefined;
  /** Releases a rendered clip that will not play. */
  readonly discard?: ((clip: LocalClip) => Effect.Effect<void>) | undefined;
  /** Build time per second of clip when `build` is absent; zero by default. */
  readonly buildRatio?: number | undefined;
}

interface Queued extends LocalClip {
  readonly length: number;
}

interface Local {
  readonly building: ReadonlyArray<Queued>;
  readonly ready: ReadonlyArray<Queued>;
  readonly playing: Queued | undefined;
  readonly autoplay: boolean;
  /** The build in flight was popped: it finishes, and its result is discarded. */
  readonly popped: ReadonlySet<string>;
}

const clip = (value: Queued): SourceClip => ({
  clipId: value.clipId,
  tag: value.tag,
  seconds: value.length,
});
const view = (local: Local): SourceState => ({
  available: true,
  building: local.building.map(clip),
  ready: local.ready.map(clip),
  playing: local.playing === undefined ? undefined : clip(local.playing),
  continuable: [],
});
const refused = (operation: string, message: string) =>
  CommandFailure.from(ReactorError.fromCode("InvalidState", message), {
    operation,
    outcome: "not-submitted",
  });

/** A local source in the caller's scope; closing the scope stops its fibers. */
export const open = Effect.fnUntraced(function* (
  options: Options = {},
): Effect.fn.Return<Source, never, Scope.Scope> {
  const state = yield* SubscriptionRef.make<Local>({
    building: [],
    ready: [],
    playing: undefined,
    autoplay: false,
    popped: new Set(),
  });
  const events = yield* PubSub.unbounded<SourceEvent>();
  const video = yield* PubSub.sliding<VideoFrame>(256);
  const audio = yield* PubSub.sliding<AudioFrame>(512);
  const counter = yield* Ref.make(0);
  const stop = yield* Ref.make<Deferred.Deferred<void> | undefined>(undefined);
  const sink: Sink = {
    video: (frame) => Effect.asVoid(PubSub.publish(video, frame)),
    audio: (frame) => Effect.asVoid(PubSub.publish(audio, frame)),
  };
  const publish = (event: SourceEvent) => Effect.asVoid(PubSub.publish(events, event));
  const change = (f: (local: Local) => Local) =>
    Effect.flatMap(SubscriptionRef.updateAndGet(state, f), (local) =>
      publish({ _tag: "State", state: view(local) }),
    );
  const when = (predicate: (local: Local) => boolean) =>
    SubscriptionRef.changes(state).pipe(
      Stream.filter(predicate),
      Stream.runHead,
      Effect.map(Option.getOrThrow),
    );

  // One build slot: the head of the building queue builds, then waits Ready unless it was popped.
  yield* Effect.forever(
    Effect.gen(function* () {
      const local = yield* when((value) => value.building.length > 0);
      const next = local.building[0]!;
      const built = yield* (
        options.build === undefined
          ? Effect.as(
              Effect.sleep(Duration.seconds(next.seconds * (options.buildRatio ?? 0))),
              undefined,
            )
          : options.build(next)
      ).pipe(Effect.exit);
      const length =
        built._tag === "Success" && typeof built.value === "number" ? built.value : next.seconds;
      const popped = (yield* SubscriptionRef.get(state)).popped.has(next.clipId);
      // The failure comes before the queue that no longer lists the clip, as H3 sends them.
      if (built._tag === "Failure")
        yield* publish({ _tag: "Failed", clip: clip(next), reason: "the local build failed" });
      yield* change((value) => ({
        ...value,
        building: value.building.filter((other) => other.clipId !== next.clipId),
        ready:
          popped || built._tag === "Failure" ? value.ready : [...value.ready, { ...next, length }],
        popped: new Set([...value.popped].filter((id) => id !== next.clipId)),
      }));
      if (built._tag === "Success" && popped) yield* options.discard?.(next) ?? Effect.void;
    }),
  ).pipe(Effect.forkScoped);

  // Autoplay: the head of the playout queue plays once nothing else does.
  yield* Effect.forever(
    Effect.gen(function* () {
      const local = yield* when(
        (value) => value.autoplay && value.playing === undefined && value.ready.length > 0,
      );
      const next = local.ready[0]!;
      const stopped = yield* Deferred.make<void>();
      yield* Ref.set(stop, stopped);
      yield* change((value) => ({ ...value, ready: value.ready.slice(1), playing: next }));
      yield* publish({ _tag: "Started", clip: clip(next) });
      // A presentation that fails did not air in full: it ends as stopped, and the failure is logged.
      const presentation =
        options.present === undefined
          ? Effect.as(Effect.sleep(Duration.seconds(next.length)), true)
          : options.present(next, sink).pipe(
              Effect.as(true),
              Effect.catchCause((cause) =>
                Effect.as(Effect.logWarning("local presentation failed", cause), false),
              ),
            );
      const finished = yield* Effect.raceFirst(
        presentation,
        Effect.as(Deferred.await(stopped), false),
      );
      yield* Ref.set(stop, undefined);
      yield* change((value) => ({ ...value, playing: undefined }));
      yield* publish({
        _tag: "Ended",
        clip: clip(next),
        termination: finished ? "finished" : "stopped",
      });
    }),
  ).pipe(Effect.forkScoped);

  const source: Source = {
    sessionId: `local-${String(yield* Ref.get(counter))}`,
    lifetime: Duration.infinity,
    events: events.pipe(
      PubSub.subscribe,
      Effect.map((subscription) =>
        Stream.concat(
          Stream.fromEffect(
            state.pipe(
              SubscriptionRef.get,
              Effect.map((local): SourceEvent => ({ _tag: "State", state: view(local) })),
            ),
          ),
          Stream.fromSubscription(subscription),
        ),
      ),
      Stream.unwrap,
    ),
    enqueue: (request, tag, _continueFrom) =>
      Effect.gen(function* () {
        const index = yield* Ref.updateAndGet(counter, (value) => value + 1);
        const seconds = request.seconds ?? requestSeconds.min;
        const queued: Queued = {
          clipId: `local-clip-${String(index)}`,
          request,
          tag,
          seconds,
          length: seconds,
        };
        yield* change((value) => ({ ...value, building: [...value.building, queued] }));
        return queued.clipId;
      }),
    remove: (clipId) =>
      Effect.gen(function* () {
        const local = yield* SubscriptionRef.get(state);
        if (local.playing?.clipId === clipId) return yield* refused("pop", "the clip is playing");
        const ready = local.ready.find((value) => value.clipId === clipId);
        if (ready !== undefined) {
          yield* change((value) => ({
            ...value,
            ready: value.ready.filter((other) => other.clipId !== clipId),
          }));
          return yield* options.discard?.(ready) ?? Effect.void;
        }
        const index = local.building.findIndex((value) => value.clipId === clipId);
        if (index < 0) return yield* refused("pop", "no such clip");
        // A popped build in flight keeps the slot until it finishes, then its result is discarded.
        yield* change((value) =>
          index === 0
            ? { ...value, popped: new Set(value.popped).add(clipId) }
            : { ...value, building: value.building.filter((other) => other.clipId !== clipId) },
        );
      }),
    move: (clipId, position) =>
      change((value) => {
        const moved = value.ready.find((other) => other.clipId === clipId);
        if (moved === undefined) return value;
        const rest = value.ready.filter((other) => other.clipId !== clipId);
        return { ...value, ready: [...rest.slice(0, position), moved, ...rest.slice(position)] };
      }),
    setAutoplay: (enabled) => change((value) => ({ ...value, autoplay: enabled })),
    cut: (next) =>
      Effect.gen(function* () {
        yield* change((value) => {
          const moved = value.ready.find((other) => other.clipId === next);
          return moved === undefined
            ? value
            : { ...value, ready: [moved, ...value.ready.filter((other) => other !== moved)] };
        });
        const current = yield* Ref.get(stop);
        if (current !== undefined) yield* Deferred.succeed(current, undefined);
      }),
    video: Stream.fromPubSub(video),
    audio: Stream.fromPubSub(audio),
    close: Effect.succeed(noAcquisition),
  };
  return source;
});
