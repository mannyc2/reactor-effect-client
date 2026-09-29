/**
 * A playout `Source` rendered in this process instead of by Reactor: one build
 * slot and a playout queue with H3's autoplay semantics, driven by the
 * application's renderer. `build` makes each clip's value and `present` plays
 * it. It costs nothing, and its session is unending unless given a `lifetime`,
 * so it serves locally rendered material (speech, stills) and demos.
 *
 * Its events keep the order the playout relies on. Each change goes out with
 * the events it implies as one step, in the order the changes happen, and a
 * clip's `Started`, `Ended` or `Failed` comes before the `State` that shows it.
 * So once `enqueue` returns, every `State` lists the clip until its `Ended` or
 * `Failed`, or until it is removed, and none lists it after.
 *
 * A clip's `build` runs in a scope the clip owns. The scope closes once the
 * clip leaves the source: it ended, failed, was stopped or removed, or the
 * source closed. Its finalizers release what the build made, which stays
 * usable until then.
 *
 * A hook that fails with the application's own error fails that clip alone:
 * `Failed` with the library's `message`, and the error itself, pretty-printed,
 * in `provider`. A hook that dies or throws is a bug the source cannot recover
 * from: the source stops, `events` dies with that defect for every reader, and
 * the playout replaces the session.
 */
import * as Arr from "effect/Array";
import * as Cause from "effect/Cause";
import * as Deferred from "effect/Deferred";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Option from "effect/Option";
import * as Predicate from "effect/Predicate";
import * as PubSub from "effect/PubSub";
import * as Random from "effect/Random";
import * as Redacted from "effect/Redacted";
import * as Scope from "effect/Scope";
import * as Semaphore from "effect/Semaphore";
import * as Stream from "effect/Stream";
import * as SubscriptionRef from "effect/SubscriptionRef";
import type * as Take from "effect/Take";
import type { Request } from "./H3.js";
import * as Deadline from "./internal/deadline.js";
import { requestSeconds } from "./internal/h3/profile.js";
import type { AudioFrame, VideoFrame } from "./Media.js";
import type { ClipTag, Source, SourceClip, SourceEvent, SourceState } from "./Playout.js";
import { noAcquisition } from "./Reactor.js";
import { CommandFailure, ReactorError } from "./ReactorError.js";

/** A clip as the renderer's hooks see it. */
export interface LocalClip {
  readonly clipId: string;
  readonly request: Request;
  readonly tag: ClipTag;
  /** Its length in seconds: as requested when `build` sees it, as built when `present` does. */
  readonly seconds: number;
}

/** What `build` made of a clip. */
export interface Rendered<A> {
  /** Handed to `present` when the clip plays. */
  readonly value: A;
  /** The built length in seconds, when it differs from the requested one. */
  readonly seconds?: number | undefined;
}

/** Where a presentation sends its media; the source's `video` and `audio` streams read it. */
export interface Sink {
  readonly video: (frame: VideoFrame) => Effect.Effect<void>;
  readonly audio: (frame: AudioFrame) => Effect.Effect<void>;
}

/**
 * The application's hooks, which render each clip; or none, for a stand-in
 * whose clips build for `buildRatio` of their length and play for their length.
 */
export type Options<A = void, E = never, R = never> =
  | {
      /** Renders a clip before it is Ready, in a scope the clip owns. */
      readonly build: (clip: LocalClip) => Effect.Effect<Rendered<A>, E, R | Scope.Scope>;
      /**
       * Plays a clip, given the value its build made; `clip.seconds` is its
       * built length. The clip ends when this completes, or when it is
       * stopped, which interrupts it. Without it a clip plays for its length.
       */
      readonly present?:
        | ((clip: LocalClip, value: A, sink: Sink) => Effect.Effect<void, E, R>)
        | undefined;
      /**
       * The session's granted length from when it opens, as a capped paid
       * session has, which a playout renews it before; unending by default.
       */
      readonly lifetime?: Duration.Input | undefined;
    }
  | {
      readonly build?: undefined;
      /** Build time per second of clip; zero by default. */
      readonly buildRatio?: number | undefined;
      /** The session's granted length from when it opens; unending by default. */
      readonly lifetime?: Duration.Input | undefined;
    };

type Hooks<A, E, R> = Extract<Options<A, E, R>, { readonly build: unknown }>;
type Build<A, E, R> = Hooks<A, E, R>["build"];
type Present<A, E, R> = NonNullable<Hooks<A, E, R>["present"]>;

/** A built clip: the value `present` gets, and the scope it was built in, open until it leaves. */
interface Built<A> extends LocalClip {
  readonly value: A;
  readonly scope: Scope.Closeable;
}

interface Local<A> {
  /** Clips enqueued so far, which numbers the next one. */
  readonly enqueued: number;
  readonly building: ReadonlyArray<LocalClip>;
  readonly ready: ReadonlyArray<Built<A>>;
  /** The clip playing, and what stops it. */
  readonly playing: { readonly clip: Built<A>; readonly stop: Deferred.Deferred<void> } | undefined;
  readonly autoplay: boolean;
  /** A Ready clip asked to play, which plays next whatever autoplay says. */
  readonly play: string | undefined;
  /** Removed clips whose build is in flight: each finishes, and its clip leaves then. */
  readonly popped: ReadonlySet<string>;
  /** The defect that lost the session for good, once one did. */
  readonly lost: Cause.Cause<never> | undefined;
}

type Removal<A> =
  | { readonly _tag: "Refused"; readonly message: string }
  | { readonly _tag: "Released"; readonly clip: Built<A> }
  | { readonly _tag: "Popped" };

const clipOf = (value: LocalClip): LocalClip => ({
  clipId: value.clipId,
  request: value.request,
  tag: value.tag,
  seconds: value.seconds,
});
const sourceClip = (value: LocalClip): SourceClip => ({
  clipId: value.clipId,
  tag: value.tag,
  seconds: value.seconds,
});
const view = <A>(local: Local<A>): SourceState => ({
  available: true,
  building: local.building.map(sourceClip),
  ready: local.ready.map(sourceClip),
  playing: local.playing === undefined ? undefined : sourceClip(local.playing.clip),
  continuable: [],
});
/** What plays once nothing does: the clip asked for, else with autoplay on the queue's head. */
const nextToPlay = <A>(local: Local<A>): Built<A> | undefined => {
  if (local.playing !== undefined) return undefined;
  if (local.play !== undefined) return local.ready.find((value) => value.clipId === local.play);
  return local.autoplay ? local.ready[0] : undefined;
};
const refused = (operation: string, message: string) =>
  CommandFailure.from(ReactorError.fromCode("InvalidState", message), {
    operation,
    outcome: "not-submitted",
  });
const invalid = (message: string) =>
  ReactorError.fromCode("InvalidInput", message, { outcome: "not-submitted" });
/** A hook's failure, in its own words, for `Failed`'s `provider`. */
const words = (cause: Cause.Cause<unknown>) => cause.pipe(Cause.pretty, Redacted.make);
/** A clip without a presentation plays for its length. */
const played = (clip: LocalClip): Effect.Effect<void> =>
  Effect.sleep(Duration.seconds(clip.seconds));

const make = Effect.fnUntraced(function* <A, E, R>(
  build: Build<A, E, R>,
  present: Present<A, E, R>,
  lifetimeInput: Duration.Input | undefined,
): Effect.fn.Return<Source, ReactorError, R | Scope.Scope> {
  const lifetime =
    lifetimeInput === undefined
      ? Duration.infinity
      : yield* Deadline.decode("LocalSource lifetime")(lifetimeInput);
  if (Duration.isZero(lifetime)) return yield* invalid("LocalSource lifetime must be positive");
  const hex = (yield* Random.nextIntBetween(0, Number.MAX_SAFE_INTEGER)).toString(16);
  const sessionId = `local-${hex.padStart(14, "0")}`;
  // Clip scopes live in their own scope, made before the fibers that use them, so a closing
  // source stops its presentation before it releases the clips.
  const clips = yield* Scope.fork(yield* Effect.scope);
  const lock = yield* Semaphore.make(1);
  const state = yield* SubscriptionRef.make<Local<A>>({
    enqueued: 0,
    building: [],
    ready: [],
    playing: undefined,
    autoplay: false,
    play: undefined,
    popped: new Set(),
    lost: undefined,
  });
  const hub = yield* PubSub.unbounded<Take.Take<SourceEvent, ReactorError>>();
  /** Done once the session is lost, which stops the build slot and playback. */
  const stopped = yield* Deferred.make<void>();
  const video = yield* PubSub.sliding<VideoFrame>(256);
  const audio = yield* PubSub.sliding<AudioFrame>(512);
  const sink: Sink = {
    video: (frame) => Effect.asVoid(PubSub.publish(video, frame)),
    audio: (frame) => Effect.asVoid(PubSub.publish(audio, frame)),
  };
  /**
   * Makes one change: `step` gives its result, the next state and the events
   * it implies, which go out with the `State` after them before any other
   * change. An interruption waits for the change to finish, so what readers
   * see never runs ahead of the state.
   */
  const modify = <X>(
    step: (local: Local<A>) => readonly [X, Local<A>, ReadonlyArray<SourceEvent>?],
  ): Effect.Effect<X> =>
    lock.withPermit(
      Effect.uninterruptible(
        Effect.gen(function* () {
          const local = yield* SubscriptionRef.get(state);
          const [result, next, events = []] = step(local);
          if (next === local) return result;
          yield* PubSub.publish(hub, Arr.append(events, { _tag: "State", state: view(next) }));
          yield* SubscriptionRef.set(state, next);
          return result;
        }),
      ),
    );
  /**
   * Loses the session for good to a defect: `events` dies with it after
   * everything published before, and the source stops. Of the cause, only the
   * defects go out: `events` can fail with the library's errors but not the
   * application's, and an interruption inside a hook is not one of `events`.
   */
  const lose = (cause: Cause.Cause<unknown>): Effect.Effect<void> =>
    lock.withPermit(
      Effect.uninterruptible(
        Effect.gen(function* () {
          const local = yield* SubscriptionRef.get(state);
          if (local.lost !== undefined) return;
          const lost = Cause.fromReasons<never>(cause.reasons.filter(Cause.isDieReason));
          yield* PubSub.publish(hub, Exit.failCause(lost));
          yield* SubscriptionRef.set(state, { ...local, lost });
          yield* Deferred.succeed(stopped, undefined);
        }),
      ),
    );
  /** The first value `pick` finds in the state, now or once it changes. */
  const first = <X>(pick: (local: Local<A>) => X | undefined) =>
    SubscriptionRef.changes(state).pipe(
      Stream.map(pick),
      Stream.filter(Predicate.isNotUndefined),
      Stream.runHead,
      Effect.map(Option.getOrThrow),
    );
  /** `effect`, where anything but an interruption loses the session. */
  const guard = <X, Y, Z>(effect: Effect.Effect<X, Y, Z>) =>
    Effect.catchCauseIf(effect, (cause) => !Cause.hasInterruptsOnly(cause), lose);
  /**
   * Runs `loop` until the source closes or its session is lost; a defect in
   * it, a hook's included, loses the session.
   */
  const keep = <X>(loop: Effect.Effect<void, X, R>) =>
    loop.pipe(Effect.forever, guard, Effect.raceFirst(Deferred.await(stopped)), Effect.forkScoped);
  const without =
    (clipId: string) =>
    <C extends LocalClip>(values: ReadonlyArray<C>): ReadonlyArray<C> =>
      values.filter((value) => value.clipId !== clipId);

  // One build slot: the head of the building queue builds, then waits Ready unless it was removed.
  yield* keep(
    Effect.gen(function* () {
      const next = yield* first((local) => local.building[0]);
      const scope = yield* Scope.fork(clips);
      const built = yield* Effect.suspend(() => build(next)).pipe(
        Scope.provide(scope),
        Effect.exit,
      );
      if (Exit.isFailure(built) && Cause.hasDies(built.cause))
        return yield* Effect.failCause(built.cause);
      const leaves = (local: Local<A>): Local<A> => ({
        ...local,
        building: without(next.clipId)(local.building),
        popped: new Set([...local.popped].filter((id) => id !== next.clipId)),
      });
      if (Exit.isFailure(built)) {
        // A removed clip leaves without a failure, as it would have without its build.
        yield* modify((local) => [
          undefined,
          leaves(local),
          local.popped.has(next.clipId)
            ? []
            : [
                {
                  _tag: "Failed",
                  clip: sourceClip(next),
                  message: "the local build failed",
                  provider: words(built.cause),
                },
              ],
        ]);
        return yield* Scope.close(scope, built);
      }
      const ready: Built<A> = {
        ...next,
        seconds: built.value.seconds ?? next.seconds,
        value: built.value.value,
        scope,
      };
      const popped = yield* modify((local) =>
        local.popped.has(next.clipId)
          ? [true, leaves(local)]
          : [false, { ...leaves(local), ready: [...local.ready, ready] }],
      );
      if (popped) yield* Scope.close(scope, Exit.void);
    }),
  );

  // Playback: a clip plays once nothing else does, as `nextToPlay` picks it.
  yield* keep(
    Effect.gen(function* () {
      yield* first(nextToPlay);
      const stop = yield* Deferred.make<void>();
      const clip = yield* modify((local) => {
        const next = nextToPlay(local);
        if (next === undefined) return [undefined, local];
        return [
          next,
          {
            ...local,
            ready: without(next.clipId)(local.ready),
            playing: { clip: next, stop },
            play: undefined,
          },
          [{ _tag: "Started", clip: sourceClip(next) }],
        ];
      });
      if (clip === undefined) return;
      const presented = yield* Effect.raceFirst(
        Effect.exit(Effect.suspend(() => present(clipOf(clip), clip.value, sink))),
        Effect.as(Deferred.await(stop), undefined),
      );
      if (presented !== undefined && Exit.isFailure(presented) && Cause.hasDies(presented.cause))
        return yield* Effect.failCause(presented.cause);
      const ended: SourceEvent =
        presented === undefined || Exit.isSuccess(presented)
          ? {
              _tag: "Ended",
              clip: sourceClip(clip),
              termination: presented === undefined ? "stopped" : "finished",
            }
          : {
              _tag: "Failed",
              clip: sourceClip(clip),
              message: "the local presentation failed",
              provider: words(presented.cause),
            };
      yield* modify((local) => [undefined, { ...local, playing: undefined }, [ended]]);
      yield* Scope.close(clip.scope, presented ?? Exit.void);
    }),
  );

  const source: Source = {
    sessionId,
    lifetime,
    events: Stream.unwrap(
      lock.withPermit(
        Effect.gen(function* () {
          // Subscribed and read under the lock, so the first State is the one every later event follows.
          const subscription = yield* PubSub.subscribe(hub);
          const local = yield* SubscriptionRef.get(state);
          const current = Stream.succeed<SourceEvent>({ _tag: "State", state: view(local) });
          return Stream.concat(
            current,
            local.lost === undefined
              ? subscription.pipe(Stream.fromSubscription, Stream.flattenTake)
              : Stream.failCause(local.lost),
          );
        }),
      ),
    ),
    enqueue: (request, tag, _continueFrom) =>
      modify((local) => {
        const clip: LocalClip = {
          clipId: `${sessionId}-clip-${String(local.enqueued + 1)}`,
          request,
          tag,
          seconds: request.seconds ?? requestSeconds.min,
        };
        return [
          clip.clipId,
          { ...local, enqueued: local.enqueued + 1, building: [...local.building, clip] },
        ];
      }),
    remove: (clipId) =>
      Effect.gen(function* () {
        const removal = yield* modify((local): readonly [Removal<A>, Local<A>] => {
          if (local.playing?.clip.clipId === clipId)
            return [{ _tag: "Refused", message: "the clip is playing" }, local];
          const ready = local.ready.find((value) => value.clipId === clipId);
          if (ready !== undefined)
            return [
              { _tag: "Released", clip: ready },
              {
                ...local,
                ready: without(clipId)(local.ready),
                play: local.play === clipId ? undefined : local.play,
              },
            ];
          const index = local.building.findIndex((value) => value.clipId === clipId);
          if (index < 0) return [{ _tag: "Refused", message: "no such clip" }, local];
          // A removed build in flight keeps the slot until it finishes; its clip leaves then.
          return [
            { _tag: "Popped" },
            index === 0
              ? { ...local, popped: new Set(local.popped).add(clipId) }
              : { ...local, building: without(clipId)(local.building) },
          ];
        });
        switch (removal._tag) {
          case "Refused":
            return yield* refused("pop", removal.message);
          case "Released":
            // A finalizer that dies is the renderer's bug, and loses the session as a hook's would.
            return yield* guard(Scope.close(removal.clip.scope, Exit.void));
          case "Popped":
            return;
        }
      }),
    move: (clipId, position) =>
      modify((local) => {
        const moved = local.ready.find((other) => other.clipId === clipId);
        if (moved === undefined) return [undefined, local];
        const rest = without(clipId)(local.ready);
        return [
          undefined,
          { ...local, ready: [...rest.slice(0, position), moved, ...rest.slice(position)] },
        ];
      }),
    setAutoplay: (enabled) =>
      modify((local) =>
        local.autoplay === enabled
          ? [undefined, local]
          : [undefined, { ...local, autoplay: enabled }],
      ),
    stop: (clipId) =>
      Effect.gen(function* () {
        const playing = (yield* SubscriptionRef.get(state)).playing;
        if (playing?.clip.clipId !== clipId) return;
        yield* Deferred.succeed(playing.stop, undefined);
        // It has ended once playback takes it off, after its `Ended` went out, or once the
        // session is lost, when playback has stopped for good.
        yield* first((local) =>
          local.playing?.clip.clipId === clipId && local.lost === undefined ? undefined : true,
        );
      }),
    play: (clipId) =>
      Effect.flatMap(
        modify((local): readonly [string | undefined, Local<A>] => {
          if (local.playing !== undefined) return ["a clip is playing", local];
          if (!local.ready.some((value) => value.clipId === clipId))
            return ["no Ready clip has this id", local];
          return [undefined, { ...local, play: clipId }];
        }),
        (refusal) => (refusal === undefined ? Effect.void : Effect.fail(refused("play", refusal))),
      ),
    video: Stream.fromPubSub(video),
    audio: Stream.fromPubSub(audio),
    close: Effect.succeed(noAcquisition),
  };
  return source;
});

/**
 * A local source in the caller's scope, whose hooks run with the services the
 * caller has; closing the scope stops its fibers, then closes every clip's
 * scope. It fails with `InvalidInput` for a `lifetime` that is not a positive,
 * finite duration, or a `buildRatio` that is negative or not finite.
 */
export const open = <A = void, E = never, R = never>(
  options?: Options<A, E, R>,
): Effect.Effect<Source, ReactorError, R | Scope.Scope> => {
  if (options?.build !== undefined)
    return make(options.build, options.present ?? played, options.lifetime);
  const ratio = options?.buildRatio ?? 0;
  if (!(Number.isFinite(ratio) && ratio >= 0))
    return Effect.fail(invalid("LocalSource buildRatio must be a finite number, not negative"));
  return make<void, E, R>(
    (clip) => Effect.as(Effect.sleep(Duration.seconds(clip.seconds * ratio)), { value: undefined }),
    played,
    options?.lifetime,
  );
};
