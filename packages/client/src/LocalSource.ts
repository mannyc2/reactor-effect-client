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
 * A clip's `build` and `present` run in a scope the clip owns. The scope
 * closes once the clip leaves the source: it ended, failed, was stopped or
 * removed, or the source closed. Its finalizers release what the hooks made,
 * which stays usable until then, and they run in full once they start.
 *
 * A hook that fails with the application's own error fails that clip alone:
 * `Failed` with the library's `message`, and the error itself, pretty-printed,
 * in `provider`. A hook that dies or throws, or a finalizer that fails, is a
 * bug the source cannot recover from: the source stops, `events` dies with
 * that defect for every reader, and the playout replaces the session.
 */
import * as Arr from "effect/Array";
import * as Cause from "effect/Cause";
import * as Clock from "effect/Clock";
import * as Deferred from "effect/Deferred";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Option from "effect/Option";
import * as Predicate from "effect/Predicate";
import * as PubSub from "effect/PubSub";
import * as Random from "effect/Random";
import * as Redacted from "effect/Redacted";
import * as References from "effect/References";
import * as Scope from "effect/Scope";
import * as Semaphore from "effect/Semaphore";
import * as Stream from "effect/Stream";
import * as SubscriptionRef from "effect/SubscriptionRef";
import type * as Take from "effect/Take";
import * as Tracer from "effect/Tracer";
import type { Request } from "./H3.js";
import * as Deadline from "./internal/deadline.js";
import { clipModel } from "./internal/h3/clipModel.js";
import { currentParent, spanOptions } from "./internal/trace.js";
import type { AudioFrame, VideoFrame } from "./Media.js";
import type { ClipModel, ClipRequest, ClipTag, Source, SourceClip, SourceEvent, SourceState } from "./Playout.js";
import { noAcquisition } from "./Reactor.js";
import { CommandFailure, ReactorError } from "./ReactorError.js";

/** A clip as the renderer's hooks see it. */
export interface LocalClip<Req extends ClipRequest = Request> {
  readonly clipId: string;
  readonly request: Req;
  readonly tag: ClipTag;
  /** Its length in seconds: as requested when `build` sees it, as built when `present` does. */
  readonly seconds: number;
}

/** What `build` made of a clip. */
export interface Rendered<A> {
  /** Handed to `present` when the clip plays. */
  readonly value: A;
  /**
   * The built length in seconds, when it differs from the requested one. It
   * must be positive and finite, or the clip fails, since a playout times its
   * plan by the lengths its clips report.
   */
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
 * Pass `model` to `open` for the model the source runs, which the playout's own
 * `model` must name alike; H3's by default. A renderer with its own lengths,
 * such as speech shorter than H3's 5 s, declares them here and passes the same
 * model to `Playout.make`.
 */
export type Options<A = void, E = never, R = never, E2 = never, R2 = never, Req extends ClipRequest = Request> =
  | {
      /** Renders a clip before it is Ready, in a scope the clip owns. */
      readonly build: (clip: LocalClip<Req>) => Effect.Effect<Rendered<A>, E, R | Scope.Scope>;
      /**
       * Plays a clip, given the value its build made, in the same scope;
       * `clip.seconds` is its built length. The clip ends when this completes,
       * or when it is stopped, which interrupts it. Without it a clip plays
       * for its length.
       */
      readonly present?:
        | ((clip: LocalClip<Req>, value: A, sink: Sink) => Effect.Effect<void, E2, R2 | Scope.Scope>)
        | undefined;
      /**
       * The session's granted length from when it opens, as a capped paid
       * session has, which a playout renews it before; unending by default.
       */
      readonly lifetime?: Duration.Input | undefined;
    }
  | {
      readonly build?: undefined;
      /** A presentation plays what a build made, so it comes only with `build`. */
      readonly present?: undefined;
      /** Build time per second of clip; zero by default. */
      readonly buildRatio?: number | undefined;
      /** The session's granted length from when it opens; unending by default. */
      readonly lifetime?: Duration.Input | undefined;
    };

type Hooks<A, E, R, E2, R2, Req extends ClipRequest> = Extract<Options<A, E, R, E2, R2, Req>, { readonly build: unknown }>;
type Build<A, E, R, Req extends ClipRequest> = Hooks<A, E, R, never, never, Req>["build"];
type Present<A, E, R, Req extends ClipRequest> = NonNullable<Hooks<A, never, never, E, R, Req>["present"]>;

interface QueuedClip<Req extends ClipRequest> extends LocalClip<Req> {
  readonly parent: Tracer.ExternalSpan | undefined;
}

/** A built clip: the value `present` gets, and the scope it was built in, open until it leaves. */
interface Built<A, Req extends ClipRequest> extends QueuedClip<Req> {
  readonly value: A;
  readonly scope: Scope.Closeable;
}

interface Local<A, Req extends ClipRequest> {
  /** Clips enqueued so far, which numbers the next one. */
  readonly enqueued: number;
  readonly building: ReadonlyArray<QueuedClip<Req>>;
  readonly ready: ReadonlyArray<Built<A, Req>>;
  /** The clip playing, and what stops it. */
  readonly playing: { readonly clip: Built<A, Req>; readonly stop: Deferred.Deferred<void> } | undefined;
  readonly autoplay: boolean;
  /** A Ready clip asked to play, which plays next whatever autoplay says. */
  readonly play: string | undefined;
  /** Removed clips whose build is in flight: each finishes, and its clip leaves then. */
  readonly popped: ReadonlySet<string>;
  /** The defect that lost the session for good, once one did. */
  readonly lost: Cause.Cause<never> | undefined;
}

type Removal<A, Req extends ClipRequest> =
  | { readonly _tag: "Refused"; readonly message: string }
  | { readonly _tag: "Released"; readonly clip: Built<A, Req> }
  | { readonly _tag: "Popped" };

const clipOf = <Req extends ClipRequest>(value: LocalClip<Req>): LocalClip<Req> => ({
  clipId: value.clipId,
  request: value.request,
  tag: value.tag,
  seconds: value.seconds,
});
const sourceClip = (value: LocalClip<ClipRequest>): SourceClip => ({
  clipId: value.clipId,
  tag: value.tag,
  seconds: value.seconds,
});
const view = <A, Req extends ClipRequest>(local: Local<A, Req>): SourceState => ({
  available: true,
  building: local.building.map(sourceClip),
  ready: local.ready.map(sourceClip),
  playing: local.playing === undefined ? undefined : sourceClip(local.playing.clip),
  continuable: [],
});
/** What plays once nothing does: the clip asked for, else with autoplay on the queue's head. */
const nextToPlay = <A, Req extends ClipRequest>(local: Local<A, Req>): Built<A, Req> | undefined => {
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
const played = (clip: LocalClip<ClipRequest>): Effect.Effect<void> =>
  Effect.sleep(Duration.seconds(clip.seconds));
/**
 * Closes a clip's scope, uninterruptibly: a scope counts as closed before its
 * finalizers run, so an interruption would skip the rest of them for good.
 */
const release = (scope: Scope.Closeable, exit: Exit.Exit<unknown, unknown>) =>
  Effect.uninterruptible(Scope.close(scope, exit));

const make = Effect.fnUntraced(function* <A, E, R, E2, R2, Req extends ClipRequest>(
  build: Build<A, E, R, Req>,
  present: Present<A, E2, R2, Req>,
  lifetimeInput: Duration.Input | undefined,
  model: ClipModel<Req>,
): Effect.fn.Return<Source<Req>, ReactorError, R | R2 | Scope.Scope> {
  const lifetime =
    lifetimeInput === undefined
      ? Duration.infinity
      : yield* Deadline.decode("LocalSource lifetime")(lifetimeInput);
  if (Duration.isZero(lifetime)) return yield* invalid("LocalSource lifetime must be positive");
  const hex = (yield* Random.nextIntBetween(0, Number.MAX_SAFE_INTEGER)).toString(16);
  const sessionId = `local-${hex.padStart(14, "0")}`;
  const acquisition = yield* currentParent;
  const hookOptions = (clip: QueuedClip<Req>): Tracer.SpanOptionsNoTrace => ({
    ...spanOptions({ acquisition, parent: clip.parent }),
    attributes: {
      "reactor.session.id": sessionId,
      "reactor.clip.id": clip.clipId,
      ...(clip.tag._tag === "Item" ? { "reactor.playout.item.key": clip.tag.key } : {}),
    },
  });
  const runHook = <X, Y, Z>(
    name: "build" | "present",
    clip: QueuedClip<Req>,
    effect: Effect.Effect<X, Y, Z>,
  ) =>
    Effect.useSpan(`LocalSource.${name}`, hookOptions(clip), (span) =>
      effect.pipe(
        Effect.withParentSpan(span, { captureStackTrace: false }),
        Effect.onExit((exit) => {
          if (Exit.isSuccess(exit) || Cause.hasInterruptsOnly(exit.cause)) return Effect.void;
          // Hook errors may hold private text. Preserve their exit for the source, while
          // ending the span with safe evidence; Effect skips ending an already-ended span.
          return Effect.gen(function* () {
            const timing = yield* References.TracerTimingEnabled;
            const endedAt = timing ? yield* Clock.currentTimeNanos : 0n;
            yield* Effect.sync(() => {
              span.attribute("error.type", "InvalidState");
              span.end(
                endedAt,
                Exit.fail(ReactorError.fromCode("InvalidState", `the local ${name} failed`)),
              );
            });
          });
        }),
      ),
    );
  // What the source owns lives in a sequential scope of its own, which the caller's scope closes
  // as one, whatever its own order: its finalizers run in the reverse of the order they are made
  // in below, so the source stops, then its fibers end with their presentations, and only then
  // are the clips released.
  const owned = yield* Scope.fork(yield* Effect.scope, "sequential");
  const clips = yield* Scope.fork(owned);
  const lock = yield* Semaphore.make(1);
  const state = yield* SubscriptionRef.make<Local<A, Req>>({
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
  /** Done once the source stops, lost or closed: the build slot and playback end, and so does a `stop`. */
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
    step: (local: Local<A, Req>) => readonly [X, Local<A, Req>, ReadonlyArray<SourceEvent>?],
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
   * Loses the session for good: `events` dies after everything published
   * before, and the source stops. Of the cause, only the defects go out:
   * `events` can fail with the library's errors but not the application's, and
   * an interruption inside a hook is not one of `events`. A cause without a
   * defect, such as a finalizer's own interruption, goes out as the defect
   * Effect squashes it to.
   */
  const lose = (cause: Cause.Cause<unknown>): Effect.Effect<void> =>
    lock.withPermit(
      Effect.uninterruptible(
        Effect.gen(function* () {
          const local = yield* SubscriptionRef.get(state);
          if (local.lost !== undefined) return;
          const dies = cause.reasons.filter(Cause.isDieReason);
          const lost =
            dies.length > 0 ? Cause.fromReasons<never>(dies) : cause.pipe(Cause.squash, Cause.die);
          yield* PubSub.publish(hub, Exit.failCause(lost));
          yield* SubscriptionRef.set(state, { ...local, lost });
          yield* Deferred.succeed(stopped, undefined);
        }),
      ),
    );
  /** The first value `pick` finds in the state, now or once it changes. */
  const first = <X>(pick: (local: Local<A, Req>) => X | undefined) =>
    SubscriptionRef.changes(state).pipe(
      Stream.map(pick),
      Stream.filter(Predicate.isNotUndefined),
      Stream.runHead,
      Effect.map(Option.getOrThrow),
    );
  /**
   * `effect`, where any failure loses the session. Its fiber's own
   * interruption is not one: Effect runs no handler for it.
   */
  const guard = <X, Y, Z>(effect: Effect.Effect<X, Y, Z>) => Effect.catchCause(effect, lose);
  /**
   * Runs `loop` until the source stops; a failure in it, a hook's defect included, loses the
   * session. A presentation stops with its playback.
   */
  const keep = <X>(loop: Effect.Effect<void, X, R | R2>) =>
    loop.pipe(
      Effect.forever,
      guard,
      Effect.raceFirst(Deferred.await(stopped)),
      Effect.forkIn(owned),
    );
  const without =
    (clipId: string) =>
    <C extends LocalClip<Req>>(values: ReadonlyArray<C>): ReadonlyArray<C> =>
      values.filter((value) => value.clipId !== clipId);

  // One build slot: the head of the building queue builds, then waits Ready unless it was removed.
  yield* keep(
    Effect.gen(function* () {
      const next = yield* first((local) => local.building[0]);
      const scope = yield* Scope.fork(clips);
      const built = yield* runHook(
        "build",
        next,
        Effect.suspend(() => build(clipOf(next))),
      ).pipe(Scope.provide(scope), Effect.exit);
      if (Exit.isFailure(built) && Cause.hasDies(built.cause))
        return yield* Effect.failCause(built.cause);
      const leaves = (local: Local<A, Req>): Local<A, Req> => ({
        ...local,
        building: without(next.clipId)(local.building),
        popped: new Set([...local.popped].filter((id) => id !== next.clipId)),
      });
      /** Fails the clip; a removed one leaves without a failure, as it would have without its build. */
      const fails = (message: string, provider: Redacted.Redacted<string>) =>
        Effect.andThen(
          modify((local) => [
            undefined,
            leaves(local),
            local.popped.has(next.clipId)
              ? []
              : [{ _tag: "Failed", clip: sourceClip(next), message, provider }],
          ]),
          release(scope, built),
        );
      if (Exit.isFailure(built)) return yield* fails("the local build failed", words(built.cause));
      const seconds = built.value.seconds ?? next.seconds;
      if (!(Number.isFinite(seconds) && seconds > 0))
        return yield* fails(
          "the local build gave a length that is not a positive, finite number of seconds",
          Redacted.make(String(seconds)),
        );
      const ready: Built<A, Req> = { ...next, seconds, value: built.value.value, scope };
      const popped = yield* modify((local) =>
        local.popped.has(next.clipId)
          ? [true, leaves(local)]
          : [false, { ...leaves(local), ready: [...local.ready, ready] }],
      );
      if (popped) yield* release(scope, Exit.void);
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
      const presentation = yield* runHook(
        "present",
        clip,
        Effect.suspend(() => present(clipOf(clip), clip.value, sink)),
      ).pipe(Scope.provide(clip.scope), Effect.forkChild());
      const stopping = yield* Effect.raceFirst(
        Effect.as(Fiber.await(presentation), false),
        Effect.as(Deferred.await(stop), true),
      );
      // A stopped presentation is interrupted and waited for, so a defect as it stops still counts.
      if (stopping) yield* Fiber.interrupt(presentation);
      const presented = yield* Fiber.await(presentation);
      if (Exit.isFailure(presented) && Cause.hasDies(presented.cause))
        return yield* Effect.failCause(presented.cause);
      const ended: SourceEvent =
        stopping || Exit.isSuccess(presented)
          ? {
              _tag: "Ended",
              clip: sourceClip(clip),
              termination: stopping ? "stopped" : "finished",
            }
          : {
              _tag: "Failed",
              clip: sourceClip(clip),
              message: "the local presentation failed",
              provider: words(presented.cause),
            };
      yield* modify((local) => [undefined, { ...local, playing: undefined }, [ended]]);
      yield* release(clip.scope, presented);
    }),
  );
  // Made last, so it runs first as the source closes: a `stop` waiting on playback returns.
  yield* Scope.addFinalizer(owned, Deferred.succeed(stopped, undefined));

  const source: Source<Req> = {
    sessionId,
    model,
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
    enqueue: Effect.fnUntraced(function* (request: Req, tag: ClipTag) {
      const parent = yield* currentParent;
      return yield* modify((local) => {
        const clip: QueuedClip<Req> = {
          clipId: `${sessionId}-clip-${String(local.enqueued + 1)}`,
          request,
          tag,
          seconds: request.seconds ?? model.defaultSeconds,
          parent,
        };
        return [
          clip.clipId,
          { ...local, enqueued: local.enqueued + 1, building: [...local.building, clip] },
        ];
      });
    }),
    remove: (clipId) =>
      Effect.gen(function* () {
        const removal = yield* modify((local): readonly [Removal<A, Req>, Local<A, Req>] => {
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
            // A finalizer that fails is the renderer's bug, and loses the session as a hook's would.
            return yield* guard(release(removal.clip.scope, Exit.void));
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
        // source stopped, lost or closed, when nothing plays any more.
        yield* Effect.raceFirst(
          first((local) => (local.playing?.clip.clipId === clipId ? undefined : true)),
          Deferred.await(stopped),
        );
      }),
    play: (clipId) =>
      Effect.flatMap(
        modify((local): readonly [string | undefined, Local<A, Req>] => {
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

const configured = <A, E, R, E2, R2, Req extends ClipRequest>(
  options: Options<A, E, R, E2, R2, Req> | undefined,
  model: ClipModel<Req>,
): Effect.Effect<Source<Req>, ReactorError, R | R2 | Scope.Scope> => {
  if (options?.build !== undefined)
    return make(options.build, options.present ?? played, options.lifetime, model);
  const ratio = options?.buildRatio ?? 0;
  if (!(Number.isFinite(ratio) && ratio >= 0))
    return Effect.fail(invalid("LocalSource buildRatio must be a finite number, not negative"));
  return make(
    (clip) => Effect.as(Effect.sleep(Duration.seconds(clip.seconds * ratio)), { value: undefined }),
    played,
    options?.lifetime,
    model,
  );
};

/**
 * A local source in the caller's scope, whose hooks run with the services the
 * caller has. Closing the scope stops the source, waits for its presentation
 * to stop, then releases every clip. It fails with `InvalidInput` for a
 * `lifetime` that is not a positive, finite duration, or a `buildRatio` that
 * is negative or not finite. Pass `model` for your renderer's request limits
 * and lengths, and pass the same model to `Playout.make`; H3's by default.
 */
export function open<A, E, R, E2, R2, Req extends ClipRequest>(
  options: Options<A, E, R, E2, R2, Req> & { readonly model: ClipModel<Req> },
): Effect.Effect<Source<Req>, ReactorError, R | R2 | Scope.Scope>;
export function open<A = void, E = never, R = never, E2 = never, R2 = never>(
  options?: Options<A, E, R, E2, R2>,
): Effect.Effect<Source, ReactorError, R | R2 | Scope.Scope>;
export function open<A, E, R, E2, R2, Req extends ClipRequest>(
  options?: (Options<A, E, R, E2, R2, Req> & { readonly model: ClipModel<Req> }) | Options<A, E, R, E2, R2>,
) {
  return options !== undefined && "model" in options
    ? configured(options, options.model)
    : configured(options, clipModel);
}
