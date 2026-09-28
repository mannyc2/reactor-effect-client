/**
 * The in-process native peer: the client's `Peer` port on one native handle.
 * libwebrtc callbacks never enter JavaScript; the notifier's readiness callback
 * opens a latch, and one pump per native queue drains it on the peer's fibers.
 */
import * as Deferred from "effect/Deferred";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Latch from "effect/Latch";
import * as Ref from "effect/Ref";
import type * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import type { Track } from "reactor-effect-client/Coordinator";
import type { AudioFrame, MediaPressure, VideoFrame } from "reactor-effect-client/Media";
import { trackFeed } from "reactor-effect-client/Peer";
import type { Peer, PeerEvent, TrackFeed } from "reactor-effect-client/Peer";
import { ReactorError } from "reactor-effect-client/ReactorError";
import { encodeText, NativeBridge, NativeCall, Ready } from "./bridge.js";
import type { Take } from "./bridge.js";
import * as Events from "./events.js";
import type { Library } from "./library.js";

/**
 * How long closing a peer waits for its native owner join. A healthy join under
 * real libwebrtc took 13-62 ms in the media load tests on Node and Bun, which
 * require it under 2 s; the default leaves five times that bound.
 */
export const defaultShutdownTimeout: Duration.Duration = Duration.seconds(10);

/** How long reading statistics may take when classifying a failed connection. */
const classifyTimeout = Duration.seconds(2);

const videoBounds = {
  capacity: 4,
  maxBytes: 64 * 1024 * 1024,
  bytes: (frame: VideoFrame) =>
    frame.data.byteLength + frame.metadata.byteLength + frame.track.length * 2,
};
const audioBounds = {
  capacity: 128,
  maxBytes: 4 * 1024 * 1024,
  bytes: (frame: AudioFrame) => frame.samples.byteLength + frame.track.length * 2,
};

/** A native peer, and the bounded owner join its scope's finalizer also runs. */
export interface NativePeer extends Peer {
  readonly shutdown: Effect.Effect<void, ReactorError>;
}

interface State {
  readonly phase: "idle" | "open" | "closed";
  readonly emit: ((event: PeerEvent) => void) | undefined;
  readonly tracks: ReadonlyArray<Track>;
  readonly video: ReadonlyMap<string, TrackFeed<VideoFrame>>;
  readonly audio: ReadonlyMap<string, TrackFeed<AudioFrame>>;
  readonly failure: ReactorError | undefined;
}

const unsupported = (message: string): ReactorError =>
  ReactorError.fromCode("InvalidInput", message, { outcome: "not-submitted" });

/** A take's thrown ABI failure, already a `ReactorError`, or an unexpected one as `Native`. */
const taking = <A>(operation: string, take: () => Take<A>): Effect.Effect<Take<A>, ReactorError> =>
  Effect.try({
    try: take,
    catch: (cause) =>
      ReactorError.is(cause)
        ? cause
        : ReactorError.fromCode("Native", `${operation} failed`, { operation, detail: cause }),
  });

export const make = Effect.fnUntraced(function* (
  library: Library,
  shutdownTimeout: Duration.Duration = defaultShutdownTimeout,
): Effect.fn.Return<NativePeer, ReactorError, Scope.Scope> {
  const wake = {
    events: yield* Latch.make(false),
    video: yield* Latch.make(false),
    audio: yield* Latch.make(false),
  };
  const bridge = yield* NativeBridge.make(library, (ready) => {
    // The notifier's callback on the JavaScript thread only opens latches.
    if ((ready & Ready.Events) !== 0) Latch.openUnsafe(wake.events);
    if ((ready & Ready.Video) !== 0) Latch.openUnsafe(wake.video);
    if ((ready & Ready.Audio) !== 0) Latch.openUnsafe(wake.audio);
  });
  const state = yield* Ref.make<State>({
    phase: "idle",
    emit: undefined,
    tracks: [],
    video: new Map(),
    audio: new Map(),
    failure: undefined,
  });

  /** Fence at once: no later event reaches the session, and every feed ends. */
  const close: Effect.Effect<void> = Effect.gen(function* () {
    const previous = yield* Ref.getAndUpdate(state, (current) => ({
      ...current,
      phase: "closed" as const,
      emit: undefined,
    }));
    if (previous.phase === "closed") return;
    bridge.close();
    // Let waiting pumps observe the close and exit.
    yield* Effect.forEach(Object.values(wake), Latch.open, { discard: true });
    const failure = previous.failure;
    yield* Effect.forEach(
      [...previous.video.values(), ...previous.audio.values()],
      (feed) => (failure === undefined ? feed.end : feed.fail(failure)),
      { discard: true },
    );
  });

  /** The peer's terminal failure: reported once as an event, then the peer closes. */
  const fail = (error: ReactorError): Effect.Effect<void> =>
    Effect.gen(function* () {
      const previous = yield* Ref.getAndUpdate(state, (current) =>
        current.phase === "closed" || current.failure !== undefined
          ? current
          : { ...current, failure: error },
      );
      if (previous.phase === "closed" || previous.failure !== undefined) return;
      previous.emit?.({ type: "error", error });
      yield* close;
    });

  /**
   * Drain one native queue whenever readiness opens its latch. Yielding after
   * every item lets readers run between frames, so a backlog released by a
   * stalled event loop reaches bounded readers at their pace rather than at once.
   */
  const pump = (latch: Latch.Latch, step: Effect.Effect<boolean, ReactorError>) =>
    Effect.gen(function* () {
      for (;;) {
        yield* latch.await;
        yield* latch.close;
        if ((yield* Ref.get(state)).phase === "closed") return;
        while (yield* step) yield* Effect.yieldNow;
      }
    }).pipe(Effect.catch(fail));

  /**
   * Report a failed connection as IceFailed or TransportFailed rather than a
   * bare state, from the statistics read before the pump takes another event.
   */
  const classify = bridge.call(NativeCall.Stats).pipe(
    Effect.map((stats) =>
      Array.isArray(stats)
        ? Events.connectionFailure(stats)
        : ReactorError.fromCode("Disconnected", "peer state failed"),
    ),
    Effect.timeoutOrElse({
      duration: classifyTimeout,
      orElse: () =>
        Effect.fail(ReactorError.fromCode("Timeout", "native failure classification timed out")),
    }),
    Effect.catch((cause) =>
      Effect.succeed(ReactorError.fromCode("Disconnected", "peer state failed", { detail: cause })),
    ),
    Effect.flatMap(fail),
  );

  const stepEvent = Effect.gen(function* () {
    const packet = yield* taking("take native event", () => bridge.takeEvent());
    if (packet === undefined || packet === null) return false;
    const event = yield* Events.decodeEvent(packet);
    if (event === "Failed") yield* classify;
    else if (event.type === "error") yield* fail(event.error);
    else (yield* Ref.get(state)).emit?.(event);
    return true;
  });

  const stepVideo = Effect.gen(function* () {
    const taken = yield* taking("take native video", () => bridge.takeVideo());
    if (taken === undefined || taken === null) return false;
    const current = yield* Ref.get(state);
    const frame = yield* Events.videoFrame(current.tracks)(taken);
    const feed = current.video.get(frame.track);
    if (feed !== undefined) yield* feed.publish(frame);
    return true;
  });

  const stepAudio = Effect.gen(function* () {
    const taken = yield* taking("take native audio", () => bridge.takeAudio());
    if (taken === undefined || taken === null) return false;
    const current = yield* Ref.get(state);
    const frame = yield* Events.audioFrame(current.tracks)(taken);
    const feed = current.audio.get(frame.track);
    if (feed !== undefined) yield* feed.publish(frame);
    return true;
  });

  const feeds = <A>(
    tracks: ReadonlyArray<Track>,
    kind: Track["kind"],
    bounds: Parameters<typeof trackFeed<A>>[0],
  ) =>
    Effect.map(
      Effect.forEach(
        tracks.filter((track) => track.direction === "recvonly" && track.kind === kind),
        (track) => Effect.map(trackFeed<A>(bounds), (feed) => [track.name, feed] as const),
      ),
      (entries): ReadonlyMap<string, TrackFeed<A>> => new Map(entries),
    );

  const media =
    <A>(kind: "video" | "audio", select: (current: State) => ReadonlyMap<string, TrackFeed<A>>) =>
    (name: string): Stream.Stream<A, ReactorError> =>
      Ref.get(state).pipe(
        Effect.flatMap((current) => {
          const feed = select(current).get(name);
          return feed === undefined
            ? Effect.fail(
                unsupported(`native ${kind} needs a declared receive track of that kind: ${name}`),
              )
            : Effect.succeed(feed.stream);
        }),
        Stream.unwrap,
      );

  const pressure: Effect.Effect<MediaPressure, ReactorError> = Effect.gen(function* () {
    const snapshot = yield* Events.decodePressure(yield* bridge.call(NativeCall.MediaSnapshot));
    const current = yield* Ref.get(state);
    let readerOverflows = 0n;
    for (const feed of [...current.video.values(), ...current.audio.values()])
      readerOverflows += yield* feed.overflows;
    return { ...snapshot, readerOverflows };
  });

  const json = (value: Parameters<typeof Events.jsonRequest>[0]) =>
    Effect.map(Events.jsonRequest(value), encodeText);

  const join: Effect.Effect<void, ReactorError> = close.pipe(
    Effect.andThen(Effect.interruptible(bridge.shutdown)),
    // A failed shutdown is a Shutdown failure; the native failure stays in `detail`.
    Effect.mapError((error) =>
      error.reason._tag === "Shutdown"
        ? error
        : ReactorError.fromCode("Shutdown", error.message, { ...error.context, detail: error }),
    ),
    // Only the wait is bounded: on expiry the join keeps its handle, retained
    // until it completes, and this peer reports the overrun.
    Effect.timeoutOrElse({
      duration: shutdownTimeout,
      orElse: () =>
        bridge.retain.pipe(
          Effect.andThen(
            Effect.fail(
              ReactorError.fromCode(
                "Shutdown",
                "native owner join exceeded its deadline; handle retained",
                {
                  operation: "shutdown native WebRTC",
                },
              ),
            ),
          ),
        ),
    }),
    Effect.uninterruptible,
  );
  const finished = yield* Deferred.make<void, ReactorError>();
  const started = yield* Ref.make(false);
  const explicit = yield* Ref.make(false);
  /** The bounded join, run once; every later call awaits the same outcome. */
  const run = Effect.gen(function* () {
    if (!(yield* Ref.getAndSet(started, true)))
      yield* Deferred.done(finished, yield* Effect.exit(join));
    return yield* Deferred.await(finished);
  }).pipe(Effect.uninterruptible);
  const shutdown = Ref.set(explicit, true).pipe(Effect.andThen(run));
  // A shutdown nobody ran explicitly reports a failure by dying, so the
  // session's close report keeps it; one a caller ran already reported to it.
  yield* Effect.addFinalizer(() =>
    Effect.flatMap(Ref.get(explicit), (reported) =>
      reported ? Effect.ignore(run) : Effect.orDie(run),
    ),
  );

  return {
    media: {
      _tag: "Decoded",
      video: media("video", (current) => current.video),
      audio: media("audio", (current) => current.audio),
      pressure,
    },
    prepare: (servers, tracks, emit) =>
      Effect.gen(function* () {
        const request = yield* Events.prepareInput({ servers, tracks }).pipe(
          Effect.flatMap(Events.prepareRequest),
        );
        const video = yield* feeds<VideoFrame>(tracks, "video", videoBounds);
        const audio = yield* feeds<AudioFrame>(tracks, "audio", audioBounds);
        yield* Ref.update(state, (current) =>
          current.phase === "closed"
            ? current
            : { ...current, phase: "open" as const, emit, tracks, video, audio },
        );
        const prepared = yield* Events.decodePrepared(
          yield* bridge.call(NativeCall.Prepare, encodeText(request)),
        );
        yield* Effect.forkScoped(pump(wake.events, stepEvent));
        yield* Effect.forkScoped(pump(wake.video, stepVideo));
        yield* Effect.forkScoped(pump(wake.audio, stepAudio));
        return prepared;
      }),
    answer: (sdp) => Effect.asVoid(bridge.call(NativeCall.Answer, encodeText(sdp))),
    send: (channel, bytes) => bridge.send(channel, bytes),
    direction: (name, active) =>
      Effect.flatMap(json({ name, active }), (request) =>
        Effect.asVoid(bridge.call(NativeCall.Direction, request)),
      ),
    maxBitrate: (name, bitsPerSecond) =>
      !Number.isSafeInteger(bitsPerSecond) || bitsPerSecond < 1 || bitsPerSecond > 0x7fffffff
        ? Effect.fail(unsupported("native max bitrate must be an integer in 1..2147483647"))
        : Effect.flatMap(json({ name, bitsPerSecond }), (request) =>
            Effect.asVoid(bridge.call(NativeCall.MaxBitrate, request)),
          ),
    stats: Effect.flatMap(bridge.call(NativeCall.Stats), (stats) =>
      Array.isArray(stats)
        ? Effect.succeed(Events.statsValue(stats) as ReadonlyArray<unknown>)
        : Effect.fail(ReactorError.fromCode("Protocol", "native stats response is not an array")),
    ),
    close,
    shutdown,
  };
});
