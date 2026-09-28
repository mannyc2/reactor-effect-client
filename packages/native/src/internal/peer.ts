/**
 * The native peer: the client's `Peer` port over the addon's `NativePeer`,
 * whether that runs in this process or in a child process. The addon's
 * queues arrive as streams; this module names frames, fans them out to
 * bounded readers, classifies failures and bounds the owner join.
 */
import * as Deferred from "effect/Deferred";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Predicate from "effect/Predicate";
import * as Redacted from "effect/Redacted";
import * as Ref from "effect/Ref";
import * as Schema from "effect/Schema";
import type * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import type { IceServer, Track } from "reactor-effect-client/Coordinator";
import type { AudioFrame, MediaPressure, VideoFrame } from "reactor-effect-client/Media";
import { PeerState, trackFeed } from "reactor-effect-client/Peer";
import type { Channel, Peer, PeerEvent, TrackFeed } from "reactor-effect-client/Peer";
import {
  IceFailed,
  Native,
  ReactorError,
  TransportFailed,
} from "reactor-effect-client/ReactorError";
import type * as Binding from "./binding.js";

/** What a peer drives: one addon `NativePeer`, here or in a child process. */
export interface NativeHandle {
  readonly prepare: (
    servers: ReadonlyArray<IceServer>,
    tracks: ReadonlyArray<Track>,
  ) => Effect.Effect<Binding.Prepared, ReactorError>;
  readonly answer: (sdp: string) => Effect.Effect<void, ReactorError>;
  readonly direction: (name: string, active: boolean) => Effect.Effect<void, ReactorError>;
  readonly maxBitrate: (name: string, bitsPerSecond: number) => Effect.Effect<void, ReactorError>;
  readonly send: (channel: Channel, bytes: Uint8Array) => Effect.Effect<void, ReactorError>;
  readonly stats: Effect.Effect<ReadonlyArray<Readonly<Record<string, unknown>>>, ReactorError>;
  readonly pressure: Effect.Effect<Binding.Pressure, ReactorError>;
  /** Each queue's items, taken as the reader pulls, until the peer closes. */
  readonly events: Stream.Stream<Binding.PeerEvent, ReactorError>;
  readonly video: Stream.Stream<Binding.Video, ReactorError>;
  readonly audio: Stream.Stream<Binding.Audio, ReactorError>;
  /** Fence at once. */
  readonly close: Effect.Effect<void>;
  /** Close and join the owner thread, without bound. */
  readonly shutdown: Effect.Effect<void, ReactorError>;
  /** Stop waiting for a join past its deadline, and report why the peer is left as it is. */
  readonly abandon: Effect.Effect<ReactorError>;
}

/** A native peer, and the bounded owner join its scope's finalizer also runs. */
export interface NativePeer extends Peer {
  readonly shutdown: Effect.Effect<void, ReactorError>;
}

/**
 * How long closing a peer waits for its native owner join. A healthy join under
 * real libwebrtc took 13-62 ms in the media load tests on Node and Bun, which
 * require it under 2 s; the default leaves five times that bound.
 */
export const defaultShutdownTimeout: Duration.Duration = Duration.seconds(10);

/** How long reading statistics may take when classifying a failed connection. */
const classifyTimeout = Duration.seconds(2);

/** Failure classes the addon returns only when it refused a call before running it. */
const refusals: ReadonlySet<Binding.FailureClass> = new Set([
  "Closed",
  "InvalidInput",
  "Overflow",
  "ChannelClosed",
]);

/**
 * A failure the addon classified. libwebrtc's text can quote peer SDP or
 * signaling material, so it stays Redacted and out of the message. A call's
 * refusal was not submitted; any other class leaves its side effect unknown.
 */
export const nativeFailure =
  (operation?: string) =>
  (failure: Binding.Failure): ReactorError => {
    const backendMessage = Redacted.make(failure.message);
    const message = `native ${operation ?? "peer"} failed (${failure.class})`;
    const context =
      operation === undefined
        ? {}
        : ({
            operation,
            outcome: refusals.has(failure.class) ? "not-submitted" : "unknown",
          } as const);
    return failure.class === "Native"
      ? ReactorError.make({ reason: Native.make({ message, backendMessage }), context })
      : ReactorError.fromCode(failure.class, message, { ...context, detail: { backendMessage } });
  };

const protocol = (message: string): ReactorError => ReactorError.fromCode("Protocol", message);

/** Report a failed connection as IceFailed or TransportFailed, from its candidate pairs. */
export const connectionFailure = (stats: ReadonlyArray<unknown>): ReactorError => {
  const entries = stats.filter(Predicate.isObject);
  const pairs = entries.filter((entry) => entry.type === "candidate-pair");
  if (pairs.some((pair) => pair.state === "succeeded" || pair.nominated === true))
    return ReactorError.make({
      reason: TransportFailed.make({
        message: "native peer failed after ICE connectivity succeeded",
        pairs: pairs.length,
      }),
    });
  const candidateTypes = new Set(
    entries
      .filter((entry) => entry.type === "local-candidate")
      .map((entry) => entry.candidateType)
      .filter(Predicate.isString),
  );
  return ReactorError.make({
    reason: IceFailed.make({
      message: "native peer found no working ICE candidate pair",
      pairs: pairs.length,
      candidateTypes: [...candidateTypes],
    }),
  });
};

const counters = new Set([
  "bytesSent",
  "bytesReceived",
  "packetsSent",
  "packetsReceived",
  "retransmittedPacketsSent",
  "priority",
]);

/** Statistics with the 64-bit counters the addon sends as decimal strings as bigints. */
export const statsValue = (value: unknown): unknown => {
  if (Array.isArray(value)) return value.map(statsValue);
  if (!Predicate.isObject(value)) return value;
  return Object.fromEntries(
    Object.entries(value).map(([key, entry]) => [
      key,
      counters.has(key) && Predicate.isString(entry) && /^-?[0-9]+$/.test(entry)
        ? BigInt(entry)
        : statsValue(entry),
    ]),
  );
};

/**
 * The declared receive track a media item names by its index in the prepare
 * request. The addon accepts at most one incoming track of each kind: pinned
 * reactor-webrtc gives its remote-track callback no MID to tell two apart.
 */
const receiving = (tracks: ReadonlyArray<Track>, index: number, kind: Track["kind"]) => {
  const track = tracks[index];
  return track?.direction === "recvonly" && track.kind === kind
    ? Effect.succeed(track.name)
    : Effect.fail(protocol(`native ${kind} was delivered without its declared receive mapping`));
};

export const videoFrame = (tracks: ReadonlyArray<Track>) => (taken: Binding.Video) =>
  Effect.flatMap(receiving(tracks, taken.track, "video"), (track) =>
    taken.width === 0 ||
    taken.height === 0 ||
    taken.data.byteLength !== taken.width * taken.height * 4
      ? Effect.fail(protocol("native BGRA frame dimensions do not match its payload"))
      : Effect.succeed<VideoFrame>({
          _tag: "VideoFrame",
          track,
          format: "BGRA",
          width: taken.width,
          height: taken.height,
          frameId: taken.frameId,
          timestampMicros: taken.timestampUs,
          sequence: taken.sequence,
          // The addon hands each frame over in an ArrayBuffer of its own.
          data: taken.data as Uint8Array<ArrayBuffer>,
          metadata: taken.metadata as Uint8Array<ArrayBuffer>,
        }),
  );

export const audioFrame = (tracks: ReadonlyArray<Track>) => (taken: Binding.Audio) =>
  Effect.flatMap(receiving(tracks, taken.track, "audio"), (track) =>
    taken.sampleRate === 0 || taken.channels === 0 || taken.samples.length % taken.channels !== 0
      ? Effect.fail(protocol("native PCM format does not match its payload"))
      : Effect.succeed<AudioFrame>({
          _tag: "AudioFrame",
          track,
          sampleRate: taken.sampleRate,
          channels: taken.channels,
          sequence: taken.sequence,
          samples: taken.samples as Int16Array<ArrayBuffer>,
        }),
  );

/**
 * Per-reader bounds: a reader that falls this far behind fails with `Overflow`.
 * Video holds about a second, 24 frames at H3's 24 fps, so a consumer that
 * pauses briefly to encode keeps every frame; a second of H3's 1344 x 768 BGRA
 * is about 99 MB.
 */
const bounds = {
  video: {
    capacity: 24,
    maxBytes: 128 * 1024 * 1024,
    bytes: (frame: VideoFrame) =>
      frame.data.byteLength + frame.metadata.byteLength + frame.track.length * 2,
  },
  audio: {
    capacity: 128,
    maxBytes: 4 * 1024 * 1024,
    bytes: (frame: AudioFrame) => frame.samples.byteLength + frame.track.length * 2,
  },
};

const isPeerState = Schema.is(PeerState);

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

export const make = Effect.fnUntraced(function* (
  native: NativeHandle,
  shutdownTimeout: Duration.Duration = defaultShutdownTimeout,
): Effect.fn.Return<NativePeer, never, Scope.Scope> {
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
    yield* native.close;
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

  /** Classify a failed connection from the statistics read before the next event. */
  const classify = native.stats.pipe(
    Effect.map(connectionFailure),
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

  const deliver = (event: PeerEvent) =>
    Effect.flatMap(Ref.get(state), (current) => Effect.sync(() => current.emit?.(event)));

  const handle = (event: Binding.PeerEvent): Effect.Effect<void> => {
    switch (event.type) {
      case "state":
        if (event.state === "failed") return classify;
        return isPeerState(event.state)
          ? deliver({ type: "state", state: event.state })
          : fail(protocol(`native peer reported an unknown state: ${event.state}`));
      case "ice": {
        const found = event.candidate;
        return deliver(
          found === undefined
            ? { type: "ice" }
            : {
                type: "ice",
                candidate: {
                  candidate: found.candidate,
                  ...(found.sdpMid === undefined ? {} : { sdp_mid: found.sdpMid }),
                  ...(found.sdpMLineIndex === undefined
                    ? {}
                    : { sdp_mline_index: found.sdpMLineIndex }),
                },
              },
        );
      }
      case "error":
        return fail(nativeFailure()(event.failure));
      default:
        return deliver(event);
    }
  };

  /** Publish one taken item to its track's readers, unless the peer has closed. */
  const publish =
    <T, A extends { readonly track: string }>(
      frame: (tracks: ReadonlyArray<Track>) => (taken: T) => Effect.Effect<A, ReactorError>,
      select: (current: State) => ReadonlyMap<string, TrackFeed<A>>,
    ) =>
    (taken: T) =>
      Effect.gen(function* () {
        const current = yield* Ref.get(state);
        if (current.phase === "closed") return;
        const named = yield* frame(current.tracks)(taken);
        const feed = select(current).get(named.track);
        if (feed !== undefined) yield* feed.publish(named);
      });

  const feeds = <A>(
    tracks: ReadonlyArray<Track>,
    kind: Track["kind"],
    bound: Parameters<typeof trackFeed<A>>[0],
  ) =>
    Effect.map(
      Effect.forEach(
        tracks.filter((track) => track.direction === "recvonly" && track.kind === kind),
        (track) => Effect.map(trackFeed<A>(bound), (feed) => [track.name, feed] as const),
      ),
      (entries): ReadonlyMap<string, TrackFeed<A>> => new Map(entries),
    );

  const media =
    <A>(kind: Track["kind"], select: (current: State) => ReadonlyMap<string, TrackFeed<A>>) =>
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
    const snapshot = yield* native.pressure;
    const current = yield* Ref.get(state);
    let readerOverflows = 0n;
    for (const feed of [...current.video.values(), ...current.audio.values()])
      readerOverflows += yield* feed.overflows;
    return { ...snapshot, readerOverflows };
  });

  const prepare: Peer["prepare"] = (servers, tracks, emit) =>
    Effect.gen(function* () {
      const incoming = (kind: Track["kind"]) =>
        tracks.filter((track) => track.direction === "recvonly" && track.kind === kind).length;
      if (incoming("video") > 1 || incoming("audio") > 1)
        return yield* ReactorError.fromCode(
          "UnsupportedCapability",
          "native WebRTC supports at most one incoming video and one incoming audio track",
          { outcome: "not-submitted" },
        );
      const video = yield* feeds<VideoFrame>(tracks, "video", bounds.video);
      const audio = yield* feeds<AudioFrame>(tracks, "audio", bounds.audio);
      yield* Ref.update(state, (current) =>
        current.phase === "closed"
          ? current
          : { ...current, phase: "open" as const, emit, tracks, video, audio },
      );
      const prepared = yield* native.prepare(servers, tracks);
      // One reader per queue; a failed read is the peer's failure.
      yield* Effect.forkScoped(Stream.runForEach(native.events, handle).pipe(Effect.catch(fail)));
      if (video.size > 0)
        yield* Effect.forkScoped(
          Stream.runForEach(
            native.video,
            publish(videoFrame, (current) => current.video),
          ).pipe(Effect.catch(fail)),
        );
      if (audio.size > 0)
        yield* Effect.forkScoped(
          Stream.runForEach(
            native.audio,
            publish(audioFrame, (current) => current.audio),
          ).pipe(Effect.catch(fail)),
        );
      return prepared;
    });

  const join: Effect.Effect<void, ReactorError> = close.pipe(
    Effect.andThen(Effect.interruptible(native.shutdown)),
    // A failed shutdown is a Shutdown failure; the native failure stays in `detail`.
    Effect.mapError((error) =>
      error.reason._tag === "Shutdown"
        ? error
        : ReactorError.fromCode("Shutdown", error.message, { ...error.context, detail: error }),
    ),
    // Only the wait is bounded: past it the handle decides what is left behind.
    Effect.timeoutOrElse({
      duration: shutdownTimeout,
      orElse: () => Effect.flatMap(native.abandon, Effect.fail),
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
    prepare,
    answer: native.answer,
    send: native.send,
    direction: native.direction,
    maxBitrate: (name, bitsPerSecond) =>
      Number.isSafeInteger(bitsPerSecond) && bitsPerSecond >= 1 && bitsPerSecond <= 0x7fffffff
        ? native.maxBitrate(name, bitsPerSecond)
        : Effect.fail(unsupported("native max bitrate must be an integer in 1..2147483647")),
    stats: Effect.map(native.stats, (stats) => statsValue(stats) as ReadonlyArray<unknown>),
    close,
    shutdown: Ref.set(explicit, true).pipe(Effect.andThen(run)),
  };
});
