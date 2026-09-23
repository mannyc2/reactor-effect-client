import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Predicate from "effect/Predicate";
import * as Queue from "effect/Queue";
import * as Result from "effect/Result";
import * as Schema from "effect/Schema";
import type * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import { IceFailed, Mapping, ReactorError, TransportFailed } from "reactor-effect-client";
import type { Track } from "reactor-effect-client";
import { Observations, errorOf } from "reactor-effect-client/host";
import type {
  AudioFrame,
  Channel,
  IceCandidate,
  IceServer,
  MediaPressure,
  MediaTrack,
  Peer,
  PeerEvent,
  PeerState,
  Prepared,
  RawMedia,
  VideoFrame,
} from "reactor-effect-client/host";
import {
  encodeNativeJson,
  encodeNativeText,
  NativeBridge,
  NativeCall,
  nativeFailure,
  Ready,
  type NativeAudio,
  type NativePacket,
  type NativeVideo,
} from "./bridge.js";

const stateValues = new Set<PeerState>([
  "new",
  "connecting",
  "connected",
  "disconnected",
  "failed",
  "closed",
]);
const statsBigInts = new Set([
  "bytesSent",
  "bytesReceived",
  "packetsSent",
  "packetsReceived",
  "retransmittedPacketsSent",
  "priority",
]);
// Bound on reading statistics to classify a failed connection.
const CLASSIFY_TIMEOUT_MS = 2000;
/**
 * How long closing a peer waits for its native owner join. A healthy join under
 * real libwebrtc took 13-62 ms in the media load tests on Node and Bun, which
 * require it under 2 s; the default leaves five times that bound.
 */
export const defaultShutdownTimeout: Duration.Duration = Duration.seconds(10);

const validateNativeTracks = (tracks: readonly Track[]): void => {
  const incomingVideo = tracks.filter(
    (track) => track.direction === "recvonly" && track.kind === "video",
  ).length;
  const incomingAudio = tracks.filter(
    (track) => track.direction === "recvonly" && track.kind === "audio",
  ).length;
  if (incomingVideo > 1 || incomingAudio > 1) {
    throw ReactorError.fromCode(
      "UnsupportedCapability",
      "native WebRTC currently supports at most one incoming video and one incoming audio track because reactor-webrtc does not expose the remote track MID to its observer",
      { outcome: "not-submitted" },
    );
  }
};

const record = (value: unknown, what: string): Record<string, unknown> => {
  if (!Predicate.isObject(value))
    throw ReactorError.fromCode("Protocol", `native ${what} is not an object`);
  return value;
};
const string = (value: unknown, what: string): string => {
  if (typeof value !== "string")
    throw ReactorError.fromCode("Protocol", `native ${what} is not a string`);
  return value;
};
const integer = (value: unknown, what: string): number => {
  if (typeof value !== "number" || !Number.isSafeInteger(value) || value < 0)
    throw ReactorError.fromCode("Protocol", `native ${what} is not a nonnegative safe integer`);
  return value;
};
const bigint = (value: unknown, what: string): bigint => {
  if (typeof value !== "string" || !/^[0-9]+$/.test(value))
    throw ReactorError.fromCode("Protocol", `native ${what} is not an unsigned integer string`);
  return BigInt(value);
};

const nativeError = (cause: unknown, operation: string): ReactorError =>
  ReactorError.is(cause) ? cause : errorOf(cause, "Native", operation);

const decodeMappings = Schema.decodeUnknownResult(
  Schema.Array(Mapping).check(Schema.isMaxLength(64)),
);
/** The client's own `Mapping` schema, so the host and the session agree on every rule. */
const parseMapping = (value: unknown): readonly Mapping[] => {
  const decoded = decodeMappings(value);
  if (Result.isFailure(decoded))
    throw ReactorError.fromCode("Protocol", "native prepare returned an invalid mapping list", {
      detail: decoded.failure,
    });
  return Object.freeze(decoded.success.map((entry) => Object.freeze({ ...entry })));
};

const parsePrepared = (value: unknown): Prepared => {
  const response = record(value, "prepare response"),
    sdp = string(response.sdp, "prepare.sdp"),
    mapping = parseMapping(response.mapping);
  if (sdp.length === 0)
    throw ReactorError.fromCode("Protocol", "native prepare returned an empty SDP offer");
  return Object.freeze({ sdp, mapping });
};

const iceServers = (
  servers: readonly IceServer[],
): readonly {
  readonly urls: readonly string[];
  readonly username: string;
  readonly credential: string;
}[] =>
  servers.map((server) => {
    const urls = typeof server.urls === "string" ? [server.urls] : [...server.urls];
    if (urls.length === 0 || urls.some((url) => typeof url !== "string" || url.length === 0))
      throw ReactorError.fromCode("InvalidInput", "native ICE server has no usable URL", {
        outcome: "not-submitted",
      });
    if (server.credential !== undefined && typeof server.credential !== "string")
      throw ReactorError.fromCode(
        "UnsupportedCapability",
        "native ICE supports password credentials, not OAuth credential objects",
        { outcome: "not-submitted" },
      );
    return Object.freeze({
      urls: Object.freeze(urls),
      username: server.username ?? "",
      credential: server.credential ?? "",
    });
  });

const bridgeEffect = <A>(
  operation: string,
  body: () => Promise<A>,
): Effect.Effect<A, ReactorError> =>
  Effect.tryPromise({ try: body, catch: (cause) => nativeError(cause, operation) });

const parseEvent = (packet: NativePacket): PeerEvent => {
  const header = packet.header,
    type = string(header.type, "event.type");
  switch (type) {
    case "state": {
      const state = string(header.state, "state");
      if (!stateValues.has(state as PeerState))
        throw ReactorError.fromCode("Protocol", `native peer reported unknown state ${state}`);
      return { type: "state", state: state as PeerState };
    }
    case "channel": {
      const channel = string(header.channel, "channel");
      if (channel !== "control" && channel !== "data")
        throw ReactorError.fromCode("Protocol", "native peer reported an unknown data channel");
      if (typeof header.open !== "boolean")
        throw ReactorError.fromCode("Protocol", "native channel event omitted its open state");
      return { type: "channel", channel, open: header.open };
    }
    case "message": {
      const channel = string(header.channel, "message.channel");
      if (channel !== "control" && channel !== "data")
        throw ReactorError.fromCode("Protocol", "native peer message named an unknown channel");
      return { type: "message", channel, bytes: packet.payload };
    }
    case "ice": {
      if (header.candidate === undefined || header.candidate === null) return { type: "ice" };
      const value = record(header.candidate, "ICE candidate"),
        candidate: IceCandidate = { candidate: string(value.candidate, "candidate.candidate") };
      if (value.sdp_mid !== null && value.sdp_mid !== undefined)
        Object.assign(candidate, { sdp_mid: string(value.sdp_mid, "candidate.sdp_mid") });
      if (value.sdp_mline_index !== null && value.sdp_mline_index !== undefined)
        Object.assign(candidate, {
          sdp_mline_index: integer(value.sdp_mline_index, "candidate.sdp_mline_index"),
        });
      return { type: "ice", candidate };
    }
    case "track":
      return {
        type: "track",
        name: string(header.name, "track.name"),
        mid: string(header.mid, "track.mid"),
      };
    case "decoded": {
      const kind = string(header.kind, "decoded.kind");
      if (kind !== "video" && kind !== "audio")
        throw ReactorError.fromCode("Protocol", "native decoded event has unknown media kind");
      return {
        type: "decoded",
        kind,
        name: string(header.name, "decoded.name"),
        mid: string(header.mid, "decoded.mid"),
      };
    }
    case "error": {
      const status = header.status;
      if (typeof status !== "number" || !Number.isSafeInteger(status))
        throw ReactorError.fromCode("Protocol", "native error event omitted its failure class");
      return {
        type: "error",
        error: nativeFailure(
          status,
          string(header.message, "error.message"),
          (code) => `native peer failed (${code})`,
          {},
        ),
      };
    }
    default:
      throw ReactorError.fromCode("Protocol", `native peer emitted unknown event type ${type}`);
  }
};

/** The native track index is the position of the track in the prepare request. */
const receiving = (tracks: readonly Track[], index: number, kind: "video" | "audio"): string => {
  const track = tracks[index];
  if (track?.direction !== "recvonly" || track.kind !== kind)
    throw ReactorError.fromCode(
      "Protocol",
      `native ${kind} was delivered without its declared receive mapping`,
    );
  return track.name;
};

const videoFrame = (tracks: readonly Track[], taken: NativeVideo): VideoFrame => {
  const track = receiving(tracks, taken.track, "video");
  if (
    taken.width === 0 ||
    taken.height === 0 ||
    taken.data.byteLength !== taken.width * taken.height * 4
  )
    throw ReactorError.fromCode(
      "Protocol",
      "native BGRA frame dimensions do not match its payload",
    );
  return Object.freeze({
    _tag: "VideoFrame",
    track,
    format: "BGRA",
    width: taken.width,
    height: taken.height,
    frameId: taken.frameId,
    timestampMicros: taken.timestampMicros,
    data: taken.data,
    metadata: taken.metadata,
  });
};

const audioFrame = (tracks: readonly Track[], taken: NativeAudio): AudioFrame => {
  const track = receiving(tracks, taken.track, "audio");
  if (taken.sampleRate === 0 || taken.channels === 0 || taken.samples.length % taken.channels)
    throw ReactorError.fromCode("Protocol", "native PCM format does not match its payload");
  return Object.freeze({
    _tag: "AudioFrame",
    track,
    sampleRate: taken.sampleRate,
    channels: taken.channels,
    samples: taken.samples,
  });
};

/** The native snapshot's transport counters; the host adds its own reader overflows. */
const parseSnapshot = (value: unknown): Omit<MediaPressure, "readerOverflows"> => {
  const s = record(value, "media snapshot");
  if (typeof s.closed !== "boolean")
    throw ReactorError.fromCode("Protocol", "native media snapshot omitted closed state");
  return Object.freeze({
    closed: s.closed,
    queuedControl: integer(s.queuedControl, "snapshot.queuedControl"),
    queuedVideo: integer(s.queuedVideo, "snapshot.queuedVideo"),
    queuedAudio: integer(s.queuedAudio, "snapshot.queuedAudio"),
    queuedBytes: integer(s.queuedBytes, "snapshot.queuedBytes"),
    droppedVideo: bigint(s.droppedVideo, "snapshot.droppedVideo"),
    droppedAudio: bigint(s.droppedAudio, "snapshot.droppedAudio"),
    pendingRequests: integer(s.pendingRequests, "snapshot.pendingRequests"),
    deliveredVideo: bigint(s.deliveredVideo, "snapshot.deliveredVideo"),
    deliveredAudio: bigint(s.deliveredAudio, "snapshot.deliveredAudio"),
  });
};

const statsValue = (value: unknown): unknown => {
  if (Array.isArray(value)) return value.map(statsValue);
  if (value === null || typeof value !== "object") return value;
  const output: Record<string, unknown> = {};
  for (const [key, entry] of Object.entries(value as Record<string, unknown>))
    output[key] =
      statsBigInts.has(key) && typeof entry === "string" && /^-?[0-9]+$/.test(entry)
        ? BigInt(entry)
        : statsValue(entry);
  return output;
};

/**
 * Classify a failed connection from its candidate pairs. reactor-webrtc does
 * not report ICE connection state, but a pair that succeeded or was nominated
 * shows ICE worked and the DTLS/SCTP transport above it failed.
 */
const connectionFailure = (stats: readonly unknown[]): ReactorError => {
  const entries = stats.filter(Predicate.isObject);
  const pairs = entries.filter((entry) => entry.type === "candidate-pair");
  if (pairs.some((pair) => pair.state === "succeeded" || pair.nominated === true))
    return new ReactorError({
      reason: new TransportFailed({
        message: "native peer failed after ICE connectivity succeeded",
        pairs: pairs.length,
      }),
    });
  const candidateTypes = [
    ...new Set(
      entries
        .filter((entry) => entry.type === "local-candidate")
        .map((entry) => entry.candidateType)
        .filter((type): type is string => typeof type === "string"),
    ),
  ];
  return new ReactorError({
    reason: new IceFailed({
      message: "native peer found no working ICE candidate pair",
      pairs: pairs.length,
      candidateTypes,
    }),
  });
};

/** One synchronous, nonblocking take from a native queue, as a pump step. */
const drain = <A>(step: () => A): Effect.Effect<A, ReactorError> =>
  Effect.try({ try: step, catch: (cause) => nativeError(cause, "drain native WebRTC") });

export class NativePeer implements Peer {
  readonly nativeTracks = false;
  readonly rawMedia: RawMedia;
  private readonly bridge: NativeBridge;
  private readonly video = new Map<string, Observations<VideoFrame>>();
  private readonly audio = new Map<string, Observations<AudioFrame>>();
  private readonly incoming = new Map<string, "video" | "audio">();
  private tracks: readonly Track[] = [];
  // Readiness wakes one pump per native queue; a pending wake coalesces.
  private readonly wakeEvents = Effect.runSync(Queue.dropping<void>(1));
  private readonly wakeVideo = Effect.runSync(Queue.dropping<void>(1));
  private readonly wakeAudio = Effect.runSync(Queue.dropping<void>(1));
  private emit: ((event: PeerEvent) => void) | undefined;
  private closed = false;
  private failureEmitted = false;
  private failure: ReactorError | undefined;

  constructor(
    libraryPath: string,
    private readonly shutdownTimeout: Duration.Input = defaultShutdownTimeout,
  ) {
    this.bridge = new NativeBridge(libraryPath, (ready) => this.wake(ready));
    this.rawMedia = Object.freeze({
      video: (name: string) =>
        Stream.unwrap(
          Effect.try({
            try: () => {
              this.requireIncoming(name, "video");
              return this.videoFeed(name).stream({ capacity: 4, maxBytes: 64 * 1024 * 1024 });
            },
            catch: (cause) => nativeError(cause, "native video track"),
          }),
        ),
      audio: (name: string) =>
        Stream.unwrap(
          Effect.try({
            try: () => {
              this.requireIncoming(name, "audio");
              return this.audioFeed(name).stream({ capacity: 128, maxBytes: 4 * 1024 * 1024 });
            },
            catch: (cause) => nativeError(cause, "native audio track"),
          }),
        ),
      snapshot: Effect.suspend(() => this.mediaSnapshot()),
    });
  }

  private requireIncoming(name: string, kind: "video" | "audio"): void {
    if (this.incoming.get(name) !== kind)
      throw ReactorError.fromCode(
        "InvalidInput",
        "native media requires a declared receive track of the requested kind",
        { outcome: "not-submitted" },
      );
  }

  /** Readers failed for falling behind, across this peer's video and audio tracks. */
  private readerOverflows(): bigint {
    let overflows = 0n;
    for (const feed of this.video.values()) overflows += feed.overflowCount;
    for (const feed of this.audio.values()) overflows += feed.overflowCount;
    return overflows;
  }

  private videoFeed(name: string): Observations<VideoFrame> {
    let feed = this.video.get(name);
    if (feed === undefined) {
      feed = new Observations<VideoFrame>();
      this.video.set(name, feed);
      if (this.failure !== undefined) feed.fail(this.failure);
      else if (this.closed) feed.end();
    }
    return feed;
  }
  private audioFeed(name: string): Observations<AudioFrame> {
    let feed = this.audio.get(name);
    if (feed === undefined) {
      feed = new Observations<AudioFrame>();
      this.audio.set(name, feed);
      if (this.failure !== undefined) feed.fail(this.failure);
      else if (this.closed) feed.end();
    }
    return feed;
  }

  private fail(error: ReactorError): void {
    if (this.closed) return;
    this.failure = error;
    // Queue failure may resume subscribers synchronously. Publish the source
    // diagnostic first; any reentrant close still sees failure and fails every
    // feed, rather than converting a terminal error into successful EOF.
    try {
      if (!this.failureEmitted) {
        this.failureEmitted = true;
        this.emit?.({ type: "error", error });
      }
    } finally {
      this.close();
    }
  }

  /** The native readiness callback, on the JavaScript thread: it only wakes pumps. */
  private wake(ready: number): void {
    if (ready & Ready.Events) Queue.offerUnsafe(this.wakeEvents, undefined);
    if (ready & Ready.Video) Queue.offerUnsafe(this.wakeVideo, undefined);
    if (ready & Ready.Audio) Queue.offerUnsafe(this.wakeAudio, undefined);
  }

  /**
   * Drain one native queue with synchronous takes whenever readiness wakes
   * it. Yielding after every item lets observers run between emits, so a
   * backlog released by a stalled event loop reaches bounded observation
   * queues at their readers' pace rather than all at once. Yielding once per
   * batch of half a subscriber's capacity overflowed an observation queue on
   * Bun under CPU contention, so each item costs one event-loop turn.
   */
  private pump(
    wake: Queue.Queue<void>,
    step: Effect.Effect<boolean, ReactorError>,
  ): Effect.Effect<void> {
    const self = this;
    return Effect.gen(function* () {
      while (!self.closed) {
        yield* Queue.take(wake);
        while (!self.closed && (yield* step)) {
          yield* Effect.yieldNow;
        }
      }
    }).pipe(Effect.catch((error) => Effect.sync(() => self.fail(error))));
  }

  /**
   * Deliver one event; false once the queue is empty or closed. A failed
   * connection is classified here before the pump takes another event, so its
   * classification decides the error; later events describe the same teardown.
   */
  private get stepEvent(): Effect.Effect<boolean, ReactorError> {
    return drain(() => {
      const packet = this.bridge.takeEvent();
      if (packet === undefined || packet === null) return false;
      const event = parseEvent(packet);
      if (event.type === "state" && event.state === "failed") return "failed";
      if (event.type === "error") this.fail(event.error);
      else this.emit?.(event);
      return true;
    }).pipe(
      Effect.filterOrElse(
        (taken): taken is boolean => taken !== "failed",
        () => this.classify.pipe(Effect.as(true)),
      ),
    );
  }

  private get stepVideo(): Effect.Effect<boolean, ReactorError> {
    return drain(() => {
      const taken = this.bridge.takeVideo();
      if (taken === undefined || taken === null) return false;
      const frame = videoFrame(this.tracks, taken);
      this.videoFeed(frame.track).emit(
        frame,
        frame.data.byteLength + frame.metadata.byteLength + frame.track.length * 2,
      );
      return true;
    });
  }

  private get stepAudio(): Effect.Effect<boolean, ReactorError> {
    return drain(() => {
      const taken = this.bridge.takeAudio();
      if (taken === undefined || taken === null) return false;
      const frame = audioFrame(this.tracks, taken);
      this.audioFeed(frame.track).emit(frame, frame.samples.byteLength + frame.track.length * 2);
      return true;
    });
  }

  /**
   * Report a failed connection as IceFailed or TransportFailed rather than a
   * bare state. The statistics read runs in the events pump, so the connection
   * scope owns it and its deadline runs on the fiber's Clock.
   */
  private get classify(): Effect.Effect<void, ReactorError> {
    return bridgeEffect("classify native failure", () => this.bridge.call(NativeCall.Stats)).pipe(
      Effect.map((stats) =>
        Array.isArray(stats)
          ? connectionFailure(stats)
          : ReactorError.fromCode("Disconnected", "peer state failed"),
      ),
      Effect.timeoutOrElse({
        duration: CLASSIFY_TIMEOUT_MS,
        orElse: () =>
          Effect.fail(ReactorError.fromCode("Timeout", "native failure classification timed out")),
      }),
      Effect.catch((cause) =>
        Effect.succeed(
          ReactorError.fromCode("Disconnected", "peer state failed", { detail: cause }),
        ),
      ),
      Effect.flatMap((error) => drain(() => this.fail(error))),
    );
  }

  prepare(
    servers: readonly IceServer[],
    tracks: readonly Track[],
    emit: (event: PeerEvent) => void,
  ): Effect.Effect<Prepared, ReactorError, Scope.Scope> {
    const self = this;
    return Effect.gen(function* () {
      yield* Effect.try({
        try: () => validateNativeTracks(tracks),
        catch: (cause) => nativeError(cause, "validate native tracks"),
      });
      self.tracks = Object.freeze([...tracks]);
      for (const track of tracks)
        if (track.direction === "recvonly") self.incoming.set(track.name, track.kind);
      self.emit = emit;
      const request = yield* Effect.try({
        try: () => encodeNativeJson({ servers: iceServers(servers), tracks }),
        catch: (cause) => nativeError(cause, "encode native prepare"),
      });
      const prepared = yield* bridgeEffect("prepare native WebRTC", () =>
        self.bridge.call(NativeCall.Prepare, request),
      ).pipe(
        Effect.flatMap((value) =>
          Effect.try({
            try: () => parsePrepared(value),
            catch: (cause) => nativeError(cause, "decode native prepare"),
          }),
        ),
      );
      yield* Effect.forkScoped(self.pump(self.wakeEvents, self.stepEvent));
      yield* Effect.forkScoped(self.pump(self.wakeVideo, self.stepVideo));
      yield* Effect.forkScoped(self.pump(self.wakeAudio, self.stepAudio));
      return prepared;
    });
  }

  answer(sdp: string): Effect.Effect<void, ReactorError> {
    return bridgeEffect("apply native SDP answer", () =>
      this.bridge.call(NativeCall.Answer, encodeNativeText(sdp)),
    ).pipe(Effect.asVoid);
  }
  send(channel: Channel, bytes: Uint8Array<ArrayBuffer>): Effect.Effect<void, ReactorError> {
    return bridgeEffect(`send native ${channel}`, () => this.bridge.send(channel, bytes));
  }
  direction(name: string, active: boolean): Effect.Effect<void, ReactorError> {
    return bridgeEffect("set native transceiver direction", () =>
      this.bridge.call(NativeCall.Direction, encodeNativeJson({ name, active })),
    ).pipe(Effect.asVoid);
  }
  maxBitrate(name: string, bitsPerSecond: number): Effect.Effect<void, ReactorError> {
    if (!Number.isSafeInteger(bitsPerSecond) || bitsPerSecond < 1 || bitsPerSecond > 0x7fffffff)
      return Effect.fail(
        ReactorError.fromCode(
          "InvalidInput",
          "native max bitrate must be an integer in 1..2147483647",
          { outcome: "not-submitted" },
        ),
      );
    return bridgeEffect("set native sender bitrate", () =>
      this.bridge.call(NativeCall.MaxBitrate, encodeNativeJson({ name, bitsPerSecond })),
    ).pipe(Effect.asVoid);
  }
  get stats(): Effect.Effect<readonly unknown[], ReactorError> {
    return bridgeEffect("native WebRTC statistics", () => this.bridge.call(NativeCall.Stats)).pipe(
      Effect.flatMap((value) =>
        Effect.try({
          try: () => {
            if (!Array.isArray(value))
              throw ReactorError.fromCode("Protocol", "native stats response is not an array");
            return Object.freeze(statsValue(value) as readonly unknown[]);
          },
          catch: (cause) => nativeError(cause, "decode native stats"),
        }),
      ),
    );
  }
  private mediaSnapshot(): Effect.Effect<MediaPressure, ReactorError> {
    return bridgeEffect("native media snapshot", () =>
      this.bridge.call(NativeCall.MediaSnapshot),
    ).pipe(
      Effect.flatMap((value) =>
        Effect.try({
          try: (): MediaPressure =>
            Object.freeze({ ...parseSnapshot(value), readerOverflows: this.readerOverflows() }),
          catch: (cause) => nativeError(cause, "decode native media snapshot"),
        }),
      ),
    );
  }

  lease(): MediaTrack {
    throw ReactorError.fromCode(
      "UnsupportedCapability",
      "native WebRTC exposes owned decoded samples, not browser MediaStreamTrack leases",
      { outcome: "not-submitted" },
    );
  }
  release(): void {
    /* lease never succeeds on this host. */
  }
  replace(): Effect.Effect<void, ReactorError> {
    return Effect.fail(
      ReactorError.fromCode(
        "UnsupportedCapability",
        "native WebRTC does not accept browser MediaStreamTrack publication",
        { outcome: "not-submitted" },
      ),
    );
  }

  close(): void {
    if (this.closed) return;
    this.closed = true;
    this.emit = undefined;
    this.bridge.close();
    // Let waiting pumps observe the close and exit.
    for (const wake of [this.wakeEvents, this.wakeVideo, this.wakeAudio])
      Queue.offerUnsafe(wake, undefined);
    for (const feed of this.video.values()) {
      if (this.failure === undefined) feed.end();
      else feed.fail(this.failure);
    }
    for (const feed of this.audio.values()) {
      if (this.failure === undefined) feed.end();
      else feed.fail(this.failure);
    }
  }

  /**
   * Close, then wait for the native owner join. Only the wait is bounded: the
   * deadline races the interruptible wait inside the uninterruptible region, so
   * expiry stops waiting while the join keeps the handle and callback, and the
   * bridge is retained until the join completes.
   */
  get shutdown(): Effect.Effect<void, ReactorError> {
    return bridgeEffect("shutdown native WebRTC", () => {
      this.close();
      return this.bridge.shutdown();
    }).pipe(
      // A failed shutdown is a Shutdown failure; the native failure it came
      // from stays in `detail` for inspection.
      Effect.mapError((error) =>
        error.reason._tag === "Shutdown"
          ? error
          : ReactorError.fromCode("Shutdown", error.message, { ...error.context, detail: error }),
      ),
      Effect.timeoutOrElse({
        duration: this.shutdownTimeout,
        orElse: () =>
          Effect.suspend(() => {
            this.bridge.retain();
            return Effect.fail(
              ReactorError.fromCode(
                "Shutdown",
                "native owner join exceeded its deadline; handle retained",
                { operation: "shutdown native WebRTC" },
              ),
            );
          }),
      }),
      Effect.uninterruptible,
    );
  }
}

/** Internal test surface. This module is not a package export. */
export const nativePeerTesting = Object.freeze({
  parsePrepared,
  parseEvent,
  videoFrame,
  audioFrame,
  parseSnapshot,
  statsValue,
  validateNativeTracks,
  connectionFailure,
});
