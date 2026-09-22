import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import type * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import type { AudioFrame, RawMedia, MediaPressure, VideoFrame } from "../../session/media.js";
import type { IceCandidate, Mapping, Track } from "../../contract.js";
import { errorOf, ReactorError, ErrorCode } from "../../errors.js";
import { Observations } from "../../observation.js";
import type { Channel, Peer, PeerEvent, Prepared } from "../../PeerTypes.js";
import {
  encodeNativeJson,
  encodeNativeText,
  NativeBridge,
  NativeCall,
  type NativePacket,
  type NativePoll,
} from "./bridge.js";

const stateValues = new Set<RTCPeerConnectionState>([
  "new",
  "connecting",
  "connected",
  "disconnected",
  "failed",
  "closed",
]);
const isErrorCode = Schema.is(ErrorCode);
const statsBigInts = new Set([
  "bytesSent",
  "bytesReceived",
  "packetsSent",
  "packetsReceived",
  "retransmittedPacketsSent",
  "priority",
]);

const validateNativeTracks = (tracks: readonly Track[]): void => {
  const incomingVideo = tracks.filter(
    (track) => track.direction === "recvonly" && track.kind === "video",
  ).length;
  const incomingAudio = tracks.filter(
    (track) => track.direction === "recvonly" && track.kind === "audio",
  ).length;
  if (incomingVideo > 1 || incomingAudio > 1) {
    throw new ReactorError(
      "UnsupportedCapability",
      "native WebRTC currently supports at most one incoming video and one incoming audio track because reactor-webrtc does not expose the remote track MID to its observer",
      { outcome: "not-submitted" },
    );
  }
};

const record = (value: unknown, what: string): Record<string, unknown> => {
  if (value === null || typeof value !== "object" || Array.isArray(value))
    throw new ReactorError("Protocol", `native ${what} is not an object`);
  return value as Record<string, unknown>;
};
const string = (value: unknown, what: string): string => {
  if (typeof value !== "string")
    throw new ReactorError("Protocol", `native ${what} is not a string`);
  return value;
};
const integer = (value: unknown, what: string): number => {
  if (typeof value !== "number" || !Number.isSafeInteger(value) || value < 0)
    throw new ReactorError("Protocol", `native ${what} is not a nonnegative safe integer`);
  return value;
};
const bigint = (value: unknown, what: string): bigint => {
  if (typeof value !== "string" || !/^[0-9]+$/.test(value))
    throw new ReactorError("Protocol", `native ${what} is not an unsigned integer string`);
  return BigInt(value);
};

const nativeError = (cause: unknown, operation: string): ReactorError =>
  cause instanceof ReactorError ? cause : errorOf(cause, "Native", operation);

const parseMapping = (value: unknown): readonly Mapping[] => {
  if (!Array.isArray(value) || value.length > 64)
    throw new ReactorError("Protocol", "native prepare returned an invalid mapping list");
  return Object.freeze(
    value.map((item): Mapping => {
      const entry = record(item, "mapping");
      const name = string(entry.name, "mapping.name"),
        kind = string(entry.kind, "mapping.kind"),
        direction = string(entry.direction, "mapping.direction"),
        mid = string(entry.mid, "mapping.mid");
      if (kind !== "audio" && kind !== "video")
        throw new ReactorError("Protocol", "native mapping has an unknown track kind");
      if (direction !== "recvonly" && direction !== "sendonly")
        throw new ReactorError("Protocol", "native mapping has an unknown direction");
      return Object.freeze({ name, kind, direction, mid });
    }),
  );
};

const parsePrepared = (value: unknown): Prepared => {
  const response = record(value, "prepare response"),
    sdp = string(response.sdp, "prepare.sdp"),
    mapping = parseMapping(response.mapping);
  if (sdp.length === 0)
    throw new ReactorError("Protocol", "native prepare returned an empty SDP offer");
  return Object.freeze({ sdp, mapping });
};

const iceServers = (
  servers: readonly RTCIceServer[],
): readonly {
  readonly urls: readonly string[];
  readonly username: string;
  readonly credential: string;
}[] =>
  servers.map((server) => {
    const urls = typeof server.urls === "string" ? [server.urls] : [...server.urls];
    if (urls.length === 0 || urls.some((url) => typeof url !== "string" || url.length === 0))
      throw new ReactorError("InvalidInput", "native ICE server has no usable URL", {
        outcome: "not-submitted",
      });
    if (server.credential !== undefined && typeof server.credential !== "string")
      throw new ReactorError(
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
      if (!stateValues.has(state as RTCPeerConnectionState))
        throw new ReactorError("Protocol", `native peer reported unknown state ${state}`);
      return { type: "state", state: state as RTCPeerConnectionState };
    }
    case "channel": {
      const channel = string(header.channel, "channel");
      if (channel !== "control" && channel !== "data")
        throw new ReactorError("Protocol", "native peer reported an unknown data channel");
      if (typeof header.open !== "boolean")
        throw new ReactorError("Protocol", "native channel event omitted its open state");
      return { type: "channel", channel, open: header.open };
    }
    case "message": {
      const channel = string(header.channel, "message.channel");
      if (channel !== "control" && channel !== "data")
        throw new ReactorError("Protocol", "native peer message named an unknown channel");
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
        throw new ReactorError("Protocol", "native decoded event has unknown media kind");
      return {
        type: "decoded",
        kind,
        name: string(header.name, "decoded.name"),
        mid: string(header.mid, "decoded.mid"),
      };
    }
    case "error": {
      const rawCode = string(header.code, "error.code"),
        code = isErrorCode(rawCode) ? rawCode : "Native";
      string(header.message, "error.message");
      return {
        type: "error",
        error: new ReactorError(code, `native peer failed (${code})`, { detail: header }),
      };
    }
    default:
      throw new ReactorError("Protocol", `native peer emitted unknown event type ${type}`);
  }
};

const parseVideo = (packet: NativePacket): VideoFrame => {
  const h = packet.header;
  if (h.type !== "video" || h.format !== "BGRA")
    throw new ReactorError("Protocol", "native video packet has an unsupported format");
  const width = integer(h.width, "video.width"),
    height = integer(h.height, "video.height"),
    dataLength = integer(h.dataLength, "video.dataLength"),
    metadataLength = integer(h.metadataLength, "video.metadataLength");
  if (
    width === 0 ||
    height === 0 ||
    dataLength !== width * height * 4 ||
    dataLength + metadataLength !== packet.payload.length
  )
    throw new ReactorError("Protocol", "native BGRA frame dimensions do not match its payload");
  return Object.freeze({
    _tag: "VideoFrame",
    track: string(h.track, "video.track"),
    width,
    height,
    frameId: bigint(h.frameId, "video.frameId"),
    timestampMicros: bigint(h.timestampMicros, "video.timestampMicros"),
    data: Uint8Array.from(packet.payload.subarray(0, dataLength)),
    metadata: Uint8Array.from(packet.payload.subarray(dataLength)),
  });
};

const parseAudio = (packet: NativePacket): AudioFrame => {
  const h = packet.header;
  if (h.type !== "audio" || h.format !== "s16le")
    throw new ReactorError("Protocol", "native audio packet has an unsupported format");
  const sampleRate = integer(h.sampleRate, "audio.sampleRate"),
    channels = integer(h.channels, "audio.channels"),
    samples = integer(h.samples, "audio.samples");
  if (
    sampleRate === 0 ||
    channels === 0 ||
    samples % channels !== 0 ||
    packet.payload.length !== samples * 2
  )
    throw new ReactorError("Protocol", "native PCM format does not match its payload");
  const view = new DataView(
      packet.payload.buffer,
      packet.payload.byteOffset,
      packet.payload.byteLength,
    ),
    pcm = new Int16Array(samples);
  for (let index = 0; index < samples; index++) pcm[index] = view.getInt16(index * 2, true);
  return Object.freeze({
    _tag: "AudioFrame",
    track: string(h.track, "audio.track"),
    sampleRate,
    channels,
    samples: pcm,
  });
};

const parseSnapshot = (value: unknown): MediaPressure => {
  const s = record(value, "media snapshot");
  if (typeof s.closed !== "boolean")
    throw new ReactorError("Protocol", "native media snapshot omitted closed state");
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

const pollPacket = (
  poll: () => Promise<NativePoll>,
): Effect.Effect<NativePacket | undefined | null, ReactorError> =>
  bridgeEffect("poll native WebRTC", async () => {
    const result = await poll();
    return result._tag === "Packet" ? result.packet : result._tag === "Closed" ? null : undefined;
  });

export class NativePeer implements Peer {
  readonly nativeTracks = false;
  readonly rawMedia: RawMedia;
  private readonly bridge: NativeBridge;
  private readonly video = new Map<string, Observations<VideoFrame>>();
  private readonly audio = new Map<string, Observations<AudioFrame>>();
  private readonly incoming = new Map<string, "video" | "audio">();
  private emit: ((event: PeerEvent) => void) | undefined;
  private closed = false;
  private failureEmitted = false;
  private failure: ReactorError | undefined;

  constructor(libraryPath: string) {
    this.bridge = new NativeBridge(libraryPath);
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
      throw new ReactorError(
        "InvalidInput",
        "native media requires a declared receive track of the requested kind",
        { outcome: "not-submitted" },
      );
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

  private pumpEvents(): Effect.Effect<void> {
    const self = this;
    return Effect.gen(function* () {
      while (!self.closed) {
        const packet = yield* pollPacket(() => self.bridge.pollEvent());
        if (self.closed) return;
        if (packet === null) return;
        if (packet === undefined) continue;
        const event = yield* Effect.try({
          try: () => parseEvent(packet),
          catch: (cause) => nativeError(cause, "decode native event"),
        });
        if (event.type === "error") {
          self.fail(event.error);
          return;
        }
        self.emit?.(event);
      }
    }).pipe(Effect.catch((error) => Effect.sync(() => self.fail(error))));
  }

  private pumpVideo(): Effect.Effect<void> {
    const self = this;
    return Effect.gen(function* () {
      while (!self.closed) {
        const packet = yield* pollPacket(() => self.bridge.pollVideo());
        if (self.closed) return;
        if (packet === null) return;
        if (packet === undefined) continue;
        const frame = yield* Effect.try({
          try: () => {
            const frame = parseVideo(packet);
            if (self.incoming.get(frame.track) !== "video")
              throw new ReactorError(
                "Protocol",
                "native video was delivered without its declared receive mapping",
              );
            return frame;
          },
          catch: (cause) => nativeError(cause, "decode native video"),
        });
        self
          .videoFeed(frame.track)
          .emit(frame, frame.data.byteLength + frame.metadata.byteLength + frame.track.length * 2);
      }
    }).pipe(Effect.catch((error) => Effect.sync(() => self.fail(error))));
  }

  private pumpAudio(): Effect.Effect<void> {
    const self = this;
    return Effect.gen(function* () {
      while (!self.closed) {
        const packet = yield* pollPacket(() => self.bridge.pollAudio());
        if (self.closed) return;
        if (packet === null) return;
        if (packet === undefined) continue;
        const frame = yield* Effect.try({
          try: () => {
            const frame = parseAudio(packet);
            if (self.incoming.get(frame.track) !== "audio")
              throw new ReactorError(
                "Protocol",
                "native audio was delivered without its declared receive mapping",
              );
            return frame;
          },
          catch: (cause) => nativeError(cause, "decode native audio"),
        });
        self.audioFeed(frame.track).emit(frame, frame.samples.byteLength + frame.track.length * 2);
      }
    }).pipe(Effect.catch((error) => Effect.sync(() => self.fail(error))));
  }

  prepare(
    servers: readonly RTCIceServer[],
    tracks: readonly Track[],
    emit: (event: PeerEvent) => void,
  ): Effect.Effect<Prepared, ReactorError, Scope.Scope> {
    const self = this;
    return Effect.gen(function* () {
      yield* Effect.try({
        try: () => validateNativeTracks(tracks),
        catch: (cause) => nativeError(cause, "validate native tracks"),
      });
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
      yield* Effect.forkScoped(self.pumpEvents());
      yield* Effect.forkScoped(self.pumpVideo());
      yield* Effect.forkScoped(self.pumpAudio());
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
        new ReactorError("InvalidInput", "native max bitrate must be an integer in 1..2147483647", {
          outcome: "not-submitted",
        }),
      );
    return bridgeEffect("set native sender bitrate", () =>
      this.bridge.call(NativeCall.MaxBitrate, encodeNativeJson({ name, bitsPerSecond })),
    ).pipe(Effect.asVoid);
  }
  stats(): Effect.Effect<readonly unknown[], ReactorError> {
    return bridgeEffect("native WebRTC statistics", () => this.bridge.call(NativeCall.Stats)).pipe(
      Effect.flatMap((value) =>
        Effect.try({
          try: () => {
            if (!Array.isArray(value))
              throw new ReactorError("Protocol", "native stats response is not an array");
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
          try: () => parseSnapshot(value),
          catch: (cause) => nativeError(cause, "decode native media snapshot"),
        }),
      ),
    );
  }

  lease(): MediaStreamTrack {
    throw new ReactorError(
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
      new ReactorError(
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
    for (const feed of this.video.values()) {
      if (this.failure === undefined) feed.end();
      else feed.fail(this.failure);
    }
    for (const feed of this.audio.values()) {
      if (this.failure === undefined) feed.end();
      else feed.fail(this.failure);
    }
  }

  shutdown(): Effect.Effect<void, ReactorError> {
    return bridgeEffect("shutdown native WebRTC", () => {
      this.close();
      return this.bridge.shutdown();
    }).pipe(
      Effect.mapError((error) =>
        error.code === "Shutdown"
          ? error
          : new ReactorError("Shutdown", error.message, error.context),
      ),
      Effect.uninterruptible,
    );
  }
}

/** Internal test surface. This module is not a package export. */
export const nativePeerTesting = Object.freeze({
  parsePrepared,
  parseEvent,
  parseVideo,
  parseAudio,
  parseSnapshot,
  statsValue,
  validateNativeTracks,
});
