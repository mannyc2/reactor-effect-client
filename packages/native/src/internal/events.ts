/**
 * What native code reports, as Schemas: peer events, the prepared offer and the
 * transport's pressure. The in-process peer decodes them from the bridge, and
 * the isolated host carries the same models between processes.
 */
import * as Effect from "effect/Effect";
import * as Predicate from "effect/Predicate";
import * as Schema from "effect/Schema";
import { Mapping, Track } from "reactor-effect-client/Coordinator";
import type { IceServer } from "reactor-effect-client/Coordinator";
import type { AudioFrame, VideoFrame } from "reactor-effect-client/Media";
import { Channel, PeerState } from "reactor-effect-client/Peer";
import type { PeerEvent, Prepared } from "reactor-effect-client/Peer";
import { IceFailed, ReactorError, TransportFailed } from "reactor-effect-client/ReactorError";
import { nativeFailure } from "./bridge.js";
import type { NativeAudio, NativePacket, NativeVideo } from "./bridge.js";

const protocol = (message: string, detail?: unknown): ReactorError =>
  ReactorError.fromCode("Protocol", message, detail === undefined ? {} : { detail });

const Count = Schema.Int.check(Schema.isGreaterThanOrEqualTo(0));
const Total = Schema.BigInt.check(Schema.isGreaterThanOrEqualToBigInt(0n));

/** Transport counters of one connection generation, before the host adds its readers' overflows. */
export const Pressure = Schema.Struct({
  closed: Schema.Boolean,
  queuedControl: Count,
  queuedVideo: Count,
  queuedAudio: Count,
  queuedBytes: Count,
  droppedVideo: Total,
  droppedAudio: Total,
  pendingRequests: Count,
  deliveredVideo: Total,
  deliveredAudio: Total,
});
export type Pressure = typeof Pressure.Type;

const Candidate = Schema.Struct({
  candidate: Schema.String,
  sdp_mid: Schema.String.pipe(Schema.NullOr, Schema.optionalKey),
  sdp_mline_index: Count.pipe(Schema.NullOr, Schema.optionalKey),
});

/** An event header as the native notifier queues it. */
const Header = Schema.Union([
  Schema.Struct({ type: Schema.Literal("state"), state: PeerState }),
  Schema.Struct({ type: Schema.Literal("channel"), channel: Channel, open: Schema.Boolean }),
  Schema.Struct({ type: Schema.Literal("message"), channel: Channel }),
  Schema.Struct({
    type: Schema.Literal("ice"),
    candidate: Candidate.pipe(Schema.NullOr, Schema.optionalKey),
  }),
  Schema.Struct({ type: Schema.Literal("track"), name: Schema.String, mid: Schema.String }),
  Schema.Struct({
    type: Schema.Literal("decoded"),
    kind: Schema.Literals(["video", "audio"]),
    name: Schema.String,
    mid: Schema.String,
  }),
  Schema.Struct({ type: Schema.Literal("error"), status: Schema.Int, message: Schema.String }),
]);

const decodeHeader = Schema.decodeUnknownEffect(Schema.fromJsonString(Header));

/** A peer event, or `Failed` for a failed connection the host classifies from its statistics. */
export const decodeEvent = (
  packet: NativePacket,
): Effect.Effect<PeerEvent | "Failed", ReactorError> =>
  decodeHeader(packet.header).pipe(
    Effect.mapError((cause) => protocol("native peer event is malformed", cause)),
    Effect.map((header): PeerEvent | "Failed" => {
      switch (header.type) {
        case "state":
          return header.state === "failed" ? "Failed" : header;
        case "message":
          return { type: "message", channel: header.channel, bytes: packet.payload };
        case "ice": {
          const candidate = header.candidate;
          if (candidate === undefined || candidate === null) return { type: "ice" };
          return {
            type: "ice",
            candidate: {
              candidate: candidate.candidate,
              ...(candidate.sdp_mid === undefined || candidate.sdp_mid === null
                ? {}
                : { sdp_mid: candidate.sdp_mid }),
              ...(candidate.sdp_mline_index === undefined || candidate.sdp_mline_index === null
                ? {}
                : { sdp_mline_index: candidate.sdp_mline_index }),
            },
          };
        }
        case "error":
          return {
            type: "error",
            error: nativeFailure({
              status: header.status,
              backendText: header.message,
              message: (code) => `native peer failed (${code})`,
              context: {},
            }),
          };
        default:
          return header;
      }
    }),
  );

const PreparedReply = Schema.Struct({
  sdp: Schema.NonEmptyString,
  mapping: Schema.Array(Mapping).check(Schema.isMaxLength(64)),
});

export const decodePrepared = (reply: unknown): Effect.Effect<Prepared, ReactorError> =>
  Schema.decodeUnknownEffect(PreparedReply)(reply).pipe(
    Effect.mapError((cause) => protocol("native prepare returned an invalid offer", cause)),
  );

/** The native snapshot, whose 64-bit counters arrive as decimal strings. */
export const decodePressure = (reply: unknown): Effect.Effect<Pressure, ReactorError> =>
  Schema.decodeUnknownEffect(Schema.toCodecJson(Pressure))(reply).pipe(
    Effect.mapError((cause) => protocol("native media snapshot is malformed", cause)),
  );

/** What a prepare sends native code: password ICE servers and the declared tracks. */
export const PrepareInput = Schema.Struct({
  servers: Schema.Array(
    Schema.Struct({
      urls: Schema.Array(Schema.String),
      username: Schema.String,
      credential: Schema.String,
    }),
  ),
  tracks: Schema.Array(Track),
});
export type PrepareInput = typeof PrepareInput.Type;

const encodePrepare = Schema.encodeEffect(Schema.fromJsonString(PrepareInput));
const encodeJson = Schema.encodeEffect(Schema.fromJsonString(Schema.Json));

const notJson = (cause: unknown): ReactorError =>
  ReactorError.fromCode("InvalidInput", "native request is not JSON", {
    outcome: "not-submitted",
    detail: cause,
  });

/** JSON request text, which native code reads as UTF-8. */
export const jsonRequest = (value: Schema.Json): Effect.Effect<string, ReactorError> =>
  encodeJson(value).pipe(Effect.mapError(notJson));

export const prepareRequest = (input: PrepareInput): Effect.Effect<string, ReactorError> =>
  encodePrepare(input).pipe(Effect.mapError(notJson));

/**
 * The prepare both hosts send. The native peer accepts at most one incoming
 * video and one incoming audio track: pinned reactor-webrtc gives its
 * remote-track callback no MID to tell two of the same kind apart.
 */
export const prepareInput = ({
  servers,
  tracks,
}: {
  readonly servers: ReadonlyArray<IceServer>;
  readonly tracks: ReadonlyArray<Track>;
}): Effect.Effect<PrepareInput, ReactorError> => {
  const incoming = (kind: Track["kind"]) =>
    tracks.filter((track) => track.direction === "recvonly" && track.kind === kind).length;
  if (incoming("video") > 1 || incoming("audio") > 1)
    return Effect.fail(
      ReactorError.fromCode(
        "UnsupportedCapability",
        "native WebRTC supports at most one incoming video and one incoming audio track",
        { outcome: "not-submitted" },
      ),
    );
  if (servers.some((server) => server.urls.length === 0))
    return Effect.fail(
      ReactorError.fromCode("InvalidInput", "native ICE server has no URL", {
        outcome: "not-submitted",
      }),
    );
  return Effect.succeed({
    servers: servers.map((server) => ({
      urls: server.urls,
      username: server.username ?? "",
      credential: server.credential ?? "",
    })),
    tracks,
  });
};

/** The native track index is the track's position in the prepare request. */
const receiving = (
  tracks: ReadonlyArray<Track>,
  index: number,
  kind: Track["kind"],
): Effect.Effect<string, ReactorError> => {
  const track = tracks[index];
  return track?.direction === "recvonly" && track.kind === kind
    ? Effect.succeed(track.name)
    : Effect.fail(protocol(`native ${kind} was delivered without its declared receive mapping`));
};

/** A taken frame as a session's `VideoFrame`, named by the prepare request's `tracks`. */
export const videoFrame =
  (tracks: ReadonlyArray<Track>) =>
  (taken: NativeVideo): Effect.Effect<VideoFrame, ReactorError> =>
    Effect.flatMap(receiving(tracks, taken.track, "video"), (track) =>
      taken.width === 0 ||
      taken.height === 0 ||
      taken.data.byteLength !== taken.width * taken.height * 4
        ? Effect.fail(protocol("native BGRA frame dimensions do not match its payload"))
        : Effect.succeed({
            _tag: "VideoFrame",
            track,
            format: "BGRA",
            width: taken.width,
            height: taken.height,
            frameId: taken.frameId,
            timestampMicros: taken.timestampMicros,
            sequence: taken.sequence,
            data: taken.data,
            metadata: taken.metadata,
          }),
    );

/** A taken block as a session's `AudioFrame`, named by the prepare request's `tracks`. */
export const audioFrame =
  (tracks: ReadonlyArray<Track>) =>
  (taken: NativeAudio): Effect.Effect<AudioFrame, ReactorError> =>
    Effect.flatMap(receiving(tracks, taken.track, "audio"), (track) =>
      taken.sampleRate === 0 || taken.channels === 0 || taken.samples.length % taken.channels !== 0
        ? Effect.fail(protocol("native PCM format does not match its payload"))
        : Effect.succeed({
            _tag: "AudioFrame",
            track,
            sampleRate: taken.sampleRate,
            channels: taken.channels,
            sequence: taken.sequence,
            samples: taken.samples,
          }),
    );

const statsBigInts = new Set([
  "bytesSent",
  "bytesReceived",
  "packetsSent",
  "packetsReceived",
  "retransmittedPacketsSent",
  "priority",
]);

/** Statistics with their 64-bit counters, which native code sends as strings, as bigints. */
export const statsValue = (value: unknown): unknown => {
  if (Array.isArray(value)) return value.map(statsValue);
  if (!Predicate.isObject(value)) return value;
  return Object.fromEntries(
    Object.entries(value).map(([key, entry]) => [
      key,
      statsBigInts.has(key) && Predicate.isString(entry) && /^-?[0-9]+$/.test(entry)
        ? BigInt(entry)
        : statsValue(entry),
    ]),
  );
};

/**
 * A failed connection, told apart by its candidate pairs: a pair that
 * succeeded or was nominated shows ICE worked and the transport above it failed.
 */
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
  const candidateTypes = [
    ...new Set(
      entries
        .filter((entry) => entry.type === "local-candidate")
        .map((entry) => entry.candidateType)
        .filter(Predicate.isString),
    ),
  ];
  return ReactorError.make({
    reason: IceFailed.make({
      message: "native peer found no working ICE candidate pair",
      pairs: pairs.length,
      candidateTypes,
    }),
  });
};
