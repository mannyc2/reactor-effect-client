/**
 * The private contract between an isolated native peer and its child process:
 * one Effect RPC group mirroring the addon's `NativePeer`, over the child's
 * IPC channel. The child relays the addon; the parent runs the same peer over
 * it that runs in process.
 *
 * The worker protocols encode every message with Schema's JSON codec, and
 * every value the addon produces crosses as its Schema. Typed arrays pass
 * through that codec as they are, for structured clone to carry; bigints
 * cross as decimal strings. Encoding and decoding a 1344x768 BGRA frame
 * through its Schema took about 1.6 us on Node and 1 us on Bun, against
 * 0.25 us for passing it on unchecked: nothing beside the copy the IPC hop
 * already makes of its 4 MB, so frames are checked like everything else.
 */
import * as Schema from "effect/Schema";
import * as Rpc from "effect/rpc/Rpc";
import * as RpcGroup from "effect/rpc/RpcGroup";
import { IceServer, Track } from "reactor-effect-client/Peer";
import { DataChannel, Prepared } from "reactor-effect-client/Peer";
import type * as Binding from "../binding.js";

/** Bytes, carried as the typed array they are. */
const Bytes = Schema.instanceOf(globalThis.Uint8Array<ArrayBufferLike>, {
  expected: "Uint8Array",
  toCodecJson: () => undefined,
});

/** PCM samples, carried as the typed array they are. */
const Samples = Schema.instanceOf(globalThis.Int16Array<ArrayBufferLike>, {
  expected: "Int16Array",
  toCodecJson: () => undefined,
});

/** The addon's failure: its class and its private diagnostic text. */
export const Failure = Schema.Struct({
  class: Schema.Literals([
    "Closed",
    "InvalidInput",
    "Native",
    "Overflow",
    "Protocol",
    "SdpRejected",
    "ChannelClosed",
  ] satisfies ReadonlyArray<Binding.FailureClass>),
  message: Schema.String,
});

/** Why the child could not open its peer, as the parent reports it. */
export const OpenFailure = Schema.Struct({
  code: Schema.Literals(["Native", "UnsupportedHost"]),
  message: Schema.String,
});

/** An RTCStatsReport-shaped array; counters beyond double precision are decimal strings. */
const Stats = Schema.Array(Schema.Record(Schema.String, Schema.Unknown));

/** What each of the addon's queues dropped, delivered and still holds. */
const Pressure = Schema.Struct({
  closed: Schema.Boolean,
  pendingRequests: Schema.Int,
  queuedControl: Schema.Int,
  queuedVideo: Schema.Int,
  queuedAudio: Schema.Int,
  queuedBytes: Schema.Int,
  droppedVideo: Schema.BigInt,
  droppedAudio: Schema.BigInt,
  deliveredVideo: Schema.BigInt,
  deliveredAudio: Schema.BigInt,
});

/** A transport event, as the addon queues it. */
const PeerEvent = Schema.Union([
  Schema.Struct({ type: Schema.Literal("state"), state: Schema.String }),
  Schema.Struct({
    type: Schema.Literal("ice"),
    candidate: Schema.optionalKey(
      Schema.Struct({
        candidate: Schema.String,
        sdpMid: Schema.optionalKey(Schema.String),
        sdpMLineIndex: Schema.optionalKey(Schema.Int),
      }),
    ),
  }),
  Schema.Struct({ type: Schema.Literal("channel"), channel: DataChannel, open: Schema.Boolean }),
  Schema.Struct({ type: Schema.Literal("message"), channel: DataChannel, bytes: Bytes }),
  Schema.Struct({ type: Schema.Literal("track"), name: Schema.String, mid: Schema.String }),
  Schema.Struct({
    type: Schema.Literal("decoded"),
    kind: Schema.Literals(["video", "audio"]),
    name: Schema.String,
    mid: Schema.String,
  }),
  Schema.Struct({ type: Schema.Literal("error"), failure: Failure }),
]);

/** One decoded BGRA frame, named by its track's index in the prepare request. */
const Video = Schema.Struct({
  track: Schema.Int,
  width: Schema.Int,
  height: Schema.Int,
  frameId: Schema.BigInt,
  timestampUs: Schema.BigInt,
  sequence: Schema.BigInt,
  data: Bytes,
  metadata: Bytes,
});

/** One block of interleaved PCM, named as a frame is. */
const Audio = Schema.Struct({
  track: Schema.Int,
  sampleRate: Schema.Int,
  channels: Schema.Int,
  sequence: Schema.BigInt,
  samples: Samples,
});

/**
 * One child per peer. `Open` loads the addon and creates the child's peer;
 * `Shutdown` closes and joins it. Every other RPC is one addon call, and each
 * of `Events`, `Video` and `Audio` takes one of its queues as the parent pulls.
 */
export class IsolatedRpcs extends RpcGroup.make(
  Rpc.make("Open", {
    payload: { addon: Schema.optionalKey(Schema.String) },
    error: OpenFailure,
  }),
  Rpc.make("Prepare", {
    payload: { servers: Schema.Array(IceServer), tracks: Schema.Array(Track) },
    success: Prepared,
    error: Failure,
  }),
  Rpc.make("Answer", { payload: { sdp: Schema.String }, error: Failure }),
  Rpc.make("Direction", {
    payload: { name: Schema.String, active: Schema.Boolean },
    error: Failure,
  }),
  Rpc.make("MaxBitrate", {
    payload: { name: Schema.String, bitsPerSecond: Schema.Int },
    error: Failure,
  }),
  Rpc.make("Send", { payload: { channel: DataChannel, bytes: Bytes }, error: Failure }),
  Rpc.make("Stats", { success: Stats, error: Failure }),
  Rpc.make("Pressure", { success: Pressure, error: Failure }),
  Rpc.make("Events", { success: PeerEvent, error: Failure, stream: true }),
  Rpc.make("Video", { success: Video, error: Failure, stream: true }),
  Rpc.make("Audio", { success: Audio, error: Failure, stream: true }),
  Rpc.make("Shutdown", { error: Failure }),
) {}
