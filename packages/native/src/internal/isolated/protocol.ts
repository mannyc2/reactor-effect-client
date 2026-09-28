/**
 * The private contract between an isolated native peer and its child process:
 * one Effect RPC group mirroring the addon's `NativePeer`, over the child's
 * IPC channel. The child relays the addon; the parent runs the same peer over
 * it that runs in process.
 *
 * The worker protocols encode every message with Schema's JSON codec. What the
 * addon produces crosses as a declaration that structured clone passes on as
 * it is, typed arrays and bigints included; the parent's peer checks what it
 * uses, as it does in process.
 */
import * as Predicate from "effect/Predicate";
import * as Schema from "effect/Schema";
import * as Rpc from "effect/unstable/rpc/Rpc";
import * as RpcGroup from "effect/unstable/rpc/RpcGroup";
import { IceServer, Track } from "reactor-effect-client/Coordinator";
import { Channel } from "reactor-effect-client/Peer";
import type * as Binding from "../binding.js";

const cloned = <T>(expected: string, is: (u: unknown) => boolean = Predicate.isObject) =>
  Schema.declare((u): u is T => is(u), { expected, toCodecJson: () => undefined });

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
    success: cloned<Binding.Prepared>("Prepared"),
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
  Rpc.make("Send", {
    payload: {
      channel: Channel,
      bytes: cloned<Uint8Array>("Uint8Array", (u) => u instanceof Uint8Array),
    },
    error: Failure,
  }),
  Rpc.make("Stats", {
    success: cloned<ReadonlyArray<Record<string, unknown>>>("statistics", Array.isArray),
    error: Failure,
  }),
  Rpc.make("Pressure", { success: cloned<Binding.Pressure>("Pressure"), error: Failure }),
  Rpc.make("Events", { success: cloned<Binding.PeerEvent>("PeerEvent"), stream: true }),
  Rpc.make("Video", { success: cloned<Binding.Video>("Video"), stream: true }),
  Rpc.make("Audio", { success: cloned<Binding.Audio>("Audio"), stream: true }),
  Rpc.make("Shutdown", { error: Failure }),
) {}
