/**
 * The private contract between an isolated native peer and its child process:
 * one Effect RPC group mirroring the `Peer` contract, over the child's IPC
 * channel. The worker protocols encode every message with Schema's JSON codec,
 * so the byte-carrying fields are declarations that structured clone passes on
 * as they are, where `Schema.Uint8Array` would become Base64 text.
 *
 * Failures cross as a plain record and are rebuilt as `ReactorError` in the
 * parent: the Native reason's backend text is Redacted, which refuses JSON
 * encoding, and it stays Redacted on each side of the channel.
 */
import * as Predicate from "effect/Predicate";
import * as Redacted from "effect/Redacted";
import * as Schema from "effect/Schema";
import * as Rpc from "effect/unstable/rpc/Rpc";
import * as RpcGroup from "effect/unstable/rpc/RpcGroup";
import {
  ErrorCode,
  IceFailed,
  Mapping,
  Native,
  ReactorError,
  Track,
  TransportFailed,
} from "reactor-effect-client";
import type { ErrorContext } from "reactor-effect-client";
import type { PeerEvent } from "reactor-effect-client/host";

/** A value structured clone carries unchanged, checked by its guard on each side. */
const cloned = <T>(is: (u: unknown) => u is T, expected: string) =>
  Schema.declare(is, { expected, toCodecJson: () => undefined });

const Bytes = cloned((u): u is Uint8Array<ArrayBuffer> => u instanceof Uint8Array, "Uint8Array");
const Samples = cloned((u): u is Int16Array<ArrayBuffer> => u instanceof Int16Array, "Int16Array");
/** Statistics records hold bigint counters, which structured clone keeps. */
const Records = cloned((u): u is readonly unknown[] => Array.isArray(u), "statistics array");

const Outcome = Schema.Literals(["not-submitted", "unknown", "replied"]);

/** A `ReactorError` as it crosses the channel: its reason fields and dispatch evidence. */
export const WireFailure = Schema.Struct({
  code: ErrorCode,
  message: Schema.String,
  operation: Schema.optionalKey(Schema.String),
  outcome: Schema.optionalKey(Outcome),
  status: Schema.optionalKey(Schema.Int),
  backendMessage: Schema.optionalKey(Schema.String),
  channel: Schema.optionalKey(Schema.String),
  pairs: Schema.optionalKey(Schema.Int),
  candidateTypes: Schema.optionalKey(Schema.Array(Schema.String)),
});
export type WireFailure = typeof WireFailure.Type;

const Channel = Schema.Literals(["control", "data"]);

const WireEvent = Schema.Union([
  Schema.Struct({
    type: Schema.Literal("state"),
    state: Schema.Literals(["new", "connecting", "connected", "disconnected", "failed", "closed"]),
  }),
  Schema.Struct({ type: Schema.Literal("channel"), channel: Channel, open: Schema.Boolean }),
  Schema.Struct({ type: Schema.Literal("message"), channel: Channel, bytes: Bytes }),
  Schema.Struct({
    type: Schema.Literal("ice"),
    candidate: Schema.optionalKey(
      Schema.Struct({
        candidate: Schema.String,
        sdp_mid: Schema.optionalKey(Schema.String),
        sdp_mline_index: Schema.optionalKey(Schema.Int),
      }),
    ),
  }),
  Schema.Struct({ type: Schema.Literal("track"), name: Schema.String, mid: Schema.String }),
  Schema.Struct({
    type: Schema.Literal("decoded"),
    kind: Schema.Literals(["video", "audio"]),
    name: Schema.String,
    mid: Schema.String,
  }),
  Schema.Struct({ type: Schema.Literal("error"), error: WireFailure }),
]);
export type WireEvent = typeof WireEvent.Type;

/** The prepare stream: the offer first, then the child's peer events in order. */
export const PrepareItem = Schema.Union([
  Schema.TaggedStruct("Prepared", { sdp: Schema.String, mapping: Schema.Array(Mapping) }),
  Schema.TaggedStruct("Event", { event: WireEvent }),
]);
export type PrepareItem = typeof PrepareItem.Type;

export const WireVideo = Schema.Struct({
  width: Schema.Int,
  height: Schema.Int,
  frameId: Schema.BigInt,
  timestampMicros: Schema.BigInt,
  data: Bytes,
  metadata: Bytes,
});
export type WireVideo = typeof WireVideo.Type;

export const WireAudio = Schema.Struct({
  sampleRate: Schema.Int,
  channels: Schema.Int,
  samples: Samples,
});
export type WireAudio = typeof WireAudio.Type;

const Count = Schema.Int.check(Schema.isGreaterThanOrEqualTo(0));
const Total = Schema.BigInt.check(Schema.isGreaterThanOrEqualToBigInt(0n));

/** The child's transport pressure, with its own readers' overflows. */
export const WirePressure = Schema.Struct({
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
  readerOverflows: Total,
});

const IceServer = Schema.Struct({
  urls: Schema.Array(Schema.String),
  username: Schema.String,
  credential: Schema.String,
});

/**
 * One child per peer. `Open` loads and verifies the library and creates the
 * child's native peer; `Shutdown` closes and joins it. Every other RPC is one
 * `Peer` operation, and `Video`/`Audio` open a track's frame stream.
 */
export class IsolatedRpcs extends RpcGroup.make(
  Rpc.make("Open", {
    payload: { libraryPath: Schema.optionalKey(Schema.String) },
    error: WireFailure,
  }),
  Rpc.make("Prepare", {
    payload: { servers: Schema.Array(IceServer), tracks: Schema.Array(Track) },
    success: PrepareItem,
    error: WireFailure,
    stream: true,
  }),
  Rpc.make("Answer", { payload: { sdp: Schema.String }, error: WireFailure }),
  Rpc.make("Send", { payload: { channel: Channel, bytes: Bytes }, error: WireFailure }),
  Rpc.make("Direction", {
    payload: { name: Schema.String, active: Schema.Boolean },
    error: WireFailure,
  }),
  Rpc.make("Replace", { payload: { name: Schema.String }, error: WireFailure }),
  Rpc.make("MaxBitrate", {
    // The child's peer rejects a value out of range, a non-finite one
    // included, as the in-process host does, so it must reach the child.
    // @effect-diagnostics-next-line schemaNumber:off
    payload: { name: Schema.String, bitsPerSecond: Schema.Number },
    error: WireFailure,
  }),
  Rpc.make("Stats", { success: Records, error: WireFailure }),
  Rpc.make("Snapshot", { success: WirePressure, error: WireFailure }),
  Rpc.make("Video", {
    payload: { name: Schema.String },
    success: WireVideo,
    error: WireFailure,
    stream: true,
  }),
  Rpc.make("Audio", {
    payload: { name: Schema.String },
    success: WireAudio,
    error: WireFailure,
    stream: true,
  }),
  Rpc.make("Shutdown", { error: WireFailure }),
) {}

const nativeDetail = (
  detail: unknown,
): { readonly status?: number; readonly backendMessage?: string; readonly channel?: string } => {
  if (!Predicate.isObject(detail)) return {};
  return {
    ...(typeof detail.status === "number" && Number.isSafeInteger(detail.status)
      ? { status: detail.status }
      : {}),
    ...(Redacted.isRedacted(detail.backendMessage) &&
    typeof Redacted.value(detail.backendMessage) === "string"
      ? { backendMessage: Redacted.value(detail.backendMessage) as string }
      : {}),
    ...(typeof detail.channel === "string" ? { channel: detail.channel } : {}),
  };
};

/**
 * A failure as the child sends it. Only the evidence the native host itself
 * attaches crosses: its failure class and backend text; any other `detail` is
 * the child's own diagnostic and stays there.
 */
export const toWire = (error: ReactorError): WireFailure => {
  const { reason, context } = error;
  const base = {
    code: reason._tag,
    message: reason.message,
    ...(context.operation === undefined ? {} : { operation: context.operation }),
    ...(context.outcome === undefined ? {} : { outcome: context.outcome }),
  };
  switch (reason._tag) {
    case "Native":
      return {
        ...base,
        ...nativeDetail(context.detail),
        ...(reason.status === undefined ? {} : { status: reason.status }),
        ...(reason.backendMessage === undefined
          ? {}
          : { backendMessage: Redacted.value(reason.backendMessage) }),
      };
    case "IceFailed":
      return { ...base, pairs: reason.pairs, candidateTypes: reason.candidateTypes };
    case "TransportFailed":
      return { ...base, pairs: reason.pairs };
    default:
      return { ...base, ...nativeDetail(context.detail) };
  }
};

/** The parent's `ReactorError` for a failure the child sent, as the in-process host raises it. */
export const fromWire = (wire: WireFailure): ReactorError => {
  const context: ErrorContext = {
    ...(wire.operation === undefined ? {} : { operation: wire.operation }),
    ...(wire.outcome === undefined ? {} : { outcome: wire.outcome }),
  };
  const backendMessage =
    wire.backendMessage === undefined ? undefined : Redacted.make(wire.backendMessage);
  const channel = wire.channel === undefined ? {} : { channel: wire.channel };
  switch (wire.code) {
    case "Native":
      return new ReactorError({
        reason: new Native({
          message: wire.message,
          ...(wire.status === undefined ? {} : { status: wire.status }),
          ...(backendMessage === undefined ? {} : { backendMessage }),
        }),
        context: wire.channel === undefined ? context : { ...context, detail: channel },
      });
    case "IceFailed":
      return new ReactorError({
        reason: new IceFailed({
          message: wire.message,
          pairs: wire.pairs ?? 0,
          candidateTypes: wire.candidateTypes ?? [],
        }),
        context,
      });
    case "TransportFailed":
      return new ReactorError({
        reason: new TransportFailed({ message: wire.message, pairs: wire.pairs ?? 0 }),
        context,
      });
    default:
      return ReactorError.fromCode(
        wire.code,
        wire.message,
        wire.status === undefined
          ? context
          : { ...context, detail: { status: wire.status, backendMessage, ...channel } },
      );
  }
};

/** A peer event as the child sends it. */
export const eventToWire = (event: PeerEvent): WireEvent => {
  switch (event.type) {
    case "error":
      return { type: "error", error: toWire(event.error) };
    case "message":
      return { ...event, bytes: event.bytes as Uint8Array<ArrayBuffer> };
    default:
      return event;
  }
};

/** A peer event as the parent delivers it. */
export const eventFromWire = (event: WireEvent): PeerEvent => {
  switch (event.type) {
    case "message":
      return { type: "message", channel: event.channel, bytes: exact(event.bytes) };
    case "error":
      return { type: "error", error: fromWire(event.error) };
    default:
      return event;
  }
};

/**
 * Bytes the parent owns. Node's advanced serialization delivers every typed
 * array of a message as a view into one shared message buffer; a frame's data
 * must be the whole of an ArrayBuffer of its own, so a view that is not is
 * copied into an exact allocation.
 */
export function exact(view: Uint8Array<ArrayBuffer>): Uint8Array<ArrayBuffer>;
export function exact(view: Int16Array<ArrayBuffer>): Int16Array<ArrayBuffer>;
export function exact(
  view: Uint8Array<ArrayBuffer> | Int16Array<ArrayBuffer>,
): Uint8Array<ArrayBuffer> | Int16Array<ArrayBuffer> {
  return view.byteOffset === 0 && view.byteLength === view.buffer.byteLength ? view : view.slice();
}
