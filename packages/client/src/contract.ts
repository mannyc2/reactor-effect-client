import * as Option from "effect/Option";
import * as Predicate from "effect/Predicate";
import * as Result from "effect/Result";
import * as Schema from "effect/Schema";
import * as SchemaTransformation from "effect/SchemaTransformation";
import { ReactorError } from "./errors.js";
import { json, jsonObject } from "./json.js";
import type { Json, JsonObject } from "./json.js";
export const SDK_PIN = "24f1865b48f03354b15402731f1b92e90ceee92a";
export const RUNTIME_PIN = "8e98536c6daedca298700dc45cbb5fd9e3676f16";
export const CLIENT_INFO = Object.freeze({
  sdk_version: "0.1.0",
  sdk_type: "typescript-effect-independent",
});
/**
 * A field the provider sends as `null` or omits for the same fact: both decode
 * as absent, so the decoded value omits it.
 */
const NullAsAbsent = <S extends Schema.Top>(schema: S) =>
  Schema.optionalKey(Schema.NullOr(schema)).pipe(
    Schema.decodeTo(
      Schema.optionalKey(Schema.toType(schema)),
      SchemaTransformation.transformOptional({
        decode: Option.filter(Predicate.isNotNull),
        encode: (value) => value,
      }),
    ),
  );
const Uint32 = Schema.Int.check(Schema.isBetween({ minimum: 0, maximum: 0xffffffff }));

export const Track = Schema.Struct({
  name: Schema.NonEmptyString,
  kind: Schema.Literals(["audio", "video"]),
  direction: Schema.Literals(["recvonly", "sendonly"]),
});
export interface Track extends Schema.Schema.Type<typeof Track> {}

/** A track and the media section a host negotiated for it. */
export const Mapping = Schema.Struct({ ...Track.fields, mid: Schema.String });
export interface Mapping extends Schema.Schema.Type<typeof Mapping> {}

export const Capabilities = Schema.Struct({
  protocol_version: Schema.NonEmptyString,
  tracks: Schema.Array(Track).check(
    Schema.isMaxLength(64),
    Schema.makeFilter((tracks) =>
      new Set(tracks.map((track) => track.name)).size === tracks.length
        ? undefined
        : "duplicate track names are ambiguous for named track operations",
    ),
  ),
  commands: NullAsAbsent(
    Schema.Array(
      Schema.Struct({
        name: Schema.NonEmptyString,
        description: NullAsAbsent(Schema.String),
        schema: Schema.optionalKey(Schema.Json),
      }),
    ),
  ),
  emission_fps: NullAsAbsent(Schema.Finite),
});
export interface Capabilities extends Schema.Schema.Type<typeof Capabilities> {}

/** A create reply's session id alone, so ownership never depends on the reply's later fields. */
export const Allocated = Schema.Struct({ session_id: Schema.NonEmptyString });

/** The decoded fields of a session descriptor. */
export const SessionDescriptor = Schema.Struct({
  session_id: Schema.NonEmptyString,
  /** Unknown future state values remain available, rather than becoming a made-up known state. */
  state: Schema.NonEmptyString,
  capabilities: NullAsAbsent(Capabilities),
  selected_transport: NullAsAbsent(
    Schema.Struct({ protocol: Schema.NonEmptyString, version: Schema.NonEmptyString }),
  ),
});
export interface Descriptor extends Schema.Schema.Type<typeof SessionDescriptor> {
  /** Includes unknown top-level fields and original null/absence distinctions. */
  readonly raw: JsonObject;
}

export const IceServers = Schema.Struct({
  ice_servers: Schema.Array(
    Schema.Struct({
      uris: Schema.Array(Schema.NonEmptyString),
      credentials: NullAsAbsent(
        Schema.Struct({ username: Schema.String, password: Schema.String }),
      ),
    }),
  ).check(Schema.isMaxLength(64)),
});

export const Registered = Schema.Struct({ connection_id: Uint32 });

export const SdpAnswer = Schema.Struct({
  sdp_answer: Schema.NonEmptyString,
  connection_id: NullAsAbsent(Uint32),
});

/** A presigned upload slot. */
export const UploadSlot = Schema.Struct({
  presigned_id: Schema.NonEmptyString,
  presigned_url: Schema.NonEmptyString,
  path: Schema.String,
});

/** A key exchange's reply. */
export const Exchanged = Schema.Struct({ jwt: Schema.NonEmptyString });

export const terminal = (state: string): boolean => state === "CLOSED" || state === "INACTIVE";

const deepFreeze = <A>(value: A): A => {
  if (value !== null && typeof value === "object" && !Object.isFrozen(value)) {
    for (const child of Object.values(value)) deepFreeze(child);
    Object.freeze(value);
  }
  return value;
};

/**
 * Decode a reply, already copied by the bounded JSON walker, through its
 * Schema. A mismatch is `Protocol`, with a message the library writes and the
 * `SchemaError`, which names the path but no input value, kept in `detail`.
 */
const decoder = <S extends Schema.Decoder<unknown>>(
  schema: S,
  what: string,
): ((value: Json) => S["Type"]) => {
  const decode = Schema.decodeUnknownResult(schema);
  return (value) => {
    const decoded = decode(value);
    if (Result.isFailure(decoded))
      throw ReactorError.fromCode("Protocol", `invalid ${what}`, { detail: decoded.failure });
    return deepFreeze(decoded.success);
  };
};

const decodeAllocated = decoder(Allocated, "create reply");
const decodeDescriptor = decoder(SessionDescriptor, "session descriptor");
const decodeCapabilities = decoder(Capabilities, "capabilities");
const decodeIceServers = decoder(IceServers, "ICE servers reply");
const decodeRegistered = decoder(Registered, "connection reply");
const decodeAnswer = decoder(SdpAnswer, "SDP answer");
const decodeUploadSlot = decoder(UploadSlot, "upload allocation");
const decodeExchanged = decoder(Exchanged, "key exchange reply");

export const parseCapabilities = (input: unknown): Capabilities => decodeCapabilities(json(input));
export const parseSessionId = (input: unknown): string => decodeAllocated(json(input)).session_id;
export const parseDescriptor = (input: unknown): Descriptor => {
  const raw = jsonObject(input);
  return Object.freeze({ ...decodeDescriptor(raw), raw });
};
export interface IceServer {
  readonly urls: string | string[];
  readonly username?: string;
  readonly credential?: string;
}
export const parseIce = (input: unknown): IceServer[] =>
  decodeIceServers(json(input)).ice_servers.map((server) =>
    server.credentials === undefined
      ? { urls: [...server.uris] }
      : {
          urls: [...server.uris],
          username: server.credentials.username,
          credential: server.credentials.password,
        },
  );
export interface IceCandidate {
  readonly candidate: string;
  readonly sdp_mid?: string;
  readonly sdp_mline_index?: number;
}
export const parseConnectionId = (input: unknown): number =>
  decodeRegistered(json(input)).connection_id;
export const parseAnswer = (input: unknown): typeof SdpAnswer.Type => decodeAnswer(json(input));
export const parseUploadSlot = (input: unknown): typeof UploadSlot.Type =>
  decodeUploadSlot(json(input));
export const parseExchangedToken = (input: unknown): string => decodeExchanged(json(input)).jwt;
