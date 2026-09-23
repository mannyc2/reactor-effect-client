import { ReactorError } from "./errors.js";
import { array, finite, jsonObject, nonempty, record, string, uint32 } from "./json.js";
import type { Json, JsonObject } from "./json.js";
export const SDK_PIN = "24f1865b48f03354b15402731f1b92e90ceee92a";
export const RUNTIME_PIN = "8e98536c6daedca298700dc45cbb5fd9e3676f16";
export const CLIENT_INFO = Object.freeze({
  sdk_version: "0.1.0",
  sdk_type: "typescript-effect-independent",
});
export interface Track {
  readonly name: string;
  readonly kind: "audio" | "video";
  readonly direction: "recvonly" | "sendonly";
}
export interface Mapping extends Track {
  readonly mid: string;
}
export interface Capabilities {
  readonly protocol_version: string;
  readonly tracks: readonly Track[];
  readonly commands?: readonly {
    readonly name: string;
    readonly description?: string;
    readonly schema?: Json;
  }[];
  readonly emission_fps?: number;
}
export interface Descriptor {
  readonly session_id: string;
  /** Unknown future state values remain available, rather than becoming a made-up known state. */
  readonly state: string;
  readonly capabilities?: Capabilities;
  readonly selected_transport?: { readonly protocol: string; readonly version: string };
  /** Includes unknown top-level fields and original null/absence distinctions. */
  readonly raw: JsonObject;
}
export const terminal = (state: string): boolean => state === "CLOSED" || state === "INACTIVE";
export const parseTrack = (input: unknown): Track => {
  const t = record(input, "track"),
    name = nonempty(t.name, "track.name");
  if (t.kind !== "audio" && t.kind !== "video")
    throw ReactorError.fromCode("Protocol", "unknown track kind");
  if (t.direction !== "recvonly" && t.direction !== "sendonly")
    throw ReactorError.fromCode("Protocol", "unknown track direction");
  return Object.freeze({ name, kind: t.kind, direction: t.direction });
};
export const parseCapabilities = (input: unknown): Capabilities => {
  const c = record(input, "capabilities"),
    tracks = array(c.tracks, "capabilities.tracks").map(parseTrack);
  if (tracks.length > 64)
    throw ReactorError.fromCode("Protocol", "at most 64 tracks are supported");
  if (new Set(tracks.map((t) => t.name)).size !== tracks.length)
    throw ReactorError.fromCode(
      "Protocol",
      "duplicate track names are ambiguous for named track operations",
    );
  const commands =
    c.commands == null
      ? undefined
      : array(c.commands, "commands").map((input) => {
          const raw = jsonObject(input),
            x = record(raw);
          return Object.freeze({
            name: nonempty(x.name, "command.name"),
            ...(x.description == null
              ? {}
              : { description: string(x.description, "command.description") }),
            ...(raw.schema === undefined ? {} : { schema: raw.schema }),
          });
        });
  return Object.freeze({
    protocol_version: nonempty(c.protocol_version, "capabilities.protocol_version"),
    tracks: Object.freeze(tracks),
    ...(commands === undefined ? {} : { commands: Object.freeze(commands) }),
    ...(c.emission_fps == null ? {} : { emission_fps: finite(c.emission_fps, "emission_fps") }),
  });
};
/** A create reply's session id alone, so ownership never depends on the reply's later fields. */
export const parseSessionId = (input: unknown): string =>
  nonempty(record(input).session_id, "session_id");
export const parseDescriptor = (input: unknown): Descriptor => {
  const raw = jsonObject(input),
    d = record(raw);
  const transport =
    d.selected_transport == null ? undefined : record(d.selected_transport, "selected_transport");
  return Object.freeze({
    session_id: parseSessionId(d),
    state: nonempty(d.state, "session.state"),
    raw,
    ...(d.capabilities == null ? {} : { capabilities: parseCapabilities(d.capabilities) }),
    ...(transport === undefined
      ? {}
      : {
          selected_transport: Object.freeze({
            protocol: nonempty(transport.protocol, "transport.protocol"),
            version: nonempty(transport.version, "transport.version"),
          }),
        }),
  });
};
export interface IceServer {
  readonly urls: string | string[];
  readonly username?: string;
  readonly credential?: string;
}
export const parseIce = (input: unknown): IceServer[] => {
  const list = array(record(input).ice_servers, "ice_servers");
  if (list.length > 64) throw ReactorError.fromCode("Protocol", "too many ICE servers");
  return list.map((x) => {
    const server = record(x, "ICE server"),
      urls = array(server.uris, "ICE server uris").map((u) => nonempty(u, "ICE URI"));
    if (server.credentials == null) return { urls };
    const auth = record(server.credentials, "ICE credentials");
    return {
      urls,
      username: string(auth.username, "ICE username"),
      credential: string(auth.password, "ICE password"),
    };
  });
};
export interface IceCandidate {
  readonly candidate: string;
  readonly sdp_mid?: string;
  readonly sdp_mline_index?: number;
}
export const parseConnectionId = (input: unknown): number =>
  uint32(record(input).connection_id, "connection_id");
export const parseAnswer = (
  input: unknown,
): { readonly sdp_answer: string; readonly connection_id?: number } => {
  const answer = record(input, "SDP answer");
  return {
    sdp_answer: nonempty(answer.sdp_answer, "sdp_answer"),
    ...(answer.connection_id == null
      ? {}
      : { connection_id: uint32(answer.connection_id, "connection_id") }),
  };
};
