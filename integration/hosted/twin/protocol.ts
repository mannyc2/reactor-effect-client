/**
 * The link between the twin's peer and its server, standing in for a WebRTC
 * connection. The SDP offer names the peer in its ICE username fragment; the
 * server binds that peer to the connection whose `sdp_params` carried it, then
 * delivers channel messages, media state and closure through a long poll, and
 * the peer posts its own channel messages. Like hosted media, it carries no
 * bearer credential: the peer id is random and known only to the two ends.
 */
import * as Schema from "effect/Schema";
import type { Mapping } from "reactor-effect-client";

export type ChannelName = "control" | "data";

/** What a connected peer renders on its receive tracks. */
export const Media = Schema.Struct({
  /** The playing clip, and how far into it the model is. */
  clip: Schema.NullOr(Schema.Struct({ id: Schema.String, elapsedMs: Schema.Finite })),
  /** The frame the model holds after a clip when it does not flush to black. */
  hold: Schema.NullOr(Schema.Struct({ id: Schema.String, frame: Schema.Int })),
  /** Receive tracks the client paused. */
  paused: Schema.Array(Schema.String),
  video: Schema.Literals(["live", "black", "frozen"]),
  audio: Schema.Boolean,
});
export interface Media extends Schema.Schema.Type<typeof Media> {}

export const Candidate = Schema.Struct({
  type: Schema.Literals(["host", "srflx", "prflx", "relay"]),
  address: Schema.String,
  port: Schema.Int,
});
export interface Candidate extends Schema.Schema.Type<typeof Candidate> {}

export const ServerEvent = Schema.Union([
  /** Connectivity checks selected this pair; both channels are open. */
  Schema.Struct({
    seq: Schema.Int,
    type: Schema.Literal("open"),
    local: Candidate,
    remote: Candidate,
  }),
  Schema.Struct({
    seq: Schema.Int,
    type: Schema.Literal("message"),
    channel: Schema.Literals(["control", "data"]),
    data: Schema.Uint8ArrayFromBase64,
  }),
  Schema.Struct({ seq: Schema.Int, type: Schema.Literal("media"), media: Media }),
  /** The server closed this connection: another replaced it, the session ended, or no pair worked. */
  Schema.Struct({
    seq: Schema.Int,
    type: Schema.Literal("closed"),
    reason: Schema.Literals(["replaced", "ended", "unreachable"]),
  }),
]);
export type ServerEvent = typeof ServerEvent.Type;

/** One long-poll response: the events after the sequence the peer acknowledged. */
export const Events = Schema.Struct({ events: Schema.Array(ServerEvent) });
export type EncodedEvent = (typeof Events.Encoded)["events"][number];

const priorities = { host: 2122260223, srflx: 1686052607, prflx: 1845501695, relay: 41885439 };

/** RFC 8445's priority of a pair, with the local side controlling, as the native host reports it. */
export const pairPriority = (local: Candidate, remote: Candidate): bigint => {
  const g = BigInt(priorities[local.type]),
    d = BigInt(priorities[remote.type]);
  return (1n << 32n) * (g < d ? g : d) + 2n * (g > d ? g : d) + (g > d ? 1n : 0n);
};

export const candidateLine = (foundation: number, candidate: Candidate): string =>
  `candidate:${foundation} 1 udp ${priorities[candidate.type]} ${candidate.address} ${candidate.port} typ ${candidate.type}`;

const candidatePattern =
  /^candidate:\S+ \d+ udp \d+ (\S+) (\d+) typ (host|srflx|prflx|relay)(?: |$)/;

export const parseCandidate = (line: string): Candidate | undefined => {
  const match = candidatePattern.exec(line);
  if (match === null) return undefined;
  return {
    type: match[3] as Candidate["type"],
    address: match[1]!,
    port: Number(match[2]),
  };
};

const sections = (mapping: readonly Mapping[], local: boolean): string[] => [
  ...mapping.flatMap((track) => [
    `m=${track.kind} 9 UDP/TLS/RTP/SAVPF 96`,
    `a=mid:${track.mid}`,
    // The answer mirrors each direction the offer declared.
    `a=${(track.direction === "recvonly") === local ? "recvonly" : "sendonly"}`,
    `a=msid:- ${track.name}`,
  ]),
  "m=application 9 UDP/DTLS/SCTP webrtc-datachannel",
  `a=mid:${mapping.length}`,
];

const sdp = (peer: string, mapping: readonly Mapping[], lines: readonly string[]): string =>
  `${[
    "v=0",
    "o=- 1 2 IN IP4 127.0.0.1",
    "s=-",
    "t=0 0",
    `a=group:BUNDLE ${[...mapping.map((track) => track.mid), String(mapping.length)].join(" ")}`,
    `a=ice-ufrag:${peer}`,
    ...lines,
  ].join("\r\n")}\r\n`;

/** The peer's offer: its identity, its tracks, and the data channel section. */
export const offer = (peer: string, mapping: readonly Mapping[]): string =>
  sdp(peer, mapping, ["a=ice-options:trickle", ...sections(mapping, true)]);

/** The server's answer, with the candidate its side of the selected pair uses. */
export const answer = (peer: string, mapping: readonly Mapping[], remote: Candidate): string =>
  sdp(peer, mapping, [`a=${candidateLine(1, remote)}`, ...sections(mapping, false)]);

/** The peer an offer or answer names. */
export const peerOf = (description: string): string | undefined =>
  /^a=ice-ufrag:([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})\r?$/m.exec(
    description,
  )?.[1];
