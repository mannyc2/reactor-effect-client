/**
 * The transport port a host implements: `reactor-effect-browser` binds it to
 * `RTCPeerConnection`, `reactor-effect-native` to libwebrtc, and `ReactorTest`
 * to the simulated Reactor. A peer carries bytes and media; session
 * allocation, commands and correlation stay in the client.
 */
import * as Context from "effect/Context";
import type * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import type * as Scope from "effect/Scope";
import type * as Stream from "effect/Stream";
import { IceCandidate, Mapping } from "./Coordinator.js";
import type { IceServer, Track } from "./Coordinator.js";
import type { AudioFrame, MediaPressure, VideoFrame } from "./Media.js";
import { ReactorError } from "./ReactorError.js";

export const Channel = Schema.Literals(["control", "data"]);
export type Channel = typeof Channel.Type;

export const PeerState = Schema.Literals([
  "new",
  "connecting",
  "connected",
  "disconnected",
  "failed",
  "closed",
]);
export type PeerState = typeof PeerState.Type;

/** What a host reports about its connection, in the order it happened. */
export const PeerEvent = Schema.Union([
  Schema.Struct({ type: Schema.Literal("state"), state: PeerState }),
  Schema.Struct({ type: Schema.Literal("channel"), channel: Channel, open: Schema.Boolean }),
  Schema.Struct({ type: Schema.Literal("message"), channel: Channel, bytes: Schema.Uint8Array }),
  /** A local candidate; none marks the end of gathering. */
  Schema.Struct({ type: Schema.Literal("ice"), candidate: Schema.optionalKey(IceCandidate) }),
  Schema.Struct({ type: Schema.Literal("track"), name: Schema.String, mid: Schema.String }),
  Schema.Struct({
    type: Schema.Literal("decoded"),
    kind: Schema.Literals(["video", "audio"]),
    name: Schema.String,
    mid: Schema.String,
  }),
  Schema.Struct({ type: Schema.Literal("error"), error: ReactorError }),
]);
export type PeerEvent = typeof PeerEvent.Type;

/** An offer and the media section each declared track was given. */
export const Prepared = Schema.Struct({ sdp: Schema.String, mapping: Schema.Array(Mapping) });
export type Prepared = typeof Prepared.Type;

/** The lifetime contract of a browser media track, without DOM types. */
export interface MediaTrack {
  readonly kind: string;
  readonly readyState: "live" | "ended";
  clone(): MediaTrack;
  stop(): void;
}

/** What a host does with media: owned decoded frames, or the platform's own tracks. */
export type PeerMedia =
  | {
      readonly _tag: "Decoded";
      /** One track's frames; the session fences them to the generation that negotiated it. */
      readonly video: (name: string) => Stream.Stream<VideoFrame, ReactorError>;
      readonly audio: (name: string) => Stream.Stream<AudioFrame, ReactorError>;
      readonly pressure: Effect.Effect<MediaPressure, ReactorError>;
    }
  | {
      readonly _tag: "Tracks";
      /** A clone of a received track, stopped when the scope closes. */
      readonly lease: (name: string) => Effect.Effect<MediaTrack, ReactorError, Scope.Scope>;
      /** Attach `track` (a session-owned clone) to a send-only track, or detach with null. */
      readonly replace: (
        name: string,
        track: MediaTrack | null,
      ) => Effect.Effect<void, ReactorError>;
    };

/** One connection generation's transport. */
export interface Peer {
  readonly media: PeerMedia;
  /**
   * Gathers and returns the offer. The host calls `emit` for every later event,
   * synchronously and without waiting: platform peers are callback-driven, so
   * this is the host boundary, and the session buffers what it receives.
   */
  readonly prepare: (
    servers: ReadonlyArray<IceServer>,
    tracks: ReadonlyArray<Track>,
    emit: (event: PeerEvent) => void,
  ) => Effect.Effect<Prepared, ReactorError, Scope.Scope>;
  readonly answer: (sdp: string) => Effect.Effect<void, ReactorError>;
  /** Local submission; never a Reactor reply. */
  readonly send: (
    channel: Channel,
    bytes: Uint8Array<ArrayBuffer>,
  ) => Effect.Effect<void, ReactorError>;
  readonly direction: (name: string, active: boolean) => Effect.Effect<void, ReactorError>;
  readonly maxBitrate: (name: string, bitsPerSecond: number) => Effect.Effect<void, ReactorError>;
  readonly stats: Effect.Effect<ReadonlyArray<unknown>, ReactorError>;
  /**
   * Fences the transport at once, so nothing is emitted or sent after it.
   * The factory's scope then shuts the host down within its own bound.
   */
  readonly close: Effect.Effect<void>;
}

/** A host's transport capability. Building its layer is the host's preflight. */
export class PeerFactory extends Context.Service<
  PeerFactory,
  {
    /**
     * Fails when the host cannot make a peer now, such as while a native
     * owner join is still outstanding. It runs before every allocation, so no
     * paid session is allocated for a peer that cannot be made.
     */
    readonly check: Effect.Effect<void, ReactorError>;
    /** A fresh peer for one connection generation; closing the scope shuts it down. */
    readonly make: Effect.Effect<Peer, ReactorError, Scope.Scope>;
  }
>()("reactor-effect-client/Peer/PeerFactory") {}
