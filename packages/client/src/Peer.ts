/**
 * The transport port a host implements: `reactor-effect-browser` binds it to
 * `RTCPeerConnection`, `reactor-effect-native` to libwebrtc, and `ReactorTest`
 * to the simulated Reactor. A peer carries bytes and media; session
 * allocation, commands and correlation stay in the client.
 */
import * as Cause from "effect/Cause";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import type * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import * as Hub from "./internal/hub.js";
import type { AudioFrame, MediaPressure, VideoFrame } from "./Media.js";
import { ReactorError } from "./ReactorError.js";

export const Track = Schema.Struct({
  name: Schema.NonEmptyString,
  kind: Schema.Literals(["audio", "video"]),
  direction: Schema.Literals(["recvonly", "sendonly"]),
});
export type Track = typeof Track.Type;

/** A track and the media section a host negotiated for it. */
export const Mapping = Schema.Struct({ ...Track.fields, mid: Schema.String });
export type Mapping = typeof Mapping.Type;

export const IceServer = Schema.Struct({
  urls: Schema.Array(Schema.NonEmptyString),
  username: Schema.optionalKey(Schema.String),
  credential: Schema.optionalKey(Schema.String),
});
export type IceServer = typeof IceServer.Type;

export const IceCandidate = Schema.Struct({
  candidate: Schema.String,
  sdp_mid: Schema.optionalKey(Schema.String),
  sdp_mline_index: Schema.optionalKey(Schema.Int),
});
export type IceCandidate = typeof IceCandidate.Type;

export const DataChannel = Schema.Literals(["control", "data"]);
export type DataChannel = typeof DataChannel.Type;

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
  Schema.Struct({ type: Schema.Literal("channel"), channel: DataChannel, open: Schema.Boolean }),
  Schema.Struct({
    type: Schema.Literal("message"),
    channel: DataChannel,
    bytes: Schema.Uint8Array,
  }),
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

/**
 * How a decoded host delivers one received track. Every reader gets every
 * frame; a reader that falls behind its bounds fails with `Overflow`, and is
 * counted in `overflows`, rather than holding up the host or silently skipping
 * frames. A reader that starts after the feed ended sees the same end.
 */
export interface TrackFeed<A> {
  readonly publish: (frame: A) => Effect.Effect<void>;
  /** A new reader each time the stream runs. */
  readonly stream: Stream.Stream<A, ReactorError>;
  readonly end: Effect.Effect<void>;
  readonly fail: (error: ReactorError) => Effect.Effect<void>;
  /** Readers that fell behind and failed; a host reports it as `readerOverflows`. */
  readonly overflows: Effect.Effect<bigint>;
}

/** A track's feed, each reader holding at most `capacity` frames and `maxBytes` of them. */
export const trackFeed = <A>(bounds: {
  readonly capacity: number;
  readonly maxBytes: number;
  readonly bytes: (frame: A) => number;
}): Effect.Effect<TrackFeed<A>> =>
  Effect.map(Hub.make<A>({ weigh: bounds.bytes }), (hub): TrackFeed<A> => ({
    publish: hub.publish,
    stream: Stream.unwrap(hub.subscribe(bounds.capacity, bounds.maxBytes)),
    end: hub.end,
    fail: (error) => hub.fail(Cause.fail(error)),
    overflows: hub.overflows,
  }));

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
    channel: DataChannel,
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
