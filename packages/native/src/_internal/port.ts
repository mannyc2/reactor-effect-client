import * as Effect from "effect/Effect";
import type * as Scope from "effect/Scope";
import type * as Stream from "effect/Stream";
import type { IceServer, Track } from "reactor-effect-client/Coordinator";
import type { AudioFrame, MediaPressure, VideoFrame } from "reactor-effect-client/Media";
import type { Channel, Peer, PeerEvent, Prepared } from "reactor-effect-client/Peer";
import type { ReactorError } from "reactor-effect-client/ReactorError";

/** A native peer's decoded media, before the session fences it to a generation. */
export interface RawMedia {
  readonly video: (name: string) => Stream.Stream<VideoFrame, ReactorError>;
  readonly audio: (name: string) => Stream.Stream<AudioFrame, ReactorError>;
  readonly snapshot: Effect.Effect<MediaPressure, ReactorError>;
}

/** What the in-process and isolated native peers share. */
interface NativeTransport {
  readonly rawMedia: RawMedia;
  prepare(
    servers: ReadonlyArray<IceServer>,
    tracks: ReadonlyArray<Track>,
    emit: (event: PeerEvent) => void,
  ): Effect.Effect<Prepared, ReactorError, Scope.Scope>;
  answer(sdp: string): Effect.Effect<void, ReactorError>;
  send(channel: Channel, bytes: Uint8Array<ArrayBuffer>): Effect.Effect<void, ReactorError>;
  direction(name: string, active: boolean): Effect.Effect<void, ReactorError>;
  maxBitrate(name: string, bitsPerSecond: number): Effect.Effect<void, ReactorError>;
  readonly stats: Effect.Effect<ReadonlyArray<unknown>, ReactorError>;
  /** Fences the peer at once. */
  close(): void;
  /** Joins the native owner within the peer's bound. */
  readonly shutdown: Effect.Effect<void, ReactorError>;
}

/**
 * A native peer as the client's `Peer` port, shut down when its scope closes.
 * A failed shutdown dies, so the session's close report keeps it.
 */
export const acquirePeer = (make: () => NativeTransport): Effect.Effect<Peer, never, Scope.Scope> =>
  Effect.acquireRelease(Effect.sync(make), (peer) =>
    Effect.sync(() => peer.close()).pipe(Effect.andThen(peer.shutdown), Effect.orDie),
  ).pipe(
    Effect.map((peer): Peer => ({
      media: {
        _tag: "Decoded",
        video: peer.rawMedia.video,
        audio: peer.rawMedia.audio,
        pressure: peer.rawMedia.snapshot,
      },
      prepare: (servers, tracks, emit) => peer.prepare(servers, tracks, emit),
      answer: (sdp) => peer.answer(sdp),
      send: (channel, bytes) => peer.send(channel, bytes),
      direction: (name, active) => peer.direction(name, active),
      maxBitrate: (name, bits) => peer.maxBitrate(name, bits),
      stats: Effect.suspend(() => peer.stats),
      close: Effect.sync(() => peer.close()),
    })),
  );
