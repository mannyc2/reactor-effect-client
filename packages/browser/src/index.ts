import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import type * as Scope from "effect/Scope";
import type { TrackMedia } from "reactor-effect-client/Media";
import { PeerFactory } from "reactor-effect-client/Peer";
import type { ReactorError } from "reactor-effect-client/ReactorError";
import type { Session } from "reactor-effect-client/Session";
import { parsed } from "reactor-effect-client/host";
import { BrowserPeer, requireBrowserPeer, toPeer } from "./_internal/peer.js";

/**
 * The browser's `PeerFactory`. Building the layer detects WebRTC support, so a
 * browser without it fails there, before any session is allocated:
 * `Reactor.layer().pipe(Layer.provide(Browser.layer))`.
 */
export const layer: Layer.Layer<PeerFactory, ReactorError> = Layer.effect(
  PeerFactory,
  parsed(requireBrowserPeer).pipe(
    Effect.as(
      PeerFactory.of({
        check: Effect.void,
        make: Effect.acquireRelease(
          Effect.sync(() => toPeer(new BrowserPeer())),
          (peer) => peer.close,
        ),
      }),
    ),
  ),
);

export interface MediaGeneration extends Omit<TrackMedia, "track" | "publish"> {
  readonly track: (name: string) => Effect.Effect<MediaStreamTrack, ReactorError, Scope.Scope>;
  readonly publish: (name: string, source: MediaStreamTrack) => Effect.Effect<void, ReactorError>;
}

/** The session's current tracks as DOM tracks: this host is their only producer. */
export const media = (session: Session): Effect.Effect<MediaGeneration, ReactorError> =>
  session.tracks.pipe(
    Effect.map((generation) => ({
      ...generation,
      track: (name: string) =>
        generation.track(name).pipe(Effect.map((track) => track as MediaStreamTrack)),
    })),
  );

export { play } from "./_internal/media.js";
export type { PlayOptions } from "./_internal/media.js";
