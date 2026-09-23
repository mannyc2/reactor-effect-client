import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import type * as Scope from "effect/Scope";
import { PeerFactory } from "reactor-effect-client";
import type { ReactorError, Session } from "reactor-effect-client";
import { parsed, trackGeneration } from "reactor-effect-client/host";
import type { TrackGeneration } from "reactor-effect-client/host";
import { BrowserPeer, requireBrowserPeer } from "./_internal/peer.js";

/**
 * Browser transport capability for the portable Client. Building the layer
 * detects WebRTC support, so a host without it fails there, before any Client
 * exists to allocate a remote session. Provide it to the client layer:
 * `Reactor.layer(configuration).pipe(Layer.provide(Browser.layer))`.
 */
export const layer: Layer.Layer<PeerFactory, ReactorError> = Layer.effect(
  PeerFactory,
  parsed(requireBrowserPeer).pipe(Effect.as(PeerFactory.of({ make: () => new BrowserPeer() }))),
);

export interface MediaGeneration extends Omit<TrackGeneration, "track" | "publish"> {
  readonly track: (name: string) => Effect.Effect<MediaStreamTrack, ReactorError, Scope.Scope>;
  readonly publish: (name: string, source: MediaStreamTrack) => Effect.Effect<void, ReactorError>;
}

/** Track leases and publication retain the generation that negotiated them. */
export const media = (session: Session): Effect.Effect<MediaGeneration, ReactorError> =>
  trackGeneration(session).pipe(
    Effect.map((generation) => ({
      ...generation,
      // The browser peer advertises this capability and is the only producer of
      // these DOM tracks. The portable session keeps their lifetime contract.
      track: (name: string) =>
        generation.track(name).pipe(Effect.map((track) => track as MediaStreamTrack)),
    })),
  );

export * as Recording from "./_internal/recording.js";
export {
  videoFrames,
  audioSamples,
  mediaFacilities,
  audioContext,
  webAudioSamples,
  play,
  nextPresentation,
} from "./_internal/media.js";
export type {
  VideoSample,
  AudioSample,
  AudioContextOptions,
  MediaOptions,
  PlayOptions,
  Presentation,
  WebAudioOptions,
  WebAudioSample,
} from "./_internal/media.js";
