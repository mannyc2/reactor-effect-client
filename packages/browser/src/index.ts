import * as Effect from "effect/Effect";
import * as Crypto from "effect/Crypto";
import * as Layer from "effect/Layer";
import * as Http from "effect/unstable/http/HttpClient";
import type * as Scope from "effect/Scope";
import { PeerFactory, make as makeClient } from "reactor-effect-client";
import type { Configuration, Factory, ReactorError, Session } from "reactor-effect-client";
import { errorOf, trackGeneration } from "reactor-effect-client/host";
import type { TrackGeneration } from "reactor-effect-client/host";
import { BrowserPeer, requireBrowserPeer } from "./_internal/peer.js";

export const layer = Layer.succeed(PeerFactory, {
  check: Effect.try({ try: requireBrowserPeer, catch: errorOf }),
  make: () => new BrowserPeer(),
});

/** Browser peer selection; acquisition and cleanup remain owned by the client. */
export const make = (
  configuration: Configuration = {},
): Effect.Effect<Factory, never, Http.HttpClient | Crypto.Crypto> =>
  makeClient(configuration).pipe(Effect.provide(layer));

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
  MediaOptions,
  Presentation,
  WebAudioOptions,
  WebAudioSample,
} from "./_internal/media.js";
