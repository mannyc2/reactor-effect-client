import * as Effect from "effect/Effect";
import * as Crypto from "effect/Crypto";
import * as Layer from "effect/Layer";
import * as Http from "effect/unstable/http/HttpClient";
import type * as Scope from "effect/Scope";
import { PeerFactory } from "../PeerFactory.js";
import { BrowserPeer, requireBrowserPeer } from "./_internal/peer.js";
import { errorOf, type ReactorError } from "../errors.js";
import { make as makeClient } from "../session/index.js";
import type { Configuration, Factory, Session } from "../session/index.js";
import type { TrackGeneration } from "../session/media.js";
import { implementationOf } from "../session/_internal/acquire.js";

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
  implementationOf(session).pipe(
    Effect.flatMap((value) => value.trackGeneration()),
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
