import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import type * as Crypto from "effect/Crypto";
import type * as Http from "effect/unstable/http/HttpClient";
import { PeerFactory, ReactorError, make as makeClient } from "reactor-effect-client";
import type { Configuration, Factory, PeerFactoryShape, Session } from "reactor-effect-client";
import { mediaGeneration } from "reactor-effect-client/host";
import type { MediaGeneration } from "reactor-effect-client/host";
import { resolveNativeBridge } from "./_internal/bridge.js";
import { NativePeer } from "./_internal/peer.js";

export interface NativeOptions {
  /** Override the reactor-effect-native shared library path. */
  readonly libraryPath?: string;
}

const factory = (options: NativeOptions): PeerFactoryShape => {
  let resolved = options.libraryPath;
  return {
    check: Effect.tryPromise({
      try: async () => {
        resolved = await resolveNativeBridge(options.libraryPath);
      },
      catch: (cause) =>
        cause instanceof ReactorError
          ? cause
          : new ReactorError({
              code: "Native",
              message: "native WebRTC preflight failed",
              context: { detail: cause, outcome: "not-submitted" },
            }),
    }),
    make: () => {
      if (resolved === undefined)
        throw new ReactorError({
          code: "InvalidState",
          message: "native WebRTC factory was not preflighted",
          context: { outcome: "not-submitted" },
        });
      return new NativePeer(resolved);
    },
  };
};

/**
 * Native transport capability for the portable Client. Koffi and the shared
 * library are loaded only when PeerFactory.check runs, so merely importing
 * this module remains safe when the optional native dependency is absent.
 */
export const layer = (options: NativeOptions = {}): Layer.Layer<PeerFactory> =>
  Layer.succeed(PeerFactory, factory(options));

/** Select the native peer while retaining the same scoped session contract. */
export const make = (
  configuration: Configuration = {},
  options: NativeOptions = {},
): Effect.Effect<Factory, never, Http.HttpClient | Crypto.Crypto> =>
  makeClient(configuration).pipe(Effect.provide(layer(options)));

/** Acquire the current decoded-media generation after session.connect succeeds. */
export const media = (session: Session): Effect.Effect<MediaGeneration, ReactorError> =>
  mediaGeneration(session);

export type {
  VideoFrame,
  AudioFrame,
  MediaGeneration,
  MediaPressure,
} from "reactor-effect-client/host";
export { uploadFile } from "reactor-effect-client/host";
export type { FileUploadOptions } from "reactor-effect-client/host";
