import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import type * as Crypto from "effect/Crypto";
import type * as Http from "effect/unstable/http/HttpClient";
import { PeerFactory, ReactorError, make as makeClient } from "reactor-effect-client";
import type { Configuration, Factory, PeerFactoryShape, Session } from "reactor-effect-client";
import { mediaGeneration } from "reactor-effect-client/host";
import type { MediaGeneration } from "reactor-effect-client/host";
import { NativeBridge, resolveNativeBridge } from "./_internal/bridge.js";
import { NativePeer, defaultShutdownTimeout } from "./_internal/peer.js";

export interface NativeOptions {
  /** Override the reactor-effect-native shared library path. */
  readonly libraryPath?: string;
  /**
   * How long closing a connection waits for the native owner join, 10 seconds
   * by default; a bare number is milliseconds. On expiry the close reports a
   * `Shutdown` error and carries on to remote termination, while the join keeps
   * the native handle and no later peer is created in this process until it
   * completes.
   */
  readonly shutdownTimeout?: Duration.Input;
}

const factory = (options: NativeOptions): PeerFactoryShape => {
  let resolved = options.libraryPath;
  const shutdownTimeout = options.shutdownTimeout ?? defaultShutdownTimeout;
  return {
    check: Effect.tryPromise({
      try: async () => {
        // NaN decodes to zero, so it fails here with every other non-positive input.
        if (
          !Duration.fromInput(shutdownTimeout).pipe(
            Option.exists(Duration.isGreaterThan(Duration.zero)),
          )
        )
          throw new ReactorError({
            code: "InvalidInput",
            message: "native shutdownTimeout must be a positive duration",
            context: { outcome: "not-submitted" },
          });
        resolved = await resolveNativeBridge(options.libraryPath);
        NativeBridge.requireUsable(resolved);
      },
      catch: (cause) =>
        ReactorError.is(cause)
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
      return new NativePeer(resolved, shutdownTimeout);
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
