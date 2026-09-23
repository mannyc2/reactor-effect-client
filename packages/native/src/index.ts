import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import { PeerFactory, ReactorError } from "reactor-effect-client";
import type { PeerFactoryShape, Session } from "reactor-effect-client";
import { mediaGeneration, parsed } from "reactor-effect-client/host";
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

const preflightError = (cause: unknown): ReactorError =>
  ReactorError.is(cause)
    ? cause
    : ReactorError.fromCode("Native", "native WebRTC preflight failed", {
        detail: cause,
        outcome: "not-submitted",
      });

/**
 * Resolve, load and verify the native library once, as the layer is built;
 * every peer the factory makes then uses that library.
 */
const acquire = (options: NativeOptions): Effect.Effect<PeerFactoryShape, ReactorError> =>
  Effect.gen(function* () {
    const shutdownTimeout = options.shutdownTimeout ?? defaultShutdownTimeout;
    // NaN decodes to zero, so it fails here with every other non-positive input.
    if (
      !Duration.fromInput(shutdownTimeout).pipe(
        Option.exists(Duration.isGreaterThan(Duration.zero)),
      )
    )
      return yield* ReactorError.fromCode(
        "InvalidInput",
        "native shutdownTimeout must be a positive duration",
        { outcome: "not-submitted" },
      );
    const resolved = yield* Effect.tryPromise({
      try: () => resolveNativeBridge(options.libraryPath),
      catch: preflightError,
    });
    return PeerFactory.of({
      // The library stays loaded, but an owner join that outlived its deadline
      // may have wedged its shared libwebrtc factory: refuse before allocation.
      check: parsed(() => NativeBridge.requireUsable(resolved)),
      make: () => new NativePeer(resolved, shutdownTimeout),
    });
  });

/**
 * Native transport capability for the portable Client. Building the layer
 * loads Koffi and the shared library and verifies the staged artifact, so an
 * unsupported host fails there, before any Client exists to allocate a remote
 * session. Merely importing this module stays safe when the optional native
 * dependency is absent. Provide it to the client layer:
 * `Reactor.layer(configuration).pipe(Layer.provide(Native.layer(options)))`.
 */
export const layer = (options: NativeOptions = {}): Layer.Layer<PeerFactory, ReactorError> =>
  Layer.effect(PeerFactory, acquire(options));

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
