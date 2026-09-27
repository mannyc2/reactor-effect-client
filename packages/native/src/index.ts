import type * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import { PeerFactory } from "reactor-effect-client/Peer";
import { ReactorError } from "reactor-effect-client/ReactorError";
import { duration, parsed } from "reactor-effect-client/host";
import { NativeBridge, resolveNativeBridge } from "./_internal/bridge.js";
import { NativePeer, defaultShutdownTimeout } from "./_internal/peer.js";
import { acquirePeer } from "./_internal/port.js";

export interface NativeOptions {
  /** Override the reactor-effect-native shared library path. */
  readonly libraryPath?: string;
  /**
   * How long closing a connection waits for the native owner join, 10 seconds
   * by default, and `"Infinity"` waits without bound. A bare number is
   * milliseconds. On expiry the close reports a `Shutdown` error and carries on
   * to remote termination, while the join keeps the native handle and no later
   * peer is created in this process until it completes.
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
const acquire = (options: NativeOptions): Effect.Effect<PeerFactory["Service"], ReactorError> =>
  Effect.gen(function* () {
    const shutdownTimeout = yield* parsed(() =>
      duration(options.shutdownTimeout ?? defaultShutdownTimeout, "native shutdownTimeout", {
        allowInfinite: true,
      }),
    );
    const resolved = yield* Effect.tryPromise({
      try: () => resolveNativeBridge(options.libraryPath),
      catch: preflightError,
    });
    return PeerFactory.of({
      // The library stays loaded, but an owner join that outlived its deadline
      // may have wedged its shared libwebrtc factory: refuse before allocation.
      check: parsed(() => NativeBridge.requireUsable(resolved)),
      make: acquirePeer(() => new NativePeer(resolved, shutdownTimeout)),
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

export * as Isolated from "./isolated.js";

export type {
  AudioFrame,
  MediaPressure,
  VideoFormat,
  VideoFrame,
} from "reactor-effect-client/Media";
