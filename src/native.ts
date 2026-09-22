import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import { ReactorError } from "./errors.js";
import { resolveNativeBridge } from "./native-bridge.js";
import { NativePeer } from "./native-peer.js";
import { PeerFactory } from "./PeerFactory.js";
import type { PeerFactoryShape } from "./PeerFactory.js";
import * as SessionClient from "./SessionClient.js";
import type * as Model from "./Model.js";

export interface NativeOptions {
  /** Override the reactor-effect-native shared library path. */
  readonly libraryPath?: string;
}

const factory = (options: NativeOptions): PeerFactoryShape => {
  let resolved = options.libraryPath;
  return {
    check: Effect.tryPromise({
      try: async () => { resolved = await resolveNativeBridge(options.libraryPath); },
      catch: (cause) => cause instanceof ReactorError
        ? cause
        : new ReactorError("Native", "native WebRTC preflight failed", { detail: cause, outcome: "not-submitted" }),
    }),
    make: () => {
      if (resolved === undefined) throw new ReactorError("InvalidState", "native WebRTC factory was not preflighted", { outcome: "not-submitted" });
      return new NativePeer(resolved);
    },
  };
};

/**
 * Native transport capability for the portable Client. Koffi and the shared
 * library are loaded only when PeerFactory.check runs, so merely importing
 * this module remains safe when the optional native dependency is absent.
 */
export const layer = (options: NativeOptions = {}) => Layer.succeed(PeerFactory, factory(options));

/** Native convenience acquisition used by server-side model adapters. */
export const make = (options: Model.Options) => SessionClient.make(factory({
  ...(options.nativeLibraryPath === undefined ? {} : { libraryPath: options.nativeLibraryPath }),
}), options);
