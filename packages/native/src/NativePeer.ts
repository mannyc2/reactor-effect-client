/**
 * The native peer: the client's `Peer` port on the Rust libwebrtc bridge,
 * loaded through the optional Koffi dependency. It runs in this process with
 * `layer`, or with `layerIsolated` in a child process per connection, so a
 * native crash or a wedged owner ends that child instead of the application.
 * Importing this module loads neither Koffi nor the library.
 */
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import { PeerFactory } from "reactor-effect-client/Peer";
import { ReactorError } from "reactor-effect-client/ReactorError";
import { requireUsable, resolve } from "./internal/library.js";
import * as Peer from "./internal/peer.js";

export interface Options {
  /** A staged library other than the package's own; its caller owns its provenance. */
  readonly libraryPath?: string | undefined;
  /**
   * How long closing a connection waits for the native owner join, 10 seconds
   * by default; `"Infinity"` waits without bound. On expiry the close reports
   * `Shutdown` and carries on to remote termination. In process, the join keeps
   * the native handle and no later peer is made until it completes; isolated,
   * the child is killed.
   */
  readonly shutdownTimeout?: Duration.Input | undefined;
}

/** A positive duration; `"Infinity"` waits without bound. */
const shutdownTimeout = (options: Options): Effect.Effect<Duration.Duration, ReactorError> =>
  Effect.fromOption(
    Duration.fromInput(options.shutdownTimeout ?? Peer.defaultShutdownTimeout),
  ).pipe(
    Effect.filterOrFail((timeout) => Duration.toMillis(timeout) > 0),
    Effect.mapError(() =>
      ReactorError.fromCode("InvalidInput", "shutdownTimeout must be a positive duration", {
        outcome: "not-submitted",
      }),
    ),
  );

/**
 * The native `PeerFactory`, in process. Building it loads Koffi and the library
 * and verifies the staged artifact, so a host that cannot run it fails there,
 * before any session is allocated.
 */
export const layer = (options: Options = {}): Layer.Layer<PeerFactory, ReactorError> =>
  Layer.effect(
    PeerFactory,
    Effect.gen(function* () {
      const timeout = yield* shutdownTimeout(options);
      const library = yield* resolve(options.libraryPath);
      return PeerFactory.of({
        // An owner join that outlived its deadline may have wedged the shared
        // libwebrtc factory: refuse before a session is allocated for a peer.
        check: requireUsable(library),
        make: Peer.make(library, timeout),
      });
    }),
  );

/**
 * The native `PeerFactory`, with each connection's peer in a child process of
 * its own, driven over Effect RPC. Building it spawns a probe child that loads
 * and verifies the library, so a host that cannot run it fails there. It needs
 * a Node.js parent process and `@effect/platform-node`, which it loads only as
 * it is built.
 */
export const layerIsolated = (options: Options = {}): Layer.Layer<PeerFactory, ReactorError> =>
  Layer.effect(
    PeerFactory,
    Effect.gen(function* () {
      const timeout = yield* shutdownTimeout(options);
      if (process.versions.bun !== undefined)
        return yield* ReactorError.fromCode(
          "UnsupportedCapability",
          "the isolated native peer needs a Node.js parent process",
          { outcome: "not-submitted" },
        );
      const host = yield* Effect.tryPromise({
        try: () => import("./internal/isolated/host.js"),
        catch: (cause) =>
          ReactorError.fromCode(
            "UnsupportedHost",
            "the isolated native peer needs @effect/platform-node",
            { outcome: "not-submitted", detail: cause },
          ),
      });
      return yield* host.factory({ libraryPath: options.libraryPath, shutdownTimeout: timeout });
    }),
  );
