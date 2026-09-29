/**
 * The native peer: the client's `Peer` port on the libwebrtc Node-API addon,
 * which this platform's `reactor-effect-native-<platform>` package carries.
 * It runs in this process with `layer`, or with `layerIsolated` in a child
 * process per connection, so a native crash or a wedged owner ends that child
 * instead of the application. Importing this module loads no addon.
 */
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as FiberSet from "effect/FiberSet";
import * as Layer from "effect/Layer";
import { PeerFactory } from "reactor-effect-client/Peer";
import { ReactorError } from "reactor-effect-client/ReactorError";
import { load } from "./internal/addon.js";
import { local, usable } from "./internal/local.js";
import type { Joins } from "./internal/local.js";
import * as Peer from "./internal/peer.js";

export interface Options {
  /** The absolute path of an addon other than the platform package's own; its caller owns its provenance. */
  readonly addon?: string | undefined;
  /**
   * How long closing a connection waits for the native owner join, 10 seconds
   * by default; `"Infinity"` waits without bound. On expiry the close reports
   * `Shutdown` and carries on to remote termination. In process, the join keeps
   * the native peer and no later peer is made until it completes; isolated,
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
 * The native `PeerFactory`, in process. Building it loads the addon, so a host
 * that cannot run it fails there, before any session is allocated.
 */
export const layer = (options: Options = {}): Layer.Layer<PeerFactory, ReactorError> =>
  Layer.effect(
    PeerFactory,
    Effect.gen(function* () {
      const timeout = yield* shutdownTimeout(options);
      const addon = yield* load(options.addon);
      const joins: Joins = yield* FiberSet.make();
      return PeerFactory.of({
        // An owner join that outlived its deadline may have wedged the shared
        // libwebrtc factory: refuse before a session is allocated for a peer.
        check: usable(joins),
        make: Effect.flatMap(local(addon, joins), (handle) => Peer.make(handle, timeout)),
      });
    }),
  );

/**
 * The native `PeerFactory`, with each connection's peer in a child process of
 * its own, driven over Effect RPC. Building it spawns a probe child that loads
 * the addon and opens a peer, so a host that cannot run it fails there. It
 * needs a Node.js parent process and `@effect/platform-node`, which it loads
 * only as it is built.
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
      return yield* host.factory({ addon: options.addon, shutdownTimeout: timeout });
    }),
  );
