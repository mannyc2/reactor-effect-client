/**
 * The isolated native host: the same native peer, run in a child process of
 * its own for each connection generation and driven over Effect RPC, so a
 * native crash or a wedged native owner ends that child rather than the
 * application. Opt in with `Native.Isolated.layer(options)` in place of
 * `Native.layer(options)`; the in-process host remains the default.
 *
 * It requires a Node.js parent process and `@effect/platform-node`, which this
 * module loads only when the layer is built, so importing the package stays
 * safe without it.
 */
import process from "node:process";
import type * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import type * as Scope from "effect/Scope";
import { PeerFactory, ReactorError } from "reactor-effect-client";
import type { PeerFactoryShape } from "reactor-effect-client";
import { duration, parsed } from "reactor-effect-client/host";
import { defaultShutdownTimeout } from "./_internal/peer.js";

export interface IsolatedOptions {
  /** Override the reactor-effect-native shared library path each child loads. */
  readonly libraryPath?: string;
  /**
   * How long closing a connection waits for its child to close and join its
   * native peer and exit, 10 seconds by default, and `"Infinity"` waits
   * without bound. A bare number is milliseconds. On expiry the child is
   * killed and the close reports a `Shutdown` error, then carries on to remote
   * termination; nothing is retained, so later peers are unaffected.
   */
  readonly shutdownTimeout?: Duration.Input;
}

const acquire = (
  options: IsolatedOptions,
): Effect.Effect<PeerFactoryShape, ReactorError, Scope.Scope> =>
  Effect.gen(function* () {
    const shutdownTimeout = yield* parsed(() =>
      duration(options.shutdownTimeout ?? defaultShutdownTimeout, "native shutdownTimeout", {
        allowInfinite: true,
      }),
    );
    if (process.versions.bun !== undefined)
      return yield* ReactorError.fromCode(
        "UnsupportedCapability",
        "the isolated native host requires a Node.js parent process",
        { outcome: "not-submitted" },
      );
    const host = yield* Effect.tryPromise({
      try: () => import("./_internal/isolated/host.js"),
      catch: (cause) =>
        ReactorError.fromCode(
          "UnsupportedHost",
          "the isolated native host requires @effect/platform-node",
          { detail: cause, outcome: "not-submitted" },
        ),
    });
    return yield* host.factory({ libraryPath: options.libraryPath, shutdownTimeout });
  });

/**
 * Isolated native transport capability for the portable Client. Building the
 * layer spawns a probe child that loads and verifies the library and opens a
 * native peer, then shuts it down, so an unsupported host or artifact fails
 * there, before any Client exists to allocate a remote session. Each peer the
 * factory makes spawns its own child at once, while the session allocates.
 * Under Bun the layer fails with `UnsupportedCapability`.
 */
export const layer = (options: IsolatedOptions = {}): Layer.Layer<PeerFactory, ReactorError> =>
  Layer.effect(PeerFactory, acquire(options));
