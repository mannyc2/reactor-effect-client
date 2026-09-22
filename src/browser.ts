import * as Effect from "effect/Effect";
import * as Crypto from "effect/Crypto";
import * as Layer from "effect/Layer";
import type * as Scope from "effect/Scope";
import * as Http from "effect/unstable/http/HttpClient";
import { PeerFactory } from "./PeerFactory.js";
import { BrowserPeer, requireBrowserPeer } from "./peer.js";
import { Session } from "./session.js";
import type { SessionOptions } from "./session.js";
import { HttpClient } from "./http.js";
import { ReactorError, errorOf } from "./errors.js";

export const layer = Layer.succeed(PeerFactory, {
  check: Effect.try({ try: requireBrowserPeer, catch: errorOf }),
  make: () => new BrowserPeer(),
});

/** Browser-specific convenience acquisition; HTTP remains supplied by the caller. */
export const connect = (options: SessionOptions): Effect.Effect<Session, ReactorError, Scope.Scope | Http.HttpClient | Crypto.Crypto> =>
  Effect.gen(function* () {
    const http = yield* Http.HttpClient;
    const crypto = yield* Crypto.Crypto;
    yield* Effect.try({ try: requireBrowserPeer, catch: errorOf });
    const identity = yield* crypto.randomBytes(16).pipe(Effect.mapError((cause) =>
      new ReactorError("InvalidState", "request identity allocation failed", { outcome: "not-submitted", detail: cause })));
    const session = yield* Effect.acquireRelease(Effect.try({
      try: () => {
        const configured = { ...options, requestNamespace: Array.from(identity, (byte) => byte.toString(16).padStart(2, "0")).join("") };
        return new Session(configured, () => new BrowserPeer(), new HttpClient(configured, http));
      },
      catch: errorOf,
    }), (session) => session.close());
    yield* session.start();
    return session;
  });

export { videoFrames, audioSamples } from "./media.js";
