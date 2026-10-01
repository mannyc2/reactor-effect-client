import { Crypto, Effect, Layer, PlatformError } from "effect";

/**
 * Effect's `Crypto` service over Web Crypto, which the client needs for
 * request identities. `@effect/platform-browser`'s `BrowserCrypto.layer`
 * provides one too; the page builds its own to do without that dependency.
 * Secure contexts all have `crypto.getRandomValues` and `crypto.subtle`.
 */
export const WebCrypto = Layer.sync(Crypto.Crypto, () =>
  Crypto.make({
    randomBytes: (size) => globalThis.crypto.getRandomValues(new Uint8Array(size)),
    digest: (algorithm, bytes) =>
      Effect.tryPromise({
        try: () =>
          globalThis.crypto.subtle
            .digest(algorithm, Uint8Array.from(bytes))
            .then((digest) => new Uint8Array(digest)),
        catch: (cause) =>
          PlatformError.systemError({ _tag: "Unknown", module: "Crypto", method: "digest", cause }),
      }),
  }),
);
