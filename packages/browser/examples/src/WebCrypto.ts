import { Crypto, Effect, Layer, PlatformError } from "effect";

/**
 * Effect's `Crypto` service over Web Crypto, which the client needs for
 * request identities. The pinned Effect stack has no browser platform layer
 * that provides it, so the page builds one; secure contexts all have
 * `crypto.getRandomValues` and `crypto.subtle`.
 */
export const WebCrypto = Layer.sync(Crypto.Crypto, () =>
  Crypto.make({
    randomBytes: (size) => globalThis.crypto.getRandomValues(new Uint8Array(size)),
    digest: (algorithm, bytes) =>
      Effect.tryPromise({
        try: async () =>
          new Uint8Array(await globalThis.crypto.subtle.digest(algorithm, Uint8Array.from(bytes))),
        catch: (cause) =>
          PlatformError.systemError({ _tag: "Unknown", module: "Crypto", method: "digest", cause }),
      }),
  }),
);
