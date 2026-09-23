import { test } from "bun:test";
import * as Crypto from "effect/Crypto";
import * as Effect from "effect/Effect";
import * as FetchHttpClient from "effect/unstable/http/FetchHttpClient";
import { FetchHttp } from "reactor-effect-client";
import { assert, equal } from "reactor-effect-test-kit";
import * as Browser from "../src/index.js";

const webCrypto = Crypto.make({
  randomBytes: (size) => globalThis.crypto.getRandomValues(new Uint8Array(size)),
  digest: (algorithm, bytes) =>
    Effect.promise(
      async () =>
        new Uint8Array(await globalThis.crypto.subtle.digest(algorithm, Uint8Array.from(bytes))),
    ),
});

test("absent built-in peer fails before allocating any remote session", async () => {
  if (typeof RTCPeerConnection === "function") return;
  let calls = 0;
  const fetch = (() => {
    calls++;
    return Promise.reject(new Error("no coordinator request is expected"));
  }) as unknown as typeof globalThis.fetch;
  const result = await Effect.runPromise(
    Effect.result(
      Effect.scoped(
        Effect.gen(function* () {
          const client = yield* Browser.make({ apiUrl: "https://coordinator.fixture" });
          return yield* client.createConnected({ model: "owner/model" });
        }),
      ),
    ).pipe(
      Effect.provide(FetchHttp.layer),
      Effect.provideService(FetchHttpClient.Fetch, fetch),
      Effect.provideService(Crypto.Crypto, webCrypto),
    ),
  );
  assert(result._tag === "Failure", "expected the host check to fail");
  equal(result.failure.code, "UnsupportedHost");
  equal(calls, 0);
});
