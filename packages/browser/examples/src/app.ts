/**
 * The page the example's server serves. It runs live when its server mints
 * tokens, and offline with `?offline` in the address, when the server has no
 * API key, or when no server answers. The choice is made once, as the
 * runtime's layer is built; the Studio's code is the same either way.
 */
import { Effect, Layer, ManagedRuntime } from "effect";
import * as FetchHttpClient from "effect/unstable/http/FetchHttpClient";
import { HttpApiClient } from "effect/unstable/httpapi";
import { Api } from "./Api.ts";
import * as Live from "./Live.ts";
import * as Offline from "./Offline.ts";
import * as Page from "./Page.ts";
import * as Studio from "./Studio.ts";

const { screen } = Page.ui;

const chosen = Layer.unwrap(
  Effect.gen(function* () {
    if (new URLSearchParams(window.location.search).has("offline"))
      return Offline.layer({ screen, reason: "Remove ?offline from the address to run live." });
    const api = yield* HttpApiClient.make(Api);
    const live = yield* api.live().pipe(Effect.orElseSucceed(() => undefined));
    if (live === undefined)
      return Offline.layer({ screen, reason: "No token server answered, so it runs offline." });
    switch (live._tag) {
      case "Available":
        return Live.layer({ screen, maxSessionSeconds: live.maxSessionSeconds });
      case "Unavailable":
        return Offline.layer({ screen, reason: "Set REACTOR_API_KEY on the server to run live." });
    }
  }),
);

Studio.mount(ManagedRuntime.make(chosen.pipe(Layer.provide(FetchHttpClient.layer))));
