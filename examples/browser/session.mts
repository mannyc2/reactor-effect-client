import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import type * as Redacted from "effect/Redacted";
import * as Reactor from "reactor-effect-client";
import * as Browser from "reactor-effect-client/browser";
import { inspectSession } from "../portable/session.mjs";
import { webCrypto } from "../portable/web-crypto.mjs";

const clientLayer = Reactor.layer().pipe(
  Layer.provide(Layer.mergeAll(Reactor.FetchHttp.layer, webCrypto, Browser.layer)),
);

/** Compilation only in verification. Run in a secure browser context with explicit credentials. */
export const inspectBrowserSession = (model: string, jwt: Redacted.Redacted<string>) =>
  Effect.scoped(inspectSession(model, jwt)).pipe(Effect.provide(clientLayer));
