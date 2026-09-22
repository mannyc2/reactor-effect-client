import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import type * as Redacted from "effect/Redacted";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as Reactor from "reactor-effect-client";
import * as Native from "reactor-effect-client/native";
import { inspectSession } from "../portable/session.mjs";

const clientLayer = Reactor.layer().pipe(
  Layer.provide(Layer.mergeAll(Reactor.FetchHttp.layer, NodeServices.layer, Native.layer())),
);

/** Compilation only in verification. Running this operation allocates a real session. */
export const inspectNativeSession = (model: string, jwt: Redacted.Redacted<string>) =>
  Effect.scoped(inspectSession(model, jwt)).pipe(Effect.provide(clientLayer));
