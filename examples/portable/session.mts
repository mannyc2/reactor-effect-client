import * as Effect from "effect/Effect";
import type * as Redacted from "effect/Redacted";
import * as Reactor from "reactor-effect-client";

/** Inert until run. The caller supplies a Client layer and a scope. */
export const inspectSession = (model: string, jwt: Redacted.Redacted<string>) =>
  Effect.gen(function* () {
    const client = yield* Reactor.Client;
    const session = yield* client.create({ model, jwt });
    yield* session.connect;
    const ready = yield* session.ready;
    // Explicit close returns evidence; scope release reuses that same report.
    const cleanup = yield* session.close;
    return { ready, cleanup };
  });
