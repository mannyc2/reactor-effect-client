// NodeHttpServer serves on a server made by Node's own http module.
// @effect-diagnostics-next-line nodeBuiltinImport:off
import { createServer } from "node:http";
import * as NodeHttpServer from "@effect/platform-node/NodeHttpServer";
import { assert, describe, it, layer } from "@effect/vitest";
import { Effect, Redacted, Ref } from "effect";
import * as FetchHttpClient from "effect/unstable/http/FetchHttpClient";
import * as HttpServer from "effect/unstable/http/HttpServer";
import * as HttpServerResponse from "effect/unstable/http/HttpServerResponse";
import * as Probes from "../Probes.js";

/** A local server answering every request with `reply` until the scope closes, and its origin. */
const serve = Effect.fnUntraced(function* (
  reply: Effect.Effect<HttpServerResponse.HttpServerResponse>,
) {
  const server = yield* NodeHttpServer.make(createServer, { port: 0, host: "127.0.0.1" });
  yield* server.serve(reply);
  return HttpServer.formatAddress(server.address);
});

layer(FetchHttpClient.layer)("a session read with the API key", (it) => {
  // The client's coordinator refuses every redirect, so a credential goes only where it was sent.
  it.effect("follows no redirect, so the key reaches nowhere else", () =>
    Effect.gen(function* () {
      const reached = yield* Ref.make(0);
      const elsewhere = yield* serve(
        Effect.as(
          Ref.update(reached, (count) => count + 1),
          HttpServerResponse.jsonUnsafe({ state: "ACTIVE" }),
        ),
      );
      const coordinator = yield* serve(
        Effect.succeed(HttpServerResponse.redirect(`${elsewhere}/sessions/s`)),
      );
      const read = yield* Probes.readSession({
        apiUrl: coordinator,
        sessionId: "s",
        credential: Redacted.make("reactor-test-key"),
      });
      assert.deepStrictEqual([read.status, yield* Ref.get(reached)], [0, 0]);
    }),
  );
});

describe("a refusal's body", () => {
  // What hosted Reactor answers a spent token is unobserved, so its code may sit under any key.
  it("keeps its codes under any key, and no sentence, URL or IP address", () => {
    assert.deepStrictEqual(
      Probes.summarizeRefusal({
        code: "SESSION_LIMIT",
        detail: "The token's sessions are used",
        type: "https://errors.example/session-limit",
        seen: "203.0.113.5",
        error: { code: "session_limit", retryable: false },
      }),
      {
        keys: ["code", "detail", "type", "seen", "error"],
        codes: {
          code: "SESSION_LIMIT",
          detail: "(text, 29 chars)",
          type: "(text, 36 chars)",
          seen: "(text, 11 chars)",
          "error.code": "session_limit",
          "error.retryable": "false",
        },
      },
    );
  });

  it("keeps codes in a list's first entries, and nothing under a key that names an address", () => {
    assert.deepStrictEqual(
      Probes.summarizeRefusal({
        errors: [{ code: "session_limit", status: "403" }, "token_used", 3, 4],
        client_ip: "198.51.100.7",
        remoteHost: "gw-7.isp.example",
        error: { endpoint: "api.example", reason: "spent", details: { code: "limit.sessions" } },
      }),
      {
        keys: ["errors", "client_ip", "remoteHost", "error"],
        codes: {
          "errors[0].code": "session_limit",
          "errors[0].status": "403",
          "errors[1]": "token_used",
          "errors[2]": "3",
          "error.reason": "spent",
          "error.details.code": "limit.sessions",
        },
      },
    );
  });
});
