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

layer(FetchHttpClient.layer)("a request with the API key", (it) => {
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

  it.effect("keeps no URL as a key in its reply's shape", () =>
    Effect.gen(function* () {
      const coordinator = yield* serve(
        Effect.succeed(
          HttpServerResponse.jsonUnsafe({
            expires_at: 1,
            "https://reactor.example/grant": { max_sessions: 1 },
          }),
        ),
      );
      const [first] = yield* Probes.run({
        apiUrl: coordinator,
        apiKey: Redacted.make("reactor-test-key"),
      });
      assert.deepStrictEqual(first?.shape, [
        "expires_at: number",
        "(key, 29 chars).max_sessions: number",
      ]);
    }),
  );

  it.effect("keeps no host name as its reply's code", () =>
    Effect.gen(function* () {
      const coordinator = yield* serve(
        Effect.succeed(
          HttpServerResponse.jsonUnsafe(
            { error: { code: "edge.reactor.example" } },
            { status: 403 },
          ),
        ),
      );
      const probes = yield* Probes.run({
        apiUrl: coordinator,
        apiKey: Redacted.make("reactor-test-key"),
      });
      assert.deepStrictEqual(
        probes.map((probe) => [probe.status, probe.code]),
        probes.map(() => [403, undefined]),
      );
    }),
  );
});

describe("a session read's body", () => {
  // The evidence is committed, so nothing kept from a body may be a URL or an address.
  it("keeps no URL or address under any key, whatever joins it", () => {
    assert.deepStrictEqual(
      Probes.summarizeBody({
        session_id: "s-1",
        state: "CLOSED",
        end_reason: "cap_reached",
        status_url: "https://reactor.example/sessions/s-1",
        closed_by: "10.0.0.7:8443",
        terminated_at: 1_790_000_000,
        "https://reactor.example/status": "ended",
        error: {
          code: "session_limit",
          origin: "wss://gpu-7.reactor.example",
          peer: "ip-10-0-0-7",
          route: "gpu/7",
          endpoint: "gpu-7",
          "203.0.113.9": "refused",
        },
      }),
      {
        keys: [
          "session_id",
          "state",
          "end_reason",
          "status_url",
          "closed_by",
          "terminated_at",
          "(key, 30 chars)",
          "error",
        ],
        state: "CLOSED",
        codes: {
          state: "CLOSED",
          end_reason: "cap_reached",
          closed_by: "(text, 13 chars)",
          terminated_at: "1790000000",
          "error.code": "session_limit",
          "error.origin": "(text, 27 chars)",
          "error.peer": "(text, 11 chars)",
          "error.route": "(text, 5 chars)",
        },
      },
    );
  });

  it("keeps its state only when it reads as a code", () => {
    assert.deepStrictEqual(Probes.summarizeBody({ state: "reactor.example:443" }), {
      keys: ["state"],
      state: "(text, 19 chars)",
      codes: { state: "(text, 19 chars)" },
    });
  });
});

describe("a refusal's body", () => {
  // What hosted Reactor answers a spent token is unobserved, so its code may sit under any key.
  it("keeps its codes under any key, and no sentence, URL, host or IP address", () => {
    assert.deepStrictEqual(
      Probes.summarizeRefusal({
        code: "SESSION_LIMIT",
        detail: "The token's sessions are used",
        type: "https://errors.example/session-limit",
        seen: "203.0.113.5",
        via: "edge.reactor.example",
        peer: "ip-203-0-113-5",
        error: { code: "session_limit", retryable: false },
      }),
      {
        keys: ["code", "detail", "type", "seen", "via", "peer", "error"],
        codes: {
          code: "SESSION_LIMIT",
          detail: "(text, 29 chars)",
          type: "(text, 36 chars)",
          seen: "(text, 11 chars)",
          via: "(text, 20 chars)",
          peer: "(text, 14 chars)",
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
        error: { endpoint: "api.example", reason: "spent", details: { code: "limit_sessions" } },
      }),
      {
        keys: ["errors", "client_ip", "remoteHost", "error"],
        codes: {
          "errors[0].code": "session_limit",
          "errors[0].status": "403",
          "errors[1]": "token_used",
          "errors[2]": "3",
          "error.reason": "spent",
          "error.details.code": "limit_sessions",
        },
      },
    );
  });
});
