// NodeHttpServer serves on a server made by Node's own http module.
// @effect-diagnostics-next-line nodeBuiltinImport:off
import { createServer } from "node:http";
import * as NodeHttpServer from "@effect/platform-node/NodeHttpServer";
import { assert, describe, it, layer } from "@effect/vitest";
import { Effect, Redacted, Ref } from "effect";
import * as FetchHttpClient from "effect/http/FetchHttpClient";
import * as HttpServer from "effect/http/HttpServer";
import * as HttpServerResponse from "effect/http/HttpServerResponse";
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

describe("a session's state", () => {
  // States come from provider replies too, and go into the evidence.
  it("is kept only when it reads as a code", () => {
    assert.deepStrictEqual(
      ["ACTIVE", "CLOSED", "10.0.0.7:8443", "https://reactor.example/s"].map(Probes.keptText),
      ["ACTIVE", "CLOSED", "(text, 13 chars)", "(text, 25 chars)"],
    );
  });
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
          via: "wss://gpu-7.reactor.example",
          seen: "ip-10-0-0-7",
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
          "error.via": "(text, 27 chars)",
          "error.seen": "(text, 11 chars)",
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
        last_seen: "ip-203-0-113-5",
        error: { code: "session_limit", retryable: false },
      }),
      {
        keys: ["code", "detail", "type", "seen", "via", "last_seen", "error"],
        codes: {
          code: "SESSION_LIMIT",
          detail: "(text, 29 chars)",
          type: "(text, 36 chars)",
          seen: "(text, 11 chars)",
          via: "(text, 20 chars)",
          last_seen: "(text, 14 chars)",
          "error.code": "session_limit",
          "error.retryable": "false",
        },
      },
    );
  });

  // A 2xx reply that names no session is summarized so too, and it may carry what a refusal
  // would not: a session's addresses and credentials.
  it("keeps no IPv6 or MAC address, no long digest, and nothing under an address or secret", () => {
    assert.deepStrictEqual(
      Probes.summarizeRefusal({
        code: "session_limit",
        session_id: "0de7cc3a-aaa4-45b7-8286-bc0711636013",
        seen_v6: "2001-db8--1",
        seen_v6_full: "2600-1f18-0-0-0-0-0-1",
        seen_hw: "00-1A-2B-3C-4D-5E",
        digest: "a".repeat(64),
        IPAddress: "gpu-7",
        endpoints: ["gpu-7"],
        port: 3478,
        token: "t0k3n",
        credentials: { api_key: "k3y", password: "pw" },
        signature: "s1g",
      }),
      {
        keys: [
          "code",
          "session_id",
          "seen_v6",
          "seen_v6_full",
          "seen_hw",
          "digest",
          "IPAddress",
          "endpoints",
          "port",
          "token",
          "credentials",
          "signature",
        ],
        codes: {
          code: "session_limit",
          session_id: "0de7cc3a-aaa4-45b7-8286-bc0711636013",
          seen_v6: "(text, 11 chars)",
          seen_v6_full: "(text, 21 chars)",
          seen_hw: "(text, 17 chars)",
          digest: "(text, 64 chars)",
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
