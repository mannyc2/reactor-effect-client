import { assert, describe, it } from "@effect/vitest";
import * as Probes from "../Probes.js";

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
