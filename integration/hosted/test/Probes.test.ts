import { assert, describe, it } from "@effect/vitest";
import * as Probes from "../Probes.js";

describe("a refusal's body", () => {
  // What hosted Reactor answers a spent token is unobserved, so its code may sit under any key.
  it("keeps its codes under any key, and no sentence, URL or address", () => {
    assert.deepStrictEqual(
      Probes.summarizeRefusal({
        code: "SESSION_LIMIT",
        detail: "The token's sessions are used",
        type: "https://errors.example/session-limit",
        endpoint: "10.0.0.1:443",
        error: { code: "session_limit", retryable: false },
      }),
      {
        keys: ["code", "detail", "type", "endpoint", "error"],
        codes: {
          code: "SESSION_LIMIT",
          detail: "(text, 29 chars)",
          type: "(text, 36 chars)",
          endpoint: "(text, 12 chars)",
          "error.code": "session_limit",
          "error.retryable": "false",
        },
      },
    );
  });
});
