import { assert, describe, it } from "@effect/vitest";
import { Cause, Redacted } from "effect";
import { ReactorError } from "reactor-effect-client/ReactorError";
import * as Run from "../Run.js";

/** A connection the addon failed, as the native peer reports it: its sentence kept redacted. */
const failed = (code: "Protocol" | "Overflow", backend: string) =>
  ReactorError.fromCode(code, `native peer failed (${code})`, {
    detail: { backendMessage: Redacted.make(backend) },
  });

describe("a native connection failure in the evidence", () => {
  it("names which of the addon's own checks failed", () => {
    assert.strictEqual(
      Run.describe(
        Cause.fail(failed("Protocol", "data data channel delivered a nonbinary message")),
      ),
      "Protocol: native peer failed (Protocol): data data channel delivered a nonbinary message",
    );
  });

  it("keeps any other backend text out", () => {
    assert.strictEqual(
      Run.describe(Cause.fail(failed("Protocol", "v=0 o=- 4611 2 IN IP4 127.0.0.1"))),
      "Protocol: native peer failed (Protocol)",
    );
  });
});
