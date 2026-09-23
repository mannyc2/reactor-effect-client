import { describe, expect, test } from "vitest";
import * as Schema from "effect/Schema";
import { AcquisitionFailure, CommandFailure, ReactorError } from "../src/index.js";
import { PolicyFailure } from "../src/orchestration/index.js";
import type { CloseReport } from "../src/SessionTypes.js";

const provider = { code: "rate_limited", message: "slow down", recoverable: true, status: 429 };
const encoded = (context: object): unknown => ({
  _tag: "ReactorError",
  code: "Timeout",
  message: "late",
  context,
});

describe("ReactorError", () => {
  test("an error built without context has an empty one", () => {
    expect(new ReactorError({ code: "Timeout", message: "late" }).context).toEqual({});
  });

  test("diagnostic JSON leaves out response bodies and causes", () => {
    const error = new ReactorError({
      code: "Http",
      message: "request failed",
      context: { operation: "create", generation: 7n, body: "secret body", detail: "secret" },
      nativeError: provider,
    });
    expect(JSON.parse(JSON.stringify(error))).toEqual({
      _tag: "ReactorError",
      code: "Http",
      message: "request failed",
      context: { operation: "create", generation: "7" },
      provider: { code: "rate_limited", status: 429 },
    });
  });

  test("decodes through its schema", () => {
    const decoded = Schema.decodeUnknownSync(ReactorError)(encoded({ sessionId: "session-1" }));
    expect(decoded).toBeInstanceOf(ReactorError);
    expect(decoded.context).toEqual({ sessionId: "session-1" });
  });
});

describe("CommandFailure", () => {
  test("keeps the failure and replaces its context with the dispatch evidence", () => {
    const error = new ReactorError({
      code: "Http",
      message: "request failed",
      context: { status: 500 },
      nativeError: provider,
    });
    const evidence = {
      operation: "enqueue",
      outcome: "unknown",
      requestId: "request-1",
      generation: 3n,
    } as const;
    const failure = CommandFailure.from(error, evidence);
    expect(failure).toBeInstanceOf(ReactorError);
    expect(failure._tag).toBe("ReactorError");
    expect([failure.code, failure.message, failure.nativeError]).toEqual([
      "Http",
      "request failed",
      provider,
    ]);
    expect(failure.context).toEqual(evidence);
  });

  test("its schema requires the evidence that each outcome implies", () => {
    const decode = Schema.decodeUnknownSync(CommandFailure);
    expect(decode(encoded({ operation: "enqueue", outcome: "not-submitted" }))).toBeInstanceOf(
      CommandFailure,
    );
    expect(() => decode(encoded({ operation: "enqueue", outcome: "unknown" }))).toThrow(
      /requestId/,
    );
    expect(() => decode(encoded({ outcome: "not-submitted" }))).toThrow(/operation/);
  });

  test("its schema tells a command failure from any other ReactorError", () => {
    const error = new ReactorError({ code: "Timeout", message: "late" });
    const failure = CommandFailure.from(error, { operation: "enqueue", outcome: "not-submitted" });
    expect(Schema.is(CommandFailure)(failure)).toBe(true);
    expect(Schema.is(CommandFailure)(error)).toBe(false);
    expect(Schema.is(ReactorError)(failure)).toBe(true);
  });
});

describe("AcquisitionFailure", () => {
  test("keeps the failure and the very report its partial lease produced", () => {
    const report: CloseReport = Object.freeze({
      localClosed: true,
      allocation: "known",
      sessionId: "session-1",
      remote: {
        attempted: true,
        responseReceived: false,
        confirmed: false,
        evidence: null,
        deleteStatus: null,
        state: null,
      },
      unpublishSubmitted: [],
      unresolvedPublications: [],
      localErrors: [],
    });
    const error = new ReactorError({
      code: "Timeout",
      message: "late",
      context: { sessionId: "session-1" },
    });
    const failure = AcquisitionFailure.from(error, report);
    expect(failure).toBeInstanceOf(ReactorError);
    expect(failure.cleanup).toBe(report);
    expect(failure.context).toEqual({ sessionId: "session-1" });
  });
});

describe("PolicyFailure", () => {
  test("a refusal names its reason and was never dispatched", () => {
    const refusal = PolicyFailure.refuse("busy", "Canvas can only change while idle", "canvas");
    expect(refusal).toBeInstanceOf(CommandFailure);
    expect([refusal.code, refusal.reason]).toEqual(["InvalidState", "busy"]);
    expect(refusal.context).toEqual({ operation: "canvas", outcome: "not-submitted" });
  });

  test("an invalid request is invalid input and keeps its cause", () => {
    const cause = new Error("unsupported field");
    const refusal = PolicyFailure.refuse(
      "invalid_request",
      "Clip request is malformed",
      undefined,
      cause,
    );
    expect(refusal.code).toBe("InvalidInput");
    expect(refusal.context).toEqual({
      operation: "enqueue",
      outcome: "not-submitted",
      detail: cause,
    });
  });
});
