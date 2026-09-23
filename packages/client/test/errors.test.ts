import { describe, expect, test } from "vitest";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import {
  AcquisitionFailure,
  CommandFailure,
  isReactorFailure,
  ReactorError,
} from "../src/index.js";
import { PolicyFailure } from "../src/orchestration/index.js";
import type { EngineError } from "../src/orchestration/index.js";
import { preworkFailure } from "../src/orchestration/request.js";
import type { CloseReport } from "../src/SessionTypes.js";

const provider = { code: "rate_limited", message: "slow down", recoverable: true, status: 429 };
const encoded = (context: object, _tag = "ReactorError"): unknown => ({
  _tag,
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
    expect(failure).not.toBeInstanceOf(ReactorError);
    expect(failure._tag).toBe("CommandFailure");
    expect([failure.code, failure.message, failure.nativeError]).toEqual([
      "Http",
      "request failed",
      provider,
    ]);
    expect(failure.context).toEqual(evidence);
  });

  test("its schema requires the evidence that each outcome implies", () => {
    const decode = Schema.decodeUnknownSync(CommandFailure);
    const command = (context: object) => encoded(context, "CommandFailure");
    expect(decode(command({ operation: "enqueue", outcome: "not-submitted" }))).toBeInstanceOf(
      CommandFailure,
    );
    expect(() => decode(command({ operation: "enqueue", outcome: "unknown" }))).toThrow(
      /requestId/,
    );
    expect(() => decode(command({ outcome: "not-submitted" }))).toThrow(/operation/);
  });

  test("its schema tells a command failure from any other ReactorError", () => {
    const error = new ReactorError({ code: "Timeout", message: "late" });
    const failure = CommandFailure.from(error, { operation: "enqueue", outcome: "not-submitted" });
    expect(Schema.is(CommandFailure)(failure)).toBe(true);
    expect(Schema.is(CommandFailure)(error)).toBe(false);
    expect(Schema.is(ReactorError)(failure)).toBe(false);
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
    expect(failure._tag).toBe("AcquisitionFailure");
    expect(failure.cleanup).toBe(report);
    expect(failure.context).toEqual({ sessionId: "session-1" });
  });
});

describe("PolicyFailure", () => {
  test("a refusal names its reason and was never dispatched", () => {
    const refusal = PolicyFailure.refuse("busy", "Canvas can only change while idle", "canvas");
    expect(refusal._tag).toBe("PolicyFailure");
    expect(refusal).not.toBeInstanceOf(CommandFailure);
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

describe("the four failure classes", () => {
  const report: CloseReport = Object.freeze({
    localClosed: true,
    allocation: "none",
    remote: {
      attempted: false,
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
  const error = new ReactorError({ code: "Timeout", message: "late" });
  const command = CommandFailure.from(error, { operation: "enqueue", outcome: "not-submitted" });
  const acquisition = AcquisitionFailure.from(error, report);
  const policy = PolicyFailure.refuse("queue_full", "The generation queue is full");

  test("each has its own tag, so catchTag separates a policy refusal from a command failure", async () => {
    const route = (failure: EngineError) =>
      Effect.runPromise(
        Effect.fail(failure).pipe(
          Effect.catchTag("PolicyFailure", (refusal) => Effect.succeed(`policy ${refusal.reason}`)),
          Effect.orElseSucceed(() => "not a refusal"),
        ),
      );
    expect(await route(policy)).toBe("policy queue_full");
    expect(await route(command)).toBe("not a refusal");
  });

  test("one guard recognizes every class and each class guard only its own", () => {
    const all = [error, command, acquisition, policy];
    expect(all.map(isReactorFailure)).toEqual([true, true, true, true]);
    expect(all.map((value) => ReactorError.is(value))).toEqual([true, false, false, false]);
    expect(all.map((value) => CommandFailure.is(value))).toEqual([false, true, false, false]);
    expect(all.map((value) => AcquisitionFailure.is(value))).toEqual([false, false, true, false]);
    expect(all.map((value) => PolicyFailure.is(value))).toEqual([false, false, false, true]);
    expect([
      isReactorFailure(new Error("late")),
      isReactorFailure({ _tag: "ReactorError" }),
    ]).toEqual([false, false]);
  });

  test("a Schema union round-trip keeps each class", () => {
    const Failure = Schema.Union([ReactorError, CommandFailure, AcquisitionFailure, PolicyFailure]);
    for (const value of [error, command, acquisition, policy]) {
      const decoded = Schema.decodeSync(Failure)(Schema.encodeSync(Failure)(value));
      expect(decoded).toBeInstanceOf(value.constructor);
      expect(decoded._tag).toBe(value._tag);
    }
  });

  test("a policy refusal's outcome is the literal not-submitted", () => {
    const outcome: "not-submitted" = policy.context.outcome;
    // An unknown-first classifier reads the outcome of either engine failure without narrowing.
    const admission = (failure: EngineError): "unknown" | "defer" | "lost" =>
      failure.context.outcome === "unknown"
        ? "unknown"
        : failure._tag === "PolicyFailure"
          ? "defer"
          : "lost";
    expect([outcome, admission(policy), admission(command)]).toEqual([
      "not-submitted",
      "defer",
      "lost",
    ]);
  });

  test("caller-owned prework keeps a refusal and a not-submitted command failure unchanged", () => {
    expect(preworkFailure("enqueue", policy)).toBe(policy);
    expect(preworkFailure("enqueue", command)).toBe(command);
    const wrapped = preworkFailure("enqueue", acquisition);
    expect(CommandFailure.is(wrapped) && [wrapped.code, wrapped.context]).toEqual([
      "Timeout",
      { operation: "enqueue", outcome: "not-submitted", detail: acquisition },
    ]);
  });
});
