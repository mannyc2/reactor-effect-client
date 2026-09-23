import { describe, expect, expectTypeOf, test } from "vitest";
import * as Duration from "effect/Duration";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Cause from "effect/Cause";
import * as Exit from "effect/Exit";
import * as Option from "effect/Option";
import * as Redacted from "effect/Redacted";
import * as Result from "effect/Result";
import * as Schema from "effect/Schema";
import * as Root from "../src/index.js";
import {
  AcquisitionFailure,
  CommandFailure,
  Failure,
  Http,
  isReactorFailure,
  Native,
  ReactorError,
  Remote,
} from "../src/index.js";
import { Correlator } from "../src/correlation.js";
import { parse, parsed, parsedInput } from "../src/errors.js";
import { json } from "../src/json.js";
// @ts-expect-error ProviderFailure is no longer exported from the root.
import type { ProviderFailure } from "../src/index.js";
import type { ErrorContext } from "../src/index.js";
import { PolicyFailure } from "../src/orchestration/index.js";
import type { EngineError } from "../src/orchestration/index.js";
import { preworkFailure } from "../src/orchestration/request.js";
import type { CloseReport } from "../src/SessionTypes.js";

type Equals<A, B> = [A] extends [B] ? ([B] extends [A] ? true : false) : false;

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
const late = ReactorError.fromCode("Timeout", "late");
const command = CommandFailure.from(late, { operation: "enqueue", outcome: "not-submitted" });
const acquisition = AcquisitionFailure.from(late, report);
const policy = PolicyFailure.refuse("QueueFull", "The generation queue is full");
const encoded = (context: object, _tag = "ReactorError"): unknown => ({
  _tag,
  reason: { _tag: "Timeout", message: "late" },
  context,
});

describe("ReactorError", () => {
  test("its reason carries the code and the message", () => {
    expect([late.reason._tag, late.message, late.context]).toEqual(["Timeout", "late", {}]);
    expect(late.reason).toBeInstanceOf(Root.Failure);
    // The reason is library-authored, so it is the native cause.
    expect(late.cause).toBe(late.reason);
  });

  test("fromCode builds the reason each code names", () => {
    expect(ReactorError.fromCode("Http", "down").reason).toBeInstanceOf(Http);
    expect(ReactorError.fromCode("RecorderDisabled", "clip failed").reason).toBeInstanceOf(Remote);
    expect(ReactorError.fromCode("Native", "native failed").reason).toBeInstanceOf(Native);
    expect(Root.ErrorCode.literals).toContain("IceFailed");
  });

  test("diagnostic JSON leaves out response bodies, backend text and details", () => {
    const http = new ReactorError({
      reason: new Http({
        message: "request failed",
        status: 429,
        retryAfter: Duration.seconds(2),
        body: "secret body",
      }),
      context: { operation: "create", generation: 7n, detail: "secret detail" },
    });
    expect(JSON.parse(JSON.stringify(http))).toEqual({
      _tag: "ReactorError",
      message: "request failed",
      reason: { _tag: "Http", message: "request failed", status: 429, retryAfterMillis: 2000 },
      context: { operation: "create", generation: "7" },
    });
    // A reason serialized on its own keeps the body out as well.
    expect(JSON.stringify(http.reason)).not.toContain("secret");
    const native = new ReactorError({
      reason: new Native({
        message: "native call failed (Native)",
        status: -2,
        backendMessage: Redacted.make("a=ice-pwd:secret"),
      }),
    });
    expect(JSON.stringify(native)).not.toContain("secret");
    expect(JSON.stringify(native.reason)).not.toContain("secret");
    expect((JSON.parse(JSON.stringify(native)) as { readonly reason: unknown }).reason).toEqual({
      _tag: "Native",
      message: "native call failed (Native)",
      status: -2,
    });
  });

  test("decodes through its schema", () => {
    const decoded = Schema.decodeUnknownSync(ReactorError)(encoded({ sessionId: "session-1" }));
    expect(decoded).toBeInstanceOf(ReactorError);
    expect(decoded.reason).toBeInstanceOf(Root.Failure);
    expect(decoded.context).toEqual({ sessionId: "session-1" });
  });

  test("catchReason and catchReasons route on the reason's tag", async () => {
    const failing = (error: ReactorError): Effect.Effect<string, ReactorError> =>
      Effect.fail(error);
    const timeout = await Effect.runPromise(
      failing(late).pipe(
        Effect.catchReason("ReactorError", "Timeout", (reason) =>
          Effect.succeed(`timeout: ${reason.message}`),
        ),
      ),
    );
    expect(timeout).toBe("timeout: late");
    const http = new ReactorError({ reason: new Http({ message: "busy", status: 503 }) });
    const routed = await Effect.runPromise(
      failing(http).pipe(
        Effect.catchReasons("ReactorError", {
          Timeout: () => Effect.succeed("timeout"),
          Http: (reason) => Effect.succeed(`http ${reason.status}`),
        }),
      ),
    );
    expect(routed).toBe("http 503");
    const other = await Effect.runPromiseExit(
      failing(http).pipe(Effect.catchReason("ReactorError", "Timeout", () => Effect.succeed(""))),
    );
    expect(Exit.isFailure(other)).toBe(true);
    const unwrapped = await Effect.runPromise(
      failing(http).pipe(
        Effect.unwrapReason("ReactorError"),
        Effect.catchTag("Http", (reason) => Effect.succeed(reason.status)),
        Effect.orElseSucceed(() => 0),
      ),
    );
    expect(unwrapped).toBe(503);
  });
});

describe("CommandFailure", () => {
  test("keeps the reason and replaces the context with the dispatch evidence", () => {
    const error = new ReactorError({
      reason: new Http({ message: "request failed", status: 500 }),
      context: { sessionId: "session-1" },
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
    expect([failure.reason, failure.message]).toEqual([error.reason, "request failed"]);
    expect(failure.context).toEqual(evidence);
  });

  test("its schema requires the evidence that each outcome implies", () => {
    const decode = Schema.decodeUnknownSync(CommandFailure);
    const failure = (context: object) => encoded(context, "CommandFailure");
    expect(decode(failure({ operation: "enqueue", outcome: "not-submitted" }))).toBeInstanceOf(
      CommandFailure,
    );
    expect(() => decode(failure({ operation: "enqueue", outcome: "unknown" }))).toThrow(
      /requestId/,
    );
    expect(() => decode(failure({ outcome: "not-submitted" }))).toThrow(/operation/);
  });

  test("its schema tells a command failure from any other ReactorError", () => {
    expect(Schema.is(CommandFailure)(command)).toBe(true);
    expect(Schema.is(CommandFailure)(late)).toBe(false);
    expect(Schema.is(ReactorError)(command)).toBe(false);
  });
});

describe("AcquisitionFailure", () => {
  test("keeps the reason, the context and the very report its partial lease produced", () => {
    const error = ReactorError.fromCode("Timeout", "late", { sessionId: "session-1" });
    const failure = AcquisitionFailure.from(error, report);
    expect(failure._tag).toBe("AcquisitionFailure");
    expect(failure.reason).toBe(error.reason);
    expect(failure.cleanup).toBe(report);
    expect(failure.context).toEqual({ sessionId: "session-1" });
  });
});

describe("PolicyFailure", () => {
  test("a refusal names its reason and was never dispatched", () => {
    const refusal = PolicyFailure.refuse("Busy", "Canvas can only change while idle", "canvas");
    expect(refusal._tag).toBe("PolicyFailure");
    expect([refusal.reason._tag, refusal.message]).toEqual([
      "Busy",
      "Canvas can only change while idle",
    ]);
    expect(refusal.context).toEqual({ operation: "canvas", outcome: "not-submitted" });
  });

  test("an invalid request keeps its cause for inspection", () => {
    const cause = new Error("unsupported field");
    const refusal = PolicyFailure.refuse(
      "InvalidRequest",
      "Clip request is malformed",
      undefined,
      cause,
    );
    expect(refusal.reason._tag).toBe("InvalidRequest");
    expect(refusal.context).toEqual({
      operation: "enqueue",
      outcome: "not-submitted",
      detail: cause,
    });
  });

  test("an unowned clip names its request field; a sequence refusal carries its code", () => {
    const missing = PolicyFailure.missing("session_anchor");
    expect([missing.reason, missing.message]).toEqual([
      expect.objectContaining({ _tag: "Missing", purpose: "session_anchor" }),
      "The session_anchor clip has no known owning session",
    ]);
    const sequence = PolicyFailure.sequence("run", "sealed", "enqueue");
    expect([sequence.reason, sequence.message]).toEqual([
      expect.objectContaining({ _tag: "Sequence", sequenceId: "run", code: "sealed" }),
      "Sequence run: sealed",
    ]);
    expect(JSON.parse(JSON.stringify(sequence))).toEqual({
      _tag: "PolicyFailure",
      message: "Sequence run: sealed",
      reason: {
        _tag: "Sequence",
        message: "Sequence run: sealed",
        sequenceId: "run",
        code: "sealed",
      },
      context: { operation: "enqueue", outcome: "not-submitted" },
    });
  });

  test("catchTag and catchReasons route a refusal by class and by reason", async () => {
    const route = (failure: EngineError) =>
      Effect.runPromise(
        Effect.fail(failure).pipe(
          Effect.catchReasons("PolicyFailure", {
            QueueFull: () => Effect.succeed("defer: queue full"),
            SessionRecovering: () => Effect.succeed("defer: recovering"),
          }),
          Effect.catchTag("PolicyFailure", (refusal) =>
            Effect.succeed(`refused: ${refusal.reason._tag}`),
          ),
          Effect.orElseSucceed(() => "not a refusal"),
        ),
      );
    expect(await route(policy)).toBe("defer: queue full");
    expect(
      await route(PolicyFailure.refuse("SessionRecovering", "Waiting for a coherent snapshot")),
    ).toBe("defer: recovering");
    expect(await route(PolicyFailure.refuse("Busy", "busy", "set_canvas"))).toBe("refused: Busy");
    expect(await route(command)).toBe("not a refusal");
  });
});

describe("the four failure classes", () => {
  test("one guard recognizes every class and each class guard only its own", () => {
    const all = [late, command, acquisition, policy];
    expect(all.map((value) => isReactorFailure(value))).toEqual([true, true, true, true]);
    expect(all.map((value) => ReactorError.is(value))).toEqual([true, false, false, false]);
    expect(all.map((value) => CommandFailure.is(value))).toEqual([false, true, false, false]);
    expect(all.map((value) => AcquisitionFailure.is(value))).toEqual([false, false, true, false]);
    expect(all.map((value) => PolicyFailure.is(value))).toEqual([false, false, false, true]);
    expect([
      isReactorFailure(new Error("late")),
      isReactorFailure({ _tag: "ReactorError" }),
    ]).toEqual([false, false]);
  });

  test("a Schema union round-trip keeps each class and its reason", () => {
    const Failure = Schema.Union([ReactorError, CommandFailure, AcquisitionFailure, PolicyFailure]);
    const http = new ReactorError({
      reason: new Http({ message: "busy", status: 503, retryAfter: Duration.seconds(1) }),
      context: { generation: 2n },
    });
    const refused = new ReactorError({
      reason: new Remote({ _tag: "RecorderDisabled", message: "clip failed", body: "disabled" }),
    });
    for (const value of [late, http, refused, command, acquisition, policy]) {
      const decoded = Schema.decodeSync(Failure)(Schema.encodeSync(Failure)(value));
      expect(decoded).toBeInstanceOf(value.constructor);
      expect(decoded.reason).toBeInstanceOf(value.reason.constructor);
      expect(decoded.reason).toEqual(value.reason);
      expect(decoded.context).toEqual(value.context);
    }
  });

  test("a policy refusal's outcome is the literal not-submitted", () => {
    const literal: Equals<PolicyFailure["context"]["outcome"], "not-submitted"> = true;
    // An unknown-first classifier reads the outcome of either engine failure without narrowing.
    const admission = (failure: EngineError): "unknown" | "defer" | "lost" =>
      failure.context.outcome === "unknown"
        ? "unknown"
        : failure._tag === "PolicyFailure"
          ? "defer"
          : "lost";
    expect([literal, admission(policy), admission(command)]).toEqual([true, "defer", "lost"]);
  });

  test("the root drops ProviderFailure and nativeError, and exports ErrorContext as a type", () => {
    const context: ErrorContext = { operation: "create" };
    // @ts-expect-error nativeError is gone; typed reasons carry the provider fields.
    const nativeError: unknown = late.nativeError;
    // The failed import above resolves to nothing.
    expectTypeOf<ProviderFailure>().toBeAny();
    expect([context.operation, nativeError]).toEqual(["create", undefined]);
    expect(["ProviderFailure", "ErrorContext"].filter((name) => name in Root)).toEqual([]);
    // @ts-expect-error ErrorContext is exported as a type only.
    expect(Root.ErrorContext).toBeUndefined();
  });

  test("caller-owned prework keeps a refusal and a not-submitted command failure unchanged", () => {
    expect(preworkFailure("enqueue", policy)).toBe(policy);
    expect(preworkFailure("enqueue", command)).toBe(command);
    const wrapped = preworkFailure("enqueue", acquisition);
    expect(CommandFailure.is(wrapped) && [wrapped.reason, wrapped.context]).toEqual([
      acquisition.reason,
      { operation: "enqueue", outcome: "not-submitted", detail: acquisition },
    ]);
  });
});

describe("classification", () => {
  const retryable = [
    new Failure({ _tag: "Overflow", message: "pending request bound reached" }),
    new Failure({ _tag: "Disconnected", message: "peer state failed" }),
    new Failure({ _tag: "ChannelClosed", message: "data channel closed" }),
    new Http({ message: "network/read failure" }),
    new Http({ message: "HTTP 429", status: 429, retryAfter: Duration.seconds(1) }),
    new Http({ message: "HTTP 503", status: 503 }),
  ];
  const unknown = {
    operation: "enqueue",
    outcome: "unknown",
    requestId: "request-1",
    generation: 1n,
  } as const;

  test("isRetryable is never true for an unknown outcome", () => {
    for (const reason of retryable) {
      expect(reason.isRetryable).toBe(true);
      expect(new CommandFailure({ reason, context: unknown }).isRetryable).toBe(false);
      expect(new ReactorError({ reason, context: { outcome: "unknown" } }).isRetryable).toBe(false);
      expect(
        new AcquisitionFailure({ reason, context: { outcome: "unknown" }, cleanup: report })
          .isRetryable,
      ).toBe(false);
      expect(
        new CommandFailure({ reason, context: { operation: "enqueue", outcome: "not-submitted" } })
          .isRetryable,
      ).toBe(true);
    }
  });

  test("backpressure and a connection lost before dispatch are retryable", () => {
    const correlator = new Correlator<unknown>("data", 2);
    const sent = correlator.register(1n, "sent");
    sent.submitted = true;
    const unsent = correlator.register(1n, "unsent");
    let bound: unknown;
    try {
      correlator.register(1n, "third");
    } catch (error) {
      bound = error;
    }
    expect(ReactorError.is(bound) && [bound.reason._tag, bound.isRetryable]).toEqual([
      "Overflow",
      true,
    ]);
    correlator.failGeneration(1n, ReactorError.fromCode("Disconnected", "peer state failed"));
    const outcomes = [sent, unsent].map((pending) =>
      Effect.runSync(Effect.flip(Deferred.await(pending.deferred))),
    );
    expect(outcomes.map((error) => [error.context.outcome, error.isRetryable])).toEqual([
      ["unknown", false],
      ["not-submitted", true],
    ]);
    expect(policy.isRetryable).toBe(true);
    expect(
      PolicyFailure.refuse("SessionRecovering", "Owning session is recovering").isRetryable,
    ).toBe(true);
    expect(PolicyFailure.refuse("Busy", "busy", "set_canvas").isRetryable).toBe(false);
    expect(PolicyFailure.sequence("run", "sealed").isRetryable).toBe(false);
  });

  test("a refusal from the provider and a local bug are not retryable", () => {
    for (const reason of [
      new Remote({ _tag: "Remote", message: "remote command error MODEL_ERROR" }),
      new Http({ message: "HTTP 400", status: 400 }),
      new Failure({ _tag: "InvalidInput", message: "invalid" }),
      new Failure({ _tag: "Timeout", message: "deadline" }),
    ])
      expect(new ReactorError({ reason }).isRetryable).toBe(false);
  });

  test("retryAfter surfaces the delay an Http reason named", () => {
    const limited = new ReactorError({
      reason: new Http({ message: "HTTP 429", status: 429, retryAfter: Duration.millis(1250) }),
      context: { outcome: "replied" },
    });
    expect(limited.retryAfter && Duration.toMillis(limited.retryAfter)).toBe(1250);
    expect(CommandFailure.from(limited, unknown).retryAfter).toEqual(limited.retryAfter);
    expect([late.retryAfter, policy.retryAfter]).toEqual([undefined, undefined]);
  });
});

describe("parsers", () => {
  test("a parser's ReactorError is a typed failure; anything else it throws stays a defect", () => {
    expect(parse(() => 1)).toEqual(Result.succeed(1));
    const rejected = parse(() => {
      throw ReactorError.fromCode("Protocol", "expected object");
    });
    expect(Result.isFailure(rejected) && rejected.failure.reason._tag).toBe("Protocol");
    expect(() =>
      parse(() => {
        throw new TypeError("a bug");
      }),
    ).toThrow(TypeError);
    const bug = Effect.runSyncExit(
      parsed(() => {
        throw new TypeError("a bug");
      }),
    );
    expect(Exit.isFailure(bug) && [Cause.hasDies(bug.cause), Cause.hasFails(bug.cause)]).toEqual([
      true,
      false,
    ]);
  });

  test("caller input chooses InvalidInput and not-submitted whatever the parser's category", () => {
    const exit = Effect.runSyncExit(parsedInput(() => json(Number.NaN), "command"));
    const failure = Exit.isFailure(exit) ? Cause.findErrorOption(exit.cause) : undefined;
    expect(failure !== undefined && Option.isSome(failure) && failure.value).toMatchObject({
      reason: { _tag: "InvalidInput", message: "expected finite number: JSON number" },
      context: { operation: "command", outcome: "not-submitted" },
    });
  });
});
