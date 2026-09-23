/** Time options are `Duration.Input`: one normalizer, the unit rule, and the 0.3.0 tombstones. */
// The tombstone test names the removed, deprecated keys on purpose.
/* oxlint-disable typescript/no-deprecated */
import { describe, expect, test } from "vitest";
import { Duration, Effect, Exit, Result } from "effect";
import { TestClock } from "effect/testing";
import type * as H3 from "../src/h3/index.js";
import { duration } from "../src/duration.js";
import { parse, ReactorError } from "../src/errors.js";
import type * as Renewal from "../src/orchestration/renewal.js";
import type { CommandOptions, SessionTimeouts } from "../src/SessionTypes.js";
import type { TokenOptions } from "../src/coordinator/index.js";
import { renewalFixture } from "./orchestration/RenewalFixture.js";
import { run as runSource, runClock, until } from "./orchestration/SourceFixture.js";
import { makeSession, withFixture } from "./fixtures.js";
import { failure, run } from "./harness.js";

const rejected = (evaluate: () => unknown) => {
  const result = parse(evaluate);
  expect(Result.isFailure(result)).toBe(true);
  if (Result.isFailure(result)) {
    expect(result.failure.reason._tag).toBe("InvalidInput");
    expect(result.failure.context.outcome).toBe("not-submitted");
  }
};

describe("duration", () => {
  test("a bare number is milliseconds and a unit string is decoded", () => {
    expect(Duration.toMillis(duration(30, "lead"))).toBe(30);
    expect(Duration.toMillis(duration("30 seconds", "lead"))).toBe(30_000);
    expect(Duration.toMillis(duration({ seconds: 1, milliseconds: 5 }, "lead"))).toBe(1_005);
  });

  test("NaN is rejected in every input shape, although Duration decodes it as zero", () => {
    expect(Duration.isZero(Duration.fromInputUnsafe(Number.NaN))).toBe(true);
    rejected(() => duration(Number.NaN, "timeout", { allowZero: true }));
    rejected(() => duration([Number.NaN, 0], "timeout", { allowZero: true }));
    rejected(() => duration({ seconds: Number.NaN }, "timeout", { allowZero: true }));
  });

  test("negative, zero, infinite, too long and fractional-second input follow the policy", () => {
    rejected(() => duration(-1, "timeout"));
    rejected(() => duration("-1 seconds", "timeout", { allowZero: true }));
    rejected(() => duration("-Infinity", "timeout", { allowInfinite: true }));
    rejected(() => duration(0, "timeout"));
    expect(Duration.isZero(duration(0, "lead", { allowZero: true }))).toBe(true);
    rejected(() => duration("Infinity", "timeout"));
    expect(Duration.isFinite(duration("Infinity", "lifetime", { allowInfinite: true }))).toBe(
      false,
    );
    rejected(() => duration("11 minutes", "timeout", { maximum: "10 minutes" }));
    expect(Duration.toMillis(duration("10 minutes", "timeout", { maximum: "10 minutes" }))).toBe(
      600_000,
    );
    rejected(() => duration(1_500, "token expiry", { wholeSeconds: true }));
    expect(Duration.toSeconds(duration(2_000, "token expiry", { wholeSeconds: true }))).toBe(2);
  });

  test("input that is not a duration is rejected", () => {
    rejected(() => duration("soon" as Duration.Input, "timeout"));
    rejected(() => duration([1] as unknown as Duration.Input, "timeout"));
  });
});

test("renewal lead: a bare 30 is 30 milliseconds, not 30 seconds", () =>
  runClock(
    Effect.gen(function* () {
      // With a one-second lifetime a 30-second lead would prepare at once; 30
      // milliseconds prepares only near expiry.
      const { sources, renewals } = yield* renewalFixture(undefined, { lead: 30 });
      yield* TestClock.adjust(900);
      expect(sources.length).toBe(1);
      yield* until(() => sources.length === 2, TestClock.adjust(20));
      const opened = renewals.find((event) => event._tag === "Opened");
      expect(opened?._tag === "Opened" && Duration.toMillis(opened.lifetime)).toBe(1_000);
    }),
  ));

test("renewal: a NaN lead fails make with InvalidInput", () =>
  runSource(
    Effect.gen(function* () {
      const made = yield* Effect.exit(renewalFixture(undefined, { lead: Number.NaN }));
      expect(Exit.isFailure(made)).toBe(true);
      if (Exit.isFailure(made)) expect(JSON.stringify(made.cause)).toContain("InvalidInput");
    }),
  ));

test("session: a call's replyTimeout overrides the session's, as a Timeout of unknown outcome", ({
  signal,
}) =>
  withFixture(async (fixture) => {
    const { session, peers } = makeSession(fixture, { replyTimeout: "10 minutes" });
    try {
      await run(session.start(), { signal });
      const peer = peers[0]!;
      peer.autoReply = false;
      const started = performance.now();
      const error = await failure(session.command("set_seed", { seed: 1 }, { replyTimeout: 20 }), {
        signal,
      });
      expect(performance.now() - started).toBeLessThan(5_000);
      expect(error.reason._tag).toBe("Timeout");
      expect(error.context.outcome).toBe("unknown");
      expect(error.context.requestId).toBeDefined();
    } finally {
      await run(session.close());
    }
  }));

test("session: invalid time options fail with InvalidInput before any request", () =>
  withFixture(async (fixture) => {
    for (const options of [
      { heartbeatInterval: Number.NaN },
      { heartbeatInterval: 0 },
      { replyTimeout: "11 minutes" },
      { connectTimeout: "Infinity" },
    ] as const) {
      const error = (() => {
        try {
          makeSession(fixture, options);
          return undefined;
        } catch (cause) {
          return cause;
        }
      })();
      expect(ReactorError.is(error) && error.reason._tag).toBe("InvalidInput");
    }
    expect(fixture.calls.length).toBe(0);
  }));

test("removed unit-suffixed keys no longer compile, held or spread", () => {
  const held = { leadSeconds: 30, reconnectTimeoutMs: 10_000 };
  // @ts-expect-error a held renewal configuration with a removed key
  const renewal: Omit<Renewal.Options, "open"> = held;
  // @ts-expect-error spreading it does not hide the removed key
  const spread: Omit<Renewal.Options, "open"> = { ...held };
  // @ts-expect-error commandTimeoutMs became replyTimeout
  const session: SessionTimeouts = { commandTimeoutMs: 5_000 };
  // @ts-expect-error heartbeatMs became heartbeatInterval
  const heartbeat: SessionTimeouts = { heartbeatMs: 0 };
  // @ts-expect-error H3 setupTimeoutMs became setupTimeout
  const provider: H3.Options = { setupTimeoutMs: 1_000 };
  const token: TokenOptions = {
    apiKey: undefined as never,
    modelName: "model",
    maxSessionDuration: "60 seconds",
    expiresAfter: "300 seconds",
    // @ts-expect-error maxSessionDurationSeconds became maxSessionDuration
    maxSessionDurationSeconds: 60,
  };
  const uploads: CommandOptions["uploads"] = new Map();
  const command: CommandOptions = { uploads, replyTimeout: "5 seconds" };
  // @ts-expect-error a bare uploads map is not a command's options object
  const bare: CommandOptions = uploads;
  void [renewal, spread, session, heartbeat, provider, token, command, bare];
});

test("renewal: a lead as long as the source lifetime is refused when the source opens", () =>
  runSource(
    Effect.gen(function* () {
      const made = yield* Effect.exit(renewalFixture(undefined, { lead: "1 second" }));
      expect(Exit.isFailure(made)).toBe(true);
      if (Exit.isFailure(made))
        expect(JSON.stringify(made.cause)).toContain("renewal lead must be shorter");
    }),
  ));
