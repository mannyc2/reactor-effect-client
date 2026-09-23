// The json() walker's own bounds, and how each caller-input site surfaces a violation: as a typed
// failure that was never submitted, not as a defect.
import { describe, expect, test } from "vitest";
import { Cause, Crypto, Effect, Exit, Option, Scope } from "effect";
import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import * as Http from "effect/unstable/http/HttpClient";
import * as Coordinator from "../src/coordinator/index.js";
import { json, structFromObject } from "../src/json.js";
import type { ReactorFailure } from "../src/errors.js";
import type { JsonObject } from "../src/json.js";
import type { CommandReply } from "../src/SessionTypes.js";
import * as H3 from "../src/h3/index.js";
import { makeSession, withFixture } from "./fixtures.js";
import type { MockPeer } from "./fixtures.js";
import { fixture as h3Fixture } from "./h3/ProviderSession.js";

const sparse = (): unknown[] => {
  const holes: unknown[] = [1];
  holes[2] = 3; // index 1 is a hole: not an own property
  return holes;
};
const getterArray = (onGet: () => void): unknown[] => {
  const input: unknown[] = [];
  Object.defineProperty(input, "0", {
    enumerable: true,
    configurable: true,
    get() {
      onGet();
      return 1;
    },
  });
  return input;
};
/** The thrown message, or "<no throw>"; keeps multi-megabyte values out of assertion diffs. */
const thrown = (body: () => unknown): string => {
  try {
    body();
    return "<no throw>";
  } catch (error) {
    return error instanceof Error ? error.message : String(error);
  }
};
const bigKeys = (): Record<string, unknown> => ({
  ["a".repeat(2_100_000)]: 1,
  ["b".repeat(2_100_000)]: false,
});

describe("json() structural bounds", () => {
  test("cumulative JSON keys count toward the text budget with scalar values", () => {
    expect(thrown(() => json(bigKeys()))).toBe("JSON text limit");
  });

  test("JSON array accessors are rejected without executing them", () => {
    let called = 0;
    const input = getterArray(() => called++);
    expect(thrown(() => json(input))).toBe("JSON accessors are not supported");
    expect(called).toBe(0);
  });

  test("a 100M-slot sparse array is charged against the node limit before any slot is read", () => {
    const huge: unknown[] = [];
    huge.length = 100_000_000;
    const started = performance.now();
    expect(thrown(() => json(huge))).toBe("JSON node/depth limit");
    // Array.map skips holes, so this once passed the node limit and took seconds to copy.
    expect(performance.now() - started).toBeLessThan(1_000);
  });

  test("a small sparse array is not JSON (JSON.stringify would invent null)", () => {
    expect(thrown(() => json(sparse()))).toBe("sparse arrays are not JSON");
  });

  test("an array's non-index own keys are never read and are absent from the copy", () => {
    let called = 0;
    const input: unknown[] = [1, "two"];
    Object.defineProperty(input, "extra", { value: true, enumerable: true });
    Object.defineProperty(input, Symbol("tag"), { value: 1, enumerable: true });
    Object.defineProperty(input, "computed", {
      enumerable: true,
      get() {
        called++;
        return 1;
      },
    });
    const copy = json(input);
    expect(called).toBe(0);
    expect(copy).toEqual([1, "two"]);
    expect(Object.getOwnPropertyNames(copy)).toEqual(["0", "1", "length"]);
    expect(Object.getOwnPropertySymbols(copy)).toEqual([]);
    expect(Object.isFrozen(copy)).toBe(true);
    expect(JSON.stringify(copy)).toBe(JSON.stringify(input));
    expect(json(/-/.exec("a-b"))).toEqual(["-"]);
  });

  // Controls: behaviour the fix must preserve.
  test("control: dense arrays, cumulative string values and the node bound", () => {
    expect(json([1, "a", [true, null], { k: [] }])).toEqual([1, "a", [true, null], { k: [] }]);
    expect(Object.isFrozen(json([1, 2]))).toBe(true);
    expect(thrown(() => json(["a".repeat(2_100_000), "b".repeat(2_100_000)]))).toBe(
      "JSON text limit",
    );
    expect(thrown(() => json(Array.from({ length: 65_536 }, () => 0)))).toBe(
      "JSON node/depth limit",
    );
    expect(json(Array.from({ length: 65_535 }, () => 0))).toHaveLength(65_535);
  });
});

const dataSent = (peer: MockPeer) => peer.sent.filter((s) => s.channel === "data").length;
const expectTypedNotSubmitted = (exit: Exit.Exit<unknown, ReactorFailure>) => {
  expect(Exit.isFailure(exit)).toBe(true);
  if (Exit.isFailure(exit)) {
    expect(Cause.hasDies(exit.cause)).toBe(false);
    const error = Cause.findErrorOption(exit.cause);
    expect(Option.isSome(error)).toBe(true);
    if (Option.isSome(error)) {
      expect(error.value.context).toMatchObject({ outcome: "not-submitted" });
      return error.value;
    }
  }
  throw new Error("expected a typed failure");
};

describe("public call sites: json() throws are typed failures, not defects", () => {
  test("coordinator limits: invalid requestTimeout is a typed, lazy InvalidInput", async () => {
    const platform = Http.make(() => Effect.die("invalid input must not execute HTTP"));
    const exit = await Effect.runPromiseExit(
      Coordinator.make({ requestTimeout: 0 }).pipe(
        Effect.provideService(Http.HttpClient, platform),
      ),
    );
    expect(expectTypedNotSubmitted(exit).reason._tag).toBe("InvalidInput");
  });

  const commandCase = (
    name: string,
    data: () => unknown,
    check?: (failure: ReactorFailure) => void,
  ) =>
    test(`session.command input: ${name} is typed, not submitted, and sends nothing`, () =>
      withFixture(async (fixture) => {
        const { session, peers } = makeSession(fixture);
        try {
          await Effect.runPromise(session.start());
          const peer = peers[0];
          if (peer === undefined) throw new Error("missing peer");
          const before = dataSent(peer);
          const exit = await Effect.runPromiseExit(session.command("fixture", data()));
          const failure = expectTypedNotSubmitted(exit);
          check?.(failure);
          expect(dataSent(peer)).toBe(before);
        } finally {
          await Effect.runPromise(session.close());
        }
      }));

  commandCase("undefined value (control)", () => ({ bad: undefined }));
  commandCase("non-finite number (control)", () => ({ bad: Number.NaN }));
  commandCase("sparse array", () => ({ list: sparse() }));
  commandCase("million-slot sparse array", () => ({ list: new Array<unknown>(1_000_000) }));
  commandCase("cumulative key text over 4 MiB", bigKeys, (failure) =>
    expect(failure.reason._tag).toBe("InvalidInput"),
  );

  test("session.command input: an array index getter is never executed", () =>
    withFixture(async (fixture) => {
      const { session } = makeSession(fixture);
      let called = 0;
      try {
        await Effect.runPromise(session.start());
        const exit = await Effect.runPromiseExit(
          session.command("fixture", { list: getterArray(() => called++) }),
        );
        expectTypedNotSubmitted(exit);
        expect(called).toBe(0);
      } finally {
        await Effect.runPromise(session.close());
      }
    }));

  const runH3 = <A, E>(effect: Effect.Effect<A, E, Scope.Scope | Crypto.Crypto>) =>
    Effect.runPromise(Effect.scoped(effect.pipe(Effect.provide(NodeCrypto.layer))));
  const unknownMessage = (data: JsonObject, sequence: bigint): CommandReply => ({
    _tag: "Model",
    kind: "message",
    outcome: "replied",
    type: "mystery_observation",
    data,
    requestId: "",
    sequence,
    generation: 1n,
    correlation: "unsolicited",
    raw: {
      request_id: "",
      kind: 0,
      payload: {
        case: "message",
        value: { type: "mystery_observation", data: { fields: new Map() } },
      },
    },
  });
  const h3Case = (name: string, data: () => Record<string, unknown>) =>
    test(`H3 decode: unknown message with ${name} fails observation with a typed Protocol error`, () =>
      runH3(
        Effect.gen(function* () {
          const fake = yield* h3Fixture();
          const provider = yield* H3.make(fake.session, {
            replyTimeout: 80,
            setupTimeout: 500,
            reconcileWindow: 30,
          });
          yield* fake.replay(unknownMessage(data() as JsonObject, 10_000n));
          const failure = yield* provider.failure.pipe(Effect.timeoutOption(500));
          expect(Option.isSome(failure)).toBe(true);
          if (Option.isSome(failure)) expect(failure.value.reason._tag).toBe("Protocol");
          expect((yield* provider.current)._tag).toBe("Unavailable");
        }),
      ));
  h3Case("a non-finite number (control)", () => ({ n: Number.NaN }));
  h3Case("cumulative key text over 4 MiB", bigKeys);
});

// Wire payloads come from objectFromStruct and are bounded at 256 KiB; structFromObject is the
// public ./wire converter and keeps its documented synchronous-throw contract.
test("structFromObject (public ./wire) rejects sparse arrays synchronously", () => {
  expect(thrown(() => structFromObject({ list: sparse() }))).not.toBe("<no throw>");
});
