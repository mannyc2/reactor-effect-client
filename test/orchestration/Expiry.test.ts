import { expect, test } from "bun:test";
import { Effect, Fiber, Result } from "effect";
import { TestClock } from "effect/testing";
import { renewalFixture } from "./RenewalFixture.js";
import { gate, member, readyState, record, runClock, until } from "./SourceFixture.js";

test("expiry settles a committed pending member as unknown before sequence retirement and never replays it", () =>
  runClock(
    Effect.gen(function* () {
      const entered = yield* gate();
      const { handle, sources, renewals, warm } = yield* renewalFixture((index) =>
        index === 0
          ? {
              execute: (plan, accept) =>
                plan.request.sequence?.final === true
                  ? entered.release.pipe(Effect.andThen(Effect.never))
                  : accept,
            }
          : {},
      );
      yield* handle.engine.enqueue(member("pending-at-expiry", false));
      yield* warm;
      yield* sources[1]!.setState(readyState({ ready: [record("foreign-warm-ready")] }));
      const prepared = yield* handle.engine.prepare(member("pending-at-expiry", true));
      const pending = yield* prepared.submit.pipe(Effect.result, Effect.forkScoped);
      yield* entered.wait;
      expect((yield* handle.sequences.get("pending-at-expiry"))?.pendingCount).toBe(1);
      yield* TestClock.adjust(600);
      yield* until(
        () => renewals.some((event) => event._tag === "Replaced"),
        TestClock.adjust(100),
      );
      const outcome = yield* Fiber.join(pending);
      expect(Result.isFailure(outcome) && outcome.failure.context.outcome).toBe("unknown");
      const sequence = yield* handle.sequences.get("pending-at-expiry");
      expect(sequence?.status).toBe("indeterminate");
      expect(sequence?.pendingCount).toBe(0);
      expect(sequence?.acceptedCount).toBe(1);
      expect(sequence?.indeterminateCount).toBe(1);
      expect(yield* Effect.result(prepared.submit)).toEqual(outcome);
      expect(sources[0]!.sends).toHaveLength(2);
      expect(sources[1]!.sends).toEqual([]);
    }),
  ));
