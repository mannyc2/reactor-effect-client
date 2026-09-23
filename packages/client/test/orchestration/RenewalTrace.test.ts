import { expect, test } from "bun:test";
import { Effect, Fiber, Result } from "effect";
import { TestClock } from "effect/testing";
import { renewalFixture } from "./RenewalFixture.js";
import { failure, gate, member, runClock } from "./SourceFixture.js";

type Action = "cancel" | "reply" | "expire";
const permutations = (remaining: readonly Action[]): Action[][] =>
  remaining.length === 0
    ? [[]]
    : remaining.flatMap((action, index) =>
        permutations(remaining.filter((_, other) => other !== index)).map((rest) => [
          action,
          ...rest,
        ]),
      );

for (const remoteOutcome of ["accepted", "rejected"] as const) {
  for (const trace of permutations(["cancel", "reply", "expire"])) {
    test(`${remoteOutcome}: ${trace.join(" -> ")} preserves the original committed outcome`, () =>
      runClock(
        Effect.gen(function* () {
          const delivery = yield* gate;
          const rejection = failure("replied", "fixture rejection");
          const { handle, sources } = yield* renewalFixture((index) =>
            index === 0
              ? {
                  execute: (_, accept) =>
                    delivery.wait.pipe(
                      Effect.andThen(
                        remoteOutcome === "accepted" ? accept : Effect.fail(rejection),
                      ),
                    ),
                }
              : {},
          );
          const submission = yield* handle.engine.prepare(member("trace"));
          expect(sources[0]!.sends).toHaveLength(0);
          const caller = yield* submission.submit.pipe(Effect.result, Effect.forkScoped);
          const original = sources[0]!;
          yield* original.lifecycle.wait((event) => event._tag === "Dispatched");
          expect((yield* submission.state)._tag).toBe("Committed");
          for (const action of trace) {
            switch (action) {
              case "cancel":
                yield* Fiber.interrupt(caller);
                break;
              case "reply":
                yield* delivery.release;
                yield* original.lifecycle.wait((event) => event._tag === "ResultKnown");
                break;
              case "expire":
                yield* TestClock.adjust(1100);
                yield* original.lifecycle.wait((event) => event._tag === "Closed");
                break;
            }
          }
          yield* original.lifecycle.wait((event) => event._tag === "Finalized");
          const outcome = yield* Effect.result(submission.submit);
          // The oracle uses external event order, not the production reducer or private source state.
          const replyArrived = trace.indexOf("reply") < trace.indexOf("expire");
          if (replyArrived && remoteOutcome === "accepted")
            expect(Result.isSuccess(outcome)).toBe(true);
          else {
            expect(Result.isFailure(outcome)).toBe(true);
            if (Result.isFailure(outcome)) {
              expect(outcome.failure.context.outcome).toBe(replyArrived ? "replied" : "unknown");
              expect(outcome.failure.context.requestId).toBe("fixture-dispatch");
              expect(outcome.failure.context.generation).toBe(1n);
              if (replyArrived) expect(outcome.failure).toBe(rejection);
            }
          }
          const report = yield* handle.close;
          expect(yield* handle.close).toBe(report);
          expect(yield* Effect.result(submission.submit)).toEqual(outcome);
          expect((yield* submission.state)._tag).toBe("Completed");
          expect((yield* handle.sequences.get("trace"))?.pendingCount).toBe(0);
          expect(sources.flatMap((source) => source.sends)).toHaveLength(1);
          expect(original.status()).toMatchObject({ closes: 1, results: 1, finalized: true });
          expect(sources.every((source) => source.status().closes === 1)).toBe(true);
        }),
      ));
  }
}
