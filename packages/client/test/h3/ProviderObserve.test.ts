/**
 * `observe` delivers only what its snapshot does not already reflect: a
 * scheduler yield between the subscription and the snapshot read lets the
 * reducer apply an event, which the snapshot then covers.
 */
import { describe, expect, test } from "vitest";
import { Effect, Fiber, Scheduler, Scope, Stream } from "effect";
import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import type { Crypto } from "effect";
import * as H3 from "../../src/h3/index.js";
import { fixture } from "./ProviderSession.js";

const run = <A, E>(effect: Effect.Effect<A, E, Scope.Scope | Crypto.Crypto>) =>
  Effect.runPromise(Effect.scoped(effect.pipe(Effect.provide(NodeCrypto.layer))));

const trial = (maxOps: number) =>
  run(
    Effect.gen(function* () {
      const fake = yield* fixture();
      const provider = yield* H3.make(fake.session, { replyTimeout: 1000 });
      // Queue one session event for the reducer fiber, then observe at once.
      yield* fake.emit("state_update", fake.state({ seed: 4242 }));
      const observation = yield* provider
        .observe({ capacity: 64 })
        .pipe(Effect.provideService(Scheduler.MaxOpsBeforeYield, maxOps));
      const seen: H3.ProviderEvent[] = [];
      const reader = yield* observation.events.pipe(
        Stream.runForEach((event) =>
          Effect.sync(() => {
            seen.push(event);
          }),
        ),
        Effect.forkScoped,
      );
      // A later acceptance is still delivered: it is never part of a snapshot.
      yield* provider.enqueue({ prompt: "A blue paper boat on clear water.", seconds: 7 });
      yield* Effect.sleep(20);
      yield* Fiber.interrupt(reader);
      return { revision: observation.revision, seen };
    }),
  );

describe("H3 observe", () => {
  test("never delivers an event its snapshot already covers, at any yield point", async () => {
    for (let maxOps = 5; maxOps <= 64; maxOps++) {
      const { revision, seen } = await trial(maxOps);
      const covered = seen.filter(
        (event) =>
          event._tag !== "Acceptance" &&
          event.source !== undefined &&
          event.source.sequence <= revision,
      );
      expect(covered, `maxOps ${maxOps}`).toEqual([]);
      expect(
        seen.some((event) => event._tag === "Acceptance"),
        `maxOps ${maxOps}`,
      ).toBe(true);
    }
  }, 60_000);
});
