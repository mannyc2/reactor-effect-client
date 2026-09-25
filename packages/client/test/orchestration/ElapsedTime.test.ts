import { expect, test } from "vitest";
import { Clock, Effect, Stream } from "effect";
import { TestClock } from "effect/testing";
import { make as orchestrate } from "../../src/orchestration/renewal.js";
import { ClipRequest } from "../../src/orchestration/request.js";
import type { EngineEvent } from "../../src/orchestration/types.js";
import * as Simulation from "../../src/simulation/index.js";
import { runClock } from "./SourceFixture.js";
import { steppableWall } from "./WallClock.js";

const clip = new ClipRequest({
  prompt: "The host waits at the desk.",
  references: [],
  durationSeconds: 5,
  metadata: {},
});

/** Advances the test clock itself: the stepped wall clock has no `adjust`. */
const elapse = (clock: TestClock.TestClock, millis: number) =>
  Effect.gen(function* () {
    for (let elapsed = 0; elapsed < millis; elapsed += 20)
      yield* clock.adjust(Math.min(20, millis - elapsed));
  });

/**
 * Simulated sessions that each live 60 s, renewed 20 s before they end. Each
 * `Prepared` is recorded at its elapsed time, in milliseconds.
 */
const renewing = Effect.gen(function* () {
  const prepared: number[] = [];
  const handle = yield* orchestrate({
    open: Simulation.source({ fixedBuildTime: 500, buildRatio: 0 }).pipe(
      Effect.map((source) => ({ source, lifetime: "60 seconds" })),
    ),
    lead: "20 seconds",
    onRenewal: (event) =>
      event._tag === "Prepared"
        ? Effect.map(Clock.monotonicTimeNanos, (at) => {
            prepared.push(Number(at / 1_000_000n));
          })
        : Effect.void,
  });
  return { handle, prepared };
});

test("a wall clock stepped back does not delay renewal: the next session is prepared at lifetime minus lead of elapsed time", () =>
  runClock(
    Effect.gen(function* () {
      const testClock = yield* TestClock.testClockWith(Effect.succeed);
      const wall = yield* steppableWall;
      yield* Effect.gen(function* () {
        const { handle, prepared } = yield* renewing;
        yield* handle.engine.enqueue(clip);
        yield* elapse(testClock, 35_000);
        yield* wall.step(-30_000);
        yield* elapse(testClock, 6_000);
        expect(prepared).toHaveLength(1);
        expect(prepared[0]).toBeGreaterThanOrEqual(40_000);
        expect(prepared[0]).toBeLessThan(40_500);
      }).pipe(Effect.provideService(Clock.Clock, wall.clock));
    }),
  ));

test("a wall clock stepped forward does not bring renewal forward", () =>
  runClock(
    Effect.gen(function* () {
      const testClock = yield* TestClock.testClockWith(Effect.succeed);
      const wall = yield* steppableWall;
      yield* Effect.gen(function* () {
        const { handle, prepared } = yield* renewing;
        yield* handle.engine.enqueue(clip);
        yield* elapse(testClock, 5_000);
        yield* wall.step(30_000);
        yield* elapse(testClock, 34_000);
        expect(prepared).toEqual([]);
        yield* elapse(testClock, 2_000);
        expect(prepared).toHaveLength(1);
        expect(prepared[0]).toBeGreaterThanOrEqual(40_000);
      }).pipe(Effect.provideService(Clock.Clock, wall.clock));
    }),
  ));

test("a simulated build is measured on elapsed time, so a wall-clock step during it leaves its length unchanged", () =>
  runClock(
    Effect.gen(function* () {
      const testClock = yield* TestClock.testClockWith(Effect.succeed);
      const wall = yield* steppableWall;
      yield* Effect.gen(function* () {
        const handle = yield* Simulation.make({ fixedBuildTime: 2_000, buildRatio: 0 });
        const events: EngineEvent[] = [];
        yield* handle.engine.events.pipe(
          Stream.runForEach((event) =>
            Effect.sync(() => {
              events.push(event);
            }),
          ),
          Effect.forkScoped,
        );
        yield* testClock.adjust(0);
        yield* handle.engine.enqueue(clip);
        yield* elapse(testClock, 1_000);
        yield* wall.step(30_000);
        yield* elapse(testClock, 1_500);
        const ready = events.find((event) => event._tag === "Ready");
        expect(ready?._tag === "Ready" ? ready.timing : undefined).toEqual({
          _tag: "Measured",
          buildMs: 2_000,
        });
      }).pipe(Effect.provideService(Clock.Clock, wall.clock));
    }),
  ));
