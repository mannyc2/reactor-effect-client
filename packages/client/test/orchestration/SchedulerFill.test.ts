import { expect, test } from "vitest";
import { Clock, Effect } from "effect";
import { TestClock } from "effect/testing";
import { makeScheduler, ItemKey } from "../../src/orchestration/scheduler.js";
import { ClipRequest } from "../../src/orchestration/request.js";
import * as Simulation from "../../src/simulation/index.js";
import { runClock } from "./SourceFixture.js";

const clip = (prompt: string, durationSeconds = 5) =>
  new ClipRequest({ prompt, references: [], durationSeconds, metadata: {} });

const advance = (milliseconds: number) =>
  Effect.gen(function* () {
    for (let elapsed = 0; elapsed < milliseconds; elapsed += 20)
      yield* TestClock.adjust(Math.min(20, milliseconds - elapsed));
  });

// Research report A, recommendation 6, and report B's fill to the avail: filler sized to
// the gap ends on an anchor instead of running past it.
test("filler sized to the gap lands an At item on its anchor", () =>
  runClock(
    Effect.gen(function* () {
      const scheduler = yield* makeScheduler({
        lanes: [{ name: "line" }],
        filler: {
          runway: { floor: "5 seconds", target: "10 seconds" },
          clip: ({ index, durationSeconds }) => clip(`filler ${index}`, durationSeconds),
        },
      });
      yield* advance(1_000);
      const anchor = (yield* Clock.currentTimeMillis) + 23_000;
      const item = yield* scheduler.submit({
        key: ItemKey.make("at"),
        lane: "line",
        request: clip("at"),
        start: { _tag: "At", time: anchor, late: { _tag: "nextBoundary" } },
      });
      yield* advance(40_000);
      const started = yield* item.started;
      expect(started._tag === "Started" && Math.abs(started.at - anchor)).toBeLessThan(1_000);
    }).pipe(Effect.provide(Simulation.layerSim({ fixedBuildTime: 500, buildRatio: 0 }))),
  ));
