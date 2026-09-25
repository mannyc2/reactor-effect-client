import { expect, test } from "vitest";
import { Effect } from "effect";
import { TestClock } from "effect/testing";
import { makeScheduler, ItemKey } from "../../src/orchestration/scheduler.js";
import type { SchedulerOptions } from "../../src/orchestration/scheduler.js";
import { ClipId, ClipRequest, PolicyFailure } from "../../src/orchestration/request.js";
import { Engine } from "../../src/orchestration/types.js";
import type { EngineShape } from "../../src/orchestration/types.js";
import * as Simulation from "../../src/simulation/index.js";
import { refusal, runClock } from "./SourceFixture.js";

const clip = (prompt: string, seconds = 5) =>
  new ClipRequest({ prompt, references: [], durationSeconds: seconds, metadata: {} });

const advance = (milliseconds: number) =>
  Effect.gen(function* () {
    for (let elapsed = 0; elapsed < milliseconds; elapsed += 20)
      yield* TestClock.adjust(Math.min(20, milliseconds - elapsed));
  });

const options = {
  lanes: [{ name: "urgent" }, { name: "normal" }],
  filler: {
    runway: { floor: "5 seconds", target: "10 seconds" },
    clip: ({ index }: { index: number }) => clip(`filler ${index}`),
  },
} satisfies SchedulerOptions;

test("a higher lane plays before a lower lane while both are waiting", () =>
  runClock(
    Effect.gen(function* () {
      const scheduler = yield* makeScheduler(options);
      yield* advance(1_000);
      const normal = yield* scheduler.submit({
        key: ItemKey.make("normal-1"),
        lane: "normal",
        request: clip("normal"),
      });
      const urgent = yield* scheduler.submit({
        key: ItemKey.make("urgent-1"),
        lane: "urgent",
        request: clip("urgent"),
      });
      yield* advance(20_000);
      const urgentStart = yield* urgent.started;
      const normalStart = yield* normal.started;
      expect(urgentStart._tag).toBe("Started");
      expect(normalStart._tag).toBe("Started");
      if (urgentStart._tag === "Started" && normalStart._tag === "Started")
        expect(urgentStart.at).toBeLessThan(normalStart.at);
      const events = yield* scheduler.state;
      expect(events.lanes.map(({ name }) => name)).toEqual(["urgent", "normal"]);
    }).pipe(Effect.provide(Simulation.layerSim({ fixedBuildTime: 500, buildRatio: 0 }))),
  ));

test("scheduler refuses a request that forces placement on another session", () =>
  runClock(
    Effect.gen(function* () {
      const scheduler = yield* makeScheduler(options);
      const result = yield* Effect.flip(
        scheduler.submit({
          key: ItemKey.make("affinity"),
          lane: "normal",
          request: new ClipRequest({
            ...clip("affinity"),
            sameSessionAs: ClipId.make("older"),
          }),
        }),
      );
      expect(refusal(result)).toBe("InvalidRequest");
    }).pipe(Effect.provide(Simulation.layerSim({ fixedBuildTime: 500, buildRatio: 0 }))),
  ));

test("a persistent capacity refusal is retried at a bounded pace", () =>
  runClock(
    Effect.gen(function* () {
      const handle = yield* Simulation.make({ fixedBuildTime: 0, buildRatio: 0 });
      let attempts = 0;
      const engine: EngineShape = {
        ...handle.engine,
        enqueue: () =>
          Effect.sync(() => {
            attempts++;
          }).pipe(
            Effect.andThen(PolicyFailure.refuse("QueueFull", "Generation capacity is occupied")),
          ),
      };
      const scheduler = yield* makeScheduler({
        lanes: [{ name: "line" }],
        filler: { runway: { floor: 0, target: "5 seconds" }, clip: () => clip("unused") },
      }).pipe(Effect.provideService(Engine, engine));
      yield* scheduler.submit({ key: ItemKey.make("paced"), lane: "line", request: clip("paced") });
      yield* advance(500);
      expect(attempts).toBe(1);
      yield* advance(2_000);
      expect(attempts).toBeGreaterThanOrEqual(2);
      expect(attempts).toBeLessThanOrEqual(3);
    }),
  ));
