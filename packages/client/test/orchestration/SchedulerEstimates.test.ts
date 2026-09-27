import { expect, test } from "vitest";
import { Effect, Stream } from "effect";
import { TestClock } from "effect/testing";
import { makeScheduler, ItemKey } from "../../src/orchestration/scheduler.js";
import type {
  AsRunStatus,
  SchedulerOptions,
  SchedulerShape,
} from "../../src/orchestration/scheduler.js";
import { ClipRequest } from "../../src/orchestration/request.js";
import * as Simulation from "../../src/simulation/index.js";
import { runClock } from "./SourceFixture.js";

const clip = (prompt: string) =>
  new ClipRequest({ prompt, references: [], durationSeconds: 5, metadata: {} });
const item = (key: string, lane = "line") => ({
  key: ItemKey.make(key),
  lane,
  request: clip(key),
});

const advance = (milliseconds: number) =>
  Effect.gen(function* () {
    for (let elapsed = 0; elapsed < milliseconds; elapsed += 20)
      yield* TestClock.adjust(Math.min(20, milliseconds - elapsed));
  });

// A zero runway floor keeps filler off.
const lanes = (...names: ReadonlyArray<string>) =>
  ({
    lanes: names.map((name) => ({ name })),
    filler: {
      runway: { floor: "0 seconds", target: "1 second" },
      clip: ({ index }: { index: number }) => clip(`filler ${index}`),
    },
  }) satisfies SchedulerOptions;

/** Three 5 s clips build and air, so the scheduler has measured three builds. */
const warmUp = (scheduler: SchedulerShape) =>
  Effect.gen(function* () {
    for (const key of ["w1", "w2", "w3"]) yield* scheduler.submit(item(key));
    yield* advance(30_000);
  });

test("WouldMissDeadline counts the measured time to build the clip", () =>
  runClock(
    Effect.gen(function* () {
      const scheduler = yield* makeScheduler(lanes("line"));
      yield* advance(1_000);
      yield* warmUp(scheduler);
      expect((yield* scheduler.state).estimates.build?.median).toBeCloseTo(1.2, 1);
      // Nothing is playing or Ready, but a clip takes about 6 s to build.
      const window = (seconds: number) => ({ startBy: `${seconds} seconds`, firm: true }) as const;
      const refused = yield* Effect.flip(scheduler.submit({ ...item("x"), window: window(3) }));
      expect(refused._tag).toBe("WouldMissDeadline");
      yield* scheduler.submit({ ...item("y"), window: window(8) });
    }).pipe(Effect.provide(Simulation.layerSim({ fixedBuildTime: 6_000, buildRatio: 0 }))),
  ));

test("a firm item that can no longer make its deadline is dropped before it is built", () =>
  runClock(
    Effect.gen(function* () {
      const scheduler = yield* makeScheduler(lanes("ack", "line"));
      yield* advance(1_000);
      yield* warmUp(scheduler);
      const statuses: AsRunStatus["_tag"][] = [];
      yield* scheduler.asRun.pipe(
        Stream.runForEach((event) =>
          Effect.sync(() => {
            if (event.key === "x") statuses.push(event.status._tag);
          }),
        ),
        Effect.forkScoped,
      );
      yield* scheduler.submit(item("a0", "ack"));
      // x can start once a0 has built and x has built after it, within its 14 s.
      const x = yield* scheduler.submit({
        ...item("x"),
        window: { startBy: "14 seconds", firm: true },
      });
      // a1 builds ahead of x, which then could not be Ready before its deadline. That is
      // known as soon as a1 arrives, 12 s before either a0 or a1 airs.
      yield* scheduler.submit(item("a1", "ack"));
      yield* advance(1_000);
      expect(statuses).toContain("Dropped");
      yield* advance(30_000);
      expect(yield* x.outcome).toEqual({ _tag: "Dropped", reason: "late" });
      expect(statuses).not.toContain("Building");
    }).pipe(Effect.provide(Simulation.layerSim({ fixedBuildTime: 6_000, buildRatio: 0 }))),
  ));

test("a runway floor below the build time is raised so filler is Ready before the picture runs out", () =>
  runClock(
    Effect.gen(function* () {
      const scheduler = yield* makeScheduler({
        lanes: [{ name: "line" }],
        filler: {
          runway: { floor: "1 second", target: "12 seconds" },
          clip: ({ index }) => clip(`filler ${index}`),
        },
      });
      // Builds take 4 s, faster than the 5.2 s clips play, so a refill started early enough
      // always lands in time; one started below a 1 s runway never does.
      yield* advance(30_000);
      const warm = (yield* scheduler.state).starved;
      yield* advance(60_000);
      expect((yield* scheduler.state).starved).toBe(warm);
    }).pipe(Effect.provide(Simulation.layerSim({ fixedBuildTime: 4_000, buildRatio: 0 }))),
  ));
