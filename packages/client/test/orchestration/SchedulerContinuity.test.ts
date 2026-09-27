import { expect, test } from "vitest";
import { Effect } from "effect";
import { TestClock } from "effect/testing";
import { ClipRequest } from "../../src/orchestration/request.js";
import type { ClipId } from "../../src/orchestration/request.js";
import { schedulerKeyOf } from "../../src/orchestration/scheduler-key.js";
import { ItemKey, makeScheduler } from "../../src/orchestration/scheduler.js";
import type { SchedulerOptions } from "../../src/orchestration/scheduler.js";
import { Engine } from "../../src/orchestration/types.js";
import type { EngineShape } from "../../src/orchestration/types.js";
import * as Simulation from "../../src/simulation/index.js";
import { runClock } from "./SourceFixture.js";

const clip = (prompt: string) =>
  new ClipRequest({ prompt, references: [], durationSeconds: 5, metadata: {} });
const part = (key: string) => ({
  key: ItemKey.make(key),
  request: clip(key),
  continuity: "previous" as const,
});

const advance = (milliseconds: number) =>
  Effect.gen(function* () {
    for (let elapsed = 0; elapsed < milliseconds; elapsed += 20)
      yield* TestClock.adjust(Math.min(20, milliseconds - elapsed));
  });

// A zero runway floor keeps filler off.
const options = {
  lanes: [{ name: "line" }],
  filler: {
    runway: { floor: "0 seconds", target: "1 second" },
    clip: ({ index }: { index: number }) => clip(`filler ${index}`),
  },
} satisfies SchedulerOptions;

/** A simulated engine that records, by item key, each clip's ID and what it continues from. */
const recording = (buildMs: number) =>
  Effect.gen(function* () {
    const handle = yield* Simulation.make({ fixedBuildTime: buildMs, buildRatio: 0 });
    const clips = new Map<string, ClipId>();
    const continues = new Map<string, ClipId | undefined>();
    const engine: EngineShape = {
      ...handle.engine,
      enqueueOnSource: (request, sessionId) =>
        handle.engine.enqueueOnSource!(request, sessionId).pipe(
          Effect.tap((clipId) =>
            Effect.sync(() => {
              const key = schedulerKeyOf(request) ?? "";
              clips.set(key, clipId);
              continues.set(key, request.continueFrom);
            }),
          ),
        ),
    };
    return { engine, clips, continues };
  });

// the-show#92: an inserted or amended beat must join its neighbours, and H3 carries motion,
// camera and audio across a boundary from the clip a new one continues from.
test("each part of a line continues from the part before it", () =>
  runClock(
    Effect.gen(function* () {
      const { engine, clips, continues } = yield* recording(500);
      const scheduler = yield* makeScheduler(options).pipe(Effect.provideService(Engine, engine));
      yield* advance(1_000);
      yield* scheduler.submitGroup({
        key: ItemKey.make("line"),
        lane: "line",
        parts: [{ key: ItemKey.make("a"), request: clip("a") }, part("b"), part("c")],
      });
      yield* advance(10_000);
      expect(continues.get("a")).toBeUndefined();
      expect(continues.get("b")).toBe(clips.get("a"));
      expect(continues.get("c")).toBe(clips.get("b"));
    }),
  ));

test("an insert after the playing clip continues from it", () =>
  runClock(
    Effect.gen(function* () {
      const { engine, clips, continues } = yield* recording(500);
      const scheduler = yield* makeScheduler(options).pipe(Effect.provideService(Engine, engine));
      yield* advance(1_000);
      yield* scheduler.submit({ key: ItemKey.make("a"), lane: "line", request: clip("a") });
      yield* scheduler.submit({ key: ItemKey.make("b"), lane: "line", request: clip("b") });
      yield* advance(3_000);
      yield* scheduler.insert({ ...part("ack"), after: ItemKey.make("a") });
      yield* advance(3_000);
      expect(continues.get("ack")).toBe(clips.get("a"));
    }),
  ));

test("a clip that continues waits for the clip before it to be built, even with an earlier deadline", () =>
  runClock(
    Effect.gen(function* () {
      const { engine, clips, continues } = yield* recording(2_000);
      const scheduler = yield* makeScheduler(options).pipe(Effect.provideService(Engine, engine));
      yield* advance(1_000);
      // The busy clip holds the build slot while a and b queue behind it.
      yield* scheduler.submit({ key: ItemKey.make("busy"), lane: "line", request: clip("busy") });
      yield* scheduler.submit({ key: ItemKey.make("a"), lane: "line", request: clip("a") });
      yield* scheduler.submit({
        ...part("b"),
        lane: "line",
        window: { startBy: "60 seconds", firm: false },
      });
      yield* advance(20_000);
      expect(continues.get("b")).toBe(clips.get("a"));
    }),
  ));
