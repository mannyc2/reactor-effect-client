import { expect, test } from "vitest";
import { Effect, Stream } from "effect";
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

/**
 * A simulated engine that records, by item key, each clip's ID and what it continues from.
 * A continued build takes `continuedMs` when given, as it took longer on hosted H3.
 */
const recording = (buildMs: number, continuedMs?: number) =>
  Effect.gen(function* () {
    const handle = yield* Simulation.make(
      continuedMs === undefined
        ? { fixedBuildTime: buildMs, buildRatio: 0 }
        : {
            build: (record) =>
              Effect.sleep(record.request.continueFrom === undefined ? buildMs : continuedMs).pipe(
                Effect.as(record.durationSeconds),
              ),
          },
    );
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

// 71d593c: a replacement continued from the clip it replaces, which never airs.
test("a replacement continues from the clip before its place, not the one it replaces", () =>
  runClock(
    Effect.gen(function* () {
      const { engine, clips, continues } = yield* recording(500);
      const scheduler = yield* makeScheduler(options).pipe(Effect.provideService(Engine, engine));
      yield* advance(1_000);
      yield* scheduler.submit({ key: ItemKey.make("a"), lane: "line", request: clip("a") });
      yield* scheduler.submit({ key: ItemKey.make("b"), lane: "line", request: clip("b") });
      yield* advance(2_000);
      yield* scheduler.replace(ItemKey.make("b"), part("b2"));
      yield* advance(2_000);
      expect(continues.get("b2")).toBe(clips.get("a"));
    }),
  ));

// The 0.7.0 scheduler-edits paid run (integration/hosted/evidence/0.7.0): a continued insert
// waited behind a build in flight, and its continued build took 5.45 s against about 2.2 s for
// an independent one. It missed the playing clip's end, and aired after the next clip, which it
// did not continue from.
test("an insert that cannot be Ready before the playing clip ends continues from the clip that airs before it", () =>
  runClock(
    Effect.gen(function* () {
      const { engine, clips, continues } = yield* recording(2_200, 5_500);
      const scheduler = yield* makeScheduler(options).pipe(Effect.provideService(Engine, engine));
      const starts: string[] = [];
      yield* scheduler.asRun.pipe(
        Stream.runForEach((event) =>
          Effect.sync(() => {
            if (event.status._tag === "Started") starts.push(event.key);
          }),
        ),
        Effect.forkScoped,
      );
      yield* advance(1_000);
      yield* scheduler.submitGroup({
        key: ItemKey.make("line"),
        lane: "line",
        parts: [
          { key: ItemKey.make("p1"), request: clip("p1") },
          { key: ItemKey.make("p2"), request: clip("p2") },
          { key: ItemKey.make("p3"), request: clip("p3") },
        ],
      });
      // p1 plays from 3.2 s to 8.2 s while p2 builds; xc can build only once p2 is built.
      yield* advance(2_400);
      yield* scheduler.insert({ ...part("xc"), before: ItemKey.make("p2") });
      yield* advance(30_000);
      expect(continues.get("xc")).toBe(clips.get("p2"));
      expect(starts).toEqual(["p1", "p2", "xc", "p3"]);
    }),
  ));

test("a clip built from the clip after its place waits behind that clip, though it is Ready in time", () =>
  runClock(
    Effect.gen(function* () {
      // No continued build is measured yet, so xc is projected to miss p1's end, but builds fast.
      const { engine, clips, continues } = yield* recording(2_200, 2_200);
      const scheduler = yield* makeScheduler(options).pipe(Effect.provideService(Engine, engine));
      const starts: string[] = [];
      yield* scheduler.asRun.pipe(
        Stream.runForEach((event) =>
          Effect.sync(() => {
            if (event.status._tag === "Started") starts.push(event.key);
          }),
        ),
        Effect.forkScoped,
      );
      yield* advance(1_000);
      yield* scheduler.submitGroup({
        key: ItemKey.make("line"),
        lane: "line",
        parts: [
          { key: ItemKey.make("p1"), request: clip("p1") },
          { key: ItemKey.make("p2"), request: clip("p2") },
          { key: ItemKey.make("p3"), request: clip("p3") },
        ],
      });
      yield* advance(2_400);
      yield* scheduler.insert({ ...part("xc"), before: ItemKey.make("p2") });
      yield* advance(30_000);
      expect(continues.get("xc")).toBe(clips.get("p2"));
      expect(starts).toEqual(["p1", "p2", "xc", "p3"]);
    }),
  ));
