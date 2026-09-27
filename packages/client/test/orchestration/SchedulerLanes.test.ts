import { expect, test } from "vitest";
import { Clock, Effect, Stream } from "effect";
import { TestClock } from "effect/testing";
import { makeScheduler, ItemKey } from "../../src/orchestration/scheduler.js";
import type { ItemHandle, LaneSpec, SchedulerOptions } from "../../src/orchestration/scheduler.js";
import { ClipRequest } from "../../src/orchestration/request.js";
import * as Simulation from "../../src/simulation/index.js";
import { runClock } from "./SourceFixture.js";

const clip = (prompt: string, durationSeconds = 5) =>
  new ClipRequest({ prompt, references: [], durationSeconds, metadata: {} });
const item = (key: string, lane: string, seconds = 5) => ({
  key: ItemKey.make(key),
  lane,
  request: clip(key, seconds),
});

const advance = (milliseconds: number) =>
  Effect.gen(function* () {
    for (let elapsed = 0; elapsed < milliseconds; elapsed += 20)
      yield* TestClock.adjust(Math.min(20, milliseconds - elapsed));
  });

// A zero runway floor keeps filler off.
const options = (...lanes: ReadonlyArray<LaneSpec>) =>
  ({
    lanes,
    filler: {
      runway: { floor: "0 seconds", target: "1 second" },
      clip: ({ index }: { index: number }) => clip(`filler ${index}`),
    },
  }) satisfies SchedulerOptions;

const quick = Simulation.layerSim({ fixedBuildTime: 500, buildRatio: 0 });

/** The keys of the handles that started, in the order they started. */
const startOrder = (handles: ReadonlyArray<ItemHandle>) =>
  Effect.gen(function* () {
    const starts: { readonly key: string; readonly at: number }[] = [];
    for (const handle of handles) {
      const status = yield* handle.started;
      if (status._tag === "Started") starts.push({ key: handle.key, at: status.at });
    }
    return starts.sort((a, b) => a.at - b.at).map(({ key }) => key);
  });

test("a new item in a replace lane replaces the lane's waiting items once it is Ready", () =>
  runClock(
    Effect.gen(function* () {
      const scheduler = yield* makeScheduler(options({ name: "status", conflict: "replace" }));
      yield* advance(1_000);
      const s1 = yield* scheduler.submit(item("s1", "status"));
      yield* advance(1_000);
      // s1 plays; s2 waits behind it until s3 replaces it.
      const s2 = yield* scheduler.submit(item("s2", "status"));
      yield* advance(1_000);
      const s3 = yield* scheduler.submit(item("s3", "status"));
      yield* advance(30_000);
      expect(yield* startOrder([s1, s2, s3])).toEqual(["s1", "s3"]);
      expect(yield* s2.outcome).toEqual({ _tag: "Dropped", reason: "replaced" });
    }).pipe(Effect.provide(quick)),
  ));

test("a skip lane refuses a new item while it has one waiting or playing", () =>
  runClock(
    Effect.gen(function* () {
      const scheduler = yield* makeScheduler(options({ name: "ack", conflict: "skip" }));
      yield* advance(1_000);
      yield* scheduler.submit(item("a1", "ack"));
      const busy = yield* Effect.flip(scheduler.submit(item("a2", "ack")));
      expect(busy._tag).toBe("LaneBusy");
      yield* advance(3_000);
      // a1 is playing.
      expect((yield* Effect.flip(scheduler.submit(item("a3", "ack"))))._tag).toBe("LaneBusy");
      yield* advance(10_000);
      yield* scheduler.submit(item("a4", "ack"));
    }).pipe(Effect.provide(quick)),
  ));

test("a Ready item in a cut lane cuts a playing clip of a lower lane", () =>
  runClock(
    Effect.gen(function* () {
      const scheduler = yield* makeScheduler(
        options({ name: "urgent", cut: true }, { name: "line" }),
      );
      yield* advance(1_000);
      const long = yield* scheduler.submit(item("long", "line", 15));
      yield* advance(3_000);
      // long plays from 1.5 s for 15 s; u builds in 500 ms and cuts it.
      const submittedAt = yield* Clock.currentTimeMillis;
      const u = yield* scheduler.submit(item("u", "urgent"));
      yield* advance(20_000);
      const started = yield* u.started;
      expect(started._tag === "Started" && started.at - submittedAt).toBeLessThan(2_000);
      const ended = yield* long.outcome;
      expect(ended._tag === "Ended" && ended.termination).toBe("stopped");
      expect(ended._tag === "Ended" && ended.airedSeconds).toBeLessThan(5);
    }).pipe(Effect.provide(quick)),
  ));

// cb42f67: a second submission to a replace lane dropped the first one before it was built,
// which let the first one's batch take effect and withdraw the lane's cover early.
test("a replace lane keeps its cover until the newest submission is Ready", () =>
  runClock(
    Effect.gen(function* () {
      const scheduler = yield* makeScheduler(
        options({ name: "top" }, { name: "status", conflict: "replace" }),
      );
      const statuses: { key: string; tag: string; at: number }[] = [];
      yield* scheduler.asRun.pipe(
        Stream.runForEach((event) =>
          Effect.sync(() => {
            statuses.push({ key: event.key, tag: event.status._tag, at: event.at });
          }),
        ),
        Effect.forkScoped,
      );
      yield* advance(1_000);
      // A long clip plays in the top lane while the status lane changes behind it. s1 is
      // built by 8 s; then another top clip holds the build slot, so s2 waits unbuilt when
      // s3 arrives and supersedes both.
      yield* scheduler.submit(item("top", "top", 15));
      yield* scheduler.submit(item("s1", "status"));
      yield* advance(7_400);
      yield* scheduler.submit(item("busy", "top"));
      yield* advance(100);
      yield* scheduler.submit(item("s2", "status"));
      yield* advance(500);
      yield* scheduler.submit(item("s3", "status"));
      yield* advance(30_000);
      const at = (key: string, tag: string) =>
        statuses.find((entry) => entry.key === key && entry.tag === tag)?.at ?? Infinity;
      expect(at("s1", "Dropped")).toBeGreaterThanOrEqual(at("s3", "Ready"));
    }).pipe(Effect.provide(Simulation.layerSim({ fixedBuildTime: 3_500, buildRatio: 0 }))),
  ));

// cb42f67: a cut lane cut a clip of its own lane that a pending batch was withdrawing.
test("a cut lane never cuts a clip of its own lane", () =>
  runClock(
    Effect.gen(function* () {
      const scheduler = yield* makeScheduler(
        options({ name: "urgent", cut: true }, { name: "line" }),
      );
      yield* advance(1_000);
      const u1 = yield* scheduler.submit(item("u1", "urgent", 15));
      yield* advance(3_000);
      yield* scheduler.edit([
        { _tag: "Withdraw", key: ItemKey.make("u1") },
        { _tag: "Submit", item: item("u2", "urgent") },
        { _tag: "Submit", item: item("l1", "line") },
        { _tag: "Submit", item: item("l2", "line") },
      ]);
      yield* advance(30_000);
      const ended = yield* u1.outcome;
      expect(ended._tag === "Ended" && ended.termination).toBe("finished");
    }).pipe(Effect.provide(quick)),
  ));
