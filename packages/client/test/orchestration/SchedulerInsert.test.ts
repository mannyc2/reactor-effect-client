import { expect, test } from "vitest";
import { Clock, Effect } from "effect";
import { TestClock } from "effect/testing";
import { makeScheduler, ItemKey } from "../../src/orchestration/scheduler.js";
import type { ItemHandle, SchedulerOptions } from "../../src/orchestration/scheduler.js";
import { ClipRequest } from "../../src/orchestration/request.js";
import * as Simulation from "../../src/simulation/index.js";
import { refusal, runClock } from "./SourceFixture.js";

const clip = (prompt: string) =>
  new ClipRequest({ prompt, references: [], durationSeconds: 5, metadata: {} });
const part = (key: string) => ({ key: ItemKey.make(key), request: clip(key) });
const submit = (key: string) => ({ key: ItemKey.make(key), lane: "line", request: clip(key) });

const advance = (milliseconds: number) =>
  Effect.gen(function* () {
    for (let elapsed = 0; elapsed < milliseconds; elapsed += 20)
      yield* TestClock.adjust(Math.min(20, milliseconds - elapsed));
  });

// A zero runway floor keeps filler off, so builds run in admission order.
const options = {
  lanes: [{ name: "line" }],
  filler: {
    runway: { floor: "0 seconds", target: "1 second" },
    clip: ({ index }: { index: number }) => clip(`filler ${index}`),
  },
} satisfies SchedulerOptions;

const quick = Simulation.layerSim({ fixedBuildTime: 500, buildRatio: 0 });
const slow = Simulation.layerSim({ fixedBuildTime: 6_000, buildRatio: 0 });

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

test("an insert airs just after the playing clip or just before a Ready one", () =>
  runClock(
    Effect.gen(function* () {
      const scheduler = yield* makeScheduler(options);
      yield* advance(1_000);
      const a = yield* scheduler.submit(submit("a"));
      const b = yield* scheduler.submit(submit("b"));
      const c = yield* scheduler.submit(submit("c"));
      yield* advance(3_000);
      const x = yield* scheduler.insert({ ...part("x"), after: ItemKey.make("a") });
      const y = yield* scheduler.insert({ ...part("y"), before: ItemKey.make("c") });
      yield* advance(40_000);
      expect(yield* startOrder([a, b, c, x, y])).toEqual(["a", "x", "b", "y", "c"]);
    }).pipe(Effect.provide(quick)),
  ));

test("an insert into a group keeps the group together, and withdrawing it leaves the group whole", () =>
  runClock(
    Effect.gen(function* () {
      const scheduler = yield* makeScheduler(options);
      yield* advance(1_000);
      const line = yield* scheduler.submitGroup({
        key: ItemKey.make("line"),
        lane: "line",
        parts: [part("a"), part("b"), part("c")],
      });
      const next = yield* scheduler.submit(submit("next"));
      yield* advance(3_000);
      const x = yield* scheduler.insert({ ...part("x"), before: ItemKey.make("b") });
      const y = yield* scheduler.insert({ ...part("y"), after: ItemKey.make("b") });
      expect(yield* scheduler.withdraw(ItemKey.make("y"))).toBe("withdrawn");
      yield* advance(40_000);
      expect(yield* startOrder([...line.parts, next, x, y])).toEqual(["a", "x", "b", "c", "next"]);
      expect(yield* y.outcome).toEqual({ _tag: "Dropped", reason: "withdrawn" });
    }).pipe(Effect.provide(quick)),
  ));

test("an insert that misses its place airs at the next boundary", () =>
  runClock(
    Effect.gen(function* () {
      const scheduler = yield* makeScheduler(options);
      yield* advance(1_000);
      const a = yield* scheduler.submit(submit("a"));
      const b = yield* scheduler.submit(submit("b"));
      // b builds from 7 s to 13 s, so x, which builds after it, cannot be Ready before b starts.
      yield* advance(8_000);
      const x = yield* scheduler.insert({ ...part("x"), before: ItemKey.make("b") });
      yield* advance(40_000);
      expect(yield* startOrder([a, b, x])).toEqual(["a", "b", "x"]);
    }).pipe(Effect.provide(slow)),
  ));

test("an insert's deadline counts only the clips ahead of its place", () =>
  runClock(
    Effect.gen(function* () {
      const scheduler = yield* makeScheduler(options);
      yield* advance(1_000);
      for (const key of ["a", "b", "c"]) yield* scheduler.submit(submit(key));
      yield* advance(3_000);
      // a plays until about 6.2 s; b and c follow it.
      const window = { startBy: "4 seconds", firm: true } as const;
      yield* scheduler.insert({ ...part("x"), before: ItemKey.make("b"), window });
      const late = yield* Effect.flip(
        scheduler.insert({ ...part("y"), after: ItemKey.make("c"), window }),
      );
      expect(late._tag).toBe("WouldMissDeadline");
    }).pipe(Effect.provide(quick)),
  ));

test("an insert needs one anchor that has not aired, and repeats return its handle", () =>
  runClock(
    Effect.gen(function* () {
      const scheduler = yield* makeScheduler(options);
      yield* advance(1_000);
      yield* scheduler.submit(submit("a"));
      yield* scheduler.submit(submit("b"));
      const x = yield* scheduler.insert({ ...part("x"), before: ItemKey.make("b") });
      expect(yield* scheduler.insert({ ...part("x"), before: ItemKey.make("b") })).toBe(x);
      const moved = yield* Effect.flip(
        scheduler.insert({ ...part("x"), after: ItemKey.make("b") }),
      );
      expect(moved._tag).toBe("KeyMismatch");
      yield* advance(2_000);
      const refused = (input: Parameters<typeof scheduler.insert>[0]) =>
        Effect.flip(scheduler.insert(input)).pipe(Effect.map(refusal));
      expect(yield* refused({ ...part("y"), before: ItemKey.make("a") })).toBe("InvalidRequest");
      expect(yield* refused({ ...part("y"), before: ItemKey.make("missing") })).toBe(
        "InvalidRequest",
      );
      expect(yield* refused(part("y"))).toBe("InvalidRequest");
      expect(
        yield* refused({ ...part("y"), before: ItemKey.make("b"), after: ItemKey.make("a") }),
      ).toBe("InvalidRequest");
    }).pipe(Effect.provide(quick)),
  ));

// 02dcc8e: an insert after an At item did not take its anchor time, so it aired first.
test("an insert after an At item airs after it", () =>
  runClock(
    Effect.gen(function* () {
      const scheduler = yield* makeScheduler(options);
      yield* advance(1_000);
      const anchor = (yield* Clock.currentTimeMillis) + 20_000;
      const at = yield* scheduler.submit({
        ...submit("at"),
        start: { _tag: "At", time: anchor, late: { _tag: "nextBoundary" } },
      });
      const x = yield* scheduler.insert({ ...part("x"), after: ItemKey.make("at") });
      yield* advance(40_000);
      expect(yield* startOrder([at, x])).toEqual(["at", "x"]);
    }).pipe(Effect.provide(quick)),
  ));
