import { expect, test } from "vitest";
import { Effect } from "effect";
import { TestClock } from "effect/testing";
import { makeScheduler, ItemKey } from "../../src/orchestration/scheduler.js";
import type { ItemHandle, SchedulerOptions } from "../../src/orchestration/scheduler.js";
import { ClipRequest } from "../../src/orchestration/request.js";
import * as Simulation from "../../src/simulation/index.js";
import { refusal, runClock } from "./SourceFixture.js";

const clip = (prompt: string) =>
  new ClipRequest({ prompt, references: [], durationSeconds: 5, metadata: {} });
const next = (key: string) => ({ key: ItemKey.make(key), request: clip(key) });
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

test("a replacement takes a Ready clip's place once it is Ready, and the old clip is dropped as replaced", () =>
  runClock(
    Effect.gen(function* () {
      const scheduler = yield* makeScheduler(options);
      yield* advance(1_000);
      const a = yield* scheduler.submit(submit("a"));
      const b = yield* scheduler.submit(submit("b"));
      yield* advance(2_000);
      const b2 = yield* scheduler.replace(ItemKey.make("b"), next("b2"));
      yield* advance(30_000);
      expect(yield* startOrder([a, b, b2])).toEqual(["a", "b2"]);
      expect(yield* b.outcome).toEqual({ _tag: "Dropped", reason: "replaced" });
    }).pipe(Effect.provide(quick)),
  ));

test("a replacement still building when the old clip starts is dropped, and the old clip airs", () =>
  runClock(
    Effect.gen(function* () {
      const scheduler = yield* makeScheduler(options);
      yield* advance(1_000);
      const a = yield* scheduler.submit(submit("a"));
      const b = yield* scheduler.submit(submit("b"));
      // a plays from 7 s to 12.2 s while b builds until 13 s, when b starts; a
      // replacement queued at 12.5 s cannot be Ready before then.
      yield* advance(11_500);
      const b2 = yield* scheduler.replace(ItemKey.make("b"), next("b2"));
      yield* advance(30_000);
      expect(yield* startOrder([a, b, b2])).toEqual(["a", "b"]);
      expect(yield* b2.outcome).toEqual({ _tag: "Dropped", reason: "withdrawn" });
    }).pipe(Effect.provide(slow)),
  ));

test("replacing a clip that has not started building swaps it at once", () =>
  runClock(
    Effect.gen(function* () {
      const scheduler = yield* makeScheduler(options);
      yield* advance(1_000);
      const a = yield* scheduler.submit(submit("a"));
      const b = yield* scheduler.submit(submit("b"));
      const b2 = yield* scheduler.replace(ItemKey.make("b"), next("b2"));
      expect(yield* b.outcome).toEqual({ _tag: "Dropped", reason: "replaced" });
      yield* advance(40_000);
      expect(yield* startOrder([a, b2])).toEqual(["a", "b2"]);
    }).pipe(Effect.provide(slow)),
  ));

test("replacing a group part keeps the group whole", () =>
  runClock(
    Effect.gen(function* () {
      const scheduler = yield* makeScheduler(options);
      yield* advance(1_000);
      const line = yield* scheduler.submitGroup({
        key: ItemKey.make("line"),
        lane: "line",
        parts: [next("a"), next("b"), next("c")],
      });
      yield* advance(2_000);
      const b2 = yield* scheduler.replace(ItemKey.make("b"), next("b2"));
      yield* advance(30_000);
      expect(yield* startOrder([...line.parts, b2])).toEqual(["a", "b2", "c"]);
    }).pipe(Effect.provide(quick)),
  ));

test("repeating a replacement returns its handle, and a started clip cannot be replaced", () =>
  runClock(
    Effect.gen(function* () {
      const scheduler = yield* makeScheduler(options);
      yield* advance(1_000);
      yield* scheduler.submit(submit("a"));
      yield* scheduler.submit(submit("b"));
      const b2 = yield* scheduler.replace(ItemKey.make("b"), next("b2"));
      expect(yield* scheduler.replace(ItemKey.make("b"), next("b2"))).toBe(b2);
      yield* advance(2_000);
      expect(refusal(yield* Effect.flip(scheduler.replace(ItemKey.make("a"), next("a2"))))).toBe(
        "InvalidRequest",
      );
    }).pipe(Effect.provide(quick)),
  ));

// aafd59e: replacing a replacement before it was built dropped only the middle item, so
// the original and the final replacement both aired.
test("replacing a replacement before it is built still replaces the original", () =>
  runClock(
    Effect.gen(function* () {
      const scheduler = yield* makeScheduler(options);
      yield* advance(1_000);
      const a = yield* scheduler.submit({
        ...submit("a"),
        request: new ClipRequest({
          prompt: "a",
          references: [],
          durationSeconds: 15,
          metadata: {},
        }),
      });
      const b = yield* scheduler.submit(submit("b"));
      // a builds until 7 s and plays until 22 s; b builds from 7 s to 13 s.
      yield* advance(7_000);
      yield* scheduler.replace(ItemKey.make("b"), next("b2"));
      yield* advance(1_000);
      const b3 = yield* scheduler.replace(ItemKey.make("b2"), next("b3"));
      yield* advance(40_000);
      expect(yield* startOrder([a, b, b3])).toEqual(["a", "b3"]);
      expect(yield* b.outcome).toEqual({ _tag: "Dropped", reason: "replaced" });
    }).pipe(Effect.provide(slow)),
  ));
