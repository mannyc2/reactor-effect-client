import { expect, test } from "vitest";
import { Effect } from "effect";
import { TestClock } from "effect/testing";
import { makeScheduler, ItemKey } from "../../src/orchestration/scheduler.js";
import type { ItemHandle, SchedulerOptions } from "../../src/orchestration/scheduler.js";
import { ClipRequest } from "../../src/orchestration/request.js";
import * as Simulation from "../../src/simulation/index.js";
import { runClock } from "./SourceFixture.js";

const clip = (prompt: string) =>
  new ClipRequest({ prompt, references: [], durationSeconds: 5, metadata: {} });
const part = (key: string) => ({ key: ItemKey.make(key), request: clip(key) });

const advance = (milliseconds: number) =>
  Effect.gen(function* () {
    for (let elapsed = 0; elapsed < milliseconds; elapsed += 20)
      yield* TestClock.adjust(Math.min(20, milliseconds - elapsed));
  });

// A zero runway floor keeps filler off, so builds run in admission order.
const options = {
  lanes: [{ name: "ack" }, { name: "line" }],
  filler: {
    runway: { floor: "0 seconds", target: "1 second" },
    clip: ({ index }: { index: number }) => clip(`filler ${index}`),
  },
} satisfies SchedulerOptions;

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

test("a later group with a deadline waits for an earlier group's parts instead of playing between them", () =>
  runClock(
    Effect.gen(function* () {
      const scheduler = yield* makeScheduler(options);
      yield* advance(1_000);
      const first = yield* scheduler.submitGroup({
        key: ItemKey.make("line-1"),
        lane: "line",
        parts: [part("1a"), part("1b"), part("1c")],
      });
      yield* advance(100);
      // Builds take longer than a clip plays, so no part is Ready ahead of its boundary.
      const second = yield* scheduler.submitGroup({
        key: ItemKey.make("line-2"),
        lane: "line",
        parts: [part("2a"), part("2b")],
        window: { startBy: "2 minutes", firm: false },
      });
      yield* advance(90_000);
      expect(yield* startOrder([...first.parts, ...second.parts])).toEqual([
        "1a",
        "1b",
        "1c",
        "2a",
        "2b",
      ]);
    }).pipe(Effect.provide(Simulation.layerSim({ fixedBuildTime: 6_000, buildRatio: 0 }))),
  ));

test("an earlier line passed by a group with a deadline waits until that group has aired", () =>
  runClock(
    Effect.gen(function* () {
      const scheduler = yield* makeScheduler(options);
      yield* advance(1_000);
      // The first clip holds the build slot while an undated line and a dated one queue.
      const busy = yield* scheduler.submit({
        key: ItemKey.make("busy"),
        lane: "line",
        request: clip("busy"),
      });
      const earlier = yield* scheduler.submitGroup({
        key: ItemKey.make("earlier"),
        lane: "line",
        parts: [part("0a"), part("0b")],
      });
      const dated = yield* scheduler.submitGroup({
        key: ItemKey.make("dated"),
        lane: "line",
        parts: [part("1a"), part("1b"), part("1c")],
        window: { startBy: "2 minutes", firm: false },
      });
      yield* advance(60_000);
      // The earlier line's first part is Ready while the dated line is still airing.
      expect(yield* startOrder([busy, ...earlier.parts, ...dated.parts])).toEqual([
        "busy",
        "1a",
        "1b",
        "1c",
        "0a",
        "0b",
      ]);
    }).pipe(Effect.provide(Simulation.layerSim({ fixedBuildTime: 3_000, buildRatio: 0 }))),
  ));

test("a higher lane goes in at the boundary between a group's parts", () =>
  runClock(
    Effect.gen(function* () {
      const scheduler = yield* makeScheduler(options);
      yield* advance(1_000);
      const line = yield* scheduler.submitGroup({
        key: ItemKey.make("line"),
        lane: "line",
        parts: [part("a"), part("b"), part("c")],
      });
      yield* advance(2_000);
      const ack = yield* scheduler.submit({
        key: ItemKey.make("ack"),
        lane: "ack",
        request: clip("ack"),
      });
      yield* advance(30_000);
      expect(yield* startOrder([...line.parts, ack])).toEqual(["a", "ack", "b", "c"]);
    }).pipe(Effect.provide(quick)),
  ));

test("withdrawing a part drops it and the parts after it, and a group key drops every unstarted part", () =>
  runClock(
    Effect.gen(function* () {
      const scheduler = yield* makeScheduler(options);
      yield* advance(1_000);
      const line = yield* scheduler.submitGroup({
        key: ItemKey.make("line"),
        lane: "line",
        parts: [part("a"), part("b"), part("c")],
      });
      yield* advance(2_000);
      expect(yield* scheduler.withdraw(ItemKey.make("b"))).toBe("withdrawn");
      const other = yield* scheduler.submitGroup({
        key: ItemKey.make("other"),
        lane: "line",
        parts: [part("x"), part("y")],
      });
      expect(yield* scheduler.withdraw(ItemKey.make("other"))).toBe("withdrawn");
      yield* advance(30_000);
      const [a, b, c] = line.parts;
      expect((yield* a.outcome)._tag).toBe("Ended");
      for (const handle of [b!, c!, ...other.parts])
        expect(yield* handle.outcome).toEqual({ _tag: "Dropped", reason: "withdrawn" });
    }).pipe(Effect.provide(quick)),
  ));

test("a failed part drops the parts after it", () =>
  runClock(
    Effect.gen(function* () {
      const scheduler = yield* makeScheduler(options);
      yield* advance(1_000);
      const line = yield* scheduler.submitGroup({
        key: ItemKey.make("line"),
        lane: "line",
        parts: [part("a"), part("b"), part("c")],
      });
      yield* advance(30_000);
      const [a, b, c] = line.parts;
      expect((yield* a.outcome)._tag).toBe("Ended");
      expect((yield* b!.outcome)._tag).toBe("Failed");
      expect(yield* c!.outcome).toEqual({ _tag: "Dropped", reason: "withdrawn" });
    }).pipe(
      Effect.provide(
        Simulation.layerSim({
          fixedBuildTime: 500,
          buildRatio: 0,
          faults: { buildFails: (sequence) => sequence === 2 },
        }),
      ),
    ),
  ));

test("repeating a group returns its handle, and a changed group or a reused part key is KeyMismatch", () =>
  runClock(
    Effect.gen(function* () {
      const scheduler = yield* makeScheduler(options);
      const group = {
        key: ItemKey.make("line"),
        lane: "line",
        parts: [part("a"), part("b")],
      } as const;
      const handle = yield* scheduler.submitGroup(group);
      expect(yield* scheduler.submitGroup(group)).toBe(handle);
      const changed = yield* Effect.flip(
        scheduler.submitGroup({ ...group, parts: [part("a"), part("c")] }),
      );
      expect(changed._tag).toBe("KeyMismatch");
      const reused = yield* Effect.flip(
        scheduler.submitGroup({ key: ItemKey.make("other"), lane: "line", parts: [part("b")] }),
      );
      expect(reused._tag).toBe("KeyMismatch");
    }).pipe(Effect.provide(quick)),
  ));
