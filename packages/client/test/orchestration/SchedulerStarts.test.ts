import { expect, test } from "vitest";
import { Clock, Effect, Stream } from "effect";
import type { Duration } from "effect";
import { TestClock } from "effect/testing";
import { makeScheduler, ItemKey } from "../../src/orchestration/scheduler.js";
import type {
  AsRunStatus,
  ItemHandle,
  SchedulerOptions,
} from "../../src/orchestration/scheduler.js";
import { ClipRequest } from "../../src/orchestration/request.js";
import * as Simulation from "../../src/simulation/index.js";
import { runClock } from "./SourceFixture.js";

const clip = (prompt: string) =>
  new ClipRequest({ prompt, references: [], durationSeconds: 5, metadata: {} });
const item = (key: string) => ({ key: ItemKey.make(key), lane: "line", request: clip(key) });

const advance = (milliseconds: number) =>
  Effect.gen(function* () {
    for (let elapsed = 0; elapsed < milliseconds; elapsed += 20)
      yield* TestClock.adjust(Math.min(20, milliseconds - elapsed));
  });

const options = (floor: Duration.Input, target: Duration.Input) =>
  ({
    lanes: [{ name: "ack" }, { name: "line" }],
    filler: {
      runway: { floor, target },
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

test("an Asap item airs at the next boundary, ahead of everything waiting in every lane", () =>
  runClock(
    Effect.gen(function* () {
      const scheduler = yield* makeScheduler(options("0 seconds", "1 second"));
      yield* advance(1_000);
      const a = yield* scheduler.submit(item("a"));
      const b = yield* scheduler.submit(item("b"));
      const ack = yield* scheduler.submit({ ...item("ack"), lane: "ack" });
      yield* advance(1_000);
      const x = yield* scheduler.submit({ ...item("x"), start: { _tag: "Asap" } });
      yield* advance(40_000);
      expect(yield* startOrder([a, b, ack, x])).toEqual(["a", "x", "ack", "b"]);
    }).pipe(Effect.provide(quick)),
  ));

test("a Manual item is built ahead and held behind filler until it is released", () =>
  runClock(
    Effect.gen(function* () {
      const scheduler = yield* makeScheduler(options("5 seconds", "10 seconds"));
      yield* advance(1_000);
      const m = yield* scheduler.submit({ ...item("m"), start: { _tag: "Manual" } });
      const statuses: AsRunStatus["_tag"][] = [];
      yield* scheduler.asRun.pipe(
        Stream.runForEach((event) =>
          Effect.sync(() => {
            if (event.key === "m") statuses.push(event.status._tag);
          }),
        ),
        Effect.forkScoped,
      );
      yield* advance(30_000);
      // Built once and held: never started, and never removed to be rebuilt.
      expect(statuses).toEqual(["Building", "Ready"]);
      const releasedAt = yield* Clock.currentTimeMillis;
      yield* scheduler.release(ItemKey.make("m"));
      yield* advance(10_000);
      const started = yield* m.started;
      // It goes at the next boundary: within one playing filler clip.
      expect(started._tag === "Started" && started.at - releasedAt).toBeLessThan(5_500);
    }).pipe(Effect.provide(quick)),
  ));

test("with filler off, a Manual item is built only once it is released", () =>
  runClock(
    Effect.gen(function* () {
      const scheduler = yield* makeScheduler(options("0 seconds", "1 second"));
      yield* advance(1_000);
      const m = yield* scheduler.submit({ ...item("m"), start: { _tag: "Manual" } });
      yield* advance(10_000);
      expect((yield* scheduler.state).lanes[1]?.keys).toEqual([ItemKey.make("m")]);
      const statuses: AsRunStatus["_tag"][] = [];
      yield* scheduler.asRun.pipe(
        Stream.runForEach((event) =>
          Effect.sync(() => {
            if (event.key === "m") statuses.push(event.status._tag);
          }),
        ),
        Effect.forkScoped,
      );
      yield* scheduler.release(ItemKey.make("m"));
      yield* advance(10_000);
      expect((yield* m.started)._tag).toBe("Started");
      expect(statuses[0]).toBe("Building");
    }).pipe(Effect.provide(quick)),
  ));
