import { expect, test } from "vitest";
import { Clock, Effect, Option, Result, Stream } from "effect";
import { TestClock } from "effect/testing";
import { ReactorError } from "../../src/errors.js";
import { ClipRequest, PolicyFailure } from "../../src/orchestration/request.js";
import { ItemKey, makeScheduler } from "../../src/orchestration/scheduler.js";
import type { AsRunEvent, ItemSpec, SchedulerOptions } from "../../src/orchestration/scheduler.js";
import { Engine } from "../../src/orchestration/types.js";
import type { EngineShape } from "../../src/orchestration/types.js";
import * as Simulation from "../../src/simulation/index.js";
import { record, runClock } from "./SourceFixture.js";
import { steppableWall } from "./WallClock.js";

const clip = (prompt: string, durationSeconds = 5) =>
  new ClipRequest({ prompt, references: [], durationSeconds, metadata: {} });

const options: SchedulerOptions = {
  lanes: [{ name: "urgent" }, { name: "line" }],
  filler: {
    runway: { floor: "5 seconds", target: "5 seconds" },
    clip: ({ index }) => clip(`Filler ${index}`),
  },
};

const elapse = (clock: TestClock.TestClock, millis: number) =>
  Effect.gen(function* () {
    for (let elapsed = 0; elapsed < millis; elapsed += 100)
      yield* clock.adjust(Math.min(100, millis - elapsed));
  });

const setup = Effect.gen(function* () {
  const handle = yield* Simulation.make({ fixedBuildTime: 0, buildRatio: 0 });
  const scheduler = yield* makeScheduler(options).pipe(
    Effect.provideService(Engine, handle.engine),
  );
  const events: AsRunEvent[] = [];
  yield* scheduler.asRun.pipe(
    Stream.runForEach((event) =>
      Effect.sync(() => {
        events.push(event);
      }),
    ),
    Effect.forkScoped,
  );
  yield* Effect.yieldNow;
  return { scheduler, events };
});

const status = (events: readonly AsRunEvent[], key: ItemSpec["key"], tag: string) =>
  events.find((event) => event.key === key && event.status._tag === tag)?.status;

test("the same key and spec return one handle, while a changed spec is KeyMismatch", () =>
  runClock(
    Effect.gen(function* () {
      const { scheduler } = yield* setup;
      const item: ItemSpec = {
        key: ItemKey.make("opening"),
        lane: "line",
        request: clip("Opening story"),
      };
      const first = yield* scheduler.submit(item);
      expect(yield* scheduler.submit(item)).toBe(first);
      const changed = yield* Effect.result(
        scheduler.submit({ ...item, request: clip("Different story") }),
      );
      expect(Result.isFailure(changed) && changed.failure._tag).toBe("KeyMismatch");
    }),
  ));

test("capture refuses nested scheduling accessors without running them", () =>
  runClock(
    Effect.gen(function* () {
      const { scheduler } = yield* setup;
      let reads = 0;
      const windows = [
        Object.defineProperty({ firm: true }, "notBefore", {
          enumerable: true,
          get: () => {
            reads++;
            return "1 second";
          },
        }),
        {
          firm: true,
          notBefore: Object.defineProperty({}, "seconds", {
            enumerable: true,
            get: () => {
              reads++;
              return 1;
            },
          }),
        },
      ];
      for (const [index, window] of windows.entries()) {
        const result = yield* Effect.result(
          scheduler.submit({
            key: ItemKey.make(`accessor-${index}`),
            lane: "line",
            request: clip("Safe capture"),
            window,
          }),
        );
        expect(Result.isFailure(result)).toBe(true);
      }
      expect(reads).toBe(0);
    }),
  ));

test("deadline admission counts the current boundary but lets a line overtake Ready filler", () =>
  runClock(
    Effect.gen(function* () {
      const handle = yield* Simulation.make({ fixedBuildTime: 0, buildRatio: 0 });
      const scheduler = yield* makeScheduler({
        lanes: [{ name: "line" }],
        filler: {
          runway: { floor: "15 seconds", target: "20 seconds" },
          clip: ({ index }) => clip(`Filler ${index}`),
        },
      }).pipe(Effect.provideService(Engine, handle.engine));
      const clock = yield* TestClock.testClockWith(Effect.succeed);
      yield* elapse(clock, 1_000);
      expect((yield* handle.engine.state).ready.length).toBeGreaterThan(1);
      const impossible = yield* Effect.result(
        scheduler.submit({
          key: ItemKey.make("impossible-deadline"),
          lane: "line",
          request: clip("Impossible line"),
          window: { startBy: "1 second", firm: true },
        }),
      );
      expect(Result.isFailure(impossible) && impossible.failure._tag).toBe("WouldMissDeadline");
      // PR #34: an already elapsed relative deadline is a deadline refusal.
      const expired = yield* Effect.result(
        scheduler.submit({
          key: ItemKey.make("elapsed-deadline"),
          lane: "line",
          request: clip("Expired line"),
          window: { startBy: -1, firm: true },
        }),
      );
      expect(Result.isFailure(expired) && expired.failure._tag).toBe("WouldMissDeadline");
      const line = yield* scheduler.submit({
        key: ItemKey.make("feasible-deadline"),
        lane: "line",
        request: clip("Feasible line"),
        window: { startBy: "8 seconds", firm: true },
      });
      yield* elapse(clock, 8_000);
      // H3 quantizes five requested seconds to 124 frames at 24 fps.
      expect(yield* line.started).toMatchObject({ _tag: "Started", durationSeconds: 124 / 24 });
    }),
  ));

test("a retiring source does not consume the replacement's deadline or build slot", () =>
  runClock(
    Effect.gen(function* () {
      const handle = yield* Simulation.make({ fixedBuildTime: 0, buildRatio: 0 });
      const baseline = yield* handle.engine.state;
      const oldClip = { ...record("old-ready", 20), sessionId: "retiring" };
      const state = {
        ...baseline,
        availability: "Unavailable" as const,
        sessions: [
          { sessionId: "retiring", availability: "Unavailable" as const },
          { sessionId: "replacement", availability: "Ready" as const },
        ],
        preferredSessionId: Option.some("replacement"),
        retiringSessionId: Option.some("retiring"),
        ready: [oldClip],
      };
      let attempts = 0;
      const engine: EngineShape = {
        ...handle.engine,
        state: Effect.succeed(state),
        observe: () => Effect.succeed({ initial: state, events: Stream.never }),
        enqueueOnSource: undefined,
        enqueue: () =>
          Effect.sync(() => {
            attempts++;
          }).pipe(Effect.andThen(PolicyFailure.refuse("SessionRecovering", "fixture refusal"))),
      };
      const scheduler = yield* makeScheduler({
        lanes: [{ name: "line" }],
        filler: { runway: { floor: 0, target: "5 seconds" }, clip: () => clip("unused") },
      }).pipe(Effect.provideService(Engine, engine));
      const accepted = yield* scheduler.submit({
        key: ItemKey.make("replacement-deadline"),
        lane: "line",
        request: clip("Replacement line"),
        window: { startBy: "2 seconds", firm: true },
      });
      expect(accepted.key).toBe("replacement-deadline");
      const clock = yield* TestClock.testClockWith(Effect.succeed);
      yield* elapse(clock, 200);
      expect(attempts).toBe(1);
    }),
  ));

const windowRun = (firm: boolean) =>
  runClock(
    Effect.gen(function* () {
      const testClock = yield* TestClock.testClockWith(Effect.succeed);
      const wall = yield* steppableWall;
      return yield* Effect.gen(function* () {
        const { scheduler, events } = yield* setup;
        const key = ItemKey.make(firm ? "firm" : "soft");
        yield* scheduler.submit({
          key,
          lane: "line",
          request: clip(firm ? "Firm line" : "Soft line"),
          window: { notBefore: "6 seconds", startBy: "18 seconds", firm },
        });
        yield* scheduler.submit({
          key: ItemKey.make("urgent-blocker"),
          lane: "urgent",
          request: clip("Urgent blocker", 15),
        });
        yield* elapse(testClock, 1_000);
        yield* wall.step(3_600_000);
        yield* elapse(testClock, 1_000);
        expect(status(events, key, "Dropped")).toBeUndefined();
        expect(status(events, key, "Started")).toBeUndefined();
        yield* wall.step(-3_600_000);
        yield* elapse(testClock, 30_000);
        return { events, key };
      }).pipe(Effect.provideService(Clock.Clock, wall.clock));
    }),
  );

test("a firm window expires on elapsed time even after a wall-clock correction", async () => {
  const { events, key } = await windowRun(true);
  expect(status(events, key, "Dropped")).toEqual({ _tag: "Dropped", reason: "late" });
  expect(status(events, key, "Started")).toBeUndefined();
});

test("a soft window stays eligible and records a late start", async () => {
  const { events, key } = await windowRun(false);
  const started = status(events, key, "Started");
  expect(started?._tag).toBe("Started");
  if (started?._tag === "Started") expect(started.lateByMillis).toBeGreaterThan(0);
  expect(status(events, key, "Dropped")).toBeUndefined();
});

test("At holds a clip until its wall anchor, then late drop refuses an overdue anchor", () =>
  runClock(
    Effect.gen(function* () {
      const clock = yield* TestClock.testClockWith(Effect.succeed);
      const { scheduler, events } = yield* setup;
      const anchor = (yield* Clock.currentTimeMillis) + 7_000;
      const key = ItemKey.make("at-anchor");
      yield* scheduler.submit({
        key,
        lane: "line",
        request: clip("At anchor"),
        start: { _tag: "At", time: anchor, late: { _tag: "nextBoundary" } },
      });
      yield* elapse(clock, 6_000);
      expect(status(events, key, "Started")).toBeUndefined();
      yield* elapse(clock, 6_000);
      const started = status(events, key, "Started");
      expect(started?._tag).toBe("Started");
      if (started?._tag === "Started") {
        expect(started.at).toBeGreaterThanOrEqual(anchor);
        expect(started.at).toBeLessThanOrEqual(anchor + 5_000);
      }
      const late = ItemKey.make("late-drop");
      yield* scheduler.submit({
        key: late,
        lane: "line",
        request: clip("Overdue anchor"),
        start: { _tag: "At", time: anchor, late: { _tag: "drop" } },
      });
      yield* elapse(clock, 500);
      expect(status(events, late, "Dropped")).toEqual({ _tag: "Dropped", reason: "late" });
    }),
  ));

test("At cannot autoplay before its anchor when filler builds fail", () =>
  runClock(
    Effect.gen(function* () {
      const handle = yield* Simulation.make({
        build: (record) =>
          Effect.sleep("500 millis").pipe(
            Effect.andThen(
              record.request.prompt.startsWith("Filler")
                ? Effect.fail(ReactorError.fromCode("InvalidState", "No filler available"))
                : Effect.succeed(record.durationSeconds),
            ),
          ),
      });
      const scheduler = yield* makeScheduler(options).pipe(
        Effect.provideService(Engine, handle.engine),
      );
      const events: AsRunEvent[] = [];
      yield* scheduler.asRun.pipe(
        Stream.runForEach((event) =>
          Effect.sync(() => {
            events.push(event);
          }),
        ),
        Effect.forkScoped,
      );
      const key = ItemKey.make("future-without-filler");
      const anchor = (yield* Clock.currentTimeMillis) + 15_000;
      yield* scheduler.submit({
        key,
        lane: "line",
        request: clip("Timed line"),
        start: { _tag: "At", time: anchor, late: { _tag: "nextBoundary" } },
      });
      const clock = yield* TestClock.testClockWith(Effect.succeed);
      yield* elapse(clock, 5_000);
      expect(status(events, key, "Started")).toBeUndefined();
    }),
  ));

test("At follows a corrected wall clock while elapsed windows remain monotonic", () =>
  runClock(
    Effect.gen(function* () {
      const clock = yield* TestClock.testClockWith(Effect.succeed);
      const wall = yield* steppableWall;
      return yield* Effect.gen(function* () {
        const { scheduler, events } = yield* setup;
        const key = ItemKey.make("wall-corrected-at");
        const anchor = (yield* Clock.currentTimeMillis) + 10_000;
        yield* scheduler.submit({
          key,
          lane: "line",
          request: clip("Wall corrected anchor"),
          start: { _tag: "At", time: anchor, late: { _tag: "nextBoundary" } },
        });
        yield* elapse(clock, 1_000);
        expect(status(events, key, "Started")).toBeUndefined();
        yield* wall.step(9_000);
        yield* elapse(clock, 5_000);
        const started = status(events, key, "Started");
        expect(started?._tag).toBe("Started");
        if (started?._tag === "Started") expect(started.at).toBeGreaterThanOrEqual(anchor);
      }).pipe(Effect.provideService(Clock.Clock, wall.clock));
    }),
  ));

test("a backward wall correction retries a refused At deferral without closing the scheduler", () =>
  runClock(
    Effect.gen(function* () {
      const clock = yield* TestClock.testClockWith(Effect.succeed);
      const wall = yield* steppableWall;
      return yield* Effect.gen(function* () {
        let successfulFiller = 0;
        const handle = yield* Simulation.make({
          build: (record) =>
            record.request.prompt.startsWith("Filler") && successfulFiller++ >= 2
              ? Effect.fail(ReactorError.fromCode("InvalidState", "Filler unavailable"))
              : Effect.succeed(record.durationSeconds),
        });
        let removals = 0;
        const engine: EngineShape = {
          ...handle.engine,
          // PR #34: a temporary refusal to hold one anchor must not kill all lanes.
          remove: (id) =>
            Effect.suspend(() =>
              ++removals === 1
                ? PolicyFailure.refuse("SessionRecovering", "Queue is synchronizing")
                : handle.engine.remove(id),
            ),
        };
        const scheduler = yield* makeScheduler(options).pipe(Effect.provideService(Engine, engine));
        const events: AsRunEvent[] = [];
        yield* scheduler.asRun.pipe(
          Stream.runForEach((event) =>
            Effect.sync(() => {
              events.push(event);
            }),
          ),
          Effect.forkScoped,
        );
        const key = ItemKey.make("backward-anchor");
        yield* scheduler.submit({
          key,
          lane: "line",
          request: clip("Timed line"),
          start: {
            _tag: "At",
            time: (yield* Clock.currentTimeMillis) + 10_000,
            late: { _tag: "nextBoundary" },
          },
        });
        yield* elapse(clock, 500);
        expect(status(events, key, "Ready")).toBeDefined();
        yield* wall.step(-30_000);
        yield* elapse(clock, 12_000);
        expect((yield* scheduler.state).accepting).toBe(true);
        expect(removals).toBe(2);
        expect(status(events, key, "Started")).toBeUndefined();
        expect(
          events.filter((event) => event.key === key && event.status._tag === "Accepted"),
        ).toHaveLength(1);
      }).pipe(Effect.provideService(Clock.Clock, wall.clock));
    }),
  ));
