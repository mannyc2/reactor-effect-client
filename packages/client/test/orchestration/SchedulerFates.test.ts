import { expect, test } from "vitest";
import { Clock, Effect, Exit, Fiber, Option, Queue, Result, Scope, Stream } from "effect";
import { TestClock } from "effect/testing";
import { ReactorError } from "../../src/errors.js";
import { ClipRequest } from "../../src/orchestration/request.js";
import * as Renewal from "../../src/orchestration/renewal.js";
import { fillerKey, schedulerKeyOf } from "../../src/orchestration/scheduler-key.js";
import { ItemKey, makeScheduler } from "../../src/orchestration/scheduler.js";
import type {
  AsRunEvent,
  AsRunStatus,
  SchedulerOptions,
} from "../../src/orchestration/scheduler.js";
import { Engine } from "../../src/orchestration/types.js";
import type { EngineEvent, EngineShape, EngineState } from "../../src/orchestration/types.js";
import * as Simulation from "../../src/simulation/index.js";
import {
  failure,
  gate,
  readyState,
  record,
  refusal,
  runClock,
  sourceFixture,
} from "./SourceFixture.js";
import type { SourceFixture } from "./SourceFixture.js";

const clip = (prompt: string) =>
  new ClipRequest({ prompt, references: [], durationSeconds: 5, metadata: {} });

const options: SchedulerOptions = {
  lanes: [{ name: "line" }],
  filler: { runway: { floor: 0, target: "5 seconds" }, clip: () => clip("Unused filler") },
};

const advance = (millis: number) =>
  Effect.gen(function* () {
    for (let elapsed = 0; elapsed < millis; elapsed += 100)
      yield* TestClock.adjust(Math.min(100, millis - elapsed));
  });

const watch = (scheduler: { readonly asRun: Stream.Stream<AsRunEvent> }) =>
  Effect.gen(function* () {
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
    return events;
  });

const settled = (effect: Effect.Effect<AsRunStatus>) =>
  Effect.gen(function* () {
    const fiber = yield* Effect.forkChild(effect);
    yield* Effect.yieldNow;
    const exit = fiber.pollUnsafe();
    return exit !== undefined && Exit.isSuccess(exit) ? exit.value : "waiting";
  });

// PR #34, c1171c9: an inbox can close after admitting a command, or before a
// later call. A controlled Scope owns this race; a live session cannot force it.
test("scope closure settles pending and subsequent scheduler calls", () =>
  runClock(
    Effect.gen(function* () {
      const handle = yield* Simulation.make({ fixedBuildTime: 0, buildRatio: 0 });
      const scope = yield* Scope.make();
      const entered = yield* gate;
      const engine: EngineShape = {
        ...handle.engine,
        enqueueOnSource: undefined,
        enqueue: () => entered.release.pipe(Effect.andThen(Effect.never)),
      };
      const scheduler = yield* makeScheduler(options).pipe(
        Effect.provideService(Engine, engine),
        Scope.provide(scope),
      );
      const spec = { key: ItemKey.make("closing"), lane: "line", request: clip("closing") };
      yield* scheduler.submit(spec);
      yield* entered.wait;
      const pending = yield* scheduler
        .withdraw(spec.key)
        .pipe(Effect.asVoid, Effect.result, Effect.forkScoped);
      yield* advance(100);
      yield* Scope.close(scope, Exit.void);
      const later = yield* Effect.forEach(
        [scheduler.submit(spec), scheduler.withdraw(spec.key), scheduler.drain()],
        (call) => call.pipe(Effect.asVoid, Effect.result, Effect.forkScoped),
      );
      yield* advance(100);
      for (const fiber of [pending, ...later]) {
        const exit = fiber.pollUnsafe();
        expect(exit).toBeDefined();
        if (exit !== undefined && Exit.isSuccess(exit)) {
          expect(Result.isFailure(exit.value)).toBe(true);
          if (Result.isFailure(exit.value))
            expect(refusal(exit.value.failure)).toBe("SessionClosed");
        }
      }
    }),
  ));

test("a terminal observation failure is available to the scheduler supervisor", () =>
  runClock(
    Effect.gen(function* () {
      const handle = yield* Simulation.make({ fixedBuildTime: 0, buildRatio: 0 });
      const feed = yield* Queue.unbounded<EngineEvent, ReactorError>();
      const engine: EngineShape = {
        ...handle.engine,
        observe: () =>
          Effect.map(handle.engine.state, (initial) => ({
            initial,
            events: Stream.fromQueue(feed),
          })),
      };
      const scheduler = yield* makeScheduler(options).pipe(Effect.provideService(Engine, engine));
      const cause = ReactorError.fromCode("Disconnected", "Observation stopped");
      yield* Queue.fail(feed, cause);
      yield* advance(100);
      expect(scheduler.failure).toBeDefined();
      expect(yield* scheduler.failure).toBe(cause);
    }),
  ));

test("Synchronizing Ready facts do not repeat an acknowledged move", () =>
  runClock(
    Effect.gen(function* () {
      const handle = yield* Simulation.make({ fixedBuildTime: 0, buildRatio: 0 });
      let stale: EngineState | undefined;
      let moves = 0;
      const engine: EngineShape = {
        ...handle.engine,
        state: Effect.suspend(() =>
          stale === undefined ? handle.engine.state : Effect.succeed(stale),
        ),
        move: (id, position, queue) =>
          Effect.gen(function* () {
            moves++;
            if (moves > 1) return yield* Effect.never;
            const before = yield* handle.engine.state;
            yield* handle.engine.move(id, position, queue);
            stale = {
              ...before,
              availability: "Synchronizing",
              sessions: before.sessions.map((session) => ({
                ...session,
                availability: "Synchronizing",
              })),
            };
          }),
      };
      const scheduler = yield* makeScheduler({
        lanes: [{ name: "line" }],
        filler: {
          runway: { floor: "10 seconds", target: "15 seconds" },
          clip: () => clip("filler"),
        },
      }).pipe(Effect.provideService(Engine, engine));
      yield* advance(500);
      yield* scheduler.submit({ key: ItemKey.make("move"), lane: "line", request: clip("line") });
      yield* advance(1_000);
      expect(moves).toBe(1);
    }),
  ));

test("terminal key history has an explicit bound and retained handles keep their fate", () =>
  runClock(
    Effect.gen(function* () {
      const handle = yield* Simulation.make({ fixedBuildTime: 0, buildRatio: 0 });
      const scheduler = yield* makeScheduler({ ...options, maxHistory: 2 }).pipe(
        Effect.provideService(Engine, handle.engine),
      );
      const specs = [0, 1, 2].map((index) => ({
        key: ItemKey.make(`history-${index}`),
        lane: "line",
        request: clip("history"),
        window: { notBefore: "1 hour" as const, firm: false },
      }));
      const handles = [];
      for (const spec of specs) {
        handles.push(yield* scheduler.submit(spec));
        yield* scheduler.withdraw(spec.key);
      }
      expect(yield* scheduler.submit(specs[2]!)).toBe(handles[2]);
      expect(yield* scheduler.submit(specs[0]!)).not.toBe(handles[0]);
      expect(yield* handles[0]!.outcome).toEqual({ _tag: "Dropped", reason: "withdrawn" });
    }),
  ));

test("a clip that starts and ends before its enqueue reply retains its observed fate", () =>
  runClock(
    Effect.gen(function* () {
      const handle = yield* Simulation.make({ fixedBuildTime: 0, buildRatio: 0 });
      const feed = yield* Queue.unbounded<EngineEvent, ReactorError>();
      const clipId = record("early-finished", 5).clipId;
      const enqueueEarly = () =>
        Effect.gen(function* () {
          yield* Queue.offer(feed, {
            _tag: "Started",
            clipId,
            durationSeconds: 5,
            at: yield* Clock.currentTimeMillis,
            atMonotonicMillis: Number((yield* Clock.monotonicTimeNanos) / 1_000_000n),
          });
          yield* Effect.sleep("5 seconds");
          yield* Queue.offer(feed, {
            _tag: "Ended",
            clipId,
            termination: "finished",
            at: yield* Clock.currentTimeMillis,
            atMonotonicMillis: Number((yield* Clock.monotonicTimeNanos) / 1_000_000n),
          });
          yield* Effect.sleep("100 millis");
          return clipId;
        });
      const engine: EngineShape = {
        ...handle.engine,
        enqueue: enqueueEarly,
        enqueueOnSource: enqueueEarly,
        observe: () =>
          Effect.map(handle.engine.state, (initial) => ({
            initial,
            events: Stream.fromQueue(feed),
          })),
      };
      const scheduler = yield* makeScheduler(options).pipe(Effect.provideService(Engine, engine));
      const events = yield* watch(scheduler);
      const key = ItemKey.make("early-finished");
      const item = yield* scheduler.submit({ key, lane: "line", request: clip("Early finish") });
      yield* advance(5_500);
      const statuses = events
        .filter((event) => event.key === key)
        .map((event) => event.status._tag);
      expect(statuses).toContain("Started");
      expect(statuses).toContain("Ended");
      expect((yield* item.firstDecisive)._tag).toBe("Started");
      expect((yield* item.outcome)._tag).toBe("Ended");
    }),
  ));

test("drain waits for the playing boundary, withdraws waiting work, and retains as-run evidence", () =>
  runClock(
    Effect.gen(function* () {
      const handle = yield* Simulation.make({ fixedBuildTime: 0, buildRatio: 0 });
      const scheduler = yield* makeScheduler(options).pipe(
        Effect.provideService(Engine, handle.engine),
      );
      const events = yield* watch(scheduler);
      const playingKey = ItemKey.make("playing");
      const playing = yield* scheduler.submit({
        key: playingKey,
        lane: "line",
        request: clip("Playing line"),
      });
      yield* advance(1_000);
      const started = yield* playing.started;
      expect(started._tag).toBe("Started");
      const waitingKey = ItemKey.make("waiting");
      const waiting = yield* scheduler.submit({
        key: waitingKey,
        lane: "line",
        request: clip("Waiting line"),
      });
      yield* advance(100);
      const draining = yield* Effect.forkScoped(scheduler.drain());
      yield* advance(100);
      expect((yield* scheduler.state).accepting).toBe(false);
      expect(draining.pollUnsafe()).toBeUndefined();
      expect(yield* waiting.outcome).toEqual({ _tag: "Dropped", reason: "withdrawn" });
      yield* advance(6_000);
      yield* Fiber.join(draining);
      const ended = yield* playing.outcome;
      expect(ended._tag).toBe("Ended");
      expect(yield* playing.firstDecisive).toEqual(started);
      if (ended._tag === "Ended") {
        expect(ended.termination).toBe("finished");
        expect(ended.airedSeconds).toBeGreaterThan(4.9);
        expect(ended.airedSeconds).toBeLessThan(5.3);
      }
      expect(
        events.filter((event) => event.key === playingKey).map((event) => event.status._tag),
      ).toEqual(["Accepted", "Building", "Ready", "Started", "Ended"]);
      expect(
        events.some((event) => event.key === waitingKey && event.status._tag === "Started"),
      ).toBe(false);
      const refused = yield* Effect.flip(
        scheduler.submit({
          key: ItemKey.make("after-drain"),
          lane: "line",
          request: clip("Late line"),
        }),
      );
      expect(refusal(refused)).toBe("InvalidRequest");
    }),
  ));

test("drain removes Ready filler before the current playing clip ends", () =>
  runClock(
    Effect.gen(function* () {
      const handle = yield* Simulation.make({ fixedBuildTime: 0, buildRatio: 0 });
      const scheduler = yield* makeScheduler({
        lanes: [{ name: "line" }],
        filler: {
          runway: { floor: "10 seconds", target: "20 seconds" },
          clip: ({ index }) => clip(`filler ${index}`),
        },
      }).pipe(Effect.provideService(Engine, handle.engine));
      yield* advance(1_000);
      const before = yield* handle.engine.state;
      expect(Option.isSome(before.playing)).toBe(true);
      expect(before.ready.length).toBeGreaterThan(0);
      const draining = yield* Effect.forkScoped(scheduler.drain());
      yield* advance(4_500);
      yield* Fiber.join(draining);
      const after = yield* handle.engine.state;
      expect(after.ready).toHaveLength(0);
      expect(Option.isNone(after.playing)).toBe(true);
      expect((yield* scheduler.state).accepting).toBe(false);
    }),
  ));

test("withdraw waits for the source removal result before publishing Dropped", () =>
  runClock(
    Effect.gen(function* () {
      const entered = yield* gate;
      const release = yield* gate;
      const handle = yield* Simulation.make({ fixedBuildTime: "10 seconds", buildRatio: 0 });
      const engine: EngineShape = {
        ...handle.engine,
        remove: (id) =>
          entered.release.pipe(
            Effect.andThen(release.wait),
            Effect.andThen(handle.engine.remove(id)),
          ),
      };
      const scheduler = yield* makeScheduler(options).pipe(Effect.provideService(Engine, engine));
      const events = yield* watch(scheduler);
      const key = ItemKey.make("withdrawn-during-build");
      const item = yield* scheduler.submit({ key, lane: "line", request: clip("Withdrawn line") });
      yield* advance(500);
      expect((yield* handle.engine.state).generationOrder).toHaveLength(1);
      const pending = yield* Effect.forkScoped(scheduler.withdraw(key));
      yield* entered.wait;
      expect(pending.pollUnsafe()).toBeUndefined();
      expect(events.some((event) => event.key === key && event.status._tag === "Dropped")).toBe(
        false,
      );
      yield* release.release;
      expect(yield* Fiber.join(pending)).toBe("withdrawn");
      expect(yield* item.outcome).toEqual({ _tag: "Dropped", reason: "withdrawn" });
      yield* advance(11_000);
      expect(events.some((event) => event.key === key && event.status._tag === "Started")).toBe(
        false,
      );
    }),
  ));

test("withdraw and drain wait for an in-flight enqueue, then remove its clip", () =>
  runClock(
    Effect.gen(function* () {
      const entered = yield* gate;
      const release = yield* gate;
      const handle = yield* Simulation.make({ fixedBuildTime: "10 seconds", buildRatio: 0 });
      const engine: EngineShape = {
        ...handle.engine,
        enqueueOnSource: undefined,
        enqueue: (request) =>
          entered.release.pipe(
            Effect.andThen(release.wait),
            Effect.andThen(handle.engine.enqueue(request)),
          ),
      };
      const scheduler = yield* makeScheduler(options).pipe(Effect.provideService(Engine, engine));
      const events = yield* watch(scheduler);
      const key = ItemKey.make("pending-enqueue");
      const item = yield* scheduler.submit({ key, lane: "line", request: clip("Delayed line") });
      yield* entered.wait;
      const withdrawing = yield* Effect.forkScoped(scheduler.withdraw(key));
      const draining = yield* Effect.forkScoped(scheduler.drain());
      yield* advance(200);
      expect(withdrawing.pollUnsafe()).toBeUndefined();
      expect(draining.pollUnsafe()).toBeUndefined();
      expect(events.some((event) => event.key === key && event.status._tag === "Dropped")).toBe(
        false,
      );
      yield* release.release;
      expect(yield* Fiber.join(withdrawing)).toBe("withdrawn");
      yield* Fiber.join(draining);
      expect(yield* item.outcome).toEqual({ _tag: "Dropped", reason: "withdrawn" });
      yield* advance(11_000);
      expect(events.some((event) => event.key === key && event.status._tag === "Started")).toBe(
        false,
      );
    }),
  ));

test("a fresh snapshot settles Unobserved after both start and end were missed", () =>
  runClock(
    Effect.gen(function* () {
      const handle = yield* Simulation.make({ fixedBuildTime: 0, buildRatio: 0 });
      const feeds: Queue.Queue<EngineEvent, ReactorError>[] = [];
      const engine: EngineShape = {
        ...handle.engine,
        observe: () =>
          Effect.gen(function* () {
            const feed = yield* Queue.unbounded<EngineEvent, ReactorError>();
            feeds.push(feed);
            return { initial: yield* handle.engine.state, events: Stream.fromQueue(feed) };
          }),
      };
      const scheduler = yield* makeScheduler(options).pipe(Effect.provideService(Engine, engine));
      const events = yield* watch(scheduler);
      const key = ItemKey.make("missed-start");
      const item = yield* scheduler.submit({ key, lane: "line", request: clip("Missed start") });
      yield* advance(7_000);
      expect(feeds).toHaveLength(1);
      expect(events.some((event) => event.key === key && event.status._tag === "Started")).toBe(
        false,
      );
      yield* Queue.fail(feeds[0]!, ReactorError.fromCode("Overflow", "Observation gap"));
      yield* advance(500);
      expect(feeds).toHaveLength(2);
      expect(yield* settled(item.started)).toEqual({ _tag: "Unobserved" });
      expect(yield* settled(item.outcome)).toEqual({ _tag: "Unobserved" });
      expect(events.some((event) => event.key === key && event.status._tag === "Started")).toBe(
        false,
      );
    }),
  ));

test("a playing recovery snapshot keeps its start evidence through the observed end", () =>
  runClock(
    Effect.gen(function* () {
      const handle = yield* Simulation.make({ fixedBuildTime: 0, buildRatio: 0 });
      const feeds: Queue.Queue<EngineEvent, ReactorError>[] = [];
      const engine: EngineShape = {
        ...handle.engine,
        observe: () =>
          feeds.length > 0
            ? handle.engine.observe()
            : Effect.gen(function* () {
                const feed = yield* Queue.unbounded<EngineEvent, ReactorError>();
                feeds.push(feed);
                return { initial: yield* handle.engine.state, events: Stream.fromQueue(feed) };
              }),
      };
      const scheduler = yield* makeScheduler(options).pipe(Effect.provideService(Engine, engine));
      const events = yield* watch(scheduler);
      const key = ItemKey.make("recovered-playing");
      const item = yield* scheduler.submit({ key, lane: "line", request: clip("Recovered line") });
      yield* advance(1_000);
      expect(feeds).toHaveLength(1);
      yield* Queue.fail(feeds[0]!, ReactorError.fromCode("Overflow", "Observation gap"));
      yield* advance(100);
      expect(yield* settled(item.started)).toMatchObject({ _tag: "Started" });
      yield* advance(6_000);
      const outcome = yield* item.outcome;
      expect(outcome._tag).toBe("Ended");
      if (outcome._tag === "Ended") expect(outcome.airedSeconds).toBeGreaterThan(0);
      expect(events.filter((event) => event.key === key).at(-1)?.status._tag).toBe("Ended");
    }),
  ));

test("an unknown enqueue is not replayed after its source retires", () =>
  runClock(
    Effect.gen(function* () {
      const sources: SourceFixture[] = [];
      const handle = yield* Renewal.make({
        open: Effect.gen(function* () {
          const index = sources.length;
          const source = yield* sourceFixture(`source-${index + 1}`, {
            execute: (plan, accept) =>
              index === 0 && plan.request.prompt === "Lost reply"
                ? Effect.fail(failure("unknown"))
                : accept,
          });
          sources.push(source);
          return { source: source.source, lifetime: "60 seconds" };
        }),
      });
      const scheduler = yield* makeScheduler(options).pipe(
        Effect.provideService(Engine, handle.engine),
      );
      const events = yield* watch(scheduler);
      const key = ItemKey.make("lost-reply");
      const item = yield* scheduler.submit({ key, lane: "line", request: clip("Lost reply") });
      yield* advance(1_000);
      expect(sources[0]!.sends).toHaveLength(1);
      expect(events.some((event) => event.key === key && event.status._tag === "Unknown")).toBe(
        true,
      );
      yield* advance(5_000);
      expect(sources).toHaveLength(2);
      expect(sources[0]!.sends).toHaveLength(1);
      expect(sources[1]!.sends.filter((plan) => plan.request.prompt === "Lost reply")).toHaveLength(
        0,
      );
      yield* advance(65_000);
      expect(events.filter((event) => event.key === key).at(-1)?.status).toEqual({
        _tag: "Unknown",
        terminal: true,
      });
      expect(yield* item.firstDecisive).toEqual({ _tag: "Unknown", terminal: true });
    }),
  ));

test("an orchestration failure ends uncertainty without claiming the clip failed", () =>
  runClock(
    Effect.gen(function* () {
      const handle = yield* Simulation.make({ fixedBuildTime: 0, buildRatio: 0 });
      const feed = yield* Queue.unbounded<EngineEvent, ReactorError>();
      const engine: EngineShape = {
        ...handle.engine,
        enqueueOnSource: undefined,
        enqueue: () => Effect.fail(failure("unknown")),
        observe: () =>
          Effect.map(handle.engine.state, (initial) => ({
            initial,
            events: Stream.fromQueue(feed),
          })),
      };
      const scheduler = yield* makeScheduler(options).pipe(Effect.provideService(Engine, engine));
      const events = yield* watch(scheduler);
      const key = ItemKey.make("unknown-on-close");
      const item = yield* scheduler.submit({ key, lane: "line", request: clip("Uncertain line") });
      yield* advance(200);
      expect(events.filter((event) => event.key === key).at(-1)?.status).toEqual({
        _tag: "Unknown",
      });
      expect(yield* settled(item.firstDecisive)).toBe("waiting");
      expect(yield* settled(item.started)).toBe("waiting");
      expect(yield* settled(item.outcome)).toBe("waiting");
      yield* Queue.offer(feed, {
        _tag: "SessionFailed",
        failure: ReactorError.fromCode("Closed", "Source closed"),
      });
      yield* advance(100);
      expect(events.filter((event) => event.key === key).at(-1)?.status).toEqual({
        _tag: "Unknown",
        terminal: true,
      });
      expect(yield* item.firstDecisive).toEqual({ _tag: "Unknown", terminal: true });
    }),
  ));

test("a submitted key adopts a queued clip found in a resumed session", () =>
  runClock(
    Effect.gen(function* () {
      const key = ItemKey.make("resumed-line");
      const pending = record("resumed-clip", 5);
      const caller = JSON.stringify({
        reactor_effect_scheduler: 1,
        key,
        application: {},
      });
      const metadata = JSON.stringify({
        reactor_effect_h3: 1,
        namespace: "resumed-session",
        submission: "prior-submission",
        caller,
      });
      let source: SourceFixture | undefined;
      const handle = yield* Renewal.make({
        open: Effect.gen(function* () {
          source = yield* sourceFixture("resumed-session", {
            initial: readyState({
              queued: [{ ...pending, provider: { ...pending.provider, metadata } }],
              generationOrder: [pending.clipId],
            }),
          });
          return { source: source.source, lifetime: "Infinity" };
        }),
      });
      const scheduler = yield* makeScheduler(options).pipe(
        Effect.provideService(Engine, handle.engine),
      );
      yield* advance(100);
      yield* scheduler.submit({ key, lane: "line", request: clip("Already admitted") });
      yield* advance(500);
      expect(
        source?.sends.filter((plan) => plan.request.prompt === "Already admitted"),
      ).toHaveLength(0);
    }),
  ));

test("withdraw of an unknown enqueue waits through an absent snapshot, then removes a keyed clip", () =>
  runClock(
    Effect.gen(function* () {
      const handle = yield* Simulation.make({ fixedBuildTime: 0, buildRatio: 0 });
      const feeds: Queue.Queue<EngineEvent, ReactorError>[] = [];
      let state = yield* handle.engine.state;
      let removals = 0;
      const engine: EngineShape = {
        ...handle.engine,
        state: Effect.sync(() => state),
        enqueueOnSource: undefined,
        enqueue: (request) =>
          request.prompt === "Unknown withdrawal"
            ? Effect.fail(failure("unknown"))
            : handle.engine.enqueue(request),
        remove: () =>
          Effect.sync(() => {
            removals++;
            state = { ...state, queued: [], generationOrder: [] };
            return "unstarted" as const;
          }),
        observe: () =>
          Effect.gen(function* () {
            const feed = yield* Queue.unbounded<EngineEvent, ReactorError>();
            feeds.push(feed);
            return { initial: state, events: Stream.fromQueue(feed) };
          }),
      };
      const scheduler = yield* makeScheduler(options).pipe(Effect.provideService(Engine, engine));
      const events = yield* watch(scheduler);
      const key = ItemKey.make("unknown-withdraw");
      yield* scheduler.submit({ key, lane: "line", request: clip("Unknown withdrawal") });
      yield* advance(500);
      expect(events.some((event) => event.key === key && event.status._tag === "Unknown")).toBe(
        true,
      );
      const withdrawing = yield* Effect.forkScoped(scheduler.withdraw(key));
      yield* advance(100);
      expect(withdrawing.pollUnsafe()).toBeUndefined();
      expect(events.some((event) => event.key === key && event.status._tag === "Dropped")).toBe(
        false,
      );
      yield* Queue.fail(feeds[0]!, ReactorError.fromCode("Overflow", "Observation gap"));
      yield* advance(200);
      expect(feeds).toHaveLength(2);
      expect(withdrawing.pollUnsafe()).toBeUndefined();
      expect(removals).toBe(0);
      const pending = record("late-keyed", 5);
      const metadata = JSON.stringify({
        reactor_effect_h3: 1,
        namespace: "recovered",
        submission: "late",
        caller: JSON.stringify({ reactor_effect_scheduler: 1, key, application: {} }),
      });
      state = {
        ...state,
        queued: [
          {
            ...pending,
            sessionId: state.sessions[0]!.sessionId,
            provider: { ...pending.provider, metadata },
          },
        ],
        generationOrder: [pending.clipId],
      };
      yield* Queue.fail(feeds[1]!, ReactorError.fromCode("Overflow", "Observation gap"));
      yield* advance(200);
      expect(yield* Fiber.join(withdrawing)).toBe("withdrawn");
      expect(removals).toBe(1);
      expect(events.some((event) => event.key === key && event.status._tag === "Dropped")).toBe(
        true,
      );
    }),
  ));

test("the initial observation adopts resumed filler before planning a build", () =>
  runClock(
    Effect.gen(function* () {
      const opened = yield* gate;
      const release = yield* gate;
      const handle = yield* Simulation.make({ fixedBuildTime: 0, buildRatio: 0 });
      const baseline = yield* handle.engine.state;
      const pending = record("prior-filler", 5);
      const metadata = JSON.stringify({
        reactor_effect_h3: 1,
        namespace: "prior",
        submission: "prior-filler",
        caller: JSON.stringify({
          reactor_effect_scheduler: 1,
          key: fillerKey(0),
          application: {},
        }),
      });
      const state = {
        ...baseline,
        queued: [
          {
            ...pending,
            sessionId: baseline.sessions[0]!.sessionId,
            provider: { ...pending.provider, metadata },
          },
        ],
        generationOrder: [pending.clipId],
      };
      const sends: string[] = [];
      const engine: EngineShape = {
        ...handle.engine,
        state: Effect.succeed(state),
        enqueueOnSource: undefined,
        enqueue: (request) =>
          Effect.sync(() => {
            sends.push(schedulerKeyOf(request) ?? "unkeyed");
            return pending.clipId;
          }),
        observe: () =>
          Effect.gen(function* () {
            yield* opened.release;
            yield* release.wait;
            return { initial: state, events: Stream.never };
          }),
      };
      const constructing = yield* Effect.forkScoped(
        makeScheduler({
          lanes: [{ name: "line" }],
          filler: {
            runway: { floor: "5 seconds", target: "5 seconds" },
            clip: () => clip("new filler"),
          },
        }).pipe(Effect.provideService(Engine, engine)),
      );
      yield* opened.wait;
      yield* advance(300);
      expect(constructing.pollUnsafe()).toBeUndefined();
      expect(sends).toHaveLength(0);
      yield* release.release;
      const scheduler = yield* Fiber.join(constructing);
      expect((yield* scheduler.state).sessions).toHaveLength(1);
      yield* advance(300);
      expect(sends).toHaveLength(0);
    }),
  ));

test("unknown filler is not replayed after an absent observation and holds drain", () =>
  runClock(
    Effect.gen(function* () {
      const handle = yield* Simulation.make({ fixedBuildTime: 0, buildRatio: 0 });
      const feeds: Queue.Queue<EngineEvent, ReactorError>[] = [];
      let sends = 0;
      const engine: EngineShape = {
        ...handle.engine,
        enqueueOnSource: undefined,
        enqueue: () =>
          Effect.sync(() => {
            sends++;
          }).pipe(Effect.andThen(Effect.fail(failure("unknown")))),
        observe: () =>
          Effect.gen(function* () {
            const feed = yield* Queue.unbounded<EngineEvent, ReactorError>();
            feeds.push(feed);
            return { initial: yield* handle.engine.state, events: Stream.fromQueue(feed) };
          }),
      };
      const scheduler = yield* makeScheduler({
        lanes: [{ name: "line" }],
        filler: {
          runway: { floor: "5 seconds", target: "5 seconds" },
          clip: () => clip("uncertain filler"),
        },
      }).pipe(Effect.provideService(Engine, engine));
      yield* advance(500);
      expect(sends).toBe(1);
      yield* Queue.fail(feeds[0]!, ReactorError.fromCode("Overflow", "Observation gap"));
      yield* advance(2_000);
      expect(feeds).toHaveLength(2);
      expect(sends).toBe(1);
      const draining = yield* Effect.forkScoped(scheduler.drain());
      yield* advance(1_000);
      expect(draining.pollUnsafe()).toBeUndefined();
    }),
  ));

test("a snapshot acquired before filler acceptance cannot erase its ownership", () =>
  runClock(
    Effect.gen(function* () {
      const handle = yield* Simulation.make({ fixedBuildTime: 0, buildRatio: 0 });
      const acquired = yield* gate;
      const deliver = yield* gate;
      const accepted = yield* gate;
      const feeds: Queue.Queue<EngineEvent, ReactorError>[] = [];
      let sends = 0;
      let observations = 0;
      const engine: EngineShape = {
        ...handle.engine,
        enqueueOnSource: undefined,
        enqueue: (request) =>
          Effect.gen(function* () {
            sends++;
            if (sends > 1) return yield* failure("not-submitted");
            yield* Queue.fail(feeds[0]!, ReactorError.fromCode("Overflow", "Snapshot race"));
            yield* acquired.wait;
            const clipId = yield* handle.engine.enqueue(request);
            yield* accepted.release;
            return clipId;
          }),
        observe: () =>
          Effect.gen(function* () {
            observations++;
            if (observations === 1) {
              const feed = yield* Queue.unbounded<EngineEvent, ReactorError>();
              feeds.push(feed);
              return { initial: yield* handle.engine.state, events: Stream.fromQueue(feed) };
            }
            const initial = yield* handle.engine.state;
            yield* acquired.release;
            yield* deliver.wait;
            return { initial, events: Stream.never };
          }),
      };
      const scheduler = yield* makeScheduler({
        lanes: [{ name: "line" }],
        filler: {
          runway: { floor: "1 second", target: "2 seconds" },
          clip: () => clip("race filler"),
        },
      }).pipe(Effect.provideService(Engine, engine));
      yield* advance(100);
      yield* accepted.wait;
      yield* advance(100);
      const beforeSnapshot = sends;
      yield* deliver.release;
      yield* advance(200);
      const state = yield* scheduler.state;
      expect(observations).toBe(2);
      expect(sends).toBe(beforeSnapshot);
      expect(
        state.playing === "filler" || state.sessions.some((s) => s.ready.includes("filler")),
      ).toBe(true);
    }),
  ));

test("a fresh snapshot forgets filler that ended during an observation gap", () =>
  runClock(
    Effect.gen(function* () {
      const handle = yield* Simulation.make({ fixedBuildTime: 0, buildRatio: 0 });
      const feeds: Queue.Queue<EngineEvent, ReactorError>[] = [];
      let sends = 0;
      let removals = 0;
      const engine: EngineShape = {
        ...handle.engine,
        enqueueOnSource: undefined,
        enqueue: (request) => {
          sends++;
          return sends === 1
            ? handle.engine.enqueue(request)
            : Effect.fail(failure("not-submitted", "no more filler"));
        },
        remove: (id) =>
          Effect.sync(() => {
            removals++;
          }).pipe(Effect.andThen(handle.engine.remove(id))),
        observe: () =>
          feeds.length > 0
            ? handle.engine.observe()
            : Effect.gen(function* () {
                const feed = yield* Queue.unbounded<EngineEvent, ReactorError>();
                feeds.push(feed);
                return { initial: yield* handle.engine.state, events: Stream.fromQueue(feed) };
              }),
      };
      const scheduler = yield* makeScheduler({
        lanes: [{ name: "line" }],
        filler: {
          runway: { floor: "5 seconds", target: "5 seconds" },
          clip: () => clip("gap filler"),
        },
      }).pipe(Effect.provideService(Engine, engine));
      yield* advance(6_000);
      expect(feeds).toHaveLength(1);
      expect(Option.isNone((yield* handle.engine.state).playing)).toBe(true);
      yield* Queue.fail(feeds[0]!, ReactorError.fromCode("Overflow", "Observation gap"));
      yield* advance(200);
      yield* scheduler.drain();
      expect(removals).toBe(0);
    }),
  ));

test("an uncertain removal has no false Dropped status or tight replay", () =>
  runClock(
    Effect.gen(function* () {
      const handle = yield* Simulation.make({ fixedBuildTime: 0, buildRatio: 0 });
      yield* handle.engine.setAutoplay(false);
      let removals = 0;
      const engine: EngineShape = {
        ...handle.engine,
        remove: () =>
          Effect.sync(() => {
            removals++;
          }).pipe(Effect.andThen(Effect.fail(failure("unknown", "remove outcome lost", "remove")))),
      };
      const scheduler = yield* makeScheduler(options).pipe(Effect.provideService(Engine, engine));
      const events = yield* watch(scheduler);
      const key = ItemKey.make("uncertain-remove");
      yield* scheduler.submit({ key, lane: "line", request: clip("Line waiting for removal") });
      yield* advance(500);
      const result = yield* Effect.result(scheduler.withdraw(key));
      expect(result._tag).toBe("Failure");
      expect(removals).toBe(1);
      const draining = yield* Effect.forkScoped(scheduler.drain());
      yield* advance(3_000);
      expect(draining.pollUnsafe()).toBeUndefined();
      expect(removals).toBe(1);
      expect(events.some((event) => event.key === key && event.status._tag === "Dropped")).toBe(
        false,
      );
    }),
  ));
