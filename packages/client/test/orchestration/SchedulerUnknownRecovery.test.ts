import { expect, test } from "vitest";
import {
  Cause,
  Clock,
  Deferred,
  Effect,
  Exit,
  Fiber,
  Option,
  Queue,
  Result,
  Scope,
  Stream,
} from "effect";
import { TestClock } from "effect/testing";
import * as H3 from "../../src/h3/index.js";
import { fromH3 } from "../../src/orchestration/h3-source.js";
import { fixture as providerFixture } from "../h3/ProviderSession.js";
import { ReactorError } from "../../src/errors.js";
import { ClipRequest } from "../../src/orchestration/request.js";
import * as Renewal from "../../src/orchestration/renewal.js";
import { fillerKey, schedulerKeyOf } from "../../src/orchestration/scheduler-key.js";
import { ItemKey, makeScheduler } from "../../src/orchestration/scheduler.js";
import type {
  AsRunEvent,
  SchedulerOptions,
  SchedulerShape,
} from "../../src/orchestration/scheduler.js";
import { Engine } from "../../src/orchestration/types.js";
import type { EngineEvent, EngineShape, EngineState } from "../../src/orchestration/types.js";
import * as Simulation from "../../src/simulation/index.js";
import {
  cleanPressure,
  failure,
  gate,
  readyState,
  record,
  refusal,
  run,
  runClock,
  sourceFixture,
  until,
} from "./SourceFixture.js";
import type { SourceFixture } from "./SourceFixture.js";
import { steppableWall } from "./WallClock.js";

const clip = (prompt: string) =>
  new ClipRequest({ prompt, references: [], durationSeconds: 5, metadata: {} });
const options: SchedulerOptions = {
  lanes: [{ name: "line" }],
  filler: { runway: { floor: 0, target: "5 seconds" }, clip: () => clip("filler") },
};
const advance = (millis: number) =>
  Effect.gen(function* () {
    for (let elapsed = 0; elapsed < millis; elapsed += 100)
      yield* TestClock.adjust(Math.min(100, millis - elapsed));
  });

for (const unknownKind of ["item", "filler"] as const) {
  test(`canonical Renewal replaces unknown ${unknownKind} and starts a distinct later line`, () =>
    runClock(
      Effect.gen(function* () {
        const sources: SourceFixture[] = [];
        const replacement = yield* gate;
        const handle = yield* Renewal.make({
          open: Effect.gen(function* () {
            const index = sources.length;
            const source = yield* sourceFixture(`recovery-${index}`, {
              execute: (plan, accept) =>
                index === 0 && plan.request.prompt === unknownKind
                  ? Effect.fail(failure("unknown"))
                  : accept,
            });
            sources.push(source);
            if (index === 1) yield* replacement.release;
            return { source: source.source, lifetime: "10 minutes" };
          }),
        });
        const scheduler = yield* makeScheduler(
          unknownKind === "item"
            ? options
            : {
                ...options,
                filler: {
                  runway: { floor: "5 seconds", target: "5 seconds" },
                  clip: () => clip("filler"),
                },
              },
        ).pipe(Effect.provideService(Engine, handle.engine));
        const first =
          unknownKind === "item"
            ? yield* scheduler.submit({
                key: ItemKey.make("L0"),
                lane: "line",
                request: clip("item"),
              })
            : undefined;
        yield* sources[0]!.lifecycle.wait((event) => event._tag === "Accounted");
        yield* replacement.wait;
        yield* sources[0]!.lifecycle.wait((event) => event._tag === "Closed");
        yield* advance(2_000);
        if (unknownKind === "filler") {
          yield* sources[1]!.setState(readyState({ ready: [...sources[1]!.accepted] }));
          for (const accepted of sources[1]!.accepted)
            yield* sources[1]!.emit({
              _tag: "Ready",
              clipId: accepted.clipId,
              durationSeconds: accepted.durationSeconds,
              timing: { _tag: "Unknown" },
            });
        }
        const later = yield* scheduler.submit({
          key: ItemKey.make("L1"),
          lane: "line",
          request: clip("later"),
        });
        yield* until(() => sources[1]!.sends.some((plan) => plan.request.prompt === "later"));
        const accepted = sources[1]!.accepted.find((entry) => entry.request?.prompt === "later")!;
        yield* sources[1]!.emit({
          _tag: "Started",
          clipId: accepted.clipId,
          durationSeconds: 5,
          at: yield* Clock.currentTimeMillis,
        });
        expect(yield* later.started).toMatchObject({ _tag: "Started", sessionId: "recovery-1" });
        expect(sources).toHaveLength(2);
        expect(sources[0]!.status().reconnects).toBe(0);
        expect(sources[0]!.status().closes).toBe(1);
        const originalKey = unknownKind === "item" ? "L0" : fillerKey(0);
        expect(
          sources
            .flatMap((source) => source.sends)
            .filter((plan) => schedulerKeyOf(plan.request) === originalKey),
        ).toHaveLength(1);
        if (first !== undefined)
          expect(yield* first.outcome).toEqual({ _tag: "Unknown", terminal: true });
        expect((yield* scheduler.state).accepting).toBe(true);
      }),
    ));
}

const scripted = (
  settings: {
    readonly scheduler?: Partial<SchedulerOptions>;
    readonly fenced?: boolean;
    readonly stop?: Effect.Effect<void>;
    readonly send?: Effect.Effect<void>;
  } = {},
) =>
  Effect.gen(function* () {
    const simulation = yield* Simulation.make({ fixedBuildTime: 0, buildRatio: 0 });
    let state = yield* simulation.engine.state;
    const feeds: Queue.Queue<EngineEvent, ReactorError>[] = [];
    const calls: ClipRequest[] = [];
    const uncertain = failure("unknown");
    const enqueue = (request: ClipRequest) =>
      Effect.gen(function* () {
        calls.push(request);
        yield* settings.send ?? Effect.void;
        return yield* uncertain;
      });
    const engine: EngineShape = {
      ...simulation.engine,
      state: Effect.sync(() => state),
      enqueue,
      enqueueOnSource: settings.fenced === true ? (request) => enqueue(request) : undefined,
      stopRenewal: settings.stop ?? Effect.void,
      observe: () =>
        Effect.gen(function* () {
          const feed = yield* Queue.unbounded<EngineEvent, ReactorError>();
          feeds.push(feed);
          return { initial: state, events: Stream.fromQueue(feed) };
        }),
    };
    const scheduler = yield* makeScheduler({ ...options, ...settings.scheduler }).pipe(
      Effect.provideService(Engine, engine),
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
    return {
      scheduler,
      engine,
      calls,
      uncertain,
      events,
      feeds,
      setState: (next: EngineState) =>
        Effect.sync(() => {
          state = next;
        }),
      refresh: Effect.gen(function* () {
        const count = feeds.length;
        yield* Queue.fail(
          feeds.at(-1)!,
          ReactorError.fromCode("Overflow", "Controlled re-observation"),
        );
        yield* until(() => feeds.length > count);
        yield* TestClock.adjust(0);
      }),
      unknown: (key: ItemKey) =>
        until(() => events.some((event) => event.key === key && event.status._tag === "Unknown")),
    };
  });
const submit = (scheduler: SchedulerShape, key: string) =>
  scheduler.submit({ key: ItemKey.make(key), lane: "line", request: clip(key) });

for (const cap of [1, 3]) {
  test(`default unknown watchdog arms at observation with cap ${cap} and no later demand`, () =>
    runClock(
      Effect.gen(function* () {
        const { scheduler, unknown, calls, uncertain } = yield* scripted({
          scheduler: { maxBuildsInFlight: cap },
        });
        const item = yield* submit(scheduler, "L0");
        yield* unknown(item.key);
        const stopped = yield* Effect.forkScoped(scheduler.failure);
        yield* TestClock.adjust(59_999);
        expect(stopped.pollUnsafe()).toBeUndefined();
        yield* TestClock.adjust(1);
        const exit = stopped.pollUnsafe();
        expect(exit).toBeDefined();
        if (exit !== undefined && Exit.isSuccess(exit)) {
          expect(exit.value).toMatchObject({
            reason: { _tag: "Timeout" },
            context: { operation: "scheduler.unknownRecovery" },
          });
          expect(exit.value.context.detail).toContain(uncertain);
        }
        expect(yield* item.outcome).toEqual({ _tag: "Unknown", terminal: true });
        expect(calls).toHaveLength(1);
        expect((yield* scheduler.state).accepting).toBe(false);
      }),
    ));
}

test("unknown timeout settles later accepted work without replay or invented rejection", () =>
  runClock(
    Effect.gen(function* () {
      const { scheduler, unknown, calls } = yield* scripted();
      const first = yield* submit(scheduler, "L0");
      yield* unknown(first.key);
      yield* TestClock.adjust(2_000);
      const later = yield* submit(scheduler, "L1");
      const stopped = yield* Effect.forkScoped(scheduler.failure);
      yield* TestClock.adjust(58_000);
      expect(stopped.pollUnsafe()).toBeDefined();
      expect(yield* first.outcome).toEqual({ _tag: "Unknown", terminal: true });
      expect(yield* later.outcome).toMatchObject({
        _tag: "Failed",
        reason: { _tag: "Scheduler", cause: { reason: { _tag: "Timeout" } } },
      });
      expect(calls.map((request) => request.prompt)).toEqual(["L0"]);
    }),
  ));

for (const timeout of [0, -1, Infinity, NaN, "Infinity", "11 minutes"] as const) {
  test(`unknown recovery rejects invalid duration ${timeout}`, () =>
    runClock(
      Effect.gen(function* () {
        const simulation = yield* Simulation.make();
        const result = yield* Effect.result(
          makeScheduler({ ...options, unknownRecoveryTimeout: timeout }).pipe(
            Effect.provideService(Engine, simulation.engine),
          ),
        );
        expect(Result.isFailure(result)).toBe(true);
        if (Result.isFailure(result)) expect(result.failure.reason._tag).toBe("InvalidInput");
      }),
    ));
}

test("unknown recovery captures a finite ten-minute maximum without evaluating accessors", () =>
  runClock(
    Effect.gen(function* () {
      const simulation = yield* Simulation.make();
      yield* makeScheduler({ ...options, unknownRecoveryTimeout: "10 minutes" }).pipe(
        Effect.provideService(Engine, simulation.engine),
      );
      let reads = 0;
      const config = {
        ...options,
        get unknownRecoveryTimeout() {
          reads++;
          return 500;
        },
      };
      const result = yield* Effect.result(
        makeScheduler(config).pipe(Effect.provideService(Engine, simulation.engine)),
      );
      expect(Result.isFailure(result)).toBe(true);
      expect(reads).toBe(0);
    }),
  ));

for (const finish of ["playing", "accepted"] as const) {
  for (const outcomeDuringDrain of [false, true]) {
    test(`${finish} drain settles at the unknown deadline while stopRenewal waits (${outcomeDuringDrain ? "late result" : "known result"})`, () =>
      runClock(
        Effect.gen(function* () {
          const stoppedRenewal = yield* gate;
          const releaseStop = yield* gate;
          const releaseSend = yield* gate;
          const fixture = yield* scripted({
            scheduler: { unknownRecoveryTimeout: "1 second" },
            stop: stoppedRenewal.release.pipe(Effect.andThen(releaseStop.wait)),
            ...(outcomeDuringDrain ? { send: releaseSend.wait } : {}),
          });
          const item = yield* submit(fixture.scheduler, "uncertain");
          if (!outcomeDuringDrain) yield* fixture.unknown(item.key);
          const draining = yield* Effect.forkScoped(
            Effect.result(fixture.scheduler.drain({ finish })),
          );
          yield* stoppedRenewal.wait;
          if (outcomeDuringDrain) {
            yield* releaseSend.release;
            yield* TestClock.adjust(0);
          }
          const queued = yield* Effect.forkScoped(
            Effect.result(submit(fixture.scheduler, "queued-after-drain").pipe(Effect.asVoid)),
          );
          const failure = yield* Effect.forkScoped(fixture.scheduler.failure);
          yield* TestClock.adjust(999);
          expect(failure.pollUnsafe()).toBeUndefined();
          yield* TestClock.adjust(1);
          expect(failure.pollUnsafe()).toBeDefined();
          expect(yield* item.outcome).toEqual({ _tag: "Unknown", terminal: true });
          for (const result of [yield* Fiber.join(draining), yield* Fiber.join(queued)]) {
            expect(Result.isFailure(result)).toBe(true);
            if (Result.isFailure(result)) expect(refusal(result.failure)).toBe("SessionClosed");
          }
          expect(fixture.calls).toHaveLength(1);
          yield* releaseStop.release;
          yield* TestClock.adjust(100);
          expect(fixture.calls).toHaveLength(1);
        }),
      ));
  }
}

test("snapshot churn, duplicate submit and a second unknown cannot extend the first deadline", () =>
  runClock(
    Effect.gen(function* () {
      const fixture = yield* scripted({
        scheduler: { maxBuildsInFlight: 3, unknownRecoveryTimeout: "1 second" },
      });
      const first = yield* submit(fixture.scheduler, "oldest");
      yield* fixture.unknown(first.key);
      const stopped = yield* Effect.forkScoped(fixture.scheduler.failure);
      yield* TestClock.adjust(400);
      expect(yield* submit(fixture.scheduler, "oldest")).toBe(first);
      yield* fixture.refresh;
      const second = yield* submit(fixture.scheduler, "second");
      yield* fixture.unknown(second.key);
      yield* TestClock.adjust(599);
      yield* fixture.refresh;
      expect(stopped.pollUnsafe()).toBeUndefined();
      yield* TestClock.adjust(1);
      expect(stopped.pollUnsafe()).toBeDefined();
      expect(fixture.calls.map((request) => request.prompt)).toEqual(["oldest", "second"]);
      expect(yield* second.outcome).toEqual({ _tag: "Unknown", terminal: true });
    }),
  ));

const keyedRecord = (key: string, sessionId: string) => {
  const pending = record(`proof-${key}`, 5);
  return {
    ...pending,
    sessionId,
    provider: {
      ...pending.provider,
      metadata: JSON.stringify({
        reactor_effect_h3: 1,
        namespace: sessionId,
        submission: key,
        caller: JSON.stringify({ reactor_effect_scheduler: 1, key, application: {} }),
      }),
    },
  };
};

for (const proof of ["Building", "Ready", "Started"] as const) {
  test(`keyed ${proof} proof cancels uncertainty without replay`, () =>
    runClock(
      Effect.gen(function* () {
        const fixture = yield* scripted({ scheduler: { unknownRecoveryTimeout: "1 second" } });
        const item = yield* submit(fixture.scheduler, "proof");
        yield* fixture.unknown(item.key);
        const baseline = yield* fixture.engine.state;
        const known = keyedRecord(item.key, baseline.sessions[0]!.sessionId);
        yield* TestClock.adjust(900);
        yield* fixture.setState({
          ...baseline,
          ...(proof === "Building" ? { queued: [known], generationOrder: [known.clipId] } : {}),
          ...(proof === "Ready" ? { ready: [known] } : {}),
          ...(proof === "Started"
            ? {
                playing: Option.some({
                  clipId: known.clipId,
                  record: Option.some(known),
                  startedAt: Option.some(900),
                  startedAtMonotonicMillis: 900,
                }),
              }
            : {}),
        });
        yield* fixture.refresh;
        const stopped = yield* Effect.forkScoped(fixture.scheduler.failure);
        yield* TestClock.adjust(1_100);
        expect(stopped.pollUnsafe()).toBeUndefined();
        expect((yield* fixture.scheduler.state).accepting).toBe(true);
        expect(fixture.events.filter((event) => event.key === item.key).at(-1)?.status._tag).toBe(
          proof,
        );
        expect(fixture.calls).toHaveLength(1);
      }),
    ));
}

for (const nextUnknown of [false, true]) {
  test(`Ready replacement clears the old capacity episode and ${nextUnknown ? "later uncertainty gets its own deadline" : "later drain reuses old uncertainty age"}`, () =>
    runClock(
      Effect.gen(function* () {
        const fixture = yield* scripted({
          fenced: true,
          scheduler: { unknownRecoveryTimeout: "1 second" },
        });
        const first = yield* submit(fixture.scheduler, "source-A");
        yield* fixture.unknown(first.key);
        const state = yield* fixture.engine.state;
        yield* TestClock.adjust(700);
        yield* fixture.setState({
          ...state,
          sessions: [...state.sessions, { sessionId: "B", availability: "Ready" }],
          preferredSessionId: Option.some("B"),
        });
        yield* fixture.refresh;
        const stopped = yield* Effect.forkScoped(fixture.scheduler.failure);
        if (nextUnknown) {
          const second = yield* submit(fixture.scheduler, "source-B");
          yield* fixture.unknown(second.key);
          yield* TestClock.adjust(999);
          expect(stopped.pollUnsafe()).toBeUndefined();
          yield* TestClock.adjust(1);
        } else {
          yield* TestClock.adjust(1_300);
          expect(stopped.pollUnsafe()).toBeUndefined();
          yield* Effect.forkScoped(Effect.result(fixture.scheduler.drain({ finish: "accepted" })));
          yield* TestClock.adjust(0);
          yield* until(() => stopped.pollUnsafe() !== undefined);
        }
        expect(stopped.pollUnsafe()).toBeDefined();
        expect(yield* first.outcome).toEqual({ _tag: "Unknown", terminal: true });
      }),
    ));
}

for (const finish of ["playing", "accepted"] as const) {
  test(`canonical blocked source close preserves its cleanup owner while ${finish} drain times out`, () =>
    runClock(
      Effect.gen(function* () {
        const closeEntered = yield* gate;
        const releaseClose = yield* gate;
        const sources: SourceFixture[] = [];
        const handle = yield* Renewal.make({
          open: Effect.gen(function* () {
            const source = yield* sourceFixture(`blocked-${sources.length}`, {
              execute: () => Effect.fail(failure("unknown")),
              close: closeEntered.release.pipe(Effect.andThen(releaseClose.wait)),
            });
            sources.push(source);
            return { source: source.source, lifetime: "10 minutes" };
          }),
        });
        const scheduler = yield* makeScheduler({
          ...options,
          unknownRecoveryTimeout: "1 second",
        }).pipe(Effect.provideService(Engine, handle.engine));
        yield* Effect.addFinalizer(() => releaseClose.release);
        const item = yield* submit(scheduler, "blocked-close");
        yield* closeEntered.wait;
        const stopped = yield* Effect.forkScoped(scheduler.failure);
        const draining = yield* Effect.forkScoped(Effect.result(scheduler.drain({ finish })));
        yield* TestClock.adjust(100);
        expect((yield* handle.engine.state).sessions).toHaveLength(0);
        yield* TestClock.adjust(900);
        expect(stopped.pollUnsafe()).toBeDefined();
        expect(yield* item.outcome).toEqual({ _tag: "Unknown", terminal: true });
        expect(Result.isFailure(yield* Fiber.join(draining))).toBe(true);
        expect(sources).toHaveLength(1);
        expect(sources[0]!.status().finalized).toBe(false);
        yield* releaseClose.release;
        const report = yield* handle.close;
        expect(report.sessions).toContain(sources[0]!.cleanup);
        expect(sources).toHaveLength(1);
        expect(sources[0]!.status().finalized).toBe(true);
        expect(sources[0]!.status().reconnects).toBe(0);
      }),
    ));
}

test("a recovery-fiber defect stays a defect while the scheduler independently bounds service", () =>
  runClock(
    Effect.gen(function* () {
      const defect = Cause.die(new Error("controlled recovery close defect"));
      const closeExit = yield* Deferred.make<Exit.Exit<void>>();
      const owner = yield* Scope.make();
      // The close owner retains its original defect. Own that scope here
      // so the test can assert the final Exit as well as the source hook.
      yield* Effect.addFinalizer(() =>
        Scope.close(owner, Exit.void).pipe(Effect.exit, Effect.asVoid),
      );
      const handle = yield* Renewal.make({
        open: sourceFixture("defective", {
          execute: () => Effect.fail(failure("unknown")),
          close: Effect.failCause(defect).pipe(
            Effect.onExit((exit) => Deferred.succeed(closeExit, exit)),
          ),
        }).pipe(Effect.map((fixture) => ({ source: fixture.source, lifetime: "10 minutes" }))),
      }).pipe(Effect.provideService(Scope.Scope, owner));
      const scheduler = yield* makeScheduler({
        ...options,
        unknownRecoveryTimeout: "1 second",
      }).pipe(Effect.provideService(Engine, handle.engine));
      const item = yield* submit(scheduler, "recovery-defect");
      const closeResult = yield* Deferred.await(closeExit);
      expect(Exit.isFailure(closeResult)).toBe(true);
      if (Exit.isFailure(closeResult))
        expect(Cause.squash(closeResult.cause)).toBe(Cause.squash(defect));
      const engineFailure = yield* Effect.forkScoped(Effect.exit(handle.engine.failure));
      const stopped = yield* Effect.forkScoped(scheduler.failure);
      yield* TestClock.adjust(999);
      // Existing Renewal recovery has no all-cause supervisor. Its original
      // defect is observed above; the later service Timeout is a separate event.
      expect(engineFailure.pollUnsafe()).toBeUndefined();
      expect(stopped.pollUnsafe()).toBeUndefined();
      yield* TestClock.adjust(1);
      expect(stopped.pollUnsafe()).toBeDefined();
      expect(yield* item.outcome).toEqual({ _tag: "Unknown", terminal: true });
      expect((yield* Fiber.join(stopped)).reason._tag).toBe("Timeout");
      for (const exit of [
        yield* Effect.exit(handle.close.pipe(Effect.asVoid)),
        yield* Effect.exit(Scope.close(owner, Exit.void)),
      ]) {
        expect(Exit.isFailure(exit)).toBe(true);
        if (Exit.isFailure(exit)) expect(Cause.squash(exit.cause)).toBe(Cause.squash(defect));
      }
    }),
  ));

test("wall clock corrections cannot extend or shorten the unknown deadline", () =>
  runClock(
    Effect.gen(function* () {
      const wall = yield* steppableWall;
      const fixture = yield* scripted({ scheduler: { unknownRecoveryTimeout: "1 second" } }).pipe(
        Effect.provideService(Clock.Clock, wall.clock),
      );
      const item = yield* submit(fixture.scheduler, "wall");
      yield* fixture.unknown(item.key);
      const stopped = yield* Effect.forkScoped(fixture.scheduler.failure);
      yield* TestClock.adjust(400);
      yield* wall.step(3_600_000);
      yield* fixture.refresh;
      yield* TestClock.adjust(599);
      yield* wall.step(-7_200_000);
      yield* fixture.refresh;
      expect(stopped.pollUnsafe()).toBeUndefined();
      yield* TestClock.adjust(1);
      expect(stopped.pollUnsafe()).toBeDefined();
    }),
  ));

test("keyed proof after timeout cannot revive admission or replace terminal Unknown", () =>
  runClock(
    Effect.gen(function* () {
      const fixture = yield* scripted({ scheduler: { unknownRecoveryTimeout: 100 } });
      const item = yield* submit(fixture.scheduler, "late");
      yield* fixture.unknown(item.key);
      yield* TestClock.adjust(100);
      const cause = yield* fixture.scheduler.failure;
      const state = yield* fixture.engine.state;
      yield* fixture.setState({
        ...state,
        ready: [keyedRecord(item.key, state.sessions[0]!.sessionId)],
      });
      yield* fixture.refresh;
      expect(yield* item.outcome).toEqual({ _tag: "Unknown", terminal: true });
      expect(yield* fixture.scheduler.failure).toBe(cause);
      expect((yield* fixture.scheduler.state).accepting).toBe(false);
      expect(refusal(yield* Effect.flip(submit(fixture.scheduler, "later")))).toBe("SessionClosed");
      expect(fixture.calls).toHaveLength(1);
    }),
  ));

test("overlapping uncertain filler identities reconcile independently and both hold drain", () =>
  runClock(
    Effect.gen(function* () {
      const fixture = yield* scripted({
        fenced: true,
        scheduler: {
          unknownRecoveryTimeout: "10 seconds",
          filler: {
            runway: { floor: "5 seconds", target: "5 seconds" },
            clip: () => clip("filler"),
          },
        },
      });
      yield* TestClock.adjust(100);
      const state = yield* fixture.engine.state;
      const sourceA = state.sessions[0]!.sessionId;
      const sourceB: EngineState = {
        ...state,
        sessions: [...state.sessions, { sessionId: "B", availability: "Ready" }],
        preferredSessionId: Option.some("B"),
      };
      yield* fixture.setState(sourceB);
      yield* fixture.refresh;
      yield* TestClock.adjust(100);
      expect(fixture.calls.map(schedulerKeyOf)).toEqual([fillerKey(0), fillerKey(1)]);
      const draining = yield* Effect.forkScoped(
        Effect.result(fixture.scheduler.drain({ finish: "accepted" })),
      );
      yield* TestClock.adjust(100);
      expect(draining.pollUnsafe()).toBeUndefined();
      // Prove B by its own key, then retire it. A's earlier uncertainty remains.
      yield* fixture.setState({ ...sourceB, ready: [keyedRecord(fillerKey(1), "B")] });
      yield* fixture.refresh;
      yield* fixture.setState({
        ...sourceB,
        sessions: state.sessions,
        preferredSessionId: Option.some(sourceA),
      });
      yield* fixture.refresh;
      yield* TestClock.adjust(100);
      expect(draining.pollUnsafe()).toBeUndefined();
      // Retiring A truthfully terminalizes its unprovable filler fate.
      yield* fixture.setState({
        ...state,
        sessions: [],
        preferredSessionId: Option.none(),
        availability: "Unavailable",
      });
      yield* fixture.refresh;
      yield* TestClock.adjust(100);
      expect(draining.pollUnsafe()).toBeDefined();
      const stopped = yield* Effect.forkScoped(fixture.scheduler.failure);
      yield* TestClock.adjust(10_000);
      expect(stopped.pollUnsafe()).toBeUndefined();
      expect(fixture.calls.map(schedulerKeyOf)).toEqual([fillerKey(0), fillerKey(1)]);
    }),
  ));

test("uncertain filler ledger fails before dispatching a 4097th identity", () =>
  // This is an operation bound: no deadline is advanced or awaited. The live
  // clock avoids sorting thousands of canceled virtual watchdog sleeps.
  run(
    Effect.gen(function* () {
      let state = readyState({
        sessions: [{ sessionId: "ledger-0", availability: "Ready" }],
        preferredSessionId: Option.some("ledger-0"),
      });
      const keys: (string | undefined)[] = [];
      const uncertain = failure("unknown");
      const filler = clip("bounded filler");
      const engine: EngineShape = {
        prepare: () => Effect.die(new Error("Unexpected unfenced preparation")),
        enqueue: () => Effect.die(new Error("Unexpected unfenced enqueue")),
        events: Stream.never,
        failure: Effect.never,
        stopRenewal: Effect.void,
        setAutoplay: () => Effect.void,
        pauseAndStop: Effect.void,
        remove: () => Effect.die(new Error("Uncertain filler cannot be removed without a clip ID")),
        move: () => Effect.void,
        setCanvas: () => Effect.void,
        state: Effect.sync(() => state),
        observe: () => Effect.succeed({ initial: state, events: Stream.never }),
        enqueueOnSource: (request) =>
          Effect.gen(function* () {
            keys.push(schedulerKeyOf(request));
            const sessionId = `ledger-${keys.length}`;
            state = {
              ...state,
              // Keep every uncertain source live; their enumeration order is
              // unrelated to the ledger bound, so preference is cheap to find.
              sessions: [{ sessionId, availability: "Ready" }, ...state.sessions],
              preferredSessionId: Option.some(sessionId),
            };
            return yield* uncertain;
          }),
      };
      const scheduler = yield* makeScheduler({
        ...options,
        unknownRecoveryTimeout: "10 minutes",
        filler: {
          runway: { floor: "5 seconds", target: "5 seconds" },
          clip: () => filler,
        },
      }).pipe(Effect.provideService(Engine, engine));
      const result = yield* scheduler.failure;
      expect(result.reason._tag).toBe("Overflow");
      expect(keys).toHaveLength(4096);
      expect(new Set(keys).size).toBe(4096);
      expect(keys.at(-1)).toBe(fillerKey(4095));
      expect(state.sessions).toHaveLength(4097);
      expect((yield* scheduler.state).accepting).toBe(false);
    }),
  ));

for (const kind of ["item", "filler"] as const) {
  test(`real H3 result hook forwards unknown ${kind} through Renewal to a usable replacement`, () =>
    runClock(
      Effect.gen(function* () {
        const fake = yield* providerFixture({
          command: { enqueue: ({ fail }) => Effect.fail(fail("unknown")) },
        });
        const provider = yield* H3.make(fake.session, {
          replyTimeout: 100,
          setupTimeout: 1_000,
          reconcileWindow: 20,
        });
        const source = yield* fromH3(fake.session, provider, {
          media: Effect.succeed({
            generation: 1n,
            tracks: [],
            retired: Effect.never,
            video: () => Stream.never,
            audio: () => Stream.never,
            snapshot: Effect.succeed(cleanPressure),
          }),
        });
        let opened = false;
        let replacement: SourceFixture | undefined;
        const handle = yield* Renewal.make({
          open: Effect.gen(function* () {
            if (opened) {
              replacement = yield* sourceFixture("h3-replacement");
              return { source: replacement.source, lifetime: "10 minutes" };
            }
            opened = true;
            return { source, lifetime: "10 minutes" };
          }),
        });
        const scheduler = yield* makeScheduler(
          kind === "item"
            ? options
            : {
                ...options,
                filler: {
                  runway: { floor: "5 seconds", target: "5 seconds" },
                  clip: () => clip("filler"),
                },
              },
        ).pipe(Effect.provideService(Engine, handle.engine));
        const first = kind === "item" ? yield* submit(scheduler, "h3-unknown") : undefined;
        yield* until(() => replacement !== undefined, TestClock.adjust(1));
        yield* advance(2_000);
        const next = replacement!;
        if (kind === "filler") {
          yield* next.setState(readyState({ ready: [...next.accepted] }));
          for (const accepted of next.accepted)
            yield* next.emit({
              _tag: "Ready",
              clipId: accepted.clipId,
              durationSeconds: accepted.durationSeconds,
              timing: { _tag: "Unknown" },
            });
        }
        const later = yield* submit(scheduler, "after-h3-unknown");
        yield* until(() => next.sends.some((plan) => plan.request.prompt === "after-h3-unknown"));
        const accepted = next.accepted.find(
          (entry) => entry.request?.prompt === "after-h3-unknown",
        )!;
        yield* next.emit({
          _tag: "Started",
          clipId: accepted.clipId,
          durationSeconds: 5,
          at: yield* Clock.currentTimeMillis,
        });
        expect(yield* later.started).toMatchObject({ _tag: "Started", sessionId: next.source.id });
        expect(fake.calls.filter((call) => call.command === "enqueue")).toHaveLength(1);
        expect(fake.lifecycleCalls.close).toBe(1);
        expect(
          next.sends.filter(
            (plan) =>
              schedulerKeyOf(plan.request) === (kind === "item" ? "h3-unknown" : fillerKey(0)),
          ),
        ).toHaveLength(0);
        if (first !== undefined)
          expect(yield* first.outcome).toEqual({ _tag: "Unknown", terminal: true });
      }),
    ));
}

for (const finish of ["playing", "accepted"] as const) {
  test(`${finish} drain uses an already acquired replacement without reopening allocation`, () =>
    runClock(
      Effect.gen(function* () {
        const prepared = yield* gate;
        const releaseUnknown = yield* gate;
        const closing = yield* gate;
        const releaseClose = yield* gate;
        const sources: SourceFixture[] = [];
        const handle = yield* Renewal.make({
          lead: "59 seconds",
          onRenewal: (event) => (event._tag === "Prepared" ? prepared.release : Effect.void),
          open: Effect.gen(function* () {
            const index = sources.length;
            const source = yield* sourceFixture(
              `prewarmed-${index}`,
              index === 0
                ? {
                    execute: () =>
                      releaseUnknown.wait.pipe(Effect.andThen(Effect.fail(failure("unknown")))),
                    close: closing.release.pipe(Effect.andThen(releaseClose.wait)),
                  }
                : {},
            );
            sources.push(source);
            return { source: source.source, lifetime: "60 seconds" };
          }),
        });
        const scheduler = yield* makeScheduler({
          ...options,
          unknownRecoveryTimeout: "1 second",
        }).pipe(Effect.provideService(Engine, handle.engine));
        yield* Effect.addFinalizer(() => releaseClose.release);
        const unknown = yield* submit(scheduler, "prewarmed-unknown");
        const preparing = yield* Effect.forkScoped(prepared.wait);
        yield* until(() => preparing.pollUnsafe() !== undefined, TestClock.adjust(100));
        expect(sources).toHaveLength(2);
        yield* releaseUnknown.release;
        yield* closing.wait;
        const later = yield* submit(scheduler, "prewarmed-later");
        const draining = yield* Effect.forkScoped(scheduler.drain({ finish }));
        yield* TestClock.adjust(100);
        yield* releaseClose.release;
        yield* TestClock.adjust(100);
        if (finish === "accepted") {
          yield* until(() => sources[1]!.accepted.length === 1);
          const accepted = sources[1]!.accepted[0]!;
          yield* sources[1]!.emit({
            _tag: "Started",
            clipId: accepted.clipId,
            durationSeconds: 5,
            at: yield* Clock.currentTimeMillis,
          });
          expect((yield* later.started)._tag).toBe("Started");
          yield* sources[1]!.setState(readyState());
          yield* sources[1]!.emit({
            _tag: "Ended",
            clipId: accepted.clipId,
            termination: "finished",
            at: yield* Clock.currentTimeMillis,
          });
        } else expect(yield* later.outcome).toEqual({ _tag: "Dropped", reason: "withdrawn" });
        yield* Fiber.join(draining);
        expect(yield* unknown.outcome).toEqual({ _tag: "Unknown", terminal: true });
        yield* handle.close;
        expect(sources).toHaveLength(2);
      }),
    ));
}

test("an unrelated later outage does not revive an old source's resolved capacity episode", () =>
  runClock(
    Effect.gen(function* () {
      const fixture = yield* scripted({ fenced: true, scheduler: { unknownRecoveryTimeout: 100 } });
      const item = yield* submit(fixture.scheduler, "old-source");
      yield* fixture.unknown(item.key);
      const initial = yield* fixture.engine.state;
      yield* fixture.setState({
        ...initial,
        sessions: [...initial.sessions, { sessionId: "B", availability: "Ready" }],
        preferredSessionId: Option.some("B"),
      });
      yield* fixture.refresh;
      yield* fixture.setState({
        ...initial,
        availability: "Unavailable",
        preferredSessionId: Option.none(),
      });
      yield* fixture.refresh;
      const stopped = yield* Effect.forkScoped(fixture.scheduler.failure);
      yield* TestClock.adjust(200);
      expect(stopped.pollUnsafe()).toBeUndefined();
      const draining = yield* Effect.forkScoped(
        Effect.result(fixture.scheduler.drain({ finish: "accepted" })),
      );
      yield* TestClock.adjust(0);
      yield* until(() => stopped.pollUnsafe() !== undefined);
      expect(stopped.pollUnsafe()).toBeDefined();
      expect(Result.isFailure(yield* Fiber.join(draining))).toBe(true);
    }),
  ));
