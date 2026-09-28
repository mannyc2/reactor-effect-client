import { expect, test } from "vitest";
import { Effect, Stream } from "effect";
import { TestClock } from "effect/testing";
import { ClipRequest } from "../../src/orchestration/request.js";
import * as Renewal from "../../src/orchestration/renewal.js";
import { ItemKey, makeScheduler } from "../../src/orchestration/scheduler.js";
import type { AsRunStatus } from "../../src/orchestration/scheduler.js";
import { Engine } from "../../src/orchestration/types.js";
import type { EngineShape } from "../../src/orchestration/types.js";
import { schedulerKeyOf } from "../../src/orchestration/scheduler-key.js";
import * as Simulation from "../../src/simulation/index.js";
import { gate, runClock } from "./SourceFixture.js";

const clip = (prompt: string, durationSeconds = 5) =>
  new ClipRequest({ prompt, references: [], durationSeconds, metadata: {} });

const advance = (millis: number) =>
  Effect.gen(function* () {
    for (let elapsed = 0; elapsed < millis; elapsed += 20)
      yield* TestClock.adjust(Math.min(20, millis - elapsed));
  });

// Research report A, recommendation 9: a replacement session is rebuilt from the plan.
test("a clip lost with its session before it aired is rebuilt on the replacement under its key", () =>
  runClock(
    Effect.gen(function* () {
      let opened = 0;
      const handle = yield* Renewal.make({
        open: Effect.gen(function* () {
          const first = opened++ === 0;
          return {
            // The first session is lost when its third clip is sent.
            source: yield* Simulation.source({
              fixedBuildTime: "300 millis",
              buildRatio: 0,
              ...(first ? { faults: { sessionFails: (sequence: number) => sequence === 3 } } : {}),
            }),
            lifetime: "Infinity",
          };
        }),
      });
      const scheduler = yield* makeScheduler({
        lanes: [{ name: "line" }],
        filler: {
          runway: { floor: "0 seconds", target: "1 second" },
          clip: ({ index }) => clip(`filler ${index}`),
        },
      }).pipe(Effect.provideService(Engine, handle.engine));
      const statuses: AsRunStatus[] = [];
      yield* scheduler.asRun.pipe(
        Stream.runForEach((event) =>
          Effect.sync(() => {
            if (event.key === "b") statuses.push(event.status);
          }),
        ),
        Effect.forkScoped,
      );
      yield* advance(1_000);
      yield* scheduler.submit({ key: ItemKey.make("a"), lane: "line", request: clip("a", 10) });
      const b = yield* scheduler.submit({
        key: ItemKey.make("b"),
        lane: "line",
        request: clip("b"),
      });
      yield* advance(2_000);
      // a plays and b is Ready when c's enqueue loses the session.
      yield* scheduler.submit({ key: ItemKey.make("c"), lane: "line", request: clip("c") });
      yield* advance(20_000);
      expect(opened).toBe(2);
      const started = yield* b.started;
      expect(started._tag).toBe("Started");
      const lostOn = statuses.find((status) => status._tag === "Ready");
      expect(lostOn?._tag === "Ready" && started._tag === "Started" && started.sessionId).not.toBe(
        lostOn?._tag === "Ready" && lostOn.sessionId,
      );
      expect(statuses.map((status) => status._tag)).toEqual([
        "Accepted",
        "Building",
        "Ready",
        "Accepted",
        "Building",
        "Ready",
        "Started",
        "Ended",
      ]);
    }),
  ));

// 0735252: a clip lost with its session while its enqueue reply was still on its way was
// carried back to Accepted, and then recorded Building with no clip, which held the only
// build slot for good.
test("a clip lost before its enqueue reply arrives is still rebuilt, and later work builds", () =>
  runClock(
    Effect.gen(function* () {
      let opened = 0;
      const handle = yield* Renewal.make({
        open: Effect.gen(function* () {
          const first = opened++ === 0;
          return {
            source: yield* Simulation.source({
              fixedBuildTime: "300 millis",
              buildRatio: 0,
              ...(first ? { faults: { sessionFails: (sequence: number) => sequence === 3 } } : {}),
            }),
            lifetime: "Infinity",
          };
        }),
      });
      const hold = yield* gate;
      const engine: EngineShape = {
        ...handle.engine,
        enqueueOnSource: (request, sessionId) =>
          handle.engine.enqueueOnSource!(request, sessionId).pipe(
            Effect.tap(() => (schedulerKeyOf(request) === "b" ? hold.wait : Effect.void)),
          ),
      };
      const scheduler = yield* makeScheduler({
        lanes: [{ name: "line" }],
        filler: {
          runway: { floor: "0 seconds", target: "1 second" },
          clip: ({ index }) => clip(`filler ${index}`),
        },
      }).pipe(Effect.provideService(Engine, engine));
      yield* advance(1_000);
      yield* scheduler.submit({ key: ItemKey.make("a"), lane: "line", request: clip("a", 10) });
      const b = yield* scheduler.submit({
        key: ItemKey.make("b"),
        lane: "line",
        request: clip("b"),
      });
      yield* advance(1_000);
      // b is accepted but its reply is held; a third enqueue then loses the session.
      yield* Effect.ignore(handle.engine.enqueue(clip("third")));
      yield* advance(1_000);
      yield* hold.release;
      yield* advance(1_000);
      const d = yield* scheduler.submit({
        key: ItemKey.make("d"),
        lane: "line",
        request: clip("d"),
      });
      yield* advance(30_000);
      expect((yield* b.started)._tag).toBe("Started");
      expect((yield* d.started)._tag).toBe("Started");
    }),
  ));

// Reactor's content-moderation docs: a session given a flagged prompt is terminated, with no
// reason promised, and a moderation check admits a command still waiting after two seconds, so
// the clip may be accepted first. Rebuilt on every replacement, it would end session after
// session, each billed at least a minute.
test("a clip lost unbuilt with two sessions in a row settles Failed, while built clips are still rebuilt", () =>
  runClock(
    Effect.gen(function* () {
      let opened = 0;
      const handle = yield* Renewal.make({
        open: Effect.gen(function* () {
          opened++;
          // Any session is terminated while it builds the flagged prompt, once it accepted it.
          return {
            source: yield* Simulation.source({
              build: (record) =>
                record.request.prompt === "flagged"
                  ? Effect.die("terminated for content")
                  : Effect.sleep(300).pipe(Effect.as(record.durationSeconds)),
            }),
            lifetime: "Infinity",
          };
        }),
      });
      const scheduler = yield* makeScheduler({
        lanes: [{ name: "line" }],
        filler: {
          runway: { floor: "0 seconds", target: "1 second" },
          clip: ({ index }) => clip(`filler ${index}`),
        },
      }).pipe(Effect.provideService(Engine, handle.engine));
      const statuses = new Map<string, AsRunStatus["_tag"][]>();
      yield* scheduler.asRun.pipe(
        Stream.runForEach((event) =>
          Effect.sync(() => {
            statuses.set(event.key, [...(statuses.get(event.key) ?? []), event.status._tag]);
          }),
        ),
        Effect.forkScoped,
      );
      yield* advance(1_000);
      // long plays while a1 and a2 wait built behind it; f is sent next and ends the session.
      const submit = (key: string, prompt = key, seconds = 5) =>
        scheduler.submit({ key: ItemKey.make(key), lane: "line", request: clip(prompt, seconds) });
      yield* submit("long", "long", 15);
      yield* submit("a1");
      const a2 = yield* submit("a2");
      const f = yield* submit("f", "flagged");
      const b = yield* submit("b");
      yield* advance(60_000);
      expect(opened, JSON.stringify(Object.fromEntries(statuses))).toBe(3);
      expect((yield* f.outcome)._tag).toBe("Failed");
      // a2 was built on both lost sessions, so it passed screening and is rebuilt again.
      expect((yield* a2.started)._tag).toBe("Started");
      expect((yield* b.started)._tag).toBe("Started");
    }),
  ));
