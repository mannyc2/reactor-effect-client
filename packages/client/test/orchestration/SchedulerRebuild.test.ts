import { expect, test } from "vitest";
import { Effect, Stream } from "effect";
import { TestClock } from "effect/testing";
import { ClipRequest } from "../../src/orchestration/request.js";
import * as Renewal from "../../src/orchestration/renewal.js";
import { ItemKey, makeScheduler } from "../../src/orchestration/scheduler.js";
import type { AsRunStatus } from "../../src/orchestration/scheduler.js";
import { Engine } from "../../src/orchestration/types.js";
import * as Simulation from "../../src/simulation/index.js";
import { runClock } from "./SourceFixture.js";

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
