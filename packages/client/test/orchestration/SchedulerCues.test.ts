import { expect, test } from "vitest";
import { Clock, Effect, Stream } from "effect";
import { TestClock } from "effect/testing";
import { makeScheduler, ItemKey } from "../../src/orchestration/scheduler.js";
import type { CueEvent, SchedulerOptions } from "../../src/orchestration/scheduler.js";
import { ClipRequest } from "../../src/orchestration/request.js";
import * as Simulation from "../../src/simulation/index.js";
import { runClock } from "./SourceFixture.js";

const clip = (prompt: string, durationSeconds = 5) =>
  new ClipRequest({ prompt, references: [], durationSeconds, metadata: {} });

const advance = (milliseconds: number) =>
  Effect.gen(function* () {
    for (let elapsed = 0; elapsed < milliseconds; elapsed += 20)
      yield* TestClock.adjust(Math.min(20, milliseconds - elapsed));
  });

// A zero runway floor keeps filler off.
const options = {
  lanes: [{ name: "urgent", cut: true }, { name: "line" }],
  filler: {
    runway: { floor: "0 seconds", target: "1 second" },
    clip: ({ index }: { index: number }) => clip(`filler ${index}`),
  },
} satisfies SchedulerOptions;

const quick = Simulation.layerSim({ fixedBuildTime: 500, buildRatio: 0 });

test("cues fire at their offsets from the start and before the end of the clip that carries them", () =>
  runClock(
    Effect.gen(function* () {
      const scheduler = yield* makeScheduler(options);
      const cues: CueEvent[] = [];
      yield* scheduler.cues.pipe(
        Stream.runForEach((cue) =>
          Effect.sync(() => {
            cues.push(cue);
          }),
        ),
        Effect.forkScoped,
      );
      yield* advance(1_000);
      const line = yield* scheduler.submit({
        key: ItemKey.make("line"),
        lane: "line",
        request: clip("line"),
        cues: [
          { name: "outro", at: { from: "end", offset: "1 second" } },
          { name: "open chart", at: { from: "start", offset: "2 seconds" } },
        ],
      });
      yield* advance(10_000);
      const started = yield* line.started;
      if (started._tag !== "Started") throw new Error("expected the line to start");
      expect(cues.map((cue) => [cue.key, cue.name])).toEqual([
        ["line", "open chart"],
        ["line", "outro"],
      ]);
      const [chart, outro] = cues;
      expect(chart!.at - started.at).toBeGreaterThanOrEqual(2_000);
      expect(chart!.at - started.at).toBeLessThan(2_200);
      const end = started.at + started.durationSeconds * 1000;
      expect(end - outro!.at).toBeLessThanOrEqual(1_000);
      expect(end - outro!.at).toBeGreaterThan(800);
    }).pipe(Effect.provide(quick)),
  ));

test("a cue due after its clip was cut never fires", () =>
  runClock(
    Effect.gen(function* () {
      const scheduler = yield* makeScheduler(options);
      const cues: CueEvent[] = [];
      yield* scheduler.cues.pipe(
        Stream.runForEach((cue) =>
          Effect.sync(() => {
            cues.push(cue);
          }),
        ),
        Effect.forkScoped,
      );
      yield* advance(1_000);
      yield* scheduler.submit({
        key: ItemKey.make("long"),
        lane: "line",
        request: clip("long", 15),
        cues: [{ name: "late overlay", at: { from: "start", offset: "10 seconds" } }],
      });
      yield* advance(3_000);
      const cutAt = yield* Clock.currentTimeMillis;
      yield* scheduler.submit({ key: ItemKey.make("u"), lane: "urgent", request: clip("u") });
      yield* advance(30_000);
      expect(cutAt).toBeGreaterThan(0);
      expect(cues).toEqual([]);
    }).pipe(Effect.provide(quick)),
  ));
