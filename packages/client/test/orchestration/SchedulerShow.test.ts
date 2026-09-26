import { expect, test } from "vitest";
import { Clock, Effect, Stream } from "effect";
import { TestClock } from "effect/testing";
import * as H3 from "../../src/h3/index.js";
import { ItemKey, makeScheduler } from "../../src/orchestration/scheduler.js";
import type {
  AsRunEvent,
  AsRunStatus,
  SchedulerOptions,
} from "../../src/orchestration/scheduler.js";
import { ClipRequest } from "../../src/orchestration/request.js";
import * as Simulation from "../../src/simulation/index.js";
import type { SimOptions } from "../../src/simulation/index.js";
import { runClock } from "./SourceFixture.js";

const RUN_MS = 30 * 60_000;
const LEAD_MS = 10_000;
const FILLER_SECONDS = 5;

const clip = (seconds: number, prompt: string) =>
  new ClipRequest({ prompt, references: [], durationSeconds: seconds, metadata: {} });

const options = (targetSeconds: number): SchedulerOptions => ({
  lanes: [{ name: "show" }],
  filler: {
    runway: {
      floor: `${targetSeconds - FILLER_SECONDS} seconds`,
      target: `${targetSeconds} seconds`,
    },
    clip: ({ index }) => clip(FILLER_SECONDS, `The host waits at the desk (${index}).`),
  },
});

const advance = (millis: number) =>
  Effect.gen(function* () {
    for (let elapsed = 0; elapsed < millis; elapsed += 20)
      yield* TestClock.adjust(Math.min(20, millis - elapsed));
  });

const hash = (n: number) => {
  let t = (n + 0x6d2b79f5) | 0;
  t = Math.imul(t ^ (t >>> 15), t | 1);
  t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
  return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
};

const jittered: SimOptions = {
  build: (entry) =>
    Effect.sleep(
      Math.round(entry.durationSeconds * 1000 * 0.41 * (0.75 + 0.5 * hash(entry.seq))),
    ).pipe(Effect.as(entry.durationSeconds)),
  faults: { buildFails: (seq) => seq % 11 === 0 },
};

interface Burst {
  readonly silenceMs: number;
  readonly seconds: readonly number[];
}

/** Bursts of clips after silence, including an eight-minute gap after the first ten minutes. */
const bursts = (): readonly Burst[] => {
  const all: Burst[] = [];
  let r = 1;
  const next = () => hash(r++ * 7919);
  for (let elapsed = 0; elapsed < RUN_MS;) {
    const outage = elapsed > 10 * 60_000 && !all.some((burst) => burst.silenceMs >= 480_000);
    const silenceMs = outage ? 480_000 : 2_000 + Math.round(next() * 88_000);
    const seconds = Array.from({ length: 1 + Math.floor(next() * 8) }, () =>
      H3.clampSecondsTo(H3.h3ReferenceTurboRealtime, (40 + Math.floor(next() * 211)) / 16.7 + 0.3),
    );
    all.push({ silenceMs, seconds });
    elapsed += silenceMs + seconds.reduce((sum, s) => sum + s * 1000, 0);
  }
  return all;
};

const perform = (targetSeconds: number) =>
  runClock(
    Effect.gen(function* () {
      const scheduler = yield* makeScheduler(options(targetSeconds));
      const events: AsRunEvent[] = [];
      yield* scheduler.asRun.pipe(
        Stream.runForEach((event) =>
          Effect.sync(() => {
            events.push(event);
          }),
        ),
        Effect.forkScoped,
      );
      yield* advance(10_000);
      const initialStarved = (yield* scheduler.state).starved;
      const written: ItemKey[] = [];
      const latest = new Map<ItemKey, AsRunStatus>();
      const starts = new Map<ItemKey, number>();
      const show = Effect.gen(function* () {
        let index = 0;
        for (const burst of bursts()) {
          yield* Effect.sleep(burst.silenceMs);
          const mine: { readonly key: ItemKey; readonly seconds: number }[] = [];
          const finish = (now: number) =>
            mine.reduce((end, entry) => {
              if (latest.get(entry.key)?._tag === "Failed") return end;
              const at = starts.get(entry.key);
              if (at !== undefined) return at + entry.seconds * 1000;
              return Math.max(end, now) + entry.seconds * 1000;
            }, now);
          for (const seconds of burst.seconds) {
            while (
              finish(yield* Clock.currentTimeMillis) - (yield* Clock.currentTimeMillis) >
              LEAD_MS
            )
              yield* Effect.sleep(20);
            const key = ItemKey.make(`show-${index++}`);
            yield* scheduler.submit({ key, lane: "show", request: clip(seconds, `Line ${index}`) });
            written.push(key);
            mine.push({ key, seconds });
          }
          while (mine.some((entry) => !latest.has(entry.key))) yield* Effect.sleep(20);
        }
      });
      yield* scheduler.asRun.pipe(
        Stream.runForEach((event) =>
          Effect.sync(() => {
            latest.set(event.key, event.status);
            if (event.status._tag === "Started") starts.set(event.key, event.status.at);
          }),
        ),
        Effect.forkScoped,
      );
      yield* Effect.forkScoped(show);
      yield* advance(RUN_MS);
      return { events, written, state: yield* scheduler.state, initialStarved };
    }).pipe(Effect.provide(Simulation.layerSim(jittered))),
  );

test("a 30-minute simulated show with jitter and build failures keeps ordered output on air", async () => {
  const { events, written, state, initialStarved } = await perform(30);
  const accepted = new Set(written);
  const starts = events.filter(
    (event): event is AsRunEvent & { status: Extract<AsRunStatus, { _tag: "Started" }> } =>
      accepted.has(event.key) && event.status._tag === "Started",
  );
  const ended = events.filter((event) => accepted.has(event.key) && event.status._tag === "Ended");
  const failed = events.filter(
    (event) => accepted.has(event.key) && event.status._tag === "Failed",
  );
  const startedKeys = new Set(starts.map((event) => event.key));
  expect(state.starved).toBe(initialStarved);
  expect(starts.map((event) => event.key)).toEqual(written.filter((key) => startedKeys.has(key)));
  expect(starts.length).toBeGreaterThan(RUN_MS / 60_000);
  expect(failed.length).toBeGreaterThan(0);
  expect(
    ended.every(
      (event) => event.status._tag === "Ended" && event.status.termination === "finished",
    ),
  ).toBe(true);
  expect(starts.every((event) => event.status.sessionId.length > 0)).toBe(true);
}, 600_000);

test("a five-second filler runway exposes starvation under the same workload", async () => {
  const { state, initialStarved } = await perform(5);
  expect(state.starved).toBeGreaterThan(initialStarved);
}, 600_000);
