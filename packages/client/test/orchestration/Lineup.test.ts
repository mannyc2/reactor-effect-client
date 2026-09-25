import { expect, test } from "vitest";
import { Clock, Effect, Exit, Option, Stream } from "effect";
import { TestClock } from "effect/testing";
import * as H3 from "../../src/h3/index.js";
import { makeLineup } from "../../src/orchestration/lineup.js";
import type { ClipFate, LineupOptions } from "../../src/orchestration/lineup.js";
import { ClipRequest, PolicyFailure } from "../../src/orchestration/request.js";
import type { ClipId } from "../../src/orchestration/request.js";
import { Engine } from "../../src/orchestration/types.js";
import type { EngineEvent, EngineShape } from "../../src/orchestration/types.js";
import * as Simulation from "../../src/simulation/index.js";
import type { SimOptions } from "../../src/simulation/index.js";
import { refusal, runClock } from "./SourceFixture.js";

const filler: LineupOptions["filler"]["clip"] = (n) =>
  new ClipRequest({
    prompt: `The host waits at the desk (${n}).`,
    references: [],
    durationSeconds: 5,
    metadata: {},
  });
const clip = (seconds: number, prompt = "The host speaks to camera.") =>
  new ClipRequest({ prompt, references: [], durationSeconds: seconds, metadata: {} });
const lineup = (ready: number, clipAt = filler) => makeLineup({ filler: { ready, clip: clipAt } });

const advance = (millis: number) =>
  Effect.gen(function* () {
    for (let elapsed = 0; elapsed < millis; elapsed += 20)
      yield* TestClock.adjust(Math.min(20, millis - elapsed));
  });

interface Logged {
  readonly t: number;
  readonly e: EngineEvent;
}
/** Every engine event from now on, with the time it was read. */
const record = (engine: EngineShape) =>
  Effect.gen(function* () {
    const log: Logged[] = [];
    const observation = yield* engine.observe({ capacity: 4096 });
    yield* observation.events.pipe(
      Stream.runForEach((e) =>
        Effect.map(Clock.currentTimeMillis, (t) => {
          log.push({ t, e });
        }),
      ),
      Effect.forkScoped,
    );
    return log;
  });
const started = (log: readonly Logged[]) =>
  log.flatMap(({ e }) => (e._tag === "Started" ? [e] : []));
/** The clips that started after `id` was Ready and before it started. */
const passedBy = (log: readonly Logged[], id: ClipId) => {
  const ready = log.find(({ e }) => e._tag === "Ready" && e.clipId === id)!.t;
  const at = started(log).find((start) => start.clipId === id)!.at;
  return started(log).filter((start) => start.at > ready && start.at < at);
};

// ---- a long simulated performance --------------------------------------------------------------

const RUN_MS = 30 * 60_000;
const LEAD_MS = 10_000; // clips are written about ten seconds ahead of playout

const hash = (n: number) => {
  let t = (n + 0x6d2b79f5) | 0;
  t = Math.imul(t ^ (t >>> 15), t | 1);
  t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
  return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
};
/** Builds take `ratio` of the clip's length with ±25% jitter, and every eleventh build fails. */
const jittered = (ratio: number): SimOptions => ({
  build: (entry) =>
    Effect.sleep(
      Math.round(entry.durationSeconds * 1000 * ratio * (0.75 + 0.5 * hash(entry.seq))),
    ).pipe(Effect.as(entry.durationSeconds)),
  faults: { buildFails: (seq) => seq % 11 === 0 },
});

interface Burst {
  readonly silenceMs: number;
  readonly seconds: readonly number[];
}
/** Bursts of 1-8 clips after 2-90 s of silence, with one 8-minute silence after the first 10 minutes. */
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

const perform = (ready: number) =>
  runClock(
    Effect.gen(function* () {
      const engine = yield* Engine;
      const log = yield* record(engine);
      const clips = yield* lineup(ready);
      const written: ClipId[] = [];
      const fates = new Map<ClipId, ClipFate>();
      const show = Effect.gen(function* () {
        for (const burst of bursts()) {
          yield* Effect.sleep(burst.silenceMs);
          const mine: { readonly clipId: ClipId; readonly seconds: number }[] = [];
          // When this burst's clips would finish: started ones at their end, waiting ones back to back.
          const finish = (now: number) =>
            mine.reduce((end, entry) => {
              const fate = fates.get(entry.clipId);
              if (fate?._tag === "Failed") return end;
              if (fate?._tag === "Started") return fate.at + fate.durationSeconds * 1000;
              return Math.max(end, now) + entry.seconds * 1000;
            }, now);
          for (const seconds of burst.seconds) {
            while (
              finish(yield* Clock.currentTimeMillis) - (yield* Clock.currentTimeMillis) >
              LEAD_MS
            )
              yield* Effect.sleep(20);
            const entry = yield* clips.enqueue(clip(seconds));
            yield* entry.fate.pipe(
              Effect.map((fate) => void fates.set(entry.clipId, fate)),
              Effect.forkScoped,
            );
            mine.push({ clipId: entry.clipId, seconds });
            written.push(entry.clipId);
          }
          while (mine.some((entry) => !fates.has(entry.clipId))) yield* Effect.sleep(20);
        }
      });
      yield* Effect.forkScoped(show);
      yield* advance(RUN_MS);
      return { log, written, fates, state: yield* clips.state };
    }).pipe(Effect.provide(Simulation.layerSim(jittered(0.41)))),
  );

test("a 30-minute simulated show at the hosted build rate never starves, keeps clips in order and passes filler only", async () => {
  const { log, written, fates, state } = await perform(3);
  const mine = new Set(written);
  const starts = started(log);
  const first = log.find(({ e }) => e._tag === "Started")!.t;
  const gaps = starts
    .slice(1)
    .map((start, i) => start.at - (starts[i]!.at + starts[i]!.durationSeconds * 1000));
  const startedAt = new Map(starts.map((start) => [start.clipId, start.at]));
  const readyTimes = new Map(
    log.flatMap(({ t, e }) => (e._tag === "Ready" ? [[e.clipId, t] as const] : [])),
  );
  // An overtake: y became Ready after x but started before it.
  const overtakes: [string, string][] = [];
  const both = [...startedAt.keys()].filter((id) => readyTimes.has(id));
  for (const x of both)
    for (const y of both)
      if (readyTimes.get(y)! > readyTimes.get(x)! && startedAt.get(y)! < startedAt.get(x)!)
        overtakes.push([mine.has(y) ? "clip" : "filler", mine.has(x) ? "clip" : "filler"]);

  expect(log.filter(({ t, e }) => e._tag === "Starved" && t >= first)).toHaveLength(0);
  expect(Math.max(...gaps)).toBeLessThan(1);
  expect(starts.filter((start) => mine.has(start.clipId)).map((start) => start.clipId)).toEqual(
    written.filter((id) => startedAt.has(id)),
  );
  expect(overtakes.length).toBeGreaterThan(0);
  expect(overtakes.filter(([y, x]) => y !== "clip" || x !== "filler")).toEqual([]);
  expect(log.filter(({ e }) => e._tag === "Ended" && e.termination === "stopped")).toHaveLength(0);
  expect(starts.filter((start) => mine.has(start.clipId)).length).toBeGreaterThan(RUN_MS / 60_000);
  // Every written clip's fate settled, as the engine saw it.
  for (const id of written) {
    const fate = fates.get(id);
    if (fate === undefined) continue; // written near the end and still waiting
    if (fate._tag === "Started") expect(fate.at).toBe(startedAt.get(id));
    else expect(log.some(({ e }) => e._tag === "Failed" && e.clipId === id)).toBe(true);
  }
  expect(state.starved).toBe(log.filter(({ e }) => e._tag === "Starved").length);
}, 600_000);

test("the same checks catch starvation: one Ready filler clip is not enough", async () => {
  const { log } = await perform(1);
  const first = log.find(({ e }) => e._tag === "Started")!.t;
  expect(log.filter(({ t, e }) => e._tag === "Starved" && t >= first).length).toBeGreaterThan(0);
}, 600_000);

// ---- the lineup's contract, one case at a time ---------------------------------------------

const quick = Simulation.layerSim({ fixedBuildTime: 500, buildRatio: 0 });

test("filler is kept Ready behind the playing clip, built one at a time", () =>
  runClock(
    Effect.gen(function* () {
      const engine = yield* Engine;
      const clips = yield* lineup(3);
      yield* advance(10_000);
      expect(yield* clips.state).toEqual({ playing: "filler", readyFiller: 3, starved: 0 });
      const state = yield* engine.state;
      expect(state.queued).toHaveLength(0);
      expect(Option.isNone(state.building)).toBe(true);
    }).pipe(Effect.provide(quick)),
  ));

test("a clip goes ahead of the Ready filler and plays next, then filler resumes", () =>
  runClock(
    Effect.gen(function* () {
      const engine = yield* Engine;
      const log = yield* record(engine);
      const clips = yield* lineup(3);
      yield* advance(10_000);
      const entry = yield* clips.enqueue(clip(6));
      yield* advance(6_000);
      const fate = yield* entry.fate;
      expect(fate._tag).toBe("Started");
      // Only the filler that was already playing when the clip became Ready went before it.
      expect(passedBy(log, entry.clipId)).toHaveLength(0);
      expect((yield* clips.state).playing).toBe("clip");
      yield* advance(8_000);
      expect(yield* clips.state).toMatchObject({ playing: "filler", starved: 0 });
    }).pipe(Effect.provide(quick)),
  ));

test("a clip enqueued while another waits goes behind it, and both go ahead of the filler", () =>
  runClock(
    Effect.gen(function* () {
      const engine = yield* Engine;
      const log = yield* record(engine);
      const clips = yield* lineup(3);
      yield* advance(10_000);
      const first = yield* clips.enqueue(clip(6));
      const second = yield* clips.enqueue(clip(6));
      yield* advance(15_000);
      const order = started(log)
        .map((start) => start.clipId)
        .filter((id) => id === first.clipId || id === second.clipId);
      expect(order).toEqual([first.clipId, second.clipId]);
      // The second clip played straight after the first: no filler came between them.
      const starts = started(log).map((start) => start.clipId);
      expect(starts[starts.indexOf(second.clipId) - 1]).toBe(first.clipId);
    }).pipe(Effect.provide(quick)),
  ));

test("the lineup tells its clips from filler by what it enqueued, not by what a request says", () =>
  runClock(
    Effect.gen(function* () {
      const engine = yield* Engine;
      const log = yield* record(engine);
      const clips = yield* lineup(3);
      yield* advance(10_000);
      // The same fields as a filler clip, but enqueued as a clip: it still goes ahead of the filler.
      const twin = yield* clips.enqueue(filler(7));
      yield* advance(6_000);
      expect((yield* twin.fate)._tag).toBe("Started");
      expect(passedBy(log, twin.clipId)).toHaveLength(0);
    }).pipe(Effect.provide(quick)),
  ));

test("a move that fails is tried again at the next change, so the clip still passes the filler", () =>
  runClock(
    Effect.gen(function* () {
      const real = yield* Engine;
      let moves = 0;
      const engine: EngineShape = {
        ...real,
        move: (id, position, queue) =>
          moves++ === 0
            ? Effect.fail(
                PolicyFailure.refuse("QueueChanged", "The fixture refuses one move", "move"),
              )
            : real.move(id, position, queue),
      };
      const log = yield* record(real);
      const clips = yield* lineup(3).pipe(Effect.provideService(Engine, engine));
      yield* advance(10_000);
      const entry = yield* clips.enqueue(clip(6));
      yield* advance(12_000);
      expect((yield* entry.fate)._tag).toBe("Started");
      expect(moves).toBeGreaterThanOrEqual(2);
      // Three filler clips were Ready ahead of it; at most one started before the retry moved it.
      expect(passedBy(log, entry.clipId).length).toBeLessThanOrEqual(1);
    }).pipe(Effect.provide(quick)),
  ));

test("a clip whose build fails has failed, and a failed filler build is replaced", () => {
  // Builds of requests whose prompt starts with FAIL fail.
  const failing = new Set<number>();
  return runClock(
    Effect.gen(function* () {
      const clips = yield* lineup(1, (n) => (n === 1 ? clip(5, "FAIL filler") : filler(n)));
      yield* advance(3_000);
      const entry = yield* clips.enqueue(clip(6, "FAIL clip"));
      yield* advance(3_000);
      expect(yield* entry.fate).toEqual({
        _tag: "Failed",
        reason: "Simulated build failed or returned an invalid duration",
      });
      yield* advance(20_000);
      expect(yield* clips.state).toMatchObject({ readyFiller: 1, starved: 0 });
      expect(failing.size).toBe(2);
    }).pipe(
      Effect.provide(
        Simulation.layerSim({
          buildRatio: 0,
          build: (entry) =>
            Effect.sync(() => {
              if (entry.request.prompt.startsWith("FAIL")) failing.add(entry.seq);
            }).pipe(Effect.andThen(Effect.sleep(500)), Effect.as(entry.durationSeconds)),
          faults: { buildFails: (seq) => failing.has(seq) },
        }),
      ),
    ),
  );
});

test("a clip lost with its session has failed, and the replacement session gets filler", () =>
  runClock(
    Effect.gen(function* () {
      let armed = false;
      const handle = yield* Simulation.make({
        fixedBuildTime: 60_000,
        buildRatio: 0,
        faults: {
          sessionFails: () => {
            const fail = armed;
            armed = false;
            return fail;
          },
        },
      });
      const clips = yield* lineup(1).pipe(Effect.provideService(Engine, handle.engine));
      yield* advance(1_000);
      const waiting = yield* clips.enqueue(clip(6));
      armed = true;
      const lost = yield* Effect.exit(clips.enqueue(clip(6)));
      expect(Exit.isFailure(lost)).toBe(true);
      yield* advance(1_000);
      const fate = yield* waiting.fate;
      expect(fate._tag === "Failed" && fate.reason.startsWith("Session lost")).toBe(true);
      yield* advance(3_000);
      const state = yield* handle.engine.state;
      expect(state.generationOrder).toHaveLength(1);
    }),
  ));

test("a removed clip has failed", () =>
  runClock(
    Effect.gen(function* () {
      const clips = yield* lineup(3);
      yield* advance(10_000);
      // Playout is full of filler, so the clip waits unbuilt.
      const entry = yield* clips.enqueue(clip(6));
      expect(yield* entry.remove).toBe("unstarted");
      expect(yield* entry.fate).toEqual({ _tag: "Failed", reason: "Removed" });
    }).pipe(
      Effect.provide(Simulation.layerSim({ fixedBuildTime: 500, buildRatio: 0, playoutLimit: 3 })),
    ),
  ));

test("a clip still waiting when the orchestration closes has failed", () =>
  runClock(
    Effect.gen(function* () {
      const handle = yield* Simulation.make({ fixedBuildTime: 60_000, buildRatio: 0 });
      const clips = yield* lineup(1).pipe(Effect.provideService(Engine, handle.engine));
      const entry = yield* clips.enqueue(clip(6));
      yield* handle.close;
      yield* advance(100);
      expect(yield* entry.fate).toEqual({ _tag: "Failed", reason: "The orchestration closed" });
    }),
  ));

test("a lineup chooses where its clips go: a request with its own position or anchor is refused", () =>
  runClock(
    Effect.gen(function* () {
      const clips = yield* lineup(1);
      const positioned = yield* Effect.flip(
        clips.enqueue(new ClipRequest({ ...clip(6), position: 0 })),
      );
      expect(refusal(positioned)).toBe("InvalidRequest");
      yield* advance(1_000);
      const anchor = Option.getOrThrow((yield* (yield* Engine).state).playing).clipId;
      const anchored = yield* Effect.flip(
        clips.enqueue(new ClipRequest({ ...clip(6), before: anchor })),
      );
      expect(refusal(anchored)).toBe("InvalidRequest");
    }).pipe(Effect.provide(quick)),
  ));

test("a lineup refuses a filler count outside 1 to 1024 and a malformed filler clip before it starts", () =>
  runClock(
    Effect.gen(function* () {
      const none = yield* Effect.flip(lineup(0));
      expect(none._tag).toBe("ReactorError");
      const blank = yield* Effect.flip(lineup(1, () => clip(6, " ")));
      expect(refusal(blank)).toBe("InvalidRequest");
      const engine = yield* Engine;
      expect((yield* engine.state).generationOrder).toHaveLength(0);
    }).pipe(Effect.provide(quick)),
  ));
