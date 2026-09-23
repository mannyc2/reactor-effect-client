import { expect, test } from "vitest";
import { Effect, Option, Result, Stream } from "effect";
import { TestClock } from "effect/testing";
import { Engine, Handle } from "../../src/orchestration/types.js";
import type { EngineEvent, LocalClipRecord } from "../../src/orchestration/types.js";
import { ClipId } from "../../src/orchestration/request.js";
import * as Simulation from "../../src/simulation/index.js";
import type { SimOptions } from "../../src/simulation/index.js";
import { buildFailsEveryNth } from "../../src/testing/Faults.js";
import {
  audioFrame,
  gate,
  member,
  request,
  run,
  runClock,
  until,
  videoFrame,
} from "./SourceFixture.js";

const advance = (millis: number) =>
  Effect.gen(function* () {
    for (let elapsed = 0; elapsed < millis; elapsed += 25)
      yield* TestClock.adjust(Math.min(25, millis - elapsed));
  });
const input = () => request({ durationSeconds: 5 });
const observe = (events: Stream.Stream<EngineEvent, unknown>) =>
  Effect.gen(function* () {
    const all: EngineEvent[] = [];
    yield* events.pipe(
      Stream.runForEach((event) =>
        Effect.sync(() => {
          all.push(event);
        }),
      ),
      Effect.forkScoped,
    );
    yield* TestClock.adjust(0);
    return all;
  });

test("simulation autoplay consumes clips in order while playout capacity keeps the next build queued", () =>
  runClock(
    Effect.gen(function* () {
      const engine = yield* Engine;
      const events = yield* observe(engine.events);
      const a = yield* engine.enqueue(input()),
        b = yield* engine.enqueue(input()),
        c = yield* engine.enqueue(input());
      yield* advance(500);
      const buffered = yield* engine.state;
      expect(Option.map(buffered.playing, (entry) => entry.clipId)).toEqual(Option.some(a));
      expect(buffered.ready.map((entry) => entry.clipId)).toEqual([b]);
      // Unlike a remote queue-head guess, this source exposes only an actually running build.
      expect(buffered.queued.map((entry) => entry.clipId)).toEqual([c]);
      expect(buffered.generationOrder).toEqual([c]);
      expect(buffered.building).toEqual(Option.none());
      yield* advance(5000);
      const advanced = yield* engine.state;
      expect(Option.map(advanced.playing, (entry) => entry.clipId)).toEqual(Option.some(b));
      expect(advanced.ready.map((entry) => entry.clipId)).toEqual([c]);
      yield* advance(11000);
      const starts = events.filter((event) => event._tag === "Started");
      expect(starts.map((event) => event.clipId)).toEqual([a, b, c]);
      expect(events.filter((event) => event._tag === "Ended").map((event) => event.clipId)).toEqual(
        [a, b, c],
      );
      expect(events.filter((event) => event._tag === "Starved")).toHaveLength(1);
      for (let index = 1; index < starts.length; index++) {
        expect(starts[index]!.at - starts[index - 1]!.at).toBeCloseTo(
          starts[index - 1]!.durationSeconds * 1000,
          5,
        );
      }
      expect((yield* engine.state).playing).toEqual(Option.none());
    }).pipe(
      Effect.provide(
        Simulation.layerSim({ buildFixedMs: 100, buildRatio: 0, queueLimit: 3, playoutLimit: 1 }),
      ),
    ),
  ));

test("removing a simulated ready clip immediately releases the next build without replaying playback", () =>
  runClock(
    Effect.gen(function* () {
      const engine = yield* Engine;
      const events = yield* observe(engine.events);
      const a = yield* engine.enqueue(input()),
        b = yield* engine.enqueue(input()),
        c = yield* engine.enqueue(input());
      yield* advance(500);
      expect(yield* engine.remove(b)).toBe("ready");
      yield* advance(200);
      const state = yield* engine.state;
      expect(Option.map(state.playing, (entry) => entry.clipId)).toEqual(Option.some(a));
      expect(state.ready.map((entry) => entry.clipId)).toEqual([c]);
      yield* advance(11000);
      expect(
        events.filter((event) => event._tag === "Started").map((event) => event.clipId),
      ).toEqual([a, c]);
    }).pipe(
      Effect.provide(
        Simulation.layerSim({ buildFixedMs: 100, buildRatio: 0, queueLimit: 3, playoutLimit: 1 }),
      ),
    ),
  ));

test("failed simulated generation is skipped and closing the scope cancels unfinished playback", () =>
  runClock(
    Effect.gen(function* () {
      let events: EngineEvent[] = [];
      yield* Effect.scoped(
        Effect.gen(function* () {
          const engine = yield* Engine;
          events = yield* observe(engine.events);
          const a = yield* engine.enqueue(input()),
            b = yield* engine.enqueue(input()),
            c = yield* engine.enqueue(input());
          yield* advance(6000);
          expect(
            events.filter((event) => event._tag === "Started").map((event) => event.clipId),
          ).toEqual([a, c]);
          expect(
            events.filter((event) => event._tag === "Failed").map((event) => event.clipId),
          ).toEqual([b]);
          expect(Option.map((yield* engine.state).playing, (entry) => entry.clipId)).toEqual(
            Option.some(c),
          );
        }).pipe(
          Effect.provide(
            Simulation.layerSim({
              buildFixedMs: 100,
              buildRatio: 0,
              queueLimit: 3,
              faults: buildFailsEveryNth(2),
            }),
          ),
        ),
      );
      const before = [...events];
      yield* advance(20000);
      expect(events).toEqual(before);
    }),
  ));

test("actual simulation build ownership stays distinct from generation position and unknown timing is explicit", () =>
  runClock(
    Effect.gen(function* () {
      for (const timing of ["measured", "unknown"] as const) {
        const source = yield* Simulation.source({ buildFixedMs: 500, buildRatio: 0, timing });
        const a = yield* (yield* source.prepareRouted({ request: input(), position: undefined }))
          .submit;
        yield* TestClock.adjust(0);
        const b = yield* (yield* source.prepareRouted({ request: input(), position: 0 })).submit;
        yield* TestClock.adjust(0);
        const state = yield* source.state;
        expect(state.generationOrder).toEqual([b, a]);
        const building = Option.getOrThrow(state.building);
        expect(building.record.clipId).toBe(a);
        expect(Option.isSome(building.startedAt)).toBe(timing === "measured");
        expect(state.queued.map((entry) => entry.clipId)).toEqual([b]);
        expect(yield* source.remove(a)).toBe("in_flight");
        yield* advance(1000);
        expect((yield* source.state).ready.map((entry) => entry.clipId)).toEqual([b]);
        yield* source.close;
      }
    }),
  ));

test("simulation renderer hooks receive local annotations, extend duration, and release unpresented records once", () =>
  runClock(
    Effect.gen(function* () {
      const built: LocalClipRecord[] = [],
        discarded: string[] = [];
      const source = yield* Simulation.source({
        build: (record) =>
          Effect.sync(() => {
            built.push(record);
            return 9.03;
          }),
        discard: (record) =>
          Effect.sync(() => {
            discarded.push(record.clipId);
          }),
      });
      const a = yield* (yield* source.prepareRouted({
        request: member("local", true, "final", { speech: "Rendered locally." }),
        position: undefined,
      })).submit;
      yield* TestClock.adjust(0);
      expect(built).toHaveLength(1);
      expect(built[0]!.request.speech).toBe("Rendered locally.");
      expect(built[0]!.seq).toBe(1);
      expect(Number.isFinite(built[0]!.enqueuedAt)).toBe(true);
      const ready = (yield* source.state).ready[0]!;
      expect(ready.durationSeconds).toBeCloseTo(217 / 24, 8);
      expect(ready.provider.frames).toBe(217);
      expect(yield* source.remove(a)).toBe("ready");
      expect(discarded).toEqual([a]);
      yield* source.close;
      expect(discarded).toEqual([a]);
    }),
  ));

test("simulation media copies renderer buffers and explicit pauseAndStop joins the active presentation", () =>
  run(
    Effect.gen(function* () {
      const held = yield* gate;
      let entered = false,
        released = 0;
      const video = videoFrame(4),
        audio = audioFrame(5, 6);
      const handle = yield* Simulation.make({
        buildFixedMs: 0,
        buildRatio: 0,
        present: (_, __, sink) =>
          Effect.gen(function* () {
            entered = true;
            yield* sink.video(video);
            yield* sink.audio(audio);
            video.data.fill(9);
            audio.samples.fill(9);
            yield* held.wait;
          }).pipe(
            Effect.ensuring(
              Effect.sync(() => {
                released++;
              }),
            ),
          ),
      });
      yield* handle.engine.enqueue(input());
      yield* until(() => entered);
      const videos = yield* handle.media.video.pipe(Stream.take(1), Stream.runCollect);
      const audios = yield* handle.media.audio.pipe(Stream.take(1), Stream.runCollect);
      expect(videos[0]!.data).toEqual(new Uint8Array([4, 0, 0, 0]));
      expect(audios[0]!.samples).toEqual(new Int16Array(5).fill(6));
      yield* handle.engine.pauseAndStop;
      yield* until(() => released === 1);
      expect((yield* handle.engine.state).playing).toEqual(Option.none());
      yield* handle.close;
      expect(released).toBe(1);
    }),
  ));

test("simulation session faults produce retained indeterminate sequence outcomes without committed replay", () =>
  run(
    Effect.gen(function* () {
      const handle = yield* Simulation.make({
        faults: { sessionFails: (sequence) => sequence === 1 },
      });
      const prepared = yield* handle.engine.prepare(member("unknown"));
      const result = yield* Effect.result(prepared.submit);
      expect(Result.isFailure(result) && result.failure.context.outcome).toBe("unknown");
      expect((yield* handle.sequences.get("unknown"))?.status).toBe("indeterminate");
      expect(yield* Effect.result(prepared.submit)).toEqual(result);
      expect((yield* prepared.state)._tag).toBe("Completed");
      yield* handle.close;
    }),
  ));

test("simulation source rejects invalid options and layerSim provides one shared orchestration handle", () =>
  runClock(
    Effect.gen(function* () {
      for (const options of [
        { queueLimit: 0 },
        { playoutLimit: -1 },
        { buildRatio: NaN },
        { buildFixedMs: -1 },
        { playoutGapMs: Infinity },
      ] satisfies SimOptions[]) {
        const invalid = yield* Effect.result(Simulation.source(options));
        expect(Result.isFailure(invalid) && invalid.failure.reason._tag).toBe("InvalidInput");
      }
      yield* Effect.gen(function* () {
        const handle = yield* Handle,
          engine = yield* Engine;
        expect(handle.engine).toBe(engine);
        const unknown = yield* Effect.result(engine.remove(ClipId.make("missing")));
        expect(Result.isFailure(unknown) && unknown.failure.context.outcome).toBe("not-submitted");
      }).pipe(Effect.provide(Simulation.layerSim()));
    }),
  ));
