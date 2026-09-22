import { expect, test } from "bun:test"
import { Effect, Option, Ref, Stream } from "effect"
import { TestClock } from "effect/testing"
import { ClipRequest } from "../../src/Clip.js"
import { ClipEngine, type EngineEvent } from "../../src/engine/Engine.js"
import { layerSim } from "../../src/engine/SimulatedTransport.js"
import { buildFailsEveryNth } from "../../testing/Faults.js"
import * as TestPlatform from "../../testing/Platform.js"

const request = () => new ClipRequest({
  prompt: "A fresh generated host clip.",
  references: [{ uri: "fixture://host" }],
  durationSeconds: 5,
  metadata: { beatId: "test-beat", segmentId: "test-segment" },
})

// Give queue wakeups between the independent builder/player fibers a turn
// before advancing to another timer; one large jump can skip that work.
const advance = (millis: number) => Effect.gen(function* () {
  for (let elapsed = 0; elapsed < millis; elapsed += 25) yield* TestClock.adjust(Math.min(25, millis - elapsed))
})

test("sim autoplay consumes ready clips in order while generation respects the playout bound", async () => {
  await Effect.runPromise(Effect.scoped(Effect.gen(function* () {
    const engine = yield* ClipEngine
    const events = yield* Ref.make<ReadonlyArray<EngineEvent>>([])
    yield* engine.events.pipe(Stream.runForEach((event) => Ref.update(events, (all) => [...all, event])), Effect.forkScoped)
    yield* TestClock.adjust(0)
    const a = yield* engine.enqueue(request())
    const b = yield* engine.enqueue(request())
    const c = yield* engine.enqueue(request())
    yield* advance(500)
    const buffered = yield* engine.state
    expect(Option.map(buffered.playing, (entry) => entry.record.clipId)).toEqual(Option.some(a))
    expect(buffered.ready.map((entry) => entry.clipId)).toEqual([b])
    expect(buffered.queued).toEqual([])
    expect(Option.map(buffered.building, (entry) => [entry.record.clipId, Option.isNone(entry.startedAt)])).toEqual(Option.some([c, true]))
    yield* advance(5_000)
    const advanced = yield* engine.state
    expect(Option.map(advanced.playing, (entry) => entry.record.clipId)).toEqual(Option.some(b))
    expect(advanced.ready.map((entry) => entry.clipId)).toEqual([c])
    yield* advance(11_000)
    const observed = yield* Ref.get(events)
    const starts = observed.filter((event) => event._tag === "Started")
    expect(starts.map((event) => event.clipId)).toEqual([a, b, c])
    expect(observed.filter((event) => event._tag === "Ended").map((event) => event.clipId)).toEqual([a, b, c])
    expect(observed.filter((event) => event._tag === "Starved")).toHaveLength(1)
    for (let index = 1; index < starts.length; index++) {
      const previous = starts[index - 1]!
      expect(starts[index]!.at - previous.at).toBeCloseTo(previous.durationSeconds * 1_000, 5)
    }
    expect(Option.isNone((yield* engine.state).playing)).toBe(true)
  }).pipe(Effect.provide(layerSim({ buildFixedMs: 100, buildRatio: 0, queueLimit: 3, playoutLimit: 1 })),
    Effect.provide(TestPlatform.layer),
    Effect.provide(TestClock.layer()))))
})

test("removing a ready clip releases the next build without replaying or waiting for the current clip to end", async () => {
  await Effect.runPromise(Effect.scoped(Effect.gen(function* () {
    const engine = yield* ClipEngine
    const events = yield* Ref.make<ReadonlyArray<EngineEvent>>([])
    yield* engine.events.pipe(Stream.runForEach((event) => Ref.update(events, (all) => [...all, event])), Effect.forkScoped)
    yield* TestClock.adjust(0)
    const a = yield* engine.enqueue(request())
    const b = yield* engine.enqueue(request())
    const c = yield* engine.enqueue(request())
    yield* advance(500)
    expect(yield* engine.remove(b)).toBe("ready")
    yield* advance(200)
    const state = yield* engine.state
    expect(Option.map(state.playing, (entry) => entry.record.clipId)).toEqual(Option.some(a))
    expect(state.ready.map((entry) => entry.clipId)).toEqual([c])
    yield* advance(11_000)
    expect((yield* Ref.get(events)).filter((event) => event._tag === "Started").map((event) => event.clipId)).toEqual([a, c])
  }).pipe(Effect.provide(layerSim({ buildFixedMs: 100, buildRatio: 0, queueLimit: 3, playoutLimit: 1 })),
    Effect.provide(TestPlatform.layer),
    Effect.provide(TestClock.layer()))))
})

test("failed generation is skipped by autoplay and closing the engine cancels unfinished playback", async () => {
  await Effect.runPromise(Effect.scoped(Effect.gen(function* () {
    const events = yield* Ref.make<ReadonlyArray<EngineEvent>>([])
    yield* Effect.scoped(Effect.gen(function* () {
      const engine = yield* ClipEngine
      yield* engine.events.pipe(Stream.runForEach((event) => Ref.update(events, (all) => [...all, event])), Effect.forkScoped)
      yield* TestClock.adjust(0)
      const a = yield* engine.enqueue(request())
      const b = yield* engine.enqueue(request())
      const c = yield* engine.enqueue(request())
      yield* advance(6_000)
      const observed = yield* Ref.get(events)
      expect(observed.filter((event) => event._tag === "Started").map((event) => event.clipId)).toEqual([a, c])
      expect(observed.filter((event) => event._tag === "Failed").map((event) => event.clipId)).toEqual([b])
      expect(Option.map((yield* engine.state).playing, (entry) => entry.record.clipId)).toEqual(Option.some(c))
    }).pipe(Effect.provide(layerSim({ buildFixedMs: 100, buildRatio: 0, queueLimit: 3, faults: buildFailsEveryNth(2) })), Effect.provide(TestPlatform.layer)))
    const before = yield* Ref.get(events)
    yield* advance(20_000)
    expect(yield* Ref.get(events)).toEqual(before)
  }).pipe(Effect.provide(TestClock.layer()))))
})
