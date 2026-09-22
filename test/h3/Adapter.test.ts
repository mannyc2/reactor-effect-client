/**
 * Offline adapter tests over the injectable transport seam. The adapter under
 * test is exactly the one the native client drives (`Session.make`); only the
 * transport is scripted. Fixtures are hand-authored from the documented
 * protocol, not captured live evidence.
 */
import { describe, expect, test } from "bun:test"
import { Crypto, Effect, Exit, Fiber, FileSystem, Option, Path, Ref, Result, Scope, Stream } from "effect"
import { HttpClient } from "effect/unstable/http"
import { ClipId, ClipRequest, type ReferenceImage } from "../../src/Clip.js"
import { h3ReferenceTurboRealtime } from "../../src/ModelProfile.js"
import { decodeMetadata } from "../../src/engine/Metadata.js"
import { ReactorError } from "../../src/Model.js"
import { make, type ReactorSession, type SessionOptions } from "../../src/engine/Session.js"
import { type EngineEvent } from "../../src/engine/Engine.js"
import { type Fake, type FakeScript, gate, makeFake, nativeRejection } from "./FakeTransport.js"
import { dataUri, pngBytes } from "../../testing/Png.js"
import * as TestPlatform from "../../testing/Platform.js"

const png = dataUri(pngBytes(64, 48))
const png2 = dataUri(pngBytes(48, 64))
const refA: ReferenceImage = { uri: png }
const refB: ReferenceImage = { uri: png2 }

const request = (overrides: Partial<ConstructorParameters<typeof ClipRequest>[0]> = {}) =>
  new ClipRequest({
    prompt: "<Picture 1> is A. She says hello.",
    references: [refA],
    durationSeconds: 7,
    metadata: { beatId: "beat_001", segmentId: "intro" },
    ...overrides
  })

const options: SessionOptions = { commandTimeoutMs: 300, setupTimeoutMs: 2_000, reconcileWindowMs: 150 }

interface Harness {
  readonly fake: Fake
  readonly session: ReactorSession
  readonly events: Effect.Effect<ReadonlyArray<EngineEvent>>
  readonly settle: Effect.Effect<void>
}

/** Build a session over a fake transport inside the test's scope and record every engine event. */
const harness = (script: FakeScript = {}, opts: Partial<SessionOptions> = {}) =>
  Effect.gen(function*() {
    const fake = yield* makeFake(script)
    const events = yield* Ref.make<ReadonlyArray<EngineEvent>>([])
    const session = yield* make(fake.transport, { ...options, ...opts })
    yield* session.engine.events.pipe(Stream.runForEach((e) => Ref.update(events, (es) => [...es, e])), Effect.forkScoped)
    yield* Effect.sleep(5) // the subscriber must be attached before the test publishes anything
    const h: Harness = { fake, session, events: Ref.get(events), settle: Effect.sleep(20) }
    return h
  })

type TestEnvironment = Scope.Scope | FileSystem.FileSystem | Path.Path | Crypto.Crypto | HttpClient.HttpClient
const run = <A, E>(effect: Effect.Effect<A, E, TestEnvironment>) =>
  Effect.runPromise(Effect.scoped(effect.pipe(Effect.provide(TestPlatform.layer))))
const tags = (events: ReadonlyArray<EngineEvent>) => events.map((e) => e._tag)

test("reconnect preserves the session and queued work, while media emitted in the gap is lost", () => run(Effect.gen(function* () {
  let reconnects = 0
  const { fake, session, events, settle } = yield* harness({ reconnect: (fake) => Effect.gen(function* () {
    reconnects++
    const lost = (yield* fake.accepted)[0]!
    yield* fake.emit(fake.clipMessage("clip_generated", lost))
    yield* fake.emit(fake.clipMessage("clip_started", lost))
    yield* fake.emit(fake.clipMessage("clip_finished", lost))
  }) })
  const lost = yield* session.engine.enqueue(request())
  const retained = yield* session.engine.enqueue(request({ metadata: { beatId: "next", segmentId: "intro" } }))
  const id = yield* session.sessionId
  yield* fake.emitControl({ _tag: "Status", status: "disconnected" })
  yield* settle
  yield* session.engine.setAutoplay(true)
  expect(reconnects).toBe(1)
  expect(yield* session.sessionId).toEqual(id)
  const state = yield* session.engine.state
  expect([...state.queued, ...Option.toArray(Option.map(state.building, (b) => b.record))].map((c) => c.clipId)).toContain(retained)
  const observed = yield* events
  expect(observed.some((e) => e._tag === "Failed" && e.clipId === lost)).toBe(true)
  expect(observed.some((e) => ["Started", "Ended", "SessionFailed"].includes(e._tag))).toBe(false)
  expect((yield* fake.calls).map((c) => c.command)).toContain("get_queue")
})))

test("a pop acknowledgement without queue removal is uncertain", () => run(Effect.gen(function* () {
  const { session } = yield* harness({ send: { pop: (_args, { fake }) => Effect.gen(function* () {
    return fake.clipMessage("clip_popped", (yield* fake.accepted)[1]!)
  }) } })
  yield* session.engine.enqueue(request())
  const id = yield* session.engine.enqueue(request())
  const result = yield* Effect.result(session.engine.remove(id))
  expect(result._tag).toBe("Failure")
  if (result._tag === "Failure") expect(result.failure._tag).toBe("Uncertain")
})))

test("a queued removal cannot hold the command permit while waiting for reconnect", () => run(Effect.gen(function* () {
  const held = yield* gate()
  let enqueues = 0, reconnects = 0
  const { fake, session, settle } = yield* harness({
    reconnect: () => Effect.sync(() => { reconnects++ }),
    send: { enqueue: (args, { defaults }) => Effect.gen(function* () {
      if (++enqueues === 2) yield* held.wait
      return yield* defaults("enqueue", args)
    }) },
  })
  const first = yield* session.engine.enqueue(request())
  const pending = yield* session.engine.enqueue(request()).pipe(Effect.forkScoped)
  yield* settle
  const removal = yield* session.engine.remove(first).pipe(Effect.forkScoped)
  yield* settle
  yield* fake.emitControl({ _tag: "Status", status: "disconnected" })
  yield* settle
  yield* held.release
  yield* Fiber.join(pending)
  expect(yield* Fiber.join(removal).pipe(Effect.timeout("2 seconds"))).toBe("in_flight")
  yield* session.engine.setAutoplay(true).pipe(Effect.timeout("2 seconds"))
  expect(reconnects).toBe(1)
})))

describe("Reactor adapter: references, metadata, acceptance, continuation", () => {
  test("validates locally, uploads each distinct image once, preserves ordered duplicates, encodes metadata, returns the provider UUID and its accepted length", () =>
    run(Effect.gen(function*() {
      const { fake, session, settle, events } = yield* harness()
      const clipId: string = yield* session.engine.enqueue(request({ references: [refA, refB, refA], durationSeconds: 7 }))
      const [accepted] = yield* fake.accepted
      expect(clipId).toBe(accepted!.clip_id)
      expect(/^[0-9a-f-]{36}$/.test(clipId)).toBe(true)
      const uploads = yield* fake.uploads
      expect(uploads.length).toBe(2)
      const enqueue = (yield* fake.calls).find((c) => c.command === "enqueue")!
      // the documented upload reference: all four fields, in request order, duplicates preserved
      const refs = enqueue.args.reference_images as ReadonlyArray<{ upload_id: string; name: string; mime_type: string; size: number }>
      expect(refs.length).toBe(3)
      expect(refs[0]!.upload_id).toBe(refs[2]!.upload_id)
      expect(refs[0]!.upload_id).not.toBe(refs[1]!.upload_id)
      for (const r of refs) expect(typeof r.name === "string" && typeof r.mime_type === "string" && typeof r.size === "number").toBe(true)
      // Autoplay is established before the first admission.
      const startup = (yield* fake.calls).map((c) => c.command)
      expect(startup.slice(0, 3)).toEqual(["get_state", "set_autoplay", "set_flush_on_clip_end"])
      expect((yield* fake.calls).find((c) => c.command === "set_autoplay")?.args.enabled).toBe(true)
      const meta = decodeMetadata(enqueue.args.metadata as string)
      expect(Option.isSome(meta) && meta.value.caller?.beatId === "beat_001" && meta.value.caller?.segmentId === "intro").toBe(true)
      expect(enqueue.args.seconds).toBe(7)
      yield* settle
      const state = yield* session.engine.state
      // 7s requested → 175 frames → 7.2917s accepted; the state carries the accepted length
      expect(Option.isSome(state.building)).toBe(true)
      expect(Option.getOrThrow(state.building).record.durationSeconds).toBeCloseTo(175 / 24, 4)
      expect(Option.isNone(Option.getOrThrow(state.building).startedAt)).toBe(true)
      expect(tags(yield* events)).toEqual(["Queued", "Building"])
    })))

  test("uploads boundary frames separately from ordered references and reuses their session cache", () =>
    run(Effect.gen(function*() {
      const { fake, session } = yield* harness()
      yield* session.engine.enqueue(request({ startingFrame: refA, endingFrame: refB }))
      const call = (yield* fake.calls).find((c) => c.command === "enqueue")!
      expect(call.args.reference_images).toEqual([call.uploads!.starting_frame!])
      expect(call.args.starting_frame).toBeUndefined()
      expect(call.args.ending_frame).toBeUndefined()
      expect(call.uploads!.ending_frame!.upload_id).not.toBe(call.uploads!.starting_frame!.upload_id)
      yield* session.engine.enqueue(request({ references: Array.from({ length: 9 }, () => refA), startingFrame: refB, endingFrame: refA }))
      expect((yield* fake.uploads).length).toBe(2)
    })))

  test("reconciles a boundary-frame acceptance delivered only by broadcast", () =>
    run(Effect.gen(function*() {
      const { fake, session } = yield* harness({ send: {
        enqueue: (args, { fake, defaults, uploads }) => Effect.gen(function*() {
          yield* defaults("enqueue", args, uploads)
          yield* fake.emit(fake.clipMessage("clip_queued", (yield* fake.accepted).at(-1)!))
          return undefined
        })
      } })
      const id: string = yield* session.engine.enqueue(request({ startingFrame: refA, endingFrame: refB }))
      expect(id).toBe((yield* fake.accepted)[0]!.clip_id)
    })))

  test.each(["startingFrame", "endingFrame"] as const)("stops the session when an accepted clip loses its %s", (field) =>
    run(Effect.gen(function*() {
      const { fake, session } = yield* harness({ send: {
        // Omit the upload map to simulate a provider accepting the wrong clip.
        enqueue: (args, { defaults }) => defaults("enqueue", args)
      } })
      const result = yield* Effect.result(session.engine.enqueue(request({ [field]: refA })))
      expect(Result.isFailure(result) && result.failure._tag === "Uncertain").toBe(true)
      const retry = yield* Effect.result(session.engine.enqueue(request()))
      expect(Result.isFailure(retry) && retry.failure._tag === "SessionFailed").toBe(true)
      expect((yield* fake.calls).filter((c) => c.command === "enqueue").length).toBe(1)
    })))

  test("refuses invalid references and lengths before anything is sent", () =>
    run(Effect.gen(function*() {
      const { fake, session } = yield* harness()
      const tall: ReferenceImage = { uri: dataUri(pngBytes(10, 100)) }
      const cases = [
        request({ references: [tall] }),
        request({ startingFrame: tall }),
        request({ endingFrame: tall }),
        request({ references: [] }),
        request({ references: Array.from({ length: 10 }, () => refA) }),
        request({ durationSeconds: 4 }),
        request({ durationSeconds: Number.NaN }),
        request({ references: [{ uri: "data:text/plain;base64,aGVsbG8=" }] }),
        request({ prompt: "" })
      ]
      for (const c of cases) {
        const r = yield* Effect.result(session.engine.enqueue(c))
        expect(Result.isFailure(r) && r.failure._tag === "InvalidRequest").toBe(true)
      }
      expect((yield* fake.calls).filter((c) => c.command === "enqueue").length).toBe(0)
    })))

  test("continuation needs a generated predecessor inside the retention window", () =>
    run(Effect.gen(function*() {
      const { fake, session, settle } = yield* harness()
      const first = yield* session.engine.enqueue(request())
      const before = yield* Effect.result(session.engine.enqueue(request({ continueFrom: ClipId.make(first) })))
      expect(Result.isFailure(before) && before.failure._tag === "InvalidRequest").toBe(true)
      const [clipA] = yield* fake.accepted
      yield* fake.emit(fake.clipMessage("clip_generated", { ...clipA!, ready: true }))
      yield* settle
      const conflicting = yield* Effect.result(session.engine.enqueue(request({ continueFrom: ClipId.make(first), startingFrame: refB })))
      expect(Result.isFailure(conflicting) && conflicting.failure._tag === "InvalidRequest").toBe(true)
      expect((yield* fake.uploads).length).toBe(1)
      const second = yield* session.engine.enqueue(request({ continueFrom: ClipId.make(first) }))
      const call = (yield* fake.calls).filter((c) => c.command === "enqueue").at(-1)!
      expect(call.args.continue_from_clip_id).toBe(first)
      // push `first` out of the window with newer generated clips
      const window = h3ReferenceTurboRealtime.continuationWindow
      for (let i = 0; i < window; i++) {
        yield* session.engine.enqueue(request())
        const clip = (yield* fake.accepted).at(-1)!
        yield* fake.emit(fake.clipMessage("clip_generated", { ...clip, ready: true }))
      }
      yield* settle
      const state = yield* session.engine.state
      expect(state.continuable.includes(ClipId.make(first))).toBe(false)
      expect(state.continuable.length).toBe(window)
      const expired = yield* Effect.result(session.engine.enqueue(request({ continueFrom: ClipId.make(first) })))
      expect(Result.isFailure(expired) && expired.failure._tag === "InvalidRequest").toBe(true)
      expect(typeof second).toBe("string")
    })))
})

describe("Reactor adapter: reply/broadcast ordering and duplicates", () => {
  test("autoplay can start and finish before the correlated enqueue reply without losing its lifecycle", () =>
    run(Effect.gen(function*() {
      const script: FakeScript = {
        send: {
          enqueue: (args, { fake, defaults }) =>
            Effect.gen(function*() {
              const reply = yield* defaults("enqueue", args)
              const clip = (yield* fake.accepted).at(-1)!
              // Autoplay is independent of the caller receiving acceptance.
              yield* fake.emit(fake.clipMessage("clip_generated", { ...clip, ready: true }))
              yield* fake.emit(fake.clipMessage("clip_started", { ...clip, ready: true }))
              yield* fake.emit(fake.clipMessage("clip_finished", { ...clip, ready: true }))
              yield* Effect.sleep(10)
              return reply
            })
        }
      }
      const { session, settle, events } = yield* harness(script)
      const clipId = yield* session.engine.enqueue(request())
      yield* settle
      const state = yield* session.engine.state
      expect(state.ready).toEqual([])
      expect(state.continuable).toContain(clipId)
      expect(Option.isNone(state.playing)).toBe(true)
      expect(Option.isNone(state.building)).toBe(true)
      expect(tags(yield* events)).toEqual(["Queued", "Building", "Ready", "Started", "Ended", "Starved"])
    })))

  test("duplicate lifecycle deliveries publish once", () =>
    run(Effect.gen(function*() {
      const { fake, session, settle, events } = yield* harness()
      yield* session.engine.enqueue(request())
      const [clip] = yield* fake.accepted
      for (let i = 0; i < 2; i++) yield* fake.emit(fake.clipMessage("clip_generated", { ...clip!, ready: true }))
      for (let i = 0; i < 2; i++) yield* fake.emit(fake.clipMessage("clip_started", { ...clip!, ready: true }))
      yield* settle
      const t = tags(yield* events)
      expect(t.filter((x) => x === "Ready").length).toBe(1)
      expect(t.filter((x) => x === "Started").length).toBe(1)
    })))

  test("an early autoplay start proves generation without a later generated event requeueing the clip", () =>
    run(Effect.gen(function*() {
      const { fake, session, settle, events } = yield* harness({ send: {
        enqueue: (args, { fake, defaults }) => Effect.gen(function*() {
          yield* defaults("enqueue", args)
          const clip = (yield* fake.accepted).at(-1)!
          yield* fake.emit(fake.clipMessage("clip_started", { ...clip, ready: true }))
          return undefined
        })
      } })
      const id = yield* session.engine.enqueue(request())
      const [clip] = yield* fake.accepted
      yield* fake.emit(fake.clipMessage("clip_generated", { ...clip!, ready: true }))
      yield* settle
      const state = yield* session.engine.state
      expect(Option.map(state.playing, (entry) => entry.record.clipId)).toEqual(Option.some(id))
      expect(Option.isNone(state.building)).toBe(true)
      expect(state.ready).toEqual([])
      expect(state.continuable).toContain(id)
      expect(tags(yield* events)).toEqual(["Queued", "Building", "Ready", "Started"])
      expect((yield* fake.calls).filter((call) => call.command === "enqueue")).toHaveLength(1)
    })))

  test("an ack without clip_queued is reconciled by the metadata token on a later broadcast", () =>
    run(Effect.gen(function*() {
      const script: FakeScript = {
        send: {
          enqueue: (args, { fake, defaults }) =>
            Effect.gen(function*() {
              yield* defaults("enqueue", args) // accepted internally
              const clip = (yield* fake.accepted).at(-1)!
              // the broadcast everyone receives, carrying our metadata, is the only proof of acceptance
              yield* fake.emit({ type: "queue_update", data: { generation: [{ ...clip }], playout: [], history: [] } })
              return undefined // bodyless acknowledgement
            })
        }
      }
      const { fake, session } = yield* harness(script)
      const clipId: string = yield* session.engine.enqueue(request())
      expect(clipId).toBe((yield* fake.accepted)[0]!.clip_id)
    })))
})

describe("Reactor adapter: autoplay, starvation, timing", () => {
  test("enables autoplay before admission but reports playback only from provider observations", () =>
    run(Effect.gen(function*() {
      const { fake, session, settle, events } = yield* harness()
      const id = yield* session.engine.enqueue(request())
      const [clip] = yield* fake.accepted
      yield* fake.emit(fake.clipMessage("clip_generated", { ...clip!, ready: true }))
      yield* settle
      expect((yield* fake.calls).filter((call) => call.command === "set_autoplay").map((call) => call.args.enabled)).toEqual([true])
      expect((yield* session.engine.state).ready.map((record) => record.clipId)).toEqual([id])
      expect(Option.isNone((yield* session.engine.state).playing)).toBe(true)
      expect(tags(yield* events)).not.toContain("Started")
      yield* fake.emit(fake.clipMessage("clip_started", { ...clip!, ready: true }))
      yield* settle
      expect(Option.map((yield* session.engine.state).playing, (entry) => entry.record.clipId)).toEqual(Option.some(id))
      expect((yield* session.engine.state).ready).toEqual([])
      expect((yield* fake.calls).filter((call) => call.command === "play")).toEqual([])
    })))

  test("builds ahead and follows automatic FIFO handoffs without sending play commands", () =>
    run(Effect.gen(function*() {
      const { fake, session, settle, events } = yield* harness()
      const a = yield* session.engine.enqueue(request())
      const b = yield* session.engine.enqueue(request())
      const [clipA, clipB] = yield* fake.accepted
      yield* fake.emit(fake.clipMessage("clip_generated", { ...clipA!, ready: true }))
      yield* fake.emit(fake.clipMessage("clip_started", { ...clipA!, ready: true }))
      yield* settle
      let state = yield* session.engine.state
      expect(Option.map(state.playing, (p) => p.record.clipId)).toEqual(Option.some(a))
      expect(Option.map(state.building, (p) => p.record.clipId)).toEqual(Option.some(b))
      yield* fake.emit(fake.clipMessage("clip_generated", { ...clipB!, ready: true }))
      yield* settle
      expect((yield* session.engine.state).ready.map((record) => record.clipId)).toEqual([b])
      yield* fake.emit(fake.clipMessage("clip_finished", { ...clipA!, ready: true }))
      yield* fake.emit(fake.clipMessage("clip_started", { ...clipB!, ready: true }))
      // A duplicate old finish cannot clear the new playing clip.
      yield* fake.emit(fake.clipMessage("clip_finished", { ...clipA!, ready: true }))
      yield* settle
      expect(Option.map((yield* session.engine.state).playing, (p) => p.record.clipId)).toEqual(Option.some(b))
      expect(tags(yield* events)).not.toContain("Starved")
      yield* fake.emit(fake.clipMessage("clip_finished", { ...clipB!, ready: true }))
      yield* settle
      const observed = yield* events
      expect(observed.filter((event) => event._tag === "Started").map((event) => event.clipId)).toEqual([a, b])
      expect(tags(observed).filter((tag) => tag === "Starved")).toHaveLength(1)
      const readies = observed.filter((event): event is Extract<EngineEvent, { _tag: "Ready" }> => event._tag === "Ready")
      expect(readies[0]!.timing._tag).toBe("Bounded")
      expect(readies[1]!.timing._tag).toBe("Unknown")
      state = yield* session.engine.state
      expect(state.started).toBe(true)
      expect(Option.isNone(state.playing) && state.ready.length === 0).toBe(true)
      expect((yield* fake.calls).filter((call) => call.command === "play")).toEqual([])
    })))

  test("a provider stop ends the playing clip without a local replay", () =>
    run(Effect.gen(function*() {
      const { fake, session, settle, events } = yield* harness()
      yield* session.engine.enqueue(request())
      const [clip] = yield* fake.accepted
      yield* fake.emit(fake.clipMessage("clip_generated", { ...clip!, ready: true }))
      yield* fake.emit(fake.clipMessage("clip_started", { ...clip!, ready: true }))
      yield* fake.emit(fake.clipMessage("clip_stopped", { ...clip!, ready: true }))
      yield* settle
      expect(Option.isNone((yield* session.engine.state).playing)).toBe(true)
      expect(tags(yield* events)).toEqual(["Queued", "Building", "Ready", "Started", "Ended", "Starved"])
      expect((yield* fake.calls).filter((call) => call.command === "play")).toEqual([])
    })))

  test("an autoplay refusal or timeout fails setup and still disconnects", () =>
    run(Effect.gen(function*() {
      for (const mode of ["native", "reply", "broadcast", "disabled", "timeout"] as const) {
        const fake = yield* makeFake({ send: {
          set_autoplay: (_args, { fake }) => mode === "native"
            ? Effect.fail(nativeRejection("UNSUPPORTED", "autoplay unavailable"))
            : mode === "reply"
            ? Effect.succeed({ type: "command_error", data: { command: "set_autoplay", reason: "autoplay unavailable" } })
            : mode === "broadcast"
            ? fake.refuse("set_autoplay", "autoplay unavailable").pipe(Effect.as(undefined))
            : mode === "disabled"
            ? Effect.succeed({ type: "autoplay_accepted", data: { enabled: false } })
            : Effect.never
        } })
        const result = yield* Effect.result(Effect.scoped(make(fake.transport, { ...options, commandTimeoutMs: 25, reconcileWindowMs: 30 })))
        expect(Result.isFailure(result) && result.failure.reason).toBe("could not enable provider autoplay")
        expect(yield* fake.disconnects).toBe(1)
        expect((yield* fake.calls).some((call) => call.command === "enqueue")).toBe(false)
      }
    })))

  test("state_update snapshots confirm missed playback facts and publish the deployment's live length bounds", () =>
    run(Effect.gen(function*() {
      const { fake, session, settle, events } = yield* harness()
      const a = yield* session.engine.enqueue(request())
      const [clip] = yield* fake.accepted
      yield* fake.emit(fake.clipMessage("clip_generated", { ...clip!, ready: true }))
      // The clip_started broadcast was lost; the next snapshot proves playback.
      yield* fake.emit(fake.stateUpdate({ playing: true, playing_clip_id: clip!.clip_id, playout_queued: 0 }))
      yield* settle
      expect(Option.map((yield* session.engine.state).playing, (p) => p.record.clipId)).toEqual(Option.some(a))
      yield* fake.emit(fake.stateUpdate({ playing: false, playing_clip_id: null, clips_played: 1 }))
      yield* settle
      expect(tags(yield* events)).toEqual(["Queued", "Building", "Ready", "Started", "Ended", "Starved"])
      yield* fake.emit(fake.stateUpdate({ clip_seconds_min: 6, clip_seconds_max: 12 }))
      yield* settle
      const tooLong = yield* Effect.result(session.engine.enqueue(request({ durationSeconds: 14 })))
      expect(Result.isFailure(tooLong) && tooLong.failure._tag === "InvalidRequest" && /6–12/.test(tooLong.failure.reason)).toBe(true)
    })))

  test("no starvation is reported before playback has begun", () =>
    run(Effect.gen(function*() {
      const { fake, session, settle, events } = yield* harness()
      yield* session.engine.enqueue(request())
      const [clip] = yield* fake.accepted
      yield* fake.emit(fake.clipMessage("clip_generated", { ...clip!, ready: true }))
      yield* settle
      expect(tags(yield* events)).not.toContain("Starved")
      expect((yield* fake.calls).filter((call) => call.command === "set_autoplay").map((call) => call.args.enabled)).toEqual([true])
    })))

  test("deployment generation capacity still rejects overflow before submission", () =>
    run(Effect.gen(function*() {
      const { fake, session, settle } = yield* harness()
      yield* fake.emit(fake.stateUpdate({ generation_capacity: 1 }))
      yield* settle
      yield* session.engine.enqueue(request())
      const overflow = yield* Effect.result(session.engine.enqueue(request()))
      expect(Result.isFailure(overflow) && overflow.failure._tag === "Rejected" && overflow.failure.code).toBe("queue_full")
      expect((yield* fake.calls).filter((call) => call.command === "enqueue")).toHaveLength(1)
    })))
})

describe("Reactor adapter: failures", () => {
  test("a definitive build failure is a Failed event; a timed-out submission is Uncertain and is not resent", () =>
    run(Effect.gen(function*() {
      let stall = false
      const script: FakeScript = {
        send: {
          enqueue: (args, { defaults }) =>
            stall ? Effect.fail(nativeRejection("REQUEST_TIMEOUT", "control request timed out")) : defaults("enqueue", args)
        }
      }
      const { fake, session, settle, events } = yield* harness(script)
      yield* session.engine.enqueue(request())
      const [clip] = yield* fake.accepted
      yield* fake.emit(fake.clipMessage("clip_failed", clip!, { reason: "content policy" }))
      yield* settle
      const failed = (yield* events).find((e): e is Extract<EngineEvent, { _tag: "Failed" }> => e._tag === "Failed")
      expect(failed?.reason).toBe("content policy")
      expect((yield* session.engine.state).failed).toEqual([ClipId.make(clip!.clip_id)])
      stall = true
      const outcome = yield* Effect.result(session.engine.enqueue(request()))
      expect(Result.isFailure(outcome) && outcome.failure._tag === "Uncertain").toBe(true)
      expect((yield* fake.calls).filter((c) => c.command === "enqueue").length).toBe(2)
      expect(Option.isNone((yield* session.engine.state).building)).toBe(true)
    })))

  test("only correlated or explicitly no-effect refusals are Rejected; an uncorrelated broadcast stays Uncertain", () =>
    run(Effect.gen(function*() {
      let mode: "native" | "message" | "broadcast" = "native"
      const script: FakeScript = {
        send: {
          enqueue: (_args, { fake }) =>
            mode === "native"
              ? Effect.fail(nativeRejection("QUEUE_FULL", "generation queue is full", 429))
              : mode === "message"
              ? Effect.succeed({ type: "command_error", data: { command: "enqueue", reason: "empty prompt" } })
              : fake.refuse("enqueue", "prompt over the limit").pipe(Effect.as(undefined))
        }
      }
      const { session } = yield* harness(script)
      const a = yield* Effect.result(session.engine.enqueue(request()))
      expect(Result.isFailure(a) && a.failure._tag === "Rejected" && a.failure.code === "QUEUE_FULL").toBe(true)
      if (Result.isFailure(a) && a.failure._tag === "Rejected") expect(a.failure.cause).toBeInstanceOf(ReactorError)
      mode = "message"
      const b = yield* Effect.result(session.engine.enqueue(request()))
      expect(Result.isFailure(b) && b.failure._tag === "Rejected" && b.failure.reason === "empty prompt").toBe(true)
      mode = "broadcast"
      const c = yield* Effect.result(session.engine.enqueue(request()))
      expect(Result.isFailure(c) && c.failure._tag === "Uncertain").toBe(true)
      expect((yield* session.engine.failure)._tag).toBe("SessionFailed")
    })))

  test("an unknown provider error code stays Uncertain with its original evidence", () =>
    run(Effect.gen(function* () {
      const { session } = yield* harness({
        send: { enqueue: () => Effect.fail(nativeRejection("MYSTERY_REMOTE_CODE", "provider outcome is ambiguous")) }
      }, { reconcileWindowMs: 10 })
      const outcome = yield* Effect.result(session.engine.enqueue(request()))
      expect(Result.isFailure(outcome) && outcome.failure._tag === "Uncertain").toBe(true)
      if (Result.isFailure(outcome) && outcome.failure._tag === "Uncertain") {
        expect(outcome.failure.code).toBe("MYSTERY_REMOTE_CODE")
        expect(outcome.failure.cause).toBeInstanceOf(ReactorError)
      }
      expect((yield* session.engine.failure)._tag).toBe("SessionFailed")
    })))

  test("prepare snapshots immutable input and rejects null, cyclic and throwing input as InvalidRequest before dispatch", () =>
    run(Effect.gen(function* () {
      const { fake, session } = yield* harness()
      const metadata = { beatId: "original", segmentId: "segment" }
      const mutable = request({ metadata })
      const prepared = yield* session.engine.prepare(mutable)
      metadata.beatId = "mutated"
      ;(mutable as { prompt: string }).prompt = "mutated prompt"
      const first = yield* prepared.submit
      const second = yield* prepared.submit
      expect(second).toBe(first)
      const enqueues = (yield* fake.calls).filter((call) => call.command === "enqueue")
      expect(enqueues).toHaveLength(1)
      expect(enqueues[0]!.args.prompt).toBe("<Picture 1> is A. She says hello.")
      const captured = decodeMetadata(enqueues[0]!.args.metadata as string)
      expect(Option.isSome(captured) && captured.value.caller?.beatId).toBe("original")

      const cyclic: Record<string, unknown> = {
        prompt: "x", references: [refA], durationSeconds: 7, metadata: {}
      }
      ;(cyclic.metadata as Record<string, unknown>).self = cyclic.metadata
      const throwing = {
        get prompt(): string { throw new Error("getter exploded") },
        references: [refA], durationSeconds: 7, metadata: {}
      }
      for (const invalid of [null, cyclic, throwing]) {
        const result = yield* Effect.result(session.engine.prepare(invalid as never))
        expect(Result.isFailure(result) && result.failure._tag === "InvalidRequest").toBe(true)
      }
      expect((yield* fake.calls).filter((call) => call.command === "enqueue")).toHaveLength(1)
    })))

  test("a terminal disconnect fails the session once, stops later commands, and teardown still attempts remote cleanup", () =>
    run(Effect.gen(function*() {
      const fake = yield* makeFake()
      const events = yield* Ref.make<ReadonlyArray<EngineEvent>>([])
      const exit = yield* Effect.exit(Effect.scoped(Effect.gen(function*() {
        const session = yield* make(fake.transport, options)
        yield* session.engine.events.pipe(Stream.runForEach((e) => Ref.update(events, (es) => [...es, e])), Effect.forkScoped)
        yield* Effect.sleep(5) // the subscriber must be attached before anything is published
        yield* session.engine.enqueue(request())
        yield* fake.emitControl({ _tag: "Status", status: "disconnected" })
        yield* fake.failControl(new ReactorError({ code: "Closed", message: "gone" }))
        const failure = yield* session.engine.failure
        expect(failure._tag).toBe("SessionFailed")
        const later = yield* Effect.result(session.engine.enqueue(request()))
        expect(Result.isFailure(later) && later.failure._tag === "SessionFailed").toBe(true)
        // let the event subscriber drain before the scope closes
        for (let i = 0; i < 50 && !tags(yield* Ref.get(events)).includes("SessionFailed"); i++) yield* Effect.sleep(10)
      })))
      expect(Exit.isSuccess(exit)).toBe(true)
      expect(tags(yield* Ref.get(events)).filter((t) => t === "SessionFailed").length).toBe(1)
      const calls = yield* fake.calls
      expect(calls.some((c) => c.command === "reset")).toBe(true)
      expect(yield* fake.disconnects).toBe(1)
    })))

  test("a stalled reset cannot mask its cleanup deadline or prevent disconnect", async () => {
    const fake = await Effect.runPromise(makeFake({ send: { reset: () => Effect.never } }))
    let session: ReactorSession | undefined
    await run(Effect.gen(function* () { session = yield* make(fake.transport, options) }))
    const cleanup = await Effect.runPromise(session!.cleanup)
    expect(Option.getOrThrow(cleanup).steps.map(({ step, ok }) => ({ step, ok }))).toEqual([
      { step: "reset", ok: false }, { step: "disconnect", ok: true },
    ])
    expect(await Effect.runPromise(fake.disconnects)).toBe(1)
  }, 8000)

  test("a malformed enqueue reply is uncertain and retires that session", () =>
    run(Effect.gen(function*() {
      const script: FakeScript = {
        send: { enqueue: () => Effect.succeed({ type: "clip_queued", data: { frames: 175 } }) }
      }
      const { session } = yield* harness(script)
      const r = yield* Effect.result(session.engine.enqueue(request()))
      expect(Result.isFailure(r) && r.failure._tag === "Uncertain").toBe(true)
      expect((yield* session.engine.failure)._tag).toBe("SessionFailed")
    })))

  test("a malformed lifecycle broadcast is a session failure", () =>
    run(Effect.gen(function*() {
      const { fake, session, settle } = yield* harness()
      yield* session.engine.enqueue(request())
      yield* fake.emit({ type: "clip_generated", data: { ready: true } })
      yield* settle
      expect((yield* session.engine.failure)._tag).toBe("SessionFailed")
    })))

  test("an incompatible deployment is refused at setup with the missing names", () =>
    run(Effect.gen(function*() {
      const fake = yield* makeFake({ schema: { openapi: "3.1.0", "x-commands": ["enqueue", "pop", "set_autoplay", "set_canvas"] } })
      const r = yield* Effect.result(make(fake.transport, options))
      expect(Result.isFailure(r)).toBe(true)
      if (Result.isFailure(r)) {
        expect(r.failure.reason).toContain("reference_images")
        expect(r.failure.reason).toContain("clip_queued")
      }
    })))
})

describe("Reactor adapter: removal, canvas, interruption", () => {
  test("removal reports where the clip was; unknown ids are NotFound and an uncorrelated broadcast refusal is Uncertain", () =>
    run(Effect.gen(function*() {
      let refuseNext = false
      const script: FakeScript = {
        send: {
          pop: (args, { fake, defaults }) =>
            refuseNext ? fake.refuse("pop", "clip is the source of a queued continuation").pipe(Effect.as(undefined)) : defaults("pop", args)
        }
      }
      const { fake, session, settle } = yield* harness(script)
      const a = yield* session.engine.enqueue(request())
      const b = yield* session.engine.enqueue(request())
      const c = yield* session.engine.enqueue(request())
      yield* settle
      expect(yield* session.engine.remove(ClipId.make(b))).toBe("unstarted")
      refuseNext = true
      const refused = yield* Effect.result(session.engine.remove(ClipId.make(c)))
      expect(Result.isFailure(refused) && refused.failure._tag === "Uncertain").toBe(true)
      refuseNext = false
      expect(yield* session.engine.remove(ClipId.make(a))).toBe("in_flight")
      // "a running build finishes but its result is discarded": the builder stays busy with A
      let state = yield* session.engine.state
      expect(Option.map(state.building, (b) => b.record.clipId)).toEqual(Option.some(ClipId.make(a)))
      expect(state.queued.map((r) => r.clipId)).toEqual([ClipId.make(c)])
      const [clipA, , clipC] = yield* fake.accepted
      // the provider's queue no longer lists A, but that does not free the GPU
      yield* fake.emit({ type: "queue_update", data: { generation: [{ ...clipC! }], playout: [], history: [] } })
      yield* settle
      expect(Option.map((yield* session.engine.state).building, (b) => b.record.clipId)).toEqual(Option.some(ClipId.make(a)))
      // if the discarded build's completion is announced anyway, it is not a ready clip
      yield* fake.emit(fake.clipMessage("clip_generated", { ...clipA!, ready: true }))
      yield* settle
      state = yield* session.engine.state
      expect(state.ready.length).toBe(0)
      expect(Option.map(state.building, (b) => b.record.clipId)).toEqual(Option.some(ClipId.make(c)))
      expect(state.queued.length).toBe(0)
      const unknown = yield* Effect.result(session.engine.remove(ClipId.make("00000000-0000-4000-8000-000000000000")))
      expect(Result.isFailure(unknown) && unknown.failure._tag === "NotFound").toBe(true)
    })))

  test("canvas changes need an idle session and a provider acknowledgement", () =>
    run(Effect.gen(function*() {
      let refuse = false
      const script: FakeScript = {
        send: { set_canvas: (args, { fake, defaults }) => refuse ? fake.refuse("set_canvas", "queue is not empty or a clip is playing").pipe(Effect.as(undefined)) : defaults("set_canvas", args) }
      }
      const { fake, session } = yield* harness(script)
      yield* session.engine.setCanvas("9:16")
      expect(Option.getOrUndefined((yield* session.engine.state).canvas)).toBe("9:16")
      refuse = true
      const refused = yield* Effect.result(session.engine.setCanvas("1:1"))
      expect(Result.isFailure(refused) && refused.failure._tag === "Uncertain").toBe(true)
      refuse = false
      yield* session.engine.enqueue(request())
      const before = (yield* fake.calls).length
      const busy = yield* Effect.result(session.engine.setCanvas("4:3"))
      expect(Result.isFailure(busy) && busy.failure._tag === "Busy").toBe(true)
      expect((yield* fake.calls).length).toBe(before) // refused locally, nothing sent
    })))

  test("lost or malformed mutation acknowledgements require positive state evidence", () =>
    run(Effect.gen(function* () {
      let canvas = "16:9"
      let canvasMode: "silent-applied" | "malformed-applied" | "silent-unapplied" = "silent-applied"
      let autoplayMode: "silent-applied" | "malformed-applied" | "silent-unapplied" = "silent-applied"
      const script: FakeScript = {
        send: {
          set_canvas: (args) => {
            if (canvasMode !== "silent-unapplied") canvas = String(args.aspect)
            return canvasMode === "malformed-applied"
              ? Effect.succeed({ type: "canvas_accepted", data: "malformed" })
              : Effect.succeed(undefined)
          },
          set_autoplay: (args, { defaults }) =>
            autoplayMode === "silent-unapplied"
              ? Effect.succeed(undefined)
              : defaults("set_autoplay", args).pipe(Effect.map(() =>
                autoplayMode === "malformed-applied" ? { type: "autoplay_accepted", data: "malformed" } : undefined)),
          get_state: (_args, { fake, defaults }) =>
            canvas === "16:9" ? defaults("get_state", {}) : Effect.succeed(fake.stateUpdate({ aspect: canvas }))
        }
      }
      const { session } = yield* harness(script)

      yield* session.engine.setCanvas("9:16")
      expect(Option.getOrUndefined((yield* session.engine.state).canvas)).toBe("9:16")
      canvasMode = "malformed-applied"
      yield* session.engine.setCanvas("4:3")
      expect(Option.getOrUndefined((yield* session.engine.state).canvas)).toBe("4:3")
      canvasMode = "silent-unapplied"
      const canvasUnknown = yield* Effect.result(session.engine.setCanvas("1:1"))
      expect(Result.isFailure(canvasUnknown) && canvasUnknown.failure._tag === "Uncertain").toBe(true)

      yield* session.engine.setAutoplay(false)
      autoplayMode = "malformed-applied"
      yield* session.engine.setAutoplay(true)
      autoplayMode = "silent-unapplied"
      const autoplayUnknown = yield* Effect.result(session.engine.setAutoplay(false))
      expect(Result.isFailure(autoplayUnknown) && autoplayUnknown.failure._tag === "Uncertain").toBe(true)
    })))

  test("pop and move can reconcile a lost or malformed acknowledgement from the authoritative queue", () =>
    run(Effect.gen(function* () {
      let popLost = false
      let moveMalformed = false
      const script: FakeScript = {
        send: {
          pop: (args, { defaults }) => defaults("pop", args).pipe(Effect.map((reply) => popLost ? undefined : reply)),
          move: (args, { defaults }) => defaults("move", args).pipe(Effect.map((reply) =>
            moveMalformed ? { type: "clip_moved", data: "malformed" } : reply))
        }
      }
      const { session, settle } = yield* harness(script)
      const a = yield* session.engine.enqueue(request())
      const b = yield* session.engine.enqueue(request())
      yield* settle
      popLost = true
      expect(yield* session.engine.remove(b)).toBe("unstarted")
      const c = yield* session.engine.enqueue(request())
      yield* settle
      moveMalformed = true
      yield* session.engine.move(c, 0, "generation")
      expect((yield* session.engine.state).queued[0]?.clipId).toBe(c)
      expect(a).not.toBe(c)
    })))

  test("a delayed uncorrelated command_error cannot turn a later mutation into a definitive refusal", () =>
    run(Effect.gen(function* () {
      let calls = 0
      let canvas = "16:9"
      const { session } = yield* harness({
        send: {
          set_canvas: (args, { fake, defaults }) => Effect.gen(function* () {
            calls++
            canvas = String(args.aspect)
            if (calls === 1) return yield* defaults("set_canvas", args)
            yield* fake.refuse("set_canvas", "late error from an older command")
            return undefined
          }),
          get_state: (_args, { fake }) => Effect.succeed(fake.stateUpdate({ aspect: canvas }))
        }
      })
      yield* session.engine.setCanvas("9:16")
      yield* session.engine.setCanvas("4:3")
      expect(Option.getOrUndefined((yield* session.engine.state).canvas)).toBe("4:3")
    })))

  test("interruption during connect, upload and enqueue releases the session without stray work", () =>
    run(Effect.gen(function*() {
      // connect never completes
      const hold = yield* gate()
      const fakeConnect = yield* makeFake({ connect: () => hold.wait })
      const connecting = yield* Effect.forkScoped(Effect.scoped(make(fakeConnect.transport, options)))
      yield* Effect.sleep(20)
      yield* Fiber.interrupt(connecting)
      expect(yield* fakeConnect.disconnects).toBe(0) // never connected: nothing remote to tear down

      // upload never completes
      const holdUpload = yield* gate()
      const { fake, session } = yield* harness({ upload: () => holdUpload.wait.pipe(Effect.map(() => ({ upload_id: "00000000-0000-4000-8000-0000000000aa", name: "x", mime_type: "image/png", size: 1 }))) })
      const uploading = yield* Effect.forkScoped(session.engine.enqueue(request()))
      yield* Effect.sleep(20)
      yield* Fiber.interrupt(uploading)
      expect((yield* fake.calls).filter((c) => c.command === "enqueue").length).toBe(0)

      // enqueue reply never completes; a broadcast for it afterwards is ignored safely
      const holdSend = yield* gate()
      const late = yield* makeFake({ send: { enqueue: () => holdSend.wait.pipe(Effect.map(() => undefined)) } })
      const lateSession = yield* make(late.transport, options)
      const sending = yield* Effect.forkScoped(lateSession.engine.enqueue(request()))
      yield* Effect.sleep(20)
      yield* Fiber.interrupt(sending)
      yield* late.emit({ type: "clip_generated", data: { clip_id: "00000000-0000-4000-8000-0000000000bb", frames: 175, metadata: JSON.stringify({ v: 1, token: "t1", caller: { beatId: "b", segmentId: "s" } }) } })
      yield* Effect.sleep(20)
      const state = yield* lateSession.engine.state
      expect(state.queued.length + state.ready.length).toBe(0)
      expect(Option.isNone(state.building)).toBe(true)
    })))

  test("caller cancellation after commit does not cancel or resend a late accepted enqueue", () =>
    run(Effect.gen(function* () {
      const held = yield* gate()
      const { fake, session, settle } = yield* harness({
        send: {
          enqueue: (args, { defaults, uploads }) => Effect.gen(function* () {
            yield* defaults("enqueue", args, uploads)
            yield* held.wait
            return undefined
          })
        }
      }, { reconcileWindowMs: 100 })
      const submitted = yield* session.engine.enqueue(request()).pipe(Effect.forkScoped)
      yield* Effect.sleep(20)
      const accepted = (yield* fake.accepted)[0]!
      yield* Fiber.interrupt(submitted)
      yield* fake.emit(fake.clipMessage("clip_queued", accepted))
      yield* held.release
      yield* settle
      const state = yield* session.engine.state
      expect([
        ...state.queued,
        ...state.ready,
        ...Option.toArray(Option.map(state.building, (building) => building.record))
      ].map((record) => record.clipId)).toContain(ClipId.make(accepted.clip_id))
      expect((yield* fake.calls).filter((call) => call.command === "enqueue")).toHaveLength(1)
    })))
})


test("session death during enqueue reconciliation preserves uncertainty", () => run(Effect.gen(function* () {
  const { session, fake } = yield* harness({ send: { enqueue: (_args, { fake }) =>
    fake.emitControl({ _tag: "Error", error: { code: "Closed", message: "provider died", recoverable: false } }).pipe(Effect.as(undefined)) } })
  const outcome = yield* Effect.result(session.engine.enqueue(request()))
  expect(outcome._tag === "Failure" && outcome.failure._tag).toBe("Uncertain")
  expect((yield* fake.calls).filter((call) => call.command === "enqueue")).toHaveLength(1)
})))

test("an accepted clip without its duration is uncertain and retires the session", () => run(Effect.gen(function* () {
  const { session } = yield* harness({ send: { enqueue: () => Effect.succeed({ type: "clip_queued", data: { clip: { clip_id: "accepted-without-length" } } }) } })
  const outcome = yield* Effect.result(session.engine.enqueue(request()))
  expect(outcome._tag === "Failure" && outcome.failure._tag).toBe("Uncertain")
})))

test("a slow lifecycle observer fails with Overflow without blocking the session reducer", () => run(Effect.gen(function* () {
  const { fake, session, settle } = yield* harness()
  const hold = yield* gate()
  let blocked = false
  const observer = yield* session.engine.events.pipe(Stream.runForEach(() => {
    if (blocked) return Effect.void
    blocked = true
    return hold.wait
  }), Effect.forkScoped)

  for (let index = 0; index < 55; index++) {
    yield* session.engine.enqueue(request({ metadata: { index } }))
    const clip = (yield* fake.accepted).at(-1)!
    yield* fake.emit(fake.clipMessage("clip_generated", { ...clip, ready: true }))
    yield* fake.emit(fake.clipMessage("clip_started", { ...clip, ready: true }))
    yield* fake.emit(fake.clipMessage("clip_finished", { ...clip, ready: true }))
    yield* settle
  }

  yield* hold.release
  const overflow = yield* Effect.result(Fiber.join(observer).pipe(Effect.timeout("2 seconds")))
  expect(Result.isFailure(overflow) && overflow.failure instanceof ReactorError && overflow.failure.code).toBe("Overflow")
  expect(typeof (yield* session.engine.enqueue(request({ metadata: { after: "observer-overflow" } })))).toBe("string")
})))

test("uncorrelated lifecycle retention is bounded and fails the session instead of growing forever", () => run(Effect.gen(function* () {
  const { fake, session } = yield* harness()
  for (let index = 0; index <= 256; index++) {
    const suffix = index.toString().padStart(12, "0")
    yield* fake.emit({ type: "clip_started", data: { clip: { clip_id: `00000000-0000-4000-8000-${suffix}`, frames: 124 } } })
  }
  const failure = yield* session.engine.failure.pipe(Effect.timeout("2 seconds"))
  expect(failure.reason).toContain("uncorrelated lifecycle observations exceeded")
})))
