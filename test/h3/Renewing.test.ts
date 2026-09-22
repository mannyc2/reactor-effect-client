import { expect, test } from "bun:test"
import { Effect, Fiber, Option, Queue, Stream } from "effect"
import { TestClock } from "effect/testing"
import { ReactorError, type Snapshot, type VideoFrame } from "../../src/Model.js"
import { ClipRequest } from "../../src/Clip.js"
import { make as makeSession } from "../../src/engine/Session.js"
import * as Renewing from "../../src/engine/Renewing.js"
import type { EngineEvent } from "../../src/engine/Engine.js"
import { gate, makeFake, nativeRejection, type Fake } from "./FakeTransport.js"
import { dataUri, pngBytes } from "../../testing/Png.js"
import * as TestPlatform from "../../testing/Platform.js"

const until = (ready: () => boolean, advance: Effect.Effect<void> = Effect.void, label = "Coordinator did not settle") => Effect.gen(function* () {
  for (let n = 0; !ready(); n++) {
    if (n > 1000) return yield* Effect.die(label)
    yield* advance
    yield* Effect.promise(() => new Promise<void>((resolve) => setTimeout(resolve, 1)))
  }
})
const request = (
  sequenceId: string,
  final = true,
  memberId = `${sequenceId}-${final ? "final" : "open"}`,
  prompt = "A host at a desk speaks."
) => new ClipRequest({
  prompt,
  references: [{ uri: dataUri(pngBytes(8, 8)) }],
  durationSeconds: 6,
  metadata: { fixture: sequenceId },
  sequence: { id: sequenceId, memberId, final }
})
const frame: VideoFrame = { _tag: "VideoFrame", track: "video", width: 1, height: 1, data: new Uint8Array(4), frameId: 0n, timestampMicros: 0n, metadata: new Uint8Array() }

test("failed or interrupted renewal acquisition immediately closes its owned session scope", async () => {
  let closed = 0, entered = false
  await Effect.runPromise(Effect.scoped(Effect.gen(function* () {
    const rejected = yield* Effect.result(Renewing.make({ open: Effect.gen(function* () {
      yield* Effect.addFinalizer(() => Effect.sync(() => { closed++ }))
      const fake = yield* makeFake({ send: { set_autoplay: (args, { defaults }) => args.enabled === false
        ? Effect.fail(nativeRejection("cannot_pause", "cannot pause")) : defaults("set_autoplay", args) } })
      return { session: yield* makeSession(fake.transport, { verifyDeployment: false }), maxSeconds: 90 }
    }) }))
    expect(rejected._tag).toBe("Failure")
    expect(closed).toBe(1)
    const pending = yield* Renewing.make({ open: Effect.gen(function* () {
      yield* Effect.addFinalizer(() => Effect.sync(() => { closed++ }))
      entered = true
      return yield* Effect.never
    }) }).pipe(Effect.forkScoped)
    yield* until(() => entered)
    yield* Fiber.interrupt(pending)
    expect(closed).toBe(2)
  }).pipe(Effect.provide(TestPlatform.layer))))
})

test("renewal keeps an open sequence on one session, drains every old frame, then switches after the explicit final member", async () => {
  const opened: Array<{ fake: Fake; video: Queue.Queue<VideoFrame>; closed: boolean }> = []
  const messages: string[] = [], received: number[] = [], renewals: Renewing.Renewal[] = []
  const open = Effect.gen(function* () {
    const fake = yield* makeFake(), video = yield* Queue.unbounded<VideoFrame>()
    const entry = { fake, video, closed: false }; opened.push(entry)
    const session = yield* makeSession({ ...fake.transport, video: Stream.fromQueue(video) }, { verifyDeployment: false })
    yield* Effect.addFinalizer(() => Effect.sync(() => { entry.closed = true }))
    return { session, maxSeconds: 90 }
  })
  await Effect.runPromise(Effect.scoped(Effect.gen(function* () {
    const session = yield* Renewing.make({ open, leadSeconds: 30, log: (s) => messages.push(s), onRenewal: (event) => Effect.sync(() => { renewals.push(event) }) })
    yield* session.media.video.pipe(Stream.runForEach((f) => Effect.sync(() => { received.push(f.data[0]!) })), Effect.forkScoped)
    yield* session.engine.enqueue(request("old", false))
    yield* TestClock.adjust(60_100)
    // Opening a session and the coordinator's next poll are independent. Keep
    // advancing the test clock until both settle instead of freezing between them.
    yield* until(() => opened.length === 2, TestClock.adjust(100))
    yield* until(() => messages.includes("Prepared the next session for renewal"), TestClock.adjust(100))
    yield* session.engine.enqueue(request("old", true))
    yield* session.engine.enqueue(request("next"))
    expect((yield* opened[0]!.fake.accepted)).toHaveLength(2)
    expect((yield* opened[1]!.fake.accepted)).toHaveLength(1)
    const old = yield* opened[0]!.fake.accepted, future = (yield* opened[1]!.fake.accepted)[0]!
    yield* opened[1]!.fake.emit(opened[1]!.fake.clipMessage("clip_generated", future))
    for (const clip of old) {
      yield* opened[0]!.fake.emit(opened[0]!.fake.clipMessage("clip_started", clip))
      // Let control establish ownership before its track frames arrive.
      yield* Effect.promise(() => new Promise<void>((r) => setTimeout(r, 5)))
      for (let n = 0; n < clip.frames; n++) { yield* Queue.offer(opened[0]!.video, { ...frame, data: new Uint8Array([1, 0, 0, 0]) }); yield* Effect.yieldNow }
      yield* opened[0]!.fake.emit(opened[0]!.fake.clipMessage("clip_finished", clip))
    }
    yield* until(() => opened[0]!.closed, TestClock.adjust(100))
    expect(messages).toContain("Switched prepared sessions at a sequence boundary")
    expect(received).toHaveLength(old.reduce((n, c) => n + c.frames, 0))
    expect((yield* opened[1]!.fake.calls).some((call) => call.command === "set_autoplay" && call.args.enabled === true)).toBe(true)
    yield* Queue.offer(opened[1]!.video, { ...frame, data: new Uint8Array([2, 0, 0, 0]) })
    yield* until(() => received.at(-1) === 2)
  }).pipe(Effect.provide(TestPlatform.layer), Effect.provide(TestClock.layer()))))
  expect(opened.every((entry) => entry.closed)).toBe(true)
  // A planned renewal proves the expected video count and reports the narrower
  // audio-tail evidence truthfully; this layer has no audio end marker.
  expect(renewals.filter((event) => event._tag === "Opened")).toHaveLength(2)
  expect(renewals.some((event) => event._tag === "Prepared")).toBe(true)
  const switched = renewals.find((event) => event._tag === "Switched")
  expect(switched).toBeDefined()
  expect(switched!.tail.video.receivedFrames).toBe(switched!.tail.video.expectedFrames)
  expect(switched!.tail.video.status).toBe("count-complete")
  expect(switched!.tail.audio.status).toBe("unverified")
  expect(switched!.tail.sourceDrops).toEqual({ video: 0n, audio: 0n })
})

test("a prepared member snapshots input but chooses the live physical session when it is submitted after renewal", async () => {
  const opened: Array<{ fake: Fake; closed: boolean }> = []
  const renewals: Renewing.Renewal[] = []
  await Effect.runPromise(Effect.scoped(Effect.gen(function* () {
    const session = yield* Renewing.make({
      open: Effect.gen(function* () {
        const fake = yield* makeFake()
        const entry = { fake, closed: false }
        opened.push(entry)
        yield* Effect.addFinalizer(() => Effect.sync(() => { entry.closed = true }))
        return { session: yield* makeSession(fake.transport, { verifyDeployment: false }), maxSeconds: 90 }
      }),
      leadSeconds: 30,
      onRenewal: (event) => Effect.sync(() => { renewals.push(event) })
    })

    const input = request("stale", true, "stale-final", "prepared-before-renewal")
    const prepared = yield* session.engine.prepare(input)
    ;(input as { prompt: string }).prompt = "caller-mutated-after-prepare"
    expect((yield* opened[0]!.fake.calls).filter((call) => call.command === "enqueue")).toHaveLength(0)

    yield* TestClock.adjust(60_100)
    yield* until(() => opened.length === 2, TestClock.adjust(100), "stale test never opened the second session")
    yield* until(() => renewals.some((event) => event._tag === "Prepared"), TestClock.adjust(100), "stale test never prepared the second session")
    const priming = yield* session.engine.enqueue(request("prime", true, "prime-final", "prime-next-session"))
    const nextClip = (yield* opened[1]!.fake.accepted).find((clip) => clip.clip_id === priming)!
    yield* opened[1]!.fake.emit(opened[1]!.fake.clipMessage("clip_generated", nextClip))
    yield* Effect.promise(() => new Promise<void>((resolve) => setTimeout(resolve, 5)))
    yield* until(() => opened[0]!.closed, TestClock.adjust(100), "stale test never switched away from the first session")

    const stale = yield* prepared.submit
    expect((yield* opened[0]!.fake.accepted)).toHaveLength(0)
    const accepted = yield* opened[1]!.fake.accepted
    expect(accepted.map((clip) => clip.clip_id)).toContain(stale)
    expect(accepted.find((clip) => clip.clip_id === stale)?.prompt).toBe("prepared-before-renewal")
  }).pipe(Effect.provide(TestPlatform.layer), Effect.provide(TestClock.layer()))))
})

test("concurrently submitted prepared members bind before dispatch and remain on one physical session", async () => {
  await Effect.runPromise(Effect.scoped(Effect.gen(function* () {
    const firstDispatch = yield* gate()
    let firstSent = false
    const fake = yield* makeFake({ send: {
      enqueue: (args, { defaults, uploads }) => args.prompt === "first-member"
        ? Effect.sync(() => { firstSent = true }).pipe(
            Effect.andThen(firstDispatch.wait),
            Effect.andThen(defaults("enqueue", args, uploads))
          )
        : defaults("enqueue", args, uploads)
    } })
    const session = yield* Renewing.make({
      open: Effect.succeed({ session: yield* makeSession(fake.transport, { verifyDeployment: false }), maxSeconds: 90 })
    })
    const first = yield* session.engine.prepare(request("concurrent", false, "first", "first-member"))
    const final = yield* session.engine.prepare(request("concurrent", true, "final", "final-member"))

    const firstFiber = yield* first.submit.pipe(Effect.forkScoped)
    yield* until(() => firstSent)
    const finalFiber = yield* final.submit.pipe(Effect.forkScoped)
    yield* Effect.yieldNow
    expect((yield* fake.calls).filter((call) => call.command === "enqueue")).toHaveLength(1)
    yield* firstDispatch.release
    yield* Fiber.join(firstFiber)
    yield* Fiber.join(finalFiber)

    expect((yield* fake.accepted).map((clip) => clip.prompt)).toEqual(["first-member", "final-member"])
    const late = yield* Effect.result(session.engine.enqueue(request("concurrent", false, "late", "late-member")))
    expect(late._tag === "Failure" && late.failure._tag === "Rejected" && late.failure.code).toBe("sequence_sealed")
  }).pipe(Effect.provide(TestPlatform.layer))))
})

test("cancelling renewing submission during reference prework never dispatches later and remains retryable", async () => {
  await Effect.runPromise(Effect.scoped(Effect.gen(function* () {
    const heldUpload = yield* gate()
    let uploadStarted = false
    const fake = yield* makeFake({
      upload: () => Effect.sync(() => { uploadStarted = true }).pipe(
        Effect.andThen(heldUpload.wait),
        Effect.as({ upload_id: "00000000-0000-4000-8000-0000000000ca", name: "cancel.png", mime_type: "image/png", size: 1 })
      )
    })
    const session = yield* Renewing.make({
      open: Effect.succeed({ session: yield* makeSession(fake.transport, { verifyDeployment: false }), maxSeconds: 90 })
    })
    const prepared = yield* session.engine.prepare(request("cancel-prework", true, "final", "cancel-prework"))

    const submitting = yield* prepared.submit.pipe(Effect.forkScoped)
    yield* until(() => uploadStarted)
    yield* Fiber.interrupt(submitting)
    yield* heldUpload.release
    yield* Effect.sleep(20)
    expect((yield* fake.calls).filter((call) => call.command === "enqueue")).toHaveLength(0)
    expect((yield* prepared.state)._tag).toBe("Prepared")

    const clipId = yield* prepared.submit
    expect((yield* fake.calls).filter((call) => call.command === "enqueue")).toHaveLength(1)
    expect((yield* fake.accepted).map((clip) => clip.clip_id)).toContain(clipId)
  }).pipe(Effect.provide(TestPlatform.layer))))
})

test("caller cancellation after renewing commit leaves session-owned outcome accounting intact", async () => {
  await Effect.runPromise(Effect.scoped(Effect.gen(function* () {
    const heldReply = yield* gate()
    const fake = yield* makeFake({ send: {
      enqueue: (args, { defaults, uploads }) => defaults("enqueue", args, uploads).pipe(
        Effect.flatMap((reply) => heldReply.wait.pipe(Effect.as(reply)))
      )
    } })
    const session = yield* Renewing.make({
      open: Effect.succeed({ session: yield* makeSession(fake.transport, { verifyDeployment: false }), maxSeconds: 90 })
    })
    const prepared = yield* session.engine.prepare(request("cancel-after-commit", true, "final", "committed-member"))
    const caller = yield* prepared.submit.pipe(Effect.forkScoped)
    for (let n = 0; n < 1000 && (yield* fake.accepted).length === 0; n++) yield* Effect.sleep(1)
    expect((yield* fake.accepted)).toHaveLength(1)
    yield* Fiber.interrupt(caller)
    yield* heldReply.release
    for (let n = 0; n < 1000 && (yield* prepared.state)._tag !== "Completed"; n++) yield* Effect.sleep(1)

    expect((yield* prepared.state)._tag).toBe("Completed")
    expect((yield* fake.calls).filter((call) => call.command === "enqueue")).toHaveLength(1)
    const late = yield* Effect.result(session.engine.enqueue(request("cancel-after-commit", false, "late", "late-member")))
    expect(late._tag === "Failure" && late.failure._tag === "Rejected" && late.failure.code).toBe("sequence_sealed")
  }).pipe(Effect.provide(TestPlatform.layer))))
})

test("a definitively rejected final member seals its sequence and releases the renewal boundary", async () => {
  const opened: Array<{ fake: Fake; closed: boolean }> = []
  const renewals: Renewing.Renewal[] = []
  await Effect.runPromise(Effect.scoped(Effect.gen(function* () {
    const session = yield* Renewing.make({
      open: Effect.gen(function* () {
        const fake = yield* makeFake({ send: {
          enqueue: (args, { defaults, uploads }) => args.prompt === "reject-final"
            ? Effect.fail(nativeRejection("QUEUE_FULL", "generation queue is full", 429))
            : defaults("enqueue", args, uploads)
        } })
        const entry = { fake, closed: false }
        opened.push(entry)
        yield* Effect.addFinalizer(() => Effect.sync(() => { entry.closed = true }))
        return { session: yield* makeSession(fake.transport, { verifyDeployment: false }), maxSeconds: 90 }
      }),
      leadSeconds: 30,
      onRenewal: (event) => Effect.sync(() => { renewals.push(event) })
    })

    const rejected = yield* Effect.result(session.engine.enqueue(request("rejected-final", true, "final", "reject-final")))
    expect(rejected._tag === "Failure" && rejected.failure._tag === "Rejected" && rejected.failure.code).toBe("QUEUE_FULL")
    const late = yield* Effect.result(session.engine.enqueue(request("rejected-final", false, "late", "late-member")))
    expect(late._tag === "Failure" && late.failure._tag === "Rejected" && late.failure.code).toBe("sequence_sealed")

    yield* TestClock.adjust(60_100)
    yield* until(() => opened.length === 2, TestClock.adjust(100), "rejected-final test never opened the second session")
    yield* until(() => renewals.some((event) => event._tag === "Prepared"), TestClock.adjust(100), "rejected-final test never prepared the second session")
    const next = yield* session.engine.enqueue(request("after-rejection", true, "next", "next-session"))
    const nextClip = (yield* opened[1]!.fake.accepted).find((clip) => clip.clip_id === next)!
    yield* opened[1]!.fake.emit(opened[1]!.fake.clipMessage("clip_generated", nextClip))
    yield* Effect.promise(() => new Promise<void>((resolve) => setTimeout(resolve, 5)))
    yield* until(() => opened[0]!.closed, TestClock.adjust(100), "rejected-final test never switched away from the first session")
    expect((yield* opened[0]!.fake.accepted)).toHaveLength(0)
  }).pipe(Effect.provide(TestPlatform.layer), Effect.provide(TestClock.layer()))))
})

test("known source drops fail planned renewal instead of claiming a clean switch", async () => {
  const opened: Fake[] = [], renewals: Renewing.Renewal[] = []
  let dropped = false
  const clean: Snapshot = { closed: false, queuedControl: 0, queuedVideo: 0, queuedAudio: 0, queuedBytes: 0,
    droppedVideo: 0n, droppedAudio: 0n, pendingRequests: 0, deliveredVideo: 0n, deliveredAudio: 0n }
  const open = Effect.gen(function* () {
    const fake = yield* makeFake(), video = yield* Queue.unbounded<VideoFrame>()
    const index = opened.length
    opened.push(fake)
    const session = yield* makeSession({
      ...fake.transport,
      video: Stream.fromQueue(video),
      snapshot: index === 0 ? Effect.sync(() => ({ ...clean, droppedVideo: dropped ? 1n : 0n })) : fake.transport.snapshot
    }, { verifyDeployment: false })
    return { session, maxSeconds: 90, video }
  })
  await Effect.runPromise(Effect.scoped(Effect.gen(function* () {
    const sources: Array<Queue.Queue<VideoFrame>> = []
    const session = yield* Renewing.make({
      open: open.pipe(Effect.tap(({ video }) => Effect.sync(() => { sources.push(video) }))),
      leadSeconds: 30,
      onRenewal: (event) => Effect.sync(() => { renewals.push(event) })
    })
    yield* session.media.video.pipe(Stream.runDrain, Effect.forkScoped)
    yield* session.engine.enqueue(request("old"))
    yield* TestClock.adjust(60_100)
    yield* until(() => opened.length === 2, TestClock.adjust(100))
    yield* until(() => renewals.some((event) => event._tag === "Prepared"), TestClock.adjust(100))
    const next = yield* session.engine.enqueue(request("next"))
    const future = (yield* opened[1]!.accepted).find((clip) => clip.clip_id === next)!
    yield* opened[1]!.emit(opened[1]!.clipMessage("clip_generated", future))
    const old = (yield* opened[0]!.accepted)[0]!
    yield* opened[0]!.emit(opened[0]!.clipMessage("clip_started", old))
    // Establish Started ownership before media can advance the frame counter.
    yield* Effect.promise(() => new Promise<void>((resolve) => setTimeout(resolve, 5)))
    for (let n = 0; n < old.frames; n++) {
      yield* Queue.offer(sources[0]!, { ...frame, data: new Uint8Array([1, 0, 0, 0]) })
      yield* Effect.yieldNow
    }
    yield* until(() => Queue.sizeUnsafe(sources[0]!) === 0)
    dropped = true
    yield* opened[0]!.emit(opened[0]!.clipMessage("clip_finished", old))
    yield* until(() => renewals.some((event) => event._tag === "Failed" || event._tag === "Switched"), TestClock.adjust(100))
    expect(renewals.some((event) => event._tag === "Switched")).toBe(false)
    const failure = yield* session.engine.failure
    expect(failure.reason).toContain("Session media source reported drops before renewal")
  }).pipe(Effect.provide(TestPlatform.layer), Effect.provide(TestClock.layer()))))
})

test("an unrecoverable session reports its queued clips lost and accepts local work on a replacement", async () => {
  const opened: Fake[] = [], events: EngineEvent[] = [], renewals: Renewing.Renewal[] = []
  const open = Effect.gen(function* () {
    const fake = yield* makeFake({ reconnect: () => Effect.fail(new ReactorError({ code: "Closed", message: "session expired" })) })
    opened.push(fake)
    return { session: yield* makeSession(fake.transport, { verifyDeployment: false }), maxSeconds: 90 }
  })
  await Effect.runPromise(Effect.scoped(Effect.gen(function* () {
    const session = yield* Renewing.make({ open, onRenewal: (event) => Effect.sync(() => { renewals.push(event) }) })
    yield* session.engine.events.pipe(Stream.runForEach((event) => Effect.sync(() => { events.push(event) })), Effect.forkScoped)
    const lost = yield* session.engine.enqueue(request("lost"))
    yield* opened[0]!.emitControl({ _tag: "Status", status: "disconnected" })
    yield* until(() => opened.length === 2)
    const next = yield* session.engine.enqueue(request("replacement"))
    expect(next).not.toBe(lost)
    // Each lost clip names the connection that took it down, so a reader can group
    // the burst under one replacement instead of reading it as unrelated failures.
    const failed = events.flatMap((e) => e._tag === "Failed" ? [e] : [])
    expect(failed.map((e) => e.clipId)).toContain(lost)
    expect(failed.every((e) => e.sessionId === "fixture-session")).toBe(true)
    expect(events.some((e) => e._tag === "SessionFailed")).toBe(false)
    expect(Option.isSome((yield* session.engine.state).building)).toBe(true)
    // The replacement is what a reader needs to group the lost clips under; without
    // it a burst of clip.lost reads as unrelated build failures.
    const replaced = renewals.find((event) => event._tag === "Replaced")
    expect(replaced).toBeDefined()
    expect(replaced!.lostClips).toBe(1)
    expect(replaced!.sessionId).toBe("fixture-session")
    expect(replaced!.reason).toContain("session expired")
  }).pipe(Effect.provide(TestPlatform.layer))))
})

test("late acceptance from a retired session cannot revive or resend an uncertain dispatch", async () => {
  const opened: Fake[] = []
  await Effect.runPromise(Effect.scoped(Effect.gen(function* () {
    const session = yield* Renewing.make({ open: Effect.gen(function* () {
      const fake = yield* makeFake({ send: { enqueue: (args, { defaults }) => defaults("enqueue", args).pipe(Effect.as(undefined)) } })
      opened.push(fake)
      return { session: yield* makeSession(fake.transport, { verifyDeployment: false, reconcileWindowMs: 10 }), maxSeconds: 90 }
    }) })
    const outcome = yield* Effect.result(session.engine.enqueue(request("uncertain")))
    expect(outcome._tag === "Failure" && outcome.failure._tag).toBe("Uncertain")
    const accepted = (yield* opened[0]!.accepted)[0]!
    yield* opened[0]!.emit(opened[0]!.clipMessage("clip_generated", accepted))
    yield* opened[0]!.emit(opened[0]!.clipMessage("clip_started", accepted))
    yield* Effect.sleep(20)
    expect((yield* session.engine.state).ready).toEqual([])
    expect(Option.isNone((yield* session.engine.state).playing)).toBe(true)
    let sends = 0
    for (const fake of opened) sends += (yield* fake.calls).filter(call => call.command === "enqueue").length
    expect(sends).toBe(1)
  }).pipe(Effect.provide(TestPlatform.layer))))
})
