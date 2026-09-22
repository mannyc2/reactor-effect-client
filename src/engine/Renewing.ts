import { Clock, Deferred, Effect, Exit, Fiber, Option, Queue, Schema, Scope, Semaphore, Stream } from "effect"
import { ClipRequest, type ClipId } from "../Clip.js"
import type { AudioFrame, VideoFrame } from "../Model.js"
import * as Sequence from "../Sequence.js"
import * as Submission from "../Submission.js"
import { Observations } from "../observation.js"
import { type ClipEngineShape, EngineEvent, type EngineState, InvalidRequest, isIdle, NotFound, Rejected, SessionFailed } from "./Engine.js"
import type { CleanupReport, ReactorSession } from "./Session.js"

export interface Options<R = never> {
  /** Acquires a fresh session, including a fresh token and explicit remote termination. */
  readonly open: Effect.Effect<{ readonly session: ReactorSession; readonly maxSeconds: number }, unknown, Scope.Scope | R>
  readonly leadSeconds?: number
  readonly log?: (message: string) => void
  /** The same moments as `log`, as data. `log` addresses an operator watching the
   * console; this is what a recording groups lost clips under after the fact. */
  readonly onRenewal?: (event: Renewal) => Effect.Effect<void>
}

/** What became of a physical session. Tail evidence is deliberately narrower
 * than "all A/V delivered": H3 video has a known cadence, while its audio stream
 * exposes neither sender timestamps nor an end-of-clip sample count. */
export type Renewal =
  | { readonly _tag: "Opened"; readonly sessionId: string | undefined; readonly maxSeconds: number }
  | { readonly _tag: "Prepared" }
  | { readonly _tag: "SetupFailed"; readonly reason: string; readonly consecutive: number }
  | { readonly _tag: "Switched"; readonly sessionId: string | undefined; readonly ageSeconds: number; readonly tail: MediaTail }
  | { readonly _tag: "Replaced"; readonly reason: string; readonly lostClips: number; readonly sessionId: string | undefined; readonly ageSeconds: number; readonly tail: MediaTail }
  | { readonly _tag: "Failed"; readonly reason: string }

export interface MediaTail {
  readonly video: {
    readonly framesPerSecond: number
    readonly expectedFrames: number
    readonly receivedFrames: number
    readonly status: "not-started" | "count-complete" | "incomplete"
  }
  /** No H3 audio end marker exists at this layer, so this must remain unverified. */
  readonly audio: { readonly receivedSamples: number; readonly status: "unverified" }
  /** Null means the lower source could no longer provide its pressure snapshot. */
  readonly sourceDrops: { readonly video: bigint | null; readonly audio: bigint | null }
  /** These coordinator-owned buffers survive a physical-session switch. */
  readonly forwarded: { readonly queuedVideoFrames: number; readonly queuedAudioSamples: number }
}

interface Slot {
  readonly session: ReactorSession & { readonly prepareEnqueue: NonNullable<ReactorSession["prepareEnqueue"]> }
  readonly scope: Scope.Closeable
  readonly openedAt: number
  readonly maxSeconds: number
  readonly clips: Set<ClipId>
  readonly sequenceIds: Set<string>
  readonly openSequences: Set<string>
  closed: boolean
  expectedFrames: number
  receivedFrames: number
  receivedAudioSamples: number
}

/** One connection can be replaced without replacing the output clock or the
 * caller's local queue. Renewals prepare a following session while the current
 * session drains; sequence affinity decides when admitting into that session is safe.
 * The switch barrier proves the expected video count and reports narrower audio
 * evidence explicitly; it does not claim viewer delivery or a complete audio tail. */
export const make = <R>(options: Options<R>): Effect.Effect<ReactorSession, unknown, Scope.Scope | R> => Effect.gen(function* () {
  const scope = yield* Effect.scope
  const context = yield* Effect.context<R>()
  const commands = yield* Semaphore.make(1)
  const events = new Observations<EngineEvent>()
  yield* Effect.addFinalizer(() => Effect.sync(() => events.end()))
  const video = yield* Queue.unbounded<VideoFrame>()
  const audio = yield* Queue.unbounded<AudioFrame>()
  const fatal = yield* Deferred.make<SessionFailed>()
  const slots: Slot[] = []
  const cleanupByStep = new Map<string, CleanupReport["steps"][number]>()
  const affinity = yield* Sequence.makeAffinity<Slot>()
  let current: Slot | undefined
  let next: Slot | undefined
  let opening: Fiber.Fiber<Slot, unknown> | undefined
  let retryAt = 0
  let autoplay = true
  let queuedFrames = 0, queuedSamples = 0
  let openFailures = 0
  const emit = (event: EngineEvent) => Effect.sync(() => events.emit(event))
  const fail = (reason: string) => Effect.gen(function* () {
    const failure = new SessionFailed({ reason })
    if (yield* Deferred.succeed(fatal, failure)) {
      yield* observe({ _tag: "Failed", reason })
      yield* emit(EngineEvent.SessionFailed({ failure }))
    }
  })
  const log = (message: string) => Effect.sync(() => options.log?.(message))
  const observe = (event: Renewal) => options.onRenewal?.(event) ?? Effect.void
  /** The retiring session's own numbers and identity, read before its slot is reused or closed. */
  const retiring = (slot: Slot) => Effect.gen(function* () {
    const now = yield* Clock.currentTimeMillis
    const pressure = yield* Effect.exit(slot.session.media.pressure)
    const sourceDrops = Exit.isSuccess(pressure)
      ? { video: pressure.value.droppedVideo, audio: pressure.value.droppedAudio }
      : { video: null, audio: null }
    return {
      sessionId: Option.getOrUndefined(yield* slot.session.sessionId),
      ageSeconds: (now - slot.openedAt) / 1000,
      tail: {
        video: {
          framesPerSecond: slot.session.media.videoFramesPerSecond,
          expectedFrames: slot.expectedFrames,
          receivedFrames: slot.receivedFrames,
          status: slot.expectedFrames === 0 ? "not-started" as const
            : slot.receivedFrames >= slot.expectedFrames ? "count-complete" as const : "incomplete" as const
        },
        audio: { receivedSamples: slot.receivedAudioSamples, status: "unverified" as const },
        sourceDrops,
        forwarded: { queuedVideoFrames: queuedFrames, queuedAudioSamples: queuedSamples }
      }
    }
  })
  const recordCleanup = (report: CleanupReport) => {
    for (const step of report.steps) {
      const previous = cleanupByStep.get(step.step)
      if (previous !== undefined) {
        const detail = previous.detail ?? step.detail
        cleanupByStep.set(step.step, previous.ok && step.ok
          ? previous
          : { step: step.step, ok: false, ...(detail === undefined ? {} : { detail }) })
      } else if (cleanupByStep.size < 64) cleanupByStep.set(step.step, step)
      else cleanupByStep.set("cleanup-report-overflow", {
        step: "cleanup-report-overflow", ok: false, detail: "more than 64 cleanup step names were observed"
      })
    }
  }
  const close = (slot: Slot) => Effect.gen(function* () {
    const index = slots.indexOf(slot)
    if (index < 0) return
    slot.closed = true
    yield* Scope.close(slot.scope, Exit.void)
    const report = yield* slot.session.cleanup
    if (Option.isSome(report)) recordCleanup(report.value)
    slots.splice(index, 1)
  })
  const retireSequences = (slot: Slot) => Effect.gen(function* () {
    yield* affinity.retire(slot)
    for (const id of slot.sequenceIds) {
      const entry = yield* affinity.get(id)
      if (entry !== undefined && entry.status !== "indeterminate" && entry.status !== "open") {
        yield* affinity.release(id).pipe(Effect.catch(() => Effect.void))
      }
    }
  })
  yield* Effect.addFinalizer(() => Effect.forEach([...slots], close, { discard: true }))

  const acquire: Effect.Effect<Slot, unknown> = Effect.gen(function* () {
    const owned = yield* Scope.make()
    let acquired: Slot | undefined
    return yield* Effect.gen(function* () {
      const result = yield* Effect.result(options.open.pipe(
        Effect.provideService(Scope.Scope, owned),
        Effect.provide(context),
        Effect.timeout("30 seconds")
      ))
      if (result._tag === "Failure") return yield* Effect.fail(result.failure)
      if (result.success.session.prepareEnqueue === undefined) {
        return yield* Effect.fail(new Error("renewing sessions require the physical prepareEnqueue composition seam"))
      }
      const slot: Slot = {
        ...result.success,
        session: result.success.session as Slot["session"],
        scope: owned,
        openedAt: yield* Clock.currentTimeMillis,
        clips: new Set(),
        sequenceIds: new Set(),
        openSequences: new Set(),
        closed: false,
        expectedFrames: 0,
        receivedFrames: 0,
        receivedAudioSamples: 0
      }
      acquired = slot
      slots.push(slot)
      yield* slot.session.engine.setAutoplay(false)
      yield* slot.session.engine.events.pipe(Stream.runForEach((event) => Effect.gen(function* () {
        if (slot.closed) return
        if (event._tag === "SessionFailed") {
          yield* Effect.forkIn(Semaphore.withPermit(commands)(replaceFailed(slot, event.failure.reason)), scope)
          return
        }
        if (event._tag === "Started") {
          slot.expectedFrames = Math.round(event.durationSeconds * slot.session.media.videoFramesPerSecond)
          slot.receivedFrames = 0
          slot.receivedAudioSamples = 0
        }
        if (event._tag === "Starved" && slot !== current) return
        yield* emit(event)
      })), Effect.catch((error) => Effect.forkIn(
        Semaphore.withPermit(commands)(replaceFailed(slot, `session lifecycle observation failed: ${error.message}`)),
        scope
      ).pipe(Effect.asVoid)), Effect.forkIn(owned))
      // Receive independently of composition. Overflow is a loss, never a frame
      // silently skipped to catch up with an encoder that cannot sustain its clock.
      yield* slot.session.media.video.pipe(Stream.runForEach((frame) => Effect.gen(function* () {
        if (slot !== current || slot.closed) return
        slot.receivedFrames++
        if (queuedFrames >= 96) { yield* fail("Session video receiver overflow"); return }
        queuedFrames++
        yield* Queue.offer(video, frame)
      })), Effect.catch((error) => log(`session video: ${error.message}`)), Effect.forkIn(owned))
      yield* slot.session.media.audio.pipe(Stream.runForEach((frame) => Effect.gen(function* () {
        if (slot !== current || slot.closed) return
        if (queuedSamples + frame.samples.length > 48_000 * 4) { yield* fail("Session audio receiver overflow"); return }
        slot.receivedAudioSamples += frame.samples.length
        queuedSamples += frame.samples.length
        yield* Queue.offer(audio, frame)
      })), Effect.catch((error) => log(`session audio: ${error.message}`)), Effect.forkIn(owned))
      yield* observe({ _tag: "Opened", sessionId: Option.getOrUndefined(yield* slot.session.sessionId), maxSeconds: slot.maxSeconds })
      return slot
    }).pipe(Effect.onExit((exit) => exit._tag === "Success" ? Effect.void : Effect.gen(function* () {
      // A failed or cancelled renewal still owns a possibly billable session.
      // Close its scope before any retry, including failure to pause autoplay.
      if (acquired !== undefined) yield* close(acquired)
      else yield* Scope.close(owned, exit)
    })))
  })

  const replaceFailed = (slot: Slot, reason: string): Effect.Effect<void> => Effect.gen(function* () {
    if (slot.closed) return
    const state = yield* slot.session.engine.state
    const lost = [...state.queued, ...state.ready,
      ...Option.match(state.building, { onNone: () => [], onSome: (b) => [b.record] }),
      ...Option.match(state.playing, { onNone: () => [], onSome: (p) => [p.record] })]
    slot.closed = true
    const retired = yield* retiring(slot)
    const owner = retired.sessionId === undefined ? {} : { sessionId: retired.sessionId }
    for (const record of lost) yield* emit(EngineEvent.Failed({ clipId: record.clipId, reason: `Session lost: ${reason}`, ...owner }))
    yield* retireSequences(slot)
    yield* close(slot)
    // Recorded whether or not this slot was the live one: the clips are lost either way.
    yield* observe({ _tag: "Replaced", reason, lostClips: lost.length, ...retired })
    if (slot === next) next = undefined
    if (slot !== current) return
    if (next === undefined) { next = yield* (opening === undefined ? acquire : Fiber.join(opening)); opening = undefined }
    const replacement = next
    current = replacement; next = undefined
    yield* replacement.session.engine.setAutoplay(autoplay)
    yield* log("Replaced lost session; local queue resumes on the new connection")
  }).pipe(Effect.catch((error) => fail(`Could not replace session: ${String(error)}`)))

  current = yield* acquire
  const owner = (id: ClipId) => slots.find((slot) => !slot.closed && slot.clips.has(id))
  const guarded = <A, E>(effect: Effect.Effect<A, E>) => Semaphore.withPermit(commands)(Effect.gen(function* () {
    if (yield* Deferred.isDone(fatal)) return yield* Effect.fail(yield* Deferred.await(fatal))
    return yield* effect
  }))

  const state = Effect.gen(function* (): Effect.fn.Return<EngineState> {
    const live = slots.filter((slot) => !slot.closed)
    const values = yield* Effect.forEach(live, (slot) => slot.session.engine.state)
    const first = values[0] ?? (yield* current!.session.engine.state)
    const builds = values.flatMap((s) => Option.match(s.building, { onNone: () => [], onSome: (b) => [b] }))
    // The current slot is normally one of the live ones we just read; asking it
    // again cost an extra state read on every tick and could answer from a
    // slightly later moment than the queues it is reported beside.
    const currentIndex = live.indexOf(current!)
    const playing = currentIndex === -1 ? (yield* current!.session.engine.state).playing : values[currentIndex]!.playing
    return { ...first, queued: [...values.flatMap((s) => s.queued), ...builds.slice(1).map((b) => b.record)], building: Option.fromUndefinedOr(builds[0]),
      ready: values.flatMap((s) => s.ready), playing,
      capacities: { generation: Math.min(...values.map((s) => s.capacities.generation), first.capacities.generation), playout: Math.min(...values.map((s) => s.capacities.playout), first.capacities.playout) } }
  })

  const sequenceFailure = (id: string, reason: Sequence.SequenceError["reason"]) =>
    new Rejected({ command: "enqueue", code: `sequence_${reason}`, reason: `sequence ${id} is ${reason}` })

  const captureRequest = (input: ClipRequest): Effect.Effect<ClipRequest, InvalidRequest> =>
    Effect.try({
      try: () => {
        const text = JSON.stringify(input, (_key, value) => {
          if (value === undefined || typeof value === "function" || typeof value === "symbol" || typeof value === "bigint") {
            throw new TypeError("clip request contains a non-JSON value")
          }
          if (typeof value === "number" && !Number.isFinite(value)) throw new TypeError("clip request contains a non-finite number")
          return value
        })
        if (text === undefined) throw new TypeError("clip request is not serializable")
        return Schema.decodeUnknownSync(ClipRequest)(JSON.parse(text))
      },
      catch: (cause) => new InvalidRequest({
        reason: `clip request is malformed or contains non-JSON/cyclic data: ${cause instanceof Error ? cause.message : String(cause)}`
      })
    })

  let submissionSeq = 0
  const refreshSequenceBoundary = (slot: Slot, id: string) => Effect.gen(function* () {
    const entry = yield* affinity.get(id)
    if (entry !== undefined && entry.status === "open") slot.openSequences.add(id)
    else slot.openSequences.delete(id)
  })

  const prepare = (input: ClipRequest): Effect.Effect<Submission.Submission<ClipId, import("./Engine.js").EnqueueError>, import("./Engine.js").EnqueueError> =>
    Effect.gen(function* () {
      // Preparation snapshots caller input only. The lazy facade below delegates
      // the actual prepare/commit/execute lifetime to the selected physical session.
      const request = yield* captureRequest(input)
      const id = `renewing-${++submissionSeq}`
      const gate = yield* Semaphore.make(1)
      let active: Submission.Submission<ClipId, import("./Engine.js").EnqueueError> | undefined

      const submit = gate.withPermit(guarded(Effect.gen(function* () {
        let child = active
        if (child === undefined) {
          const sequence = request.sequence
          const binding = sequence === undefined ? undefined : yield* affinity.get(sequence.id)
          if (sequence !== undefined && binding !== undefined && (binding.status !== "open" || binding.sealRequested)) {
            const reason: Sequence.SequenceError["reason"] = binding.status === "sealed" ? "sealed"
              : binding.status === "indeterminate" ? "indeterminate"
              : binding.status === "retired" ? "retired" : "sealing"
            return yield* sequenceFailure(sequence.id, reason)
          }
          const bound = binding?.owner
          const anchored = request.before === undefined ? undefined : owner(request.before)
          if (bound !== undefined && anchored !== undefined && bound !== anchored) {
            return yield* new Rejected({ command: "enqueue", code: "sequence_anchor_mismatch", reason: "sequence and before anchor belong to different sessions" })
          }
          const target = bound ?? anchored ?? (next !== undefined && current!.openSequences.size === 0 ? next : current!)
          if (target.closed) return yield* new Rejected({ command: "enqueue", code: "session_recovering", reason: "Waiting for a replacement session" })

          const own = yield* target.session.engine.state
          const position = request.before === undefined ? own.queued.length : own.queued.findIndex((clip) => clip.clipId === request.before)
          child = yield* target.session.prepareEnqueue({ ...request, position: position < 0 ? 0 : position }, {
            commit: (submissionId) => Effect.gen(function* () {
              if (target.closed) return yield* new Rejected({ command: "enqueue", code: "session_recovering", reason: "Waiting for a replacement session" })
              if (sequence === undefined) return
              const existing = yield* affinity.get(sequence.id)
              if (existing !== undefined) {
                if (existing.status !== "open" || existing.sealRequested) {
                  const reason: Sequence.SequenceError["reason"] = existing.status === "sealed" ? "sealed"
                    : existing.status === "indeterminate" ? "indeterminate"
                    : existing.status === "retired" ? "retired" : "sealing"
                  return yield* sequenceFailure(sequence.id, reason)
                }
                if (existing.owner !== target) return yield* sequenceFailure(sequence.id, "owner-mismatch")
              } else {
                yield* affinity.bind(sequence.id, target).pipe(Effect.mapError((error) => sequenceFailure(sequence.id, error.reason)))
              }
              const memberId = sequence.memberId ?? submissionId
              yield* affinity.begin(sequence.id, memberId).pipe(Effect.mapError((error) => sequenceFailure(sequence.id, error.reason)))
              target.sequenceIds.add(sequence.id)
              yield* refreshSequenceBoundary(target, sequence.id)
            }),
            result: (submissionId, result) => Effect.gen(function* () {
              const memberId = sequence?.memberId ?? submissionId
              if (result._tag === "Success") {
                target.clips.add(result.success)
                if (sequence !== undefined) {
                  yield* affinity.accepted(sequence.id, memberId, result.success, sequence.final).pipe(
                    Effect.mapError((error) => sequenceFailure(sequence.id, error.reason))
                  )
                  yield* refreshSequenceBoundary(target, sequence.id)
                }
                return
              }

              if (sequence !== undefined) {
                if (result.failure._tag === "Uncertain") {
                  yield* affinity.uncertain(sequence.id, memberId, result.failure.reason).pipe(
                    Effect.mapError((error) => sequenceFailure(sequence.id, error.reason))
                  )
                } else if (result.failure._tag === "SessionFailed") {
                  yield* affinity.retire(target)
                } else {
                  yield* affinity.rejected(sequence.id, memberId, result.failure.reason, sequence.final).pipe(
                    Effect.mapError((error) => sequenceFailure(sequence.id, error.reason))
                  )
                }
                yield* refreshSequenceBoundary(target, sequence.id)
              }

              if (result.failure._tag === "SessionFailed" || result.failure._tag === "Uncertain") {
                yield* Semaphore.withPermit(commands)(replaceFailed(target, result.failure.reason)).pipe(Effect.forkIn(scope), Effect.asVoid)
              }
            })
          })
          active = child
        }

        const selected = child
        return yield* selected.submit.pipe(
          Effect.onExit(() => selected.state.pipe(Effect.flatMap((state) => Effect.sync(() => {
            if (state._tag === "Prepared" && active === selected) active = undefined
          })))),
          Effect.catchTag("SessionFailed", () => Effect.fail(new Rejected({
            command: "enqueue", code: "session_recovering", reason: "The failed session was retired before retrying local work"
          })))
        )
      })))

      return {
        id,
        submit,
        state: Effect.suspend(() => active === undefined
          ? Effect.succeed({ _tag: "Prepared" } as const)
          : active.state)
      }
    })

  const enqueue = (request: ClipRequest) => prepare(request).pipe(Effect.flatMap((submission) => submission.submit))

  const tick = guarded(Effect.gen(function* () {
    const active = current!
    if (active.closed) return
    const left = active.maxSeconds - ((yield* Clock.currentTimeMillis) - active.openedAt) / 1000
    if (opening !== undefined && opening.pollUnsafe() !== undefined) {
      const result = yield* Effect.result(Fiber.join(opening))
      opening = undefined
      if (result._tag === "Success") { next = result.success; openFailures = 0; yield* log("Prepared the next session for renewal"); yield* observe({ _tag: "Prepared" }) }
      else {
        openFailures++
        retryAt = (yield* Clock.currentTimeMillis) + 5_000
        yield* log(`Renewal setup failed: ${String(result.failure)}`)
        yield* observe({ _tag: "SetupFailed", reason: String(result.failure), consecutive: openFailures })
        if (openFailures >= 3) yield* fail("Session renewal failed three times")
      }
    }
    if (next === undefined && opening === undefined && (yield* Clock.currentTimeMillis) >= retryAt && left <= Math.min(options.leadSeconds ?? 60, active.maxSeconds / 3)) {
      opening = yield* acquire.pipe(Effect.forkIn(scope))
    }
    if (next === undefined || active.openSequences.size > 0 || active.receivedFrames < active.expectedFrames) return
    if (!isIdle(yield* active.session.engine.state) || (yield* next.session.engine.state).ready.length === 0) return
    const pressure = yield* Effect.exit(active.session.media.pressure)
    if (Exit.isSuccess(pressure) && (pressure.value.droppedVideo > 0n || pressure.value.droppedAudio > 0n)) {
      yield* fail(`Session media source reported drops before renewal: video=${pressure.value.droppedVideo} audio=${pressure.value.droppedAudio}`)
      return
    }
    const previous = active
    const retired = yield* retiring(previous)
    current = next; next = undefined
    yield* current.session.engine.setAutoplay(autoplay)
    yield* retireSequences(previous)
    yield* log("Switched prepared sessions at a sequence boundary")
    yield* observe({ _tag: "Switched", ...retired })
    yield* Effect.forkIn(close(previous), scope)
  }))
  yield* Effect.forkIn(Effect.forever(tick.pipe(Effect.andThen(Effect.sleep("100 millis"))))
    .pipe(Effect.catch((error) => fail(`Session coordinator: ${String(error)}`))), scope)

  const engine: ClipEngineShape = {
    prepare, enqueue, state, events: events.stream({ capacity: 256, maxBytes: 1_048_576 }), failure: Deferred.await(fatal),
    setAutoplay: (enabled) => guarded(Effect.gen(function* () { autoplay = enabled; yield* current!.session.engine.setAutoplay(enabled); if (!enabled && next !== undefined) yield* next.session.engine.setAutoplay(false) })),
    stop: guarded(Effect.gen(function* () { autoplay = false; if (next !== undefined) yield* next.session.engine.setAutoplay(false); yield* current!.session.engine.stop })),
    setCanvas: (canvas) => guarded(Effect.suspend(() => current!.session.engine.setCanvas(canvas))),
    remove: (id) => guarded(Effect.suspend(() => { const slot = owner(id); return slot === undefined ? Effect.fail(new NotFound({ clipId: id })) : slot.session.engine.remove(id) })),
    move: (id, position, queue) => guarded(Effect.gen(function* () {
      const slot = owner(id)
      if (slot === undefined) return yield* new NotFound({ clipId: id })
      const field = queue === "generation" ? "queued" : "ready"
      const preceding = slots.slice(0, slots.indexOf(slot)).filter((s) => !s.closed)
      const offset = (yield* Effect.forEach(preceding, (s) => s.session.engine.state)).reduce((n, s) => n + s[field].length, 0)
      yield* slot.session.engine.move(id, Math.max(0, position - offset), queue)
    })),
  }
  return {
    engine,
    media: {
      video: Stream.fromQueue(video).pipe(Stream.tap(() => Effect.sync(() => { queuedFrames-- }))),
      audio: Stream.fromQueue(audio).pipe(Stream.tap((frame) => Effect.sync(() => { queuedSamples -= frame.samples.length }))),
      pressure: Effect.suspend(() => current!.session.media.pressure),
      videoFramesPerSecond: current.session.media.videoFramesPerSecond
    },
    sessionId: Effect.suspend(() => current!.session.sessionId), validCommands: Effect.suspend(() => current!.session.validCommands),
    cleanup: Effect.sync(() => slots.length > 0
      ? Option.none<CleanupReport>()
      : Option.some<CleanupReport>({ steps: [...cleanupByStep.values()] })),
  }
})
