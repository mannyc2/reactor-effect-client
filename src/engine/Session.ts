import { Clock, Context, Crypto, Data, Deferred, Effect, FileSystem, Layer, Option, Path, Ref, Result, Schema, Scope, Semaphore, Stream } from "effect"
import { HttpClient } from "effect/unstable/http"
import { ClipRequest, type Canvas, type ClipId, type ReferenceImage } from "../Clip.js"
import { h3ReferenceTurboRealtime, isRequestableSeconds, type ModelProfile } from "../ModelProfile.js"
import * as Submission from "../Submission.js"
import { Observations } from "../observation.js"
import {
  Busy,
  BuildTiming,
  type ClipRecord,
  type CommandError,
  emptyState,
  EngineEvent,
  type EngineState,
  type EnqueueError,
  InvalidRequest,
  isIdle,
  NotFound,
  Rejected,
  rememberContinuable,
  type RemoveOutcome,
  SessionFailed,
  Uncertain,
  ClipEngine,
  type ClipEngineShape
} from "./Engine.js"
import { decodeMetadata, encodeMetadata } from "./Metadata.js"
import type { AudioFrame, Client, ControlEvent, ReactorError, Snapshot, UploadReference, VideoFrame } from "../Model.js"
import { makeReferenceUploader } from "./References.js"
import { acceptedSeconds, type ClipInfo, clipIdOf, type CommandErrorPayload, commands, type Decoded, decodeMessage, requiredDeploymentNames } from "./Wire.js"

// ---------------------------------------------------------------------------
// The transport seam. The native client satisfies it as-is; tests inject a scripted one.
// ---------------------------------------------------------------------------

export type TransportShape = Pick<Client, "connect" | "reconnect" | "send" | "uploadFile" | "requestSchema" | "events" | "video" | "audio" | "snapshot" | "disconnect"> & {
  /** Local renderers may keep the full request beside the provider-style wire command after commit. */
  readonly registerRequest?: (token: string, request: ClipRequest) => Effect.Effect<void, ReactorError>
  readonly releaseRequest?: (token: string) => Effect.Effect<void>
  /** Local transports may own non-provider reference schemes such as sim://. Native transport leaves this absent. */
  readonly prepareReferences?: (references: ReadonlyArray<ReferenceImage>) => Effect.Effect<ReadonlyArray<UploadReference>, ReactorError>
  /** A transport that actually observes build start/completion can supply the measured interval for this clip. */
  readonly buildTiming?: (clipId: ClipId) => Effect.Effect<Option.Option<BuildTiming>>
}

export class ReactorTransport extends Context.Service<ReactorTransport, TransportShape>()("reactor-effect-client/ReactorTransport") {}

/** The session's actual output tracks and their queue pressure. Single reader per stream. */
export interface MediaShape {
  readonly video: Stream.Stream<VideoFrame, ReactorError>
  readonly audio: Stream.Stream<AudioFrame, ReactorError>
  readonly pressure: Effect.Effect<Snapshot, ReactorError>
  /** Accepted model output cadence used only for bounded frame accounting. */
  readonly videoFramesPerSecond: number
}
export class ReactorMedia extends Context.Service<ReactorMedia, MediaShape>()("reactor-effect-client/ReactorMedia") {}

export class SessionSetupError extends Data.TaggedError("SessionSetupError")<{ readonly reason: string; readonly cause?: unknown }> {}

export interface SessionOptions {
  readonly profile?: ModelProfile
  /** Canvas to establish before any work is submitted. */
  readonly canvas?: Canvas
  /** Bound on a command's correlated reply. */
  readonly commandTimeoutMs?: number
  /** Bound on connect and on the first state_update. */
  readonly setupTimeoutMs?: number
  /** After an ack without clip_queued, or a timeout, how long a broadcast carrying our metadata token may still prove acceptance. */
  readonly reconcileWindowMs?: number
  /** Stay inside the provider's 30-second recoverable connection window. */
  readonly reconnectWindowMs?: number
  readonly references?: { readonly maxBytes?: number; readonly timeoutMs?: number; readonly stagingDirectory?: string }
  /** Check the session's OpenAPI document for the command/message names this adapter relies on. */
  readonly verifyDeployment?: boolean
  /**
   * Hold the last frame between clips instead of flushing to black
   * (`set_flush_on_clip_end` false). Default true: a livestream wants no black
   * flash at a boundary. The provider notes this does not guarantee visual
   * continuity; continuations do that.
   */
  readonly holdLastFrame?: boolean
}

export interface CleanupReport {
  readonly steps: ReadonlyArray<{ readonly step: string; readonly ok: boolean; readonly detail?: string }>
}

/** Coordinator-only hooks around one enqueue's generic Submission boundary. */
export interface EnqueueHooks {
  /** Runs atomically after caller-owned reference/admission prework and before dispatch. */
  readonly commit?: (submissionId: string) => Effect.Effect<void, EnqueueError>
  /** Runs inside the session-owned committed execution exactly once for its typed result. */
  readonly result?: (submissionId: string, result: Result.Result<ClipId, EnqueueError>) => Effect.Effect<void, EnqueueError>
}

export interface ReactorSession {
  readonly engine: ClipEngineShape
  readonly media: MediaShape
  /** Internal coordinator seam preserving the child's prepare/commit/execute lifetime. */
  readonly prepareEnqueue?: (request: ClipRequest, hooks?: EnqueueHooks) => Effect.Effect<Submission.Submission<ClipId, EnqueueError>, EnqueueError>
  /** The coordinator's session id, once reported. The host needs it for remote termination. */
  readonly sessionId: Effect.Effect<Option.Option<string>>
  /** What teardown managed to do; filled when the scope closes. */
  readonly cleanup: Effect.Effect<Option.Option<CleanupReport>>
  readonly validCommands: Effect.Effect<Option.Option<ReadonlyArray<string>>>
}

/** The provider delivers these out of order and more than once: autoplay can
 * prove playback before the generated broadcast arrives. Ordering them lets the
 * furthest point reached decide what is a duplicate and what is a step we never
 * saw, and makes "failed but also started" and "finished without ever starting"
 * unrepresentable — a set of visited phases allowed both. */
const reach = { generated: 1, started: 2, finished: 3 } as const
type Phase = keyof typeof reach | "failed"

interface Local {
  readonly record: ClipRecord
  readonly admittedAt: number
  /** Nothing was building or queued when this was accepted: admission → ready bounds its build. */
  readonly idleAtAdmission: boolean
  /** How far this clip got. `null` until the first observation; `failed` is absorbing. */
  reached: Phase | null
}

const reachedAtLeast = (entry: Local, phase: keyof typeof reach): boolean =>
  entry.reached !== null && entry.reached !== "failed" && reach[entry.reached] >= reach[phase]

const terminalCodes = new Set(["Closed", "TerminalSession", "Protocol", "Overflow", "Shutdown"])
const knownNoEffectCodes = new Set([
  "QUEUE_FULL",
  "INVALID_ARGUMENT",
  "INVALID_REQUEST",
  "BAD_REQUEST",
  "UNSUPPORTED_COMMAND",
  "NOT_FOUND"
])
const mutatingCommands = new Set<string>([
  commands.enqueue,
  commands.pop,
  commands.move,
  commands.stop,
  commands.setAutoplay,
  commands.setCanvas,
  commands.setFlushOnClipEnd,
  commands.reset
])

/** Classify a transport failure of a sent command. */
const classifySendError = (command: string, error: ReactorError): Rejected | Uncertain | SessionFailed | InvalidRequest => {
  if (error.code === "InvalidInput") return new InvalidRequest({ reason: error.message })
  const code = error.nativeError?.code ?? error.context.remoteCode
  if (error.context.outcome === "unknown") return new Uncertain({ command, reason: error.message, code, cause: error })
  if (terminalCodes.has(error.code)) return new SessionFailed({ reason: `${error.code}: ${error.message}`, cause: error })
  if (error.context.outcome === "not-submitted" || (error.context.outcome === "replied" && code !== undefined && knownNoEffectCodes.has(code))) {
    return new Rejected({ command, code, reason: error.nativeError?.message ?? error.message, cause: error })
  }
  if (error.code === "Timeout" || error.code === "Disconnected" || error.code === "Aborted") {
    return new Uncertain({ command, reason: error.message, code, cause: error })
  }
  return new Uncertain({ command, reason: error.nativeError?.message ?? error.message, code, cause: error })
}

const nowMs = Clock.currentTimeMillis
const maxTrackedClips = 4_096
const maxOrphanClips = 256
const maxOrphanEventsPerClip = 8

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
    catch: () => new InvalidRequest({ reason: "clip request is malformed or contains non-JSON/cyclic data" })
  })

export const make = (transport: TransportShape, options: SessionOptions): Effect.Effect<
  ReactorSession,
  SessionSetupError,
  Scope.Scope | FileSystem.FileSystem | Path.Path | Crypto.Crypto | HttpClient.HttpClient
> =>
  Effect.gen(function*() {
    const scope = yield* Effect.scope
    const profile = options.profile ?? h3ReferenceTurboRealtime
    const commandTimeoutMs = options.commandTimeoutMs ?? 15_000
    const setupTimeoutMs = options.setupTimeoutMs ?? 60_000
    const reconcileWindowMs = options.reconcileWindowMs ?? 5_000

    // ---- state
    const state = yield* Ref.make<EngineState>(emptyState(profile.expectedCapacities))
    const local = yield* Ref.make(new Map<ClipId, Local>())
    const orphans = yield* Ref.make(new Map<string, Array<Decoded>>())
    const pendingByToken = yield* Ref.make(new Map<string, Deferred.Deferred<ClipInfo, Rejected | Uncertain>>())
    const events = new Observations<EngineEvent>()
    yield* Effect.addFinalizer(() => Effect.sync(() => events.end()))
    const fatal = yield* Deferred.make<SessionFailed>()
    const initialState = yield* Deferred.make<void>()
    const sessionId = yield* Ref.make(Option.none<string>())
    const validCommands = yield* Ref.make(Option.none<ReadonlyArray<string>>())
    const connected = yield* Ref.make(false)
    const cleanupReport = yield* Ref.make(Option.none<CleanupReport>())
    const lock = yield* Semaphore.make(1)
    const submit = yield* Semaphore.make(1)
    const tokenSeq = yield* Ref.make(0)
    /** Deployment-published length bounds from `state_update`; the profile's values until the session reports them. */
    const liveBounds = yield* Ref.make(profile.requestSeconds)
    /** Commands awaiting a possible broadcast `command_error` (a refused command resolves without a reply). */
    const refusals = yield* Ref.make(new Map<string, Deferred.Deferred<CommandErrorPayload>>())
    /** In-flight builds the provider agreed to discard; they leave our state when the provider shows the builder moved on. */
    const discarded = yield* Ref.make(new Set<ClipId>())
    let recovering: Deferred.Deferred<void> | undefined
    let autoplayEnabled = true

    const emit = (event: EngineEvent) => Effect.sync(() => events.emit(event))
    const log = (message: string) => Effect.log(`[reactor] ${message}`)

    const failSession = (reason: string, cause?: unknown) =>
      Effect.gen(function*() {
        const failure = new SessionFailed({ reason, ...(cause === undefined ? {} : { cause }) })
        if (yield* Deferred.succeed(fatal, failure)) {
          // pending reconciliations can no longer be proven
          const pending = yield* Ref.getAndSet(pendingByToken, new Map())
          for (const d of pending.values()) yield* Deferred.fail(d, new Uncertain({ command: commands.enqueue, reason }))
          yield* emit(EngineEvent.SessionFailed({ failure }))
        }
        return failure
      })

    // ---- cleanup is registered before anything can allocate or emit
    yield* Effect.addFinalizer(() =>
      Effect.gen(function*() {
        const steps: Array<{ step: string; ok: boolean; detail?: string }> = []
        const attempt = (step: string, effect: Effect.Effect<unknown, unknown>) =>
          Effect.result(effect.pipe(Effect.interruptible, Effect.timeout(5_000))).pipe(Effect.map((r) => {
            steps.push(Result.isSuccess(r) ? { step, ok: true } : { step, ok: false, detail: String(r.failure) })
          }))
        if (yield* Ref.get(connected)) {
          yield* attempt("reset", transport.send(commands.reset, {}))
          yield* attempt("disconnect", transport.disconnect)
        }
        yield* Ref.set(cleanupReport, Option.some({ steps }))
        for (const s of steps) if (!s.ok) yield* log(`cleanup step ${s.step} failed: ${s.detail ?? ""}`)
      }).pipe(Effect.uninterruptible)
    )

    // ---- reducer: every state change goes through here, and its event is published afterwards
    const promoteBuilding = (s: EngineState): { state: EngineState; promoted: ClipRecord | undefined } => {
      if (Option.isSome(s.building) || s.queued.length === 0) return { state: s, promoted: undefined }
      const head = s.queued[0]!
      // Builds consume the generation queue front-first; the provider gives no build-start observation.
      return { state: { ...s, queued: s.queued.slice(1), building: Option.some({ record: head, startedAt: Option.none() }) }, promoted: head }
    }

    const register = (clip: ClipInfo, request: ClipRequest, admittedAt: number, seq: number) =>
      Effect.gen(function*() {
        // The provider may already be playing this clip. An acknowledgement that
        // lost a requested boundary frame must stop the session, not trigger a retry.
        if (request.startingFrame !== undefined && clip.has_starting_frame !== true) {
          return yield* new SessionFailed({ reason: "enqueue acknowledgement did not retain the requested starting frame" })
        }
        if (request.endingFrame !== undefined && clip.has_ending_frame !== true) {
          return yield* new SessionFailed({ reason: "enqueue acknowledgement did not retain the requested ending frame" })
        }
        const clipId = clipIdOf(clip)
        const seconds = acceptedSeconds(clip, profile.fps)
        if (seconds === undefined) return yield* new Rejected({ command: commands.enqueue, code: "malformed_reply", reason: "clip_queued carried no length" })
        const tracked = yield* Ref.get(local)
        if (!tracked.has(clipId) && tracked.size >= maxTrackedClips) {
          return yield* new SessionFailed({ reason: `session clip tracking exceeded ${maxTrackedClips} entries` })
        }
        const s0 = yield* Ref.get(state)
        const record: ClipRecord = { clipId, seq, durationSeconds: seconds, request, enqueuedAt: admittedAt }
        yield* Ref.update(local, (m) => new Map(m).set(clipId, {
          record,
          admittedAt,
          idleAtAdmission: s0.queued.length === 0 && Option.isNone(s0.building),
          reached: null
        }))
        const { state: s1, promoted } = promoteBuilding({ ...s0, queued: [...s0.queued, record] })
        yield* Ref.set(state, s1)
        yield* emit(EngineEvent.Queued({ clipId, durationSeconds: seconds }))
        if (promoted !== undefined) yield* emit(EngineEvent.Building({ clipId: promoted.clipId, startedAt: Option.none() }))
        // lifecycle that outran the reply
        const early = yield* Ref.modify(orphans, (m) => {
          const list = m.get(clip.clip_id) ?? []
          const next = new Map(m)
          next.delete(clip.clip_id)
          return [list, next] as const
        })
        for (const message of early) yield* applyLifecycle(message)
        return clipId
      })

    /** A discarded in-flight build is gone once another build finished or the provider's queue no longer lists it. */
    const dropDiscardedBuild = (except?: ClipId) =>
      Effect.gen(function*() {
        const s0 = yield* Ref.get(state)
        const d = yield* Ref.get(discarded)
        const stale = Option.filter(s0.building, (b) => d.has(b.record.clipId) && b.record.clipId !== except)
        if (Option.isNone(stale)) return
        yield* Ref.update(discarded, (set) => { const n = new Set(set); n.delete(stale.value.record.clipId); return n })
        const { state: s1, promoted } = promoteBuilding({ ...s0, building: Option.none() })
        yield* Ref.set(state, s1)
        if (promoted !== undefined) yield* emit(EngineEvent.Building({ clipId: promoted.clipId, startedAt: Option.none() }))
      })

    const applyLifecycle = (message: Decoded): Effect.Effect<void> =>
      Effect.gen(function*() {
        if (message._tag !== "ClipGenerated" && message._tag !== "ClipFailed" && message._tag !== "ClipStarted" &&
          message._tag !== "ClipFinished" && message._tag !== "ClipStopped") return
        const clip = message.clip
        const clipId = clipIdOf(clip)
        const entry = (yield* Ref.get(local)).get(clipId)
        if (entry === undefined) {
          // Not ours yet: either a reply is still in flight (buffer) or acceptance is being reconciled by token.
          const token = decodeMetadata(clip.metadata)
          const pending = Option.isSome(token) ? (yield* Ref.get(pendingByToken)).get(token.value.token) : undefined
          const retained = yield* Ref.modify(orphans, (m) => {
            const existing = m.get(clip.clip_id) ?? []
            if (existing.length >= maxOrphanEventsPerClip || (!m.has(clip.clip_id) && m.size >= maxOrphanClips)) return [false, m] as const
            return [true, new Map(m).set(clip.clip_id, [...existing, message])] as const
          })
          if (!retained) {
            yield* failSession(`uncorrelated lifecycle observations exceeded ${maxOrphanClips} clips or ${maxOrphanEventsPerClip} events per clip`)
            return
          }
          if (pending !== undefined) yield* Deferred.succeed(pending, clip)
          return
        }
        if (entry.reached === "failed") return
        const now = yield* nowMs
        const phase: Phase = message._tag === "ClipGenerated" ? "generated" : message._tag === "ClipFailed" ? "failed" : message._tag === "ClipStarted" ? "started" : "finished"
        if (phase !== "failed" && reachedAtLeast(entry, phase)) return // duplicate or late delivery
        // Autoplay may prove playback before a generated broadcast reaches us.
        // Starting necessarily proves generation; a late generated event must
        // not put an already playing clip back in the ready queue.
        if (phase === "started" && !reachedAtLeast(entry, "generated")) yield* applyLifecycle({ _tag: "ClipGenerated", clip })
        entry.reached = phase
        const record = entry.record
        const isDiscarded = (yield* Ref.get(discarded)).has(clipId)
        if (isDiscarded && (phase === "generated" || phase === "failed")) {
          // a popped in-flight build finishing: the provider discards its result, and so do we
          yield* dropDiscardedBuild()
          return
        }
        if (phase === "generated" || phase === "failed") yield* dropDiscardedBuild(clipId)
        switch (message._tag) {
          case "ClipGenerated": {
            const s0 = yield* Ref.get(state)
            const wasQueued = s0.queued.some((r) => r.clipId === clipId) || Option.exists(s0.building, (b) => b.record.clipId === clipId)
            const seconds = acceptedSeconds(clip, profile.fps) ?? record.durationSeconds
            const done: ClipRecord = { ...record, durationSeconds: seconds }
            const cleared: EngineState = {
              ...s0,
              queued: s0.queued.filter((r) => r.clipId !== clipId),
              building: Option.filter(s0.building, (b) => b.record.clipId !== clipId),
              ready: wasQueued || !s0.ready.some((r) => r.clipId === clipId) ? [...s0.ready, done] : s0.ready,
              continuable: rememberContinuable(s0, clipId, profile.continuationWindow)
            }
            const { state: s1, promoted } = promoteBuilding(cleared)
            yield* Ref.set(state, s1)
            const measured = transport.buildTiming === undefined ? Option.none<BuildTiming>() : yield* transport.buildTiming(clipId)
            const timing = Option.getOrElse(measured, () => entry.idleAtAdmission ? BuildTiming.Bounded({ admissionToReadyMs: now - entry.admittedAt }) : BuildTiming.Unknown())
            yield* emit(EngineEvent.Ready({ clipId, timing, durationSeconds: seconds }))
            if (promoted !== undefined) yield* emit(EngineEvent.Building({ clipId: promoted.clipId, startedAt: Option.none() }))
            return
          }
          case "ClipFailed": {
            const s0 = yield* Ref.get(state)
            const { state: s1, promoted } = promoteBuilding({
              ...s0,
              queued: s0.queued.filter((r) => r.clipId !== clipId),
              building: Option.filter(s0.building, (b) => b.record.clipId !== clipId),
              ready: s0.ready.filter((r) => r.clipId !== clipId),
              playing: Option.filter(s0.playing, (p) => p.record.clipId !== clipId),
              failed: [...s0.failed, clipId]
            })
            yield* Ref.set(state, s1)
            yield* emit(EngineEvent.Failed({ clipId, reason: message.reason }))
            if (promoted !== undefined) yield* emit(EngineEvent.Building({ clipId: promoted.clipId, startedAt: Option.none() }))
            return
          }
          case "ClipStarted": {
            const s0 = yield* Ref.get(state)
            const playingRecord = s0.ready.find((r) => r.clipId === clipId) ?? record
            yield* Ref.set(state, {
              ...s0,
              ready: s0.ready.filter((r) => r.clipId !== clipId),
              playing: Option.some({ record: playingRecord, startedAt: now }),
              started: true
            })
            yield* emit(EngineEvent.Started({ clipId, at: now, durationSeconds: playingRecord.durationSeconds }))
            return
          }
          case "ClipFinished":
          case "ClipStopped": {
            const s0 = yield* Ref.get(state)
            const s1: EngineState = { ...s0, playing: Option.filter(s0.playing, (p) => p.record.clipId !== clipId) }
            yield* Ref.set(state, s1)
            yield* emit(EngineEvent.Ended({ clipId, termination: message._tag === "ClipStopped" ? "stopped" : "finished" }))
            // Playback had begun and nothing is ready to follow: dead air until the next clip_generated.
            if (s1.started && s1.ready.length === 0 && Option.isNone(s1.playing)) yield* emit(EngineEvent.Starved({ at: now }))
            return
          }
        }
      })

    const applyMessage = (raw: unknown): Effect.Effect<void> =>
      Effect.gen(function*() {
        const decoded = decodeMessage(raw)
        if (Result.isFailure(decoded)) {
          // A message we rely on that we cannot read means our view of the session is no longer trustworthy.
          yield* failSession(`malformed ${decoded.failure.type} message: ${decoded.failure.reason}`)
          return
        }
        const message = decoded.success
        switch (message._tag) {
          case "ClipQueued": {
            // Reply to our enqueue (normally consumed by the sender) or a broadcast: prove acceptance by token.
            const token = decodeMetadata(message.clip.metadata)
            const pending = Option.isSome(token) ? (yield* Ref.get(pendingByToken)).get(token.value.token) : undefined
            if (pending !== undefined) yield* Deferred.succeed(pending, message.clip)
            return
          }
          case "ClipGenerated":
          case "ClipFailed":
          case "ClipStarted":
          case "ClipFinished":
          case "ClipStopped":
            return yield* applyLifecycle(message)
          case "ClipMoved":
          case "ClipPopped":
            return
          case "QueueUpdate": {
            const u = message.update
            // Acceptance proof for a pending token, and a missed clip_generated for a clip the provider lists as ready.
            for (const clip of [...(u.generation ?? []), ...(u.playout ?? [])]) {
              const token = decodeMetadata(clip.metadata)
              const pending = Option.isSome(token) ? (yield* Ref.get(pendingByToken)).get(token.value.token) : undefined
              if (pending !== undefined) yield* Deferred.succeed(pending, clip)
            }
            for (const clip of u.playout ?? []) {
              const entry = (yield* Ref.get(local)).get(clipIdOf(clip))
              if (entry !== undefined && !reachedAtLeast(entry, "generated")) yield* applyLifecycle({ _tag: "ClipGenerated", clip })
            }
            yield* Ref.update(state, (s) => {
              const entries = (infos: ReadonlyArray<ClipInfo>, records: ReadonlyArray<ClipRecord>) => infos.flatMap((info) => records.filter((r) => r.clipId === info.clip_id))
              return { ...s, queued: u.generation === undefined ? s.queued : entries(u.generation, s.queued), ready: u.playout === undefined ? s.ready : entries(u.playout, s.ready) }
            })
            return
          }
          case "StateUpdate": {
            const u = message.update
            if (u.valid_commands !== undefined) yield* Ref.set(validCommands, Option.some(u.valid_commands))
            if (u.generation_capacity !== undefined || u.playout_capacity !== undefined) {
              yield* Ref.update(state, (s) => ({
                ...s,
                capacities: { generation: u.generation_capacity ?? s.capacities.generation, playout: u.playout_capacity ?? s.capacities.playout }
              }))
            }
            // deployment-published length bounds win over the profile; a writer must not hardcode them
            if (u.clip_seconds_min !== undefined || u.clip_seconds_max !== undefined) {
              const bounds = yield* Ref.updateAndGet(liveBounds, (b) => ({ min: u.clip_seconds_min ?? b.min, max: u.clip_seconds_max ?? b.max }))
              if (Math.abs(bounds.min - profile.requestSeconds.min) > 1e-3 || Math.abs(bounds.max - profile.requestSeconds.max) > 1e-3) {
                yield* log(`deployment clip bounds ${bounds.min}–${bounds.max}s differ from the profile's ${profile.requestSeconds.min}–${profile.requestSeconds.max}s; using the deployment's`)
              }
            }
            // the snapshot also confirms playback facts we may have missed
            if (u.playing === false || u.playing_clip_id === null) {
              const s = yield* Ref.get(state)
              if (Option.isSome(s.playing)) yield* applyLifecycle({ _tag: "ClipFinished", clip: { clip_id: s.playing.value.record.clipId } })
            } else if (typeof u.playing_clip_id === "string") {
              const entry = (yield* Ref.get(local)).get(u.playing_clip_id as ClipId)
              if (entry !== undefined && !reachedAtLeast(entry, "started")) yield* applyLifecycle({ _tag: "ClipStarted", clip: { clip_id: u.playing_clip_id } })
            }
            yield* Deferred.succeed(initialState, undefined)
            return
          }
          case "CommandError": {
            // command_error broadcasts have no request identity. They are useful
            // evidence that something failed, but they cannot safely be assigned
            // to a later request of the same command.
            const e = message.error
            const waiter = e.command === undefined ? undefined : (yield* Ref.get(refusals)).get(e.command)
            if (waiter !== undefined) yield* Deferred.succeed(waiter, e)
            yield* log(`command_error ${e.command ?? "?"}: ${e.reason ?? e.message ?? "?"}`)
            return
          }
          case "AutoplayAccepted":
            return
          case "CanvasAccepted":
          case "Other":
            return
        }
      })

    const recover = Effect.gen(function* () {
      const before = yield* Ref.get(state)
      const pending = [...before.queued, ...before.ready,
        ...Option.match(before.building, { onNone: () => [], onSome: (b) => [b.record] }),
        ...Option.match(before.playing, { onNone: () => [], onSome: (p) => [p.record] })]
      const reconnect = Effect.gen(function* () {
        while (true) {
          const result = yield* Effect.result(transport.reconnect)
          if (Result.isSuccess(result)) return
          if (terminalCodes.has(result.failure.code)) return yield* Effect.fail(result.failure)
          yield* Effect.sleep("500 millis")
        }
      })
      yield* reconnect
      const read = (command: string, args: Schema.JsonObject = {}) => transport.send(command, args).pipe(Effect.flatMap((raw) => {
        const decoded = decodeMessage(raw)
        return Result.isSuccess(decoded) ? Effect.succeed(decoded.success) : Effect.fail(new SessionSetupError({ reason: `reconnect ${command}: invalid reply` }))
      }))
      const paused = yield* read(commands.setAutoplay, { enabled: false })
      if (paused._tag !== "AutoplayAccepted" || paused.enabled) return yield* new SessionSetupError({ reason: "reconnect could not pause emission" })
      const snapshot = yield* read(commands.getState)
      const queue = yield* read(commands.getQueue)
      if (snapshot._tag !== "StateUpdate" || queue._tag !== "QueueUpdate" || queue.update.generation === undefined || queue.update.playout === undefined) {
        return yield* new SessionSetupError({ reason: "reconnect did not return both queue snapshots" })
      }
      const playingId = snapshot.update.playing_clip_id
      if (typeof playingId === "string") {
        const stopped = yield* read(commands.stop)
        if (stopped._tag !== "ClipStopped") return yield* new SessionSetupError({ reason: "reconnect could not stop partial clip" })
      }
      const retained = new Set([...queue.update.generation, ...queue.update.playout].map((c) => c.clip_id))
      yield* lock.withPermits(1)(Effect.gen(function* () {
        // No lifecycle from the gap establishes delivery. Only queued media
        // survives; clips in flight on either side of the disconnect are lost.
        for (const record of pending) {
          if (!retained.has(record.clipId) || record.clipId === playingId || Option.exists(before.playing, (p) => p.record.clipId === record.clipId)) {
            yield* applyLifecycle({ _tag: "ClipFailed", clip: { clip_id: record.clipId }, reason: "Media lost during reconnect" })
          }
        }
        yield* applyMessage({ type: "queue_update", data: queue.update })
        yield* applyMessage({ type: "state_update", data: { ...snapshot.update, playing: false, playing_clip_id: null } })
      }))
      const restored = yield* read(commands.setAutoplay, { enabled: autoplayEnabled })
      if (restored._tag !== "AutoplayAccepted" || restored.enabled !== autoplayEnabled) return yield* new SessionSetupError({ reason: "reconnect autoplay was not confirmed" })
      yield* log("reconnected and reconciled queues; gap media marked lost")
    }).pipe(submit.withPermits(1), Effect.timeout(options.reconnectWindowMs ?? 29_000),
      Effect.catch((error) => failSession(`reconnect failed: ${String(error)}`, error)),
      Effect.ensuring(Effect.suspend(() => {
        const pending = recovering
        recovering = undefined
        return pending === undefined ? Effect.void : Deferred.succeed(pending, undefined).pipe(Effect.asVoid)
      })))

    const onControl = (event: ControlEvent): Effect.Effect<void> =>
      lock.withPermits(1)(Effect.gen(function*() {
        switch (event._tag) {
          case "Message":
            if (recovering !== undefined) return
            return yield* applyMessage(event.message)
          case "Session":
            yield* Ref.set(sessionId, Option.fromUndefinedOr(event.sessionId))
            return
          case "Status":
            if (event.status === "disconnected" && (yield* Ref.get(connected)) && recovering === undefined) {
              recovering = yield* Deferred.make<void>()
              yield* Effect.forkIn(recover, scope)
            }
            return
          case "Error": {
            const decoded = Schema.decodeUnknownResult(Schema.Struct({ code: Schema.String, message: Schema.String, recoverable: Schema.optionalKey(Schema.Boolean) }))(event.error)
            if (Result.isSuccess(decoded) && decoded.success.recoverable === false) yield* failSession(`${decoded.success.code}: ${decoded.success.message}`)
            else yield* log(`provider error event: ${Result.isSuccess(decoded) ? `${decoded.success.code}: ${decoded.success.message}` : "unreadable"}`)
            return
          }
          case "RuntimeMessage":
          case "Track":
          case "Capabilities":
            return
        }
      }))

    // readers first, then connect
    yield* transport.events.pipe(
      Stream.runForEach(onControl),
      Effect.catch((error) => failSession(`control stream failed: ${error.message}`, error).pipe(Effect.asVoid)),
      Effect.forkScoped
    )

    yield* transport.connect().pipe(
      Effect.timeoutOrElse({ duration: setupTimeoutMs, orElse: () => Effect.fail(new SessionSetupError({ reason: "connect timed out" })) }),
      Effect.mapError((e) => e instanceof SessionSetupError ? e : new SessionSetupError({ reason: `connect failed: ${e.message}`, cause: e }))
    )
    yield* Ref.set(connected, true)

    // The first state_update is broadcast on connect; ask for it as well so a lost broadcast
    // cannot stall startup. The reply goes through the same reducer.
    yield* transport.send(commands.getState, {}).pipe(
      Effect.timeout(setupTimeoutMs),
      Effect.flatMap((reply) => reply === undefined ? Effect.void : lock.withPermits(1)(applyMessage(reply))),
      Effect.catch((e) => log(`get_state after connect failed: ${String(e)}`))
    )
    yield* Deferred.await(initialState).pipe(
      Effect.timeoutOrElse({ duration: setupTimeoutMs, orElse: () => Effect.fail(new SessionSetupError({ reason: "no state_update after connect" })) })
    )
    const s0 = yield* Ref.get(state)
    if (!isIdle(s0)) return yield* new SessionSetupError({ reason: "session is not idle: it already holds clips" })

    if (options.verifyDeployment ?? true) {
      const schema = yield* transport.requestSchema.pipe(
        Effect.timeout(setupTimeoutMs),
        Effect.mapError((e) => new SessionSetupError({ reason: `could not read the session's schema: ${String(e)}`, cause: e }))
      )
      const text = JSON.stringify(schema ?? null)
      const missing = requiredDeploymentNames.filter((name) => !text.includes(`"${name}"`) && !text.includes(name))
      if (schema === undefined || missing.length > 0) {
        return yield* new SessionSetupError({
          reason: `deployment schema does not describe ${missing.length > 0 ? missing.join(", ") : "anything"}; this adapter targets ${profile.modelName}`
        })
      }
    }

    // ---- commands
    const guard = <A, E, R>(effect: Effect.Effect<A, E, R>): Effect.Effect<A, E | SessionFailed, R> =>
      Effect.gen(function*() {
        if (recovering !== undefined) yield* Deferred.await(recovering)
        if (yield* Deferred.isDone(fatal)) return yield* Effect.fail(yield* Deferred.await(fatal))
        return yield* effect
      })

    const sendDecoded = (command: string, args: Schema.JsonObject, uploads?: Readonly<Record<string, UploadReference>>) =>
      transport.send(command, args, uploads).pipe(
        Effect.timeoutOrElse({ duration: commandTimeoutMs, orElse: () => Effect.fail(new Uncertain({ command, reason: `no reply within ${commandTimeoutMs}ms` })) }),
        Effect.mapError((e) => e instanceof Uncertain ? e : classifySendError(command, e)),
        Effect.flatMap((reply) => {
          if (reply === undefined) return Effect.succeed(Option.none<Decoded>())
          const decoded = decodeMessage(reply)
          return Result.isFailure(decoded)
            ? Effect.fail(mutatingCommands.has(command)
              ? new Uncertain({ command, code: "malformed_reply", reason: decoded.failure.reason, cause: decoded.failure })
              : new Rejected({ command, code: "malformed_reply", reason: decoded.failure.reason }))
            : Effect.succeed(Option.some(decoded.success))
        })
      )

    /**
     * Send a command whose refusal arrives as a broadcast rather than a reply. Returns the decoded
     * reply, or the broadcast refusal observed within the reconciliation window, or `None` when the
     * command was acknowledged silently and nothing refused it.
     */
    const sendReconciled = (command: string, args: Schema.JsonObject) =>
      Effect.gen(function*() {
        const refusal = yield* Deferred.make<CommandErrorPayload>()
        yield* Ref.update(refusals, (m) => new Map(m).set(command, refusal))
        const reply = yield* sendDecoded(command, args).pipe(Effect.mapError((e) => e._tag === "InvalidRequest" ? new Rejected({ command, reason: e.reason }) : e))
        if (Option.isSome(reply)) return reply
        const refused = yield* Deferred.await(refusal).pipe(Effect.timeoutOption(reconcileWindowMs))
        if (Option.isSome(refused)) {
          return yield* new Uncertain({
            command,
            code: refused.value.code,
            reason: `uncorrelated command_error: ${refused.value.reason ?? refused.value.message ?? "refused"}`
          })
        }
        return Option.none<Decoded>()
      }).pipe(Effect.ensuring(Ref.update(refusals, (m) => { const n = new Map(m); n.delete(command); return n })))

    const stateEvidence = (matches: (state: import("./Wire.js").StateUpdate) => boolean) =>
      Effect.gen(function* () {
        const snapshot = yield* sendDecoded(commands.getState, {})
        if (Option.isNone(snapshot) || snapshot.value._tag !== "StateUpdate") return false
        yield* lock.withPermits(1)(applyMessage({ type: "state_update", data: snapshot.value.update }))
        return matches(snapshot.value.update)
      }).pipe(Effect.catch(() => Effect.succeed(false)))

    const setCanvas = (canvas: Canvas) =>
      guard(submit.withPermits(1)(Effect.gen(function*() {
        if (!isIdle(yield* Ref.get(state))) return yield* new Busy({ reason: "canvas can only change while both queues are empty and nothing is playing" })
        const sent = yield* Effect.result(sendReconciled(commands.setCanvas, { aspect: canvas }))
        if (Result.isFailure(sent)) {
          const error = sent.failure
          if (error._tag !== "Uncertain") return yield* error
          if (!(yield* stateEvidence((update) => update.aspect === canvas))) return yield* error
        } else {
          const reply = sent.success
          if (Option.isSome(reply) && reply.value._tag === "CommandError") {
            const reason = reply.value.error.reason ?? reply.value.error.message ?? "refused"
            return yield* (/queue|playing|busy|empty/i.test(reason) ? new Busy({ reason }) : new Rejected({ command: commands.setCanvas, code: reply.value.error.code, reason }))
          }
          const acknowledged = Option.isSome(reply) && reply.value._tag === "CanvasAccepted"
            && (reply.value.aspect === undefined || reply.value.aspect === canvas)
          if (!acknowledged && !(yield* stateEvidence((update) => update.aspect === canvas))) {
            return yield* new Uncertain({ command: commands.setCanvas, code: "missing_ack", reason: "canvas change was not positively confirmed" })
          }
        }
        yield* Ref.update(state, (s) => ({ ...s, canvas: Option.some(canvas) }))
      })))

    const uploader = yield* makeReferenceUploader(transport.uploadFile, {
      profile,
      load: { maxBytes: options.references?.maxBytes ?? profile.references.maxBytes, timeoutMs: options.references?.timeoutMs ?? 20_000 },
      ...(options.references?.stagingDirectory === undefined ? {} : { stagingDirectory: options.references.stagingDirectory })
    }).pipe(Effect.mapError((cause) => new SessionSetupError({ reason: `reference staging setup failed: ${cause.reason}`, cause })))

    const prepare = (input: ClipRequest, hooks: EnqueueHooks = {}): Effect.Effect<Submission.Submission<ClipId, EnqueueError>, EnqueueError> =>
      Effect.gen(function*() {
        // Capture a validated, detached request before the inert handle exists.
        // Later caller mutation therefore cannot change what submit dispatches.
        const request = yield* captureRequest(input)
        // Allocate correlation identity without registering a remote operation.
        // A Submission is inert until submit reaches its commit point.
        const seq = yield* Ref.updateAndGet(tokenSeq, (n) => n + 1)
        const token = `t${seq}`
        const submission = yield* Submission.make({
          id: token,
          prepare: guard(Effect.gen(function*() {
            // Everything here remains caller-owned and interruptible. Cancellation
            // can abandon reference IO without creating a remote enqueue.
            const metadata = encodeMetadata(request.metadata, token)
            if (metadata.length > profile.metadataMaxChars) {
              return yield* new InvalidRequest({ reason: `metadata is ${metadata.length} chars; the limit is ${profile.metadataMaxChars}` })
            }
            const bounds = yield* Ref.get(liveBounds)
            if (!isRequestableSeconds(profile, request.durationSeconds) || request.durationSeconds < bounds.min || request.durationSeconds > bounds.max) {
              return yield* new InvalidRequest({ reason: `clip length ${request.durationSeconds} is outside the session's ${bounds.min}–${bounds.max}s` })
            }
            if (request.references.length < profile.references.min || request.references.length > profile.references.max) {
              return yield* new InvalidRequest({ reason: `${request.references.length} references; the model takes ${profile.references.min}–${profile.references.max}` })
            }
            if (request.prompt.length === 0 || request.prompt.length > profile.prompt.maxChars) {
              return yield* new InvalidRequest({ reason: `prompt length ${request.prompt.length} is outside 1–${profile.prompt.maxChars}` })
            }
            if (request.startingFrame !== undefined && request.continueFrom !== undefined) {
              return yield* new InvalidRequest({ reason: "a clip cannot use both a starting frame and a continuation" })
            }

            const frameReferences: ReadonlyArray<readonly [string, ReferenceImage]> = [
              ...(request.startingFrame === undefined ? [] : [["starting_frame", request.startingFrame] as const]),
              ...(request.endingFrame === undefined ? [] : [["ending_frame", request.endingFrame] as const])
            ]
            const allReferences = [...request.references, ...frameReferences.map(([, ref]) => ref)]
            const preparedUploads = transport.prepareReferences === undefined
              ? (yield* uploader.prepare(allReferences).pipe(
                Effect.mapError((e) => e._tag === "ReferenceError" ? new InvalidRequest({ reason: `${e.uri}: ${e.reason}` }) : new Rejected({ command: "upload", reason: `${e.uri}: ${e.reason}` }))
              )).map((prepared) => prepared.upload)
              : yield* transport.prepareReferences(allReferences).pipe(Effect.mapError((e) => classifySendError(commands.enqueue, e)))
            const uploads = Object.fromEntries(frameReferences.map(([name], index) => [name, preparedUploads[request.references.length + index]!]))
            const args: Schema.JsonObject = {
              prompt: request.prompt,
              reference_images: preparedUploads.slice(0, request.references.length).map((upload) => ({ upload_id: upload.upload_id, name: upload.name, mime_type: upload.mime_type, size: upload.size })),
              seconds: request.durationSeconds,
              metadata,
              ...(request.seed === undefined ? {} : { seed: request.seed }),
              ...(request.position === undefined ? {} : { position: request.position }),
              ...(request.continueFrom === undefined ? {} : { continue_from_clip_id: request.continueFrom })
            }

            // Serialize the final admission check through the commit. Once this
            // permit is handed to execute, only the session-owned execution releases it.
            yield* Effect.acquireRelease(submit.take(1), () => submit.release(1))
            return yield* Effect.gen(function*() {
              const s = yield* Ref.get(state)
              if (request.continueFrom !== undefined && !s.continuable.includes(request.continueFrom)) {
                return yield* new InvalidRequest({ reason: `continue_from ${request.continueFrom} is not a continuable clip of this session (window ${profile.continuationWindow})` })
              }
              if (s.queued.length + (Option.isSome(s.building) ? 1 : 0) >= s.capacities.generation) {
                return yield* new Rejected({ command: commands.enqueue, code: "queue_full", reason: `generation queue holds ${s.capacities.generation}` })
              }
              return { args, uploads, frameReferences }
            })
          })),
          ...(hooks.commit === undefined ? {} : { commit: () => hooks.commit!(token) }),
          execute: ({ args, uploads, frameReferences }) => {
            const execution = Effect.gen(function*() {
            const pending = yield* Deferred.make<ClipInfo, Rejected | Uncertain>()
            // The token becomes observable no later than the dispatch itself.
            yield* Ref.update(pendingByToken, (m) => new Map(m).set(token, pending))
            yield* (transport.registerRequest?.(token, request).pipe(Effect.mapError((e) => classifySendError(commands.enqueue, e))) ?? Effect.void)
            const admittedAt = yield* nowMs
            const finish = (clip: ClipInfo) => lock.withPermits(1)(register(clip, request, admittedAt, seq)).pipe(
              Effect.catch((error) => failSession(error.reason).pipe(Effect.andThen(
                Effect.fail(new Uncertain({ command: commands.enqueue, reason: error.reason })))))
            )
            const reconcile = (why: string, original?: Uncertain) => Deferred.await(pending).pipe(
              Effect.timeoutOrElse({
                duration: reconcileWindowMs,
                orElse: () => {
                  const uncertainty = new Uncertain({
                    command: commands.enqueue,
                    reason: why,
                    ...(original?.code === undefined ? {} : { code: original.code }),
                    ...(original?.cause === undefined ? {} : { cause: original.cause })
                  })
                  return failSession(`enqueue outcome remained unresolved: ${why}`, uncertainty).pipe(
                    Effect.andThen(Effect.fail(uncertainty))
                  )
                }
              }),
              Effect.flatMap(finish)
            )

            const sent = yield* Effect.result(sendDecoded(commands.enqueue, args, frameReferences.length === 0 ? undefined : uploads))
            if (Result.isFailure(sent)) {
              const e = sent.failure
              if (e._tag === "SessionFailed") {
                yield* failSession(e.reason, e.cause)
                return yield* new Uncertain({ command: commands.enqueue, reason: e.reason })
              }
              if (e._tag === "Uncertain") return yield* reconcile(`${e.reason}; no broadcast proved acceptance within ${reconcileWindowMs}ms`, e)
              return yield* Effect.fail(e)
            }
            const reply = sent.success
            if (Option.isNone(reply)) return yield* reconcile("enqueue was acknowledged without clip_queued")
            switch (reply.value._tag) {
              case "ClipQueued":
                return yield* finish(reply.value.clip)
              case "CommandError":
                return yield* new Rejected({ command: commands.enqueue, code: reply.value.error.code, reason: reply.value.error.reason ?? reply.value.error.message ?? "refused" })
              default:
                return yield* reconcile(`enqueue replied with ${reply.value._tag}`)
            }
            }).pipe(Effect.ensuring(Effect.all([
              Ref.update(pendingByToken, (m) => { const n = new Map(m); n.delete(token); return n }),
              transport.releaseRequest?.(token) ?? Effect.void
            ], { discard: true })))
            if (hooks.result === undefined) return execution
            return Effect.result(execution).pipe(Effect.flatMap((result) =>
              hooks.result!(token, result).pipe(Effect.andThen(Result.isSuccess(result)
                ? Effect.succeed(result.success)
                : Effect.fail(result.failure)))
            ))
          }
        }).pipe(Scope.provide(scope))
        return submission
      })

    const enqueue = (request: ClipRequest): Effect.Effect<ClipId, EnqueueError> =>
      prepare(request).pipe(Effect.flatMap((submission) => submission.submit))

    const remove = (clipId: ClipId): Effect.Effect<RemoveOutcome, NotFound | CommandError> =>
      guard(submit.withPermits(1)(Effect.gen(function*() {
        const s = yield* Ref.get(state)
        const where: RemoveOutcome | undefined = s.queued.some((r) => r.clipId === clipId)
          ? "unstarted"
          : Option.exists(s.building, (b) => b.record.clipId === clipId)
          ? "in_flight"
          : s.ready.some((r) => r.clipId === clipId)
          ? "ready"
          : undefined
        if (where === undefined) return yield* new NotFound({ clipId })
        const sent = yield* Effect.result(sendReconciled(commands.pop, { clip_id: clipId }))
        if (Result.isSuccess(sent) && Option.isSome(sent.success) && sent.success.value._tag === "CommandError") {
          return yield* new Rejected({ command: commands.pop, code: sent.success.value.error.code, reason: sent.success.value.error.reason ?? sent.success.value.error.message ?? "refused" })
        }
        if (Result.isFailure(sent) && sent.failure._tag !== "Uncertain") return yield* sent.failure
        const snapshot = yield* sendDecoded(commands.getQueue, {}).pipe(Effect.mapError((e) => e._tag === "InvalidRequest" ? new Rejected({ command: commands.getQueue, reason: e.reason }) : e))
        if (Option.isNone(snapshot) || snapshot.value._tag !== "QueueUpdate" || snapshot.value.update.generation === undefined || snapshot.value.update.playout === undefined) return yield* new Uncertain({ command: commands.pop, reason: "removal queue snapshot unavailable" })
        if (snapshot.value.update.playout.some((c) => c.clip_id === clipId) || snapshot.value.update.generation.some((c) => c.clip_id === clipId)) return yield* new Uncertain({ command: commands.pop, reason: "clip remains queued after removal" })
        yield* lock.withPermits(1)(Effect.gen(function*() {
          if (where === "in_flight") {
            // The GPUs cannot abandon the build; it is discarded when it finishes. Until the provider shows
            // the builder moved on, the builder is still busy with it.
            yield* Ref.update(discarded, (d) => new Set(d).add(clipId))
            return
          }
          const s1 = yield* Ref.get(state)
          const { state: s2, promoted } = promoteBuilding({
            ...s1,
            queued: s1.queued.filter((r) => r.clipId !== clipId),
            ready: s1.ready.filter((r) => r.clipId !== clipId)
          })
          yield* Ref.set(state, s2)
          if (promoted !== undefined) yield* emit(EngineEvent.Building({ clipId: promoted.clipId, startedAt: Option.none() }))
        }))
        return where
      })))

    const setAutoplay = (enabled: boolean) => guard(submit.withPermits(1)(Effect.gen(function* () {
      const sent = yield* Effect.result(sendReconciled(commands.setAutoplay, { enabled }))
      if (Result.isFailure(sent)) {
        if (sent.failure._tag !== "Uncertain") return yield* sent.failure
        if (!(yield* stateEvidence((update) => update.autoplay === enabled))) return yield* sent.failure
      } else {
        const reply = sent.success
        if (Option.isSome(reply) && reply.value._tag === "CommandError") return yield* new Rejected({ command: commands.setAutoplay, reason: reply.value.error.reason ?? "refused" })
        const acknowledged = Option.isSome(reply) && reply.value._tag === "AutoplayAccepted" && reply.value.enabled === enabled
        if (!acknowledged && !(yield* stateEvidence((update) => update.autoplay === enabled))) {
          return yield* new Uncertain({ command: commands.setAutoplay, code: "missing_ack", reason: "autoplay state not confirmed" })
        }
      }
      autoplayEnabled = enabled
    })))
    const move = (clipId: ClipId, position: number, queue: "generation" | "playout") => guard(submit.withPermits(1)(Effect.gen(function* () {
      const s = yield* Ref.get(state)
      const field = queue === "generation" ? "queued" : "ready"
      if (!s[field].some((r) => r.clipId === clipId)) return yield* new NotFound({ clipId })
      const sent = yield* Effect.result(sendReconciled(commands.move, { clip_id: clipId, position, queue }))
      if (Result.isSuccess(sent) && Option.isSome(sent.success) && sent.success.value._tag === "CommandError") return yield* new Rejected({ command: commands.move, reason: sent.success.value.error.reason ?? "refused" })
      if (Result.isFailure(sent) && sent.failure._tag !== "Uncertain") return yield* sent.failure
      // An ack alone proves nothing. Read the queue before trusting the edit.
      const snapshot = yield* sendDecoded(commands.getQueue, {}).pipe(Effect.mapError((e) => e._tag === "InvalidRequest" ? new Rejected({ command: commands.getQueue, reason: e.reason }) : e))
      if (Option.isSome(snapshot) && snapshot.value._tag === "QueueUpdate") yield* lock.withPermits(1)(applyMessage({ type: "queue_update", data: snapshot.value.update }))
      const actual = (yield* Ref.get(state))[field]
      if (actual[position]?.clipId !== clipId) return yield* new Uncertain({ command: commands.move, reason: "requested position not confirmed by queue_update" })
    })))
    const stop = Effect.gen(function* () {
      yield* setAutoplay(false)
      return yield* submit.withPermits(1)(Effect.gen(function* () {
        const sent = yield* Effect.result(sendReconciled(commands.stop, {}))
        if (Result.isSuccess(sent) && Option.isSome(sent.success) && sent.success.value._tag === "ClipStopped") {
          yield* lock.withPermits(1)(applyLifecycle(sent.success.value))
          return
        }
        if (Result.isSuccess(sent) && Option.isSome(sent.success) && sent.success.value._tag === "CommandError") {
          return yield* new Rejected({ command: commands.stop, reason: sent.success.value.error.reason ?? "refused" })
        }
        if (Result.isFailure(sent) && sent.failure._tag !== "Uncertain") return yield* sent.failure
        if (!(yield* stateEvidence((update) => update.playing === false || update.playing_clip_id === null))) {
          return yield* (Result.isFailure(sent)
            ? sent.failure
            : new Uncertain({ command: commands.stop, code: "missing_ack", reason: "clip_stopped not confirmed" }))
        }
      }))
    })

    const engine: ClipEngineShape = {
      prepare,
      enqueue,
      move,
      setAutoplay,
      stop,
      remove,
      setCanvas,
      state: Ref.get(state),
      events: events.stream({ capacity: 256, maxBytes: 1_048_576 }),
      failure: Deferred.await(fatal)
    }

    // Presentation is a prerequisite to admission. Provider autoplay can then
    // join already generated clips without another control round trip.
    if (options.canvas !== undefined) {
      yield* setCanvas(options.canvas).pipe(Effect.mapError((e) => new SessionSetupError({ reason: `canvas: ${e._tag} ${"reason" in e ? e.reason : ""}`, cause: e })))
    }
    const autoplay = yield* sendReconciled(commands.setAutoplay, { enabled: true }).pipe(Effect.result)
    if (Result.isFailure(autoplay) || (Option.isSome(autoplay.success) && (autoplay.success.value._tag === "CommandError"
      || (autoplay.success.value._tag === "AutoplayAccepted" && autoplay.success.value.enabled === false)))) {
      return yield* new SessionSetupError({ reason: "could not enable provider autoplay", cause: Result.isFailure(autoplay) ? autoplay.failure : autoplay.success })
    }
    if (options.holdLastFrame ?? true) {
      const flush = yield* sendReconciled(commands.setFlushOnClipEnd, { enabled: false }).pipe(Effect.result)
      if (Result.isFailure(flush) || (Option.isSome(flush.success) && flush.success.value._tag === "CommandError")) {
        yield* log("set_flush_on_clip_end refused or unsupported; boundaries will flush to black")
      }
    }

    const session: ReactorSession = {
      engine,
      media: { video: transport.video, audio: transport.audio, pressure: transport.snapshot, videoFramesPerSecond: profile.fps },
      prepareEnqueue: prepare,
      sessionId: Ref.get(sessionId),
      cleanup: Ref.get(cleanupReport),
      validCommands: Ref.get(validCommands)
    }
    return session
  })

export class ReactorSessionHandle extends Context.Service<ReactorSessionHandle, ReactorSession>()("reactor-effect-client/ReactorSession") {}

/** One session behind `ClipEngine`, `ReactorMedia` and the session handle; requires a `ReactorTransport`. */
export const layer = (options: SessionOptions): Layer.Layer<
  ClipEngine | ReactorMedia | ReactorSessionHandle,
  SessionSetupError,
  ReactorTransport | FileSystem.FileSystem | Path.Path | Crypto.Crypto | HttpClient.HttpClient
> => {
  const session = Layer.effect(
    ReactorSessionHandle,
    Effect.gen(function*() {
      const transport = yield* ReactorTransport
      return yield* make(transport, options)
    })
  )
  const engine = Layer.effect(ClipEngine, Effect.gen(function*() { return (yield* ReactorSessionHandle).engine }))
  const media = Layer.effect(ReactorMedia, Effect.gen(function*() { return (yield* ReactorSessionHandle).media }))
  return Layer.provideMerge(Layer.merge(engine, media), session)
}
