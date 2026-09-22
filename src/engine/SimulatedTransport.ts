import { Clock, Deferred, Effect, Layer, Option, Queue, Scope, Stream } from "effect"
import type { ClipId, ClipRequest, ReferenceImage } from "../Clip.js"
import { alignFrames, alignSecondsTo, h3ReferenceTurboRealtime, type ModelProfile } from "../ModelProfile.js"
import { ReactorError, type AudioFrame, type ControlEvent, type Snapshot, type UploadReference, type VideoFrame } from "../Model.js"
import { type BuildTiming, type ClipRecord } from "./Engine.js"
import { decodeMetadata } from "./Metadata.js"
import { layer as sessionLayer, ReactorTransport, type TransportShape } from "./Session.js"
import { commands } from "./Wire.js"

export interface SimulatedFaults {
  readonly buildFails?: (seq: number) => boolean
  readonly sessionFails?: (seq: number) => boolean
}

export interface SimulatedMediaSink {
  readonly video: (frame: VideoFrame) => Effect.Effect<void>
  readonly audio: (frame: AudioFrame) => Effect.Effect<void>
}

export interface SimOptions {
  readonly buildRatio: number
  readonly buildFixedMs: number
  readonly profile?: ModelProfile
  readonly queueLimit?: number
  readonly playoutLimit?: number
  readonly timing?: "measured" | "unknown"
  readonly playoutGapMs?: number
  readonly faults?: SimulatedFaults
  /** Optional local renderer. It completes before Ready and returns the corrected duration. */
  readonly build?: (record: ClipRecord) => Effect.Effect<number, Error>
  /** Optional media emitter. It runs only after the same Session lifecycle emits Started. */
  readonly present?: (record: ClipRecord, startedAt: number, sink: SimulatedMediaSink) => Effect.Effect<void, Error>
  /** Release renderer-owned artifacts when a built clip is discarded before presentation. */
  readonly discard?: (record: ClipRecord) => Effect.Effect<void>
}

interface SimClip {
  readonly record: ClipRecord
  info: {
    readonly clip_id: string
    readonly metadata: string
    readonly prompt: string
    readonly has_starting_frame: boolean
    readonly has_ending_frame: boolean
    frames: number
    seconds: number
    ready: boolean
  }
  popped: boolean
}

const providerError = (code: ReactorError["code"], message: string) => new ReactorError({ code, message })
const nativeTimeout = () => new ReactorError({
  code: "Native",
  message: "simulated connection lost after command submission",
  context: { outcome: "unknown" },
  nativeError: { code: "REQUEST_TIMEOUT", message: "control request timed out", recoverable: true, operation: "send_command" }
})

const schema = {
  openapi: "3.1.0",
  info: { title: "simulated h3-reference-to-video-turbo-realtime", version: "local" },
  "x-commands": ["enqueue", "pop", "move", "stop", "get_queue", "set_autoplay", "set_canvas", "set_flush_on_clip_end", "reset", "get_state"],
  "x-command-parameters": { enqueue: ["prompt", "reference_images", "seconds", "metadata", "continue_from_clip_id"] },
  "x-messages": ["clip_queued", "clip_generated", "clip_failed", "clip_started", "clip_finished", "clip_stopped", "clip_popped", "clip_moved", "queue_update", "state_update", "command_error", "autoplay_accepted", "canvas_accepted", "flush_accepted", "session_reset"]
}

/** Provider-shaped local transport. Queue simulation lives here; Session remains the sole public engine/lifecycle fold. */
export const make = (options: SimOptions): Effect.Effect<TransportShape, never, Scope.Scope> => Effect.gen(function* () {
  const scope = yield* Effect.scope
  const profile = options.profile ?? h3ReferenceTurboRealtime
  const generationCapacity = options.queueLimit ?? profile.expectedCapacities.generation
  const playoutCapacity = options.playoutLimit ?? profile.expectedCapacities.playout
  const control = yield* Queue.unbounded<ControlEvent, ReactorError>()
  const video = yield* Queue.bounded<VideoFrame>(4)
  const audio = yield* Queue.bounded<AudioFrame>(8)
  const buildSignal = yield* Queue.unbounded<void>()
  const playSignal = yield* Queue.unbounded<void>()
  const prepared = new Map<string, ClipRequest>()
  const timings = new Map<ClipId, BuildTiming>()
  const referenceIds = new Map<string, string>()
  let generation: SimClip[] = []
  let playout: SimClip[] = []
  let playing: SimClip | undefined
  let building: SimClip | undefined
  let currentStop: Deferred.Deferred<void> | undefined
  let sequence = 0
  let uploadSequence = 0
  let autoplay = false
  let canvas = "16:9"
  let closed = false
  let deliveredVideo = 0n
  let deliveredAudio = 0n

  const clipJson = (clip: SimClip) => ({
    ...clip.info,
    continue_from_clip_id: clip.record.request.continueFrom ?? null,
    seed: clip.record.request.seed ?? 1,
    has_reference_image: clip.record.request.references.length > 0,
    reference_image_count: clip.record.request.references.length
  })
  const message = (type: string, clip: SimClip, extra: Record<string, unknown> = {}) => ({ type, data: { clip: clipJson(clip), ...extra } })
  const queueUpdate = () => ({ type: "queue_update", data: {
    generation: generation.filter((clip) => !clip.popped).map(clipJson),
    playout: playout.filter((clip) => !clip.popped).map(clipJson),
    history: []
  } })
  const stateUpdate = () => ({ type: "state_update", data: {
    playing: playing !== undefined,
    playing_clip_id: playing?.info.clip_id ?? null,
    autoplay,
    flush_on_clip_end: false,
    generation_queued: generation.filter((clip) => !clip.popped).length,
    generation_capacity: generationCapacity,
    playout_queued: playout.filter((clip) => !clip.popped).length,
    playout_capacity: playoutCapacity,
    clip_seconds: 10,
    clip_seconds_min: profile.requestSeconds.min,
    clip_seconds_max: profile.requestSeconds.max,
    aspect: canvas,
    width: 1344,
    height: 768,
    clips_played: 0,
    seconds_sent: 0,
    valid_commands: ["enqueue", "pop", "move", "stop", "get_queue", "set_autoplay", "set_canvas", "reset", "get_state"]
  } })
  const emit = (raw: unknown) => Queue.offer(control, { _tag: "Message", message: raw as never }).pipe(Effect.asVoid)
  const sink: SimulatedMediaSink = {
    video: (frame) => Queue.offer(video, frame).pipe(Effect.tap(() => Effect.sync(() => { deliveredVideo++ })), Effect.asVoid),
    audio: (frame) => Queue.offer(audio, frame).pipe(Effect.tap(() => Effect.sync(() => { deliveredAudio++ })), Effect.asVoid)
  }

  const builder = Effect.gen(function* () {
    while (true) {
      const next = generation.find((clip) => !clip.popped)
      if (building !== undefined || next === undefined || playout.length >= playoutCapacity) {
        yield* Queue.take(buildSignal)
        continue
      }
      building = next
      const startedAt = yield* Clock.currentTimeMillis
      const built = yield* Effect.result(options.build === undefined
        ? Effect.sleep(options.buildFixedMs + options.buildRatio * next.record.durationSeconds * 1000).pipe(Effect.as(next.record.durationSeconds))
        : options.build(next.record))
      const finishedAt = yield* Clock.currentTimeMillis
      generation = generation.filter((clip) => clip !== next)
      building = undefined
      if (options.timing !== "unknown") timings.set(next.record.clipId, { _tag: "Measured", buildMs: finishedAt - startedAt })
      if (next.popped) {
        if (options.discard !== undefined) yield* options.discard(next.record)
        yield* Queue.offer(buildSignal, undefined)
        continue
      }
      if (built._tag === "Failure" || options.faults?.buildFails?.(next.record.seq) === true) {
        yield* emit(message("clip_failed", next, { reason: built._tag === "Failure" ? built.failure.message : "simulated build failure" }))
        yield* Queue.offer(buildSignal, undefined)
        continue
      }
      const seconds = Math.ceil(Math.max(profile.requestSeconds.min, built.success) * profile.fps) / profile.fps
      next.info = { ...next.info, seconds, frames: Math.round(seconds * profile.fps), ready: true }
      playout.push(next)
      yield* emit(message("clip_generated", next))
      yield* Queue.offer(playSignal, undefined)
      yield* Queue.offer(buildSignal, undefined)
    }
  })

  const player = Effect.gen(function* () {
    while (true) {
      const next = autoplay && playing === undefined ? playout.find((clip) => !clip.popped) : undefined
      if (next === undefined) {
        yield* Queue.take(playSignal)
        continue
      }
      playout = playout.filter((clip) => clip !== next)
      playing = next
      const startedAt = yield* Clock.currentTimeMillis
      const stopped = yield* Deferred.make<void>()
      currentStop = stopped
      yield* emit(message("clip_started", next))
      yield* Queue.offer(buildSignal, undefined)
      const presentation = options.present === undefined
        ? Effect.sleep(next.info.seconds * 1000)
        : options.present({ ...next.record, durationSeconds: next.info.seconds }, startedAt, sink)
      const result = yield* Effect.result(Effect.raceFirst(presentation, Deferred.await(stopped)))
      currentStop = undefined
      if (playing !== next) continue
      playing = undefined
      if (result._tag === "Failure") yield* emit(message("clip_failed", next, { reason: result.failure instanceof Error ? result.failure.message : String(result.failure) }))
      else yield* emit(message("clip_finished", next))
      yield* Queue.offer(buildSignal, undefined)
      if ((options.playoutGapMs ?? 0) > 0) yield* Effect.sleep(options.playoutGapMs!)
      yield* Queue.offer(playSignal, undefined)
    }
  })

  yield* Effect.forkScoped(builder)
  yield* Effect.forkScoped(player)

  const uuid = (n: number): string => `00000000-0000-4000-8000-${n.toString(16).padStart(12, "0")}`
  const upload = (key: string): UploadReference => {
    const id = referenceIds.get(key) ?? uuid(++uploadSequence)
    referenceIds.set(key, id)
    return { upload_id: id, name: "simulated.png", mime_type: "image/png", size: 0 }
  }
  const send: TransportShape["send"] = (command, args = {}, uploads) => Effect.gen(function* () {
    if (closed) return yield* providerError("Closed", "simulated transport is closed")
    switch (command) {
      case commands.enqueue: {
        const metadata = typeof args.metadata === "string" ? args.metadata : ""
        const decoded = decodeMetadata(metadata)
        if (Option.isNone(decoded)) return yield* providerError("Protocol", "simulated enqueue metadata has no request token")
        const request = prepared.get(decoded.value.token)
        if (request === undefined) return yield* providerError("Protocol", "simulated enqueue has no prepared ClipRequest")
        if (generation.filter((clip) => !clip.popped).length >= generationCapacity) {
          return { type: "command_error", data: { command, code: "queue_full", reason: "generation queue is full" } }
        }
        const seq = ++sequence
        if (options.faults?.sessionFails?.(seq) === true) {
          // The caller first observes that this sent command has no trustworthy reply.
          // The transport's terminal failure is a separate later observation, matching
          // a real control connection that dies after an in-flight request times out.
          yield* Effect.sleep(200).pipe(
            Effect.andThen(Queue.fail(control, providerError("Closed", "simulated transport loss"))),
            Effect.forkIn(scope)
          )
          return yield* nativeTimeout()
        }
        const seconds = alignSecondsTo(profile, typeof args.seconds === "number" ? args.seconds : request.durationSeconds)
        const record: ClipRecord = { clipId: uuid(0x100000 + seq) as ClipId, seq, durationSeconds: seconds, request, enqueuedAt: yield* Clock.currentTimeMillis }
        const clip: SimClip = { record, popped: false, info: {
          clip_id: record.clipId,
          frames: alignFrames(profile, seconds),
          seconds,
          metadata,
          prompt: typeof args.prompt === "string" ? args.prompt : request.prompt,
          ready: false,
          has_starting_frame: uploads?.starting_frame !== undefined,
          has_ending_frame: uploads?.ending_frame !== undefined
        } }
        const position = typeof args.position === "number" ? Math.max(0, Math.floor(args.position)) : generation.length
        generation.splice(Math.min(position, generation.length), 0, clip)
        prepared.delete(decoded.value.token)
        yield* Queue.offer(buildSignal, undefined)
        return message("clip_queued", clip)
      }
      case commands.pop: {
        const id = typeof args.clip_id === "string" ? args.clip_id : ""
        const clip = [...generation, ...playout].find((entry) => entry.info.clip_id === id)
        if (clip === undefined) return { type: "command_error", data: { command, reason: `unknown clip ${id}` } }
        clip.popped = true
        if (building !== clip) generation = generation.filter((entry) => entry !== clip)
        if (playout.includes(clip)) {
          playout = playout.filter((entry) => entry !== clip)
          if (options.discard !== undefined) yield* options.discard(clip.record)
        }
        yield* Queue.offer(buildSignal, undefined)
        return message("clip_popped", clip)
      }
      case commands.move: {
        const id = typeof args.clip_id === "string" ? args.clip_id : ""
        const queue = args.queue === "generation" ? generation : playout
        const index = queue.findIndex((clip) => clip.info.clip_id === id && !clip.popped)
        if (index < 0) return { type: "command_error", data: { command, reason: "unknown clip" } }
        const [clip] = queue.splice(index, 1)
        queue.splice(Math.max(0, Math.min(typeof args.position === "number" ? Math.floor(args.position) : 0, queue.length)), 0, clip!)
        return message("clip_moved", clip!)
      }
      case commands.stop: {
        const clip = playing
        if (clip === undefined) return { type: "command_error", data: { command, reason: "nothing playing" } }
        playing = undefined
        if (currentStop !== undefined) yield* Deferred.succeed(currentStop, undefined)
        return message("clip_stopped", clip)
      }
      case commands.getQueue:
        return queueUpdate()
      case commands.getState:
        return stateUpdate()
      case commands.setAutoplay:
        autoplay = args.enabled === true
        yield* Queue.offer(playSignal, undefined)
        return { type: "autoplay_accepted", data: { enabled: autoplay } }
      case commands.setCanvas:
        canvas = typeof args.aspect === "string" ? args.aspect : canvas
        return { type: "canvas_accepted", data: { aspect: canvas, width: 1344, height: 768 } }
      case commands.setFlushOnClipEnd:
        return { type: "flush_accepted", data: { enabled: args.enabled === true } }
      case commands.reset:
        generation = []
        playout = []
        if (playing !== undefined && currentStop !== undefined) yield* Deferred.succeed(currentStop, undefined)
        playing = undefined
        building = undefined
        return { type: "session_reset", data: { cleared_clips: 0, was_playing: false } }
      default:
        return { type: "command_error", data: { command, reason: "unsupported simulated command" } }
    }
  })

  const transport: TransportShape = {
    connect: () => Effect.gen(function* () {
      closed = false
      yield* Queue.offer(control, { _tag: "Session", sessionId: "simulated-session" })
      yield* Queue.offer(control, { _tag: "Status", status: "ready" })
      yield* emit(stateUpdate())
      yield* emit(queueUpdate())
    }),
    reconnect: Effect.void,
    send,
    uploadFile: (path) => Effect.succeed(upload(path)),
    requestSchema: Effect.succeed(schema),
    events: Stream.fromQueue(control),
    video: Stream.fromQueue(video),
    audio: Stream.fromQueue(audio),
    snapshot: Effect.gen(function* () {
      const queuedVideo = yield* Queue.size(video)
      const queuedAudio = yield* Queue.size(audio)
      return { closed, queuedControl: 0, queuedVideo, queuedAudio, queuedBytes: 0, droppedVideo: 0n, droppedAudio: 0n, pendingRequests: 0, deliveredVideo, deliveredAudio } satisfies Snapshot
    }),
    disconnect: Effect.sync(() => { closed = true }),
    registerRequest: (token, request) => Effect.sync(() => { prepared.set(token, request) }),
    releaseRequest: (token) => Effect.sync(() => { prepared.delete(token) }),
    prepareReferences: (references: ReadonlyArray<ReferenceImage>) => Effect.succeed(references.map((reference) => upload(reference.uri))),
    buildTiming: (clipId) => Effect.sync(() => Option.fromUndefinedOr(timings.get(clipId)))
  }
  return transport
})

export const transportLayer = (options: SimOptions): Layer.Layer<ReactorTransport> =>
  Layer.effect(ReactorTransport, make(options))

/** The same Session engine/media services used by the native adapter, backed by the provider-shaped local transport. */
export const layerSim = (options: SimOptions) =>
  sessionLayer({
    ...(options.profile === undefined ? {} : { profile: options.profile }),
    commandTimeoutMs: 1_000,
    setupTimeoutMs: 2_000,
    reconcileWindowMs: 100,
    verifyDeployment: true
  }).pipe(Layer.provide(transportLayer(options)))
