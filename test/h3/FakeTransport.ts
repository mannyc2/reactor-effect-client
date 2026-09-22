/**
 * A scripted Reactor transport for adapter tests. It speaks the same
 * `{ type, data }` envelopes the native client delivers and answers commands
 * with hand-authored fixtures following the published H3 Reference Turbo
 * Realtime schema page (clip messages carry `data.clip`, `clip_failed` adds
 * `reason`, `clip_finished` adds `seconds_sent`, `state_update` carries
 * `playing`/`playing_clip_id`/capacities/length bounds, `queue_update` lists
 * `generation` (waiting and in-flight) / `playout` / an always-empty
 * `history`, refusals are `command_error { command, reason }`). They are NOT
 * captured live evidence; update them from a real deployment's `requestSchema`
 * document when an authorized live test supplies one.
 */
import { Deferred, Effect, Queue, Ref, Result, Schema, Stream } from "effect"
import { randomUUID } from "node:crypto"
import { alignFrames, h3ReferenceTurboRealtime } from "../../src/ModelProfile.js"
import { type ControlEvent, ReactorError, type UploadReference } from "../../src/Model.js"
import type { TransportShape } from "../../src/engine/Session.js"
import { decodeMessage } from "../../src/engine/Wire.js"

export interface ClipFixture {
  readonly clip_id: string
  readonly frames: number
  readonly seconds: number
  readonly metadata: string
  readonly prompt: string
  readonly ready: boolean
  readonly has_starting_frame?: boolean
  readonly has_ending_frame?: boolean
}

export interface SendContext {
  readonly fake: Fake
  readonly defaults: (command: string, args: Schema.JsonObject, uploads?: Readonly<Record<string, UploadReference>>) => Effect.Effect<Schema.Json | undefined, ReactorError>
  readonly uploads?: Readonly<Record<string, UploadReference>>
}
export type SendHandler = (args: Schema.JsonObject, context: SendContext) => Effect.Effect<Schema.Json | undefined, ReactorError>

export interface FakeScript {
  readonly send?: Readonly<Record<string, SendHandler>>
  readonly schema?: Schema.Json
  readonly connect?: (fake: Fake) => Effect.Effect<void, ReactorError>
  readonly reconnect?: (fake: Fake) => Effect.Effect<void, ReactorError>
  readonly upload?: (path: string) => Effect.Effect<UploadReference, ReactorError>
}

export interface Fake {
  readonly transport: TransportShape
  /** Raw provider delivery, including missing/reordered lifecycle events.
   * Tests send starts and finishes explicitly; no local command invents them. */
  readonly emit: (message: Schema.Json) => Effect.Effect<void>
  readonly emitControl: (event: ControlEvent) => Effect.Effect<void>
  readonly failControl: (error: ReactorError) => Effect.Effect<void>
  readonly calls: Effect.Effect<ReadonlyArray<{ readonly command: string; readonly args: Schema.JsonObject; readonly uploads?: Readonly<Record<string, UploadReference>> }>>
  readonly uploads: Effect.Effect<ReadonlyArray<string>>
  readonly accepted: Effect.Effect<ReadonlyArray<ClipFixture>>
  readonly disconnects: Effect.Effect<number>
  /** A clip message as the provider echoes it: the full ClipInfo under `data.clip`. */
  readonly clipMessage: (type: string, clip: ClipFixture, extra?: Record<string, Schema.Json>) => Schema.Json
  /** The documented refusal path: the command resolved with no reply and this broadcast names it. */
  readonly refuse: (command: string, reason: string) => Effect.Effect<void>
  /** A documented `state_update` snapshot. */
  readonly stateUpdate: (overrides?: Record<string, Schema.Json>) => Schema.Json
}

const json = (value: Schema.Json): Schema.Json => value

export const nativeRejection = (code: string, message: string, status?: number) =>
  new ReactorError({
    code: "Native",
    message,
    context: { outcome: /TIMEOUT|DISCONNECT|CONNECTION|CANCEL/i.test(code) ? "unknown" : "replied" },
    nativeError: { code, message, recoverable: true, operation: "send_command", ...(status === undefined ? {} : { status }) }
  })

export const defaultSchema: Schema.Json = {
  openapi: "3.1.0",
  info: { title: "h3-reference-to-video-turbo-realtime (fixture)", version: "fixture" },
  "x-commands": ["enqueue", "pop", "move", "stop", "set_autoplay", "set_canvas", "reset", "get_state", "get_queue"],
  "x-command-parameters": { enqueue: ["prompt", "reference_images", "seconds", "metadata", "continue_from_clip_id"] },
  "x-messages": ["clip_queued", "clip_generated", "clip_failed", "clip_started", "clip_finished", "clip_stopped", "clip_popped", "queue_update", "state_update", "command_error", "autoplay_accepted", "canvas_accepted", "session_reset"]
}

export const makeFake = (script: FakeScript = {}): Effect.Effect<Fake> =>
  Effect.gen(function*() {
    const control = yield* Queue.unbounded<ControlEvent, ReactorError>()
    const calls = yield* Ref.make<ReadonlyArray<{ command: string; args: Schema.JsonObject; uploads?: Readonly<Record<string, UploadReference>> }>>([])
    const uploads = yield* Ref.make<ReadonlyArray<string>>([])
    const accepted = yield* Ref.make<ReadonlyArray<ClipFixture>>([])
    const disconnects = yield* Ref.make(0)
    const uploadIds = new Map<string, string>()
    let autoplay = false
    let generation: ClipFixture[] = [], playout: ClipFixture[] = []
    let playing: ClipFixture | undefined
    const popped = new Set<string>()

    const clipInfo = (clip: ClipFixture): Schema.Json => ({ ...clip, seed: 7, has_reference_image: true, reference_image_count: 1 })
    const clipMessage = (type: string, clip: ClipFixture, extra: Record<string, Schema.Json> = {}): Schema.Json => ({ type, data: { clip: clipInfo(clip), ...extra } })
    const stateUpdate = (overrides: Record<string, Schema.Json> = {}): Schema.Json => ({
      type: "state_update",
      data: {
        playing: playing !== undefined, playing_clip_id: playing?.clip_id ?? null, autoplay, flush_on_clip_end: true,
        generation_queued: generation.length, generation_capacity: 20, playout_queued: playout.length, playout_capacity: 10,
        clip_seconds: 10, clip_seconds_min: 5.0, clip_seconds_max: 15.084, seed: 1,
        aspect: "16:9", width: 1344, height: 768, clips_played: 0, seconds_sent: 0,
        valid_commands: ["enqueue", "set_canvas", "set_autoplay", "reset", "get_state"],
        ...overrides
      }
    })
    const emit = (message: Schema.Json) => Effect.gen(function* () {
      const decoded = decodeMessage(message)
      if (Result.isSuccess(decoded) && "clip" in decoded.success) {
        const event = decoded.success, id = event.clip.clip_id
        const clip = (yield* Ref.get(accepted)).find((c) => c.clip_id === id)
        if (clip !== undefined) switch (event._tag) {
          case "ClipGenerated": generation = generation.filter((c) => c.clip_id !== id); if (!popped.has(id) && !playout.some((c) => c.clip_id === id)) playout.push({ ...clip, ready: true }); break
          case "ClipStarted": playout = playout.filter((c) => c.clip_id !== id); playing = clip; break
          case "ClipFinished": case "ClipStopped": if (playing?.clip_id === id) playing = undefined; break
          case "ClipFailed": generation = generation.filter((c) => c.clip_id !== id); playout = playout.filter((c) => c.clip_id !== id); break
        }
      }
      yield* Queue.offer(control, { _tag: "Message", message })
    })
    const refuse = (command: string, reason: string) => emit({ type: "command_error", data: { command, reason } })
    const emitControl = (event: ControlEvent) => Queue.offer(control, event).pipe(Effect.asVoid)

    const defaults = (command: string, args: Schema.JsonObject, attachments?: Readonly<Record<string, UploadReference>>): Effect.Effect<Schema.Json | undefined, ReactorError> =>
      Effect.gen(function*() {
        const reply: Schema.Json | undefined = yield* Effect.gen(function*() {
          switch (command) {
          case "enqueue": {
            const seconds = typeof args.seconds === "number" ? args.seconds : 10
            const frames = alignFrames(h3ReferenceTurboRealtime, seconds)
            const clip: ClipFixture = {
              clip_id: randomUUID(),
              frames,
              seconds: frames / h3ReferenceTurboRealtime.fps,
              metadata: typeof args.metadata === "string" ? args.metadata : "",
              prompt: typeof args.prompt === "string" ? args.prompt : "",
              ready: false,
              has_starting_frame: attachments?.starting_frame !== undefined,
              has_ending_frame: attachments?.ending_frame !== undefined
            }
            yield* Ref.update(accepted, (a) => [...a, clip])
            generation.splice(typeof args.position === "number" ? args.position : generation.length, 0, clip)
            return json({ type: "clip_queued", data: { clip: clipInfo(clip) } })
          }
          case "pop": {
            const id = typeof args.clip_id === "string" ? args.clip_id : ""
            const clip = (yield* Ref.get(accepted)).find((c) => c.clip_id === id)
            if (clip === undefined) {
              // documented refusal: no reply, broadcast command_error
              yield* refuse("pop", `unknown clip ${id}`)
              return undefined
            }
            popped.add(id); generation = generation.filter((c) => c.clip_id !== id); playout = playout.filter((c) => c.clip_id !== id)
            return json({ type: "clip_popped", data: { clip: clipInfo(clip) } })
          }
          case "get_queue": return { type: "queue_update", data: { generation: generation.map(clipInfo), playout: playout.map(clipInfo), history: [] } }
          case "move": {
            const queue = args.queue === "generation" ? generation : playout
            const index = queue.findIndex((c) => c.clip_id === args.clip_id)
            if (index < 0) { yield* refuse("move", "unknown clip"); return undefined }
            const clip = queue.splice(index, 1)[0]!
            queue.splice(typeof args.position === "number" ? args.position : 0, 0, clip)
            return { type: "clip_moved", data: { clip: clipInfo(clip) } }
          }
          case "stop": {
            const clip = playing
            if (clip === undefined) { yield* refuse("stop", "nothing playing"); return undefined }
            playing = undefined
            return clipMessage("clip_stopped", clip)
          }
          case "set_autoplay":
            autoplay = args.enabled === true
            return json({ type: "autoplay_accepted", data: { enabled: autoplay } })
          case "set_canvas":
            return json({ type: "canvas_accepted", data: { aspect: args.aspect ?? "16:9", width: 1344, height: 768 } })
          case "set_flush_on_clip_end":
            return json({ type: "flush_accepted", data: { enabled: args.enabled === true } })
          case "reset":
            return json({ type: "session_reset", data: { cleared_clips: 0, was_playing: false } })
          case "get_state":
            return stateUpdate()
          default:
            yield* refuse(command, "unknown command")
            return undefined
          }
        })
        return reply
      })

    const fake: Fake = {
      transport: {
        connect: () =>
          Effect.gen(function*() {
            if (script.connect !== undefined) return yield* script.connect(fake)
            yield* emitControl({ _tag: "Status", status: "connecting" })
            yield* emitControl({ _tag: "Session", sessionId: "fixture-session" })
            yield* emitControl({ _tag: "Status", status: "ready" })
            yield* emit(stateUpdate())
            yield* emit({ type: "queue_update", data: { generation: [], playout: [], history: [] } })
          }),
        send: (command, args = {}, attachments) =>
          Effect.gen(function*() {
            const uploaded = attachments === undefined ? {} : { uploads: attachments }
            yield* Ref.update(calls, (c) => [...c, { command, args, ...uploaded }])
            const handler = script.send?.[command]
            return handler === undefined ? yield* defaults(command, args, attachments) : yield* handler(args, { fake, defaults, ...uploaded })
          }),
        uploadFile: (path) =>
          Effect.gen(function*() {
            yield* Ref.update(uploads, (u) => [...u, path])
            if (script.upload !== undefined) return yield* script.upload(path)
            const id = uploadIds.get(path) ?? randomUUID()
            uploadIds.set(path, id)
            return { upload_id: id, name: path.split("/").pop() ?? "reference", mime_type: "image/png", size: 123 }
          }),
        requestSchema: Effect.succeed(script.schema ?? defaultSchema),
        reconnect: Effect.suspend(() => script.reconnect?.(fake) ?? Effect.void),
        events: Stream.fromQueue(control),
        video: Stream.empty,
        audio: Stream.empty,
        snapshot: Effect.succeed({ closed: false, queuedControl: 0, queuedVideo: 0, queuedAudio: 0, queuedBytes: 0, droppedVideo: 0n, droppedAudio: 0n, pendingRequests: 0, deliveredVideo: 0n, deliveredAudio: 0n }),
        disconnect: Ref.update(disconnects, (n) => n + 1)
      },
      emit,
      emitControl,
      refuse,
      stateUpdate,
      failControl: (error) => Queue.fail(control, error).pipe(Effect.asVoid),
      calls: Ref.get(calls),
      uploads: Ref.get(uploads),
      accepted: Ref.get(accepted),
      disconnects: Ref.get(disconnects),
      clipMessage
    }
    return fake
  })

/** Never resolves until released: for interruption tests. */
export const gate = () => Effect.gen(function*() {
  const d = yield* Deferred.make<void>()
  return { wait: Deferred.await(d), release: Deferred.succeed(d, undefined).pipe(Effect.asVoid) }
})

/** A valid PNG of the given size, for reference validation tests. */
