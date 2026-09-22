import { Result, Schema } from "effect"
import { ClipId } from "../Clip.js"

/**
 * The H3 Reference Turbo Realtime wire messages this adapter consumes, as
 * delivered by the native client: every model message is `{ type, data }`.
 *
 * What is modeled here is the documented base queue protocol shared by
 * Reactor's clip-queue models (the FastH3 schema page documents it: every
 * message is `{ type, data }`, clip messages carry `data.clip`, `queue_update`
 * carries `generation`/`playout`/`history`, `state_update` carries the live
 * capacities and length bounds, and a refused command resolves without a reply
 * while a broadcast `command_error { command, reason }` names the refusal)
 * plus the H3 Reference facts recorded in the handoff (`reference_images`,
 * prompt limits, duration grid, continuation retention). Only the fields the
 * adapter needs are required; everything else is optional and preserved.
 * Unknown message types are not errors. A known type whose payload does not
 * carry what the adapter needs is a malformed message.
 *
 * These match the published H3 Reference Turbo Realtime schema page (commands,
 * replies, events, the clip object, `queue_update`, `state_update`, the upload
 * reference). The session's OpenAPI document (`requestSchema`) is still checked
 * at startup for the names below, since a deployment can differ from the page.
 */

export const commands = {
  enqueue: "enqueue",
  pop: "pop",
  move: "move",
  stop: "stop",
  getQueue: "get_queue",
  setAutoplay: "set_autoplay",
  setCanvas: "set_canvas",
  setFlushOnClipEnd: "set_flush_on_clip_end",
  reset: "reset",
  getState: "get_state"
} as const

export const messages = {
  clipQueued: "clip_queued",
  clipGenerated: "clip_generated",
  clipFailed: "clip_failed",
  clipStarted: "clip_started",
  clipFinished: "clip_finished",
  clipStopped: "clip_stopped",
  clipPopped: "clip_popped",
  clipMoved: "clip_moved",
  queueUpdate: "queue_update",
  stateUpdate: "state_update",
  commandError: "command_error",
  autoplayAccepted: "autoplay_accepted",
  canvasAccepted: "canvas_accepted",
  flushAccepted: "flush_accepted",
  sessionReset: "session_reset"
} as const

/** Names the running deployment must know for this adapter to drive it. */
export const requiredDeploymentNames: ReadonlyArray<string> = [
  commands.enqueue,
  commands.pop,
  commands.move,
  commands.stop,
  commands.getQueue,
  commands.setAutoplay,
  commands.setCanvas,
  "reference_images",
  messages.clipQueued,
  messages.clipGenerated,
  messages.clipFailed,
  messages.clipStarted,
  messages.clipFinished,
  messages.queueUpdate,
  messages.stateUpdate
]

export const Envelope = Schema.Struct({
  type: Schema.String,
  data: Schema.optionalKey(Schema.NullOr(Schema.Json))
})
export type Envelope = typeof Envelope.Type

const FiniteNumber = Schema.Number.check(Schema.isFinite())

/** Everything a clip is, as the provider echoes it. `frames`/`seconds` give the accepted length. */
export const ClipInfo = Schema.Struct({
  clip_id: Schema.String.check(Schema.isUUID()),
  frames: Schema.optionalKey(FiniteNumber),
  seconds: Schema.optionalKey(FiniteNumber),
  metadata: Schema.optionalKey(Schema.NullOr(Schema.String)),
  prompt: Schema.optionalKey(Schema.String),
  ready: Schema.optionalKey(Schema.Boolean),
  seed: Schema.optionalKey(FiniteNumber),
  continue_from_clip_id: Schema.optionalKey(Schema.NullOr(Schema.String)),
  has_starting_frame: Schema.optionalKey(Schema.Boolean),
  has_ending_frame: Schema.optionalKey(Schema.Boolean)
})
export type ClipInfo = typeof ClipInfo.Type

/** Lifecycle payloads either are a ClipInfo or wrap one under `clip`. */
const ClipPayload = Schema.Union([ClipInfo, Schema.Struct({ clip: ClipInfo, reason: Schema.optionalKey(Schema.String) })])
const FailedPayload = Schema.Union([
  Schema.Struct({ ...ClipInfo.fields, reason: Schema.optionalKey(Schema.String), error: Schema.optionalKey(Schema.String) }),
  Schema.Struct({ clip: ClipInfo, reason: Schema.optionalKey(Schema.String), error: Schema.optionalKey(Schema.String) })
])

export const QueueUpdate = Schema.Struct({
  generation: Schema.optionalKey(Schema.Array(ClipInfo)),
  playout: Schema.optionalKey(Schema.Array(ClipInfo)),
  history: Schema.optionalKey(Schema.Array(ClipInfo))
})
export type QueueUpdate = typeof QueueUpdate.Type

/** The documented snapshot: everything observable except queue contents. */
export const StateUpdate = Schema.Struct({
  playing: Schema.optionalKey(Schema.Boolean),
  playing_clip_id: Schema.optionalKey(Schema.NullOr(Schema.String)),
  autoplay: Schema.optionalKey(Schema.Boolean),
  flush_on_clip_end: Schema.optionalKey(Schema.Boolean),
  generation_queued: Schema.optionalKey(FiniteNumber),
  generation_capacity: Schema.optionalKey(FiniteNumber),
  playout_queued: Schema.optionalKey(FiniteNumber),
  playout_capacity: Schema.optionalKey(FiniteNumber),
  clip_seconds: Schema.optionalKey(FiniteNumber),
  clip_seconds_min: Schema.optionalKey(FiniteNumber),
  clip_seconds_max: Schema.optionalKey(FiniteNumber),
  aspect: Schema.optionalKey(Schema.String),
  width: Schema.optionalKey(FiniteNumber),
  height: Schema.optionalKey(FiniteNumber),
  clips_played: Schema.optionalKey(FiniteNumber),
  seconds_sent: Schema.optionalKey(FiniteNumber),
  valid_commands: Schema.optionalKey(Schema.Array(Schema.String))
})
export type StateUpdate = typeof StateUpdate.Type

export const CommandErrorPayload = Schema.Struct({
  command: Schema.optionalKey(Schema.String),
  code: Schema.optionalKey(Schema.String),
  reason: Schema.optionalKey(Schema.String),
  message: Schema.optionalKey(Schema.String)
})
export type CommandErrorPayload = typeof CommandErrorPayload.Type

export const CanvasAccepted = Schema.Struct({
  aspect: Schema.optionalKey(Schema.String),
  width: Schema.optionalKey(FiniteNumber),
  height: Schema.optionalKey(FiniteNumber)
})

export const AutoplayAccepted = Schema.Struct({ enabled: Schema.optionalKey(Schema.Boolean) })

export class MalformedMessage {
  readonly _tag = "MalformedMessage"
  constructor(readonly type: string, readonly reason: string) {}
}

export type Decoded =
  | { readonly _tag: "ClipQueued"; readonly clip: ClipInfo }
  | { readonly _tag: "ClipGenerated"; readonly clip: ClipInfo }
  | { readonly _tag: "ClipFailed"; readonly clip: ClipInfo; readonly reason: string }
  | { readonly _tag: "ClipStarted"; readonly clip: ClipInfo }
  | { readonly _tag: "ClipFinished"; readonly clip: ClipInfo }
  | { readonly _tag: "ClipStopped"; readonly clip: ClipInfo }
  | { readonly _tag: "ClipMoved"; readonly clip: ClipInfo }
  | { readonly _tag: "ClipPopped"; readonly clip: ClipInfo }
  | { readonly _tag: "QueueUpdate"; readonly update: QueueUpdate }
  | { readonly _tag: "StateUpdate"; readonly update: StateUpdate }
  | { readonly _tag: "CommandError"; readonly error: CommandErrorPayload }
  | { readonly _tag: "AutoplayAccepted"; readonly enabled: boolean | undefined }
  | { readonly _tag: "CanvasAccepted"; readonly aspect: string | undefined; readonly width: number | undefined; readonly height: number | undefined }
  | { readonly _tag: "Other"; readonly type: string; readonly data: unknown }

const decodeWith = <A>(schema: Schema.ConstraintDecoder<A, never>, value: unknown): Result.Result<A, string> => {
  const result = Schema.decodeUnknownResult(schema)(value)
  return Result.isSuccess(result) ? Result.succeed(result.success) : Result.fail(String(result.failure))
}

const unwrapClip = (value: unknown): Result.Result<{ clip: ClipInfo; reason?: string }, string> => {
  const r = decodeWith(ClipPayload, value)
  if (Result.isFailure(r)) return Result.fail(r.failure)
  const p = r.success
  return "clip" in p ? Result.succeed({ clip: p.clip, ...(p.reason === undefined ? {} : { reason: p.reason }) }) : Result.succeed({ clip: p })
}

/** Decode one model message. Unknown types are `Other`; known types with unusable payloads are malformed. */
export const decodeMessage = (raw: unknown): Result.Result<Decoded, MalformedMessage> => {
  const env = decodeWith(Envelope, raw)
  if (Result.isFailure(env)) return Result.fail(new MalformedMessage("?", "not a { type, data } envelope"))
  const { type, data } = env.success
  const clipOf = (tag: "ClipQueued" | "ClipGenerated" | "ClipStarted" | "ClipFinished" | "ClipStopped" | "ClipPopped" | "ClipMoved"): Result.Result<Decoded, MalformedMessage> => {
    const c = unwrapClip(data)
    return Result.isFailure(c) ? Result.fail(new MalformedMessage(type, `payload is not a clip: ${c.failure}`)) : Result.succeed({ _tag: tag, clip: c.success.clip })
  }
  switch (type) {
    case messages.clipQueued:
      return clipOf("ClipQueued")
    case messages.clipGenerated:
      return clipOf("ClipGenerated")
    case messages.clipStarted:
      return clipOf("ClipStarted")
    case messages.clipFinished:
      return clipOf("ClipFinished")
    case messages.clipStopped:
      return clipOf("ClipStopped")
    case messages.clipMoved: return clipOf("ClipMoved")
    case messages.clipPopped:
      return clipOf("ClipPopped")
    case messages.clipFailed: {
      const f = decodeWith(FailedPayload, data)
      if (Result.isFailure(f)) return Result.fail(new MalformedMessage(type, `payload is not a failed clip: ${f.failure}`))
      const p = f.success
      const clip = "clip" in p ? p.clip : p
      return Result.succeed({ _tag: "ClipFailed", clip, reason: p.reason ?? p.error ?? "build failed" })
    }
    case messages.queueUpdate: {
      const q = decodeWith(QueueUpdate, data ?? {})
      return Result.isFailure(q) ? Result.fail(new MalformedMessage(type, q.failure)) : Result.succeed({ _tag: "QueueUpdate", update: q.success })
    }
    case messages.stateUpdate: {
      const st = decodeWith(StateUpdate, data ?? {})
      return Result.isFailure(st) ? Result.fail(new MalformedMessage(type, st.failure)) : Result.succeed({ _tag: "StateUpdate", update: st.success })
    }
    case messages.commandError: {
      const e = decodeWith(CommandErrorPayload, data ?? {})
      return Result.isFailure(e) ? Result.fail(new MalformedMessage(type, e.failure)) : Result.succeed({ _tag: "CommandError", error: e.success })
    }
    case messages.autoplayAccepted: {
      const a = decodeWith(AutoplayAccepted, data ?? {})
      return Result.isFailure(a) ? Result.fail(new MalformedMessage(type, a.failure)) : Result.succeed({ _tag: "AutoplayAccepted", enabled: a.success.enabled })
    }
    case messages.canvasAccepted: {
      const c = decodeWith(CanvasAccepted, data ?? {})
      return Result.isFailure(c)
        ? Result.fail(new MalformedMessage(type, c.failure))
        : Result.succeed({ _tag: "CanvasAccepted", aspect: c.success.aspect, width: c.success.width, height: c.success.height })
    }
    default:
      return Result.succeed({ _tag: "Other", type, data })
  }
}

/** The provider's accepted clip length in seconds, from whichever field it reported. */
export const acceptedSeconds = (clip: ClipInfo, fps: number): number | undefined =>
  clip.seconds !== undefined && clip.seconds > 0 ? clip.seconds : clip.frames !== undefined && clip.frames > 0 ? clip.frames / fps : undefined

export const clipIdOf = (clip: ClipInfo): ClipId => ClipId.make(clip.clip_id)
