import { Context, Data, Effect, Option, Stream } from "effect"
import { type Canvas, ClipId, type ClipRequest } from "../Clip.js"
import type { ReactorError } from "../Model.js"
import type { Submission } from "../Submission.js"

// ---------------------------------------------------------------------------
// Interface
// ---------------------------------------------------------------------------

/**
 * How much the engine actually knows about a build's duration. The real
 * provider exposes no build-start observation, so most builds are `Unknown`
 * or `Bounded` (queue admission → ready, which includes waiting). Only the
 * simulator, or a deployment that reports it, produces `Measured`.
 */
export type BuildTiming = Data.TaggedEnum<{
  Measured: { readonly buildMs: number }
  Bounded: { readonly admissionToReadyMs: number }
  Unknown: {}
}>
export const BuildTiming = Data.taggedEnum<BuildTiming>()

export type EngineEvent = Data.TaggedEnum<{
  Queued: { readonly clipId: ClipId; readonly durationSeconds: number }
  /** `startedAt` is `None` when the provider gives no build-start observation. */
  Building: { readonly clipId: ClipId; readonly startedAt: Option.Option<number> }
  /** The accepted/corrected duration is mandatory: local renderers may extend a clip before it becomes ready. */
  Ready: { readonly clipId: ClipId; readonly timing: BuildTiming; readonly durationSeconds: number }
  /** `sessionId` is set when a connection took the clip down with it, so a burst of failures can be read as the one session loss it was. */
  Failed: { readonly clipId: ClipId; readonly reason: string; readonly sessionId?: string }
  /** `at` is when the control message arrived, not when a viewer saw the first frame. */
  Started: { readonly clipId: ClipId; readonly at: number; readonly durationSeconds: number }
  /** Provider media emission ended. This does not prove compositor/encoder output completion. */
  Ended: { readonly clipId: ClipId; readonly termination: "finished" | "stopped" }
  /** Playback had begun, a clip ended, and the playout queue was empty: dead air until the next clip is ready. */
  Starved: { readonly at: number }
  /** Terminal. Nothing after this can be trusted; the owning run must stop. */
  SessionFailed: { readonly failure: SessionFailed }
}>
export const EngineEvent = Data.taggedEnum<EngineEvent>()

export interface ClipRecord {
  readonly clipId: ClipId
  readonly seq: number
  /** The provider's accepted length, after alignment. */
  readonly durationSeconds: number
  readonly request: ClipRequest
  readonly enqueuedAt: number
}

export interface EngineState {
  /** In the generation queue behind the current build. */
  readonly queued: ReadonlyArray<ClipRecord>
  /** The one clip being built. `startedAt` is `None` when the provider exposes no build-start observation. */
  readonly building: Option.Option<{ readonly record: ClipRecord; readonly startedAt: Option.Option<number> }>
  /** Playout queue: built, waiting to play. */
  readonly ready: ReadonlyArray<ClipRecord>
  readonly playing: Option.Option<{ readonly record: ClipRecord; readonly startedAt: number }>
  /**
   * Successfully generated clips still eligible as `continueFrom` targets, oldest first.
   * Bounded by the profile's retention window; older ids are not valid continuations.
   */
  readonly continuable: ReadonlyArray<ClipId>
  readonly failed: ReadonlyArray<ClipId>
  /** Playout has begun (the audience is watching). Before this there is no clock pressure. */
  readonly started: boolean
  readonly canvas: Option.Option<Canvas>
  readonly capacities: { readonly generation: number; readonly playout: number }
}

/** The request violates the model contract; nothing was sent. */
export class InvalidRequest extends Data.TaggedError("InvalidRequest")<{ readonly reason: string }> {}
/** The provider answered and refused. Nothing was accepted. */
export class Rejected extends Data.TaggedError("Rejected")<{
  readonly command: string
  readonly code?: string | undefined
  readonly reason: string
  readonly cause?: unknown
}> {}
/** The command may have been sent/applied but its outcome could not be established. */
export class Uncertain extends Data.TaggedError("Uncertain")<{
  readonly command: string
  readonly reason: string
  readonly code?: string | undefined
  readonly cause?: unknown
}> {}
/** The session is unusable. Terminal for the owning run. */
export class SessionFailed extends Data.TaggedError("SessionFailed")<{ readonly reason: string; readonly cause?: unknown }> {}
/** The session is not idle, so the command is refused locally. */
export class Busy extends Data.TaggedError("Busy")<{ readonly reason: string }> {}
export class NotFound extends Data.TaggedError("NotFound")<{ readonly clipId: ClipId }> {}

export type EnqueueError = InvalidRequest | Rejected | Uncertain | SessionFailed
export type CommandError = Rejected | Uncertain | SessionFailed

/**
 * Where a removed clip was. The provider's `pop` removes a clip from whichever
 * queue holds it and discards an in-flight build when it completes, so removal
 * is never limited to unstarted work.
 */
export type RemoveOutcome = "unstarted" | "in_flight" | "ready"

export interface ClipEngineShape {
  /**
   * Create an inert, submit-once operation. No reference IO, upload, queue
   * admission or provider command starts until submission.submit is run.
   */
  readonly prepare: (request: ClipRequest) => Effect.Effect<Submission<ClipId, EnqueueError>, EnqueueError>
  /** Resolves with the provider's accepted clip id promptly after acknowledged acceptance.
   * Ready clips play automatically in order; presentation prerequisites must
   * be satisfied before enqueueing, even if generation finishes immediately. */
  readonly enqueue: (request: ClipRequest) => Effect.Effect<ClipId, EnqueueError>
  /** Removes the clip from whichever queue holds it (see `RemoveOutcome`). */
  readonly move: (clipId: ClipId, position: number, queue: "generation" | "playout") => Effect.Effect<void, NotFound | CommandError>
  readonly setAutoplay: (enabled: boolean) => Effect.Effect<void, CommandError>
  readonly stop: Effect.Effect<void, CommandError>
  readonly remove: (clipId: ClipId) => Effect.Effect<RemoveOutcome, NotFound | CommandError>
  /** Only legal while both queues are empty and nothing is playing; resolves once the provider acknowledges. */
  readonly setCanvas: (canvas: Canvas) => Effect.Effect<void, Busy | CommandError>
  readonly state: Effect.Effect<EngineState>
  /** Lifecycle events. State changes are applied before the corresponding event is published. */
  readonly events: Stream.Stream<EngineEvent, ReactorError>
  /** Resolves when the session fails terminally. */
  readonly failure: Effect.Effect<SessionFailed>
}

export class ClipEngine extends Context.Service<ClipEngine, ClipEngineShape>()("reactor-effect-client/ClipEngine") {}

export const emptyState = (capacities: EngineState["capacities"]): EngineState => ({
  queued: [],
  building: Option.none(),
  ready: [],
  playing: Option.none(),
  continuable: [],
  failed: [],
  started: false,
  canvas: Option.none(),
  capacities
})

export const isIdle = (s: EngineState) =>
  s.queued.length === 0 && Option.isNone(s.building) && s.ready.length === 0 && Option.isNone(s.playing)

/** Remember a generated clip as a continuation target, forgetting the oldest beyond the window. */
export const rememberContinuable = (s: EngineState, clipId: ClipId, window: number): ReadonlyArray<ClipId> =>
  [...s.continuable.filter((id) => id !== clipId), clipId].slice(-window)

// ---------------------------------------------------------------------------
// Playback timing helpers
// ---------------------------------------------------------------------------

/** Content that is certain to play: the remainder of the playing clip plus ready clips. */
export const securedMs = (state: EngineState, now: number): number => {
  const playing = Option.match(state.playing, {
    onNone: () => 0,
    onSome: ({ record, startedAt }) => Math.max(0, record.durationSeconds * 1000 - (now - startedAt))
  })
  return playing + state.ready.reduce((acc, r) => acc + r.durationSeconds * 1000, 0)
}

/** Everything committed to the engine, including what is still building. */
export const committedMs = (state: EngineState, now: number): number =>
  securedMs(state, now) +
  Option.match(state.building, { onNone: () => 0, onSome: (b) => b.record.durationSeconds * 1000 }) +
  state.queued.reduce((acc, r) => acc + r.durationSeconds * 1000, 0)

export const pendingCount = (state: EngineState): number =>
  state.queued.length + (Option.isSome(state.building) ? 1 : 0) + state.ready.length

export const isLive = (state: EngineState): boolean => state.started
