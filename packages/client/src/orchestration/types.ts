import * as Context from "effect/Context";
import * as Data from "effect/Data";
import type * as Duration from "effect/Duration";
import type * as Effect from "effect/Effect";
import type * as Option from "effect/Option";
import type * as Result from "effect/Result";
import * as Schema from "effect/Schema";
import type * as Scope from "effect/Scope";
import type * as Stream from "effect/Stream";
import type { ReactorError, ReactorFailure } from "../errors.js";
import type { ObservationOptions } from "../observation.js";
import type { Clip } from "../h3/messages.js";
import type { CommandFailure, PolicyFailure } from "../errors.js";
import { CommandFailureFromJson, PolicyFailureFromJson } from "../errors.js";
import { CloseReport } from "../SessionTypes.js";
import type { AudioFrame, VideoFrame, MediaPressure } from "../session/media.js";
import type { Submission } from "../Submission.js";
import type { Affinity } from "../Sequence.js";
import type { Canvas, ClipId, ClipRequest } from "./request.js";

/**
 * How long a clip took to become Ready, measured on the monotonic clock, so a
 * correction of the host's wall clock does not change it.
 */
export type BuildTiming =
  | { readonly _tag: "Measured"; readonly buildMs: number }
  | { readonly _tag: "Bounded"; readonly admissionToReadyMs: number }
  | { readonly _tag: "Unknown" };
export const BuildTiming = Data.taggedEnum<BuildTiming>();

/** A provider clip is visible even when this application never submitted it. */
export interface ClipRecord {
  readonly clipId: ClipId;
  /** Physical session that owns this clip, including during a renewal overlap. */
  readonly sessionId: string;
  readonly durationSeconds: number;
  readonly provider: Clip;
  readonly request?: ClipRequest;
  readonly seq?: number;
  /** When this process enqueued the clip: epoch milliseconds of the local observation. */
  readonly enqueuedAt?: number;
}

/** The renderer hook receives a locally authored request with its annotation. */
export interface LocalClipRecord extends ClipRecord {
  readonly request: ClipRequest;
  readonly seq: number;
  readonly enqueuedAt: number;
}

export interface Playback {
  readonly clipId: ClipId;
  /** Initial provider state may name a playing clip without describing it. */
  readonly record: Option.Option<ClipRecord>;
  /**
   * When the start was observed locally, in epoch milliseconds, never
   * reconstructed from queue order. It is a recorded instant: compare it with
   * wall time (`Clock.currentTimeMillis`), and measure elapsed time on the
   * monotonic clock instead.
   */
  readonly startedAt: Option.Option<number>;
  /** Monotonic start observation, available only when this process saw Started. */
  readonly startedAtMonotonicMillis?: number;
}

export interface EngineState {
  readonly availability: "Ready" | "Synchronizing" | "Unavailable";
  /** Every live physical source, including one with no clips yet. */
  readonly sessions: readonly {
    readonly sessionId: string;
    readonly availability: EngineState["availability"];
  }[];
  /** The source that receives an unconstrained new request. */
  readonly preferredSessionId: Option.Option<string>;
  /** The source draining ahead of a ready replacement, if there is one. */
  readonly retiringSessionId: Option.Option<string>;
  /** The retiring source's final observed clip has arrived in full. Absent on physical sources. */
  readonly handoffReady?: boolean;
  readonly queued: readonly ClipRecord[];
  /** Provider queue order, including an independently observed active build. */
  readonly generationOrder: readonly ClipId[];
  readonly building: Option.Option<{
    readonly record: ClipRecord;
    /** When the build was seen to start, in epoch milliseconds of the local observation. */
    readonly startedAt: Option.Option<number>;
  }>;
  readonly ready: readonly ClipRecord[];
  readonly playing: Option.Option<Playback>;
  readonly continuable: readonly ClipId[];
  readonly failed: readonly ClipId[];
  readonly started: boolean;
  readonly canvas: Option.Option<Canvas>;
  readonly capacities: { readonly generation: number; readonly playout: number };
}

export type EngineEvent =
  | { readonly _tag: "Queued"; readonly clipId: ClipId; readonly durationSeconds: number }
  | {
      readonly _tag: "Building";
      readonly clipId: ClipId;
      readonly startedAt: Option.Option<number>;
    }
  | {
      readonly _tag: "Ready";
      readonly clipId: ClipId;
      readonly timing: BuildTiming;
      readonly durationSeconds: number;
    }
  | {
      readonly _tag: "Started";
      readonly clipId: ClipId;
      readonly durationSeconds: number;
      /** Epoch milliseconds of the local observation, as every event's `at` is. */
      readonly at: number;
      /** Monotonic instant captured with `at`, for elapsed as-run accounting. */
      readonly atMonotonicMillis?: number;
    }
  | {
      readonly _tag: "Ended";
      readonly clipId: ClipId;
      readonly termination: "finished" | "stopped";
      /** Local observation time of the end, when the source provides it. */
      readonly at?: number;
      readonly atMonotonicMillis?: number;
    }
  | {
      readonly _tag: "Failed";
      readonly clipId: ClipId;
      readonly reason: string;
      readonly sessionId?: string;
    }
  | { readonly _tag: "Starved"; readonly at: number }
  /** Media completion wakes policies before the next autoplay boundary. */
  | { readonly _tag: "HandoffReady"; readonly sessionId: string }
  | { readonly _tag: "SessionFailed"; readonly failure: ReactorFailure };
export const EngineEvent = Data.taggedEnum<EngineEvent>();

export type RemoveOutcome = "unstarted" | "in_flight" | "generation" | "ready";
/**
 * What an orchestration command can fail with: a dispatched or undispatched
 * command's `CommandFailure`, or a local `PolicyFailure` refusal. Both carry
 * `context.outcome`, so a caller reads the dispatch evidence without narrowing.
 */
export type EngineError = CommandFailure | PolicyFailure;

/**
 * An engine's state and every event after it, with no gap between them: the
 * subscription is acquired before the state is read. An event may repeat what
 * `initial` already reflects, so apply events idempotently by `clipId`. The
 * stream is bounded per observer like any observation; after an `Overflow`,
 * observe again for a fresh state and subscription.
 */
export interface EngineObservation {
  readonly initial: EngineState;
  readonly events: Stream.Stream<EngineEvent, ReactorError>;
}
export type ObserveEngine = (
  options?: ObservationOptions,
) => Effect.Effect<EngineObservation, ReactorError, Scope.Scope>;

export interface EngineShape {
  readonly prepare: (
    request: ClipRequest,
  ) => Effect.Effect<Submission<ClipId, EngineError>, EngineError>;
  readonly enqueue: (request: ClipRequest) => Effect.Effect<ClipId, EngineError>;
  /** Refuse before dispatch if routing would select another physical source. */
  readonly enqueueOnSource?:
    | ((request: ClipRequest, expectedSessionId: string) => Effect.Effect<ClipId, EngineError>)
    | undefined;
  readonly state: Effect.Effect<EngineState>;
  /** Later events only, subscribed when the stream runs; use `observe` to pair them with a state. */
  readonly events: Stream.Stream<EngineEvent, ReactorError>;
  readonly observe: ObserveEngine;
  /**
   * The terminal failure, as it was raised: a session's `ReactorError`, a
   * replacement's `AcquisitionFailure` with its cleanup, or the failed command.
   */
  readonly failure: Effect.Effect<ReactorFailure>;
  /** Permanently stop new renewal allocations; already acquired sources may finish. */
  readonly stopRenewal: Effect.Effect<void, EngineError>;
  readonly setAutoplay: (enabled: boolean) => Effect.Effect<void, EngineError>;
  readonly pauseAndStop: Effect.Effect<void, EngineError>;
  readonly remove: (id: ClipId) => Effect.Effect<RemoveOutcome, EngineError>;
  /** A global rank within this clip's physical session range in the chosen queue. */
  readonly move: (
    id: ClipId,
    position: number,
    queue: "generation" | "playout",
  ) => Effect.Effect<void, EngineError>;
  readonly setCanvas: (canvas: Canvas) => Effect.Effect<void, EngineError>;
}

/**
 * An orchestration's recovering media output, one logical track per kind that
 * continues across reconnects and source replacements, and ends with the
 * orchestration's terminal failure.
 *
 * - Consumption is mandatory: while it runs, the orchestration buffers what the
 *   reader has not taken (by default 96 video frames and 192,000 audio
 *   samples; see `maxQueuedVideoFrames` and `maxQueuedAudioSamples`), and an
 *   output that stays undrained past that fails the orchestration with
 *   `Overflow`. That failure is terminal.
 * - Each output has one reader at a time. A second concurrent reader fails
 *   with `AlreadyReading` rather than silently splitting the frames; a reader
 *   that ends releases the output, and a later reader resumes from what is
 *   retained. A preview that wants only the newest frame derives it per
 *   consumer with `Stream.buffer({ capacity: 1, strategy: "sliding" })`.
 * - `pressure` reports the loss of the logical output: `droppedVideo`,
 *   `droppedAudio` and `readerOverflows` accumulate across generations and
 *   across source replacements, counting each source only while it fed this
 *   output. A total that cannot be read fails `pressure` with `InvalidState`
 *   rather than reporting zero.
 */
export interface MediaShape {
  readonly video: Stream.Stream<VideoFrame, ReactorFailure>;
  readonly audio: Stream.Stream<AudioFrame, ReactorFailure>;
  readonly pressure: Effect.Effect<MediaPressure, ReactorError>;
  readonly videoFramesPerSecond: number;
}

export type MediaState =
  | { readonly _tag: "Ready"; readonly sessionId: string; readonly generation: bigint }
  | { readonly _tag: "Recovering"; readonly sessionId: string; readonly cause: ReactorFailure }
  | { readonly _tag: "Failed"; readonly cause: ReactorFailure }
  | { readonly _tag: "Closed" };

export interface MediaTail {
  readonly video: {
    readonly framesPerSecond: number;
    readonly expectedFrames: number;
    readonly receivedFrames: number;
    readonly status: "not-started" | "count-complete" | "incomplete";
  };
  readonly audio: { readonly receivedSamples: number; readonly status: "unverified" };
  readonly sourceDrops: { readonly video: bigint | null; readonly audio: bigint | null };
  readonly forwarded: { readonly queuedVideoFrames: number; readonly queuedAudioSamples: number };
}

export type Renewal =
  | { readonly _tag: "Opened"; readonly sessionId: string; readonly lifetime: Duration.Duration }
  | { readonly _tag: "Prepared" }
  | { readonly _tag: "SetupFailed"; readonly reason: string; readonly consecutive: number }
  | { readonly _tag: "Recovering"; readonly sessionId: string; readonly reason: string }
  | { readonly _tag: "Reconnected"; readonly sessionId: string; readonly generation: bigint }
  | {
      readonly _tag: "Switched";
      readonly sessionId: string;
      readonly ageSeconds: number;
      readonly tail: MediaTail;
    }
  | {
      readonly _tag: "Replaced";
      readonly reason: string;
      readonly lostClips: number;
      readonly sessionId: string;
      readonly ageSeconds: number;
      readonly tail: MediaTail;
    }
  | { readonly _tag: "Failed"; readonly reason: string };

/**
 * One handle's observation, in the order its changes happened: engine events,
 * renewal events and media-state transitions from a single stream, so a
 * `Replaced` follows the `Failed` events of the clips it lost and no media
 * state arrives out of order.
 */
export type HandleEvent =
  | { readonly _tag: "Engine"; readonly event: EngineEvent }
  | { readonly _tag: "Renewal"; readonly event: Renewal }
  | { readonly _tag: "Media"; readonly state: MediaState };

/**
 * The handle's current engine and media state, and every handle event after
 * it. The subscription is acquired before the state is read, so no event falls
 * between them; an event may repeat what `initial` already reflects.
 */
export interface HandleObservation {
  readonly initial: { readonly engine: EngineState; readonly media: MediaState };
  readonly events: Stream.Stream<HandleEvent, ReactorError>;
}

export interface MediaSource {
  readonly video: Stream.Stream<VideoFrame, ReactorError>;
  readonly audio: Stream.Stream<AudioFrame, ReactorError>;
  readonly pressure: Effect.Effect<MediaPressure, ReactorError>;
  readonly videoFramesPerSecond: number;
  readonly generation: bigint;
}

export const PolicyCleanup = Schema.Struct({
  operation: Schema.String,
  result: Schema.Result(Schema.Void, Schema.Union([CommandFailureFromJson, PolicyFailureFromJson])),
});
export interface PolicyCleanup extends Schema.Schema.Type<typeof PolicyCleanup> {}

export const SourceCleanup = Schema.Struct({
  /** The unmodified lifecycle owner's canonical cleanup evidence. */
  lease: CloseReport,
  policy: Schema.Array(PolicyCleanup),
});
export interface SourceCleanup extends Schema.Schema.Type<typeof SourceCleanup> {}

/**
 * An orchestration's cleanup evidence. As a Schema it encodes for persistence
 * (`Schema.toCodecJson(CleanupReport)` for JSON); its failures encode as
 * diagnostic JSON, without provider text.
 */
export const CleanupReport = Schema.Struct({ sessions: Schema.Array(SourceCleanup) });
export interface CleanupReport extends Schema.Schema.Type<typeof CleanupReport> {}

/** Resolved by the one router; the full request remains a local annotation. */
export interface RoutedRequest {
  readonly request: ClipRequest;
  readonly position: number | undefined;
}

export interface EnqueueHooks {
  readonly commit?: (submissionId: string) => Effect.Effect<void, EngineError>;
  readonly result?: (
    submissionId: string,
    result: Result.Result<ClipId, EngineError>,
  ) => Effect.Effect<void>;
}

/**
 * One physical provider view or an explicit production simulator. It owns no
 * sequence routing; a scheduled handle supplies every RoutedRequest.
 */
export interface Source {
  readonly id: string;
  readonly state: Effect.Effect<EngineState>;
  readonly events: Stream.Stream<EngineEvent, ReactorError>;
  readonly observe: ObserveEngine;
  readonly prepareRouted: (
    request: RoutedRequest,
    hooks?: EnqueueHooks,
  ) => Effect.Effect<Submission<ClipId, EngineError>, EngineError>;
  readonly media: Effect.Effect<MediaSource, ReactorError>;
  readonly reconnect: Effect.Effect<void, ReactorError | CommandFailure>;
  readonly refresh: Effect.Effect<void, EngineError>;
  readonly setAutoplay: (enabled: boolean) => Effect.Effect<void, EngineError>;
  readonly stop: Effect.Effect<void, EngineError>;
  readonly remove: (id: ClipId) => Effect.Effect<RemoveOutcome, EngineError>;
  readonly move: (id: ClipId, position: number) => Effect.Effect<void, EngineError>;
  readonly setCanvas: (canvas: Canvas) => Effect.Effect<void, EngineError>;
  readonly close: Effect.Effect<SourceCleanup>;
}

export interface HandleShape {
  readonly engine: EngineShape;
  readonly media: MediaShape;
  readonly mediaState: Effect.Effect<MediaState>;
  /** Engine, renewal and media changes in one ordered stream, after the current state. */
  readonly observe: (
    options?: ObservationOptions,
  ) => Effect.Effect<HandleObservation, ReactorError, Scope.Scope>;
  readonly sessionId: Effect.Effect<Option.Option<string>>;
  readonly close: Effect.Effect<CleanupReport>;
  readonly cleanup: Effect.Effect<Option.Option<CleanupReport>>;
  readonly sequences: Pick<
    Affinity<string>,
    "get" | "snapshots" | "seal" | "acknowledgeIndeterminate" | "release"
  >;
}

export class Engine extends Context.Service<Engine, EngineShape>()(
  "reactor-effect-client/Orchestration/Engine",
) {}
export class Media extends Context.Service<Media, MediaShape>()(
  "reactor-effect-client/Orchestration/Media",
) {}
export class Handle extends Context.Service<Handle, HandleShape>()(
  "reactor-effect-client/Orchestration/Handle",
) {}

/** The three services of one handle, so its facets can never come from two orchestrations. */
export const handleContext = (handle: HandleShape): Context.Context<Engine | Media | Handle> =>
  Context.make(Engine, handle.engine).pipe(
    Context.add(Media, handle.media),
    Context.add(Handle, handle),
  );
