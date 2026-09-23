import * as Context from "effect/Context";
import * as Data from "effect/Data";
import type * as Effect from "effect/Effect";
import type * as Option from "effect/Option";
import type * as Result from "effect/Result";
import type * as Stream from "effect/Stream";
import type { ReactorError, ReactorFailure } from "../errors.js";
import type { Clip } from "../h3/messages.js";
import type { CommandFailure, PolicyFailure } from "../errors.js";
import type { CloseReport } from "../SessionTypes.js";
import type { AudioFrame, VideoFrame, MediaPressure } from "../session/media.js";
import type { Submission } from "../Submission.js";
import type { Affinity } from "../Sequence.js";
import type { Canvas, ClipId, ClipRequest } from "./request.js";

export type BuildTiming =
  | { readonly _tag: "Measured"; readonly buildMs: number }
  | { readonly _tag: "Bounded"; readonly admissionToReadyMs: number }
  | { readonly _tag: "Unknown" };
export const BuildTiming = Data.taggedEnum<BuildTiming>();

/** A provider clip is visible even when this application never submitted it. */
export interface ClipRecord {
  readonly clipId: ClipId;
  readonly durationSeconds: number;
  readonly provider: Clip;
  readonly request?: ClipRequest;
  readonly seq?: number;
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
  /** A local observation time, never reconstructed from queue order. */
  readonly startedAt: Option.Option<number>;
}

export interface EngineState {
  readonly availability: "Ready" | "Synchronizing" | "Unavailable";
  readonly queued: readonly ClipRecord[];
  /** Provider queue order, including an independently observed active build. */
  readonly generationOrder: readonly ClipId[];
  readonly building: Option.Option<{
    readonly record: ClipRecord;
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
      readonly at: number;
    }
  | {
      readonly _tag: "Ended";
      readonly clipId: ClipId;
      readonly termination: "finished" | "stopped";
    }
  | {
      readonly _tag: "Failed";
      readonly clipId: ClipId;
      readonly reason: string;
      readonly sessionId?: string;
    }
  | { readonly _tag: "Starved"; readonly at: number }
  | { readonly _tag: "SessionFailed"; readonly failure: ReactorFailure };
export const EngineEvent = Data.taggedEnum<EngineEvent>();

export type RemoveOutcome = "unstarted" | "in_flight" | "generation" | "ready";
/**
 * What an orchestration command can fail with: a dispatched or undispatched
 * command's `CommandFailure`, or a local `PolicyFailure` refusal. Both carry
 * `context.outcome`, so a caller reads the dispatch evidence without narrowing.
 */
export type EngineError = CommandFailure | PolicyFailure;

export interface EngineShape {
  readonly prepare: (
    request: ClipRequest,
  ) => Effect.Effect<Submission<ClipId, EngineError>, EngineError>;
  readonly enqueue: (request: ClipRequest) => Effect.Effect<ClipId, EngineError>;
  readonly state: Effect.Effect<EngineState>;
  readonly events: Stream.Stream<EngineEvent, ReactorError>;
  /**
   * The terminal failure, as it was raised: a session's `ReactorError`, a
   * replacement's `AcquisitionFailure` with its cleanup, or the failed command.
   */
  readonly failure: Effect.Effect<ReactorFailure>;
  readonly setAutoplay: (enabled: boolean) => Effect.Effect<void, EngineError>;
  readonly pauseAndStop: Effect.Effect<void, EngineError>;
  readonly remove: (id: ClipId) => Effect.Effect<RemoveOutcome, EngineError>;
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
 *   reader has not taken (96 video frames, 192,000 audio samples), and an
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

export interface MediaSource {
  readonly video: Stream.Stream<VideoFrame, ReactorError>;
  readonly audio: Stream.Stream<AudioFrame, ReactorError>;
  readonly pressure: Effect.Effect<MediaPressure, ReactorError>;
  readonly videoFramesPerSecond: number;
  readonly generation: bigint;
}

export interface PolicyCleanup {
  readonly operation: string;
  readonly result: Result.Result<void, EngineError>;
}

export interface SourceCleanup {
  /** The unmodified lifecycle owner's canonical cleanup evidence. */
  readonly lease: CloseReport;
  readonly policy: readonly PolicyCleanup[];
}

export interface CleanupReport {
  readonly sessions: readonly SourceCleanup[];
}

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
