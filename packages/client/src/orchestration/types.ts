import * as Context from "effect/Context";
import * as Data from "effect/Data";
import type * as Effect from "effect/Effect";
import type * as Option from "effect/Option";
import type * as Result from "effect/Result";
import type * as Stream from "effect/Stream";
import type { ReactorError } from "../errors.js";
import type { Clip } from "../h3/messages.js";
import type { CommandFailure } from "../session/commands.js";
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
  | { readonly _tag: "SessionFailed"; readonly failure: ReactorError };
export const EngineEvent = Data.taggedEnum<EngineEvent>();

export type RemoveOutcome = "unstarted" | "in_flight" | "generation" | "ready";
export type EnqueueError = CommandFailure;
export type CommandError = CommandFailure;

export interface EngineShape {
  readonly prepare: (
    request: ClipRequest,
  ) => Effect.Effect<Submission<ClipId, CommandFailure>, CommandFailure>;
  readonly enqueue: (request: ClipRequest) => Effect.Effect<ClipId, CommandFailure>;
  readonly state: Effect.Effect<EngineState>;
  readonly events: Stream.Stream<EngineEvent, ReactorError>;
  readonly failure: Effect.Effect<ReactorError>;
  readonly setAutoplay: (enabled: boolean) => Effect.Effect<void, CommandFailure>;
  readonly pauseAndStop: Effect.Effect<void, CommandFailure>;
  readonly remove: (id: ClipId) => Effect.Effect<RemoveOutcome, CommandFailure>;
  readonly move: (
    id: ClipId,
    position: number,
    queue: "generation" | "playout",
  ) => Effect.Effect<void, CommandFailure>;
  readonly setCanvas: (canvas: Canvas) => Effect.Effect<void, CommandFailure>;
}

export interface MediaShape {
  readonly video: Stream.Stream<VideoFrame, ReactorError>;
  readonly audio: Stream.Stream<AudioFrame, ReactorError>;
  readonly pressure: Effect.Effect<MediaPressure, ReactorError>;
  readonly videoFramesPerSecond: number;
}

export type MediaState =
  | { readonly _tag: "Ready"; readonly sessionId: string; readonly generation: bigint }
  | { readonly _tag: "Recovering"; readonly sessionId: string; readonly cause: ReactorError }
  | { readonly _tag: "Failed"; readonly cause: ReactorError }
  | { readonly _tag: "Closed" };

export interface MediaSource extends MediaShape {
  readonly generation: bigint;
}

export interface PolicyCleanup {
  readonly operation: string;
  readonly result: Result.Result<void, CommandFailure>;
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
  readonly commit?: (submissionId: string) => Effect.Effect<void, CommandFailure>;
  readonly result?: (
    submissionId: string,
    result: Result.Result<ClipId, CommandFailure>,
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
  ) => Effect.Effect<Submission<ClipId, CommandFailure>, CommandFailure>;
  readonly media: Effect.Effect<MediaSource, ReactorError>;
  readonly reconnect: Effect.Effect<void, ReactorError>;
  readonly refresh: Effect.Effect<void, CommandFailure>;
  readonly setAutoplay: (enabled: boolean) => Effect.Effect<void, CommandFailure>;
  readonly stop: Effect.Effect<void, CommandFailure>;
  readonly remove: (id: ClipId) => Effect.Effect<RemoveOutcome, CommandFailure>;
  readonly move: (id: ClipId, position: number) => Effect.Effect<void, CommandFailure>;
  readonly setCanvas: (canvas: Canvas) => Effect.Effect<void, CommandFailure>;
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
