import type * as Effect from "effect/Effect";
import type * as Stream from "effect/Stream";
import type * as Scope from "effect/Scope";
import type { Track } from "../contract.js";
import type { ReactorError } from "../errors.js";
import type { MediaTrack } from "../PeerTypes.js";

/**
 * One decoded video frame. Every subscriber to a track receives the same frame
 * object and the same buffers, so a reader treats them as read-only and copies
 * the bytes before changing them.
 */
export interface VideoFrame {
  readonly _tag: "VideoFrame";
  readonly track: string;
  readonly width: number;
  readonly height: number;
  /** Zero means the sender supplied no frame identity. */
  readonly frameId: bigint;
  /** Sender-clock microseconds; zero means absent. */
  readonly timestampMicros: bigint;
  /** Owned BGRA bytes. Retaining a frame never retains native memory. */
  readonly data: Uint8Array;
  readonly metadata: Uint8Array;
}

/**
 * One block of decoded audio, shared read-only across a track's subscribers
 * as a `VideoFrame` is.
 */
export interface AudioFrame {
  readonly _tag: "AudioFrame";
  readonly track: string;
  readonly sampleRate: number;
  readonly channels: number;
  /** Owned, interleaved signed 16-bit PCM samples. */
  readonly samples: Int16Array;
}

/** Observed transport pressure, scoped to one connection generation. */
export interface MediaPressure {
  readonly closed: boolean;
  readonly queuedControl: number;
  readonly queuedVideo: number;
  readonly queuedAudio: number;
  readonly queuedBytes: number;
  readonly droppedVideo: bigint;
  readonly droppedAudio: bigint;
  readonly pendingRequests: number;
  readonly deliveredVideo: bigint;
  readonly deliveredAudio: bigint;
}

/** A peer capability. It owns its bounded buffers and source termination. */
export interface RawMedia {
  readonly video: (name: string) => Stream.Stream<VideoFrame, ReactorError>;
  readonly audio: (name: string) => Stream.Stream<AudioFrame, ReactorError>;
  readonly snapshot: Effect.Effect<MediaPressure, ReactorError>;
}

/**
 * A generation's streams end or fail with their source. Reconnect creates a new
 * value; it never silently replaces the source underneath an existing reader.
 */
export interface MediaGeneration extends RawMedia {
  readonly generation: bigint;
  readonly tracks: readonly Track[];
  readonly retired: Effect.Effect<never, ReactorError>;
}

/** Host track capability bound to one negotiated connection, without DOM types. */
export interface TrackGeneration {
  readonly generation: bigint;
  readonly tracks: readonly Track[];
  readonly retired: Effect.Effect<never, ReactorError>;
  readonly track: (name: string) => Effect.Effect<MediaTrack, ReactorError, Scope.Scope>;
  readonly publish: (name: string, source: MediaTrack) => Effect.Effect<void, ReactorError>;
  readonly unpublish: (name: string) => Effect.Effect<void, ReactorError>;
  readonly setTrackActive: (name: string, active: boolean) => Effect.Effect<void, ReactorError>;
  readonly setMaxBitrate: (
    name: string,
    bitsPerSecond: number,
  ) => Effect.Effect<void, ReactorError>;
}
