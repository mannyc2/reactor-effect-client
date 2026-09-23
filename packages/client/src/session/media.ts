import type * as Effect from "effect/Effect";
import type * as Stream from "effect/Stream";
import type * as Scope from "effect/Scope";
import type { Track } from "../contract.js";
import type { ReactorError } from "../errors.js";
import type { MediaTrack } from "../PeerTypes.js";

/**
 * How a frame's `data` lays out its pixels: four bytes per pixel, rows packed
 * without padding, in this channel order. A host declares the format it
 * produces; nothing converts between formats implicitly.
 */
export type VideoFormat = "BGRA" | "RGBA";

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
  /**
   * The frame's admission sequence on its track in this generation, from 0,
   * taken before any bounded queue could drop it: a gap between consecutive
   * frames is the count of frames the host dropped there. `recorder` makes the
   * gaps explicit.
   */
  readonly sequence: bigint;
  /** The pixel layout of `data`; the native host produces `"BGRA"`. */
  readonly format: VideoFormat;
  /**
   * Owned pixel bytes in `format`, `width * height * 4` of them: the whole of
   * an `ArrayBuffer` of their own, so the buffer can be transferred. Retaining
   * a frame never retains native memory.
   */
  readonly data: Uint8Array<ArrayBuffer>;
  readonly metadata: Uint8Array<ArrayBuffer>;
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
  /** The block's admission sequence on its track in this generation, as a `VideoFrame`'s. */
  readonly sequence: bigint;
  /** Owned, interleaved signed 16-bit PCM samples, in an `ArrayBuffer` of their own. */
  readonly samples: Int16Array<ArrayBuffer>;
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
  /**
   * Readers of this generation's tracks that fell behind their per-reader
   * bound. Each such reader failed with `Overflow` and missed every frame
   * after it: the loss a recorder cannot see in `droppedVideo`/`droppedAudio`,
   * which count what the transport discarded before any reader.
   */
  readonly readerOverflows: bigint;
}

/** A peer capability. It owns its bounded buffers and source termination. */
export interface RawMedia {
  readonly video: (name: string) => Stream.Stream<VideoFrame, ReactorError>;
  readonly audio: (name: string) => Stream.Stream<AudioFrame, ReactorError>;
  readonly snapshot: Effect.Effect<MediaPressure, ReactorError>;
}

/**
 * A generation's streams end or fail with their source.
 *
 * Two ways to consume a track over one generation:
 * - **Recorder:** read the stream directly. Every element is an admitted
 *   frame. Loss before admission shows as a rise in `droppedVideo` or
 *   `droppedAudio`; a reader that falls behind its bound fails with
 *   `Overflow` (and counts in `readerOverflows`); the generation's end is
 *   `retired`.
 * - **Preview:** keep only the newest frame, per consumer, with
 *   `Stream.buffer({ capacity: 1, strategy: "sliding" })`. It never overflows
 *   and never delays other readers. Reconnect creates a new
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
