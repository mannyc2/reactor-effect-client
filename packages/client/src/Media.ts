/**
 * Media a connected session carries: owned decoded frames from a native or
 * simulated host, or platform tracks from a browser. Every value here belongs
 * to one connection generation; a reconnect produces new ones.
 */
import type * as Effect from "effect/Effect";
import type * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import type { Track } from "./Peer.js";
import type { MediaTrack } from "./Peer.js";
import type { ReactorError } from "./ReactorError.js";

/** Four bytes per pixel, rows packed without padding, in this channel order. */
export type VideoFormat = "BGRA" | "RGBA";

/**
 * One decoded video frame. Every reader of a track receives the same frame and
 * buffers, so treat them as read-only and copy before changing them.
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
   * taken before any bounded queue could drop it: a gap is the count the host
   * dropped there. `recorder` makes the gaps explicit.
   */
  readonly sequence: bigint;
  readonly format: VideoFormat;
  /** `width * height * 4` owned bytes: the whole of an `ArrayBuffer` of their own. */
  readonly data: Uint8Array<ArrayBuffer>;
  readonly metadata: Uint8Array<ArrayBuffer>;
}

/** One block of decoded audio, shared read-only across a track's readers. */
export interface AudioFrame {
  readonly _tag: "AudioFrame";
  readonly track: string;
  readonly sampleRate: number;
  readonly channels: number;
  readonly sequence: bigint;
  /** Owned, interleaved signed 16-bit PCM in an `ArrayBuffer` of its own. */
  readonly samples: Int16Array<ArrayBuffer>;
}

/** Transport pressure observed in one connection generation. */
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
  /** Readers that fell behind their bound, failed with `Overflow` and missed the rest. */
  readonly readerOverflows: bigint;
}

interface Generation {
  readonly generation: bigint;
  readonly tracks: ReadonlyArray<Track>;
  /** Fails when this generation ends. */
  readonly retired: Effect.Effect<never, ReactorError>;
}

/**
 * A generation's decoded media. Read a track directly to record it (a reader
 * that falls behind fails with `Overflow`), or keep the newest frame for a
 * preview with `Stream.buffer({ capacity: 1, strategy: "sliding" })`.
 */
export interface DecodedMedia extends Generation {
  readonly video: (name: string) => Stream.Stream<VideoFrame, ReactorError>;
  readonly audio: (name: string) => Stream.Stream<AudioFrame, ReactorError>;
  readonly pressure: Effect.Effect<MediaPressure, ReactorError>;
  /** Pauses or resumes a received track, here and at Reactor. */
  readonly setTrackActive: (name: string, active: boolean) => Effect.Effect<void, ReactorError>;
}

/** A generation's platform tracks: leases of received tracks and publication of sent ones. */
export interface TrackMedia extends Generation {
  readonly track: (name: string) => Effect.Effect<MediaTrack, ReactorError, Scope.Scope>;
  readonly publish: (name: string, source: MediaTrack) => Effect.Effect<void, ReactorError>;
  readonly unpublish: (name: string) => Effect.Effect<void, ReactorError>;
  readonly setTrackActive: (name: string, active: boolean) => Effect.Effect<void, ReactorError>;
  readonly setMaxBitrate: (
    name: string,
    bitsPerSecond: number,
  ) => Effect.Effect<void, ReactorError>;
}

/** A recorder's view of one track: each frame, and each run the host dropped, in place. */
export type Recorded<F> =
  | { readonly _tag: "Frame"; readonly frame: F }
  | {
      readonly _tag: "Lost";
      /** The sequence of the last frame received before the loss. */
      readonly after: bigint;
      readonly count: bigint;
    };

/**
 * Every frame a track delivers, with a `Lost` element wherever the host dropped
 * frames between two it delivered. The view starts at the first frame
 * received, and a sequence that goes back starts a new run (a new generation).
 */
export const recorder = <F extends { readonly sequence: bigint }, E, R>(
  frames: Stream.Stream<F, E, R>,
): Stream.Stream<Recorded<F>, E, R> =>
  frames.pipe(
    Stream.mapAccum(
      (): bigint | undefined => undefined,
      (last, frame): readonly [bigint, ReadonlyArray<Recorded<F>>] => {
        const received: Recorded<F> = { _tag: "Frame", frame };
        if (last === undefined || frame.sequence <= last || frame.sequence === last + 1n)
          return [frame.sequence, [received]];
        return [
          frame.sequence,
          [{ _tag: "Lost", after: last, count: frame.sequence - last - 1n }, received],
        ];
      },
    ),
  );
