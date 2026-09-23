import type * as Duration from "effect/Duration";
import type * as Effect from "effect/Effect";
import type { ModelProfile } from "../h3/profile.js";
import type { LocalClipRecord } from "../orchestration/types.js";
import type { AudioFrame, VideoFrame } from "../session/media.js";

export interface SimulatedFaults {
  readonly buildFails?: (sequence: number) => boolean;
  readonly sessionFails?: (sequence: number) => boolean;
}

export interface SimulatedMediaSink {
  readonly video: (frame: VideoFrame) => Effect.Effect<void>;
  readonly audio: (frame: AudioFrame) => Effect.Effect<void>;
}

export interface SimOptions {
  /** Simulated build time per second of clip, 0.1 by default. */
  readonly buildRatio?: number;
  /**
   * Simulated build time added to every clip's, zero by default. A bare number
   * is milliseconds.
   */
  readonly fixedBuildTime?: Duration.Input | undefined;
  readonly profile?: ModelProfile;
  readonly queueLimit?: number;
  readonly playoutLimit?: number;
  readonly timing?: "measured" | "unknown";
  /**
   * A pause between presented clips, zero by default. A bare number is
   * milliseconds.
   */
  readonly playoutGap?: Duration.Input | undefined;
  /** @deprecated Removed in 0.3.0: use `fixedBuildTime` (a bare number is milliseconds). */
  readonly buildFixedMs?: never;
  /** @deprecated Removed in 0.3.0: use `playoutGap` (a bare number is milliseconds). */
  readonly playoutGapMs?: never;
  readonly faults?: SimulatedFaults;
  /** Completes before Ready and may extend duration to fit locally rendered speech. */
  readonly build?: (record: LocalClipRecord) => Effect.Effect<number, Error>;
  /** Starts after the source emits Started; media remains owned by the caller's renderer. */
  readonly present?: (
    record: LocalClipRecord,
    startedAt: number,
    sink: SimulatedMediaSink,
  ) => Effect.Effect<void, Error>;
  /** Release a rendered clip that was discarded without presentation. */
  readonly discard?: (record: LocalClipRecord) => Effect.Effect<void>;
}
