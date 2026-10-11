/** Playback of streaming PCM, independent of its provider and presentation host. */
import type * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import type * as Stream from "effect/Stream";
import type { AudioFrame } from "./Media.js";

export class PlaybackError extends Schema.TaggedError<PlaybackError>(
  "reactor-effect-client/AudioPlayback/PlaybackError",
)("PlaybackError", {
  reason: Schema.Union([
    Schema.TaggedStruct("UnsupportedHost", { message: Schema.String }),
    Schema.TaggedStruct("InvalidAudio", { message: Schema.String }),
    Schema.TaggedStruct("Busy", { message: Schema.String }),
    Schema.TaggedStruct("Closed", { message: Schema.String }),
    Schema.TaggedStruct("Playback", { message: Schema.String }),
    Schema.TaggedStruct("Timeout", { message: Schema.String }),
  ]),
  /** Host diagnostics are private until explicitly inspected. */
  detail: Schema.Defect().pipe(Schema.Redacted, Schema.optionalKey),
}) {}

/** One acquired output, owned by its host's scope. */
export interface Playback {
  /**
   * Plays one stream in order, preserving its errors and requirements. Completion
   * means its last sample finished local playback, not that a remote audience
   * heard it. Interruption or failure silences queued audio. Concurrent calls fail
   * with Busy; the application chooses whether to interrupt or queue utterances.
   */
  readonly play: <E, R>(
    frames: Stream.Stream<AudioFrame, E, R>,
  ) => Effect.Effect<void, E | PlaybackError, R>;
  /** Current root-mean-square amplitude, from zero to one, of playing audio. */
  readonly level: Effect.Effect<number, PlaybackError>;
}
