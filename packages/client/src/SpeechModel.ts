/** Streaming speech generation, independent of a provider, transport or playback device. */
import * as Context from "effect/Context";
import * as Schema from "effect/Schema";
import type * as Stream from "effect/Stream";
import type { AudioFrame } from "./Media.js";

export interface Request {
  /** Complete text for one utterance. Providers must keep it out of errors and telemetry. */
  readonly text: string;
}

/** A generation's failure, with evidence of whether the provider received its request. */
export class SpeechError extends Schema.TaggedError<SpeechError>(
  "reactor-effect-client/SpeechModel/SpeechError",
)("SpeechError", {
  reason: Schema.Union([
    Schema.TaggedStruct("InvalidInput", { message: Schema.String }),
    Schema.TaggedStruct("Transport", { message: Schema.String }),
    Schema.TaggedStruct("Http", { status: Schema.Int }),
    Schema.TaggedStruct("InvalidAudio", { message: Schema.String }),
  ]),
  outcome: Schema.Literals(["not-submitted", "unknown", "replied"]),
  /** Foreign diagnostics can contain the transcript or credentials. Inspect explicitly only. */
  detail: Schema.Defect().pipe(Schema.Redacted, Schema.optionalKey),
}) {}

/**
 * Each consumption of `stream` starts one generation. Blocks arrive in order as
 * generated, without playback pacing; the consumer owns presentation and its clock.
 * `sequence` starts at zero for each utterance. Completion means the provider's
 * output ended, not that a listener heard it. The final duration is not known ahead.
 *
 * Interrupting consumption releases the local request; it does not prove remote
 * cancellation or refund. A stream can fail after emitting audio. Adapters never
 * retry or switch providers implicitly, which could duplicate audible output.
 */
export class SpeechModel extends Context.Service<
  SpeechModel,
  { readonly stream: (request: Request) => Stream.Stream<AudioFrame, SpeechError> }
>()("reactor-effect-client/SpeechModel") {}
