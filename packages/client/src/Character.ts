/** A persistent character, independent of its renderer, host or speech provider. */
import type * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";

/** One advance of a character's animation and presentation. */
export interface Frame {
  /** Nonnegative elapsed seconds since the previous frame. */
  readonly seconds: number;
  /** Normalized mouth opening, from zero (closed) to one (open). */
  readonly mouth: number;
}

/** Loading, rendering or capturing a character failed. */
export class CharacterError extends Schema.TaggedError<CharacterError>(
  "reactor-effect-client/Character/CharacterError",
)("CharacterError", {
  reason: Schema.Union([
    Schema.TaggedStruct("Load", { message: Schema.String }),
    Schema.TaggedStruct("Render", { message: Schema.String }),
    Schema.TaggedStruct("Capture", { message: Schema.String }),
    Schema.TaggedStruct("InvalidInput", { message: Schema.String }),
    Schema.TaggedStruct("UnsupportedHost", { message: Schema.String }),
  ]),
  /** Foreign diagnostics may contain private model addresses. Inspect explicitly only. */
  detail: Schema.Defect().pipe(Schema.Redacted, Schema.optionalKey),
}) {}

/**
 * One loaded character. Its caller acquires it in a scope and owns its lifetime.
 * Rendering advances idle animation and draws the resulting pose. It makes no
 * claim about a clip duration, captured video or delivery to an audience.
 */
export interface Renderer {
  readonly render: (frame: Frame) => Effect.Effect<void, CharacterError>;
}
