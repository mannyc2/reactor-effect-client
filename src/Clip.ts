import { Schema } from "effect"
import { alignSecondsTo, clampSecondsTo, h3ReferenceTurboRealtime, maxSeconds, minSeconds } from "./ModelProfile.js"

export const profile = h3ReferenceTurboRealtime
export const FPS = profile.fps
export const MIN_SECONDS = minSeconds(profile)
export const MAX_SECONDS = maxSeconds(profile)
export const PROMPT_MAX_CHARS = profile.prompt.maxChars

export const alignSeconds = (seconds: number): number => alignSecondsTo(profile, seconds)
export const clampSeconds = (seconds: number): number => clampSecondsTo(profile, seconds)

export const ClipId = Schema.String.pipe(Schema.brand("ClipId"))
export type ClipId = typeof ClipId.Type

export const Canvas = Schema.Literals(["16:9", "1:1", "9:16", "4:3"])
export type Canvas = typeof Canvas.Type

/** A transport reference. Applications may carry additional fields; the SDK only owns the URI. */
export const ReferenceImage = Schema.Struct({ uri: Schema.String })
export type ReferenceImage = typeof ReferenceImage.Type

/** Opaque application metadata. H3 stores it inside the adapter envelope and never interprets its keys. */
export const ClipMetadata = Schema.JsonObject
export type ClipMetadata = typeof ClipMetadata.Type

/**
 * Optional affinity for incrementally admitted work. The first possibly-effectful
 * dispatch binds the sequence to one session; later members never migrate or resend.
 * `final` says no more members will be admitted after this one.
 */
export const ClipSequence = Schema.Struct({
  id: Schema.NonEmptyString,
  memberId: Schema.optionalKey(Schema.NonEmptyString),
  final: Schema.Boolean
})
export type ClipSequence = typeof ClipSequence.Type

export class ClipRequest extends Schema.Class<ClipRequest>("ClipRequest")({
  /** Optional spoken text for alternate renderers; H3 uses the compiled prompt. */
  speech: Schema.optionalKey(Schema.String),
  prompt: Schema.String,
  references: Schema.Array(ReferenceImage),
  durationSeconds: Schema.Number,
  continueFrom: Schema.optional(ClipId),
  startingFrame: Schema.optional(ReferenceImage),
  endingFrame: Schema.optional(ReferenceImage),
  seed: Schema.optional(Schema.Int),
  position: Schema.optional(Schema.Natural),
  before: Schema.optionalKey(ClipId),
  metadata: ClipMetadata,
  sequence: Schema.optionalKey(ClipSequence)
}) {}
