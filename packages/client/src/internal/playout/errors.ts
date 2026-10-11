/**
 * Playout's item identity, the refusals a submission can meet before anything
 * is sent, and the filler failure that stops a playout.
 */
import * as Schema from "effect/Schema";

/** A source command's dispatch outcome and what can make a later attempt useful. */
export class CommandError extends Schema.TaggedError<CommandError>()("CommandError", {
  reason: Schema.Union([
    Schema.TaggedStruct("NotSubmitted", {
      next: Schema.Literals(["retry", "await-change", "none"]),
    }),
    Schema.TaggedStruct("Replied", { next: Schema.Literals(["retry", "none"]) }),
    Schema.TaggedStruct("Unknown", {}),
  ]),
  operation: Schema.Literals(["enqueue", "remove", "move", "setAutoplay", "stop", "play"]),
  message: Schema.String,
  /** Provider diagnostics are private and never decide a retry. */
  detail: Schema.Defect().pipe(Schema.Redacted, Schema.optionalKey),
}) {}

/** A source was lost, its media failed, or its opening options were invalid. */
export class SourceError extends Schema.TaggedError<SourceError>()("SourceError", {
  reason: Schema.Union([
    Schema.TaggedStruct("Lost", {}),
    Schema.TaggedStruct("Media", {}),
    Schema.TaggedStruct("InvalidInput", {}),
  ]),
  message: Schema.String,
  detail: Schema.Defect().pipe(Schema.Redacted, Schema.optionalKey),
}) {}

/** A failure the playout itself produces, without interpreting a provider's errors. */
export class PlayoutStopped extends Schema.TaggedError<PlayoutStopped>()("PlayoutStopped", {
  reason: Schema.Union([
    Schema.TaggedStruct("OpenTimeout", {}),
    Schema.TaggedStruct("SessionInUse", {}),
    Schema.TaggedStruct("ModelMismatch", {}),
    Schema.TaggedStruct("Moderated", {}),
    Schema.TaggedStruct("NoSession", {}),
  ]),
  message: Schema.String,
}) {}

export const ItemKey = Schema.NonEmptyString.pipe(Schema.brand("ItemKey"));
export type ItemKey = typeof ItemKey.Type;

/** The key is already used by an item with a different spec. */
export class KeyMismatch extends Schema.TaggedError<KeyMismatch>()("KeyMismatch", {
  key: ItemKey,
}) {}

/** The item could not start before its firm `startBy`, as far as the playout can project. */
export class WouldMissDeadline extends Schema.TaggedError<WouldMissDeadline>()(
  "WouldMissDeadline",
  { key: ItemKey },
) {}

/** The lane skips new items while one of its items is waiting or playing. */
export class LaneBusy extends Schema.TaggedError<LaneBusy>()("LaneBusy", {
  key: ItemKey,
  lane: Schema.String,
}) {}

/** The request names a lane, anchor or item the playout does not know, or is malformed. */
export class InvalidItem extends Schema.TaggedError<InvalidItem>()("InvalidItem", {
  key: Schema.String,
  message: Schema.String,
}) {}

/**
 * The filler clip at `index` asked for a request outside its model's documented
 * limits. The playout fails with it rather than skip the clip and leave the air
 * uncovered, since asking again would get the same request.
 */
export class InvalidFiller extends Schema.TaggedError<InvalidFiller>()("InvalidFiller", {
  index: Schema.Int,
  message: Schema.String,
}) {}

/** The playout is draining or closed, so it admits nothing new. */
export class PlayoutClosed extends Schema.TaggedError<PlayoutClosed>()("PlayoutClosed", {}) {}

export type SubmitError = KeyMismatch | WouldMissDeadline | LaneBusy | InvalidItem | PlayoutClosed;
