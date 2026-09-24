import * as Effect from "effect/Effect";
import * as Result from "effect/Result";
import * as Schema from "effect/Schema";
import { jsonObject } from "../json.js";
import {
  AcquisitionFailure,
  CommandFailure,
  parse,
  PolicyFailure,
  ReactorError,
} from "../errors.js";

export { PolicyFailure };

export const ClipId = Schema.String.pipe(Schema.brand("ClipId"));
export type ClipId = typeof ClipId.Type;
export const Canvas = Schema.Literals(["16:9", "1:1", "9:16", "4:3"]);
export type Canvas = typeof Canvas.Type;
export const ReferenceImage = Schema.Struct({ uri: Schema.String });
export type ReferenceImage = typeof ReferenceImage.Type;
/** An audio reference's location, loaded as an image reference is. */
export const ReferenceAudio = Schema.Struct({ uri: Schema.String });
export type ReferenceAudio = typeof ReferenceAudio.Type;
export const ClipMetadata = Schema.JsonObject;
export type ClipMetadata = typeof ClipMetadata.Type;
export const ClipSequence = Schema.Struct({
  id: Schema.NonEmptyString,
  final: Schema.Boolean,
  memberId: Schema.optionalKey(Schema.NonEmptyString),
});
export type ClipSequence = typeof ClipSequence.Type;

/** Application input. Provider commands never accept these scheduling fields. */
export class ClipRequest extends Schema.Class<ClipRequest>("OrchestrationClipRequest")({
  prompt: Schema.String,
  references: Schema.Array(ReferenceImage),
  /**
   * Up to three audio references whose voice or soundscape conditions the
   * clip's soundtrack, at most two with `continueFrom`. A clip with audio needs
   * an image reference or `continueFrom`.
   */
  audio: Schema.optionalKey(Schema.Array(ReferenceAudio)),
  durationSeconds: Schema.Finite,
  metadata: ClipMetadata,
  seed: Schema.optionalKey(Schema.Finite),
  position: Schema.optionalKey(Schema.Finite),
  /** Physical source affinity only; unlike before, this does not choose queue position. */
  sameSessionAs: Schema.optionalKey(ClipId),
  before: Schema.optionalKey(ClipId),
  continueFrom: Schema.optionalKey(ClipId),
  speech: Schema.optionalKey(Schema.String),
  sequence: Schema.optionalKey(ClipSequence),
}) {}

const captured = new WeakSet<ClipRequest>();
const fields = new Set([
  "prompt",
  "references",
  "audio",
  "durationSeconds",
  "metadata",
  "seed",
  "position",
  "sameSessionAs",
  "before",
  "continueFrom",
  "speech",
  "sequence",
]);

const freeze = <A>(value: A): A => {
  if (value !== null && typeof value === "object" && !Object.isFrozen(value)) {
    for (const child of Object.values(value)) freeze(child);
    Object.freeze(value);
  }
  return value;
};

/** Capture application input once; routing and a physical source share that value. */
const invalidRequest = (cause: unknown): PolicyFailure =>
  PolicyFailure.refuse(
    "InvalidRequest",
    "Clip request is malformed or contains unsupported fields",
    "enqueue",
    cause,
  );

/** The request's own data fields, copied without invoking accessors, or why they are not. */
const ownedFields = (input: ClipRequest): Result.Result<Record<string, unknown>, string> => {
  if (input === null || typeof input !== "object" || Array.isArray(input))
    return Result.fail("request must be an object");
  const raw: Record<string, unknown> = {};
  if (Object.getOwnPropertySymbols(input).length > 0)
    return Result.fail("request contains symbol fields");
  for (const [key, descriptor] of Object.entries(Object.getOwnPropertyDescriptors(input))) {
    if (!fields.has(key) || !("value" in descriptor))
      return Result.fail("request contains unsupported fields or accessors");
    Object.defineProperty(raw, key, { value: descriptor.value, enumerable: true });
  }
  // The shared JSON boundary owns cycle, prototype, finite-number and byte
  // validation. It copies nested arrays/objects without invoking accessors.
  return Result.mapError(
    parse(() => jsonObject(raw)),
    (error) => error.message,
  );
};

/** Checks the decoded request's cross-field rules, then freezes and records it. */
const checked = (request: ClipRequest): Result.Result<ClipRequest, string> => {
  if (
    request.prompt.trim().length === 0 ||
    !Number.isFinite(request.durationSeconds) ||
    request.durationSeconds <= 0
  )
    return Result.fail("prompt and positive finite duration are required");
  if (
    request.references.length > 9 ||
    request.references.some((reference) => reference.uri.length === 0)
  )
    return Result.fail("references must contain at most nine nonempty URIs");
  const audio = request.audio ?? [];
  const continued = request.continueFrom !== undefined;
  if (audio.length > (continued ? 2 : 3) || audio.some((reference) => reference.uri.length === 0))
    return Result.fail(
      "audio must contain at most three nonempty URIs, or two with a continuation",
    );
  if (audio.length > 0 && request.references.length === 0 && !continued)
    return Result.fail("audio needs an image reference or a continuation");
  if (
    request.position !== undefined &&
    (!Number.isSafeInteger(request.position) || request.position < 0)
  )
    return Result.fail("position must be a nonnegative safe integer");
  if (request.seed !== undefined && (!Number.isSafeInteger(request.seed) || request.seed < 0))
    return Result.fail("seed must be a nonnegative safe integer");
  if (request.sameSessionAs === "") return Result.fail("source affinity identity cannot be empty");
  if (
    request.sequence !== undefined &&
    (request.sequence.id.length === 0 || request.sequence.memberId === "")
  )
    return Result.fail("sequence identities cannot be empty");
  freeze(request);
  captured.add(request);
  return Result.succeed(request);
};

/**
 * Parses caller input into a frozen, recorded request. Each check returns its
 * rejection as a Result, so a malformed request is an InvalidRequest refusal
 * and a bug in a check stays a defect.
 */
export const captureRequest = (input: ClipRequest): Effect.Effect<ClipRequest, PolicyFailure> =>
  Effect.gen(function* () {
    if (input !== null && typeof input === "object" && captured.has(input)) return input;
    const owned = yield* Effect.fromResult(ownedFields(input));
    const request = yield* Schema.decodeUnknownEffect(ClipRequest)(owned, {
      onExcessProperty: "error",
    });
    return yield* Effect.fromResult(checked(request));
  }).pipe(Effect.mapError(invalidRequest));

/** An error during caller-owned prework cannot imply that enqueue was sent. */
export const preworkFailure = (
  operation: string,
  cause: unknown,
): CommandFailure | PolicyFailure =>
  PolicyFailure.is(cause) || (CommandFailure.is(cause) && cause.context.outcome === "not-submitted")
    ? cause
    : CommandFailure.from(
        ReactorError.is(cause) || CommandFailure.is(cause) || AcquisitionFailure.is(cause)
          ? cause
          : ReactorError.fromCode("InvalidInput", `${operation} preparation failed`),
        {
          operation,
          outcome: "not-submitted",
          detail: cause,
        },
      );
