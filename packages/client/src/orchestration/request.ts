import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import { jsonObject } from "../json.js";
import { CommandFailure } from "../session/commands.js";
import { ReactorError } from "../errors.js";

export const ClipId = Schema.String.pipe(Schema.brand("ClipId"));
export type ClipId = typeof ClipId.Type;
export const Canvas = Schema.Literals(["16:9", "1:1", "9:16", "4:3"]);
export type Canvas = typeof Canvas.Type;
export const ReferenceImage = Schema.Struct({ uri: Schema.String });
export type ReferenceImage = typeof ReferenceImage.Type;
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
  durationSeconds: Schema.Number,
  metadata: ClipMetadata,
  seed: Schema.optionalKey(Schema.Number),
  position: Schema.optionalKey(Schema.Number),
  /** Physical source affinity only; unlike before, this does not choose queue position. */
  sameSessionAs: Schema.optionalKey(ClipId),
  before: Schema.optionalKey(ClipId),
  continueFrom: Schema.optionalKey(ClipId),
  speech: Schema.optionalKey(Schema.String),
  sequence: Schema.optionalKey(ClipSequence),
}) {}

/** Local admission is a policy decision, with explicit proof of no dispatch. */
export class PolicyFailure extends CommandFailure.extend<PolicyFailure>(
  "reactor-effect-client/PolicyFailure",
)({ reason: Schema.String }) {
  /** A local refusal of `operation`, which was therefore never dispatched. */
  static refuse(
    reason: string,
    message: string,
    operation = "enqueue",
    cause?: unknown,
  ): PolicyFailure {
    return new PolicyFailure({
      code: reason === "invalid_request" ? "InvalidInput" : "InvalidState",
      message,
      context: {
        operation,
        outcome: "not-submitted",
        ...(cause === undefined ? {} : { detail: cause }),
      },
      reason,
    });
  }
}

const captured = new WeakSet<ClipRequest>();
const fields = new Set([
  "prompt",
  "references",
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
export const captureRequest = (input: ClipRequest): Effect.Effect<ClipRequest, PolicyFailure> =>
  Effect.try({
    try: () => {
      if (input !== null && typeof input === "object" && captured.has(input)) return input;
      if (input === null || typeof input !== "object" || Array.isArray(input))
        throw new TypeError("request must be an object");
      const raw: Record<string, unknown> = {};
      if (Object.getOwnPropertySymbols(input).length > 0)
        throw new TypeError("request contains symbol fields");
      for (const [key, descriptor] of Object.entries(Object.getOwnPropertyDescriptors(input))) {
        if (!fields.has(key) || !("value" in descriptor))
          throw new TypeError("request contains unsupported fields or accessors");
        Object.defineProperty(raw, key, { value: descriptor.value, enumerable: true });
      }
      // The shared JSON boundary owns cycle, prototype, finite-number and byte
      // validation. It copies nested arrays/objects without invoking accessors.
      const owned = jsonObject(raw);
      const request = Schema.decodeUnknownSync(ClipRequest)(owned, { onExcessProperty: "error" });
      if (
        request.prompt.trim().length === 0 ||
        !Number.isFinite(request.durationSeconds) ||
        request.durationSeconds <= 0
      ) {
        throw new TypeError("prompt and positive finite duration are required");
      }
      if (
        request.references.length > 9 ||
        request.references.some((reference) => reference.uri.length === 0)
      ) {
        throw new TypeError("references must contain at most nine nonempty URIs");
      }
      if (
        request.position !== undefined &&
        (!Number.isSafeInteger(request.position) || request.position < 0)
      ) {
        throw new TypeError("position must be a nonnegative safe integer");
      }
      if (request.seed !== undefined && (!Number.isSafeInteger(request.seed) || request.seed < 0)) {
        throw new TypeError("seed must be a nonnegative safe integer");
      }
      if (request.sameSessionAs === "")
        throw new TypeError("source affinity identity cannot be empty");
      if (
        request.sequence !== undefined &&
        (request.sequence.id.length === 0 || request.sequence.memberId === "")
      ) {
        throw new TypeError("sequence identities cannot be empty");
      }
      freeze(request);
      captured.add(request);
      return request;
    },
    catch: (cause) =>
      PolicyFailure.refuse(
        "invalid_request",
        "Clip request is malformed or contains unsupported fields",
        "enqueue",
        cause,
      ),
  });

/** An error during caller-owned prework cannot imply that enqueue was sent. */
export const preworkFailure = (operation: string, cause: unknown): CommandFailure =>
  cause instanceof CommandFailure && cause.context.outcome === "not-submitted"
    ? cause
    : CommandFailure.from(
        cause instanceof ReactorError
          ? cause
          : new ReactorError({ code: "InvalidInput", message: `${operation} preparation failed` }),
        {
          operation,
          outcome: "not-submitted",
          detail: cause,
        },
      );
