import { ReactorError } from "../../errors.js";
import { checkedString } from "../../protobuf.js";
import { referenceLimits, requestSeconds } from "../profile.js";
import type { Request, ValidatedReference } from "../types.js";
import { captureReference, plain } from "./references.js";

export interface CapturedRequest extends Omit<Request, "references"> {
  readonly references: readonly ValidatedReference[];
}
const bad = (message: string): never => {
  throw new ReactorError("InvalidInput", message, {
    operation: "enqueue",
    outcome: "not-submitted",
  });
};
export const nonnegative = (value: unknown, field: string): number => {
  if (typeof value !== "number" || !Number.isSafeInteger(value) || value < 0)
    return bad(`${field} must be a nonnegative safe integer`);
  return value;
};
export const seconds = (
  value: unknown,
  bounds: { readonly min: number; readonly max: number } = requestSeconds,
): number => {
  if (
    typeof value !== "number" ||
    !Number.isFinite(value) ||
    value < bounds.min ||
    value > bounds.max
  )
    return bad("Clip duration is outside the accepted request bounds");
  return value;
};
export const clipId = (input: unknown): string => {
  if (
    typeof input !== "string" ||
    !/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(input)
  )
    return bad("clipId must be a UUID");
  return input;
};
export const captureRequest = (input: Request, maxPromptBytes: number): CapturedRequest => {
  const request = plain(input, [
    "prompt",
    "references",
    "seconds",
    "seed",
    "position",
    "continueFrom",
    "metadata",
  ]);
  if (typeof request.prompt !== "string" || request.prompt.trim().length === 0)
    return bad("Prompt must contain text");
  checkedString(request.prompt, maxPromptBytes);
  if (new TextEncoder().encode(request.prompt).length > maxPromptBytes)
    return bad("Prompt exceeds the configured SDK byte bound");
  if (request.metadata !== undefined && typeof request.metadata !== "string")
    return bad("Metadata must be a string");
  const references = request.references ?? [];
  if (!Array.isArray(references) || references.length > referenceLimits.maxImages)
    return bad("H3 accepts at most nine image references");
  return Object.freeze({
    prompt: request.prompt,
    references: Object.freeze(
      references.map((reference: unknown) => captureReference(reference as ValidatedReference)),
    ),
    ...(request.seconds === undefined ? {} : { seconds: seconds(request.seconds) }),
    ...(request.seed === undefined ? {} : { seed: nonnegative(request.seed, "seed") }),
    ...(request.position === undefined
      ? {}
      : { position: nonnegative(request.position, "position") }),
    ...(request.continueFrom === undefined ? {} : { continueFrom: clipId(request.continueFrom) }),
    ...(request.metadata === undefined ? {} : { metadata: checkedString(request.metadata, 2000) }),
  });
};

interface Metadata {
  readonly namespace: string;
  readonly submission: string;
  readonly caller: string;
}
/** Namespaced acceptance metadata is not a claim about the provider's own state. */
export const encodeMetadata = (namespace: string, submission: string, caller = ""): string => {
  const value = JSON.stringify({ reactor_effect_h3: 1, namespace, submission, caller });
  if (Array.from(value).length > 2000)
    return bad("Metadata including acceptance identity exceeds 2000 characters");
  return value;
};
export const decodeMetadata = (value: string): Metadata | undefined => {
  try {
    const object: unknown = JSON.parse(value);
    const fields = plain(object, ["reactor_effect_h3", "namespace", "submission", "caller"]);
    if (
      fields.reactor_effect_h3 !== 1 ||
      typeof fields.namespace !== "string" ||
      typeof fields.submission !== "string" ||
      typeof fields.caller !== "string"
    )
      return undefined;
    return { namespace: fields.namespace, submission: fields.submission, caller: fields.caller };
  } catch {
    return undefined;
  }
};
