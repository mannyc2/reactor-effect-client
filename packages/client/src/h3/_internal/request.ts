import { ReactorError } from "../../errors.js";
import { checkedString } from "../../protobuf.js";
import type { UploadReference } from "../../wire.generated.js";
import { metadataMaxChars, referenceLimits, requestSeconds } from "../profile.js";
import type { Request, ValidatedReference } from "../types.js";
import type { CommandArgs } from "./contracts.js";
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
    ...(request.metadata === undefined
      ? {}
      : { metadata: checkedString(request.metadata, metadataMaxChars) }),
  });
};

/** Project validated, captured input once; no reference validation or host IO happens here. */
export const enqueueArguments = (
  request: CapturedRequest,
  files: readonly UploadReference[],
  metadata: string,
): CommandArgs<"enqueue"> =>
  Object.freeze({
    prompt: request.prompt,
    reference_images: files.map((file) => ({
      upload_id: file.upload_id,
      name: file.name,
      mime_type: file.mime_type,
      size: Number(file.size),
    })),
    metadata,
    ...(request.seconds === undefined ? {} : { seconds: request.seconds }),
    ...(request.seed === undefined ? {} : { seed: request.seed }),
    ...(request.position === undefined ? {} : { position: request.position }),
    ...(request.continueFrom === undefined ? {} : { continue_from_clip_id: request.continueFrom }),
  });
