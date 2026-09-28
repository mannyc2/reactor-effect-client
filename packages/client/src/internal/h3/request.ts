/** An H3 clip request, and its projection onto the `enqueue` command's arguments. */
import * as Schema from "effect/Schema";
import type { UploadReference } from "../../Session.js";
import type { CommandArgs } from "./commands.js";
import {
  audioReferenceLimits,
  metadataMaxChars,
  referenceLimits,
  requestSeconds,
} from "./profile.js";
import type { ValidatedAudioReference, ValidatedReference } from "./references.js";
import {
  Reference,
  ValidatedAudioReferenceSchema,
  ValidatedReferenceSchema,
} from "./references.js";

const isWellFormed = (text: string): boolean => {
  for (let index = 0; index < text.length; index++) {
    const code = text.charCodeAt(index);
    if (code >= 0xdc00 && code <= 0xdfff) return false;
    if (code >= 0xd800 && code <= 0xdbff) {
      const next = text.charCodeAt(++index);
      if (!(next >= 0xdc00 && next <= 0xdfff)) return false;
    }
  }
  return true;
};

/** The SDK's own prompt bound; the provider's tokenizer decides the model's. */
const maxPromptBytes = 1_048_576;

/**
 * A clip request. Up to three audio references (`Audio 1`, `Audio 2`, ... in
 * the prompt), each 2–15 s of WAV, MP3, AAC/M4A, OGG/Opus, FLAC or WebM, mono
 * or stereo, at most 25 MiB. A clip with audio needs at least one image
 * reference or a `continueFrom`, and a continued clip takes at most two,
 * because its continuation uses the third for the previous clip's soundtrack.
 * Audio is sent only to a deployment that declares it (`Contract.referenceAudio`).
 */
export const Request = Schema.Struct({
  prompt: Schema.String.check(
    Schema.makeFilter((prompt) => prompt.trim().length > 0 || "must contain text"),
    Schema.makeFilter((prompt) => isWellFormed(prompt) || "must be well-formed UTF-16"),
    Schema.makeFilter(
      (prompt) =>
        new TextEncoder().encode(prompt).length <= maxPromptBytes ||
        "exceeds the SDK's 1 MiB bound",
    ),
  ),
  references: Schema.optionalKey(
    Schema.Array(Schema.Union([Reference, ValidatedReferenceSchema])).check(
      Schema.isMaxLength(referenceLimits.maxImages),
    ),
  ),
  audio: Schema.optionalKey(
    Schema.Array(Schema.Union([Reference, ValidatedAudioReferenceSchema])).check(
      Schema.isMaxLength(audioReferenceLimits.maxAudio),
    ),
  ),
  seconds: Schema.optionalKey(
    Schema.Finite.check(
      Schema.isBetween({ minimum: requestSeconds.min, maximum: requestSeconds.max }),
    ),
  ),
  seed: Schema.optionalKey(Schema.Natural),
  position: Schema.optionalKey(Schema.Natural),
  continueFrom: Schema.optionalKey(Schema.String.check(Schema.isUUID())),
  /** The caller's own metadata; acceptance wraps it and gives it back unchanged. */
  metadata: Schema.optionalKey(
    Schema.String.check(
      Schema.makeFilter(
        (metadata) => Array.from(metadata).length <= metadataMaxChars || "is too long",
      ),
    ),
  ),
}).check(
  Schema.makeFilter((request) => {
    const images = request.references?.length ?? 0;
    const audio = request.audio?.length ?? 0;
    const continued = request.continueFrom !== undefined;
    if (continued && audio > audioReferenceLimits.maxAudioWithContinuation)
      return "a continued clip takes at most two audio references";
    if (audio > 0 && images === 0 && !continued)
      return "audio references need an image reference or a continuation";
    if (images + audio + (continued ? 1 : 0) > audioReferenceLimits.maxTotal)
      return "H3 takes at most twelve references in total";
    return true;
  }),
);
export type Request = typeof Request.Type;

/** A request whose references were validated, and so hold their own copies. */
export interface Captured extends Omit<Request, "references" | "audio"> {
  readonly references: ReadonlyArray<ValidatedReference>;
  readonly audio: ReadonlyArray<ValidatedAudioReference>;
}

const wireUpload = (file: UploadReference) => ({
  upload_id: file.uploadId,
  name: file.name,
  mime_type: file.mimeType,
  size: Number(file.size),
});

/** The `enqueue` arguments; audio is sent only when the request has some. */
export const enqueueArguments = (input: {
  readonly request: Captured;
  readonly images: ReadonlyArray<UploadReference>;
  readonly audio: ReadonlyArray<UploadReference>;
  readonly metadata: string;
}): CommandArgs<"enqueue"> => ({
  prompt: input.request.prompt,
  reference_images: input.images.map(wireUpload),
  ...(input.audio.length === 0 ? {} : { reference_audios: input.audio.map(wireUpload) }),
  metadata: input.metadata,
  ...(input.request.seconds === undefined ? {} : { seconds: input.request.seconds }),
  ...(input.request.seed === undefined ? {} : { seed: input.request.seed }),
  ...(input.request.position === undefined ? {} : { position: input.request.position }),
  ...(input.request.continueFrom === undefined
    ? {}
    : { continue_from_clip_id: input.request.continueFrom }),
});
