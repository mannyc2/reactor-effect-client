/**
 * What a caller asks of Vidu S2-Avatar, in Reactor's words, and its encoding
 * as each command's arguments. A field left out is left out on the wire: the
 * model drops a command carrying an explicit `null` without a word, so nothing
 * here encodes one.
 */
import * as Schema from "effect/Schema";

const Text = (most: number) => Schema.String.check(Schema.isMinLength(1), Schema.isMaxLength(most));
const Short = (most: number) => Schema.String.check(Schema.isMaxLength(most));

/** Turn-taking, `vad` on the wire: when the character decides the caller has finished. */
export const Vad = Schema.Struct({
  /** `server` filters back-channel sounds and noise; `semantic` lets the caller interrupt at once. */
  type: Schema.optionalKey(Schema.Literals(["server", "semantic"])),
  /** With `server`, how much noise is filtered, from 0 to 1; 0.5 by default. */
  threshold: Schema.optionalKey(Schema.Finite.check(Schema.isBetween({ minimum: 0, maximum: 1 }))),
  /** With `server`, the silence before the character answers; 400 ms by default. */
  silenceDurationMs: Schema.optionalKey(
    Schema.Int.check(Schema.isBetween({ minimum: 200, maximum: 6000 })),
  ),
}).pipe(Schema.encodeKeys({ silenceDurationMs: "silence_duration_ms" }));
export type Vad = typeof Vad.Type;

/** Reply generation, `llm` on the wire. */
export const Llm = Schema.Struct({
  /** The longest reply, in tokens; 50 by default. */
  maxTokens: Schema.optionalKey(Schema.Int.check(Schema.isGreaterThan(0))),
  temperature: Schema.optionalKey(
    Schema.Finite.check(Schema.isBetween({ minimum: 0, maximum: 2, exclusiveMaximum: true })),
  ),
  topP: Schema.optionalKey(
    Schema.Finite.check(Schema.isBetween({ minimum: 0, maximum: 1, exclusiveMinimum: true })),
  ),
  topK: Schema.optionalKey(Schema.Int.check(Schema.isBetween({ minimum: 0, maximum: 100 }))),
  frequencyPenalty: Schema.optionalKey(Schema.Finite.check(Schema.isGreaterThan(0))),
  presencePenalty: Schema.optionalKey(
    Schema.Finite.check(Schema.isBetween({ minimum: 0, maximum: 2 })),
  ),
  /** A fixed seed makes a demo more repeatable; -1 picks one at random. */
  seed: Schema.optionalKey(Schema.Int.check(Schema.isGreaterThanOrEqualTo(-1))),
}).pipe(
  Schema.encodeKeys({
    maxTokens: "max_tokens",
    topP: "top_p",
    topK: "top_k",
    frequencyPenalty: "frequency_penalty",
    presencePenalty: "presence_penalty",
  }),
);
export type Llm = typeof Llm.Type;

/** A call's settings: who the character is, how it sounds and how it takes turns. */
export const Call = Schema.Struct({
  /** Who the character is and how it behaves. */
  persona: Text(50_000),
  /** A `voice` from `listVoices`; the model's default when left out. */
  voice: Schema.optionalKey(Text(64)),
  /** What the character says or does first, before anyone speaks; it waits when left out. */
  greeting: Schema.optionalKey(Short(200)),
  /** The language the character speaks; it follows the conversation when left out. */
  language: Schema.optionalKey(Text(40)),
  /** `audio` forwards the `mic` track; `video` the `webcam` track too. Fixed for the call. */
  callMode: Schema.optionalKey(Schema.Literals(["audio", "video"])),
  /** Transcripts of both sides; on unless false. */
  transcripts: Schema.optionalKey(Schema.Boolean),
  /** Expands a short persona before the call starts. */
  personaEnhance: Schema.optionalKey(Schema.Boolean),
  vad: Schema.optionalKey(Vad),
  llm: Schema.optionalKey(Llm),
}).pipe(Schema.encodeKeys({ callMode: "call_mode", personaEnhance: "persona_enhance" }));
export type Call = typeof Call.Type;

/**
 * Changes to a live call, applied together or not at all: a voice or persona
 * after the current sentence, `vad` and `llm` on the next turn.
 */
export const CallUpdate = Schema.Struct({
  voice: Schema.optionalKey(Text(64)),
  persona: Schema.optionalKey(Text(50_000)),
  vad: Schema.optionalKey(Vad),
  llm: Schema.optionalKey(Llm),
}).check(
  Schema.makeFilter((update) => Object.keys(update).length > 0 || "names at least one change"),
);
export type CallUpdate = typeof CallUpdate.Type;

/** Something for the character to hold or wear, or a background, from a public image. */
export const ReferenceImage = Schema.Struct({
  imageUrl: Text(2_048),
  /** Your own id for it, unique in the call; an id already in effect is replaced. */
  imageId: Text(128),
  /** Inferred from the image when left out. */
  kind: Schema.optionalKey(Schema.Literals(["object", "garment", "background"])),
  /** One sentence describing the change. */
  text: Schema.optionalKey(Short(200)),
}).pipe(Schema.encodeKeys({ imageUrl: "image_url", imageId: "image_id" }));
export type ReferenceImage = typeof ReferenceImage.Type;

export const ReferenceImages = Schema.Array(ReferenceImage).check(
  Schema.isMinLength(1),
  Schema.isMaxLength(3),
  Schema.makeFilter(
    (images) =>
      new Set(images.map((image) => image.imageId)).size === images.length ||
      "gives each image its own id",
  ),
);

/** The image types the model takes. */
export const ImageType = Schema.Literals(["image/png", "image/jpeg", "image/webp", "image/heic"]);
export type ImageType = typeof ImageType.Type;

/**
 * The photo an avatar is made from: one person, full or half body, as bytes
 * the session uploads (under 20 MB, the session's upload limit permitting) or
 * as a public URL the model fetches.
 */
export const AvatarImage = Schema.Union([
  Schema.Struct({
    bytes: Schema.Uint8Array.check(
      Schema.makeFilter((bytes) => bytes.byteLength > 0 || "is empty"),
    ),
    type: ImageType,
    name: Schema.optionalKey(Short(100)),
  }),
  Schema.Struct({ url: Text(2_048), name: Schema.optionalKey(Short(100)) }),
]);
export type AvatarImage = typeof AvatarImage.Type;
