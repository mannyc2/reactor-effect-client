/**
 * Reference validation: an image's or audio clip's container and bounds are
 * read from its own header, once, before anything is uploaded or sent.
 */
import * as Effect from "effect/Effect";
import * as Result from "effect/Result";
import * as Schema from "effect/Schema";
import { ReactorError } from "../../ReactorError.js";
import type { UploadReference } from "../../Session.js";
import {
  audioMimeTypes,
  audioReferenceLimits,
  imageMimeTypes,
  referenceLimits,
} from "./profile.js";

export type ImageMimeType = (typeof imageMimeTypes)[number];
export type AudioMimeType = (typeof audioMimeTypes)[number];

interface ImageFacts {
  readonly mimeType: ImageMimeType;
  readonly width: number;
  readonly height: number;
}
interface AudioFacts {
  readonly mimeType: AudioMimeType;
  readonly seconds: number | null;
  readonly channels: number | null;
}

const uint16 = (b: Uint8Array, o: number) => (b[o] ?? 0) * 256 + (b[o + 1] ?? 0);
const uint32 = (b: Uint8Array, o: number) =>
  (b[o] ?? 0) * 0x1000000 + (b[o + 1] ?? 0) * 65536 + (b[o + 2] ?? 0) * 256 + (b[o + 3] ?? 0);
const le16 = (b: Uint8Array, o: number) => (b[o] ?? 0) + (b[o + 1] ?? 0) * 256;
const le24 = (b: Uint8Array, o: number) => le16(b, o) + (b[o + 2] ?? 0) * 65536;
const le32 = (b: Uint8Array, o: number) => le24(b, o) + (b[o + 3] ?? 0) * 0x1000000;
const ascii = (b: Uint8Array, o: number, count: number) =>
  String.fromCharCode(...b.subarray(o, o + count));

/** The only image-header validator used by the provider and host reference loaders. */
const inspectImage = (bytes: Uint8Array): Result.Result<ImageFacts, string> => {
  if (
    bytes.length >= 33 &&
    ascii(bytes, 1, 3) === "PNG" &&
    bytes[0] === 137 &&
    bytes[4] === 13 &&
    bytes[5] === 10 &&
    bytes[6] === 26 &&
    bytes[7] === 10
  ) {
    if (uint32(bytes, 8) !== 13 || ascii(bytes, 12, 4) !== "IHDR")
      return Result.fail("PNG has no complete image header");
    let offset = 8,
      hasData = false,
      ended = false;
    while (offset + 12 <= bytes.length) {
      const size = uint32(bytes, offset),
        type = ascii(bytes, offset + 4, 4);
      if (size > bytes.length - offset - 12) return Result.fail("PNG chunk is truncated");
      if (type === "IDAT" && size > 0) hasData = true;
      offset += size + 12;
      if (type === "IEND") {
        ended = size === 0 && offset === bytes.length;
        break;
      }
    }
    if (!hasData || !ended) return Result.fail("PNG is incomplete");
    return Result.succeed({
      mimeType: "image/png",
      width: uint32(bytes, 16),
      height: uint32(bytes, 20),
    });
  }
  if (
    bytes.length >= 4 &&
    bytes[0] === 255 &&
    bytes[1] === 216 &&
    bytes.at(-2) === 255 &&
    bytes.at(-1) === 217
  ) {
    let offset = 2;
    while (offset + 4 <= bytes.length) {
      if (bytes[offset] !== 255) break;
      const marker = bytes[offset + 1] ?? 0;
      if (marker === 255) {
        offset++;
        continue;
      }
      if (marker === 216 || marker === 1 || (marker >= 208 && marker <= 215)) {
        offset += 2;
        continue;
      }
      const size = uint16(bytes, offset + 2);
      if (size < 2 || offset + 2 + size > bytes.length) break;
      if (
        marker >= 192 &&
        marker <= 207 &&
        marker !== 196 &&
        marker !== 200 &&
        marker !== 204 &&
        size >= 8
      ) {
        return Result.succeed({
          mimeType: "image/jpeg",
          height: uint16(bytes, offset + 5),
          width: uint16(bytes, offset + 7),
        });
      }
      if (marker === 218 || marker === 217) break;
      offset += 2 + size;
    }
    return Result.fail("JPEG has no complete dimension header");
  }
  if (bytes.length >= 30 && ascii(bytes, 0, 4) === "RIFF" && ascii(bytes, 8, 4) === "WEBP") {
    if (le32(bytes, 4) + 8 !== bytes.length || le32(bytes, 16) > bytes.length - 20)
      return Result.fail("WebP is truncated");
    const type = ascii(bytes, 12, 4);
    if (type === "VP8 " && bytes[23] === 157 && bytes[24] === 1 && bytes[25] === 42)
      return Result.succeed({
        mimeType: "image/webp",
        width: le16(bytes, 26) & 16383,
        height: le16(bytes, 28) & 16383,
      });
    if (type === "VP8L" && bytes[20] === 47) {
      const bits = le32(bytes, 21);
      return Result.succeed({
        mimeType: "image/webp",
        width: (bits & 16383) + 1,
        height: ((bits >>> 14) & 16383) + 1,
      });
    }
    if (type === "VP8X" && le32(bytes, 16) >= 10)
      return Result.succeed({
        mimeType: "image/webp",
        width: le24(bytes, 24) + 1,
        height: le24(bytes, 27) + 1,
      });
  }
  return Result.fail("Reference must contain a supported PNG, JPEG, or WebP image");
};

/** A WAV's length and channels, from its `fmt ` chunk and the data it holds. */
const inspectWav = (bytes: Uint8Array): Result.Result<AudioFacts, string> => {
  if (le32(bytes, 4) + 8 > bytes.length) return Result.fail("WAV is truncated");
  let offset = 12,
    channels: number | undefined,
    byteRate: number | undefined,
    data: number | undefined;
  while (offset + 8 <= bytes.length) {
    const id = ascii(bytes, offset, 4),
      size = le32(bytes, offset + 4),
      body = offset + 8;
    if (id === "fmt ") {
      if (size < 16 || body + 16 > bytes.length)
        return Result.fail("WAV format chunk is truncated");
      channels = le16(bytes, body + 2);
      byteRate = le32(bytes, body + 8);
    } else if (id === "data") {
      // A streaming writer may leave the size at its maximum: count what is there.
      data = Math.min(size, bytes.length - body);
      break;
    }
    offset = body + size + (size % 2);
  }
  if (channels === undefined || byteRate === undefined || data === undefined)
    return Result.fail("WAV has no format or data chunk");
  if (channels === 0 || byteRate === 0) return Result.fail("WAV format is invalid");
  return Result.succeed({ mimeType: "audio/wav", seconds: data / byteRate, channels });
};

/** A FLAC's length and channels, from its STREAMINFO block; an unknown length stays null. */
const inspectFlac = (bytes: Uint8Array): Result.Result<AudioFacts, string> => {
  const info = 8;
  if (bytes.length < info + 34 || ((bytes[4] ?? 0) & 127) !== 0)
    return Result.fail("FLAC has no stream info");
  if (uint16(bytes, 5) * 256 + (bytes[7] ?? 0) < 34)
    return Result.fail("FLAC stream info is truncated");
  const rate =
    (bytes[info + 10] ?? 0) * 4096 + (bytes[info + 11] ?? 0) * 16 + ((bytes[info + 12] ?? 0) >> 4);
  const channels = (((bytes[info + 12] ?? 0) >> 1) & 7) + 1;
  const samples = ((bytes[info + 13] ?? 0) & 15) * 0x100000000 + uint32(bytes, info + 14);
  if (rate === 0) return Result.fail("FLAC sample rate is invalid");
  return Result.succeed({
    mimeType: "audio/flac",
    seconds: samples === 0 ? null : samples / rate,
    channels,
  });
};

/** An Ogg stream's channels from its first page's Opus or Vorbis header; its length stays null. */
const inspectOgg = (bytes: Uint8Array): Result.Result<AudioFacts, string> => {
  const payload = bytes.length > 26 ? 27 + (bytes[26] ?? 0) : bytes.length;
  const channels =
    ascii(bytes, payload, 8) === "OpusHead"
      ? bytes[payload + 9]
      : bytes[payload] === 1 && ascii(bytes, payload + 1, 6) === "vorbis"
        ? bytes[payload + 11]
        : undefined;
  return Result.succeed({ mimeType: "audio/ogg", seconds: null, channels: channels ?? null });
};

/**
 * The only audio-container identification used by the provider and its loaders.
 * WAV and FLAC headers give a length and channel count that are checked here;
 * the other formats are recognized by their container and checked by H3.
 */
const inspectAudio = (bytes: Uint8Array): Result.Result<AudioFacts, string> => {
  if (bytes.length >= 12 && ascii(bytes, 0, 4) === "RIFF" && ascii(bytes, 8, 4) === "WAVE")
    return inspectWav(bytes);
  if (bytes.length >= 4 && ascii(bytes, 0, 4) === "fLaC") return inspectFlac(bytes);
  if (bytes.length >= 4 && ascii(bytes, 0, 4) === "OggS") return inspectOgg(bytes);
  if (bytes.length >= 12 && ascii(bytes, 4, 4) === "ftyp")
    return Result.succeed({ mimeType: "audio/mp4", seconds: null, channels: null });
  if (bytes.length >= 4 && uint32(bytes, 0) === 0x1a45dfa3)
    return Result.succeed({ mimeType: "audio/webm", seconds: null, channels: null });
  if (bytes.length >= 3 && ascii(bytes, 0, 3) === "ID3")
    return Result.succeed({ mimeType: "audio/mpeg", seconds: null, channels: null });
  if (bytes.length >= 2 && bytes[0] === 255 && ((bytes[1] ?? 0) & 246) === 240)
    return Result.succeed({ mimeType: "audio/aac", seconds: null, channels: null });
  if (
    bytes.length >= 2 &&
    bytes[0] === 255 &&
    ((bytes[1] ?? 0) & 224) === 224 &&
    ((bytes[1] ?? 0) & 6) !== 0
  )
    return Result.succeed({ mimeType: "audio/mpeg", seconds: null, channels: null });
  return Result.fail("Audio reference must be WAV, MP3, AAC/M4A, OGG/Opus, FLAC or WebM");
};

/** Bytes held for upload, or a file the session already holds. */
export type Material =
  | { readonly _tag: "Bytes"; readonly bytes: Uint8Array<ArrayBuffer> }
  | { readonly _tag: "Uploaded"; readonly file: UploadReference };

/** An image reference whose header was read; its bytes are a private copy. */
export interface ValidatedReference {
  readonly _tag: "ValidatedReference";
  readonly mimeType: ImageMimeType;
  readonly size: number;
  readonly width: number | null;
  readonly height: number | null;
}

/**
 * An audio reference whose container was read. `seconds` and `channels` come
 * from a WAV or FLAC header; for the other formats they are null, and H3
 * checks them when it receives the clip.
 */
export interface ValidatedAudioReference {
  readonly _tag: "ValidatedAudioReference";
  readonly mimeType: AudioMimeType;
  readonly size: number;
  readonly seconds: number | null;
  readonly channels: number | null;
}

/** Only this module constructs one, from bytes it validated and copied. */
class ValidatedImage implements ValidatedReference {
  readonly _tag = "ValidatedReference" as const;
  readonly #material: Material;
  constructor(
    readonly mimeType: ImageMimeType,
    readonly size: number,
    readonly width: number | null,
    readonly height: number | null,
    material: Material,
  ) {
    this.#material = material;
  }
  static material(self: ValidatedImage): Material {
    return self.#material;
  }
}

class ValidatedAudio implements ValidatedAudioReference {
  readonly _tag = "ValidatedAudioReference" as const;
  readonly #material: Material;
  constructor(
    readonly mimeType: AudioMimeType,
    readonly size: number,
    readonly seconds: number | null,
    readonly channels: number | null,
    material: Material,
  ) {
    this.#material = material;
  }
  static material(self: ValidatedAudio): Material {
    return self.#material;
  }
}

/** Schemas that admit only references this module validated, never a lookalike object. */
export const ValidatedReferenceSchema = Schema.declare(
  (input: unknown): input is ValidatedReference => input instanceof ValidatedImage,
  { expected: "a reference returned by H3.validateReference" },
);
export const ValidatedAudioReferenceSchema = Schema.declare(
  (input: unknown): input is ValidatedAudioReference => input instanceof ValidatedAudio,
  { expected: "an audio reference returned by H3.validateAudioReference" },
);

/** The bytes or file behind a validated reference; a lookalike object has none. */
export const materialOf = (
  reference: ValidatedReference | ValidatedAudioReference,
): Material | undefined =>
  reference instanceof ValidatedImage
    ? ValidatedImage.material(reference)
    : reference instanceof ValidatedAudio
      ? ValidatedAudio.material(reference)
      : undefined;

const uploadedFile = <const M extends string>(mimeTypes: ReadonlyArray<M>, maxBytes: number) =>
  Schema.Struct({
    uploadId: Schema.String.check(Schema.isUUID()),
    name: Schema.String.check(Schema.isNonEmpty(), Schema.isMaxLength(1024)),
    mimeType: Schema.Literals(mimeTypes),
    size: Schema.BigInt.check(Schema.isBetweenBigInt({ minimum: 1n, maximum: BigInt(maxBytes) })),
  });

/** A file the session already uploaded, as a request names it. */
export const UploadedImage = uploadedFile(imageMimeTypes, referenceLimits.maxBytes);
export const UploadedAudio = uploadedFile(audioMimeTypes, audioReferenceLimits.maxBytes);

/** A reference as a request supplies it: bytes to validate and upload, or an uploaded file. */
export const Reference = Schema.Union([
  Schema.TaggedStruct("Bytes", { bytes: Schema.Uint8Array }),
  Schema.TaggedStruct("Uploaded", {
    file: Schema.Struct({
      uploadId: Schema.String,
      name: Schema.String,
      mimeType: Schema.String,
      size: Schema.BigInt,
    }),
  }),
]);
export type Reference = typeof Reference.Type;

const invalid = (message: string): ReactorError =>
  ReactorError.fromCode("InvalidInput", message, {
    operation: "H3 reference",
    outcome: "not-submitted",
  });

const image = (reference: Reference): Result.Result<ValidatedImage, string> => {
  if (reference._tag === "Uploaded")
    return Result.mapBoth(Schema.decodeUnknownResult(UploadedImage)(reference.file), {
      onFailure: () => "Uploaded reference has an invalid identity, type or size",
      onSuccess: (file) =>
        new ValidatedImage(file.mimeType, Number(file.size), null, null, {
          _tag: "Uploaded",
          file,
        }),
    });
  if (reference.bytes.byteLength === 0 || reference.bytes.byteLength > referenceLimits.maxBytes)
    return Result.fail("Image exceeds the SDK byte bound");
  const bytes = new Uint8Array(reference.bytes);
  return Result.flatMap(inspectImage(bytes), (facts) => {
    const aspect = facts.width / facts.height;
    return facts.width <= 0 ||
      facts.height <= 0 ||
      facts.width * facts.height > referenceLimits.maxPixels ||
      aspect < referenceLimits.minAspect ||
      aspect > referenceLimits.maxAspect
      ? Result.fail("Image dimensions exceed the SDK bounds")
      : Result.succeed(
          new ValidatedImage(facts.mimeType, bytes.length, facts.width, facts.height, {
            _tag: "Bytes",
            bytes,
          }),
        );
  });
};

const audio = (reference: Reference): Result.Result<ValidatedAudio, string> => {
  if (reference._tag === "Uploaded")
    return Result.mapBoth(Schema.decodeUnknownResult(UploadedAudio)(reference.file), {
      onFailure: () => "Uploaded audio has an invalid identity, type or size",
      onSuccess: (file) =>
        new ValidatedAudio(file.mimeType, Number(file.size), null, null, {
          _tag: "Uploaded",
          file,
        }),
    });
  if (
    reference.bytes.byteLength === 0 ||
    reference.bytes.byteLength > audioReferenceLimits.maxBytes
  )
    return Result.fail("Audio exceeds the 25 MiB bound");
  const bytes = new Uint8Array(reference.bytes);
  return Result.flatMap(inspectAudio(bytes), (facts) =>
    facts.channels !== null &&
    (facts.channels < 1 || facts.channels > audioReferenceLimits.maxChannels)
      ? Result.fail("Audio must be mono or stereo")
      : facts.seconds !== null &&
          (facts.seconds < audioReferenceLimits.minSeconds ||
            facts.seconds > audioReferenceLimits.maxSeconds)
        ? Result.fail("Audio must be 2 to 15 seconds long")
        : Result.succeed(
            new ValidatedAudio(facts.mimeType, bytes.length, facts.seconds, facts.channels, {
              _tag: "Bytes",
              bytes,
            }),
          ),
  );
};

/** Validates an image reference once, so a request reuses it without checking it again. */
export const validateReference = (
  reference: Reference | ValidatedReference,
): Result.Result<ValidatedReference, ReactorError> =>
  reference instanceof ValidatedImage
    ? Result.succeed(reference)
    : reference._tag === "ValidatedReference"
      ? Result.fail(invalid("Reference was not returned by H3.validateReference"))
      : Result.mapError(image(reference), invalid);

/**
 * Validates an audio reference once: its container, and for WAV and FLAC its
 * length and channels.
 */
export const validateAudioReference = (
  reference: Reference | ValidatedAudioReference,
): Result.Result<ValidatedAudioReference, ReactorError> =>
  reference instanceof ValidatedAudio
    ? Result.succeed(reference)
    : reference._tag === "ValidatedAudioReference"
      ? Result.fail(invalid("Audio reference was not returned by H3.validateAudioReference"))
      : Result.mapError(audio(reference), invalid);

export const validateReferenceEffect = (
  reference: Reference | ValidatedReference,
): Effect.Effect<ValidatedReference, ReactorError> =>
  Effect.fromResult(validateReference(reference));
export const validateAudioReferenceEffect = (
  reference: Reference | ValidatedAudioReference,
): Effect.Effect<ValidatedAudioReference, ReactorError> =>
  Effect.fromResult(validateAudioReference(reference));
