import * as Effect from "effect/Effect";
import { parsedInput, ReactorError } from "../../errors.js";
import type { UploadReference } from "../../wire.generated.js";
import {
  audioMimeTypes,
  audioReferenceLimits,
  imageMimeTypes,
  referenceLimits,
} from "../profile.js";
import type { Reference, ValidatedAudioReference, ValidatedReference } from "../types.js";

interface ImageFacts {
  readonly mimeType: ValidatedReference["mimeType"];
  readonly width: number;
  readonly height: number;
}
export type Material =
  | { readonly _tag: "Bytes"; readonly bytes: Uint8Array<ArrayBuffer> }
  | { readonly _tag: "Uploaded"; readonly file: UploadReference };
const materials = new WeakMap<ValidatedReference | ValidatedAudioReference, Material>();
const invalid = (message: string): never => {
  throw ReactorError.fromCode("InvalidInput", message, {
    operation: "H3 reference",
    outcome: "not-submitted",
  });
};
const uint16 = (b: Uint8Array, o: number) => b[o]! * 256 + b[o + 1]!;
const uint32 = (b: Uint8Array, o: number) =>
  b[o]! * 0x1000000 + b[o + 1]! * 65536 + b[o + 2]! * 256 + b[o + 3]!;
const le16 = (b: Uint8Array, o: number) => b[o]! + b[o + 1]! * 256;
const le24 = (b: Uint8Array, o: number) => le16(b, o) + b[o + 2]! * 65536;
const le32 = (b: Uint8Array, o: number) => le24(b, o) + b[o + 3]! * 0x1000000;
const ascii = (b: Uint8Array, o: number, count: number) =>
  String.fromCharCode(...b.subarray(o, o + count));

/** The only image-header validator used by the provider and host reference loaders. */
const inspectImage = (bytes: Uint8Array): ImageFacts => {
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
      return invalid("PNG has no complete image header");
    let offset = 8,
      hasData = false,
      ended = false;
    while (offset + 12 <= bytes.length) {
      const size = uint32(bytes, offset),
        type = ascii(bytes, offset + 4, 4);
      if (size > bytes.length - offset - 12) return invalid("PNG chunk is truncated");
      if (type === "IDAT" && size > 0) hasData = true;
      offset += size + 12;
      if (type === "IEND") {
        ended = size === 0 && offset === bytes.length;
        break;
      }
    }
    if (!hasData || !ended) return invalid("PNG is incomplete");
    return { mimeType: "image/png", width: uint32(bytes, 16), height: uint32(bytes, 20) };
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
      const marker = bytes[offset + 1]!;
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
        return {
          mimeType: "image/jpeg",
          height: uint16(bytes, offset + 5),
          width: uint16(bytes, offset + 7),
        };
      }
      if (marker === 218 || marker === 217) break;
      offset += 2 + size;
    }
    return invalid("JPEG has no complete dimension header");
  }
  if (bytes.length >= 30 && ascii(bytes, 0, 4) === "RIFF" && ascii(bytes, 8, 4) === "WEBP") {
    if (le32(bytes, 4) + 8 !== bytes.length || le32(bytes, 16) > bytes.length - 20)
      return invalid("WebP is truncated");
    const type = ascii(bytes, 12, 4);
    if (type === "VP8 " && bytes[23] === 157 && bytes[24] === 1 && bytes[25] === 42)
      return {
        mimeType: "image/webp",
        width: le16(bytes, 26) & 16383,
        height: le16(bytes, 28) & 16383,
      };
    if (type === "VP8L" && bytes[20] === 47) {
      const bits = le32(bytes, 21);
      return {
        mimeType: "image/webp",
        width: (bits & 16383) + 1,
        height: ((bits >>> 14) & 16383) + 1,
      };
    }
    if (type === "VP8X" && le32(bytes, 16) >= 10)
      return { mimeType: "image/webp", width: le24(bytes, 24) + 1, height: le24(bytes, 27) + 1 };
  }
  return invalid("Reference must contain a supported PNG, JPEG, or WebP image");
};

interface AudioFacts {
  readonly mimeType: ValidatedAudioReference["mimeType"];
  readonly seconds: number | null;
  readonly channels: number | null;
}

/** A WAV's length and channels, from its `fmt ` chunk and the data it holds. */
const inspectWav = (bytes: Uint8Array): AudioFacts => {
  if (le32(bytes, 4) + 8 > bytes.length) return invalid("WAV is truncated");
  let offset = 12,
    channels: number | undefined,
    byteRate: number | undefined,
    data: number | undefined;
  while (offset + 8 <= bytes.length) {
    const id = ascii(bytes, offset, 4),
      size = le32(bytes, offset + 4),
      body = offset + 8;
    if (id === "fmt ") {
      if (size < 16 || body + 16 > bytes.length) return invalid("WAV format chunk is truncated");
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
    return invalid("WAV has no format or data chunk");
  if (channels === 0 || byteRate === 0) return invalid("WAV format is invalid");
  return { mimeType: "audio/wav", seconds: data / byteRate, channels };
};

/** A FLAC's length and channels, from its STREAMINFO block; an unknown length stays null. */
const inspectFlac = (bytes: Uint8Array): AudioFacts => {
  const info = 8;
  if (bytes.length < info + 34 || (bytes[4]! & 127) !== 0)
    return invalid("FLAC has no stream info");
  if (uint16(bytes, 5) * 256 + bytes[7]! < 34) return invalid("FLAC stream info is truncated");
  const rate = bytes[info + 10]! * 4096 + bytes[info + 11]! * 16 + (bytes[info + 12]! >> 4);
  const channels = ((bytes[info + 12]! >> 1) & 7) + 1;
  const samples = (bytes[info + 13]! & 15) * 0x100000000 + uint32(bytes, info + 14);
  if (rate === 0) return invalid("FLAC sample rate is invalid");
  return { mimeType: "audio/flac", seconds: samples === 0 ? null : samples / rate, channels };
};

/** An Ogg stream's channels from its first page's Opus or Vorbis header; its length stays null. */
const inspectOgg = (bytes: Uint8Array): AudioFacts => {
  const payload = bytes.length > 26 ? 27 + bytes[26]! : bytes.length;
  const channels =
    ascii(bytes, payload, 8) === "OpusHead"
      ? bytes[payload + 9]
      : bytes[payload] === 1 && ascii(bytes, payload + 1, 6) === "vorbis"
        ? bytes[payload + 11]
        : undefined;
  return { mimeType: "audio/ogg", seconds: null, channels: channels ?? null };
};

/**
 * The only audio-container identification used by the provider and its loaders.
 * WAV and FLAC headers give a length and channel count that are checked here;
 * the other formats are recognized by their container and checked by H3.
 */
const inspectAudio = (bytes: Uint8Array): AudioFacts => {
  if (bytes.length >= 12 && ascii(bytes, 0, 4) === "RIFF" && ascii(bytes, 8, 4) === "WAVE")
    return inspectWav(bytes);
  if (bytes.length >= 4 && ascii(bytes, 0, 4) === "fLaC") return inspectFlac(bytes);
  if (bytes.length >= 4 && ascii(bytes, 0, 4) === "OggS") return inspectOgg(bytes);
  if (bytes.length >= 12 && ascii(bytes, 4, 4) === "ftyp")
    return { mimeType: "audio/mp4", seconds: null, channels: null };
  if (bytes.length >= 4 && uint32(bytes, 0) === 0x1a45dfa3)
    return { mimeType: "audio/webm", seconds: null, channels: null };
  if (bytes.length >= 3 && ascii(bytes, 0, 3) === "ID3")
    return { mimeType: "audio/mpeg", seconds: null, channels: null };
  if (bytes.length >= 2 && bytes[0] === 255 && (bytes[1]! & 246) === 240)
    return { mimeType: "audio/aac", seconds: null, channels: null };
  if (bytes.length >= 2 && bytes[0] === 255 && (bytes[1]! & 224) === 224 && (bytes[1]! & 6) !== 0)
    return { mimeType: "audio/mpeg", seconds: null, channels: null };
  return invalid("Audio reference must be WAV, MP3, AAC/M4A, OGG/Opus, FLAC or WebM");
};

export const plain = (input: unknown, allowed: readonly string[]): Record<string, unknown> => {
  if (
    input === null ||
    typeof input !== "object" ||
    Array.isArray(input) ||
    (Object.getPrototypeOf(input) !== Object.prototype && Object.getPrototypeOf(input) !== null) ||
    Object.getOwnPropertySymbols(input).length !== 0
  )
    return invalid("Expected a plain input object");
  const output: Record<string, unknown> = {};
  for (const [key, descriptor] of Object.entries(Object.getOwnPropertyDescriptors(input))) {
    if (!allowed.includes(key) || !("value" in descriptor))
      return invalid("Input contains unsupported fields or accessors");
    Object.defineProperty(output, key, { value: descriptor.value, enumerable: true });
  }
  return output;
};

/** Validates wire identity and a safe JSON size projection without inventing upload provenance. */
export const checkedUpload = (
  input: unknown,
  kind: "image" | "audio" = "image",
): UploadReference => {
  const [mimeTypes, maxBytes]: readonly [readonly string[], number] =
    kind === "image"
      ? [imageMimeTypes, referenceLimits.maxBytes]
      : [audioMimeTypes, audioReferenceLimits.maxBytes];
  const file = plain(input, ["upload_id", "name", "mime_type", "size", "_unknown"]);
  if (
    typeof file.upload_id !== "string" ||
    !/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(file.upload_id) ||
    typeof file.name !== "string" ||
    file.name.length === 0 ||
    file.name.length > 1024 ||
    !mimeTypes.includes(String(file.mime_type)) ||
    typeof file.size !== "bigint" ||
    file.size <= 0n ||
    file.size > BigInt(maxBytes)
  ) {
    return invalid("Uploaded reference has invalid identity, type, or size");
  }
  return Object.freeze({
    upload_id: file.upload_id,
    name: file.name,
    mime_type: file.mime_type as string,
    size: file.size,
  });
};

export const captureReference = (input: Reference | ValidatedReference): ValidatedReference => {
  if (
    input !== null &&
    typeof input === "object" &&
    materials.has(input as ValidatedReference) &&
    (input as ValidatedReference)._tag === "ValidatedReference"
  )
    return input as ValidatedReference;
  const object = plain(input, ["_tag", "bytes", "file"]);
  let material: Material, facts: ValidatedReference;
  if (object._tag === "Bytes" && object.bytes instanceof Uint8Array && object.file === undefined) {
    if (object.bytes.byteLength === 0 || object.bytes.byteLength > referenceLimits.maxBytes)
      return invalid("Image exceeds the SDK byte bound");
    const bytes = new Uint8Array(object.bytes),
      dimensions = inspectImage(bytes);
    const { width, height } = dimensions,
      aspect = width / height;
    if (
      width <= 0 ||
      height <= 0 ||
      width * height > referenceLimits.maxPixels ||
      aspect < referenceLimits.minAspect ||
      aspect > referenceLimits.maxAspect
    )
      return invalid("Image dimensions exceed the SDK bounds");
    material = { _tag: "Bytes", bytes };
    facts = { _tag: "ValidatedReference", ...dimensions, size: bytes.length } as ValidatedReference;
  } else if (object._tag === "Uploaded" && object.bytes === undefined) {
    const file = checkedUpload(object.file);
    material = { _tag: "Uploaded", file };
    facts = {
      _tag: "ValidatedReference",
      mimeType: file.mime_type as ValidatedReference["mimeType"],
      size: Number(file.size),
      width: null,
      height: null,
    } as ValidatedReference;
  } else return invalid("Reference must be Bytes or Uploaded");
  const validated = Object.freeze(facts) as ValidatedReference;
  materials.set(validated, material);
  return validated;
};

export const captureAudioReference = (
  input: Reference | ValidatedAudioReference,
): ValidatedAudioReference => {
  if (
    input !== null &&
    typeof input === "object" &&
    materials.has(input as ValidatedAudioReference) &&
    (input as ValidatedAudioReference)._tag === "ValidatedAudioReference"
  )
    return input as ValidatedAudioReference;
  const object = plain(input, ["_tag", "bytes", "file"]);
  let material: Material, facts: ValidatedAudioReference;
  if (object._tag === "Bytes" && object.bytes instanceof Uint8Array && object.file === undefined) {
    if (object.bytes.byteLength === 0 || object.bytes.byteLength > audioReferenceLimits.maxBytes)
      return invalid("Audio exceeds the 25 MiB bound");
    const bytes = new Uint8Array(object.bytes),
      audio = inspectAudio(bytes);
    if (
      audio.channels !== null &&
      (audio.channels < 1 || audio.channels > audioReferenceLimits.maxChannels)
    )
      return invalid("Audio must be mono or stereo");
    if (
      audio.seconds !== null &&
      (audio.seconds < audioReferenceLimits.minSeconds ||
        audio.seconds > audioReferenceLimits.maxSeconds)
    )
      return invalid("Audio must be 2 to 15 seconds long");
    material = { _tag: "Bytes", bytes };
    facts = {
      _tag: "ValidatedAudioReference",
      ...audio,
      size: bytes.length,
    } as ValidatedAudioReference;
  } else if (object._tag === "Uploaded" && object.bytes === undefined) {
    const file = checkedUpload(object.file, "audio");
    material = { _tag: "Uploaded", file };
    facts = {
      _tag: "ValidatedAudioReference",
      mimeType: file.mime_type as ValidatedAudioReference["mimeType"],
      size: Number(file.size),
      seconds: null,
      channels: null,
    } as ValidatedAudioReference;
  } else return invalid("Audio reference must be Bytes or Uploaded");
  const validated = Object.freeze(facts) as ValidatedAudioReference;
  materials.set(validated, material);
  return validated;
};

export const referenceMaterial = (
  reference: ValidatedReference | ValidatedAudioReference,
): Material => materials.get(reference) ?? invalid("Unknown validated reference");
export const validateReference = (
  input: Reference,
): Effect.Effect<ValidatedReference, ReactorError> =>
  parsedInput(() => captureReference(input), "H3 reference");
/**
 * Validate an audio reference once: its container, and for WAV and FLAC its
 * length and channels, so a request can reuse it without checking it again.
 */
export const validateAudioReference = (
  input: Reference,
): Effect.Effect<ValidatedAudioReference, ReactorError> =>
  parsedInput(() => captureAudioReference(input), "H3 reference");
