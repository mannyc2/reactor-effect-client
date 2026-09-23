import * as Effect from "effect/Effect";
import { ReactorError } from "../../errors.js";
import type { UploadReference } from "../../wire.generated.js";
import { imageMimeTypes, referenceLimits } from "../profile.js";
import type { Reference, ValidatedReference } from "../types.js";

interface ImageFacts {
  readonly mimeType: ValidatedReference["mimeType"];
  readonly width: number;
  readonly height: number;
}
export type Material =
  | { readonly _tag: "Bytes"; readonly bytes: Uint8Array<ArrayBuffer> }
  | { readonly _tag: "Uploaded"; readonly file: UploadReference };
const materials = new WeakMap<ValidatedReference, Material>();
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
export const checkedUpload = (input: unknown): UploadReference => {
  const file = plain(input, ["upload_id", "name", "mime_type", "size", "_unknown"]);
  if (
    typeof file.upload_id !== "string" ||
    !/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(file.upload_id) ||
    typeof file.name !== "string" ||
    file.name.length === 0 ||
    file.name.length > 1024 ||
    !(imageMimeTypes as readonly string[]).includes(String(file.mime_type)) ||
    typeof file.size !== "bigint" ||
    file.size <= 0n ||
    file.size > BigInt(referenceLimits.maxBytes)
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
  if (input !== null && typeof input === "object" && materials.has(input as ValidatedReference))
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
  } else return invalid("Reference must be Bytes or Uploaded; reference audio is unsupported");
  const validated = Object.freeze(facts) as ValidatedReference;
  materials.set(validated, material);
  return validated;
};

export const referenceMaterial = (reference: ValidatedReference): Material =>
  materials.get(reference) ?? invalid("Unknown validated reference");
export const validateReference = (
  input: Reference,
): Effect.Effect<ValidatedReference, ReactorError> =>
  Effect.try({
    try: () => captureReference(input),
    catch: (error) =>
      ReactorError.is(error)
        ? error
        : ReactorError.fromCode("InvalidInput", "Invalid H3 reference", {
            outcome: "not-submitted",
          }),
  });
