import { ReactorError } from "../../errors.js";
import { nonempty, record } from "../../json.js";
import type { UploadReference } from "../../wire.generated.js";

const maximumWireSize = (1n << 63n) - 1n;

const invalid = (message: string): never => {
  throw ReactorError.fromCode("InvalidInput", message, { outcome: "not-submitted" });
};

/**
 * Detach caller-owned references before correlation or dispatch can begin.
 * Each rejection is an InvalidInput ReactorError that was never submitted.
 */
export const captureUploads = (
  input: ReadonlyMap<string, UploadReference>,
): Map<string, UploadReference> => {
  if (!(input instanceof Map)) return invalid("upload references must be a Map");
  const result = new Map<string, UploadReference>();
  for (const [key, value] of input as ReadonlyMap<unknown, unknown>) {
    if (result.size >= 128) return invalid("too many upload references");
    const reference = record(value, "upload reference");
    const size = reference.size;
    if (typeof size !== "bigint" || size < 0n || size > maximumWireSize)
      return invalid("upload size must be a nonnegative signed-64-bit bigint");
    result.set(
      nonempty(key, "upload key"),
      Object.freeze({
        upload_id: nonempty(reference.upload_id, "upload id"),
        name: nonempty(reference.name, "upload name"),
        mime_type: nonempty(reference.mime_type, "upload MIME type"),
        size,
      }),
    );
  }
  return result;
};
