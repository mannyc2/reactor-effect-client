import { ReactorError } from "../../errors.js";
import { nonempty } from "../../json.js";
import type { UploadReference } from "../../wire.generated.js";

const maximumWireSize = (1n << 63n) - 1n;

/** Detach caller-owned references before correlation or dispatch can begin. */
export const captureUploads = (
  input: ReadonlyMap<string, UploadReference>,
): Map<string, UploadReference> => {
  try {
    const result = new Map<string, UploadReference>();
    for (const [key, value] of input) {
      if (result.size >= 128) throw new TypeError("too many upload references");
      nonempty(key, "upload key");
      if (value === null || typeof value !== "object")
        throw new TypeError("upload reference must be an object");
      if (typeof value.size !== "bigint" || value.size < 0n || value.size > maximumWireSize) {
        throw new TypeError("upload size must be a nonnegative signed-64-bit bigint");
      }
      result.set(
        key,
        Object.freeze({
          upload_id: nonempty(value.upload_id, "upload id"),
          name: nonempty(value.name, "upload name"),
          mime_type: nonempty(value.mime_type, "upload MIME type"),
          size: value.size,
        }),
      );
    }
    return result;
  } catch (cause) {
    throw ReactorError.fromCode("InvalidInput", "invalid command upload reference", {
      outcome: "not-submitted",
      detail: cause,
    });
  }
};
