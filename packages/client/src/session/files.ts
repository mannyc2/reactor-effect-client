import type * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import { collectBytes } from "../bytes.js";
import { duration } from "../duration.js";
import { parsed, ReactorError } from "../errors.js";
import type { UploadTimeoutOptions } from "../SessionTypes.js";
import type { Session, Uploaded } from "./index.js";

export interface FileUploadOptions extends UploadTimeoutOptions {
  readonly mimeType?: string;
  readonly maxBytes?: number;
  /**
   * How long reading the file may take; 30 seconds by default. A bare number
   * is milliseconds.
   */
  readonly readTimeout?: Duration.Input | undefined;
}

/** Scoped, bounded host file loading shared by uploads and reference preparation. */
export const readFileBytes = (
  file: string,
  maxBytes: number,
): Effect.Effect<Uint8Array, ReactorError, FileSystem.FileSystem> =>
  Effect.gen(function* () {
    if (
      typeof file !== "string" ||
      file.length === 0 ||
      !Number.isSafeInteger(maxBytes) ||
      maxBytes <= 0
    ) {
      return yield* ReactorError.fromCode(
        "InvalidInput",
        "A file and positive byte bound are required",
        { outcome: "not-submitted" },
      );
    }
    const fs = yield* FileSystem.FileSystem;
    return yield* collectBytes(fs.stream(file, { bytesToRead: maxBytes + 1 }), maxBytes).pipe(
      Effect.mapError((cause) =>
        ReactorError.is(cause)
          ? cause
          : ReactorError.fromCode("Upload", "Source file could not be read", {
              outcome: "not-submitted",
              detail: cause,
            }),
      ),
    );
  });

/** File IO is a host convenience; the result retains canonical upload evidence. */
export const uploadFile = (
  session: Session,
  file: string,
  options: FileUploadOptions = {},
): Effect.Effect<Uploaded, ReactorError, FileSystem.FileSystem | Path.Path> =>
  Effect.gen(function* () {
    const before = yield* session.ready;
    const timeout = yield* parsed(() =>
      duration(options.readTimeout ?? "30 seconds", "file read timeout"),
    );
    const bytes = yield* readFileBytes(file, options.maxBytes ?? 64 * 1024 * 1024).pipe(
      Effect.timeoutOrElse({
        duration: timeout,
        orElse: () =>
          Effect.fail(
            ReactorError.fromCode("Upload", "Source file read timed out", {
              outcome: "not-submitted",
            }),
          ),
      }),
    );
    const after = yield* session.ready;
    if (after.generation !== before.generation)
      return yield* ReactorError.fromCode(
        "Disconnected",
        "Connection changed while reading the upload file",
        { outcome: "not-submitted" },
      );
    const path = yield* Path.Path;
    const name = path.basename(file);
    const extension = path.extname(name).toLowerCase();
    const mime =
      options.mimeType ??
      (extension === ".png"
        ? "image/png"
        : extension === ".webp"
          ? "image/webp"
          : extension === ".jpg" || extension === ".jpeg"
            ? "image/jpeg"
            : "application/octet-stream");
    return yield* session.upload(
      name,
      mime,
      bytes,
      options.uploadTimeout === undefined ? {} : { uploadTimeout: options.uploadTimeout },
    );
  });
