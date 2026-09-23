import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import { collectBytes } from "../bytes.js";
import { ReactorError } from "../errors.js";
import type { Session, Uploaded } from "./index.js";

export interface FileUploadOptions {
  readonly mimeType?: string;
  readonly maxBytes?: number;
  readonly readTimeoutMs?: number;
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
      return yield* new ReactorError({
        code: "InvalidInput",
        message: "A file and positive byte bound are required",
        context: { outcome: "not-submitted" },
      });
    }
    const fs = yield* FileSystem.FileSystem;
    return yield* collectBytes(fs.stream(file, { bytesToRead: maxBytes + 1 }), maxBytes).pipe(
      Effect.mapError((cause) =>
        ReactorError.is(cause)
          ? cause
          : new ReactorError({
              code: "Upload",
              message: "Source file could not be read",
              context: { outcome: "not-submitted", detail: cause },
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
    const timeout = options.readTimeoutMs ?? 30_000;
    if (!Number.isFinite(timeout) || timeout <= 0)
      return yield* new ReactorError({
        code: "InvalidInput",
        message: "Invalid file-read deadline",
        context: { outcome: "not-submitted" },
      });
    const bytes = yield* readFileBytes(file, options.maxBytes ?? 64 * 1024 * 1024).pipe(
      Effect.timeoutOrElse({
        duration: timeout,
        orElse: () =>
          Effect.fail(
            new ReactorError({
              code: "Upload",
              message: "Source file read timed out",
              context: { outcome: "not-submitted" },
            }),
          ),
      }),
    );
    const after = yield* session.ready;
    if (after.generation !== before.generation)
      return yield* new ReactorError({
        code: "Disconnected",
        message: "Connection changed while reading the upload file",
        context: { outcome: "not-submitted" },
      });
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
    return yield* session.upload(name, mime, bytes);
  });
