import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";
import * as Http from "effect/unstable/http/HttpClient";
import { ReactorError } from "../errors.js";
import { collectBytes } from "../bytes.js";
import { readFileBytes } from "../session/files.js";

export interface LoadLimits {
  readonly maxBytes: number;
  readonly timeoutMs: number;
}

const failure = (message: string, detail?: unknown): ReactorError =>
  new ReactorError("Upload", message, {
    operation: "load reference",
    outcome: "not-submitted",
    ...(detail === undefined ? {} : { detail }),
  });

/**
 * Every call rereads the URI. The H3 provider deduplicates subsequent uploads by
 * the bytes' hash; a URI cache cannot establish that mutable content is unchanged.
 * This loader works independently of any live provider session.
 */
export const loadReferenceBytes = (
  uri: string,
  limits: LoadLimits,
): Effect.Effect<Uint8Array, ReactorError, FileSystem.FileSystem | Path.Path | Http.HttpClient> =>
  Effect.gen(function* () {
    if (
      typeof uri !== "string" ||
      !Number.isSafeInteger(limits.maxBytes) ||
      limits.maxBytes <= 0 ||
      !Number.isFinite(limits.timeoutMs) ||
      limits.timeoutMs <= 0
    ) {
      return yield* Effect.fail(
        failure("Reference URI and positive finite loading bounds are required"),
      );
    }
    if (uri.startsWith("data:")) {
      const comma = uri.indexOf(",");
      if (comma < 0 || !uri.slice(0, comma).endsWith(";base64"))
        return yield* Effect.fail(failure("Only base64 data URIs are supported"));
      const encoded = uri.slice(comma + 1);
      if (encoded.length > 4 * Math.ceil(limits.maxBytes / 3))
        return yield* Effect.fail(failure("Reference exceeds the byte bound"));
      const result = Schema.decodeUnknownResult(Schema.Uint8ArrayFromBase64)(encoded);
      if (result._tag === "Failure")
        return yield* Effect.fail(failure("Invalid base64 reference", result.failure));
      if (result.success.byteLength > limits.maxBytes)
        return yield* Effect.fail(failure("Reference exceeds the byte bound"));
      return result.success;
    }
    if (uri.startsWith("/") || uri.startsWith("file://")) {
      const path = uri.startsWith("/")
        ? uri
        : yield* Effect.gen(function* () {
            const url = yield* Effect.try({
              try: () => new URL(uri),
              catch: (cause) => failure("Invalid file URI", cause),
            });
            return yield* (yield* Path.Path)
              .fromFileUrl(url)
              .pipe(Effect.mapError((cause) => failure("Invalid file URI", cause)));
          });
      return yield* readFileBytes(path, limits.maxBytes).pipe(
        Effect.mapError((cause) =>
          cause instanceof ReactorError
            ? cause
            : failure("Reference file could not be read", cause),
        ),
      );
    }
    if (uri.startsWith("http://") || uri.startsWith("https://")) {
      const http = Http.withScope(yield* Http.HttpClient);
      return yield* Effect.scoped(
        Effect.gen(function* () {
          const response = yield* http
            .get(uri)
            .pipe(Effect.mapError((cause) => failure("Reference download failed", cause)));
          if (response.status < 200 || response.status >= 300)
            return yield* Effect.fail(
              failure(`Reference download returned HTTP ${response.status}`),
            );
          const declared = Number(response.headers["content-length"] ?? "0");
          if (Number.isFinite(declared) && declared > limits.maxBytes)
            return yield* Effect.fail(failure("Reference exceeds the byte bound"));
          return yield* collectBytes(response.stream, limits.maxBytes).pipe(
            Effect.mapError((cause) =>
              cause instanceof ReactorError ? cause : failure("Reference download failed", cause),
            ),
          );
        }),
      );
    }
    return yield* Effect.fail(failure("Unsupported reference URI scheme"));
  }).pipe(
    Effect.timeoutOrElse({
      duration: limits.timeoutMs,
      orElse: () => Effect.fail(failure("Reference loading timed out")),
    }),
  );
