import * as Effect from "effect/Effect";
import * as Stream from "effect/Stream";
import { ReactorError } from "./errors.js";

/** Copy borrowed chunks once, enforcing both memory and chunk-admission bounds. */
export const collectBytes = <E, R>(
  source: Stream.Stream<Uint8Array, E, R>,
  maxBytes: number,
): Effect.Effect<Uint8Array, E | ReactorError, R> =>
  Effect.suspend(() => {
    if (!Number.isSafeInteger(maxBytes) || maxBytes <= 0)
      return Effect.fail(
        new ReactorError({
          code: "InvalidInput",
          message: "Invalid byte collection bound",
          context: { outcome: "not-submitted" },
        }),
      );
    return Stream.runFoldEffect(
      source,
      () => ({ chunks: [] as Uint8Array[], total: 0 }),
      (buffer, chunk) => {
        if (chunk.byteLength > maxBytes - buffer.total)
          return Effect.fail(
            new ReactorError({
              code: "Upload",
              message: "Source exceeds the byte bound",
              context: { outcome: "not-submitted" },
            }),
          );
        if (buffer.chunks.length >= 16_384)
          return Effect.fail(
            new ReactorError({
              code: "Upload",
              message: "Source exceeds the chunk bound",
              context: { outcome: "not-submitted" },
            }),
          );
        buffer.total += chunk.byteLength;
        buffer.chunks.push(new Uint8Array(chunk));
        return Effect.succeed(buffer);
      },
    ).pipe(
      Effect.map(({ chunks, total }) => {
        const bytes = new Uint8Array(total);
        let offset = 0;
        for (const chunk of chunks) {
          bytes.set(chunk, offset);
          offset += chunk.byteLength;
        }
        return bytes;
      }),
    );
  });
