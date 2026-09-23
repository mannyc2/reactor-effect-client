import * as Effect from "effect/Effect";
import * as Stream from "effect/Stream";
import * as HttpClientError from "effect/unstable/http/HttpClientError";
import type * as HttpClientResponse from "effect/unstable/http/HttpClientResponse";
import { ReactorError } from "../../errors.js";

export interface HttpReply {
  readonly status: number;
  readonly headers: Headers;
  readonly bytes: Uint8Array<ArrayBuffer>;
}

/** Count every emitted chunk, including empty chunks, before retaining owned bytes. */
export const readBody = (
  response: HttpClientResponse.HttpClientResponse,
  operation: string,
  maxBytes: number,
  maxChunks: number,
) =>
  Effect.gen(function* () {
    let size = 0;
    let count = 0;
    const chunks: Uint8Array<ArrayBuffer>[] = [];
    yield* response.stream.pipe(
      Stream.runForEach((chunk) =>
        Effect.gen(function* () {
          size += chunk.byteLength;
          count++;
          if (size > maxBytes || count > maxChunks)
            return yield* Effect.fail(
              new ReactorError({
                code: "Overflow",
                message: `${operation} response exceeds its ${maxBytes} byte/${maxChunks} chunk bound`,
                context: { operation, status: response.status, outcome: "replied" },
              }),
            );
          // A supplied HTTP implementation may reuse its transport buffer.
          if (chunk.byteLength !== 0) chunks.push(new Uint8Array(chunk));
        }),
      ),
      Effect.catch((error) =>
        count === 0 &&
        HttpClientError.isHttpClientError(error) &&
        error.reason._tag === "EmptyBodyError"
          ? Effect.void
          : Effect.fail(error),
      ),
    );
    const bytes = new Uint8Array(size);
    let offset = 0;
    for (const chunk of chunks) {
      bytes.set(chunk, offset);
      offset += chunk.byteLength;
    }
    return bytes;
  });

/** An absent body is not JSON null. Invalid UTF-8 is never repaired silently. */
export const decodeJsonReply = (reply: HttpReply, operation?: string): unknown => {
  try {
    const result: unknown = JSON.parse(
      new TextDecoder("utf-8", { fatal: true }).decode(reply.bytes),
    );
    return result;
  } catch (cause) {
    throw new ReactorError({
      code: "Protocol",
      message:
        operation === undefined
          ? "invalid UTF-8/JSON HTTP response"
          : `Reactor ${operation} request or response failed`,
      context: {
        status: reply.status,
        ...(operation === undefined ? {} : { operation }),
        outcome: "replied",
        detail: cause,
      },
    });
  }
};

export const retryAfterMs = (headers: Headers): number | undefined => {
  const raw = headers.get("retry-after");
  if (raw === null || !/^\s*(?:\d+(?:\.\d*)?|\.\d+)\s*$/.test(raw)) return undefined;
  const seconds = Number(raw);
  return Number.isFinite(seconds) && seconds >= 0 && Number.isFinite(seconds * 1000)
    ? seconds * 1000
    : undefined;
};
