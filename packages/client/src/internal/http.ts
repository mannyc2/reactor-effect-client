/**
 * The client's bounded HTTP reads: a URL checked before anything requests it, and a response body
 * read within a byte bound.
 */
import * as Effect from "effect/Effect";
import * as Stream from "effect/Stream";
import type * as HttpClientError from "effect/http/HttpClientError";
import type * as HttpClientResponse from "effect/http/HttpClientResponse";
import { ReactorError } from "../ReactorError.js";

// The client calls its HTTP helpers directly, on values it holds; nothing pipes into them.
// @effect-diagnostics-next-line missingPipeableSignature:off
export const checkedUrl = (url: string, base?: string): Effect.Effect<URL, ReactorError> =>
  Effect.try({
    try: () => new URL(url, base),
    catch: (cause) => ReactorError.fromCode("Protocol", "HTTP URL is malformed", { detail: cause }),
  }).pipe(
    Effect.filterOrFail(
      (value) =>
        (value.protocol === "https:" || value.protocol === "http:") &&
        value.username === "" &&
        value.password === "",
      () =>
        ReactorError.fromCode(
          "Protocol",
          "HTTP URL must use http(s) and contain no embedded credentials",
        ),
    ),
  );

/** The body within `maxBytes` and 16,384 chunks, since every chunk, even an empty one, costs memory. */
// Called directly on a response the caller holds, as `checkedUrl` is.
// @effect-diagnostics-next-line missingPipeableSignature:off
export const bodyWithin = (
  response: HttpClientResponse.HttpClientResponse,
  maxBytes: number,
): Effect.Effect<Uint8Array, HttpClientError.HttpClientError | ReactorError> => {
  const overflow = ReactorError.fromCode(
    "Overflow",
    `response exceeds its ${maxBytes} byte bound`,
    {
      outcome: "replied",
    },
  );
  const refused: Stream.Stream<Uint8Array, HttpClientError.HttpClientError | ReactorError> =
    Stream.fail(overflow);
  return response.stream.pipe(
    Stream.catchIf(
      (error) => error.reason._tag === "EmptyBodyError",
      () => Stream.empty,
    ),
    Stream.limitBytes(maxBytes, () => refused),
    Stream.zipWithIndex,
    Stream.mapEffect(([chunk, index]) =>
      index < 16_384 ? Effect.succeed(chunk) : Effect.fail(overflow),
    ),
    Stream.mkUint8Array,
  );
};
