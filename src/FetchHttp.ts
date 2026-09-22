import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Stream from "effect/Stream";
import * as HttpClient from "effect/unstable/http/HttpClient";
import * as HttpClientError from "effect/unstable/http/HttpClientError";
import * as HttpClientResponse from "effect/unstable/http/HttpClientResponse";
import * as FetchHttpClient from "effect/unstable/http/FetchHttpClient";
import { fromOwnedReadableStream } from "./media-stream.js";
import { ReactorError } from "./errors.js";

/**
 * A Fetch implementation of Effect's HttpClient. The small response adaptation
 * owns both cancellation and reader-lock release, and preserves an empty body
 * as zero bytes. Applications may supply another HttpClient implementation.
 */
export const layer = Layer.effect(HttpClient.HttpClient, Effect.gen(function* () {
  const fetch = yield* FetchHttpClient.Fetch;
  return HttpClient.make((request, url, signal) => {
    const send = (body: BodyInit | undefined) => Effect.tryPromise({
      try: () => fetch(url, {
        method: request.method,
        headers: request.headers,
        ...(body === undefined ? {} : { body }),
        signal,
        credentials: "omit",
        redirect: "error",
        // Node requires this option for streaming request bodies. It is ignored
        // by browsers, and is not part of the portable coordinator contract.
        ...(body instanceof ReadableStream ? { duplex: "half" } : {}),
      }),
      catch: (cause) => new HttpClientError.HttpClientError({
        reason: new HttpClientError.TransportError({ request, cause }),
      }),
    }).pipe(Effect.map((source) => {
      const response = HttpClientResponse.fromWeb(request, source);
      const stream = source.body === null ? Stream.empty : fromOwnedReadableStream({
        evaluate: () => source.body!,
        onError: (cause) => new ReactorError("Http", "HTTP response body failed", { detail: cause }),
      }).pipe(Stream.mapError((cause) => new HttpClientError.HttpClientError({
        reason: new HttpClientError.DecodeError({ request, response, cause }),
      })));
      // Retain the official response's public accessors and identity; only its
      // stream owns the additional reader-release contract.
      return Object.create(response, { stream: { get: () => stream } }) as HttpClientResponse.HttpClientResponse;
    }));

    switch (request.body._tag) {
      case "Raw": return send(request.body.body as BodyInit);
      case "Uint8Array": return send(new Uint8Array(request.body.body));
      case "FormData": return send(request.body.formData);
      case "Stream": return Stream.toReadableStreamEffect(request.body.stream).pipe(Effect.flatMap(send));
      default: return send(undefined);
    }
  });
}));
