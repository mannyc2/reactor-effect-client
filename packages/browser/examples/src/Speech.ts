/**
 * The character page's speech model: the server's `/api/speech`, which runs
 * the provider with a key the page never sees. The page consumes
 * `SpeechModel` and nothing else, as it would any provider's layer.
 */
import { Effect, Layer, Stream } from "effect";
import { HttpApiClient } from "effect/http-api";
import type { AudioFrame } from "reactor-effect-client/Media";
import { SpeechError, SpeechModel } from "reactor-effect-client/SpeechModel";
import { Api } from "./Api.ts";

/** One block from the server as the PCM a speaker plays. */
const frame = (
  block: { readonly sampleRate: number; readonly channels: number; readonly pcm: Uint8Array },
  sequence: number,
): AudioFrame => {
  const view = new DataView(block.pcm.buffer, block.pcm.byteOffset, block.pcm.byteLength);
  const samples = new Int16Array(Math.floor(block.pcm.byteLength / 2));
  for (let index = 0; index < samples.length; index++)
    samples[index] = view.getInt16(index * 2, true);
  return {
    _tag: "AudioFrame",
    track: "speech",
    sampleRate: block.sampleRate,
    channels: block.channels,
    sequence: BigInt(sequence),
    samples,
  };
};

/**
 * The request reached the server, so whether the provider received it is
 * unknown from here; a provider failure the server saw keeps its own outcome.
 */
const lost = (message: string) =>
  SpeechError.make({ reason: { _tag: "Transport", message }, outcome: "unknown" });

export const layer = Layer.effect(
  SpeechModel,
  Effect.gen(function* () {
    const api = yield* HttpApiClient.make(Api);
    return SpeechModel.of({
      stream: (request) =>
        Stream.unwrap(
          api.speak({ payload: { text: request.text } }).pipe(
            Effect.catchTags({
              SpeechUnavailable: () =>
                Effect.fail(
                  SpeechError.make({
                    reason: { _tag: "Http", status: 503 },
                    outcome: "not-submitted",
                  }),
                ),
              SchemaError: () =>
                Effect.fail(
                  SpeechError.make({
                    reason: { _tag: "InvalidInput", message: "Speech takes 1 to 500 characters" },
                    outcome: "not-submitted",
                  }),
                ),
              HttpClientError: () => Effect.fail(lost("The speech request failed")),
            }),
          ),
        ).pipe(
          Stream.catchTags({
            HttpClientError: () => Stream.fail(lost("The speech response ended unexpectedly")),
            SchemaError: () => Stream.fail(lost("The speech response was malformed")),
            SseError: () => Stream.fail(lost("The speech response was malformed")),
            // The server never asks a page to reconnect: one request is one utterance.
            Retry: () => Stream.fail(lost("The speech response asked to reconnect")),
          }),
          Stream.zipWithIndex,
          Stream.map(([block, sequence]) => frame(block, sequence)),
        ),
    });
  }),
);
