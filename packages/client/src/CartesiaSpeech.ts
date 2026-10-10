/** Cartesia's streaming bytes endpoint as an Effect speech model. */
import * as Config from "effect/Config";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Redacted from "effect/Redacted";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as Model from "effect/ai/Model";
import * as FetchHttpClient from "effect/http/FetchHttpClient";
import * as HttpClient from "effect/http/HttpClient";
import * as HttpClientRequest from "effect/http/HttpClientRequest";
import type { AudioFrame } from "./Media.js";
import { SpeechError, SpeechModel } from "./SpeechModel.js";

export interface Options {
  readonly apiKey: Redacted.Redacted<string>;
  readonly voice: string;
}

const Body = Schema.Struct({
  model_id: Schema.NonEmptyString,
  transcript: Schema.NonEmptyString,
  voice: Schema.NonEmptyString,
  output_format: Schema.Struct({
    container: Schema.Literal("raw"),
    encoding: Schema.Literal("pcm_s16le"),
    sample_rate: Schema.Literal(24000),
  }),
});

interface DecoderState {
  readonly pending: number | undefined;
  readonly sequence: bigint;
}

/** HTTP chunks may split a sample; only a trailing half-sample at EOF is malformed. */
const decode = Effect.fnUntraced(function* (
  state: DecoderState,
  chunk: Uint8Array | undefined,
): Effect.fn.Return<readonly [DecoderState, ReadonlyArray<AudioFrame>], SpeechError> {
  if (chunk === undefined) {
    if (state.pending !== undefined || state.sequence === 0n)
      return yield* SpeechError.make({
        reason: {
          _tag: "InvalidAudio",
          message: "Speech response is empty or ends with an incomplete PCM sample",
        },
        outcome: "replied",
      });
    return [state, []];
  }
  const offset = state.pending === undefined ? 0 : 1;
  const bytes = new Uint8Array(chunk.byteLength + offset);
  if (state.pending !== undefined) bytes[0] = state.pending;
  bytes.set(chunk, offset);
  const samples = new Int16Array(Math.floor(bytes.byteLength / 2));
  const view = new DataView(bytes.buffer);
  for (let index = 0; index < samples.length; index++)
    samples[index] = view.getInt16(index * 2, true);
  const pending = bytes.byteLength % 2 === 0 ? undefined : bytes.at(-1);
  if (samples.length === 0) return [{ ...state, pending }, []];
  return [
    { pending, sequence: state.sequence + 1n },
    [
      {
        _tag: "AudioFrame",
        track: "speech",
        sampleRate: 24000,
        channels: 1,
        sequence: state.sequence,
        samples,
      },
    ],
  ];
});

/**
 * A native Effect Model: provide it as a Layer, or use its provider/model metadata.
 * Construction makes no request. Each stream requests mono, 24 kHz signed 16-bit
 * PCM using Cartesia API version 2026-08-14. Model and voice selection are explicit.
 */
// Like Effect's model constructors, this takes a name and configuration rather than a piped value.
// @effect-diagnostics-next-line missingPipeableSignature:off
export const model = (name: string, options: Options) =>
  Model.make(
    "cartesia",
    name,
    Layer.effect(
      SpeechModel,
      Effect.gen(function* () {
        const http = (yield* HttpClient.HttpClient).pipe(HttpClient.withScope);
        return SpeechModel.of({
          stream: (request) =>
            Stream.unwrap(
              Effect.gen(function* () {
                const body = yield* Schema.encodeEffect(Schema.fromJsonString(Body))({
                  model_id: name,
                  transcript: request.text,
                  voice: options.voice,
                  output_format: { container: "raw", encoding: "pcm_s16le", sample_rate: 24000 },
                }).pipe(
                  Effect.mapError(() =>
                    SpeechError.make({
                      reason: {
                        _tag: "InvalidInput",
                        message: "Speech requires a model, voice and nonempty text",
                      },
                      outcome: "not-submitted",
                    }),
                  ),
                );
                const response = yield* HttpClientRequest.post(
                  "https://api.cartesia.ai/tts/bytes",
                ).pipe(
                  HttpClientRequest.bearerToken(options.apiKey),
                  HttpClientRequest.setHeader("Cartesia-Version", "2026-08-14"),
                  HttpClientRequest.bodyText(body, "application/json"),
                  http.execute,
                  Effect.provideService(FetchHttpClient.RequestInit, {
                    credentials: "omit",
                    redirect: "error",
                  }),
                  Effect.provideService(HttpClient.TracerDisabledWhen, () => true),
                  Effect.mapError((cause) =>
                    SpeechError.make({
                      reason: { _tag: "Transport", message: "Speech request failed" },
                      outcome: "unknown",
                      detail: Redacted.make(cause),
                    }),
                  ),
                );
                if (response.status !== 200)
                  return yield* SpeechError.make({
                    reason: { _tag: "Http", status: response.status },
                    outcome: "replied",
                  });
                if (response.headers["content-type"]?.toLowerCase().startsWith("audio/") !== true)
                  return yield* SpeechError.make({
                    reason: {
                      _tag: "InvalidAudio",
                      message: "Speech response has no audio content type",
                    },
                    outcome: "replied",
                  });
                return response.stream.pipe(
                  Stream.mapError((cause) =>
                    SpeechError.make({
                      reason: { _tag: "Transport", message: "Speech response ended unexpectedly" },
                      outcome: "replied",
                      detail: Redacted.make(cause),
                    }),
                  ),
                  Stream.concat(Stream.succeed(undefined)),
                  Stream.mapAccumEffect(
                    (): DecoderState => ({ pending: undefined, sequence: 0n }),
                    decode,
                  ),
                );
              }),
            ).pipe(Stream.withSpan("CartesiaSpeech.stream", { captureStackTrace: false })),
        });
      }),
    ),
  );

/** Reads CARTESIA_API_KEY, CARTESIA_MODEL and CARTESIA_VOICE_ID at the composition root. */
export const layerConfig = Layer.unwrap(
  Effect.gen(function* () {
    const apiKey = yield* Config.Redacted("CARTESIA_API_KEY");
    const name = yield* Config.String("CARTESIA_MODEL");
    const voice = yield* Config.String("CARTESIA_VOICE_ID");
    return model(name, { apiKey, voice });
  }),
);
