/** OpenRouter's streaming speech endpoint as an Effect speech model. */
import * as Config from "effect/Config";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Redacted from "effect/Redacted";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as SchemaTransformation from "effect/SchemaTransformation";
import * as Model from "effect/ai/Model";
import * as FetchHttpClient from "effect/http/FetchHttpClient";
import * as HttpClient from "effect/http/HttpClient";
import * as HttpClientRequest from "effect/http/HttpClientRequest";
import * as SpeechPcm from "./internal/speechPcm.js";
import { SpeechError, SpeechModel } from "./SpeechModel.js";

export interface Options {
  readonly apiKey: Redacted.Redacted<string>;
  readonly voice: string;
}

const Body = Schema.Struct({
  model: Schema.NonEmptyString,
  input: Schema.NonEmptyString,
  voice: Schema.NonEmptyString,
  response_format: Schema.Literal("pcm"),
});

const PositiveIntFromString = Schema.FiniteFromString.check(
  Schema.isInt(),
  Schema.isGreaterThan(0),
);
const ContentType = Schema.TemplateLiteralParser([
  "audio/pcm;",
  Schema.String.pipe(
    Schema.decodeTo(
      Schema.Record(Schema.String, Schema.String),
      SchemaTransformation.splitKeyValue({ separator: ";" }),
    ),
    Schema.decodeTo(
      Schema.Struct({ rate: PositiveIntFromString, channels: PositiveIntFromString }),
    ),
  ),
]);

/**
 * A native Effect Model: provide it as a Layer, or use its provider/model metadata.
 * Construction makes no request. Each stream requests signed 16-bit little-endian
 * PCM, with sample rate and channel count taken from the response Content-Type.
 * Model and voice selection are explicit; no request is retried by this adapter.
 */
// Like Effect's model constructors, this takes a name and configuration rather than a piped value.
// @effect-diagnostics-next-line missingPipeableSignature:off
export const model = (name: string, options: Options) =>
  Model.make(
    "openrouter",
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
                  model: name,
                  input: request.text,
                  voice: options.voice,
                  response_format: "pcm",
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
                  "https://openrouter.ai/api/v1/audio/speech",
                ).pipe(
                  HttpClientRequest.bearerToken(options.apiKey),
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
                const [, format] = yield* Schema.decodeUnknownEffect(ContentType)(
                  response.headers["content-type"]?.toLowerCase().replace(/\s+/g, ""),
                ).pipe(
                  Effect.mapError(() =>
                    SpeechError.make({
                      reason: {
                        _tag: "InvalidAudio",
                        message: "Speech response requires PCM with a valid rate and channel count",
                      },
                      outcome: "replied",
                    }),
                  ),
                );
                return response.stream.pipe(
                  Stream.mapError((cause) =>
                    SpeechError.make({
                      reason: { _tag: "Transport", message: "Speech response ended unexpectedly" },
                      outcome: "replied",
                      detail: Redacted.make(cause),
                    }),
                  ),
                  SpeechPcm.decode({ sampleRate: format.rate, channels: format.channels }),
                );
              }),
            ).pipe(Stream.withSpan("OpenRouterSpeech.stream", { captureStackTrace: false })),
        });
      }),
    ),
  );

/** Reads OPENROUTER_API_KEY, OPENROUTER_SPEECH_MODEL and OPENROUTER_SPEECH_VOICE at the composition root. */
export const layerConfig = Layer.unwrap(
  Effect.gen(function* () {
    const apiKey = yield* Config.Redacted("OPENROUTER_API_KEY");
    const name = yield* Config.String("OPENROUTER_SPEECH_MODEL");
    const voice = yield* Config.String("OPENROUTER_SPEECH_VOICE");
    return model(name, { apiKey, voice });
  }),
);
