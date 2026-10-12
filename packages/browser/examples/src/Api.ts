import { Schema } from "effect";
import { HttpApi, HttpApiEndpoint, HttpApiGroup, HttpApiSchema } from "effect/http-api";
import { SpeechError } from "reactor-effect-client/SpeechModel";

/**
 * What the pages need from their server: whether the Studio can run live,
 * tokens for its H3 session, and the character page's speech and model. The
 * server holds every API key; no page sees one. Shared by `server.ts` and the
 * pages' typed clients.
 */

/** Whether the server mints tokens, and the cap each session it creates runs under. */
export const Live = Schema.Union([
  Schema.TaggedStruct("Available", { maxSessionSeconds: Schema.Int }),
  /** The server has no API key: the page runs offline. */
  Schema.TaggedStruct("Unavailable", {}),
]);
export type Live = typeof Live.Type;

/**
 * Without `session` the token may create one short session; with it, the
 * token is bound to that open session, so the session carries on past its
 * first token.
 */
export const TokenRequest = Schema.Struct({ session: Schema.optionalKey(Schema.String) });

export const SessionToken = Schema.Struct({
  jwt: Schema.String,
  /** When the token expires, in seconds since the epoch. */
  expiresAt: Schema.Finite,
  /** The longest session the token can start; absent for a bound token. */
  maxSessionSeconds: Schema.optionalKey(Schema.Int),
});

export class TokenUnavailable extends Schema.TaggedError<TokenUnavailable>()(
  "TokenUnavailable",
  { message: Schema.String },
  { httpApiStatus: 503 },
) {}

/** What the character page can use, as the server is set up. */
export const Character = Schema.Struct({
  speech: Schema.Union([
    Schema.TaggedStruct("Available", { provider: Schema.String, model: Schema.String }),
    /** The server has no speech provider configured. */
    Schema.TaggedStruct("Unavailable", {}),
  ]),
  avatar: Schema.Union([
    /** `model` is the address of the model's `.model3.json`. */
    Schema.TaggedStruct("Available", { model: Schema.String }),
    /** What the server is missing, to show the person setting it up. */
    Schema.TaggedStruct("Unavailable", { missing: Schema.String }),
  ]),
});
export type Character = typeof Character.Type;

/** One utterance; its length bounds what one request can cost. */
export const SpeechRequest = Schema.Struct({
  text: Schema.NonEmptyString.check(Schema.isMaxLength(500)),
});

/** One block of generated audio: interleaved signed 16-bit little-endian PCM. */
export const SpeechBlock = Schema.Struct({
  sampleRate: Schema.Int,
  channels: Schema.Int,
  pcm: Schema.Uint8ArrayFromBase64,
});

export class SpeechUnavailable extends Schema.TaggedError<SpeechUnavailable>()(
  "SpeechUnavailable",
  { message: Schema.String },
  { httpApiStatus: 503 },
) {}

export class Api extends HttpApi.make("reactor-browser-example")
  .add(
    HttpApiGroup.make("session", { topLevel: true }).add(
      HttpApiEndpoint.get("live", "/api/live", { success: Live }),
      HttpApiEndpoint.post("token", "/api/token", {
        payload: TokenRequest,
        success: SessionToken,
        error: TokenUnavailable,
      }),
    ),
  )
  .add(
    HttpApiGroup.make("character", { topLevel: true }).add(
      HttpApiEndpoint.get("character", "/api/character", { success: Character }),
      /**
       * Streams one utterance as it is generated. A provider failure, before
       * or after audio, arrives as the stream's `SpeechError`, so a response
       * that ends without one is the provider's complete output.
       */
      HttpApiEndpoint.post("speak", "/api/speech", {
        payload: SpeechRequest,
        success: HttpApiSchema.StreamSse({ data: SpeechBlock, error: SpeechError }),
        error: SpeechUnavailable,
      }),
    ),
  ) {}
