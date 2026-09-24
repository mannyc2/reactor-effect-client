import { Schema } from "effect";
import { HttpApi, HttpApiEndpoint, HttpApiGroup } from "effect/unstable/httpapi";

/**
 * The one thing the page needs from its server: a token for one short H3
 * session. The server holds the API key and mints the token; the page never
 * sees the key. Shared by `server.ts` and the page's typed client.
 */
export const SessionToken = Schema.Struct({
  jwt: Schema.String,
  /** When the token expires, in seconds since the epoch. */
  expiresAt: Schema.Finite,
  /** The longest session the token can start. */
  maxSessionSeconds: Schema.Int,
});

export class TokenUnavailable extends Schema.TaggedError<TokenUnavailable>()(
  "TokenUnavailable",
  { message: Schema.String },
  { httpApiStatus: 503 },
) {}

export class Api extends HttpApi.make("reactor-browser-example").add(
  HttpApiGroup.make("session", { topLevel: true }).add(
    HttpApiEndpoint.post("token", "/api/token", {
      success: SessionToken,
      error: TokenUnavailable,
    }),
  ),
) {}
