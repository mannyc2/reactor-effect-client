import { Schema } from "effect";
import { HttpApi, HttpApiEndpoint, HttpApiGroup } from "effect/unstable/httpapi";

/**
 * The one thing the page needs from its server: tokens for its H3 session.
 * The server holds the API key and mints them; the page never sees the key.
 * Without `session` the token may create one short session; with it, the
 * token is bound to that open session, so the session carries on past its
 * first token. Shared by `server.ts` and the page's typed client.
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

export class Api extends HttpApi.make("reactor-browser-example").add(
  HttpApiGroup.make("session", { topLevel: true }).add(
    HttpApiEndpoint.post("token", "/api/token", {
      payload: TokenRequest,
      success: SessionToken,
      error: TokenUnavailable,
    }),
  ),
) {}
