import { Schema } from "effect";
import { HttpApi, HttpApiEndpoint, HttpApiGroup } from "effect/unstable/httpapi";

/**
 * What the page needs from its server: whether it can run live, and tokens
 * for its H3 session. The server holds the API key and mints them; the page
 * never sees the key. Shared by `server.ts` and the page's typed client.
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

export class Api extends HttpApi.make("reactor-browser-example").add(
  HttpApiGroup.make("session", { topLevel: true }).add(
    HttpApiEndpoint.get("live", "/api/live", { success: Live }),
    HttpApiEndpoint.post("token", "/api/token", {
      payload: TokenRequest,
      success: SessionToken,
      error: TokenUnavailable,
    }),
  ),
) {}
