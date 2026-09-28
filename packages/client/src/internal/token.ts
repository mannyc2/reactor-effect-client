/**
 * A session's current token. A session-scoped token lives at most six hours and
 * acts only on the sessions it created or was bound to, while a session may run
 * longer: so the session keeps one token for every call and, a margin before it
 * expires, mints the next bound to its own id. Concurrent calls share one mint.
 */
import * as Clock from "effect/Clock";
import * as Effect from "effect/Effect";
import type * as Redacted from "effect/Redacted";
import * as SynchronizedRef from "effect/SynchronizedRef";
import type { TokenGrant, Tokens } from "../Coordinator.js";
import { ReactorError } from "../ReactorError.js";

/**
 * How long before expiry the next token is minted, as Reactor's own guidance
 * does: a minute, or a quarter of a shorter token's life, so a short token is
 * refreshed once near its end rather than on every call.
 */
const refreshMarginMs = 60_000;
/** After a failed refresh, how long calls keep the current token before minting again. */
const retryMs = 10_000;

interface State {
  readonly grant: TokenGrant | undefined;
  /** When `grant` was minted, in epoch milliseconds. */
  readonly mintedAt: number;
  readonly sessionId: string | undefined;
  readonly retryAt: number;
}

export interface Token {
  /** The token for the next call: minted when first needed, refreshed before it expires. */
  readonly current: Effect.Effect<Redacted.Redacted<string> | undefined, ReactorError>;
  /** From now on, refreshed tokens are bound to `sessionId`. */
  readonly bind: (sessionId: string) => Effect.Effect<void>;
}

export const make = Effect.fnUntraced(function* (input: {
  /** An attached session needs no `create`. */
  readonly tokens: (Pick<Tokens, "bind"> & Partial<Pick<Tokens, "create">>) | undefined;
  /** An attached session's id; a created session's is bound once it is allocated. */
  readonly sessionId: string | undefined;
  /** A refresh failed while the current token still works. */
  readonly onRefreshFailure: (error: ReactorError) => Effect.Effect<void>;
}) {
  const { tokens, onRefreshFailure } = input;
  const state = yield* SynchronizedRef.make<State>({
    grant: undefined,
    mintedAt: 0,
    sessionId: input.sessionId,
    retryAt: 0,
  });
  const current: Token["current"] =
    tokens === undefined
      ? Effect.undefined
      : SynchronizedRef.modifyEffect(state, (value) =>
          Effect.gen(function* () {
            const now = yield* Clock.currentTimeMillis;
            const held = value.grant;
            const mint =
              value.sessionId !== undefined
                ? tokens.bind(value.sessionId)
                : (tokens.create ??
                  Effect.fail(
                    ReactorError.fromCode("InvalidInput", "creating a session needs a token", {
                      outcome: "not-submitted",
                    }),
                  ));
            if (held === undefined) {
              const grant = yield* mint;
              return [grant.jwt, { ...value, grant, mintedAt: now }] as const;
            }
            const expiresAt = held.expiresAt * 1000;
            const valid = expiresAt > now;
            const margin = Math.min(refreshMarginMs, (expiresAt - value.mintedAt) / 4);
            if (expiresAt - now > margin || (valid && now < value.retryAt))
              return [held.jwt, value] as const;
            const next = yield* Effect.result(mint);
            if (next._tag === "Success" && next.success.expiresAt > held.expiresAt)
              return [
                next.success.jwt,
                { ...value, grant: next.success, mintedAt: now, retryAt: 0 },
              ] as const;
            if (next._tag === "Failure") {
              if (!valid) return yield* next.failure;
              yield* onRefreshFailure(next.failure);
            }
            return [held.jwt, { ...value, retryAt: now + retryMs }] as const;
          }),
        );
  return {
    current,
    bind: (sessionId) => SynchronizedRef.update(state, (value) => ({ ...value, sessionId })),
  } satisfies Token;
});
