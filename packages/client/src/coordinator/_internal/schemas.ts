import { Clock, Duration, Effect, Redacted, Result, Schema } from "effect";
import { duration } from "../../duration.js";
import { parse, parsedInput, ReactorError } from "../../errors.js";

const failure = (operation: string, message: string, cause?: unknown) =>
  ReactorError.fromCode("Protocol", message, {
    operation,
    outcome: "replied",
    ...(cause === undefined ? {} : { detail: cause }),
  });
const PositiveInteger = Schema.Int.check(Schema.isGreaterThan(0));
const Pricing = Schema.Struct({
  settings: Schema.Struct({
    currency_code: Schema.Literal("USD"),
    credits_per_dollar: PositiveInteger,
  }),
  models: Schema.Array(
    Schema.Struct({
      name: Schema.String,
      rate: Schema.Struct({
        amount_per_sec: Schema.Finite,
        unit: Schema.String,
        denomination: Schema.String,
      }),
    }),
  ),
});

const unknownRate = (cause?: unknown) =>
  failure("pricing", "Reactor pricing has an unknown model, currency or rate unit", cause);

/** The one rate this client prices: whole credits per second. Other models' rates may differ. */
const CreditsRate = Schema.Struct({
  amount_per_sec: PositiveInteger,
  unit: Schema.Literal("credits"),
  denomination: Schema.Literal("second"),
});

/**
 * Pricing lists a model by its bare name, `h3-reference-to-video-turbo-realtime`,
 * where sessions and tokens take its connect slug,
 * `reactor/h3-reference-to-video-turbo-realtime`: the name after the owner.
 */
const pricedAs = (model: string): string => model.slice(model.lastIndexOf("/") + 1);

/**
 * The rate of `model`, given by its connect slug (or its bare name). Preserve
 * the integer ratio; callers decide whether the returned price fits their budget.
 */
export const modelRate = (value: unknown, model: string) =>
  Schema.decodeUnknownEffect(Pricing)(value).pipe(
    Effect.flatMap((decoded) => {
      const matches = decoded.models.filter(
        (entry) => entry.name === model || entry.name === pricedAs(model),
      );
      return Schema.decodeUnknownEffect(CreditsRate)(
        matches.length === 1 ? matches[0]!.rate : undefined,
      ).pipe(
        Effect.map((rate) => ({
          creditsPerDollar: decoded.settings.credits_per_dollar,
          creditsPerSecond: rate.amount_per_sec,
        })),
      );
    }),
    Effect.mapError(unknownRate),
  );

export interface TokenOptions {
  readonly apiKey: Redacted.Redacted<string>;
  readonly modelName: string;
  /**
   * The longest session the token may start, a whole number of seconds up to
   * one day. A bare number is milliseconds.
   */
  readonly maxSessionDuration: Duration.Input;
  /**
   * How long the token stays valid, a whole number of seconds more than 30
   * seconds past `maxSessionDuration`, so cleanup still holds a valid token. A
   * bare number is milliseconds.
   */
  readonly expiresAfter: Duration.Input;
  /**
   * @deprecated Removed in 0.3.0: use `maxSessionDuration` (a bare number is
   * milliseconds, so `maxSessionDurationSeconds: 60` becomes
   * `maxSessionDuration: "60 seconds"`).
   */
  readonly maxSessionDurationSeconds?: never;
  /**
   * @deprecated Removed in 0.3.0: use `expiresAfter` (a bare number is
   * milliseconds, so `expiresAfterSeconds: 300` becomes
   * `expiresAfter: "300 seconds"`).
   */
  readonly expiresAfterSeconds?: never;
}
/** Token options after validation, in the wire's whole seconds. */
export interface TokenRequest {
  readonly apiKey: Redacted.Redacted<string>;
  readonly modelName: string;
  readonly maxSessionDurationSeconds: number;
  readonly expiresAfterSeconds: number;
}
export interface TokenGrant {
  readonly jwt: Redacted.Redacted<string>;
  readonly expiresAt: number;
  readonly granted: { readonly maxSessions: 1; readonly maxSessionSeconds: number };
}
const wholeSeconds = (value: Duration.Duration): number => Math.round(Duration.toSeconds(value));
export const validateTokenOptions = (
  input: TokenOptions,
): Effect.Effect<TokenRequest, ReactorError> =>
  parsedInput(() => {
    const reject = (): never => {
      throw ReactorError.fromCode(
        "InvalidInput",
        "A bounded session needs a model, redacted key, positive duration, and token expiry allowing cleanup",
        { operation: "token", outcome: "not-submitted" },
      );
    };
    if (input === null || typeof input !== "object") return reject();
    const bounded = parse(() => ({
      maxSessionDurationSeconds: wholeSeconds(
        duration(input.maxSessionDuration, "session duration", {
          maximum: "1 day",
          wholeSeconds: true,
        }),
      ),
      // Whole milliseconds stay exact up to this bound, so the seconds do too.
      expiresAfterSeconds: wholeSeconds(
        duration(input.expiresAfter, "token expiry", {
          maximum: Duration.millis(Number.MAX_SAFE_INTEGER),
          wholeSeconds: true,
        }),
      ),
    }));
    if (Result.isFailure(bounded)) return reject();
    const options: TokenRequest = {
      apiKey: input.apiKey,
      modelName: input.modelName,
      ...bounded.success,
    };
    if (
      !Redacted.isRedacted(options.apiKey) ||
      Redacted.value(options.apiKey).length === 0 ||
      typeof options.modelName !== "string" ||
      options.modelName.length === 0 ||
      options.expiresAfterSeconds <= options.maxSessionDurationSeconds + 30
    )
      return reject();
    return Object.freeze(options);
  }, "token");

const Token = Schema.Struct({
  jwt: Schema.NonEmptyString,
  expires_at: Schema.Finite,
});
const TokenClaims = Schema.StringFromBase64Url.pipe(
  Schema.decodeTo(Schema.fromJsonString(Schema.Struct({ authorization_details: Schema.Unknown }))),
);
const SessionAuthorization = Schema.Tuple([
  Schema.Struct({
    type: Schema.Literal("session"),
    resources: Schema.Struct({
      models: Schema.Struct({ match: Schema.Tuple([Schema.NonEmptyString]) }),
    }),
    constraints: Schema.Struct({
      max_sessions: Schema.Literal(1),
      max_session_duration_seconds: PositiveInteger,
    }),
  }),
]);

export const decodeToken = (raw: unknown) =>
  Schema.decodeUnknownEffect(Token)(raw).pipe(
    Effect.mapError((cause) =>
      failure("token", "Reactor returned an invalid token response", cause),
    ),
  );

/** Validate the issuer's returned authority; token expiry alone never caps a remote session. */
export const grantedLimits = (jwt: string, options: TokenRequest) =>
  Effect.gen(function* () {
    const payload = /^[A-Za-z0-9_-]+\.([A-Za-z0-9_-]+)\.[A-Za-z0-9_-]+$/.exec(jwt)?.[1];
    const claims = yield* Schema.decodeUnknownEffect(TokenClaims)(payload);
    const [authorization] = yield* Schema.decodeUnknownEffect(SessionAuthorization, {
      onExcessProperty: "error",
    })(claims.authorization_details);
    const seconds = authorization.constraints.max_session_duration_seconds;
    if (
      authorization.resources.models.match[0] !== options.modelName ||
      seconds > options.maxSessionDurationSeconds
    )
      return yield* failure(
        "token",
        "Reactor returned a token outside the requested session grant",
      );
    return { maxSessions: authorization.constraints.max_sessions, maxSessionSeconds: seconds };
  }).pipe(
    Effect.mapError((cause) =>
      failure("token", "Reactor returned an invalid or unbounded session grant", cause),
    ),
  );

const Transport = Schema.Struct({
  protocol: Schema.NonEmptyString,
  version: Schema.NonEmptyString,
});
export const Inspection = Schema.Struct({
  observedAt: Schema.Finite,
  state: Schema.String,
  hasCapabilities: Schema.Boolean,
  selectedTransport: Schema.NullOr(Transport),
  cluster: Schema.NullOr(Schema.String),
  zone: Schema.NullOr(Schema.String),
  serverVersion: Schema.NullOr(Schema.String),
  additional: Schema.Record(
    Schema.String,
    Schema.Union([Schema.String, Schema.Finite, Schema.Boolean, Schema.Null]),
  ),
});
export type Inspection = typeof Inspection.Type;
const OptionalText = Schema.optionalKey(Schema.NullOr(Schema.String));
const modeledFields = new Set([
  "session_id",
  "state",
  "capabilities",
  "selected_transport",
  "cluster",
  "zone",
  "server_info",
]);
const sensitiveField = /secret|token|credential|password|key|jwt|url|auth/i;

const additionalFacts = (raw: unknown): Record<string, string | number | boolean | null> => {
  const facts: [string, string | number | boolean | null][] = [];
  if (typeof raw !== "object" || raw === null || Array.isArray(raw)) return {};
  for (const [key, value] of Object.entries(raw as Record<string, unknown>).slice(0, 64)) {
    if (modeledFields.has(key) || sensitiveField.test(key)) continue;
    if (value === null || typeof value === "number" || typeof value === "boolean")
      facts.push([key, value]);
    else if (typeof value === "string") facts.push([key, value.slice(0, 200)]);
  }
  return Object.fromEntries(facts);
};

// Inspection needs coordinator facts, even when connection capabilities are
// only partially published. Allocation/readiness uses the stricter Descriptor.
const SessionDescription = Schema.Struct({
  session_id: Schema.NonEmptyString,
  state: Schema.NonEmptyString,
  capabilities: Schema.optionalKey(Schema.NullOr(Schema.JsonObject)),
  selected_transport: Schema.optionalKey(Schema.NullOr(Transport)),
  cluster: OptionalText,
  zone: OptionalText,
  server_info: Schema.optionalKey(Schema.NullOr(Schema.Struct({ server_version: Schema.String }))),
});

export const decodeInspection = (
  raw: unknown,
  sessionId: string,
): Effect.Effect<Inspection, ReactorError> =>
  Effect.gen(function* () {
    const value = yield* Schema.decodeUnknownEffect(SessionDescription)(raw).pipe(
      Effect.mapError((cause) =>
        failure("inspect", "Reactor returned an invalid session description", cause),
      ),
    );
    if (value.session_id !== sessionId)
      return yield* failure("inspect", "Reactor returned a different session identity");
    return {
      observedAt: yield* Clock.currentTimeMillis,
      state: value.state,
      hasCapabilities: value.capabilities != null,
      selectedTransport: value.selected_transport ?? null,
      cluster: value.cluster ?? null,
      zone: value.zone ?? null,
      serverVersion: value.server_info?.server_version ?? null,
      additional: additionalFacts(raw),
    };
  });

// A termination probe promises state, not a complete connectable descriptor.
// When it includes identity, that identity must match the requested session.
const TerminalState = Schema.Struct({
  session_id: Schema.optionalKey(Schema.NonEmptyString),
  state: Schema.NonEmptyString,
});
export const decodeTermination = (raw: unknown, sessionId: string) =>
  Schema.decodeUnknownEffect(TerminalState)(raw).pipe(
    Effect.mapError((cause) =>
      failure("terminate", "Reactor returned an invalid termination description", cause),
    ),
    Effect.filterOrFail(
      (value) => value.session_id === undefined || value.session_id === sessionId,
      () => failure("terminate", "Reactor returned a different session identity"),
    ),
  );
