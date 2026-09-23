import { Clock, Effect, Redacted, Result, Schema } from "effect";
import { parse, parsedInput, positiveLimit, ReactorError } from "../../errors.js";

const failure = (operation: string, message: string, cause?: unknown) =>
  ReactorError.fromCode("Protocol", message, {
    operation,
    outcome: "replied",
    ...(cause === undefined ? {} : { detail: cause }),
  });
const PositiveInteger = Schema.Number.check(
  Schema.isFinite(),
  Schema.isInt(),
  Schema.isGreaterThan(0),
);
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

/** Preserve the integer ratio; callers decide whether the returned price fits their budget. */
export const modelRate = (value: unknown, model: string) =>
  Schema.decodeUnknownEffect(Pricing)(value).pipe(
    Effect.mapError(unknownRate),
    Effect.flatMap((decoded) => {
      const matches = decoded.models.filter((entry) => entry.name === model);
      const rate = matches[0]?.rate;
      return matches.length !== 1 ||
        rate?.unit !== "credits" ||
        rate.denomination !== "second" ||
        !Number.isSafeInteger(rate.amount_per_sec) ||
        rate.amount_per_sec <= 0 ||
        !Number.isSafeInteger(decoded.settings.credits_per_dollar)
        ? Effect.fail(unknownRate())
        : Effect.succeed({
            creditsPerDollar: decoded.settings.credits_per_dollar,
            creditsPerSecond: rate.amount_per_sec,
          });
    }),
  );

export interface TokenOptions {
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
export const validateTokenOptions = (
  input: TokenOptions,
): Effect.Effect<TokenOptions, ReactorError> =>
  parsedInput(() => {
    const reject = (): never => {
      throw ReactorError.fromCode(
        "InvalidInput",
        "A bounded session needs a model, redacted key, positive duration, and token expiry allowing cleanup",
        { operation: "token", outcome: "not-submitted" },
      );
    };
    if (input === null || typeof input !== "object") return reject();
    const options: TokenOptions = {
      apiKey: input.apiKey,
      modelName: input.modelName,
      maxSessionDurationSeconds: input.maxSessionDurationSeconds,
      expiresAfterSeconds: input.expiresAfterSeconds,
    };
    if (
      !Redacted.isRedacted(options.apiKey) ||
      Redacted.value(options.apiKey).length === 0 ||
      typeof options.modelName !== "string" ||
      options.modelName.length === 0 ||
      options.expiresAfterSeconds <= options.maxSessionDurationSeconds + 30
    )
      return reject();
    const bounded = parse(() => {
      positiveLimit(options.maxSessionDurationSeconds, "session duration", 86_400);
      positiveLimit(options.expiresAfterSeconds, "token expiry", Number.MAX_SAFE_INTEGER);
    });
    return Result.isFailure(bounded) ? reject() : Object.freeze(options);
  }, "token");

const Token = Schema.Struct({
  jwt: Schema.NonEmptyString,
  expires_at: Schema.Number.check(Schema.isFinite()),
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
export const grantedLimits = (jwt: string, options: TokenOptions) =>
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
    Effect.mapError(() =>
      failure("token", "Reactor returned an invalid or unbounded session grant"),
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
