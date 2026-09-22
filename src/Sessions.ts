import { Clock, Effect, Exit, Redacted, Schema, Stream } from "effect"
import { Headers, HttpClient, HttpClientError, HttpClientRequest, type HttpClientResponse } from "effect/unstable/http"

const apiUrl = "https://api.reactor.inc"

const originalCauses = new WeakMap<SessionError, unknown>()
export class SessionError extends Schema.TaggedError<SessionError>("reactor-effect-client/SessionError")("SessionError", {
  operation: Schema.Literals(["pricing", "token", "inspect", "terminate"]),
  message: Schema.String,
  status: Schema.optionalKey(Schema.Number),
}) {
  /** Original platform cause for explicit inspection; never serialized into logs. */
  get sourceCause(): unknown { return originalCauses.get(this) }
}

const error = (operation: SessionError["operation"], message: string, status?: number, cause?: unknown) => {
  const failure = new SessionError({ operation, message, ...(status === undefined ? {} : { status }) })
  if (cause !== undefined) originalCauses.set(failure, HttpClientError.isHttpClientError(cause)
    ? ("cause" in cause.reason ? cause.reason.cause : cause.reason._tag) : cause)
  return failure
}

/** Bound actual successful and error response bytes, including bodies without Content-Length. */
const responseText = (response: HttpClientResponse.HttpClientResponse) => Effect.gen(function* () {
  const maximum = 2 * 1024 * 1024
  let total = 0
  const chunks: Array<Uint8Array> = []
  yield* response.stream.pipe(Stream.runForEach((chunk) => Effect.try({
    try: () => {
      total += chunk.byteLength
      if (total > maximum || chunks.length >= 16_384) throw new Error("Coordinator response exceeded its byte/chunk bound")
      chunks.push(chunk)
    }, catch: (cause) => cause,
  })), Effect.catch((cause) => HttpClientError.isHttpClientError(cause) && cause.reason._tag === "EmptyBodyError"
    ? Effect.void : Effect.fail(cause)))
  return yield* Effect.try({ try: () => {
    const bytes = new Uint8Array(total)
    let offset = 0
    for (const chunk of chunks) { bytes.set(chunk, offset); offset += chunk.byteLength }
    return new TextDecoder("utf-8", { fatal: true }).decode(bytes)
  }, catch: (cause) => cause })
})
const request = <A, E>(
  operation: SessionError["operation"],
  input: HttpClientRequest.HttpClientRequest,
  timeoutMillis: number,
  read: (response: HttpClientResponse.HttpClientResponse) => Effect.Effect<A, E>,
) => Effect.gen(function* () {
  const http = HttpClient.withScope(yield* HttpClient.HttpClient)
  return yield* http.execute(input).pipe(Effect.flatMap(read))
}).pipe(
  // The scope includes decoding: receiving headers must not release ownership
  // of a stalled body. Explicit interruption also bounds requests in finalizers.
  Effect.scoped,
  Effect.interruptible,
  Effect.timeout(timeoutMillis),
  // Transport and decode failures can contain credentials or response bodies.
  // Keep transport causes available through sourceCause, excluding request and
  // response wrappers from ordinary serialized errors and automatic spans.
  Effect.mapError((cause) => cause instanceof SessionError ? cause : error(operation,
    operation === "terminate" ? "Remote termination could not be confirmed" : `Reactor ${operation} request or response failed`, undefined, cause)),
  Effect.updateService(Headers.CurrentRedactedNames, (names) => [...names, "reactor-api-key"]),
  // Automatic HTTP spans retain raw transport causes before the mapping above.
  // Keep the named Sessions spans, whose failures have already been sanitized.
  Effect.provideService(HttpClient.TracerDisabledWhen, () => true),
)

const requestJson = (operation: SessionError["operation"], input: HttpClientRequest.HttpClientRequest, timeoutMillis: number) =>
  request(operation, input, timeoutMillis, (response) => Effect.gen(function* () {
    if (response.status < 200 || response.status >= 300) return yield* Effect.fail(error(operation, `Reactor ${operation} request failed with HTTP ${response.status}`, response.status))
    // The HTTP client's JSON accessor treats an empty body as null; Reactor's
    // JSON contract requires an actual JSON document, including for pricing.
    return yield* responseText(response).pipe(Effect.flatMap(Schema.decodeUnknownEffect(Schema.fromJsonString(Schema.Json))))
  }))

/** Economics are returned as facts. Each caller owns its allowed rate and spend. */
export const pricing = Effect.fn("reactor.sessions.pricing")(function* () {
  return yield* requestJson("pricing", HttpClientRequest.get(`${apiUrl}/pricing`), 8_000)
})

const PositiveInteger = Schema.Number.check(Schema.isFinite(), Schema.isInt(), Schema.isGreaterThan(0))
const Pricing = Schema.Struct({
  settings: Schema.Struct({ currency_code: Schema.Literal("USD"), credits_per_dollar: PositiveInteger }),
  models: Schema.Array(Schema.Struct({
    name: Schema.String,
    rate: Schema.Struct({ amount_per_sec: Schema.Number, unit: Schema.String, denomination: Schema.String }),
  })),
})

/** Keep the integer credit ratio so callers can compare a budget without rounding a rate. */
export const modelRate = (value: unknown, model: string) => Effect.try({
  try: () => {
    const decoded = Schema.decodeUnknownSync(Pricing)(value)
    const matches = decoded.models.filter((entry) => entry.name === model)
    const rate = matches[0]?.rate
    if (matches.length !== 1 || rate?.unit !== "credits" || rate.denomination !== "second"
      || !Number.isSafeInteger(rate.amount_per_sec) || rate.amount_per_sec <= 0 || !Number.isSafeInteger(decoded.settings.credits_per_dollar)) {
      throw error("pricing", "Reactor pricing has an unknown model, currency or rate unit")
    }
    return { creditsPerDollar: decoded.settings.credits_per_dollar, creditsPerSecond: rate.amount_per_sec }
  }, catch: (cause) => cause instanceof SessionError ? cause : error("pricing", "Reactor pricing has an unknown model, currency or rate unit", undefined, cause),
})

export interface TokenOptions {
  readonly apiKey: Redacted.Redacted<string>
  readonly modelName: string
  readonly maxSessionDurationSeconds: number
  readonly expiresAfterSeconds: number
}

/** One session, with a server-enforced duration cap separate from JWT expiry. */
export const tokenRequest = (options: Omit<TokenOptions, "apiKey">) => ({
  expires_after: options.expiresAfterSeconds,
  authorization_details: [{
    type: "session",
    resources: { models: { match: [options.modelName] } },
    constraints: { max_sessions: 1, max_session_duration_seconds: options.maxSessionDurationSeconds },
  }],
})

const Token = Schema.Struct({ jwt: Schema.NonEmptyString, expires_at: Schema.Number.check(Schema.isFinite()) })
const TokenClaims = Schema.StringFromBase64Url.pipe(Schema.decodeTo(Schema.fromJsonString(Schema.Struct({ authorization_details: Schema.Unknown }))))
const SessionAuthorization = Schema.Tuple([Schema.Struct({
  type: Schema.Literal("session"),
  resources: Schema.Struct({ models: Schema.Struct({ match: Schema.Tuple([Schema.NonEmptyString]) }) }),
  constraints: Schema.Struct({ max_sessions: Schema.Literal(1), max_session_duration_seconds: PositiveInteger }),
})])

const grantedLimits = (jwt: string, options: TokenOptions) => Effect.gen(function* () {
  // HTTPS authenticates the issuer. Reading its returned grant verifies the
  // requested cap was retained; neither token expiry nor an absent claim caps
  // a session. Unknown authority must not silently become a bounded credential.
  // https://docs.reactor.inc/authentication
  const payload = /^[A-Za-z0-9_-]+\.([A-Za-z0-9_-]+)\.[A-Za-z0-9_-]+$/.exec(jwt)?.[1]
  const claims = yield* Schema.decodeUnknownEffect(TokenClaims)(payload)
  const [authorization] = yield* Schema.decodeUnknownEffect(SessionAuthorization, { onExcessProperty: "error" })(claims.authorization_details)
  const seconds = authorization.constraints.max_session_duration_seconds
  if (authorization.resources.models.match[0] !== options.modelName || seconds > options.maxSessionDurationSeconds) {
    return yield* Effect.fail(error("token", "Reactor returned a token outside the requested session grant"))
  }
  return { maxSessions: authorization.constraints.max_sessions, maxSessionSeconds: seconds }
}).pipe(Effect.mapError(() => error("token", "Reactor returned an invalid or unbounded session grant")))

// https://docs.reactor.inc/authentication
export const mintToken = Effect.fn("reactor.sessions.mintToken")(function* (input: TokenOptions) {
  const options = yield* Effect.try({
    try: () => {
      const options: TokenOptions = { apiKey: input.apiKey, modelName: input.modelName,
        maxSessionDurationSeconds: input.maxSessionDurationSeconds, expiresAfterSeconds: input.expiresAfterSeconds }
      if (!Redacted.isRedacted(options.apiKey) || typeof options.modelName !== "string" || options.modelName.length === 0
        || ![options.maxSessionDurationSeconds, options.expiresAfterSeconds].every((value) => Number.isSafeInteger(value) && value > 0)
        || options.expiresAfterSeconds <= options.maxSessionDurationSeconds + 30) throw new Error("Invalid bounded grant input")
      return Object.freeze(options)
    },
    catch: () => error("token", "A bounded session needs a model, redacted key, positive duration, and token expiry allowing cleanup"),
  })
  const raw = yield* requestJson("token", HttpClientRequest.post(`${apiUrl}/tokens`).pipe(
    HttpClientRequest.setHeader("Reactor-API-Key", Redacted.value(options.apiKey)),
    HttpClientRequest.bodyJsonUnsafe(tokenRequest(options)),
  ), 8_000)
  const value = yield* Schema.decodeUnknownEffect(Token)(raw).pipe(Effect.mapError(() => error("token", "Reactor returned an invalid token response")))
  if (value.expires_at * 1_000 < (yield* Clock.currentTimeMillis) + (options.maxSessionDurationSeconds + 30) * 1_000) {
    return yield* Effect.fail(error("token", "Reactor returned a token without enough lifetime for the bounded session and cleanup"))
  }
  const granted = yield* grantedLimits(value.jwt, options)
  return { jwt: Redacted.make(value.jwt), expiresAt: value.expires_at, granted }
})

const Transport = Schema.Struct({ protocol: Schema.String, version: Schema.String })
export const Inspection = Schema.Struct({
  observedAt: Schema.Number, state: Schema.String,
  hasCapabilities: Schema.Boolean, selectedTransport: Schema.NullOr(Transport),
  // Placement and build, not credentials. A session assigned to a distant zone
  // explains a slow readiness poll or a starved transport that a timeout alone
  // cannot; the coordinator reports both and we were discarding them.
  cluster: Schema.NullOr(Schema.String), zone: Schema.NullOr(Schema.String),
  serverVersion: Schema.NullOr(Schema.String),
  /** Unmodeled top-level scalars, such as a close reason or timestamps. */
  additional: Schema.Record(Schema.String, Schema.Union([Schema.String, Schema.Number, Schema.Boolean, Schema.Null])),
})
const OptionalText = Schema.optionalKey(Schema.NullOr(Schema.String))
const modeledFields = new Set(["session_id", "state", "capabilities", "selected_transport", "cluster", "zone", "server_info"])
const sensitiveField = /secret|token|credential|password|key|jwt|url|auth/i
// The typed projection above once discarded everything else, so a server-side
// close reason would have been invisible. Nested values stay excluded: transport
// descriptions carry ICE credentials, and credential-like names are skipped too.
const additionalFacts = (raw: Schema.Json): Record<string, string | number | boolean | null> => {
  const facts: Record<string, string | number | boolean | null> = {}
  if (typeof raw !== "object" || raw === null || Array.isArray(raw)) return facts
  for (const [key, value] of Object.entries(raw).slice(0, 64)) {
    if (modeledFields.has(key) || sensitiveField.test(key) || (typeof value === "object" && value !== null)) continue
    facts[key] = typeof value === "string" ? value.slice(0, 200) : value
  }
  return facts
}
const SessionDescription = Schema.Struct({
  session_id: Schema.String, state: Schema.String,
  capabilities: Schema.optionalKey(Schema.NullOr(Schema.JsonObject)),
  selected_transport: Schema.optionalKey(Schema.NullOr(Transport)),
  cluster: OptionalText, zone: OptionalText,
  server_info: Schema.optionalKey(Schema.NullOr(Schema.Struct({ server_version: Schema.String }))),
})

/** Capture coordinator state before teardown erases the startup evidence. */
export const inspect = Effect.fn("reactor.sessions.inspect")(function* (jwt: Redacted.Redacted<string>, sessionId: string) {
  if (!Redacted.isRedacted(jwt) || typeof sessionId !== "string" || sessionId.length === 0) return yield* Effect.fail(error("inspect", "Session inspection needs a token and observed session identity"))
  const raw = yield* requestJson("inspect", HttpClientRequest.get(`${apiUrl}/sessions/${encodeURIComponent(sessionId)}`).pipe(
    HttpClientRequest.bearerToken(jwt),
    HttpClientRequest.setHeaders({ "Reactor-API-Version": "1", "Reactor-API-Accept-Version": "1" }),
  ), 1_000)
  const value = yield* Schema.decodeUnknownEffect(SessionDescription)(raw).pipe(Effect.mapError(() => error("inspect", "Reactor returned an invalid session description")))
  if (value.session_id !== sessionId) return yield* Effect.fail(error("inspect", "Reactor returned a different session identity"))
  return {
    observedAt: yield* Clock.currentTimeMillis, state: value.state,
    hasCapabilities: value.capabilities != null, selectedTransport: value.selected_transport ?? null,
    cluster: value.cluster ?? null, zone: value.zone ?? null,
    serverVersion: value.server_info?.server_version ?? null,
    additional: additionalFacts(raw),
  }
})

export interface Termination {
  readonly confirmed: boolean
  /** The DELETE's status, or null when that request produced no response. */
  readonly deleteStatus: number | null
  /** The state the confirming GET observed, or null when the session was absent. */
  readonly state: string | null
}

// A coordinator that hides sessions a token cannot address answers 404 for both
// "terminated" and "never yours". A DELETE explicitly refused on authority is
// the one observation that makes the second reading likelier than the first, so
// it is the only one that withdraws confirmation from an absent session. A lost
// DELETE response still confirms: that is why the follow-up GET exists.
const refusedOnAuthority = (status: number | null) => status === 401 || status === 403

/** Confirm remote termination independently of the native disconnect completion. */
export const terminate = Effect.fn("reactor.sessions.terminate")(function* (jwt: Redacted.Redacted<string>, sessionId: string) {
  if (!Redacted.isRedacted(jwt) || typeof sessionId !== "string" || sessionId.length === 0) return yield* Effect.fail(error("terminate", "Remote termination needs a token and observed session identity"))
  const url = `${apiUrl}/sessions/${encodeURIComponent(sessionId)}`
  // This matches the pinned native coordinator's HTTP contract. The follow-up
  // GET still matters when DELETE loses its response after succeeding remotely.
  // https://github.com/reactor-team/reactor-client-sdks/blob/956eaeaee8e6ced01e581219feff0cd7f4bf3238/crates/reactor-core/src/coordinator.rs
  const headers = { Authorization: `Bearer ${Redacted.value(jwt)}`, "Reactor-API-Version": "1", "Reactor-API-Accept-Version": "1" }
  const deleted = yield* Effect.exit(request("terminate", HttpClientRequest.delete(url, { headers }), 3_000, (response) => Effect.succeed(response.status)))
  const deleteStatus = Exit.isSuccess(deleted) ? deleted.value : null
  const result: Termination = yield* request("terminate", HttpClientRequest.get(url, { headers }), 3_000,
    (response) => Effect.gen(function* () {
      if (response.status === 404) {
        if (refusedOnAuthority(deleteStatus)) return yield* Effect.fail(error("terminate", "Remote termination could not be confirmed", response.status))
        return { confirmed: true, deleteStatus, state: null }
      }
      if (response.status < 200 || response.status >= 300) return yield* Effect.fail(error("terminate", "Remote termination could not be confirmed", response.status))
      // Identity is checked when the coordinator supplies it; requiring it here
      // would make confirmation depend on a field this response never promised.
      const body = yield* responseText(response).pipe(Effect.flatMap(Schema.decodeUnknownEffect(Schema.fromJsonString(
        Schema.Struct({ session_id: Schema.optionalKey(Schema.String), state: Schema.String }),
      ))))
      if (body.session_id !== undefined && body.session_id !== sessionId) return yield* Effect.fail(error("terminate", "Reactor returned a different session identity", response.status))
      return { confirmed: body.state === "INACTIVE" || body.state === "CLOSED", deleteStatus, state: body.state }
    }))
  return result
})
