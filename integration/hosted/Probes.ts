/**
 * Free questions to the coordinator about tokens and the API key, each one
 * request that allocates nothing: what a token's lifetime and grant come back
 * as, what `/tokens` replies with, and what a bind or the key does with a
 * session that does not exist. Raw requests, so an answer the SDK would refuse
 * is still recorded. Only statuses, error codes, numbers and key names are
 * kept: never a token, and never provider text.
 */
import * as Clock from "effect/Clock";
import * as Effect from "effect/Effect";
import * as Predicate from "effect/Predicate";
import * as Redacted from "effect/Redacted";
import * as HttpClient from "effect/unstable/http/HttpClient";
import * as HttpClientRequest from "effect/unstable/http/HttpClientRequest";
import * as H3 from "reactor-effect-client/H3";
import type { Probe } from "./Evidence.js";

/** A session id no account holds. */
const unknownSession = "00000000-0000-4000-8000-000000000000";
/** Every probe token expires this soon; none is ever used. */
const probeSeconds = 15;

/** Each leaf under `path`: its path and type, never its value. */
const leaves = (value: unknown, path: string): ReadonlyArray<string> => {
  if (Array.isArray(value))
    return value.length === 0 ? [`${path}[]: empty`] : leaves(value[0], `${path}[]`);
  if (Predicate.isObject(value))
    return Object.entries(value).flatMap(([key, item]) =>
      leaves(item, path === "" ? key : `${path}.${key}`),
    );
  return [`${path}: ${value === null ? "null" : typeof value}`];
};

/** A code short and plain enough to be an identifier rather than provider text. */
const codeOf = (body: unknown): string | undefined => {
  const error = Predicate.isObject(body) ? body.error : undefined;
  const code = Predicate.isObject(error) ? error.code : undefined;
  return Predicate.isString(code) && /^[\w.-]{1,64}$/.test(code) ? code : undefined;
};

const numberOrNull = (value: unknown): number | null =>
  typeof value === "number" && Number.isFinite(value) ? value : null;

/** The grant a `/tokens` reply echoes: counts and caps, never the token. */
const echoOf = (body: unknown): NonNullable<Probe["echo"]> => {
  const details = Predicate.isObject(body) ? body.authorization_details : undefined;
  const entry = Array.isArray(details) ? (details[0] as unknown) : undefined;
  if (!Predicate.isObject(entry)) return { echoed: false };
  const constraints = Predicate.isObject(entry.constraints) ? entry.constraints : {};
  const resources = Predicate.isObject(entry.resources) ? entry.resources : {};
  const sessions = Predicate.isObject(resources.sessions) ? resources.sessions : {};
  return {
    echoed: true,
    maxSessions: numberOrNull(constraints.max_sessions),
    maxSessionSeconds: numberOrNull(constraints.max_session_duration_seconds),
    capStated: "max_session_duration_seconds" in constraints,
    bound: Array.isArray(sessions.bind) ? sessions.bind.length : 0,
  };
};

/** Every probe, in order; each failure to reach the coordinator is recorded as status 0. */
export const run = (input: {
  readonly apiUrl: string;
  readonly apiKey: Redacted.Redacted<string>;
}): Effect.Effect<ReadonlyArray<Probe>, never, HttpClient.HttpClient> =>
  Effect.gen(function* () {
    const client = yield* HttpClient.HttpClient;
    const base = input.apiUrl.replace(/\/$/, "");
    const send = (request: HttpClientRequest.HttpClientRequest) =>
      client.execute(request).pipe(
        Effect.flatMap((response) =>
          response.json.pipe(
            Effect.orElseSucceed(() => undefined),
            Effect.map((body): { readonly status: number; readonly body: unknown } => ({
              status: response.status,
              body,
            })),
          ),
        ),
        Effect.timeout("8 seconds"),
        Effect.orElseSucceed(() => ({ status: 0, body: undefined })),
      );
    const token = (
      name: string,
      authorization: Record<string, unknown>,
      expiresAfter: number,
      options: { readonly shape?: boolean } = {},
    ) =>
      Effect.gen(function* () {
        const sentAt = yield* Clock.currentTimeMillis;
        const reply = yield* send(
          HttpClientRequest.post(`${base}/tokens`).pipe(
            HttpClientRequest.setHeader("reactor-api-key", Redacted.value(input.apiKey)),
            HttpClientRequest.setHeader("reactor-api-version", "1"),
            HttpClientRequest.bodyJsonUnsafe({
              authorization_details: [
                {
                  type: "session",
                  resources: { models: { match: [H3.modelName] } },
                  ...authorization,
                },
              ],
              expires_after: expiresAfter,
            }),
          ),
        );
        const body = reply.body;
        const expiresAt = Predicate.isObject(body) ? numberOrNull(body.expires_at) : null;
        const code = codeOf(body);
        return {
          name,
          status: reply.status,
          requestedSeconds: expiresAfter,
          ...(expiresAt === null ? {} : { lifetimeSeconds: Math.round(expiresAt - sentAt / 1000) }),
          ...(code === undefined ? {} : { code }),
          ...(reply.status === 200 ? { echo: echoOf(body) } : {}),
          ...(options.shape === true && reply.status === 200 ? { shape: leaves(body, "") } : {}),
        } satisfies Probe;
      });
    const withKey = (name: string, method: "GET" | "DELETE") =>
      Effect.gen(function* () {
        const reply = yield* send(
          HttpClientRequest.make(method)(`${base}/sessions/${unknownSession}`).pipe(
            HttpClientRequest.bearerToken(input.apiKey),
            HttpClientRequest.setHeader("reactor-api-version", "1"),
          ),
        );
        const code = codeOf(reply.body);
        return {
          name,
          status: reply.status,
          ...(code === undefined ? {} : { code }),
        } satisfies Probe;
      });
    const capped = { constraints: { max_sessions: 1, max_session_duration_seconds: 50 } };
    return [
      yield* token("a 15 s token", capped, probeSeconds, { shape: true }),
      // Capped at one second, so the long-lived probe token could run nothing.
      yield* token(
        "a 7 h token",
        { constraints: { max_sessions: 1, max_session_duration_seconds: 1 } },
        7 * 3600,
      ),
      yield* token("an uncapped token", { constraints: { max_sessions: 1 } }, probeSeconds),
      yield* token(
        "a token for three sessions",
        { constraints: { max_sessions: 3, max_session_duration_seconds: 50 } },
        probeSeconds,
      ),
      yield* token(
        "a token bound to an unknown session",
        { resources: { models: { match: [H3.modelName] }, sessions: { bind: [unknownSession] } } },
        probeSeconds,
      ),
      yield* withKey("the API key reading an unknown session", "GET"),
      yield* withKey("the API key ending an unknown session", "DELETE"),
    ];
  });

/** Keys whose values may say why a session ended. */
const telling = /reason|termin|end|moderat|close|error|status|state/i;

/**
 * The coordinator's read of a session, raw: its status, top-level key names,
 * its state, and any identifier-like value under a key that may say why it
 * ended (free text only by its length).
 */
export const readSession = (input: {
  readonly apiUrl: string;
  readonly sessionId: string;
  readonly credential: Redacted.Redacted<string>;
}) =>
  Effect.gen(function* () {
    const client = yield* HttpClient.HttpClient;
    const reply = yield* client
      .execute(
        HttpClientRequest.get(
          `${input.apiUrl.replace(/\/$/, "")}/sessions/${encodeURIComponent(input.sessionId)}`,
        ).pipe(
          HttpClientRequest.bearerToken(input.credential),
          HttpClientRequest.setHeader("reactor-api-version", "1"),
        ),
      )
      .pipe(
        Effect.flatMap((response) =>
          Effect.map(
            Effect.orElseSucceed(response.json, () => undefined),
            (body): { readonly status: number; readonly body: unknown } => ({
              status: response.status,
              body,
            }),
          ),
        ),
        Effect.timeout("8 seconds"),
        Effect.orElseSucceed(() => ({ status: 0, body: undefined })),
      );
    const body = Predicate.isObject(reply.body) ? reply.body : {};
    const codes: Record<string, string> = {};
    const note = (key: string, value: unknown) => {
      if (Predicate.isString(value))
        codes[key] = /^[\w.:/-]{1,64}$/.test(value) ? value : `(text, ${value.length} chars)`;
      else if (typeof value === "number" || typeof value === "boolean") codes[key] = String(value);
    };
    for (const [key, value] of Object.entries(body)) {
      if (!telling.test(key)) continue;
      if (Predicate.isObject(value) && !Array.isArray(value))
        for (const [inner, item] of Object.entries(value)) note(`${key}.${inner}`, item);
      else note(key, value);
    }
    const state = Predicate.isString(body.state) ? body.state : undefined;
    return {
      status: reply.status,
      keys: Object.keys(body),
      ...(state === undefined ? {} : { state }),
      ...(Object.keys(codes).length === 0 ? {} : { codes }),
    };
  });
