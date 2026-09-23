import * as Effect from "effect/Effect";
import * as Clock from "effect/Clock";
import * as Redacted from "effect/Redacted";
import * as CoreHttpClient from "effect/unstable/http/HttpClient";
import * as CoreHttpClientError from "effect/unstable/http/HttpClientError";
import * as HttpHeaders from "effect/unstable/http/Headers";
import * as HttpClientRequest from "effect/unstable/http/HttpClientRequest";
import * as HttpBody from "effect/unstable/http/HttpBody";
import * as FetchHttpClient from "effect/unstable/http/FetchHttpClient";
import { errorOf, positiveLimit, ReactorError } from "../../errors.js";
import { array, json, nonempty, record, string, uint32 } from "../../json.js";
import type { Json } from "../../json.js";
import {
  CLIENT_INFO,
  parseAnswer,
  parseConnectionId,
  parseDescriptor,
  parseIce,
  terminal,
} from "../../contract.js";
import type { Descriptor, IceCandidate, IceServer, Mapping } from "../../contract.js";
import { decodeJsonReply, readBody, retryAfterMs } from "./response.js";
import type { HttpReply } from "./response.js";
import {
  decodeInspection,
  decodeToken,
  decodeTermination,
  grantedLimits,
  validateTokenOptions,
} from "./schemas.js";
import type { TokenOptions } from "./schemas.js";

export interface Poll {
  readonly attempts: number;
  readonly initialMs: number;
  readonly maxMs: number;
}
export const SESSION_POLL: Poll = Object.freeze({ attempts: 20, initialMs: 200, maxMs: 10_000 });
export const SDP_POLL: Poll = Object.freeze({ attempts: 6, initialMs: 200, maxMs: 15_000 });
export interface HttpOptions {
  readonly apiUrl: string;
  readonly local?: boolean;
  /** Evaluated for each authenticated request, including signaling for local sessions. */
  readonly credential?: Effect.Effect<string | undefined, ReactorError>;
  readonly requestTimeoutMs?: number;
  readonly maxResponseBytes?: number;
  readonly maxResponseChunks?: number;
  readonly sessionPoll?: Poll;
  readonly sdpPoll?: Poll;
}
interface Request {
  readonly url: string;
  readonly operation: string;
  readonly method?: "GET" | "POST" | "PUT" | "DELETE";
  readonly auth?: "coordinator" | "signaling" | "same-origin" | "none";
  readonly body?: string | Uint8Array<ArrayBuffer>;
  readonly contentType?: string;
  readonly headers?: Readonly<Record<string, string>>;
  readonly accepted?: readonly number[];
  readonly maxBytes?: number;
  readonly timeoutMs?: number;
}
export interface UploadAllocation {
  readonly presigned_id: string;
  readonly presigned_url: string;
  readonly path: string;
}
export interface Termination {
  readonly attempted: boolean;
  readonly responseReceived: boolean;
  readonly confirmed: boolean;
  readonly evidence: "absent" | "terminal" | null;
  /** DELETE/stop status; null means no response headers were received. */
  readonly deleteStatus: number | null;
  /** State from the independent confirmation, when it was valid. */
  readonly state: string | null;
  readonly error?: ReactorError;
}
const pure = <A>(f: () => A): Effect.Effect<A, ReactorError> =>
  Effect.try({ try: f, catch: errorOf });
const validatePoll = (p: Poll): Poll => {
  positiveLimit(p.attempts, "poll attempts", 1000);
  positiveLimit(p.initialMs, "poll initialMs", 60_000);
  positiveLimit(p.maxMs, "poll maxMs", 60_000);
  if (p.initialMs > p.maxMs)
    throw new ReactorError({ code: "Protocol", message: "poll initial delay exceeds maximum" });
  return Object.freeze({ ...p });
};
const checkedUrl = (url: string): URL => {
  const value = new URL(url);
  if (
    (value.protocol !== "https:" && value.protocol !== "http:") ||
    value.username ||
    value.password
  )
    throw new ReactorError({
      code: "Protocol",
      message: "HTTP URL must use http(s) and contain no embedded credentials",
    });
  return value;
};
/** Reactor HTTP protocol over the application-supplied Effect HTTP client. */
export class CoordinatorClient {
  readonly apiUrl: string;
  readonly local: boolean;
  readonly sessionPoll: Poll;
  readonly sdpPoll: Poll;
  private readonly timeoutMs: number;
  private readonly maxBytes: number;
  private readonly maxChunks: number;
  readonly options: HttpOptions;
  constructor(
    options: HttpOptions,
    private readonly client: CoreHttpClient.HttpClient,
  ) {
    this.options = Object.freeze({ ...options });
    const base = checkedUrl(options.apiUrl);
    if (base.search || base.hash)
      throw new ReactorError({
        code: "Protocol",
        message: "apiUrl cannot contain a query or fragment",
      });
    this.apiUrl = base.href.replace(/\/$/, "");
    this.local = options.local ?? false;
    this.timeoutMs = positiveLimit(options.requestTimeoutMs ?? 15_000, "request timeout", 600_000);
    this.maxBytes = positiveLimit(
      options.maxResponseBytes ?? 2_097_152,
      "HTTP response bound",
      64 * 1024 * 1024,
    );
    this.maxChunks = positiveLimit(
      options.maxResponseChunks ?? 16_384,
      "HTTP response chunk bound",
      1_048_576,
    );
    this.sessionPoll = validatePoll(options.sessionPoll ?? SESSION_POLL);
    this.sdpPoll = validatePoll(options.sdpPoll ?? SDP_POLL);
  }
  path(path: string): string {
    return `${this.apiUrl}${path}`;
  }
  sessionPath(id: string): string {
    return this.path(`/sessions/${encodeURIComponent(nonempty(id, "session id"))}`);
  }
  transportPath(id: string): string {
    return `${this.sessionPath(id)}/transport/webrtc`;
  }

  request(request: Request): Effect.Effect<HttpReply, ReactorError> {
    const self = this;
    return Effect.suspend(() => {
      // Crossing execute is the conservative boundary at which a mutation may
      // have reached the server. A transport failure cannot prove non-delivery.
      let submitted = false;
      let status: number | undefined;
      const operation = request.operation;
      const networkError = (cause: unknown): ReactorError => {
        if (cause instanceof ReactorError)
          return new ReactorError({
            code: cause.code,
            message: cause.message,
            context: {
              operation,
              outcome: submitted ? "unknown" : "not-submitted",
              ...(status === undefined ? {} : { status }),
              ...cause.context,
            },
          });
        const detail = CoreHttpClientError.isHttpClientError(cause)
          ? "cause" in cause.reason
            ? cause.reason.cause
            : cause.reason._tag
          : cause;
        return new ReactorError({
          code: "Http",
          message: `${operation}: network/read failure`,
          context: {
            operation,
            detail,
            outcome: submitted ? "unknown" : "not-submitted",
            ...(status === undefined ? {} : { status }),
          },
        });
      };
      const action = Effect.gen(function* () {
        const url = yield* pure(() => checkedUrl(request.url));
        const bound = yield* pure(() =>
          positiveLimit(request.maxBytes ?? self.maxBytes, "HTTP body limit", 64 * 1024 * 1024),
        );
        const headers = yield* pure(() => new Headers(request.headers));
        const auth = request.auth ?? "coordinator";
        if (auth === "coordinator" || auth === "signaling") {
          headers.set("Reactor-API-Version", "1");
          headers.set("Reactor-API-Accept-Version", "1");
          if (auth === "signaling") headers.set("Reactor-WebRTC-Version", "1.0");
        }
        const authenticate =
          auth === "signaling" ||
          (auth === "coordinator" && !self.local) ||
          (auth === "same-origin" && url.origin === new URL(self.apiUrl).origin);
        if (authenticate) {
          const token = yield* self.options.credential ?? Effect.succeed(undefined);
          if (token !== undefined)
            yield* pure(() => headers.set("Authorization", `Bearer ${nonempty(token, "JWT")}`));
        }
        if (request.contentType !== undefined)
          yield* pure(() => headers.set("Content-Type", request.contentType!));
        const outgoing = yield* pure(() =>
          HttpClientRequest.make(request.method ?? "GET")(url.href, {
            ...(request.body === undefined ? {} : { body: HttpBody.raw(request.body) }),
          }).pipe(HttpClientRequest.setHeaders(headers)),
        );
        submitted = true;
        const response = yield* CoreHttpClient.withScope(self.client)
          .execute(outgoing)
          .pipe(Effect.mapError(networkError));
        status = response.status;
        const bytes = yield* readBody(response, operation, bound, self.maxChunks).pipe(
          Effect.mapError(networkError),
        );
        const reply: HttpReply = {
          status: response.status,
          headers: new Headers(response.headers),
          bytes,
        };
        if (
          (response.status >= 200 && response.status < 300) ||
          request.accepted?.includes(response.status)
        )
          return reply;
        const retry = retryAfterMs(reply.headers);
        return yield* Effect.fail(
          new ReactorError({
            code: response.status === 426 || response.status === 501 ? "VersionMismatch" : "Http",
            message: `${operation}: HTTP ${response.status}`,
            context: {
              operation,
              status: response.status,
              body: new TextDecoder().decode(bytes),
              outcome: "replied",
              ...(retry === undefined ? {} : { retryAfterMs: retry }),
            },
          }),
        );
      });
      return action.pipe(
        Effect.mapError(networkError),
        Effect.scoped,
        Effect.interruptible,
        Effect.provideService(FetchHttpClient.RequestInit, {
          credentials: "omit",
          redirect: "error",
        }),
        Effect.provideService(CoreHttpClient.TracerDisabledWhen, () => true),
        Effect.updateService(HttpHeaders.CurrentRedactedNames, (names) => [
          ...names,
          "reactor-api-key",
        ]),
        Effect.timeoutOrElse({
          duration: request.timeoutMs ?? self.timeoutMs,
          orElse: () =>
            Effect.fail(
              new ReactorError({
                code: "Timeout",
                message: `${operation}: deadline during request or response read`,
                context: {
                  operation,
                  outcome: submitted ? "unknown" : "not-submitted",
                  ...(status === undefined ? {} : { status }),
                },
              }),
            ),
        }),
      );
    });
  }
  private jsonRequest(
    request: Omit<Request, "body" | "contentType">,
    body?: Json,
  ): Effect.Effect<unknown, ReactorError> {
    return pure(() => ({
      ...request,
      ...(body === undefined
        ? {}
        : { body: JSON.stringify(body), contentType: "application/json" }),
    })).pipe(
      Effect.flatMap((input) => this.request(input)),
      Effect.flatMap((reply) => pure(() => decodeJsonReply(reply, request.operation))),
    );
  }
  create(
    model: { readonly name: string; readonly version?: string },
    extraArgs?: Json,
  ): Effect.Effect<Descriptor, ReactorError> {
    return pure(() =>
      json(
        this.local
          ? { ...(extraArgs === undefined ? {} : { extra_args: extraArgs }) }
          : {
              model: {
                name: nonempty(model.name, "model name"),
                ...(model.version === undefined ? {} : { version: model.version }),
              },
              client_info: CLIENT_INFO,
              supported_transports: [{ protocol: "webrtc", version: "1.0" }],
              ...(extraArgs === undefined ? {} : { extra_args: extraArgs }),
            },
      ),
    ).pipe(
      Effect.flatMap((body) =>
        this.jsonRequest(
          {
            operation: "create session",
            method: "POST",
            url: this.path(this.local ? "/start_session" : "/sessions"),
          },
          body,
        ),
      ),
      Effect.flatMap((raw) => pure(() => parseDescriptor(raw))),
    );
  }
  read(id: string): Effect.Effect<Descriptor, ReactorError> {
    return pure(() => (this.local ? this.path("/session") : this.sessionPath(id))).pipe(
      Effect.flatMap((url) => this.jsonRequest({ operation: "read session", url })),
      Effect.flatMap((raw) =>
        pure(() => {
          const descriptor = parseDescriptor(raw);
          if (descriptor.session_id !== id)
            throw new ReactorError({
              code: "Protocol",
              message: "session descriptor identity mismatch",
            });
          return descriptor;
        }),
      ),
    );
  }
  ready(id: string, initial?: Descriptor): Effect.Effect<Descriptor, ReactorError> {
    const self = this;
    return Effect.gen(function* () {
      for (let attempt = 0; attempt < self.sessionPoll.attempts; attempt++) {
        const descriptor = attempt === 0 && initial !== undefined ? initial : yield* self.read(id);
        if (descriptor.session_id !== id)
          return yield* Effect.fail(
            new ReactorError({ code: "Protocol", message: "ready descriptor id mismatch" }),
          );
        if (terminal(descriptor.state))
          return yield* Effect.fail(
            new ReactorError({
              code: "TerminalSession",
              message: descriptor.state,
              context: { sessionId: id },
            }),
          );
        if (descriptor.capabilities !== undefined && descriptor.selected_transport !== undefined)
          return descriptor;
        if (attempt + 1 < self.sessionPoll.attempts)
          yield* Effect.sleep(
            Math.min(self.sessionPoll.initialMs * 2 ** attempt, self.sessionPoll.maxMs),
          );
      }
      return yield* Effect.fail(
        new ReactorError({
          code: "Timeout",
          message: "session capabilities/transport not ready",
          context: { sessionId: id },
        }),
      );
    });
  }
  iceServers(id: string): Effect.Effect<IceServer[], ReactorError> {
    return pure(() => `${this.transportPath(id)}/ice_servers`).pipe(
      Effect.flatMap((url) =>
        this.jsonRequest({ operation: "ICE servers", url, auth: "signaling" }),
      ),
      Effect.flatMap((raw) => pure(() => parseIce(raw))),
    );
  }
  register(id: string): Effect.Effect<number, ReactorError> {
    return pure(() => `${this.transportPath(id)}/connections`).pipe(
      Effect.flatMap((url) =>
        this.jsonRequest(
          { operation: "register connection", method: "POST", url, auth: "signaling" },
          {},
        ),
      ),
      Effect.flatMap((raw) => pure(() => parseConnectionId(raw))),
    );
  }
  offer(
    id: string,
    cid: number,
    offer: string,
    mapping: readonly Mapping[],
    replace: boolean,
  ): Effect.Effect<void, ReactorError> {
    return pure(() => ({
      url: `${this.transportPath(id)}/connections/${uint32(cid, "connection id")}/sdp_params`,
      body: JSON.stringify(
        json({
          sdp_offer: nonempty(offer, "SDP offer"),
          client_info: CLIENT_INFO,
          track_mapping: mapping,
        }),
      ),
    })).pipe(
      Effect.flatMap(({ url, body }) =>
        this.request({
          operation: "SDP offer",
          method: replace ? "PUT" : "POST",
          auth: "signaling",
          url,
          contentType: "application/json",
          body,
        }),
      ),
      Effect.asVoid,
    );
  }
  answer(id: string, cid: number): Effect.Effect<ReturnType<typeof parseAnswer>, ReactorError> {
    const self = this;
    return Effect.gen(function* () {
      const url = yield* pure(
        () => `${self.transportPath(id)}/connections/${uint32(cid, "connection id")}/sdp_params`,
      );
      for (let attempt = 0; attempt < self.sdpPoll.attempts; attempt++) {
        const reply = yield* self.request({ operation: "SDP answer", auth: "signaling", url });
        if (reply.status !== 202) return yield* pure(() => parseAnswer(decodeJsonReply(reply)));
        if (attempt + 1 < self.sdpPoll.attempts)
          yield* Effect.sleep(Math.min(self.sdpPoll.initialMs * 2 ** attempt, self.sdpPoll.maxMs));
      }
      return yield* Effect.fail(
        new ReactorError({
          code: "Timeout",
          message: "SDP answer not ready",
          context: { sessionId: id },
        }),
      );
    });
  }
  ice(
    id: string,
    cid: number,
    candidates: readonly IceCandidate[],
    isFinal: boolean,
  ): Effect.Effect<void, ReactorError> {
    return pure(() => ({
      url: `${this.transportPath(id)}/connections/${uint32(cid, "connection id")}/ice_candidates`,
      body: JSON.stringify(json({ candidates, is_final: isFinal, client_info: CLIENT_INFO })),
    })).pipe(
      Effect.flatMap(({ url, body }) =>
        this.request({
          operation: "ICE candidates",
          method: "POST",
          auth: "signaling",
          contentType: "application/json",
          url,
          body,
        }),
      ),
      Effect.asVoid,
    );
  }
  allocateUpload(
    id: string,
    name: string,
    mimeType: string,
    size: number,
  ): Effect.Effect<UploadAllocation, ReactorError> {
    return pure(() => ({
      url: `${this.sessionPath(id)}/uploads`,
      body: {
        name: nonempty(name, "upload name"),
        mime_type: nonempty(mimeType, "MIME type"),
        size: positiveLimit(size, "upload size"),
      },
    })).pipe(
      Effect.flatMap(({ url, body }) =>
        this.jsonRequest({ operation: "allocate upload", method: "POST", url }, body),
      ),
      Effect.flatMap((raw) =>
        pure(() => {
          const allocation = record(raw, "upload allocation");
          const url = nonempty(allocation.presigned_url, "presigned_url");
          checkedUrl(url);
          return {
            presigned_id: nonempty(allocation.presigned_id, "presigned_id"),
            presigned_url: url,
            path: string(allocation.path, "upload path"),
          };
        }),
      ),
    );
  }
  putUpload(
    allocation: UploadAllocation,
    bytes: Uint8Array<ArrayBuffer>,
    mimeType: string,
  ): Effect.Effect<void, ReactorError> {
    return pure(() => ({
      url: checkedUrl(allocation.presigned_url).href,
      contentType: nonempty(mimeType, "MIME type"),
    })).pipe(
      Effect.flatMap(({ url, contentType }) =>
        this.request({
          operation: "transfer upload",
          method: "PUT",
          auth: "none",
          url,
          body: bytes,
          contentType,
        }),
      ),
      Effect.asVoid,
    );
  }
  /** Both scoped sessions and external supervisors use this exact evidence algorithm. */
  terminate(id: string): Effect.Effect<Termination> {
    const self = this;
    return Effect.gen(function* () {
      const paths = yield* Effect.result(
        pure(() => {
          const session = self.sessionPath(id);
          return {
            remove: self.local ? self.path("/stop_session") : session,
            read: self.local ? self.path("/session") : session,
          };
        }),
      );
      if (paths._tag === "Failure")
        return {
          attempted: false,
          responseReceived: false,
          confirmed: false,
          evidence: null,
          deleteStatus: null,
          state: null,
          error: paths.failure,
        };
      const timeoutMs = self.options.requestTimeoutMs ?? 3_000;
      const response = yield* Effect.result(
        self.request({
          operation: "terminate",
          method: self.local ? "POST" : "DELETE",
          url: paths.success.remove,
          accepted: [404],
          timeoutMs,
        }),
      );
      const deleteStatus =
        (response._tag === "Success" ? response.success.status : response.failure.context.status) ??
        null;
      const base = {
        attempted:
          response._tag === "Success" || response.failure.context.outcome !== "not-submitted",
        responseReceived: deleteStatus !== null,
        deleteStatus,
      };
      // A response to DELETE alone is never proof. The GET also runs after a
      // timeout, read failure or refusal, and uses the same bounded body reader.
      const confirmation = yield* Effect.result(
        self.request({
          operation: "terminate",
          url: paths.success.read,
          accepted: [404],
          timeoutMs,
        }),
      );
      if (confirmation._tag === "Failure") {
        return {
          ...base,
          confirmed: false,
          evidence: null,
          state: null,
          error: confirmation.failure,
        };
      }
      if (confirmation.success.status === 404) {
        if (deleteStatus === 401 || deleteStatus === 403)
          return {
            ...base,
            confirmed: false,
            evidence: null,
            state: null,
            error: new ReactorError({
              code: "Http",
              message: "Remote termination could not be confirmed after authority refusal",
              context: { operation: "terminate", status: deleteStatus, outcome: "replied" },
            }),
          };
        return { ...base, confirmed: true, evidence: "absent" as const, state: null };
      }
      const decoded = yield* Effect.result(
        pure(() => decodeJsonReply(confirmation.success, "terminate")).pipe(
          Effect.flatMap((raw) => decodeTermination(raw, id)),
        ),
      );
      if (decoded._tag === "Failure")
        return { ...base, confirmed: false, evidence: null, state: null, error: decoded.failure };
      const state = decoded.success.state;
      return {
        ...base,
        confirmed: terminal(state),
        evidence: terminal(state) ? ("terminal" as const) : null,
        state,
        ...(!terminal(state) && response._tag === "Failure" ? { error: response.failure } : {}),
      };
    }).pipe(Effect.withSpan("reactor.coordinator.terminate"));
  }
  /** Pricing remains provider JSON; modelRate interprets only known rate units. */
  pricing(): Effect.Effect<Json, ReactorError> {
    return this.jsonRequest({
      operation: "pricing",
      url: this.path("/pricing"),
      auth: "none",
      timeoutMs: this.options.requestTimeoutMs ?? 8_000,
    }).pipe(
      Effect.flatMap((raw) => pure(() => json(raw))),
      Effect.withSpan("reactor.coordinator.pricing"),
    );
  }
  mintToken(input: TokenOptions) {
    const self = this;
    return Effect.gen(function* () {
      const options = yield* validateTokenOptions(input);
      const raw = yield* self.issueToken(
        Redacted.value(options.apiKey),
        tokenBody({
          models: [options.modelName],
          max_sessions: 1,
          max_session_duration_seconds: options.maxSessionDurationSeconds,
          expires_after: BigInt(options.expiresAfterSeconds),
        }),
      );
      const value = yield* decodeToken(raw);
      if (
        value.expires_at * 1_000 <
        (yield* Clock.currentTimeMillis) + (options.maxSessionDurationSeconds + 30) * 1_000
      )
        return yield* Effect.fail(
          new ReactorError({
            code: "Protocol",
            message:
              "Reactor returned a token without enough lifetime for the bounded session and cleanup",
            context: { operation: "token", outcome: "replied" },
          }),
        );
      const granted = yield* grantedLimits(value.jwt, options);
      return { jwt: Redacted.make(value.jwt), expiresAt: value.expires_at, granted };
    }).pipe(Effect.withSpan("reactor.coordinator.mintToken"));
  }
  inspect(id: string) {
    return pure(() =>
      this.local ? (nonempty(id, "session id"), this.path("/session")) : this.sessionPath(id),
    ).pipe(
      Effect.flatMap((url) =>
        this.jsonRequest({
          operation: "inspect",
          url,
          timeoutMs: this.options.requestTimeoutMs ?? 1_000,
        }),
      ),
      Effect.flatMap((raw) => decodeInspection(raw, id)),
      Effect.mapError(
        (error) =>
          new ReactorError({
            code: error.code,
            message: error.message,
            context: { operation: "inspect", ...error.context },
          }),
      ),
      Effect.withSpan("reactor.coordinator.inspect"),
    );
  }
  private issueToken(apiKey: string, body: string) {
    return this.request({
      operation: "token",
      method: "POST",
      url: this.path("/tokens"),
      auth: "none",
      contentType: "application/json",
      headers: { "Reactor-API-Key": apiKey },
      body,
      timeoutMs: this.options.requestTimeoutMs ?? 8_000,
    }).pipe(Effect.flatMap((reply) => pure(() => decodeJsonReply(reply, "token"))));
  }
  /** Source auth.rs semantics; model restrictions make a token session-scoped. No retries. */
  exchangeKey(
    apiKey: string,
    constraints: TokenConstraints = {},
  ): Effect.Effect<string, ReactorError> {
    return pure(() => tokenBody(constraints)).pipe(
      Effect.flatMap((body) => this.issueToken(apiKey, body)),
      Effect.flatMap((raw) => pure(() => nonempty(record(raw).jwt, "jwt"))),
    );
  }
}
export interface TokenConstraints {
  readonly models?: readonly string[];
  readonly max_sessions?: number;
  readonly max_session_duration_seconds?: number;
  readonly expires_after?: bigint;
}
export const tokenBody = (input: TokenConstraints): string => {
  const c = record(input),
    keys = new Set(["models", "max_sessions", "max_session_duration_seconds", "expires_after"]);
  for (const key of Object.keys(c))
    if (!keys.has(key))
      throw new ReactorError({ code: "Protocol", message: `unknown token constraint: ${key}` });
  const parts: string[] = [];
  if (c.models !== undefined) {
    const models = array(c.models, "models").map((m) => nonempty(m, "model"));
    const limits = {
      ...(c.max_sessions === undefined
        ? {}
        : { max_sessions: uint32(c.max_sessions, "max_sessions") }),
      ...(c.max_session_duration_seconds === undefined
        ? {}
        : {
            max_session_duration_seconds: positiveLimit(
              uint32(c.max_session_duration_seconds, "max_session_duration_seconds"),
              "session duration",
              86_400,
            ),
          }),
    };
    parts.push(
      `"authorization_details":${JSON.stringify([{ type: "session", resources: { models: { match: models } }, ...(Object.keys(limits).length ? { constraints: limits } : {}) }])}`,
    );
  }
  if (c.expires_after !== undefined) {
    if (
      typeof c.expires_after !== "bigint" ||
      c.expires_after < 0n ||
      c.expires_after > 0xffffffffffffffffn
    )
      throw new ReactorError({
        code: "Protocol",
        message: "expires_after must be a uint64 bigint",
      });
    parts.push(`"expires_after":${c.expires_after}`);
  }
  return parts.length ? `{${parts.join(",")}}` : "null";
};

/** Construct the protocol adapter from a supplied platform client. No I/O runs here. */
export const make = (
  options: HttpOptions,
): Effect.Effect<CoordinatorClient, ReactorError, CoreHttpClient.HttpClient> =>
  Effect.flatMap(CoreHttpClient.HttpClient, (client) =>
    pure(() => new CoordinatorClient(options, client)),
  );
