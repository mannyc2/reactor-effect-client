/**
 * Reactor's HTTP API: pricing, tokens, inspection, termination, recordings,
 * and the signaling a session runs. Built over the application's
 * `HttpClient`; constructing it makes no request.
 */
import * as Arr from "effect/Array";
import * as Clock from "effect/Clock";
import * as Config from "effect/Config";
import * as Context from "effect/Context";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Predicate from "effect/Predicate";
import * as Redacted from "effect/Redacted";
import * as Ref from "effect/Ref";
import * as Schedule from "effect/Schedule";
import * as Schema from "effect/Schema";
import * as SchemaTransformation from "effect/SchemaTransformation";
import * as Stream from "effect/Stream";
import * as FetchHttpClient from "effect/http/FetchHttpClient";
import * as Headers from "effect/http/Headers";
import * as HttpClient from "effect/http/HttpClient";
import type * as HttpClientError from "effect/http/HttpClientError";
import * as HttpClientRequest from "effect/http/HttpClientRequest";
import type * as HttpClientResponse from "effect/http/HttpClientResponse";
import * as Recording from "./internal/recording.js";
import { IceCandidate, IceServer, Mapping, Track } from "./Peer.js";
import type { ClipReady } from "./Session.js";
import { FailureSummary, Http, ReactorError, summarize } from "./ReactorError.js";

export const defaultApiUrl = "https://api.reactor.inc";

const clientInfo = { sdk_version: "0.9.2", sdk_type: "typescript-effect-independent" } as const;

/** A field the provider sends as `null` or omits for the same fact decodes as absent. */
const NullAsAbsent = <S extends Schema.Top>(schema: S) =>
  schema.pipe(
    Schema.NullOr,
    Schema.optionalKey,
    Schema.decodeTo(
      Schema.toType(schema).pipe(Schema.optionalKey),
      SchemaTransformation.transformOptional({
        decode: Option.filter(Predicate.isNotNull),
        encode: (value) => value,
      }),
    ),
  );
const Uint32 = Schema.Int.check(Schema.isBetween({ minimum: 0, maximum: 0xffffffff }));
const PositiveInt = Schema.Int.check(Schema.isGreaterThan(0));

export const Capabilities = Schema.Struct({
  protocol_version: Schema.NonEmptyString,
  tracks: Schema.Array(Track).check(
    Schema.isMaxLength(64),
    Schema.makeFilter((tracks) =>
      new Set(tracks.map((track) => track.name)).size === tracks.length
        ? undefined
        : "duplicate track names are ambiguous for named track operations",
    ),
  ),
  commands: Schema.Struct({
    name: Schema.NonEmptyString,
    description: NullAsAbsent(Schema.String),
    schema: Schema.optionalKey(Schema.Json),
  }).pipe(Schema.Array, NullAsAbsent),
  emission_fps: NullAsAbsent(Schema.Finite),
});
export type Capabilities = typeof Capabilities.Type;

export const Transport = Schema.Struct({
  protocol: Schema.NonEmptyString,
  version: Schema.NonEmptyString,
});

/** A session as the coordinator describes it; unknown states stay as they are. */
export const Descriptor = Schema.Struct({
  session_id: Schema.NonEmptyString,
  state: Schema.NonEmptyString,
  capabilities: NullAsAbsent(Capabilities),
  selected_transport: NullAsAbsent(Transport),
});
export type Descriptor = typeof Descriptor.Type;

/**
 * Whether a session state is final. Only `CLOSED` is: hosted Reactor reads
 * `INACTIVE` for a session whose last connection dropped, and that session still
 * runs (and bills) for the documented 30 s reconnect window. A paid run adopted
 * one 9 s after its owner was killed, then ended it with a DELETE that
 * answered 200 and moved it to `CLOSED`.
 */
export const isTerminal = (state: string): boolean => state === "CLOSED";

/** The coordinator's ICE server list, as it sends it. */
export const IceServersReply = Schema.Struct({
  ice_servers: Schema.Array(
    Schema.Struct({
      uris: Schema.Array(Schema.NonEmptyString),
      credentials: NullAsAbsent(
        Schema.Struct({ username: Schema.String, password: Schema.String }),
      ),
    }),
  ).check(Schema.isMaxLength(64)),
});

const Allocated = Schema.Struct({ session_id: Schema.NonEmptyString });
export const Registered = Schema.Struct({ connection_id: Uint32 });
export const SdpAnswer = Schema.Struct({
  sdp_answer: Schema.NonEmptyString,
  connection_id: NullAsAbsent(Uint32),
});
export type SdpAnswer = typeof SdpAnswer.Type;
export const UploadSlot = Schema.Struct({
  presigned_id: Schema.NonEmptyString,
  presigned_url: Schema.NonEmptyString,
  path: Schema.String,
});
export type UploadSlot = typeof UploadSlot.Type;

/** A create reply whose session id is valid; the rest is decoded by `describe`. */
export interface Allocation {
  readonly sessionId: string;
  readonly reply: unknown;
}

/**
 * A remote termination verdict. A DELETE response alone is never proof: an
 * independent read confirms the session is gone or terminal.
 */
export const Termination = Schema.Struct({
  attempted: Schema.Boolean,
  responseReceived: Schema.Boolean,
  confirmed: Schema.Boolean,
  evidence: Schema.NullOr(Schema.Literals(["absent", "terminal"])),
  /** DELETE status; null means no response arrived. */
  deleteStatus: Schema.NullOr(Schema.Int),
  /** State from the independent confirmation, when it was valid. */
  state: Schema.NullOr(Schema.String),
  error: Schema.optionalKey(FailureSummary),
});
export type Termination = typeof Termination.Type;

export const notTerminated: Termination = {
  attempted: false,
  responseReceived: false,
  confirmed: false,
  evidence: null,
  deleteStatus: null,
  state: null,
};

/** A termination verdict as span attributes: a successful terminate says nothing about billing. */
export const terminationAttributes = (termination: Termination): Record<string, unknown> => ({
  "reactor.termination.attempted": termination.attempted,
  "reactor.termination.confirmed": termination.confirmed,
  ...(termination.evidence === null
    ? {}
    : { "reactor.termination.evidence": termination.evidence }),
});

const Pricing = Schema.Struct({
  settings: Schema.Struct({
    currency_code: Schema.optional(Schema.Literal("USD")),
    credits_per_dollar: PositiveInt,
  }),
  models: Schema.Unknown.pipe(Schema.Array, Schema.optional),
});
const PricedModel = Schema.Struct({ name: Schema.String, rate: Schema.Unknown });
const isPricedModel = Schema.is(PricedModel);
const CreditsRate = Schema.Union([
  Schema.Struct({
    amount_per_sec: PositiveInt,
    unit: Schema.Literal("credits"),
    denomination: Schema.Literal("second"),
  }),
  Schema.Struct({
    amount_per_min: PositiveInt,
    unit: Schema.Literal("credits"),
    denomination: Schema.Literal("minute"),
  }),
]);

/**
 * A model's price. Reactor's billing page says it bills by the session-minute
 * and gives rates per minute, while its pricing API states some models' rates,
 * H3's among them, per second, and its dashboard listed paid sessions to the
 * second (a 10 s session as 10 s on September 29, 2026). `per` is the unit
 * the pricing states, and callers decide how time rounds.
 */
export interface Rate {
  readonly creditsPerDollar: number;
  readonly creditsPerSecond: number;
  readonly per: "second" | "minute";
}

/**
 * The rate of `model` in `pricing`, by its connect slug or bare name: the
 * catalog lists `h3-reference-to-video-turbo-realtime` where sessions take
 * `reactor/h3-reference-to-video-turbo-realtime`. Only the settings are read
 * strictly, so an entry this client cannot price never stops it pricing another.
 */
export const modelRate = Effect.fnUntraced(
  function* (pricing: unknown, model: string) {
    const bare = model.slice(model.lastIndexOf("/") + 1);
    const decoded = yield* Schema.decodeUnknownEffect(Pricing)(pricing);
    const matches = (decoded.models ?? [])
      .filter(isPricedModel)
      .filter((entry) => entry.name === model || entry.name === bare);
    const rate = yield* Schema.decodeUnknownEffect(CreditsRate)(
      matches.length === 1 ? matches[0]?.rate : undefined,
    );
    return {
      creditsPerDollar: decoded.settings.credits_per_dollar,
      creditsPerSecond: "amount_per_sec" in rate ? rate.amount_per_sec : rate.amount_per_min / 60,
      per: rate.denomination,
    } satisfies Rate;
  },
  Effect.mapError((cause) =>
    ReactorError.fromCode(
      "Protocol",
      "Reactor pricing has an unknown model, currency or rate unit",
      {
        operation: "pricing",
        outcome: "replied",
        detail: cause,
      },
    ),
  ),
);

export interface TokenOptions {
  /** The API key; the service's configured key when omitted. */
  readonly apiKey?: Redacted.Redacted<string> | undefined;
  readonly modelName: string;
  /**
   * Caps each session the token creates, from one second to a day, or
   * `"unlimited"` for Reactor's default of no cap. An uncapped session bills
   * until something terminates it, so it is never a default here. A token that
   * creates no session, one only `bind`s, needs none.
   */
  readonly maxSessionDuration?: Duration.Input | "unlimited" | undefined;
  /**
   * Sessions the token may create, 1 to 500; 1 by default. With `bind`,
   * Reactor counts the bound sessions in it and by default leaves no room to create.
   */
  readonly maxSessions?: number | undefined;
  /** Open sessions of this account the token acts on besides those it creates. */
  readonly bind?: ReadonlyArray<string> | undefined;
  /**
   * How long the token stays valid; Reactor's default of an hour when omitted.
   * Reactor clamps it to six hours without saying so: read `expiresAt`.
   */
  readonly expiresAfter?: Duration.Input | undefined;
}

/** What Reactor says a token grants, from its echo of the request; undefined where it says nothing. */
export interface Granted {
  readonly models: ReadonlyArray<string>;
  readonly maxSessions: number | undefined;
  readonly maxSessionSeconds: number | "unlimited" | undefined;
  /** Open sessions the token acts on besides those it creates. */
  readonly bound: ReadonlyArray<string>;
}

export interface TokenGrant {
  readonly jwt: Redacted.Redacted<string>;
  /** When the token expires, in seconds since the epoch, as Reactor set it. */
  readonly expiresAt: number;
  /**
   * The cap on each session the token creates, in seconds: as asked, or the
   * narrower cap Reactor's echo states; undefined for none.
   */
  readonly maxSessionSeconds: number | undefined;
  /** Reactor's echo of the grant, when its reply carries one; a grant wider than asked is refused. */
  readonly granted?: Granted | undefined;
}

/**
 * How an application gives sessions their tokens without handing over its API
 * key. A session-scoped token acts only on the sessions it created or was bound
 * to, and lives at most six hours, while a session can run a day or more; so a
 * session starts on a token from `create` and, before that expires, carries on
 * with one from `bind` for its own id.
 */
export interface Tokens {
  /** A token that may create one session. */
  readonly create: Effect.Effect<TokenGrant, ReactorError>;
  /** A fresh token bound to an open session. */
  readonly bind: (sessionId: string) => Effect.Effect<TokenGrant, ReactorError>;
}

/**
 * Tokens that are never refreshed, for a session shorter than its token: the
 * same `token` creates the session and serves its every later call, and the
 * session's calls fail once it expires.
 */
export const fixedTokens = (token: TokenGrant): Tokens => ({
  create: Effect.succeed(token),
  bind: () => Effect.succeed(token),
});

const Count = Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 500 }));
const Seconds = Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 86_400 }));
/** One `authorization_details` entry of a session-scoped token, as `POST /tokens` takes it. */
const SessionAuthorization = Schema.Struct({
  type: Schema.Literal("session"),
  resources: Schema.Struct({
    models: Schema.Struct({ match: Schema.NonEmptyArray(Schema.NonEmptyString) }),
    sessions: Schema.optionalKey(
      Schema.Struct({ bind: Schema.NonEmptyArray(Schema.NonEmptyString) }),
    ),
  }),
  constraints: Schema.optionalKey(
    Schema.Struct({
      max_sessions: Schema.optionalKey(Count),
      max_session_duration_seconds: Schema.optionalKey(Seconds),
    }),
  ),
});
/** Reactor's echo of a grant: the request's shape, with the resolved count and bound set. */
const Stated = Schema.Int.pipe(Schema.NullOr, Schema.optionalKey);
const GrantEcho = Schema.Struct({
  type: Schema.Literal("session"),
  resources: Schema.Struct({
    models: Schema.Struct({ match: Schema.Array(Schema.String) }),
    sessions: Schema.Struct({
      bind: Schema.String.pipe(Schema.Array, Schema.optionalKey),
    }).pipe(Schema.NullOr, Schema.optionalKey),
  }),
  constraints: Schema.Struct({
    max_sessions: Stated,
    max_session_duration_seconds: Stated,
  }).pipe(Schema.NullOr, Schema.optionalKey),
});
const TokenReply = Schema.Struct({
  jwt: Schema.NonEmptyString,
  expires_at: Schema.Finite,
  authorization_details: GrantEcho.pipe(Schema.Array, Schema.optionalKey),
});
const TokenRequestBody = Schema.Struct({
  authorization_details: Schema.Tuple([SessionAuthorization]),
  expires_after: Schema.optionalKey(Schema.Int),
});
const ClientInfo = Schema.Struct({ sdk_version: Schema.String, sdk_type: Schema.String });
const CreateBody = Schema.Struct({
  model: Schema.Struct({ name: Schema.NonEmptyString, version: Schema.optionalKey(Schema.String) }),
  client_info: ClientInfo,
  supported_transports: Schema.Array(Transport),
  extra_args: Schema.optionalKey(Schema.Json),
});
const OfferBody = Schema.Struct({
  sdp_offer: Schema.NonEmptyString,
  client_info: ClientInfo,
  track_mapping: Schema.Array(Mapping),
});
const IceBody = Schema.Struct({
  candidates: Schema.Array(IceCandidate),
  is_final: Schema.Boolean,
  client_info: ClientInfo,
});
const UploadBody = Schema.Struct({
  name: Schema.NonEmptyString,
  mime_type: Schema.NonEmptyString,
  size: PositiveInt,
});

export const Inspection = Schema.Struct({
  observedAt: Schema.Finite,
  state: Schema.String,
  hasCapabilities: Schema.Boolean,
  selectedTransport: Schema.NullOr(Transport),
  cluster: Schema.NullOr(Schema.String),
  zone: Schema.NullOr(Schema.String),
  serverVersion: Schema.NullOr(Schema.String),
});
export type Inspection = typeof Inspection.Type;

const SessionDescription = Schema.Struct({
  session_id: Schema.NonEmptyString,
  state: Schema.NonEmptyString,
  capabilities: Schema.Unknown.pipe(Schema.NullOr, Schema.optionalKey),
  selected_transport: Transport.pipe(Schema.NullOr, Schema.optionalKey),
  cluster: Schema.String.pipe(Schema.NullOr, Schema.optionalKey),
  zone: Schema.String.pipe(Schema.NullOr, Schema.optionalKey),
  server_info: Schema.Struct({ server_version: Schema.String }).pipe(
    Schema.NullOr,
    Schema.optionalKey,
  ),
});
/** A termination probe promises state; when it names a session, it must be the one asked for. */
const TerminalState = Schema.Struct({
  session_id: Schema.optionalKey(Schema.NonEmptyString),
  state: Schema.NonEmptyString,
});

export type Segment = Recording.Segment;
export type DownloadedClip = Recording.DownloadedClip;
export type DownloadOptions = Recording.DownloadOptions;
export const parsePlaylist = Recording.parsePlaylist;

/** The coordinator calls one session makes, authorized by its token. */
export interface Signaling {
  /** Resolves once the reply names the allocated session; `describe` decodes the rest. */
  readonly create: (
    model: { readonly name: string; readonly version?: string | undefined },
    extraArgs?: Schema.Json,
  ) => Effect.Effect<Allocation, ReactorError>;
  /** A reply that cannot describe its session still names one its owner must terminate. */
  readonly describe: (allocation: Allocation) => Effect.Effect<Descriptor, ReactorError>;
  readonly read: (sessionId: string) => Effect.Effect<Descriptor, ReactorError>;
  /** Polls until the session publishes capabilities and a transport, or ends. */
  readonly ready: (
    sessionId: string,
    initial?: Descriptor,
  ) => Effect.Effect<Descriptor, ReactorError>;
  readonly iceServers: (sessionId: string) => Effect.Effect<ReadonlyArray<IceServer>, ReactorError>;
  readonly register: (sessionId: string) => Effect.Effect<number, ReactorError>;
  readonly offer: (
    sessionId: string,
    connectionId: number,
    sdp: string,
    mapping: ReadonlyArray<Mapping>,
    replace: boolean,
  ) => Effect.Effect<void, ReactorError>;
  /** Polls while the answer is pending (202); the caller's deadline bounds it. */
  readonly answer: (
    sessionId: string,
    connectionId: number,
  ) => Effect.Effect<SdpAnswer, ReactorError>;
  readonly ice: (
    sessionId: string,
    connectionId: number,
    candidates: ReadonlyArray<IceCandidate>,
    isFinal: boolean,
  ) => Effect.Effect<void, ReactorError>;
  readonly allocateUpload: (
    sessionId: string,
    name: string,
    mimeType: string,
    size: number,
  ) => Effect.Effect<UploadSlot, ReactorError>;
  /** Sends no credential: the presigned URL is its own authority. */
  readonly putUpload: (
    slot: UploadSlot,
    bytes: Uint8Array<ArrayBuffer>,
    mimeType: string,
  ) => Effect.Effect<void, ReactorError>;
  readonly terminate: (sessionId: string) => Effect.Effect<Termination>;
  /** Polls a recording's playlist, then fetches and joins its segments within a deadline. */
  readonly downloadClip: (
    clip: ClipReady,
    options?: DownloadOptions,
  ) => Effect.Effect<DownloadedClip, ReactorError>;
}

export interface Options {
  readonly apiUrl?: string | undefined;
  /**
   * The API key `mintToken` uses when its options name none. Without a
   * `credential` it also authorizes `inspect`, `terminate` and `downloadClip`,
   * since the key may act on any session of its account.
   */
  readonly apiKey?: Redacted.Redacted<string> | undefined;
  /**
   * The session token that authorizes `inspect`, `terminate` and
   * `downloadClip`, in place of the API key.
   */
  readonly credential?: Effect.Effect<Redacted.Redacted<string>, ReactorError> | undefined;
}

export class CoordinatorClient extends Context.Service<
  CoordinatorClient,
  {
    readonly apiUrl: string;
    /** The pricing catalog, as the provider publishes it. */
    readonly pricing: Effect.Effect<Schema.Json, ReactorError>;
    readonly mintToken: (options: TokenOptions) => Effect.Effect<TokenGrant, ReactorError>;
    /**
     * `Tokens` minted with the API key: each `create` token may create one
     * session capped as `maxSessionDuration` says, and each `bind` token acts
     * on the one session it names.
     */
    readonly tokens: (
      options: Omit<TokenOptions, "maxSessions" | "bind"> & {
        readonly maxSessionDuration: Duration.Input | "unlimited";
      },
    ) => Tokens;
    readonly inspect: (sessionId: string) => Effect.Effect<Inspection, ReactorError>;
    /** Uncertainty stays in the report; supervisors choose their own failure policy. */
    readonly terminate: (sessionId: string) => Effect.Effect<Termination>;
    readonly downloadClip: (
      clip: ClipReady,
      options?: DownloadOptions,
    ) => Effect.Effect<DownloadedClip, ReactorError>;
    /**
     * The calls one session makes, each with the token `credential` then gives.
     * It is the session's own seam, not an application's: its `create`
     * allocates a session that nothing owns, closes or reports on, so allocate
     * with `Reactor.create`.
     */
    readonly signaling: (credential: Credential) => Signaling;
  }
>()("reactor-effect-client/CoordinatorClient") {}

/** A session's token, when it has one; failing or late, the request is never sent. */
type Credential = Effect.Effect<Redacted.Redacted<string> | undefined, ReactorError>;

/** How a call is addressed, and whether it carries the session's token. */
type Route =
  /** An API path with the session's token and API version. */
  | "session"
  /** The same, and the WebRTC signaling version. */
  | "signaling"
  /** An API path with neither: pricing and tokens. */
  | "public"
  /** A recording's URL: the token goes only to the API's own origin. */
  | "recording"
  /** A presigned upload URL, its own authority: no credential. */
  | "upload";

interface Call {
  readonly operation: string;
  /** A path under the API for its routes, an absolute URL for a recording or upload. */
  readonly request: HttpClientRequest.HttpClientRequest;
  /** `session` by default. */
  readonly route?: Route;
  /** Statuses answered as data rather than failures, besides 2xx. */
  readonly accepted?: ReadonlyArray<number>;
  /** The largest body read; 2 MiB by default. */
  readonly maxBytes?: number;
  /** 15 seconds by default. */
  readonly timeout?: Duration.Input;
  /** Sees the status as soon as it arrives, even if reading the body then fails. */
  readonly onStatus?: (status: number) => Effect.Effect<void>;
}

const checkedUrl = (url: string, base?: string): Effect.Effect<URL, ReactorError> =>
  Effect.try({
    try: () => new URL(url, base),
    catch: (cause) => ReactorError.fromCode("Protocol", "HTTP URL is malformed", { detail: cause }),
  }).pipe(
    Effect.filterOrFail(
      (value) =>
        (value.protocol === "https:" || value.protocol === "http:") &&
        value.username === "" &&
        value.password === "",
      () =>
        ReactorError.fromCode(
          "Protocol",
          "HTTP URL must use http(s) and contain no embedded credentials",
        ),
    ),
  );

/** A response's `Retry-After` when it names a delay in seconds; an HTTP date is not read. */
const retryAfterOf = (headers: Headers.Headers): Duration.Duration | undefined => {
  const raw = headers["retry-after"];
  if (raw === undefined || !/^\s*(?:\d+(?:\.\d*)?|\.\d+)\s*$/.test(raw)) return undefined;
  const seconds = Number(raw);
  return Number.isFinite(seconds) ? Duration.seconds(seconds) : undefined;
};

/** The body within `maxBytes` and 16,384 chunks, since every chunk, even an empty one, costs memory. */
const bodyWithin = (
  response: HttpClientResponse.HttpClientResponse,
  maxBytes: number,
): Effect.Effect<Uint8Array, HttpClientError.HttpClientError | ReactorError> => {
  const overflow = ReactorError.fromCode(
    "Overflow",
    `response exceeds its ${maxBytes} byte bound`,
    {
      outcome: "replied",
    },
  );
  const refused: Stream.Stream<Uint8Array, HttpClientError.HttpClientError | ReactorError> =
    Stream.fail(overflow);
  return response.stream.pipe(
    Stream.catchIf(
      (error) => error.reason._tag === "EmptyBodyError",
      () => Stream.empty,
    ),
    Stream.limitBytes(maxBytes, () => refused),
    Stream.zipWithIndex,
    Stream.mapEffect(([chunk, index]) =>
      index < 16_384 ? Effect.succeed(chunk) : Effect.fail(overflow),
    ),
    Stream.mkUint8Array,
  );
};

/** A transport failure once a request may have reached the server: its outcome is unknown. */
const lost =
  (operation: string, status?: number) =>
  (cause: HttpClientError.HttpClientError | ReactorError): ReactorError =>
    ReactorError.is(cause)
      ? cause
      : ReactorError.make({
          reason: Http.make({
            message: `${operation}: network/read failure`,
            ...(status === undefined ? {} : { status }),
          }),
          context: { operation, outcome: "unknown", detail: Redacted.make(cause) },
        });

/** The reply when its call accepts the status; otherwise the failure the status says. */
const answered = (call: Call, status: number, bytes: Uint8Array, headers: Headers.Headers) => {
  const retryAfter = retryAfterOf(headers);
  if ((status >= 200 && status < 300) || call.accepted?.includes(status) === true)
    return Effect.succeed<Recording.Reply>({ status, bytes, retryAfter });
  const message = `${call.operation}: HTTP ${String(status)}`;
  const body = Redacted.make(new TextDecoder().decode(bytes));
  const context = { operation: call.operation, outcome: "replied" } as const;
  return Effect.fail(
    status === 426 || status === 501
      ? ReactorError.fromCode("VersionMismatch", message, { ...context, detail: { status, body } })
      : ReactorError.make({
          reason: Http.make({
            message,
            status,
            body,
            ...(retryAfter === undefined ? {} : { retryAfter }),
          }),
          context,
        }),
  );
};

const utf8 = new TextDecoder("utf-8", { fatal: true });

/** Decodes a JSON reply through its Schema; the SchemaError names a path, never the value. */
const decodeReply = <S extends Schema.Top & { readonly DecodingServices: never }>(
  schema: S,
  operation: string,
) => {
  const protocol = (message: string) => (detail: unknown) =>
    ReactorError.fromCode("Protocol", message, { operation, outcome: "replied", detail });
  const decode = Schema.decodeEffect(Schema.fromJsonString(schema));
  return (reply: Recording.Reply) =>
    Effect.try({
      try: () => utf8.decode(reply.bytes),
      catch: protocol(`${operation} reply is not UTF-8`),
    }).pipe(
      Effect.flatMap((text) =>
        Effect.mapError(decode(text), protocol(`invalid ${operation} reply`)),
      ),
    );
};

/** `request` with `value` as its JSON body; a value its Schema refuses is never sent. */
const withBody = <S extends Schema.Top & { readonly EncodingServices: never }>(
  schema: S,
  operation: string,
) => {
  const encode = HttpClientRequest.schemaBodyJson(schema);
  return (request: HttpClientRequest.HttpClientRequest, value: S["Type"]) =>
    Effect.mapError(encode(request, value), (cause) =>
      ReactorError.fromCode("InvalidInput", `${operation} body cannot be encoded`, {
        operation,
        outcome: "not-submitted",
        detail: cause,
      }),
    );
};

const sessionPath = (id: string) => `/sessions/${encodeURIComponent(id)}`;
const transportPath = (id: string) => `${sessionPath(id)}/transport/webrtc`;
const connectionPath = (id: string, cid: number) =>
  `${transportPath(id)}/connections/${String(cid)}`;

/** Doubling waits from 200 ms, capped: at most 20 descriptor reads. */
const sessionPoll = Schedule.min([
  Schedule.exponential("200 millis"),
  Schedule.spaced("10 seconds"),
]).pipe(Schedule.upTo({ times: 19 }));
/** Doubling waits from 200 ms up to 2 s; the caller's connect deadline bounds the whole wait. */
const sdpPoll = Schedule.min([Schedule.exponential("200 millis"), Schedule.spaced("2 seconds")]);

const createBody = withBody(CreateBody, "create session");
const offerBody = withBody(OfferBody, "SDP offer");
const iceBody = withBody(IceBody, "ICE candidates");
const uploadBody = withBody(UploadBody, "allocate upload");
const tokenBody = withBody(TokenRequestBody, "token");

/** A CoordinatorClient over the current `HttpClient`. No request runs while it is built. */
export const make = Effect.fnUntraced(function* (options: Options = {}) {
  const base = yield* checkedUrl(options.apiUrl ?? defaultApiUrl).pipe(
    Effect.filterOrFail(
      (url) => url.search === "" && url.hash === "",
      () => ReactorError.fromCode("InvalidInput", "apiUrl cannot contain a query or fragment"),
    ),
  );
  const apiUrl = base.href.replace(/\/$/, "");
  const http = (yield* HttpClient.HttpClient).pipe(
    // Never follow a redirect with a credential, never send ambient cookies, and trace the
    // operations rather than each request.
    HttpClient.transform((response) =>
      response.pipe(
        Effect.provideService(FetchHttpClient.RequestInit, {
          credentials: "omit",
          redirect: "error",
        }),
        Effect.provideService(HttpClient.TracerDisabledWhen, () => true),
        Effect.updateService(Headers.CurrentRedactedNames, (names) => [
          ...names,
          "reactor-api-key",
        ]),
      ),
    ),
  );
  const api = http.pipe(HttpClient.mapRequest(HttpClientRequest.prependUrl(apiUrl)));
  const versioned = api.pipe(
    HttpClient.mapRequest(
      HttpClientRequest.setHeaders({
        "reactor-api-version": "1",
        "reactor-api-accept-version": "1",
      }),
    ),
  );
  const clients: Record<Route, HttpClient.HttpClient> = {
    session: versioned,
    signaling: versioned.pipe(
      HttpClient.mapRequest(HttpClientRequest.setHeader("reactor-webrtc-version", "1.0")),
    ),
    public: api,
    recording: http,
    upload: http,
  };

  const call = Effect.fnUntraced(function* (
    credential: Credential,
    spec: Call,
  ): Effect.fn.Return<Recording.Reply, ReactorError> {
    const { operation } = spec;
    const route = spec.route ?? "session";
    const deadline = spec.timeout ?? "15 seconds";
    const url =
      route === "recording" || route === "upload" ? yield* checkedUrl(spec.request.url) : base;
    // A request whose token could not be had, at all or in time, was never sent. The token
    // has its own deadline, before the request's starts: a slow mint is not a request that
    // may have landed.
    const token =
      route === "public" || route === "upload" || url.origin !== base.origin
        ? undefined
        : yield* credential.pipe(
            Effect.timeoutOrElse({
              duration: deadline,
              orElse: () =>
                Effect.fail(ReactorError.fromCode("Timeout", `${operation}: no token in time`)),
            }),
            Effect.mapError((error) =>
              ReactorError.make({
                reason: error.reason,
                context: { ...error.context, operation, outcome: "not-submitted" },
              }),
            ),
          );
    const request =
      token === undefined ? spec.request : HttpClientRequest.bearerToken(spec.request, token);
    return yield* clients[route].execute(request).pipe(
      // Crossing execute is where a mutation may have reached the server: a transport
      // failure after it cannot prove non-delivery.
      Effect.mapError(lost(operation)),
      Effect.tap((response) => spec.onStatus?.(response.status) ?? Effect.void),
      Effect.flatMap((response) =>
        bodyWithin(response, spec.maxBytes ?? 2_097_152).pipe(
          Effect.mapError(lost(operation, response.status)),
          Effect.flatMap((bytes) => answered(spec, response.status, bytes, response.headers)),
        ),
      ),
      Effect.scoped,
      Effect.timeoutOrElse({
        duration: deadline,
        orElse: () =>
          Effect.fail(
            ReactorError.fromCode("Timeout", `${operation}: deadline`, {
              operation,
              outcome: "unknown",
            }),
          ),
      }),
    );
  });

  const signaling = (credential: Credential): Signaling => {
    const request = (spec: Call) => call(credential, spec);

    const read = (sessionId: string) =>
      request({
        operation: "read session",
        request: HttpClientRequest.get(sessionPath(sessionId)),
      }).pipe(
        Effect.flatMap(decodeReply(Descriptor, "session descriptor")),
        Effect.filterOrFail(
          (descriptor) => descriptor.session_id === sessionId,
          () => ReactorError.fromCode("Protocol", "session descriptor identity mismatch"),
        ),
      );

    const terminate = Effect.fnUntraced(
      function* (sessionId: string): Effect.fn.Return<Termination> {
        const status = yield* Ref.make<number | null>(null);
        const path = sessionPath(sessionId);
        const removal = yield* Effect.result(
          request({
            operation: "terminate",
            request: HttpClientRequest.delete(path),
            accepted: [404],
            timeout: "3 seconds",
            onStatus: (value) => Ref.set(status, value),
          }),
        );
        const deleteStatus = yield* Ref.get(status);
        const base = {
          attempted:
            removal._tag === "Success" || removal.failure.context.outcome !== "not-submitted",
          responseReceived: deleteStatus !== null,
          deleteStatus,
        };
        const failed = (error: ReactorError): Termination => ({
          ...base,
          confirmed: false,
          evidence: null,
          state: null,
          error: summarize(error),
        });
        const confirmation = yield* Effect.result(
          request({
            operation: "terminate",
            request: HttpClientRequest.get(path),
            accepted: [404],
            timeout: "3 seconds",
          }),
        );
        if (confirmation._tag === "Failure") return failed(confirmation.failure);
        if (confirmation.success.status === 404)
          return deleteStatus === 401 || deleteStatus === 403
            ? failed(
                ReactorError.make({
                  reason: Http.make({
                    message: "remote termination could not be confirmed after authority refusal",
                    status: deleteStatus,
                  }),
                  context: { operation: "terminate", outcome: "replied" },
                }),
              )
            : { ...base, confirmed: true, evidence: "absent", state: null };
        const described = yield* Effect.result(
          decodeReply(
            TerminalState,
            "termination description",
          )(confirmation.success).pipe(
            Effect.filterOrFail(
              (value) => value.session_id === undefined || value.session_id === sessionId,
              () => ReactorError.fromCode("Protocol", "termination names another session"),
            ),
          ),
        );
        if (described._tag === "Failure") return failed(described.failure);
        const state = described.success.state;
        const terminal = isTerminal(state);
        return {
          ...base,
          confirmed: terminal,
          evidence: terminal ? "terminal" : null,
          state,
          ...(!terminal && removal._tag === "Failure" ? { error: summarize(removal.failure) } : {}),
        };
      },
      Effect.tap((termination) =>
        Effect.annotateCurrentSpan({
          ...terminationAttributes(termination),
          ...(termination.error === undefined ? {} : { "error.type": termination.error.reason }),
        }),
      ),
      Effect.withSpan(
        "CoordinatorClient.terminate",
        (sessionId) => ({ kind: "client", attributes: { "reactor.session.id": sessionId } }),
        { captureStackTrace: false },
      ),
    );

    return {
      create: (model, extraArgs) =>
        createBody(HttpClientRequest.post("/sessions"), {
          model: {
            name: model.name,
            ...(model.version === undefined ? {} : { version: model.version }),
          },
          client_info: clientInfo,
          supported_transports: [{ protocol: "webrtc", version: "1.0" }],
          ...(extraArgs === undefined ? {} : { extra_args: extraArgs }),
        }).pipe(
          Effect.flatMap((body) => request({ operation: "create session", request: body })),
          // A refusal (4xx) proves nothing was allocated; a server error may follow an allocation.
          Effect.mapError((error) =>
            error.context.outcome === "replied" &&
            error.reason._tag === "Http" &&
            (error.reason.status ?? 0) >= 500
              ? ReactorError.make({
                  reason: error.reason,
                  context: { ...error.context, outcome: "unknown" },
                })
              : error,
          ),
          Effect.flatMap(decodeReply(Schema.Unknown, "create session")),
          Effect.flatMap((reply) =>
            Schema.decodeUnknownEffect(Allocated)(reply).pipe(
              Effect.map((allocated) => ({ sessionId: allocated.session_id, reply })),
              // A session may have been made all the same: the reply's body goes with the
              // failure, redacted as provider text is, for whoever must find that session.
              Effect.mapError((cause) =>
                ReactorError.fromCode("Protocol", "create reply names no session", {
                  operation: "create session",
                  outcome: "unknown",
                  detail: { body: reply, cause },
                }),
              ),
            ),
          ),
        ),
      describe: (allocation) =>
        Schema.decodeUnknownEffect(Descriptor)(allocation.reply).pipe(
          Effect.mapError((cause) =>
            ReactorError.fromCode("Protocol", "invalid session descriptor", {
              operation: "create session",
              sessionId: allocation.sessionId,
              outcome: "replied",
              detail: cause,
            }),
          ),
        ),
      read,
      ready: Effect.fnUntraced(function* (sessionId: string, initial?: Descriptor) {
        const first = yield* Ref.make(initial);
        const poll = Ref.getAndSet(first, undefined).pipe(
          Effect.filterOrElse(
            (cached): cached is Descriptor => cached !== undefined,
            () => read(sessionId),
          ),
          Effect.filterOrFail(
            (descriptor) => !isTerminal(descriptor.state),
            (descriptor) =>
              ReactorError.fromCode("TerminalSession", descriptor.state, { sessionId }),
          ),
        );
        const ready = (descriptor: Descriptor) =>
          descriptor.capabilities !== undefined && descriptor.selected_transport !== undefined;
        return yield* poll.pipe(
          Effect.repeat({ schedule: sessionPoll, until: ready }),
          Effect.filterOrFail(ready, () =>
            ReactorError.fromCode("Timeout", "session capabilities/transport not ready", {
              sessionId,
            }),
          ),
        );
      }),
      iceServers: (sessionId) =>
        request({
          operation: "ICE servers",
          route: "signaling",
          request: HttpClientRequest.get(`${transportPath(sessionId)}/ice_servers`),
        }).pipe(
          Effect.flatMap(decodeReply(IceServersReply, "ICE servers")),
          Effect.map((reply) =>
            reply.ice_servers.map((server): IceServer =>
              server.credentials === undefined
                ? { urls: server.uris }
                : {
                    urls: server.uris,
                    username: server.credentials.username,
                    credential: server.credentials.password,
                  },
            ),
          ),
        ),
      register: (sessionId) =>
        request({
          operation: "register connection",
          route: "signaling",
          request: HttpClientRequest.post(`${transportPath(sessionId)}/connections`).pipe(
            HttpClientRequest.bodyText("{}", "application/json"),
          ),
        }).pipe(
          Effect.flatMap(decodeReply(Registered, "connection")),
          Effect.map((reply) => reply.connection_id),
        ),
      offer: (sessionId, cid, sdp, mapping, replace) =>
        offerBody(
          HttpClientRequest.make(replace ? "PUT" : "POST")(
            `${connectionPath(sessionId, cid)}/sdp_params`,
          ),
          { sdp_offer: sdp, client_info: clientInfo, track_mapping: mapping },
        ).pipe(
          Effect.flatMap((body) =>
            request({ operation: "SDP offer", route: "signaling", request: body }),
          ),
          Effect.asVoid,
        ),
      answer: (sessionId, cid) =>
        request({
          operation: "SDP answer",
          route: "signaling",
          request: HttpClientRequest.get(`${connectionPath(sessionId, cid)}/sdp_params`),
        }).pipe(
          Effect.repeat({ schedule: sdpPoll, until: (reply) => reply.status !== 202 }),
          Effect.flatMap(decodeReply(SdpAnswer, "SDP answer")),
        ),
      ice: (sessionId, cid, candidates, isFinal) =>
        iceBody(HttpClientRequest.post(`${connectionPath(sessionId, cid)}/ice_candidates`), {
          candidates,
          is_final: isFinal,
          client_info: clientInfo,
        }).pipe(
          Effect.flatMap((body) =>
            request({ operation: "ICE candidates", route: "signaling", request: body }),
          ),
          Effect.asVoid,
        ),
      allocateUpload: (sessionId, name, mimeType, size) =>
        uploadBody(HttpClientRequest.post(`${sessionPath(sessionId)}/uploads`), {
          name,
          mime_type: mimeType,
          size,
        }).pipe(
          Effect.flatMap((body) => request({ operation: "allocate upload", request: body })),
          Effect.flatMap(decodeReply(UploadSlot, "upload allocation")),
          Effect.tap((slot) => checkedUrl(slot.presigned_url)),
        ),
      putUpload: (slot, bytes, mimeType) =>
        request({
          operation: "transfer upload",
          route: "upload",
          request: HttpClientRequest.put(slot.presigned_url).pipe(
            HttpClientRequest.bodyUint8Array(bytes, mimeType),
          ),
        }).pipe(Effect.asVoid),
      terminate,
      downloadClip: (clip, downloadOptions) =>
        Recording.download({
          fetcher: {
            fetch: (operation, url, maxBytes) =>
              request({
                operation,
                route: "recording",
                request: HttpClientRequest.get(url),
                maxBytes,
              }),
            read,
          },
          clip,
          ...downloadOptions,
        }).pipe(
          Effect.withSpan(
            "CoordinatorClient.downloadClip",
            { kind: "client", attributes: { "reactor.session.id": clip.sessionId } },
            { captureStackTrace: false },
          ),
        ),
    };
  };

  // A server holding the key reads, ends and downloads any session of its account with the
  // key as the bearer. It goes only to the API's own origin, as every credential does.
  const configured =
    options.credential ??
    (options.apiKey === undefined ? Effect.undefined : Effect.succeed(options.apiKey));
  const app = (spec: Call) => call(configured, spec);
  const appSignaling = signaling(configured);

  const mintToken = Effect.fnUntraced(
    function* (input: TokenOptions) {
      const invalid = (message: string) =>
        ReactorError.fromCode("InvalidInput", message, {
          operation: "token",
          outcome: "not-submitted",
        });
      const apiKey = input.apiKey ?? options.apiKey;
      if (apiKey === undefined || Redacted.value(apiKey).length === 0)
        return yield* invalid("a token needs an API key");
      if (input.modelName.length === 0) return yield* invalid("a token needs a model");
      const bind = input.bind ?? [];
      if (bind.some((id) => id.length === 0)) return yield* invalid("a bound session needs an id");
      const maxSessions = input.maxSessions ?? (bind.length === 0 ? 1 : undefined);
      if (
        maxSessions !== undefined &&
        !(
          Number.isInteger(maxSessions) &&
          maxSessions >= Math.max(1, bind.length) &&
          maxSessions <= 500
        )
      )
        return yield* invalid("maxSessions is an integer from 1, or the bound count, to 500");
      // Reactor's default leaves a bound token no room to create; any other token creates.
      const creates = maxSessions !== undefined && maxSessions > bind.length;
      const cap = input.maxSessionDuration;
      let seconds: number | undefined;
      if (cap === undefined) {
        if (creates)
          return yield* invalid(
            'a token that creates sessions needs a maxSessionDuration: a duration or "unlimited"',
          );
      } else if (cap !== "unlimited") {
        const duration = Duration.fromInput(cap);
        seconds = Option.isSome(duration) ? Duration.toSeconds(duration.value) : Number.NaN;
        if (!(Number.isInteger(seconds) && seconds >= 1 && seconds <= 86_400))
          return yield* invalid("maxSessionDuration is whole seconds from one to a day");
      }
      let expiresAfter: number | undefined;
      if (input.expiresAfter !== undefined) {
        const expiry = Duration.fromInput(input.expiresAfter);
        expiresAfter = Option.isSome(expiry) ? Duration.toSeconds(expiry.value) : Number.NaN;
        if (!(Number.isSafeInteger(expiresAfter) && expiresAfter >= 1))
          return yield* invalid("expiresAfter is whole seconds, at least one");
      }
      const request = yield* tokenBody(
        HttpClientRequest.post("/tokens").pipe(
          HttpClientRequest.setHeader("reactor-api-key", Redacted.value(apiKey)),
        ),
        {
          authorization_details: [
            {
              type: "session",
              resources: {
                models: { match: [input.modelName] },
                ...(Arr.isReadonlyArrayNonEmpty(bind) ? { sessions: { bind } } : {}),
              },
              ...(maxSessions === undefined && seconds === undefined
                ? {}
                : {
                    constraints: {
                      ...(maxSessions === undefined ? {} : { max_sessions: maxSessions }),
                      ...(seconds === undefined ? {} : { max_session_duration_seconds: seconds }),
                    },
                  }),
            },
          ],
          ...(expiresAfter === undefined ? {} : { expires_after: expiresAfter }),
        },
      );
      const token = yield* app({
        operation: "token",
        request,
        route: "public",
        timeout: "8 seconds",
      }).pipe(Effect.flatMap(decodeReply(TokenReply, "token")));
      const protocol = (message: string) =>
        ReactorError.fromCode("Protocol", message, { operation: "token", outcome: "replied" });
      if (token.expires_at * 1_000 <= (yield* Clock.currentTimeMillis))
        return yield* protocol("the token has already expired");
      const [entry, ...others] = token.authorization_details ?? [];
      if (others.length > 0) return yield* protocol("the token grants other authority than asked");
      const stated = entry?.constraints?.max_session_duration_seconds;
      const granted: Granted | undefined =
        entry === undefined
          ? undefined
          : {
              models: entry.resources.models.match,
              maxSessions: entry.constraints?.max_sessions ?? undefined,
              // A null cap says there is none; an absent one says nothing.
              maxSessionSeconds: stated === null ? "unlimited" : stated,
              bound: entry.resources.sessions?.bind ?? [],
            };
      if (
        granted !== undefined &&
        (granted.models.some((model) => model !== input.modelName) ||
          granted.bound.some((id) => !bind.includes(id)) ||
          (granted.maxSessions ?? 0) > (maxSessions ?? bind.length) ||
          (seconds !== undefined &&
            (granted.maxSessionSeconds === "unlimited" ||
              (granted.maxSessionSeconds ?? 0) > seconds)))
      )
        return yield* protocol("the token grants more than was asked");
      // A narrower grant caps each session at what it grants.
      const narrower =
        Predicate.isNumber(granted?.maxSessionSeconds) &&
        (seconds === undefined || granted.maxSessionSeconds < seconds);
      return {
        jwt: Redacted.make(token.jwt),
        expiresAt: token.expires_at,
        maxSessionSeconds: narrower ? granted.maxSessionSeconds : seconds,
        ...(granted === undefined ? {} : { granted }),
      } satisfies TokenGrant;
    },
    Effect.withSpan(
      "CoordinatorClient.mintToken",
      { kind: "client" },
      { captureStackTrace: false },
    ),
  );

  return CoordinatorClient.of({
    apiUrl,
    signaling,
    mintToken,
    tokens: (input) => ({
      create: mintToken({ ...input, maxSessions: 1 }),
      bind: (sessionId) =>
        mintToken({
          modelName: input.modelName,
          bind: [sessionId],
          ...(input.apiKey === undefined ? {} : { apiKey: input.apiKey }),
          ...(input.expiresAfter === undefined ? {} : { expiresAfter: input.expiresAfter }),
        }),
    }),
    pricing: app({
      operation: "pricing",
      request: HttpClientRequest.get("/pricing"),
      route: "public",
      timeout: "8 seconds",
    }).pipe(
      Effect.flatMap(decodeReply(Schema.Json, "pricing")),
      Effect.withSpan(
        "CoordinatorClient.pricing",
        { kind: "client" },
        { captureStackTrace: false },
      ),
    ),
    inspect: Effect.fnUntraced(
      function* (sessionId: string) {
        const value = yield* app({
          operation: "inspect",
          request: HttpClientRequest.get(sessionPath(sessionId)),
          timeout: "1 second",
        }).pipe(Effect.flatMap(decodeReply(SessionDescription, "inspect")));
        if (value.session_id !== sessionId)
          return yield* ReactorError.fromCode("Protocol", "inspection names another session", {
            operation: "inspect",
            outcome: "replied",
          });
        return {
          observedAt: yield* Clock.currentTimeMillis,
          state: value.state,
          hasCapabilities: value.capabilities != null,
          selectedTransport: value.selected_transport ?? null,
          cluster: value.cluster ?? null,
          zone: value.zone ?? null,
          serverVersion: value.server_info?.server_version ?? null,
        } satisfies Inspection;
      },
      Effect.withSpan(
        "CoordinatorClient.inspect",
        (sessionId) => ({ kind: "client", attributes: { "reactor.session.id": sessionId } }),
        { captureStackTrace: false },
      ),
    ),
    terminate: appSignaling.terminate,
    downloadClip: appSignaling.downloadClip,
  });
});

/** A CoordinatorClient service over the application's `HttpClient`. */
export const layer = (
  options: Options = {},
): Layer.Layer<CoordinatorClient, ReactorError, HttpClient.HttpClient> =>
  Layer.effect(CoordinatorClient, make(options));

/**
 * A CoordinatorClient configured from the environment: `REACTOR_API_URL` (optional)
 * and `REACTOR_API_KEY` (optional, for `mintToken`, `inspect`, `terminate`
 * and `downloadClip`).
 */
export const layerConfig: Layer.Layer<
  CoordinatorClient,
  ReactorError | Config.ConfigError,
  HttpClient.HttpClient
> = Layer.effect(
  CoordinatorClient,
  Effect.gen(function* () {
    const apiUrl = yield* Config.String("REACTOR_API_URL").pipe(Config.withDefault(defaultApiUrl));
    const apiKey = yield* Config.Redacted("REACTOR_API_KEY").pipe(Config.option);
    return yield* make({ apiUrl, ...(Option.isSome(apiKey) ? { apiKey: apiKey.value } : {}) });
  }),
);
