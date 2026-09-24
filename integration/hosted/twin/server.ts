/**
 * A loopback twin of hosted Reactor: every coordinator route the public client
 * uses for an H3 session, with the paths, headers and JSON shapes it sends and
 * decodes, and the link the twin's peer uses in place of WebRTC. Tokens are
 * HS256 JWTs granting exactly the session count and length asked for; a
 * session ends when DELETE is accepted and on its own at the granted cap; and
 * the H3 model runs server-side, so any process that knows the URL can drive
 * it. Faults change one of those behaviours each.
 */
import { Buffer } from "node:buffer";
import { createHmac, randomBytes, randomUUID, timingSafeEqual } from "node:crypto";
import { createServer } from "node:http";
import type { IncomingHttpHeaders, IncomingMessage, Server, ServerResponse } from "node:http";
import type { AddressInfo } from "node:net";
import * as Data from "effect/Data";
import * as Predicate from "effect/Predicate";
import type { JsonObject, Mapping } from "reactor-effect-client";
import * as H3 from "reactor-effect-client/h3";
import { H3Model, tracks } from "./h3.js";
import { answer, parseCandidate, peerOf } from "./protocol.js";
import type { Candidate, ChannelName, EncodedEvent } from "./protocol.js";

/** With `slowDelete`, a session reads STOPPING for this long after DELETE, then CLOSED. */
const stoppingMs = 1500;
/** An SDP answer is ready this long after its offer; polls before then see 202. */
const negotiationMs = 50;
/** A peer that stops polling for this long is disconnected. Its session goes on. */
const livenessMs = 2000;
/** A peer bound by an offer has this long to poll for the first time. */
const firstPollMs = 10_000;
/** A poll with nothing to deliver is answered empty after this long. */
const holdMs = 1000;
/** Events a peer may leave unacknowledged before its connection is dropped. */
const maxOutbox = 4096;
const maxBody = 1_048_576;
/** The wire's own message bound. */
const maxMessage = 262_144;

export interface TwinRate {
  readonly creditsPerSecond: number;
  readonly creditsPerDollar: number;
}

export interface TwinFaults {
  /** Tokens grant two sessions instead of the one asked for. */
  readonly overgrant?: boolean;
  /**
   * DELETE answers 202 and the session reads STOPPING for about 1.5 s before
   * CLOSED: termination the client cannot confirm with its one read.
   */
  readonly slowDelete?: boolean;
  /** DELETE answers 202 and the session runs on until its cap. */
  readonly ignoreDelete?: boolean;
  /** The model receives each enqueue and never answers it. */
  readonly dropEnqueueReply?: boolean;
  /** Video frames are black while a clip plays. */
  readonly blackFrames?: boolean;
  /** Video frames repeat one image while a clip plays. */
  readonly frozenFrames?: boolean;
  /** The session offers audio and sends none. */
  readonly noAudio?: boolean;
}

export interface TwinOptions {
  /** The key `POST /tokens` accepts; by default a fresh `twin-api-key-` one. */
  readonly apiKey?: string;
  /**
   * Select a relay candidate pair, as a network that blocks direct paths
   * would. By default the selected pair is host to host.
   */
  readonly relay?: boolean;
  /** The published H3 rate; by default a 60 s session costs US$0.10. */
  readonly rate?: TwinRate;
  /**
   * End every session after this many seconds when that is sooner than its
   * token grants, so a test can watch the cap fire. Tokens still grant what
   * was asked for.
   */
  readonly capSeconds?: number;
  readonly faults?: TwinFaults;
}

/** A session as the twin sees it. */
export interface TwinSession {
  /** The coordinator state a read returns. */
  readonly state: string;
  /** Whether a peer is attached and polling. */
  readonly connected: boolean;
}

export interface Twin extends AsyncDisposable {
  /** The coordinator URL, for `Reactor.layer({ apiUrl })` and `twinPeers`. */
  readonly url: string;
  readonly apiKey: string;
  readonly sessionsCreated: number;
  /** DELETE requests received, including repeats and ignored ones. */
  readonly deletes: number;
  /** Enqueue commands the model received, including unanswered ones. */
  readonly enqueues: number;
  /** Every session, by id. */
  readonly sessions: ReadonlyMap<string, TwinSession>;
  readonly close: () => Promise<void>;
}

/** A request the twin refuses, with the status and error body a client sees. */
class Refusal extends Data.TaggedError("Refusal")<{
  readonly status: number;
  readonly code: string;
  readonly message: string;
}> {}

const refuse = (status: number, code: string, message: string): Refusal =>
  new Refusal({ status, code, message });

const field = (value: unknown, ...path: readonly string[]): unknown => {
  let current = value;
  for (const key of path) {
    if (!Predicate.isObject(current)) return undefined;
    current = current[key];
  }
  return current;
};

const positive = (value: unknown, maximum: number): value is number =>
  typeof value === "number" && Number.isSafeInteger(value) && value > 0 && value <= maximum;

const read = (request: IncomingMessage, limit = maxBody): Promise<Uint8Array> =>
  new Promise((resolve, reject) => {
    const chunks: Buffer[] = [];
    let size = 0;
    request.on("data", (chunk: Buffer) => {
      size += chunk.byteLength;
      if (size <= limit) chunks.push(chunk);
    });
    request.on("end", () =>
      size > limit
        ? reject(refuse(413, "too_large", `the body exceeds ${limit} bytes`))
        : resolve(new Uint8Array(Buffer.concat(chunks))),
    );
    request.on("error", reject);
  });

const json = (bytes: Uint8Array): unknown => {
  try {
    const value: unknown = JSON.parse(new TextDecoder("utf-8", { fatal: true }).decode(bytes));
    return value;
  } catch {
    throw refuse(400, "invalid_json", "the body is not JSON");
  }
};

const respond = (response: ServerResponse, status: number, body?: unknown): void => {
  if (body === undefined) {
    response.writeHead(status).end();
    return;
  }
  const text = JSON.stringify(body);
  response
    .writeHead(status, {
      "content-type": "application/json",
      "content-length": Buffer.byteLength(text),
    })
    .end(text);
};

interface Grant {
  readonly id: string;
  readonly model: string;
  readonly maxSessions: number;
  readonly maxSessionSeconds: number;
  /** Seconds since the epoch. */
  readonly expiresAt: number;
  sessions: number;
}

type Outgoing =
  | { readonly type: "open"; readonly local: Candidate; readonly remote: Candidate }
  | { readonly type: "message"; readonly channel: ChannelName; readonly bytes: Uint8Array }
  | { readonly type: "media" }
  | { readonly type: "closed"; readonly reason: "replaced" | "ended" | "unreachable" };

interface Link {
  readonly id: string;
  readonly session: Session;
  readonly connection: Connection;
  readonly outbox: { readonly seq: number; readonly event: Outgoing }[];
  seq: number;
  /** Bound and not closed. */
  live: boolean;
  /** Its first poll arrived: channels are open. */
  opened: boolean;
  lastSeen: number;
  waiting: ServerResponse | undefined;
  hold: ReturnType<typeof setTimeout> | undefined;
  /** A release of the held poll is scheduled for the end of this turn. */
  flushing: boolean;
}

interface Connection {
  readonly id: number;
  readonly candidates: Candidate[];
  offer: { readonly readyAt: number; readonly answer: string } | undefined;
}

interface Session {
  readonly id: string;
  readonly grant: Grant;
  state: "ACTIVE" | "STOPPING" | "CLOSED";
  readonly model: H3Model;
  readonly connections: Map<number, Connection>;
  peer: Link | undefined;
  readonly timers: Set<ReturnType<typeof setTimeout>>;
}

class TwinServer implements Twin {
  readonly url: string;
  readonly apiKey: string;
  private readonly port: number;
  private readonly secret = randomBytes(32);
  private readonly turnPassword = randomBytes(12).toString("hex");
  private readonly grants = new Map<string, Grant>();
  private readonly records = new Map<string, Session>();
  private readonly links = new Map<string, Link>();
  private readonly uploads = new Map<string, number>();
  private readonly sweeper: ReturnType<typeof setInterval>;
  private nextConnection = 1001;
  private created = 0;
  private deleted = 0;
  private enqueued = 0;
  private closing: Promise<void> | undefined;

  constructor(
    private readonly server: Server,
    private readonly options: TwinOptions,
  ) {
    this.port = (server.address() as AddressInfo).port;
    this.url = `http://127.0.0.1:${this.port}`;
    this.apiKey = options.apiKey ?? `twin-api-key-${randomBytes(16).toString("hex")}`;
    server.on("request", (request: IncomingMessage, response: ServerResponse) =>
      this.handle(request, response),
    );
    this.sweeper = setInterval(() => this.sweep(), 250);
  }

  get sessionsCreated(): number {
    return this.created;
  }

  get deletes(): number {
    return this.deleted;
  }

  get enqueues(): number {
    return this.enqueued;
  }

  get sessions(): ReadonlyMap<string, TwinSession> {
    return new Map(
      [...this.records].map(([id, session]) => [
        id,
        { state: session.state, connected: session.peer?.opened === true },
      ]),
    );
  }

  close(): Promise<void> {
    if (this.closing !== undefined) return this.closing;
    clearInterval(this.sweeper);
    for (const session of this.records.values()) {
      session.model.close();
      for (const timer of session.timers) clearTimeout(timer);
    }
    for (const link of this.links.values()) clearTimeout(link.hold);
    this.closing = new Promise((resolve) => this.server.close(() => resolve()));
    this.server.closeAllConnections();
    return this.closing;
  }

  [Symbol.asyncDispose](): Promise<void> {
    return this.close();
  }

  private handle(request: IncomingMessage, response: ServerResponse): void {
    this.route(request, response).catch((cause: unknown) => {
      if (response.headersSent) return;
      if (cause instanceof Refusal)
        respond(response, cause.status, { error: { code: cause.code, message: cause.message } });
      else respond(response, 500, { error: { code: "internal", message: String(cause) } });
    });
  }

  private async route(request: IncomingMessage, response: ServerResponse): Promise<void> {
    const url = new URL(request.url ?? "/", this.url);
    const method = request.method ?? "GET";
    const parts = url.pathname.split("/").slice(1).map(decodeURIComponent);
    if (parts[0] === "twin") return this.loopback(method, parts.slice(1), url, request, response);
    if (url.pathname === "/pricing" && method === "GET")
      return respond(response, 200, this.pricing());
    if (url.pathname === "/tokens" && method === "POST")
      return respond(response, 200, this.token(request.headers, json(await read(request))));
    if (parts[0] === "sessions") {
      const [status, body] = await this.coordinator(method, parts.slice(1), request);
      return respond(response, status, body);
    }
    throw refuse(404, "not_found", "no such route");
  }

  private pricing(): JsonObject {
    const rate = this.options.rate ?? { creditsPerSecond: 5, creditsPerDollar: 3000 };
    return {
      settings: { currency_code: "USD", credits_per_dollar: rate.creditsPerDollar },
      models: [
        {
          name: H3.modelName,
          rate: { amount_per_sec: rate.creditsPerSecond, unit: "credits", denomination: "second" },
        },
      ],
    };
  }

  /** `POST /tokens`: the API key buys one bounded session scope, signed HS256. */
  private token(headers: IncomingHttpHeaders, body: unknown): JsonObject {
    const key = headers["reactor-api-key"];
    const expected = Buffer.from(this.apiKey);
    if (
      typeof key !== "string" ||
      Buffer.byteLength(key) !== expected.byteLength ||
      !timingSafeEqual(Buffer.from(key), expected)
    )
      throw refuse(401, "unauthorized", "a valid Reactor-API-Key is required");
    const details = field(body, "authorization_details");
    const detail: unknown = Array.isArray(details) && details.length === 1 ? details[0] : undefined;
    const models = field(detail, "resources", "models", "match");
    const model: unknown = Array.isArray(models) && models.length === 1 ? models[0] : undefined;
    const sessions = field(detail, "constraints", "max_sessions");
    const seconds = field(detail, "constraints", "max_session_duration_seconds");
    const expiresAfter = field(body, "expires_after");
    if (
      field(detail, "type") !== "session" ||
      typeof model !== "string" ||
      !positive(sessions, 0xffffffff) ||
      !positive(seconds, 86_400) ||
      !positive(expiresAfter, Number.MAX_SAFE_INTEGER)
    )
      throw refuse(400, "invalid_request", "the twin grants one bounded session scope per token");
    const issuedAt = Math.floor(Date.now() / 1000);
    const grant: Grant = {
      id: `twin-jwt-${randomUUID()}`,
      model,
      maxSessions: this.options.faults?.overgrant === true ? 2 : sessions,
      maxSessionSeconds: seconds,
      expiresAt: issuedAt + expiresAfter,
      sessions: 0,
    };
    this.grants.set(grant.id, grant);
    return { jwt: this.sign(grant, issuedAt), expires_at: grant.expiresAt };
  }

  /** The claims say plainly that the twin made the token, for anyone who decodes one. */
  private sign(grant: Grant, issuedAt: number): string {
    const encode = (value: JsonObject) => Buffer.from(JSON.stringify(value)).toString("base64url");
    const signed = `${encode({ alg: "HS256", typ: "JWT", kid: "reactor-twin" })}.${encode({
      iss: "reactor-twin",
      aud: this.url,
      sub: "reactor-twin-account",
      iat: issuedAt,
      exp: grant.expiresAt,
      jti: grant.id,
      authorization_details: [
        {
          type: "session",
          resources: { models: { match: [grant.model] } },
          constraints: {
            max_sessions: grant.maxSessions,
            max_session_duration_seconds: grant.maxSessionSeconds,
          },
        },
      ],
    })}`;
    return `${signed}.${createHmac("sha256", this.secret).update(signed).digest("base64url")}`;
  }

  /** The grant a request's bearer token proves; a missing, foreign or expired one is 401. */
  private verify(headers: IncomingHttpHeaders): Grant {
    const token = /^Bearer ([\w-]+)\.([\w-]+)\.([\w-]+)$/.exec(headers.authorization ?? "");
    const unauthorized = refuse(401, "unauthorized", "a valid bearer token is required");
    if (token === null) throw unauthorized;
    const signature = Buffer.from(token[3]!, "base64url");
    const expected = createHmac("sha256", this.secret).update(`${token[1]}.${token[2]}`).digest();
    if (signature.byteLength !== expected.byteLength || !timingSafeEqual(signature, expected))
      throw unauthorized;
    const id = field(json(Buffer.from(token[2]!, "base64url")), "jti");
    const grant = typeof id === "string" ? this.grants.get(id) : undefined;
    if (grant === undefined || grant.expiresAt * 1000 <= Date.now()) throw unauthorized;
    return grant;
  }

  /** The session and signaling routes under `/sessions`. */
  private async coordinator(
    method: string,
    parts: readonly string[],
    request: IncomingMessage,
  ): Promise<readonly [number, unknown?]> {
    const signaling = parts[1] === "transport";
    if (request.headers["reactor-api-version"] !== "1")
      throw refuse(426, "unsupported_version", "Reactor-API-Version 1 is required");
    if (signaling && request.headers["reactor-webrtc-version"] !== "1.0")
      throw refuse(426, "unsupported_version", "Reactor-WebRTC-Version 1.0 is required");
    const grant = this.verify(request.headers);
    if (parts.length === 0) {
      if (method !== "POST") throw refuse(405, "method_not_allowed", method);
      return [200, this.create(grant, json(await read(request)))];
    }
    const session = this.records.get(parts[0]!);
    if (session === undefined) throw refuse(404, "not_found", "no such session");
    if (session.grant !== grant)
      throw refuse(403, "forbidden", "the token does not grant this session");
    if (parts.length === 1) {
      if (method === "GET") return [200, this.describe(session)];
      if (method !== "DELETE") throw refuse(405, "method_not_allowed", method);
      this.remove(session);
      return [202];
    }
    if (parts[1] === "uploads" && parts.length === 2 && method === "POST")
      return [200, this.allocate(json(await read(request)))];
    const [transport, protocol, resource, cid, operation] = parts.slice(1);
    if (transport !== "transport" || protocol !== "webrtc")
      throw refuse(404, "not_found", "no such route");
    if (resource === "ice_servers" && cid === undefined && method === "GET")
      return [200, this.iceServers()];
    if (resource !== "connections") throw refuse(404, "not_found", "no such route");
    if (session.state !== "ACTIVE") throw refuse(409, "session_ended", "the session has ended");
    if (cid === undefined) {
      if (method !== "POST") throw refuse(405, "method_not_allowed", method);
      await read(request);
      const connection: Connection = {
        id: this.nextConnection++,
        candidates: [],
        offer: undefined,
      };
      session.connections.set(connection.id, connection);
      return [200, { connection_id: connection.id }];
    }
    const connection = session.connections.get(Number(cid));
    if (connection === undefined || parts.length !== 6)
      throw refuse(404, "not_found", "no such connection");
    if (operation === "ice_candidates" && method === "POST") {
      const candidates = field(json(await read(request)), "candidates");
      if (!Array.isArray(candidates)) throw refuse(400, "invalid_request", "candidates");
      for (const entry of candidates as unknown[]) {
        const line = field(entry, "candidate");
        const candidate = typeof line === "string" ? parseCandidate(line) : undefined;
        if (candidate !== undefined) connection.candidates.push(candidate);
      }
      return [204];
    }
    if (operation !== "sdp_params") throw refuse(404, "not_found", "no such route");
    if (method === "GET") {
      const offer = connection.offer;
      if (offer === undefined || Date.now() < offer.readyAt) return [202];
      return [200, { sdp_answer: offer.answer, connection_id: connection.id }];
    }
    if (method !== "POST" && method !== "PUT") throw refuse(405, "method_not_allowed", method);
    this.offer(session, connection, json(await read(request)));
    return [204];
  }

  private create(grant: Grant, body: unknown): JsonObject {
    const model = field(body, "model", "name");
    const transports = field(body, "supported_transports");
    if (typeof model !== "string" || !Array.isArray(transports))
      throw refuse(400, "invalid_request", "a model and supported transports are required");
    if (model !== H3.modelName) throw refuse(404, "unknown_model", "the twin serves H3 only");
    if (model !== grant.model)
      throw refuse(403, "forbidden", "the token does not grant this model");
    if (
      !(transports as unknown[]).some(
        (entry) => field(entry, "protocol") === "webrtc" && field(entry, "version") === "1.0",
      )
    )
      throw refuse(400, "unsupported_transport", "the twin speaks WebRTC 1.0 only");
    if (grant.sessions >= grant.maxSessions)
      throw refuse(403, "session_limit", "the token's sessions are used");
    grant.sessions++;
    this.created++;
    const faults = this.options.faults ?? {};
    const session: Session = {
      id: `sess_${randomUUID()}`,
      grant,
      state: "ACTIVE",
      model: new H3Model(
        {
          send: (channel, bytes) => this.deliver(session, { type: "message", channel, bytes }),
          changed: () => this.deliver(session, { type: "media" }),
          enqueued: () => {
            this.enqueued++;
          },
        },
        faults,
      ),
      connections: new Map(),
      peer: undefined,
      timers: new Set(),
    };
    this.records.set(session.id, session);
    // The server-side cap: the session ends at its granted length, whatever the client does.
    const cap = Math.min(grant.maxSessionSeconds, this.options.capSeconds ?? Infinity);
    this.later(session, cap * 1000, () => this.end(session));
    return this.describe(session);
  }

  private describe(session: Session): JsonObject {
    return {
      session_id: session.id,
      state: session.state,
      capabilities: { protocol_version: "1.0", tracks: [...tracks], emission_fps: 24 },
      selected_transport: { protocol: "webrtc", version: "1.0" },
      cluster: "loopback",
      zone: "local",
      server_info: { server_version: "reactor-twin" },
    };
  }

  private later(session: Session, ms: number, body: () => void): void {
    const timer = setTimeout(() => {
      session.timers.delete(timer);
      body();
    }, ms);
    session.timers.add(timer);
  }

  /** DELETE ends the session at once, so the client's read after it confirms, fault aside. */
  private remove(session: Session): void {
    this.deleted++;
    const faults = this.options.faults ?? {};
    if (session.state !== "ACTIVE" || faults.ignoreDelete === true) return;
    if (faults.slowDelete !== true) return this.end(session);
    session.state = "STOPPING";
    this.later(session, stoppingMs, () => this.end(session));
  }

  private end(session: Session): void {
    if (session.state === "CLOSED") return;
    session.state = "CLOSED";
    session.model.close();
    for (const timer of session.timers) clearTimeout(timer);
    session.timers.clear();
    if (session.peer !== undefined) this.closeLink(session.peer, "ended");
  }

  private iceServers(): JsonObject {
    return {
      ice_servers: [
        { uris: [`stun:127.0.0.1:${this.port}`], credentials: null },
        {
          uris: [`turn:127.0.0.1:${this.port}?transport=udp`],
          credentials: { username: "reactor-twin", password: this.turnPassword },
        },
      ],
    };
  }

  private allocate(body: unknown): JsonObject {
    const name = field(body, "name");
    const size = field(body, "size");
    if (typeof name !== "string" || typeof field(body, "mime_type") !== "string")
      throw refuse(400, "invalid_request", "an upload needs a name, MIME type and size");
    if (!positive(size, 64 * 1024 * 1024))
      throw refuse(400, "invalid_request", "an upload needs a name, MIME type and size");
    const id = randomUUID();
    this.uploads.set(id, size);
    return {
      presigned_id: id,
      presigned_url: `${this.url}/twin/uploads/${id}`,
      path: `uploads/${id}/${name}`,
    };
  }

  /** An offer names its peer; the peer replaces whatever connection the session had. */
  private offer(session: Session, connection: Connection, body: unknown): void {
    const description = field(body, "sdp_offer");
    const mapping = field(body, "track_mapping");
    const peer = typeof description === "string" ? peerOf(description) : undefined;
    if (peer === undefined || !Array.isArray(mapping))
      throw refuse(400, "invalid_offer", "the offer names no twin peer");
    if (this.links.has(peer)) throw refuse(409, "peer_bound", "the peer is already bound");
    const tracksOffered = (mapping as unknown[]).map((entry): Mapping => {
      const name = field(entry, "name"),
        kind = field(entry, "kind"),
        direction = field(entry, "direction"),
        mid = field(entry, "mid");
      if (
        typeof name !== "string" ||
        (kind !== "audio" && kind !== "video") ||
        (direction !== "recvonly" && direction !== "sendonly") ||
        typeof mid !== "string"
      )
        throw refuse(400, "invalid_offer", "the track mapping is malformed");
      return { name, kind, direction, mid };
    });
    if (session.peer !== undefined) this.closeLink(session.peer, "replaced");
    const link: Link = {
      id: peer,
      session,
      connection,
      outbox: [],
      seq: 0,
      live: true,
      opened: false,
      lastSeen: Date.now(),
      waiting: undefined,
      hold: undefined,
      flushing: false,
    };
    this.links.set(peer, link);
    session.peer = link;
    connection.offer = {
      readyAt: Date.now() + negotiationMs,
      answer: answer(peer, tracksOffered, this.remote()),
    };
  }

  private remote(): Candidate {
    return { type: "host", address: "127.0.0.1", port: this.port };
  }

  /** Connectivity checks: a direct pair, or with `relay` only a relay pair works. */
  private select(candidates: readonly Candidate[]): Candidate | undefined {
    if (this.options.relay === true) return candidates.find((entry) => entry.type === "relay");
    return (
      candidates.find((entry) => entry.type === "host") ??
      candidates.find((entry) => entry.type !== "relay") ??
      candidates[0]
    );
  }

  /** The routes under `/twin`: the peer link and upload transfers. */
  private async loopback(
    method: string,
    parts: readonly string[],
    url: URL,
    request: IncomingMessage,
    response: ServerResponse,
  ): Promise<void> {
    if (parts[0] === "uploads" && parts.length === 2 && method === "PUT") {
      const expected = this.uploads.get(parts[1]!);
      if (expected === undefined) throw refuse(404, "not_found", "no such upload");
      const bytes = await read(request, expected);
      if (bytes.byteLength !== expected)
        throw refuse(400, "size_mismatch", "the upload size differs");
      return respond(response, 200);
    }
    const link = parts[0] === "peers" ? this.links.get(parts[1] ?? "") : undefined;
    if (link === undefined) throw refuse(404, "not_found", "no such peer");
    if (parts.length === 3 && parts[2] === "events" && method === "GET")
      return this.poll(link, Number(url.searchParams.get("after") ?? "0"), response);
    if (parts.length === 4 && parts[2] === "channels" && method === "POST") {
      const channel = parts[3];
      if (channel !== "control" && channel !== "data")
        throw refuse(404, "not_found", "no such channel");
      const bytes = await read(request, maxMessage);
      if (!link.live) throw refuse(410, "gone", "the connection is closed");
      if (!link.opened) throw refuse(409, "not_open", "the channel is not open yet");
      link.lastSeen = Date.now();
      try {
        link.session.model.receive(channel, bytes);
      } catch {
        throw refuse(400, "malformed", "the channel message does not decode");
      }
      return respond(response, 204);
    }
    if (parts.length === 2 && method === "DELETE") {
      // The peer closed its side: nothing more is delivered to it.
      this.detach(link);
      this.links.delete(link.id);
      return respond(response, 204);
    }
    throw refuse(404, "not_found", "no such route");
  }

  private poll(link: Link, after: number, response: ServerResponse): void {
    link.lastSeen = Date.now();
    if (!link.opened && link.live) this.open(link);
    while (link.outbox.length > 0 && link.outbox[0]!.seq <= after) link.outbox.shift();
    if (link.outbox.length > 0 || !link.live) return this.answerPoll(link, response);
    this.release(link);
    link.waiting = response;
    link.hold = setTimeout(() => this.release(link), holdMs);
    response.on("close", () => {
      if (link.waiting !== response) return;
      clearTimeout(link.hold);
      link.waiting = undefined;
      link.lastSeen = Date.now();
    });
  }

  /** The first poll opens both channels on the pair the connectivity checks selected. */
  private open(link: Link): void {
    link.opened = true;
    const local = this.select(link.connection.candidates);
    if (local === undefined) return this.closeLink(link, "unreachable");
    this.push(link, { type: "open", local, remote: this.remote() });
    this.push(link, { type: "media" });
    // As on connect, the model broadcasts its full queues and state.
    link.session.model.connected();
  }

  private answerPoll(link: Link, response: ServerResponse): void {
    const now = Date.now();
    const events = link.outbox.slice(0, 256).map(({ seq, event }): EncodedEvent => {
      switch (event.type) {
        case "open":
          return { seq, type: "open", local: event.local, remote: event.remote };
        case "message":
          return {
            seq,
            type: "message",
            channel: event.channel,
            data: Buffer.from(event.bytes).toString("base64"),
          };
        case "media":
          return { seq, type: "media", media: link.session.model.media(now) };
        case "closed":
          return { seq, type: "closed", reason: event.reason };
      }
    });
    link.lastSeen = now;
    respond(response, events.length === 0 && !link.live ? 410 : 200, { events });
  }

  /** Answer a held poll with whatever is queued, or nothing. */
  private release(link: Link): void {
    const waiting = link.waiting;
    if (waiting === undefined || this.closing !== undefined) return;
    clearTimeout(link.hold);
    link.waiting = undefined;
    this.answerPoll(link, waiting);
  }

  private push(link: Link, event: Outgoing): void {
    link.outbox.push({ seq: ++link.seq, event });
    if (link.outbox.length > maxOutbox) {
      // A peer this far behind has lost the connection.
      link.outbox.length = 0;
      this.detach(link);
      return;
    }
    // What one turn pushes, such as a reply and the events after it, goes out together.
    if (link.waiting === undefined || link.flushing) return;
    link.flushing = true;
    setImmediate(() => {
      link.flushing = false;
      this.release(link);
    });
  }

  /** Model output reaches the session's open connection; before a channel opens it is lost. */
  private deliver(session: Session, event: Outgoing): void {
    const link = session.peer;
    if (link?.live === true && link.opened) this.push(link, event);
  }

  private detach(link: Link): void {
    link.live = false;
    if (link.session.peer === link) link.session.peer = undefined;
  }

  private closeLink(link: Link, reason: "replaced" | "ended" | "unreachable"): void {
    if (!link.live) return;
    this.push(link, { type: "closed", reason });
    this.detach(link);
    this.release(link);
  }

  /** A peer that stopped polling is disconnected; the session and its model go on. */
  private sweep(): void {
    const now = Date.now();
    for (const link of this.links.values()) {
      if (link.waiting !== undefined) continue;
      if (now - link.lastSeen <= (link.opened ? livenessMs : firstPollMs)) continue;
      this.detach(link);
      this.links.delete(link.id);
    }
  }
}

/** Start a twin on an ephemeral loopback port. Close it to stop every session and timer. */
export const startTwin = async (options: TwinOptions = {}): Promise<Twin> => {
  const server = createServer();
  await new Promise<void>((resolve, reject) => {
    server.once("error", reject);
    server.listen(0, "127.0.0.1", () => {
      server.off("error", reject);
      resolve();
    });
  });
  return new TwinServer(server, options);
};
