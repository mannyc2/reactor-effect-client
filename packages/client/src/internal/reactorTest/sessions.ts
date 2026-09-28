/**
 * The simulated coordinator's state: token grants, the sessions they allocate
 * and those sessions' lifetimes, and the test peers signaling binds to them.
 */
import * as Clock from "effect/Clock";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Encoding from "effect/Encoding";
import * as Exit from "effect/Exit";
import * as FiberSet from "effect/FiberSet";
import * as Option from "effect/Option";
import * as Ref from "effect/Ref";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import type { IceServersReply, Registered, SdpAnswer } from "../../Coordinator.js";
import type { Descriptor } from "../../Coordinator.js";
import type { SessionAuthorization } from "../../Coordinator.js";
import { h3ReferenceTurboRealtime as profile } from "../h3/profile.js";
import type { Billing, Entry, Options, SessionInfo } from "../../ReactorTest.js";
import * as Wire from "../wire.js";
import * as Faults from "./faults.js";
import { deployment } from "./h3.js";
import type { Link } from "./peer.js";
import * as Playout from "./playout.js";
import type { Sampler } from "./timing.js";

/** A request the simulated coordinator refuses, with the status a client sees. */
export class Refusal extends Schema.TaggedError<Refusal>(
  "reactor-effect-client/ReactorTest/Refusal",
)("Refusal", {
  status: Schema.Int,
  code: Schema.String,
  reason: Schema.String,
  /** Seconds to wait, sent as `Retry-After` and `retry_after_seconds`. */
  retryAfter: Schema.optionalKey(Schema.Int),
}) {}

const refuse = (status: number, code: string, reason: string) =>
  Refusal.make({ status, code, reason });

/** Reactor's ceiling on a token's life: a longer one is clamped without a word. */
const maxTokenSeconds = 21_600;
/** How long Reactor keeps a session that lost its last connection. */
const reconnectWindowMs = 30_000;
/** Sessions an account may create back to back before the per-minute rate applies. */
const burst = 3;

type Phase = SessionInfo["state"];

interface Grant {
  readonly jwt: string;
  readonly models: ReadonlyArray<string>;
  /** Sessions it may create, the bound ones counted in. */
  readonly maxSessions: number;
  /** Undefined for sessions without a cap. */
  readonly maxSessionSeconds: number | undefined;
  /** Seconds since the epoch. */
  readonly expiresAt: number;
  readonly created: number;
  /** Open sessions it acts on besides those it created. */
  readonly bound: ReadonlySet<string>;
}

interface State {
  readonly phase: Phase;
  /** Connections lost with none left, so a 30 s window closes on the latest. */
  readonly drops: number;
  readonly activeAt: number | undefined;
  readonly endedAt: number | undefined;
  readonly deletes: number;
  /** The peer the last SDP offer named, open or not. */
  readonly bound: Link | undefined;
  readonly connected: boolean;
  /** Each registered connection and, once offered, when its answer is ready. */
  readonly connections: ReadonlyMap<number, { readonly at: number; readonly sdp: string }>;
}

interface Session {
  readonly id: string;
  readonly model: string;
  /** The token that created it, which acts on it without a bind. */
  readonly creator: string;
  readonly maxSessionSeconds: number | undefined;
  readonly expiresAt: number;
  readonly state: Ref.Ref<State>;
  readonly scope: Scope.Closeable;
  readonly playout: Playout.Playout;
}

const descriptor = (id: string, phase: Phase) =>
  ({
    session_id: id,
    state: phase,
    // Whether hosted Reactor still describes an INACTIVE session's capabilities and
    // transport is unobserved (paid run tokens 83d17eb7 saw only the state), so the
    // most permissive reading is modelled: they stay, and a reconnect can use them.
    ...(phase === "ACTIVE" || phase === "INACTIVE"
      ? {
          capabilities: {
            protocol_version: "1.0",
            tracks: [
              { name: profile.tracks.video, kind: "video", direction: "recvonly" },
              { name: profile.tracks.audio, kind: "audio", direction: "recvonly" },
            ],
            emission_fps: profile.fps,
          },
          selected_transport: { protocol: "webrtc", version: "1.0" },
        }
      : {}),
  }) satisfies (typeof Descriptor)["Encoded"];

export const make = Effect.fnUntraced(function* (options: Options, timing: Sampler) {
  const scope = yield* Effect.scope;
  const lifetimes = yield* FiberSet.make<void>();
  const faults = yield* Faults.make(options.faults);
  const grants = yield* Ref.make<ReadonlyMap<string, Grant>>(new Map());
  const sessions = yield* Ref.make<ReadonlyMap<string, Session>>(new Map());
  const peers = yield* Ref.make<ReadonlyMap<string, Link>>(new Map());
  const bindings = yield* Ref.make<ReadonlyMap<string, Session>>(new Map());
  const counts = yield* Ref.make({ grants: 0, sessions: 0, connections: 1000, peers: 0 });
  /** The account's session-creation bucket: `burst` at once, refilled at the per-minute rate. */
  const bucket = yield* Ref.make({ tokens: burst, at: 0 });
  const entries = yield* Ref.make<ReadonlyArray<Entry>>([]);
  /** Upload slots handed out and not yet filled, with the session that asked for each. */
  const slots = yield* Ref.make<ReadonlyMap<string, string>>(new Map());
  const uploads = yield* Ref.make(0);
  const openapi = yield* Schema.decodeUnknownEffect(Wire.StructJson)(
    deployment(options.referenceAudio),
  ).pipe(Effect.orDie);

  const count = (key: "grants" | "sessions" | "connections" | "peers") =>
    Ref.modify(counts, (all) => [all[key] + 1, { ...all, [key]: all[key] + 1 }] as const);
  const log = (entry: Omit<Entry, "at">) =>
    Effect.flatMap(Playout.monotonic, (at) =>
      Ref.update(entries, (all) => [...all.slice(-9_999), { at, ...entry }]),
    );
  const later = (ms: number, effect: Effect.Effect<void>) =>
    FiberSet.run(lifetimes, Effect.sleep(Duration.millis(ms)).pipe(Effect.andThen(effect)));
  const setBinding = (peerId: string, session: Session | undefined) =>
    Ref.update(bindings, (all) => {
      const next = new Map(all);
      if (session !== undefined) next.set(peerId, session);
      else next.delete(peerId);
      return next;
    });

  /** A session token, or the API key, which acts on every session of the account. */
  const authorize = (jwt: string | undefined) =>
    Effect.gen(function* () {
      if (jwt !== undefined && jwt === options.apiKey) return "key" as const;
      const grant = jwt === undefined ? undefined : (yield* Ref.get(grants)).get(jwt);
      if (grant === undefined || grant.expiresAt * 1000 <= (yield* Clock.currentTimeMillis))
        return yield* refuse(401, "unauthorized", "a valid bearer token is required");
      return grant;
    });
  /** `live` refuses a session not ACTIVE, or not ACTIVE or INACTIVE when a connection may return. */
  const owned = (jwt: string | undefined, id: string, live?: "active" | "connectable") =>
    Effect.gen(function* () {
      const grant = yield* authorize(jwt);
      const session = (yield* Ref.get(sessions)).get(id);
      if (session === undefined) return yield* refuse(404, "not_found", "no such session");
      if (grant !== "key" && session.creator !== grant.jwt && !grant.bound.has(id))
        return yield* refuse(
          403,
          "forbidden",
          "the token is not bound to this session: authorization_details.resources.sessions.bind",
        );
      const { phase } = yield* Ref.get(session.state);
      const accepted =
        live === undefined ||
        phase === "ACTIVE" ||
        (live === "connectable" && phase === "INACTIVE");
      if (!accepted) return yield* refuse(409, "session_not_active", `the session is ${phase}`);
      return session;
    });
  const connection = (session: Session, cid: number) =>
    Effect.flatMap(Ref.get(session.state), (state) =>
      state.connections.has(cid)
        ? Effect.succeed(state.connections.get(cid))
        : Effect.fail(refuse(404, "not_found", "no such connection")),
    );

  /** The link stops carrying the session: it closes, or the session moves on. */
  const unlink = (
    session: Session,
    link: Link,
    reason: "ended" | "replaced" | "disconnected",
  ): Effect.Effect<void> =>
    Effect.gen(function* () {
      const previous = yield* Ref.getAndUpdate(session.state, (state) =>
        state.bound === link ? { ...state, bound: undefined, connected: false } : state,
      );
      if (previous.bound !== link) return;
      yield* setBinding(link.id, undefined);
      yield* session.playout.disconnect(link);
      yield* log({ sessionId: session.id, kind: "session", name: reason });
      yield* link.drop(reason);
      if (reason !== "disconnected") return;
      // Reactor ends a session 30 s after its last connection drops, unless one returns.
      // Meanwhile hosted Reactor reads it INACTIVE (paid run tokens 83d17eb7), still billing.
      const { drops } = yield* Ref.updateAndGet(session.state, (state) => ({
        ...state,
        phase: state.phase === "ACTIVE" ? ("INACTIVE" as const) : state.phase,
        drops: state.drops + 1,
      }));
      yield* later(
        reconnectWindowMs,
        Effect.flatMap(Ref.get(session.state), (state) =>
          state.bound === undefined && state.drops === drops
            ? end(session, "abandoned")
            : Effect.void,
        ),
      );
    });
  const end = (session: Session, reason: string): Effect.Effect<void> =>
    Effect.gen(function* () {
      const now = yield* Playout.monotonic;
      const previous = yield* Ref.getAndUpdate(session.state, (state) =>
        state.phase === "CLOSED" ? state : { ...state, phase: "CLOSED" as const, endedAt: now },
      );
      if (previous.phase === "CLOSED") return;
      if (previous.bound !== undefined) yield* unlink(session, previous.bound, "ended");
      yield* log({ sessionId: session.id, kind: "session", name: reason });
      yield* Scope.close(session.scope, Exit.void);
    });
  const activate = (session: Session) =>
    Effect.gen(function* () {
      const now = yield* Playout.monotonic;
      const previous = yield* Ref.getAndUpdate(session.state, (state) =>
        state.phase === "PENDING" ? { ...state, phase: "ACTIVE" as const, activeAt: now } : state,
      );
      if (previous.phase !== "PENDING") return;
      yield* log({ sessionId: session.id, kind: "session", name: "active" });
      const expire = yield* faults.standing((fault) => fault._tag === "Expire");
      const cap =
        session.maxSessionSeconds === undefined ? Infinity : session.maxSessionSeconds * 1000;
      const early = expire?._tag === "Expire" ? Duration.toMillis(expire.after) : Infinity;
      const lifetime = Math.min(cap, early);
      if (Number.isFinite(lifetime)) yield* later(lifetime, end(session, "expired"));
    });
  /** Connectivity succeeded for the peer the session is bound to. */
  const open = (session: Session, link: Link) =>
    Effect.gen(function* () {
      const state = yield* Ref.get(session.state);
      if (state.phase !== "ACTIVE" || state.bound !== link) return;
      yield* link.open;
      yield* Ref.update(session.state, (current) => ({ ...current, connected: true }));
      yield* log({ sessionId: session.id, kind: "session", name: "connected" });
      yield* session.playout.connect(link);
      const fault = yield* faults.trip((candidate) => candidate._tag === "Disconnect");
      if (fault?._tag === "Disconnect")
        yield* later(Duration.toMillis(fault.after), unlink(session, link, "disconnected"));
    });

  return {
    options,
    timing,
    // Test peers
    nextPeer: Effect.map(count("peers"), (n) => `reactor-test-peer-${n}`),
    attach: (link: Link) => Ref.update(peers, (all) => new Map(all).set(link.id, link)),
    detach: (peerId: string) =>
      Effect.gen(function* () {
        const link = (yield* Ref.get(peers)).get(peerId);
        const session = (yield* Ref.get(bindings)).get(peerId);
        yield* Ref.update(peers, (all) => new Map([...all].filter(([key]) => key !== peerId)));
        if (link !== undefined && session !== undefined)
          yield* unlink(session, link, "disconnected");
      }),
    answered: (peerId: string) =>
      Effect.gen(function* () {
        const link = (yield* Ref.get(peers)).get(peerId);
        const session = (yield* Ref.get(bindings)).get(peerId);
        if (link !== undefined && session !== undefined)
          yield* later(yield* timing.delay("connect"), open(session, link));
      }),
    receive: (peerId: string, channel: "control" | "data", bytes: Uint8Array) =>
      Effect.gen(function* () {
        const link = (yield* Ref.get(peers)).get(peerId);
        const session = (yield* Ref.get(bindings)).get(peerId);
        if (link !== undefined && session !== undefined)
          yield* session.playout.receive(link, channel, bytes);
      }),

    // Coordinator
    pricing: {
      settings: { currency_code: "USD", credits_per_dollar: options.creditsPerDollar },
      models: [
        {
          name: profile.modelName.slice(profile.modelName.lastIndexOf("/") + 1),
          rate: {
            amount_per_min: options.creditsPerMinute,
            unit: "credits",
            denomination: "minute",
          },
        },
      ],
    },
    mint: (
      key: string | undefined,
      authorization: (typeof SessionAuthorization)["Type"],
      expiresAfter: number | undefined,
    ) =>
      Effect.gen(function* () {
        if (key !== options.apiKey)
          return yield* refuse(401, "unauthorized", "a valid Reactor-API-Key is required");
        const models = authorization.resources.models.match;
        const bind = authorization.resources.sessions?.bind ?? [];
        const all = yield* Ref.get(sessions);
        for (const id of bind) {
          const session = all.get(id);
          const open = session !== undefined && (yield* Ref.get(session.state)).phase !== "CLOSED";
          // A closed, foreign or unknown id is refused alike, so bind cannot find sessions.
          if (!open || !models.includes(session.model))
            return yield* refuse(403, "forbidden", "a bound session is not open for this token");
        }
        const n = yield* count("grants");
        const issuedAt = Math.floor((yield* Clock.currentTimeMillis) / 1000);
        const asked = authorization.constraints?.max_session_duration_seconds;
        // An over-granting token lets its sessions run twice as long as was asked.
        const overGrant = (yield* faults.trip((fault) => fault._tag === "OverGrant")) !== undefined;
        const cap = asked !== undefined && overGrant ? asked * 2 : asked;
        const grant: Grant = {
          jwt: "",
          models,
          maxSessions:
            authorization.constraints?.max_sessions ?? (bind.length > 0 ? bind.length : 5),
          maxSessionSeconds: cap,
          expiresAt: issuedAt + Math.min(expiresAfter ?? 3_600, maxTokenSeconds),
          created: 0,
          bound: new Set(bind),
        };
        const echo = {
          type: "session" as const,
          resources: {
            models: { match: models },
            ...(bind.length > 0 ? { sessions: { bind } } : {}),
          },
          constraints: {
            max_sessions: grant.maxSessions,
            ...(cap === undefined ? {} : { max_session_duration_seconds: cap }),
          },
        };
        // Bound sessions live on the server, not in the token's claims.
        const claims = {
          iss: "reactor-test",
          iat: issuedAt,
          exp: grant.expiresAt,
          jti: `reactor-test-grant-${n}`,
          authorization_details: [{ ...echo, resources: { models: { match: models } } }],
        };
        const jwt = [{ alg: "none", typ: "JWT" }, claims, "reactor-test"]
          .map((part) => Encoding.encodeBase64Url(JSON.stringify(part)))
          .join(".");
        yield* Ref.update(grants, (current) => new Map(current).set(jwt, { ...grant, jwt }));
        return { jwt, expires_at: grant.expiresAt, authorization_details: [echo] };
      }),
    create: (jwt: string | undefined, model: string, webrtc: boolean) =>
      Effect.gen(function* () {
        const grant = yield* authorize(jwt);
        if (grant === "key")
          return yield* refuse(403, "forbidden", "sessions are created with a session token");
        if (model !== profile.modelName)
          return yield* refuse(404, "unknown_model", "ReactorTest serves H3 only");
        if (!grant.models.includes(model))
          return yield* refuse(403, "forbidden", "the token does not grant this model");
        if (!webrtc) return yield* refuse(400, "unsupported_transport", "WebRTC 1.0 only");
        if (grant.created >= grant.maxSessions - grant.bound.size)
          return yield* refuse(403, "session_limit", "the token's sessions are used");
        const refusal = yield* faults.trip((fault) => fault._tag === "RefuseAllocation");
        if (refusal?._tag === "RefuseAllocation")
          return yield* (refusal.status ?? 403) >= 500
            ? refuse(refusal.status ?? 500, "server_error", "allocation failed")
            : refuse(refusal.status ?? 403, "allocation_refused", "allocation refused");
        const running = yield* Effect.filter([...(yield* Ref.get(sessions)).values()], (session) =>
          Effect.map(Ref.get(session.state), (state) => state.phase !== "CLOSED"),
        );
        if (running.length >= options.concurrentSessions)
          return yield* refuse(429, "concurrent_limit", "too many concurrent sessions");
        // A token bucket: `burst` back to back, then one every minute / sessionsPerMinute.
        const now = yield* Playout.monotonic;
        const refillMs = 60_000 / options.sessionsPerMinute;
        const wait = yield* Ref.modify(bucket, (current) => {
          const tokens = Math.min(burst, current.tokens + (now - current.at) / refillMs);
          return tokens >= 1
            ? ([0, { tokens: tokens - 1, at: now }] as const)
            : ([Math.ceil(((1 - tokens) * refillMs) / 1000), { tokens, at: now }] as const);
        });
        if (wait > 0)
          return yield* Refusal.make({
            status: 429,
            code: "rate_limited",
            reason: "too many sessions this minute",
            retryAfter: wait,
          });
        yield* Ref.update(grants, (all) =>
          new Map(all).set(grant.jwt, { ...grant, created: grant.created + 1 }),
        );
        const id = `sess_reactor_test_${yield* count("sessions")}`;
        const sessionScope = yield* Scope.fork(scope);
        const session: Session = {
          id,
          model,
          creator: grant.jwt,
          maxSessionSeconds: grant.maxSessionSeconds,
          expiresAt: grant.expiresAt,
          scope: sessionScope,
          state: yield* Ref.make<State>({
            phase: "PENDING",
            drops: 0,
            activeAt: undefined,
            endedAt: undefined,
            deletes: 0,
            bound: undefined,
            connected: false,
            connections: new Map(),
          }),
          playout: yield* Playout.make(id, {
            options,
            faults,
            timing,
            openapi,
            log,
            // The session a moderation verdict ends is this one, built just below.
            terminate: (afterMs) =>
              Effect.suspend(() => Effect.asVoid(later(afterMs, end(session, "moderated")))),
          }).pipe(Scope.provide(sessionScope)),
        };
        yield* Ref.update(sessions, (all) => new Map(all).set(id, session));
        yield* log({ sessionId: id, kind: "session", name: "created" });
        yield* later(yield* timing.delay("allocation"), activate(session));
        return descriptor(id, "PENDING");
      }),
    read: (jwt: string | undefined, id: string) =>
      Effect.flatMap(owned(jwt, id), (session) =>
        Effect.map(Ref.get(session.state), (state) => descriptor(id, state.phase)),
      ),
    upload: (jwt: string | undefined, id: string, name: string, size: number) =>
      Effect.gen(function* () {
        yield* owned(jwt, id, "active");
        const n = yield* Ref.updateAndGet(uploads, (value) => value + 1);
        const slot = `00000000-0000-4000-8000-${n.toString(16).padStart(12, "0")}`;
        yield* Ref.update(slots, (all) => new Map(all).set(slot, id));
        yield* log({ sessionId: id, kind: "upload", name: `allocated ${name} (${size} bytes)` });
        return slot;
      }),
    stored: (slot: string, bytes: number) =>
      Effect.gen(function* () {
        const sessionId = (yield* Ref.get(slots)).get(slot);
        if (sessionId === undefined) return yield* refuse(404, "not_found", "no such upload");
        yield* log({ sessionId, kind: "upload", name: `stored ${bytes} bytes` });
      }),
    remove: (jwt: string | undefined, id: string) =>
      Effect.gen(function* () {
        const session = yield* owned(jwt, id);
        const state = yield* Ref.updateAndGet(session.state, (current) => ({
          ...current,
          deletes: current.deletes + 1,
        }));
        yield* log({ sessionId: id, kind: "session", name: "deleted" });
        if (state.phase === "CLOSED" || state.phase === "STOPPING") return;
        if ((yield* faults.standing((fault) => fault._tag === "IgnoreDelete")) !== undefined)
          return;
        const slow = yield* faults.standing((fault) => fault._tag === "SlowDelete");
        if (slow?._tag !== "SlowDelete") return yield* end(session, "terminated");
        yield* Ref.update(session.state, (current) => ({ ...current, phase: "STOPPING" as const }));
        yield* later(Duration.toMillis(slow.for), end(session, "terminated"));
      }),
    iceServers: (jwt: string | undefined, id: string) =>
      Effect.as(owned(jwt, id), {
        ice_servers: [{ uris: ["stun:stun.reactor.test:3478"], credentials: null }],
      } satisfies (typeof IceServersReply)["Encoded"]),
    register: (jwt: string | undefined, id: string) =>
      Effect.gen(function* () {
        const session = yield* owned(jwt, id, "connectable");
        if ((yield* faults.trip((fault) => fault._tag === "RefuseConnect")) !== undefined)
          return yield* refuse(403, "connect_refused", "connection refused");
        const cid = yield* count("connections");
        yield* Ref.update(session.state, (state) => ({
          ...state,
          connections: new Map(state.connections).set(cid, { at: Infinity, sdp: "" }),
        }));
        return { connection_id: cid } satisfies (typeof Registered)["Encoded"];
      }),
    offer: (jwt: string | undefined, id: string, cid: number, sdp: string) =>
      Effect.gen(function* () {
        const session = yield* owned(jwt, id, "connectable");
        yield* connection(session, cid);
        const peerId = /^a=ice-ufrag:([\w-]+)\r?$/m.exec(sdp)?.[1] ?? "";
        const link = (yield* Ref.get(peers)).get(peerId);
        if (link === undefined)
          return yield* refuse(400, "invalid_offer", "the offer names no peer");
        const previous = (yield* Ref.get(session.state)).bound;
        if (previous !== undefined && previous !== link)
          yield* unlink(session, previous, "replaced");
        yield* setBinding(peerId, session);
        const at = (yield* Playout.monotonic) + (yield* timing.delay("negotiation"));
        const answer = `v=0\r\no=- 1 2 IN IP4 127.0.0.1\r\ns=reactor-test\r\na=ice-ufrag:${peerId}\r\n`;
        // A connection returning to an INACTIVE session makes it ACTIVE again.
        yield* Ref.update(session.state, (state) => ({
          ...state,
          phase: state.phase === "INACTIVE" ? ("ACTIVE" as const) : state.phase,
          bound: link,
          connected: false,
          connections: new Map(state.connections).set(cid, { at, sdp: answer }),
        }));
      }),
    answer: (jwt: string | undefined, id: string, cid: number) =>
      Effect.gen(function* () {
        const negotiation = yield* connection(yield* owned(jwt, id), cid);
        const now = yield* Playout.monotonic;
        return negotiation !== undefined && negotiation.at <= now
          ? Option.some({
              sdp_answer: negotiation.sdp,
              connection_id: cid,
            } satisfies (typeof SdpAnswer)["Encoded"])
          : Option.none();
      }),
    candidates: (jwt: string | undefined, id: string, cid: number) =>
      Effect.flatMap(owned(jwt, id), (session) => connection(session, cid)),

    // Control
    info: Effect.flatMap(Ref.get(sessions), (all) =>
      Effect.forEach([...all.values()], (session) =>
        Effect.map(Ref.get(session.state), (state): SessionInfo => ({
          id: session.id,
          state: state.phase,
          connected: state.connected,
          deletes: state.deletes,
          grant: {
            maxSessionSeconds: session.maxSessionSeconds,
            expiresAt: session.expiresAt,
          },
        })),
      ),
    ),
    billing: Effect.gen(function* () {
      const now = yield* Playout.monotonic;
      const billed = yield* Effect.forEach([...(yield* Ref.get(sessions)).values()], (session) =>
        Effect.map(Ref.get(session.state), (state) =>
          state.activeAt === undefined ? 0 : ((state.endedAt ?? now) - state.activeAt) / 1000,
        ),
      );
      const minutes = billed.reduce((sum, seconds) => sum + Math.ceil(seconds / 60), 0);
      return {
        seconds: billed.reduce((sum, seconds) => sum + seconds, 0),
        minutes,
        usd: (minutes * options.creditsPerMinute) / options.creditsPerDollar,
      } satisfies Billing;
    }),
    log: Ref.get(entries),
    inject: faults.arm,
  };
});

export type Sessions = Effect.Success<ReturnType<typeof make>>;
