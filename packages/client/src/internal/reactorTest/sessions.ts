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
import type { Billing, Entry, Options, SessionInfo } from "../../ReactorTest.js";
import * as Wire from "../wire.js";
import * as Faults from "./faults.js";
import { deployment, documented } from "./h3.js";
import type { Link } from "./peer.js";
import * as Playout from "./playout.js";
import type { Sampler } from "./timing.js";

const Id = Schema.String.check(Schema.isNonEmpty());
/**
 * A session-scoped token request, as Reactor's authentication docs state it:
 * one `session` entry naming at least one model, optionally sessions to bind,
 * up to 500 sessions and sessions of one second to a day.
 */
export const Authorization = Schema.Struct({
  type: Schema.Literal("session"),
  resources: Schema.Struct({
    models: Schema.Struct({ match: Schema.NonEmptyArray(Id) }),
    sessions: Schema.optionalKey(Schema.Struct({ bind: Schema.NonEmptyArray(Id) })),
  }),
  constraints: Schema.optionalKey(
    Schema.Struct({
      max_sessions: Schema.optionalKey(
        Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 500 })),
      ),
      max_session_duration_seconds: Schema.optionalKey(
        Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 86_400 })),
      ),
    }),
  ),
});

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
/** Reactor caps a clip of a session's last seconds at five minutes by default. */
const maxClipSeconds = 300;
/** A recording's segments, each two seconds of it. */
const segmentSeconds = 2;
const segmentBytes = 1_024;
/** Where a recording's segments are served: another origin, as a CDN's would be. */
export const clips = "https://clips.reactor.test";

/** A clip or recording the recorder holds. */
interface Recording {
  readonly sessionId: string;
  /** Monotonic milliseconds from which its playlist is served. */
  readonly readyAt: number;
  readonly segments: number;
}
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

/**
 * A registered connection slot. Several can carry one session at once; an
 * offer on a slot binds its transport, and a later offer on the same slot,
 * a reconnect, replaces it.
 */
export interface Slot {
  /** Once offered, the answer and when it is ready. */
  readonly answer: { readonly at: number; readonly sdp: string } | undefined;
  /** The transport the slot's last offer named. */
  readonly link: Link | undefined;
  /** Its transport is connected and both its channels are open. */
  readonly open: boolean;
}

interface State {
  readonly phase: Phase;
  /** Times the session lost its last open connection, so a 30 s window closes on the latest. */
  readonly drops: number;
  readonly activeAt: number | undefined;
  readonly endedAt: number | undefined;
  readonly deletes: number;
  readonly connections: ReadonlyMap<number, Slot>;
}

const withSlot = (state: State, cid: number, slot: Slot): State => ({
  ...state,
  connections: new Map(state.connections).set(cid, slot),
});
const anyOpen = (state: State): boolean =>
  [...state.connections.values()].some((slot) => slot.open);

interface Session {
  readonly id: string;
  readonly model: string;
  /** The SDK that created it, as it named itself. */
  readonly client: SessionInfo["client"];
  /** The token that created it, which acts on it without a bind. */
  readonly creator: string;
  readonly maxSessionSeconds: number | undefined;
  readonly expiresAt: number;
  readonly state: Ref.Ref<State>;
  readonly scope: Scope.Closeable;
  readonly playout: Playout.Playout;
}

const descriptor = (id: string, phase: Phase) => ({
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
            { name: documented.tracks.video, kind: "video", direction: "recvonly" },
            { name: documented.tracks.audio, kind: "audio", direction: "recvonly" },
          ],
          emission_fps: documented.fps,
        },
        selected_transport: { protocol: "webrtc", version: "1.0" },
      }
    : {}),
});

export const make = Effect.fnUntraced(function* (options: Options, timing: Sampler) {
  const scope = yield* Effect.scope;
  const lifetimes = yield* FiberSet.make<void>();
  const faults = yield* Faults.make(options.faults);
  const grants = yield* Ref.make<ReadonlyMap<string, Grant>>(new Map());
  const sessions = yield* Ref.make<ReadonlyMap<string, Session>>(new Map());
  const peers = yield* Ref.make<ReadonlyMap<string, Link>>(new Map());
  /** The session and slot each test peer's last offer bound it to. */
  const bindings = yield* Ref.make<
    ReadonlyMap<string, { readonly session: Session; readonly cid: number }>
  >(new Map());
  const counts = yield* Ref.make({
    grants: 0,
    sessions: 0,
    connections: 1000,
    peers: 0,
    recordings: 0,
  });
  /** The account's session-creation bucket: `burst` at once, refilled at the per-minute rate. */
  const bucket = yield* Ref.make({ tokens: burst, at: 0 });
  const entries = yield* Ref.make<ReadonlyArray<Entry>>([]);
  /** Upload slots handed out and not yet filled, with the session that asked for each. */
  const slots = yield* Ref.make<ReadonlyMap<string, string>>(new Map());
  const uploads = yield* Ref.make(0);
  const recordings = yield* Ref.make<ReadonlyMap<string, Recording>>(new Map());
  const openapi = yield* Schema.decodeUnknownEffect(Wire.StructJson)(
    deployment(options.referenceAudio),
  ).pipe(Effect.orDie);

  const count = (key: "grants" | "sessions" | "connections" | "peers" | "recordings") =>
    Ref.modify(counts, (all) => [all[key] + 1, { ...all, [key]: all[key] + 1 }] as const);
  const log = (entry: Omit<Entry, "at">) =>
    Effect.flatMap(Playout.monotonic, (at) =>
      Ref.update(entries, (all) => [...all.slice(-9_999), { at, ...entry }]),
    );
  const later = (ms: number, effect: Effect.Effect<void>) =>
    FiberSet.run(lifetimes, Effect.sleep(Duration.millis(ms)).pipe(Effect.andThen(effect)));
  const setBinding = (
    peerId: string,
    binding: { readonly session: Session; readonly cid: number } | undefined,
  ) =>
    Ref.update(bindings, (all) => {
      const next = new Map(all);
      if (binding !== undefined) next.set(peerId, binding);
      else next.delete(peerId);
      return next;
    });

  /**
   * A session token, or the API key where Reactor takes it as the bearer:
   * reading a session and ending one. Paid run tokens 7bc779d4 read an
   * unknown session with the key (404) and ended a live one (200); Reactor's
   * docs name the key for `DELETE` only, so its other calls refuse it.
   */
  const authorize = (jwt: string | undefined, access: "token" | "key") =>
    Effect.gen(function* () {
      if (jwt !== undefined && jwt === options.apiKey && access === "key") return "key" as const;
      const grant = jwt === undefined ? undefined : (yield* Ref.get(grants)).get(jwt);
      if (grant === undefined || grant.expiresAt * 1000 <= (yield* Clock.currentTimeMillis))
        return yield* refuse(401, "unauthorized", "a valid session token is required");
      return grant;
    });
  /**
   * A session the bearer may act on. `live` refuses one not ACTIVE, or not
   * ACTIVE or INACTIVE when a connection may return; `key` admits the API key.
   */
  const owned = (
    jwt: string | undefined,
    id: string,
    live?: "active" | "connectable",
    access: "token" | "key" = "token",
  ) =>
    Effect.gen(function* () {
      const grant = yield* authorize(jwt, access);
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
    Effect.flatMap(Ref.get(session.state), (state) => {
      const slot = state.connections.get(cid);
      return slot === undefined
        ? Effect.fail(refuse(404, "not_found", "no such connection"))
        : Effect.succeed(slot);
    });

  /** The slot's transport stops carrying the session: it closes, or another replaces it. */
  const unlink = (
    session: Session,
    cid: number,
    link: Link,
    reason: "ended" | "replaced" | "disconnected",
  ): Effect.Effect<void> =>
    Effect.gen(function* () {
      const unbound = yield* Ref.modify(session.state, (state) => {
        const slot = state.connections.get(cid);
        return slot?.link === link
          ? ([true, withSlot(state, cid, { ...slot, link: undefined, open: false })] as const)
          : ([false, state] as const);
      });
      if (!unbound) return;
      yield* setBinding(link.id, undefined);
      yield* session.playout.disconnect(cid);
      yield* log({ sessionId: session.id, kind: "session", name: reason });
      yield* link.drop(reason);
      if (reason !== "disconnected") return;
      // Reactor ends a session 30 s after its last connection drops, unless one returns.
      // Meanwhile hosted Reactor reads it INACTIVE (paid run tokens 83d17eb7), still billing.
      const drops = yield* Ref.modify(session.state, (state) => {
        if (anyOpen(state)) return [undefined, state] as const;
        const next = {
          ...state,
          phase: state.phase === "ACTIVE" ? ("INACTIVE" as const) : state.phase,
          drops: state.drops + 1,
        };
        return [next.drops, next] as const;
      });
      if (drops === undefined) return;
      yield* later(
        reconnectWindowMs,
        Effect.flatMap(Ref.get(session.state), (state) =>
          !anyOpen(state) && state.drops === drops ? end(session, "abandoned") : Effect.void,
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
      for (const [cid, slot] of previous.connections)
        if (slot.link !== undefined) yield* unlink(session, cid, slot.link, "ended");
      yield* log({ sessionId: session.id, kind: "session", name: reason });
      yield* Scope.close(session.scope, Exit.void);
    });
  /**
   * A clip of the session's last `seconds`, or a recording of all of it so
   * far: its playlist is ready as the clip predicts, unless a fault makes it late.
   */
  const record = (session: Session, kind: "snap" | "recording", seconds: number) =>
    Effect.gen(function* () {
      const now = yield* Playout.monotonic;
      const { activeAt } = yield* Ref.get(session.state);
      const elapsed = activeAt === undefined ? 0 : (now - activeAt) / 1000;
      const start = kind === "snap" ? Math.max(0, elapsed - Math.min(seconds, maxClipSeconds)) : 0;
      const late = yield* faults.trip((fault) => fault._tag === "LateRecording");
      const id = `rec_reactor_test_${yield* count("recordings")}`;
      yield* Ref.update(recordings, (all) =>
        new Map(all).set(id, {
          sessionId: session.id,
          readyAt: now + (late?._tag === "LateRecording" ? Duration.toMillis(late.by) : 0),
          segments: Math.max(1, Math.ceil((elapsed - start) / segmentSeconds)),
        }),
      );
      yield* log({ sessionId: session.id, kind: "session", name: `${kind} ${id}` });
      return {
        sessionId: session.id,
        kind,
        startMarker: start,
        endMarker: elapsed,
        nowMarker: elapsed,
        // A test clock can stand between two milliseconds, and a BigInt takes whole ones.
        predictedReadyAtMs: BigInt(Math.round(yield* Clock.currentTimeMillis)),
        playlistUrl: `/clips/${id}.m3u8`,
      };
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
      const uncapped = yield* faults.standing((fault) => fault._tag === "IgnoreCap");
      const cap =
        session.maxSessionSeconds === undefined || uncapped !== undefined
          ? Infinity
          : session.maxSessionSeconds * 1000;
      const early = expire?._tag === "Expire" ? Duration.toMillis(expire.after) : Infinity;
      const lifetime = Math.min(cap, early);
      if (Number.isFinite(lifetime)) yield* later(lifetime, end(session, "expired"));
    });
  /** Connectivity succeeded for the transport a slot is bound to. */
  const open = (session: Session, cid: number, link: Link) =>
    Effect.gen(function* () {
      const state = yield* Ref.get(session.state);
      const slot = state.connections.get(cid);
      if (state.phase !== "ACTIVE" || slot?.link !== link) return;
      yield* link.open;
      yield* Ref.update(session.state, (current) =>
        withSlot(current, cid, { ...slot, open: true }),
      );
      yield* log({ sessionId: session.id, kind: "session", name: "connected" });
      yield* session.playout.connect(cid, link);
      const fault = yield* faults.trip((candidate) => candidate._tag === "Disconnect");
      if (fault?._tag === "Disconnect")
        yield* later(Duration.toMillis(fault.after), unlink(session, cid, link, "disconnected"));
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
        const bound = (yield* Ref.get(bindings)).get(peerId);
        yield* Ref.update(peers, (all) => new Map([...all].filter(([key]) => key !== peerId)));
        if (link !== undefined && bound !== undefined)
          yield* unlink(bound.session, bound.cid, link, "disconnected");
      }),
    answered: (peerId: string) =>
      Effect.gen(function* () {
        const link = (yield* Ref.get(peers)).get(peerId);
        const bound = (yield* Ref.get(bindings)).get(peerId);
        if (link !== undefined && bound !== undefined)
          yield* later(yield* timing.delay("connect"), open(bound.session, bound.cid, link));
      }),
    receive: (peerId: string, channel: "control" | "data", bytes: Uint8Array) =>
      Effect.gen(function* () {
        const link = (yield* Ref.get(peers)).get(peerId);
        const bound = (yield* Ref.get(bindings)).get(peerId);
        if (link !== undefined && bound !== undefined)
          yield* bound.session.playout.receive(bound.cid, link, channel, bytes);
      }),

    // Coordinator
    pricing: {
      settings: { currency_code: "USD", credits_per_dollar: options.creditsPerDollar },
      models: [
        {
          name: documented.modelName.slice(documented.modelName.lastIndexOf("/") + 1),
          rate: {
            amount_per_sec: options.creditsPerSecond,
            unit: "credits",
            denomination: "second",
          },
        },
      ],
    },
    mint: (
      key: string | undefined,
      authorization: (typeof Authorization)["Type"],
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
        const misgrant = yield* faults.trip((fault) => fault._tag === "OverGrant");
        const how = misgrant?._tag === "OverGrant" ? (misgrant.grant ?? "longer") : undefined;
        // An over-granting token lets its sessions run twice as long as was asked, or uncapped.
        const cap =
          asked === undefined || how === "uncapped"
            ? undefined
            : asked * (how === "longer" ? 2 : 1);
        const granted = how === "bound" ? [...bind, `sess_reactor_test_unasked_${n}`] : bind;
        const grant: Grant = {
          jwt: "",
          models,
          maxSessions:
            authorization.constraints?.max_sessions ?? (bind.length > 0 ? bind.length : 5),
          maxSessionSeconds: cap,
          expiresAt:
            how === "expired"
              ? issuedAt - 1
              : issuedAt + Math.min(expiresAfter ?? 3_600, maxTokenSeconds),
          created: 0,
          bound: new Set(granted),
        };
        const echo = {
          type: "session" as const,
          resources: {
            models: { match: models },
            ...(granted.length > 0 ? { sessions: { bind: granted } } : {}),
          },
          constraints: {
            max_sessions: grant.maxSessions,
            ...(cap !== undefined ? { max_session_duration_seconds: cap } : {}),
            ...(how === "uncapped" ? { max_session_duration_seconds: null } : {}),
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
        return {
          jwt,
          expires_at: grant.expiresAt,
          ...(how === "silent" ? {} : { authorization_details: [echo] }),
        };
      }),
    create: (
      jwt: string | undefined,
      model: string,
      webrtc: boolean,
      client: SessionInfo["client"],
    ) =>
      Effect.gen(function* () {
        const grant = yield* authorize(jwt, "token");
        if (grant === "key")
          return yield* refuse(401, "unauthorized", "a session token is required");
        if (model !== documented.modelName)
          return yield* refuse(404, "unknown_model", "ReactorTest serves H3 only");
        if (!grant.models.includes(model))
          return yield* refuse(403, "forbidden", "the token does not grant this model");
        if (!webrtc) return yield* refuse(400, "unsupported_transport", "WebRTC 1.0 only");
        if ((yield* faults.trip((fault) => fault._tag === "StallAllocation")) !== undefined)
          return yield* Effect.never;
        const spent = grant.created >= grant.maxSessions - grant.bound.size;
        if (
          spent &&
          (yield* faults.trip((fault) => fault._tag === "RepeatSession")) !== undefined
        ) {
          const last = [...(yield* Ref.get(sessions)).values()].findLast(
            (session) => session.creator === grant.jwt,
          );
          if (last !== undefined) return descriptor(last.id, (yield* Ref.get(last.state)).phase);
        }
        if (
          spent &&
          (yield* faults.trip((fault) => fault._tag === "IgnoreSessionLimit")) === undefined
        )
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
          client,
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
            connections: new Map(),
          }),
          playout: yield* Playout.make(id, {
            options,
            faults,
            timing,
            openapi,
            log,
            // The session a moderation verdict ends is this one, built just below. It ends
            // outside its own scope, which the ending closes.
            terminate: Effect.suspend(() => Effect.asVoid(later(0, end(session, "moderated")))),
            record: (kind, seconds) => record(session, kind, seconds),
          }).pipe(Scope.provide(sessionScope)),
        };
        yield* Ref.update(sessions, (all) => new Map(all).set(id, session));
        yield* log({ sessionId: id, kind: "session", name: "created" });
        yield* later(yield* timing.delay("allocation"), activate(session));
        if ((yield* faults.trip((fault) => fault._tag === "UnnamedAllocation")) === undefined)
          return descriptor(id, "PENDING");
        const { session_id: _, ...unnamed } = descriptor(id, "PENDING");
        return unnamed;
      }),
    read: (jwt: string | undefined, id: string) =>
      Effect.gen(function* () {
        const session = yield* owned(jwt, id, undefined, "key");
        if ((yield* faults.trip((fault) => fault._tag === "MissingSession")) !== undefined)
          return yield* refuse(404, "not_found", "no such session");
        return descriptor(id, (yield* Ref.get(session.state)).phase);
      }),
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
        const session = yield* owned(jwt, id, undefined, "key");
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
      }),
    register: (jwt: string | undefined, id: string) =>
      Effect.gen(function* () {
        const session = yield* owned(jwt, id, "connectable");
        if ((yield* faults.trip((fault) => fault._tag === "RefuseConnect")) !== undefined)
          return yield* refuse(403, "connect_refused", "connection refused");
        const cid = yield* count("connections");
        yield* Ref.update(session.state, (state) =>
          withSlot(state, cid, { answer: undefined, link: undefined, open: false }),
        );
        return { connection_id: cid };
      }),
    offer: (jwt: string | undefined, id: string, cid: number, sdp: string, replace: boolean) =>
      Effect.gen(function* () {
        const session = yield* owned(jwt, id, "connectable");
        const refusal = replace
          ? yield* faults.trip((fault) => fault._tag === "RefuseReconnect")
          : undefined;
        if (refusal?._tag === "RefuseReconnect")
          return yield* Refusal.make({
            status: refusal.status ?? 503,
            code: "reconnect_refused",
            reason: "reconnect refused",
            ...(refusal.retryAfter === undefined
              ? {}
              : { retryAfter: Math.ceil(Duration.toSeconds(refusal.retryAfter)) }),
          });
        const previous = (yield* connection(session, cid)).link;
        const peerId = /^a=ice-ufrag:([\w-]+)\r?$/m.exec(sdp)?.[1] ?? "";
        const link = (yield* Ref.get(peers)).get(peerId);
        if (link === undefined)
          return yield* refuse(400, "invalid_offer", "the offer names no peer");
        if (previous !== undefined && previous !== link)
          yield* unlink(session, cid, previous, "replaced");
        yield* setBinding(peerId, { session, cid });
        const at = (yield* Playout.monotonic) + (yield* timing.delay("negotiation"));
        const answer = `v=0\r\no=- 1 2 IN IP4 127.0.0.1\r\ns=reactor-test\r\na=ice-ufrag:${peerId}\r\n`;
        // A connection returning to an INACTIVE session makes it ACTIVE again.
        yield* Ref.update(session.state, (state) =>
          withSlot(
            {
              ...state,
              phase: state.phase === "INACTIVE" ? ("ACTIVE" as const) : state.phase,
            },
            cid,
            { answer: { at, sdp: answer }, link, open: false },
          ),
        );
      }),
    answer: (jwt: string | undefined, id: string, cid: number) =>
      Effect.gen(function* () {
        const { answer } = yield* connection(yield* owned(jwt, id), cid);
        const now = yield* Playout.monotonic;
        return answer !== undefined && answer.at <= now
          ? Option.some({ sdp_answer: answer.sdp, connection_id: cid })
          : Option.none();
      }),
    candidates: (jwt: string | undefined, id: string, cid: number) =>
      Effect.flatMap(owned(jwt, id), (session) => connection(session, cid)),

    /** A recording's HLS playlist for a bearer that acts on its session; none until it is ready. */
    playlist: (jwt: string | undefined, id: string) =>
      Effect.gen(function* () {
        const recording = (yield* Ref.get(recordings)).get(id);
        if (recording === undefined) return yield* refuse(404, "not_found", "no such clip");
        yield* owned(jwt, recording.sessionId);
        if ((yield* Playout.monotonic) < recording.readyAt) return Option.none();
        const media = Array.from(
          { length: recording.segments },
          (_, index) => `#EXTINF:${segmentSeconds.toFixed(3)},\n${clips}/${id}/${index}.m4s`,
        );
        return Option.some(
          [
            "#EXTM3U",
            "#EXT-X-VERSION:7",
            `#EXT-X-TARGETDURATION:${segmentSeconds}`,
            "#EXT-X-PLAYLIST-TYPE:VOD",
            `#EXT-X-MAP:URI="${clips}/${id}/init.mp4"`,
            ...media,
            "#EXT-X-ENDLIST",
            "",
          ].join("\n"),
        );
      }),
    /** A recording's segment, served to whoever holds its URL, as a CDN would. */
    segment: (id: string, file: string) =>
      Effect.gen(function* () {
        const recording = (yield* Ref.get(recordings)).get(id);
        const index = file === "init.mp4" ? -1 : Number.parseInt(file, 10);
        if (
          recording === undefined ||
          (yield* Playout.monotonic) < recording.readyAt ||
          !(file === "init.mp4" || (file === `${index}.m4s` && index < recording.segments))
        )
          return yield* refuse(404, "not_found", "no such segment");
        // Each segment's bytes name it, so a joined download shows its order.
        const bytes = new Uint8Array(index < 0 ? 8 : segmentBytes).fill(index < 0 ? 255 : index);
        return bytes;
      }),

    // Control
    info: Effect.flatMap(Ref.get(sessions), (all) =>
      Effect.forEach([...all.values()], (session) =>
        Effect.map(Ref.get(session.state), (state): SessionInfo => ({
          id: session.id,
          state: state.phase,
          connected: anyOpen(state),
          deletes: state.deletes,
          grant: {
            maxSessionSeconds: session.maxSessionSeconds,
            expiresAt: session.expiresAt,
          },
          client: session.client,
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
      const seconds = billed.reduce((sum, each) => sum + each, 0);
      return {
        seconds,
        usd: (seconds * options.creditsPerSecond) / options.creditsPerDollar,
      } satisfies Billing;
    }),
    log: Ref.get(entries),
    /** Adds to the log, as the coordinator does for each request it serves. */
    note: log,
    inject: faults.arm,
  };
});

export type Sessions = Effect.Success<ReturnType<typeof make>>;
