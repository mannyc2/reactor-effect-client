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
import * as Random from "effect/Random";
import * as Ref from "effect/Ref";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import type { IceServersReply, Registered, SdpAnswer } from "../../Coordinator.js";
import type { Descriptor } from "../../Coordinator.js";
import type { SessionAuthorization } from "../../Coordinator.js";
import { h3ReferenceTurboRealtime as profile } from "../../h3/profile.js";
import { structFromObject } from "../../json.js";
import type { Billing, Entry, Options, SessionInfo } from "../../ReactorTest.js";
import * as Faults from "./faults.js";
import { deployment } from "./h3.js";
import type { Link } from "./peer.js";
import * as Playout from "./playout.js";

/** A request the simulated coordinator refuses, with the status a client sees. */
export class Refusal extends Schema.TaggedError<Refusal>(
  "reactor-effect-client/ReactorTest/Refusal",
)("Refusal", { status: Schema.Int, code: Schema.String, reason: Schema.String }) {}

const refuse = (status: number, code: string, reason: string) =>
  Refusal.make({ status, code, reason });

type Phase = SessionInfo["state"];

interface Grant {
  readonly jwt: string;
  readonly model: string;
  readonly maxSessions: number;
  readonly maxSessionSeconds: number;
  /** Seconds since the epoch. */
  readonly expiresAt: number;
  readonly sessions: number;
}

interface State {
  readonly phase: Phase;
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
  readonly grant: Grant;
  readonly state: Ref.Ref<State>;
  readonly scope: Scope.Closeable;
  readonly playout: Playout.Playout;
}

const descriptor = (id: string, phase: Phase) =>
  ({
    session_id: id,
    state: phase,
    ...(phase === "ACTIVE"
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

export const make = Effect.fnUntraced(function* (options: Options) {
  const scope = yield* Effect.scope;
  const lifetimes = yield* FiberSet.make<void>();
  const faults = yield* Faults.make(options.faults);
  const random = yield* Effect.withFiber((fiber) =>
    Effect.succeed(fiber.getRef(Random.Random)),
  ).pipe(Random.withSeed(options.seed));
  const grants = yield* Ref.make<ReadonlyMap<string, Grant>>(new Map());
  const sessions = yield* Ref.make<ReadonlyMap<string, Session>>(new Map());
  const peers = yield* Ref.make<ReadonlyMap<string, Link>>(new Map());
  const bindings = yield* Ref.make<ReadonlyMap<string, Session>>(new Map());
  const counts = yield* Ref.make({ grants: 0, sessions: 0, connections: 1000, peers: 0 });
  const entries = yield* Ref.make<ReadonlyArray<Entry>>([]);
  const openapi = structFromObject(deployment());

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

  const authorize = (jwt: string | undefined) =>
    Effect.gen(function* () {
      const grant = jwt === undefined ? undefined : (yield* Ref.get(grants)).get(jwt);
      if (grant === undefined || grant.expiresAt * 1000 <= (yield* Clock.currentTimeMillis))
        return yield* refuse(401, "unauthorized", "a valid bearer token is required");
      return grant;
    });
  const owned = (jwt: string | undefined, id: string, active = false) =>
    Effect.gen(function* () {
      const grant = yield* authorize(jwt);
      const session = (yield* Ref.get(sessions)).get(id);
      if (session === undefined) return yield* refuse(404, "not_found", "no such session");
      if (session.grant.jwt !== grant.jwt)
        return yield* refuse(403, "forbidden", "the token does not grant this session");
      const { phase } = yield* Ref.get(session.state);
      if (active && phase !== "ACTIVE")
        return yield* refuse(409, "session_not_active", `the session is ${phase}`);
      return session;
    });
  const connection = (session: Session, cid: number) =>
    Effect.flatMap(Ref.get(session.state), (state) =>
      state.connections.has(cid)
        ? Effect.succeed(state.connections.get(cid))
        : Effect.fail(refuse(404, "not_found", "no such connection")),
    );

  /** The link stops carrying the session: it closes, or the session moves on. */
  const unlink = (session: Session, link: Link, reason: "ended" | "replaced" | "disconnected") =>
    Effect.gen(function* () {
      const previous = yield* Ref.getAndUpdate(session.state, (state) =>
        state.bound === link ? { ...state, bound: undefined, connected: false } : state,
      );
      if (previous.bound !== link) return;
      yield* setBinding(link.id, undefined);
      yield* session.playout.disconnect(link);
      yield* log({ sessionId: session.id, kind: "session", name: reason });
      yield* link.drop(reason);
    });
  const end = (session: Session, reason: string) =>
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
      const cap = session.grant.maxSessionSeconds * 1000;
      const early = expire?._tag === "Expire" ? Duration.toMillis(expire.after) : cap;
      yield* later(Math.min(cap, early), end(session, "expired"));
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
    channelMs: Duration.toMillis(options.channelLatency),
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
          yield* later(Duration.toMillis(options.connect), open(session, link));
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
            amount_per_sec: options.creditsPerSecond,
            unit: "credits",
            denomination: "second",
          },
        },
      ],
    },
    mint: (
      key: string | undefined,
      authorization: (typeof SessionAuthorization)["Type"],
      expiresAfter: number,
    ) =>
      Effect.gen(function* () {
        if (key !== options.apiKey)
          return yield* refuse(401, "unauthorized", "a valid Reactor-API-Key is required");
        const n = yield* count("grants");
        const issuedAt = Math.floor((yield* Clock.currentTimeMillis) / 1000);
        const [{ resources, constraints }] = authorization;
        const claims = {
          iss: "reactor-test",
          iat: issuedAt,
          exp: issuedAt + expiresAfter,
          jti: `reactor-test-grant-${n}`,
          authorization_details: authorization,
        };
        const jwt = [{ alg: "none", typ: "JWT" }, claims, "reactor-test"]
          .map((part) => Encoding.encodeBase64Url(JSON.stringify(part)))
          .join(".");
        const grant: Grant = {
          jwt,
          model: resources.models.match[0],
          maxSessions: constraints.max_sessions,
          maxSessionSeconds: constraints.max_session_duration_seconds,
          expiresAt: claims.exp,
          sessions: 0,
        };
        yield* Ref.update(grants, (all) => new Map(all).set(jwt, grant));
        return { jwt, expires_at: grant.expiresAt };
      }),
    create: (jwt: string | undefined, model: string, webrtc: boolean) =>
      Effect.gen(function* () {
        const grant = yield* authorize(jwt);
        if (model !== profile.modelName)
          return yield* refuse(404, "unknown_model", "ReactorTest serves H3 only");
        if (model !== grant.model)
          return yield* refuse(403, "forbidden", "the token does not grant this model");
        if (!webrtc) return yield* refuse(400, "unsupported_transport", "WebRTC 1.0 only");
        if (grant.sessions >= grant.maxSessions)
          return yield* refuse(403, "session_limit", "the token's sessions are used");
        if ((yield* faults.trip((fault) => fault._tag === "RefuseAllocation")) !== undefined)
          return yield* refuse(403, "allocation_refused", "allocation refused");
        yield* Ref.update(grants, (all) =>
          new Map(all).set(grant.jwt, { ...grant, sessions: grant.sessions + 1 }),
        );
        const id = `sess_reactor_test_${yield* count("sessions")}`;
        const sessionScope = yield* Scope.fork(scope);
        const session: Session = {
          id,
          grant,
          scope: sessionScope,
          state: yield* Ref.make<State>({
            phase: "PENDING",
            activeAt: undefined,
            endedAt: undefined,
            deletes: 0,
            bound: undefined,
            connected: false,
            connections: new Map(),
          }),
          playout: yield* Playout.make(id, { options, faults, random, openapi, log }).pipe(
            Scope.provide(sessionScope),
          ),
        };
        yield* Ref.update(sessions, (all) => new Map(all).set(id, session));
        yield* log({ sessionId: id, kind: "session", name: "created" });
        yield* later(Duration.toMillis(options.allocation), activate(session));
        return descriptor(id, "PENDING");
      }),
    read: (jwt: string | undefined, id: string) =>
      Effect.flatMap(owned(jwt, id), (session) =>
        Effect.map(Ref.get(session.state), (state) => descriptor(id, state.phase)),
      ),
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
        const session = yield* owned(jwt, id, true);
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
        const session = yield* owned(jwt, id, true);
        yield* connection(session, cid);
        const peerId = /^a=ice-ufrag:([\w-]+)\r?$/m.exec(sdp)?.[1] ?? "";
        const link = (yield* Ref.get(peers)).get(peerId);
        if (link === undefined)
          return yield* refuse(400, "invalid_offer", "the offer names no peer");
        const previous = (yield* Ref.get(session.state)).bound;
        if (previous !== undefined && previous !== link)
          yield* unlink(session, previous, "replaced");
        yield* setBinding(peerId, session);
        const at = (yield* Playout.monotonic) + Duration.toMillis(options.negotiation);
        const answer = `v=0\r\no=- 1 2 IN IP4 127.0.0.1\r\ns=reactor-test\r\na=ice-ufrag:${peerId}\r\n`;
        yield* Ref.update(session.state, (state) => ({
          ...state,
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
            maxSessionSeconds: session.grant.maxSessionSeconds,
            expiresAt: session.grant.expiresAt,
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
        usd: (minutes * 60 * options.creditsPerSecond) / options.creditsPerDollar,
      } satisfies Billing;
    }),
    log: Ref.get(entries),
    inject: faults.arm,
  };
});

export type Sessions = Effect.Success<ReturnType<typeof make>>;
