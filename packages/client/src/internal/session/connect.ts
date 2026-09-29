/**
 * Allocation and connection: create or identify the remote session, then
 * negotiate each connection generation (describe, prepare, register, trickle
 * ICE, offer, answer, ready) within its deadline. A failed attempt fails and
 * closes its generation; a reconnect replaces the previous one and never
 * replays a command. Once acquired, a session with a reconnect policy
 * reconnects each connection it drops on its own.
 */
import * as Cause from "effect/Cause";
import * as Clock from "effect/Clock";
import * as Deferred from "effect/Deferred";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as ErrorReporter from "effect/ErrorReporter";
import * as Exit from "effect/Exit";
import * as Option from "effect/Option";
import * as Queue from "effect/Queue";
import * as Ref from "effect/Ref";
import * as Schedule from "effect/Schedule";
import * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import * as SubscriptionRef from "effect/SubscriptionRef";
import type { PeerEvent, PeerFactory } from "../../Peer.js";
import { ReactorError } from "../../ReactorError.js";
import type { ReadyDescriptor } from "../../Session.js";
import { take } from "../queue.js";
import * as Stats from "../stats.js";
import type { Token } from "../token.js";
import type { Generation } from "./generation.js";
import type { Ice } from "./ice.js";
import type { Inbound } from "./inbound.js";
import type { Connection, Core, Known, Link, RemoteSession, State } from "./model.js";
import { failureOr, isClosing, isKnown, newLink, timedOut } from "./model.js";
import type { Requests } from "./requests.js";
import type { Tracks } from "./tracks.js";

/** Marks a connect phase on the current span, as SqlClient marks a transaction's. */
const phase = (name: string): Effect.Effect<void> =>
  Effect.currentSpan.pipe(
    Effect.flatMap((span) =>
      Clock.currentTimeNanos.pipe(Effect.map((now) => span.event(`reactor.connect.${name}`, now))),
    ),
    Effect.ignore,
  );

/**
 * What a failed create leaves: nothing, when the request was never sent or
 * the coordinator refused it (4xx); an unknown allocation after a server
 * error, a timeout or a lost reply.
 */
const afterCreateFailure = (error: ReactorError): RemoteSession | undefined => {
  if (error.context.outcome === "not-submitted") return undefined;
  const status = error.reason._tag === "Http" ? error.reason.status : undefined;
  const refused =
    error.context.outcome === "replied" && status !== undefined && status >= 400 && status < 500;
  return refused ? undefined : { ownership: "unknown" };
};

/** A connection attempt: the first connect, a reconnect asked for, or the session's own. */
type Attempt = "connect" | "reconnect" | "own";

/** The sessions each attempt may begin a generation in. */
const beginsFrom: Record<Attempt, (session: State) => boolean> = {
  connect: (session) => session.status === "idle",
  reconnect: (session) => session.status === "ready" || session.status === "disconnected",
  own: (session) => session.status === "disconnected" && session.reconnecting,
};

/**
 * How long the session's own reconnect waits on a connection it made ready before it counts the
 * connection as back: one that drops sooner fails that reconnect's attempt.
 */
const settle = Duration.seconds(10);

/** Whether no later attempt can reconnect the session after `error`: it ended, or is gone. */
const ends = (error: ReactorError): boolean => {
  switch (error.reason._tag) {
    case "TerminalSession":
    case "Moderated":
      return true;
    case "Http":
      return error.reason.status === 404;
    default:
      return false;
  }
};

export const make = ({
  core,
  generation,
  ice,
  inbound,
  requests,
  tracks,
  peers,
  root,
  token,
  closing,
  resumeTracks,
}: {
  readonly core: Core;
  readonly generation: Generation;
  readonly ice: Ice;
  readonly inbound: Inbound;
  readonly requests: Requests;
  readonly tracks: Tracks;
  readonly peers: PeerFactory["Service"];
  /** The session's scope, which every generation's scope forks from. */
  readonly root: Scope.Scope;
  readonly token: Token;
  /** Fails once the session starts closing, ending an allocation in flight. */
  readonly closing: Deferred.Deferred<never, ReactorError>;
  /** Resume receive-only tracks as each connection becomes ready; Reactor sends no media until then. */
  readonly resumeTracks: boolean;
}) => {
  const { intent, settings, signaling, state, publish, transition } = core;
  const { current, guard, fail, background } = generation;

  /** Adds what a known remote session has learned; an unknown one stays as it is. */
  const learn = (patch: Partial<Omit<Known, "ownership" | "id">>) =>
    SubscriptionRef.update(state, (session): State => ({
      ...session,
      remote: isKnown(session.remote) ? { ...session.remote, ...patch } : session.remote,
    }));

  /** An allocation interrupted or failed without a verdict leaves an unknown session. */
  const allocationUnknown = SubscriptionRef.update(state, (session): State => ({
    ...session,
    remote: session.remote?.ownership === "allocating" ? { ownership: "unknown" } : session.remote,
  }));

  const allocate: Effect.Effect<string, ReactorError> = Effect.uninterruptibleMask((restore) =>
    Effect.gen(function* () {
      const session = yield* SubscriptionRef.get(state);
      if (isClosing(session.status))
        return yield* ReactorError.fromCode("Closed", "session is closed", {
          outcome: "not-submitted",
        });
      if (isKnown(session.remote)) return session.remote.id;
      if (session.remote !== undefined)
        return yield* ReactorError.fromCode(
          "InvalidState",
          "session allocation is already pending or unresolved",
          { outcome: "unknown" },
        );
      if (intent._tag === "Attach") {
        const remote: Known = {
          ownership: intent.adopt ? "owned" : "attached",
          id: intent.sessionId,
          ...(intent.connectionId === undefined ? {} : { connectionId: intent.connectionId }),
        };
        yield* SubscriptionRef.update(state, (held): State => ({ ...held, remote }));
        return remote.id;
      }
      yield* SubscriptionRef.update(state, (held): State => ({
        ...held,
        remote: { ownership: "allocating" },
      }));
      const allocation = yield* restore(
        signaling
          .create(intent.model, intent.extraArgs)
          .pipe(Effect.raceFirst(Deferred.await(closing))),
      ).pipe(
        Effect.tapError((error) =>
          SubscriptionRef.update(state, (held): State => ({
            ...held,
            remote:
              held.remote?.ownership === "allocating" ? afterCreateFailure(error) : held.remote,
          })),
        ),
        Effect.onInterrupt(() => allocationUnknown),
      );
      // Ownership rests on the id alone: a reply that cannot describe the
      // session still names one its owner must terminate.
      yield* SubscriptionRef.update(state, (held): State => ({
        ...held,
        remote: { ownership: "owned", id: allocation.sessionId },
      }));
      yield* token.bind(allocation.sessionId);
      const descriptor = yield* signaling.describe(allocation);
      yield* SubscriptionRef.update(state, (held): State => ({
        ...held,
        remote: { ownership: "owned", id: allocation.sessionId, descriptor },
      }));
      return allocation.sessionId;
    }),
  );

  /** Fails `c` for `cause`, the attempt's own failure if it has one, and closes it. */
  const abandon = (c: Connection, cause: Cause.Cause<ReactorError>) =>
    fail(
      c,
      failureOr(() => ReactorError.fromCode("Aborted", "connection attempt stopped"))(cause),
    ).pipe(Effect.ensuring(Scope.close(c.scope, Exit.void)));

  /**
   * Retires `previous` for the generation that replaced it. Its host's shutdown is not the new
   * attempt's to fail: the host's own failure, which a finalizer that cannot fail dies with, is a
   * Diagnostic on the retired generation, and any other defect is reported as a bug.
   */
  const retire = (previous: Connection) =>
    fail(previous, ReactorError.fromCode("Disconnected", "connection retired for reconnect")).pipe(
      Effect.ensuring(Scope.close(previous.scope, Exit.void)),
      Effect.catchCause((cause) =>
        Effect.gen(function* () {
          const dies = cause.reasons.filter(Cause.isDieReason);
          for (const { defect } of dies)
            if (ReactorError.is(defect))
              yield* publish({ _tag: "Diagnostic", error: defect }, previous.generation);
          const bugs = dies.filter(({ defect }) => !ReactorError.is(defect));
          if (bugs.length > 0) yield* ErrorReporter.report(Cause.fromReasons(bugs));
        }),
      ),
    );

  /**
   * A new generation, with its event fiber; the previous one is retired. It takes the session
   * over in one step with its checks, so no two attempts share a generation and none begins once
   * the session closes. The session's own reconnect begins nothing once another attempt, a ready
   * connection or the close has taken over. An attempt acquires it uninterruptibly, so every
   * generation it claims is one the attempt fails and closes if it goes no further, a defect as
   * it takes over included.
   */
  const begin = Effect.fnUntraced(function* (attempt: Attempt) {
    const session = yield* SubscriptionRef.get(state);
    const reconnect = attempt !== "connect";
    if (!beginsFrom[attempt](session)) {
      if (attempt === "own") return undefined;
      return yield* ReactorError.fromCode("InvalidState", `${attempt} while ${session.status}`);
    }
    if (reconnect && !isKnown(session.remote))
      return yield* ReactorError.fromCode(
        "InvalidState",
        "cannot reconnect without a known session",
      );
    if (reconnect && session.moderated)
      return yield* ReactorError.fromCode("Moderated", "content moderation ended the session", {
        outcome: "not-submitted",
      });
    const scope = yield* Scope.fork(root);
    const peer = yield* Scope.provide(peers.make, scope).pipe(
      Effect.onError((cause) => Scope.close(scope, Exit.failCause(cause))),
    );
    const c: Connection = {
      generation: session.generation + 1n,
      scope,
      peer,
      ready: yield* Deferred.make<void, ReactorError>(),
      failed: yield* Deferred.make<never, ReactorError>(),
      events: yield* Queue.unbounded<PeerEvent>(),
      iceWake: yield* Queue.dropping<void>(1),
      link: yield* Ref.make<Link>(newLink),
    };
    const previous = session.connection;
    const claimed = yield* SubscriptionRef.modify(state, (held) => {
      if (held.connection !== previous || !beginsFrom[attempt](held)) return [false, held] as const;
      const next: State = {
        ...held,
        status: "connecting",
        generation: c.generation,
        connection: c,
        lastError: undefined,
        received: new Set(),
        sampler: Stats.initialSampler,
      };
      return [true, next] as const;
    });
    if (!claimed) {
      yield* Scope.close(scope, Exit.void);
      if (attempt === "own") return undefined;
      const held = yield* SubscriptionRef.get(state);
      return yield* ReactorError.fromCode("InvalidState", `${attempt} while ${held.status}`);
    }
    // The previous generation is retired whatever happens, and a defect meanwhile fails and
    // closes this one.
    yield* Effect.gen(function* () {
      yield* publish({ _tag: "Status", status: "connecting" });
      yield* Scope.addFinalizer(
        scope,
        fail(
          c,
          ReactorError.fromCode("Aborted", "connection scope closed", { generation: c.generation }),
        ),
      );
      yield* take(c.events).pipe(
        Effect.flatMap((event) => inbound.apply(c, event)),
        Effect.forever,
        Effect.forkIn(scope),
      );
    }).pipe(
      Effect.ensuring(previous === undefined ? Effect.void : retire(previous)),
      Effect.onError((cause) => abandon(c, cause)),
    );
    return c;
  });

  /** Negotiates `c` through to ready, and keeps it alive. */
  const negotiate = Effect.fnUntraced(function* (c: Connection, reconnect: boolean) {
    if (!reconnect) yield* guard(c, allocate);
    yield* current(c);
    yield* transition("waiting");
    const known = (yield* SubscriptionRef.get(state)).remote;
    if (!isKnown(known)) return yield* ReactorError.fromCode("InvalidState", "no known session id");
    yield* Effect.annotateCurrentSpan("reactor.session.id", known.id);
    const descriptor = yield* guard(
      c,
      signaling.ready(known.id, reconnect ? undefined : known.descriptor),
    );
    yield* learn({ descriptor });
    yield* phase("described");
    const capabilities = descriptor.capabilities;
    const transport = descriptor.selected_transport;
    if (capabilities === undefined || transport === undefined)
      return yield* ReactorError.fromCode("Protocol", "missing ready capabilities or transport");
    if (transport.protocol !== "webrtc" || transport.version !== "1.0")
      return yield* ReactorError.fromCode(
        "VersionMismatch",
        `unsupported transport ${transport.protocol}/${transport.version}`,
      );
    const ready: ReadyDescriptor = { ...descriptor, capabilities, selected_transport: transport };
    const servers = yield* guard(c, signaling.iceServers(known.id));
    const prepared = yield* guard(
      c,
      Scope.provide(
        c.peer.prepare(servers, capabilities.tracks, (event) => {
          // The host boundary: hosts emit from platform callbacks; the
          // generation's fiber applies the events in order.
          Queue.offerUnsafe(c.events, event);
        }),
        c.scope,
      ),
    );
    yield* Ref.update(c.link, (link): Link => ({ ...link, mapping: prepared.mapping }));
    yield* phase("prepared");
    const previousId = known.connectionId;
    const connectionId = previousId ?? (yield* guard(c, signaling.register(known.id)));
    yield* Ref.update(c.link, (link): Link => ({ ...link, connectionId }));
    yield* learn({ connectionId });
    yield* phase("registered");
    // Registration, then buffered ICE (the empty final batch included), then offer, then answer.
    yield* ice.trickle(c);
    yield* guard(
      c,
      signaling.offer(
        known.id,
        connectionId,
        prepared.sdp,
        prepared.mapping,
        reconnect && previousId !== undefined,
      ),
    );
    yield* phase("offered");
    const answer = yield* guard(c, signaling.answer(known.id, connectionId));
    const negotiatedId = answer.connection_id ?? connectionId;
    yield* Ref.update(c.link, (link): Link => ({ ...link, connectionId: negotiatedId }));
    yield* learn({ connectionId: negotiatedId });
    yield* guard(c, c.peer.answer(answer.sdp_answer));
    yield* phase("answered");
    yield* guard(c, Deferred.await(c.ready)).pipe(
      Effect.timeoutOrElse({
        duration: settings.readyTimeout,
        orElse: timedOut("peer and both channels ready"),
      }),
    );
    yield* current(c);
    yield* phase("ready");
    yield* Ref.update(c.link, (link): Link => ({
      ...link,
      negotiated: {
        ownership: known.ownership,
        sessionId: known.id,
        descriptor: ready,
        connectionId: negotiatedId,
      },
    }));
    yield* transition("ready");
    // Hosted Reactor holds a connection's media until that connection
    // resumes its receive-only tracks, attached or not.
    for (const track of capabilities.tracks)
      if (track.direction === "recvonly" && resumeTracks) {
        const resumed = yield* Effect.result(tracks.setTrackActive(track.name, true, c));
        if (resumed._tag === "Failure")
          yield* publish({ _tag: "Diagnostic", error: resumed.failure }, c.generation);
      }
    for (const [name, bitrate] of (yield* SubscriptionRef.get(state)).bitrates) {
      const applied = yield* Effect.result(guard(c, c.peer.maxBitrate(name, bitrate)));
      if (applied._tag === "Failure")
        yield* publish({ _tag: "Diagnostic", error: applied.failure }, c.generation);
    }
    if (Duration.isFinite(settings.heartbeat))
      yield* background(
        c,
        requests
          .notification(c, { case: "ping", value: {} })
          .pipe(Effect.repeat(Schedule.spaced(settings.heartbeat)), Effect.asVoid),
      );
  });

  /**
   * A connection attempt: a failed one leaves its generation failed and closed. The generation is
   * acquired and released around its negotiation, so an interrupt that comes while it begins
   * lands in the negotiation, and still fails and closes it.
   */
  const attempt = (kind: Attempt) =>
    Effect.acquireUseRelease(
      begin(kind),
      (c) =>
        Effect.gen(function* () {
          if (c === undefined) return;
          const reconnect = kind !== "connect";
          yield* Effect.annotateCurrentSpan("reactor.connection.generation", c.generation);
          yield* negotiate(c, reconnect).pipe(
            Effect.timeoutOrElse({
              duration: reconnect ? settings.reconnectTimeout : settings.connectTimeout,
              orElse: timedOut(reconnect ? "reconnect" : "connect"),
            }),
          );
        }),
      (c, exit) =>
        c === undefined || Exit.isSuccess(exit)
          ? Effect.void
          : Effect.andThen(allocationUnknown, abandon(c, exit.cause)),
    ).pipe(
      Effect.withSpan(
        kind === "connect" ? "Session.connect" : "Session.reconnect",
        { kind: "client" },
        { captureStackTrace: false },
      ),
    );

  /**
   * The session's own reconnect stopped for `error`, unless another attempt or the close took
   * over: the session stays disconnected, and says why.
   */
  const stop = Effect.fnUntraced(function* (error: ReactorError) {
    const news = yield* SubscriptionRef.modify(state, (session) => {
      if (!beginsFrom.own(session)) return [false, session] as const;
      const next: State = { ...session, reconnecting: false, lastError: error };
      return [session.lastError !== error, next] as const;
    });
    // The last attempt's own failure was published with its drop.
    if (news) yield* publish({ _tag: "Diagnostic", error });
  });

  /** The ready connection stays up for `settle`, or this fails with why it went down sooner. */
  const holds = Effect.gen(function* () {
    const { generation } = yield* SubscriptionRef.get(state);
    const down = yield* SubscriptionRef.changes(state).pipe(
      Stream.filter((session) => session.status !== "ready" || session.generation !== generation),
      Stream.runHead,
      Effect.timeoutOption(settle),
      Effect.map(Option.flatten),
    );
    if (Option.isNone(down)) return;
    const why =
      down.value.lastError ?? ReactorError.fromCode("Disconnected", "connection went down");
    return yield* why;
  });

  /**
   * Reconnects each connection the session drops: an attempt at once, then again on `schedule`,
   * which sees each failure, until a connection has stayed ready for `settle`. A connection that
   * drops sooner fails its attempt, so one that keeps dropping is tried on the schedule, within
   * `reconnectTimeout` of the drop that began the reconnect. It stops once the schedule stops, no
   * attempt can succeed or that time has passed; a connection ready then still has `settle` to
   * stay up, and the reconnect stops where it drops if it does not. A defect ends that reconnect,
   * is reported, and the next drop is reconnected again.
   */
  const reconnectEachDrop = (schedule: Schedule.Schedule<unknown, ReactorError>) =>
    Effect.gen(function* () {
      yield* SubscriptionRef.changes(state).pipe(Stream.filter(beginsFrom.own), Stream.runHead);
      const reconnected = yield* attempt("own").pipe(
        Effect.andThen(holds),
        Effect.retry({
          schedule,
          while: (error) =>
            ends(error)
              ? Effect.succeed(false)
              : Effect.map(SubscriptionRef.get(state), beginsFrom.own),
        }),
        Effect.timeoutOrElse({
          duration: settings.reconnectTimeout,
          orElse: timedOut("reconnect"),
        }),
        Effect.exit,
      );
      if (Exit.isSuccess(reconnected)) return;
      if (Cause.hasDies(reconnected.cause)) yield* ErrorReporter.report(reconnected.cause);
      // A connection ready as the time ran out still has `settle` to stay up.
      const up = (yield* SubscriptionRef.get(state)).status === "ready";
      if (up && Exit.isSuccess(yield* Effect.exit(holds))) return;
      yield* reconnected.cause.pipe(
        failureOr(() => ReactorError.fromCode("Aborted", "reconnecting stopped")),
        stop,
      );
    }).pipe(Effect.forever);

  /**
   * Once the acquisition has connected, the session reconnects each connection it drops on its
   * own, one dropped meanwhile included, in a fiber its close interrupts.
   */
  const arm = Effect.gen(function* () {
    if (settings.reconnect === undefined) return;
    yield* SubscriptionRef.update(state, (session): State => ({
      ...session,
      reconnects: true,
      reconnecting: session.status === "disconnected",
    }));
    yield* Effect.forkIn(reconnectEachDrop(settings.reconnect), root);
  });

  return { allocate, connect: attempt("connect"), reconnect: attempt("reconnect"), arm };
};
