/**
 * Allocation and connection: create or identify the remote session, then
 * negotiate each connection generation (describe, prepare, register, trickle
 * ICE, offer, answer, ready) within its deadline. A failed attempt fails and
 * closes its generation; a reconnect replaces the previous one and never
 * replays a command. Once acquired, a session with a reconnect policy
 * reconnects each connection it drops on its own, and a reconnect asked for
 * meanwhile joins that one.
 */
import * as Cause from "effect/Cause";
import * as Clock from "effect/Clock";
import * as Deferred from "effect/Deferred";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as ErrorReporter from "effect/ErrorReporter";
import * as Exit from "effect/Exit";
import * as Option from "effect/Option";
import * as Predicate from "effect/Predicate";
import * as PubSub from "effect/PubSub";
import * as Queue from "effect/Queue";
import * as Ref from "effect/Ref";
import * as Schedule from "effect/Schedule";
import * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import * as SubscriptionRef from "effect/SubscriptionRef";
import type * as Tracer from "effect/Tracer";
import type { PeerEvent, PeerFactory } from "../../Peer.js";
import { ReactorError } from "../../ReactorError.js";
import type { ReadyDescriptor } from "../../Session.js";
import { take } from "../queue.js";
import * as Stats from "../stats.js";
import type { Token } from "../token.js";
import { type Generation, retired } from "./generation.js";
import type { Ice } from "./ice.js";
import type { Inbound } from "./inbound.js";
import type { Connection, Core, Known, Link, RemoteSession, State } from "./model.js";
import {
  ends,
  failureOr,
  isClosing,
  isKnown,
  moderationEnded,
  newLink,
  timedOut,
} from "./model.js";
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

/** How an attempt, or a step of one, fails once the session's close has overtaken it. */
const closed = () =>
  ReactorError.fromCode("Closed", "session is closed", { outcome: "not-submitted" });

/** A connection attempt: the first connect, a reconnect asked for, or the session's own. */
type Attempt = "connect" | "reconnect" | "own";

/** The sessions each attempt may begin a generation in. */
const beginsFrom: Record<Attempt, (session: State) => boolean> = {
  connect: (session) => session.status === "idle",
  // While the session reconnects on its own, a reconnect asked for joins that one.
  reconnect: (session) =>
    (session.status === "ready" || session.status === "disconnected") && !session.reconnecting,
  own: (session) => session.status === "disconnected" && session.reconnecting,
};

/** Why `attempt` cannot begin from `session`: closed once the session's close has begun. */
const refusal = (attempt: Attempt, session: State) =>
  isClosing(session.status)
    ? closed()
    : ReactorError.fromCode("InvalidState", `${attempt} while ${session.status}`);

/**
 * What decides a reconnect asked for that begins no generation of its own: the first connection
 * ready from generation `ready` on, or the first lasting drop from `stop` on.
 */
interface Joined {
  readonly ready: bigint;
  readonly stop: bigint;
}

/** Joining the session's own reconnect, found under way in `from`: what follows its drop. */
const joinsOwn = (from: State): Joined => ({
  ready: from.status === "disconnected" ? from.generation + 1n : from.generation,
  stop: from.generation,
});

/**
 * Joining whatever replaced the connection a reconnect found in `look` while it made its peer:
 * the look stands for a drop, and only a later generation decides.
 */
const joinsAfter = (look: State): Joined => ({
  ready: look.generation + 1n,
  stop: look.generation + 1n,
});

/**
 * Where a reconnect asked for stands at `session` once it has joined `from`: done once a
 * connection it counts is ready, failed at a lasting drop it counts or once the session closes,
 * and undecided until then. A state of an earlier generation is older than what it joined, and
 * decides nothing.
 */
const joined =
  (from: Joined) =>
  (session: State): Exit.Exit<void, ReactorError> | undefined => {
    if (isClosing(session.status)) return Exit.fail(closed());
    if (session.status === "ready") return session.generation >= from.ready ? Exit.void : undefined;
    const stopped =
      session.status === "disconnected" && !session.reconnecting && session.generation >= from.stop;
    return stopped
      ? Exit.fail(session.lastError ?? ReactorError.fromCode("Aborted", "reconnecting stopped"))
      : undefined;
  };

/**
 * How long the session's own reconnect waits on a connection it made ready before it counts the
 * connection as back: one that drops sooner fails that reconnect's attempt.
 */
const settle = Duration.seconds(10);

/** A reconnect out of time; `failure` is why its last attempt, or its last connection, failed. */
const outOfTime = (failure: ReactorError | undefined) =>
  ReactorError.fromCode("Timeout", "reconnect: deadline", { detail: failure });

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
  const { current, guard, reportHost, fail, background } = generation;

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
      if (isClosing(session.status)) return yield* closed();
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

  /**
   * Closes a peer's `scope` with `exit`. What its host dies of as it shuts the peer down is no
   * attempt's to fail: it is reported on `generation`, the session's current one when omitted.
   */
  const shutDown = (
    scope: Scope.Closeable,
    exit: Exit.Exit<unknown, unknown>,
    generation?: bigint,
  ) => Scope.close(scope, exit).pipe(Effect.catchCause((cause) => reportHost(cause, generation)));

  /**
   * Fails `c` for `cause`, the attempt's own failure if it has one, and closes it: the attempt
   * fails with that alone, and its host's shutdown is reported on `c`'s generation.
   */
  const abandon = (c: Connection, cause: Cause.Cause<ReactorError>) =>
    fail(
      c,
      failureOr(() => ReactorError.fromCode("Aborted", "connection attempt stopped"))(cause),
    ).pipe(Effect.ensuring(shutDown(c.scope, Exit.void, c.generation)));

  /**
   * Retires `previous` for the generation that replaced it. Its host's shutdown is not the new
   * attempt's to fail: it is reported on the retired generation.
   */
  const retire = (previous: Connection) =>
    fail(previous, ReactorError.fromCode("Disconnected", "connection retired for reconnect")).pipe(
      Effect.ensuring(shutDown(previous.scope, Exit.void, previous.generation)),
    );

  /**
   * A reconnect asked for that joined `from` rather than begin a generation: it waits on `states`,
   * every state since before it looked, until what it joined decides it.
   */
  const join = (from: Joined, states: PubSub.Subscription<State>) =>
    Stream.fromSubscription(states).pipe(
      Stream.map(joined(from)),
      Stream.filter(Predicate.isNotUndefined),
      Stream.runHead,
      Effect.flatMap(Option.getOrThrow),
    );

  /**
   * A new generation, with its event fiber; the previous one is retired. It takes the session over
   * in one step with its checks, so no two attempts share a generation and none begins once the
   * session closes. The session's own reconnect begins nothing once another attempt, a ready
   * connection or the close has taken over. A reconnect asked for while the session reconnects on
   * its own begins nothing either: it joins that reconnect, watched from before it looks. Nor does
   * one whose connection another attempt replaced while it made its peer, the session's own
   * reconnect say: it joins whatever came after the connection it looked at. The session's own
   * attempt keeps the generation it claims in `began` as it claims it. An attempt acquires it
   * uninterruptibly, so every generation it claims is one the attempt fails and closes if it goes
   * no further, a defect as it takes over included.
   */
  const begin = Effect.fnUntraced(function* (attempt: Attempt, began?: Ref.Ref<bigint>) {
    const states = attempt === "reconnect" ? yield* PubSub.subscribe(state.pubsub) : undefined;
    const session = yield* SubscriptionRef.get(state);
    const reconnect = attempt !== "connect";
    if (!beginsFrom[attempt](session)) {
      if (states !== undefined && session.reconnecting)
        return { joins: join(joinsOwn(session), states) };
      if (attempt === "own") return undefined;
      return yield* refusal(attempt, session);
    }
    if (reconnect && !isKnown(session.remote))
      return yield* ReactorError.fromCode(
        "InvalidState",
        "cannot reconnect without a known session",
      );
    if (reconnect && session.moderated) return yield* moderationEnded({ outcome: "not-submitted" });
    const scope = yield* Scope.fork(root);
    const peer = yield* Scope.provide(peers.make, scope).pipe(
      Effect.onError((cause) => shutDown(scope, Exit.failCause(cause))),
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
    // The session as it was, if this could not take it over.
    const unclaimed = yield* SubscriptionRef.modify(
      state,
      (held): readonly [State | undefined, State] => {
        if (held.connection !== previous || !beginsFrom[attempt](held)) return [held, held];
        const next: State = {
          ...held,
          status: "connecting",
          generation: c.generation,
          connection: c,
          lastError: undefined,
          received: new Set(),
          sampler: Stats.initialSampler,
        };
        return [undefined, next];
      },
    );
    if (unclaimed !== undefined) {
      yield* shutDown(scope, Exit.void);
      if (states !== undefined && unclaimed.reconnecting)
        return { joins: join(joinsOwn(unclaimed), states) };
      // Only the connection changed: what replaced it is ready or down for good.
      if (states !== undefined && beginsFrom.reconnect(unclaimed))
        return { joins: join(joinsAfter(session), states) };
      if (attempt === "own") return undefined;
      return yield* refusal(attempt, unclaimed);
    }
    if (began !== undefined) yield* Ref.set(began, c.generation);
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

  /** Moves the session on to waiting, which fails once the session's close has begun. */
  const reachWaiting = Effect.flatMap(transition("waiting"), (moved) =>
    moved ? Effect.void : Effect.fail(closed()),
  );

  /**
   * Moves the session to ready on `c`, in one step with the checks that it may: `c` is the
   * session's connection, waiting, and has not failed. Otherwise this fails as `current(c)` does,
   * with `c`'s own failure first, or as closed once the session's close has begun.
   */
  const reachReady = Effect.fnUntraced(function* (c: Connection) {
    const refused = yield* SubscriptionRef.modify(
      state,
      (session): readonly [ReactorError | undefined, State] => {
        if (isClosing(session.status)) return [closed(), session];
        // Read with the move: a drop fails `c` before it moves the session on.
        const failure = Ref.getUnsafe(c.link).failure;
        if (failure !== undefined) return [failure, session];
        if (session.connection !== c || session.status !== "waiting") return [retired(c), session];
        // A ready connection ends the session's own reconnect.
        return [undefined, { ...session, status: "ready", reconnecting: false }];
      },
    );
    if (refused !== undefined) return yield* refused;
    yield* publish({ _tag: "Status", status: "ready" }, c.generation);
  });

  /** Negotiates `c` through to ready, its tracks resumed and its heartbeat running. */
  const negotiate = Effect.fnUntraced(function* (c: Connection, reconnect: boolean) {
    if (!reconnect) yield* guard(c, allocate);
    yield* current(c);
    yield* reachWaiting;
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
    // Last, so a deadline can cut only work before the connection is ready. It readies `c` alone,
    // and only while `c` is up: a drop meanwhile fails the negotiation with that drop.
    yield* reachReady(c);
  });

  /**
   * A connection attempt: a failed one fails with its own failure, and leaves its generation
   * failed and closed. The generation is acquired and released around its negotiation, so an
   * interrupt that comes while it begins lands in the negotiation, and still fails and closes it.
   * The session's own attempt that fails before it has a generation says why, as a failed
   * generation does: no caller hears of it otherwise. Its span begins a trace of its own, linked
   * to the session's `acquisition`, which may have ended hours before, and it keeps the generation
   * it begins in `began`. A reconnect asked for while the session reconnects on its own waits that
   * reconnect out, and ends as it does.
   */
  const attempt = (kind: Attempt, acquisition?: Tracer.AnySpan, began?: Ref.Ref<bigint>) =>
    Effect.acquireUseRelease(
      kind === "own"
        ? begin(kind, began).pipe(
            Effect.tapError((error) => publish({ _tag: "Diagnostic", error })),
          )
        : begin(kind),
      (c) =>
        Effect.gen(function* () {
          if (c === undefined) return;
          if ("joins" in c) return yield* c.joins;
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
        c === undefined || "joins" in c || Exit.isSuccess(exit)
          ? Effect.void
          : Effect.andThen(allocationUnknown, abandon(c, exit.cause)),
    ).pipe(
      Effect.scoped,
      Effect.withSpan(
        kind === "connect" ? "Session.connect" : "Session.reconnect",
        kind === "own"
          ? {
              kind: "client",
              root: true,
              links: acquisition === undefined ? [] : [{ span: acquisition, attributes: {} }],
            }
          : { kind: "client" },
        { captureStackTrace: false },
      ),
    );

  /**
   * The session's own reconnect stopped for `error` on `generation`, the latest it began, unless
   * another attempt, another generation or the close took over: the session stays disconnected,
   * and says why, unless it `said` so already.
   */
  const stop = Effect.fnUntraced(function* (
    error: ReactorError,
    said: boolean,
    generation: bigint,
  ) {
    const news = yield* SubscriptionRef.modify(state, (session) => {
      if (!beginsFrom.own(session) || session.generation !== generation)
        return [false, session] as const;
      const next: State = { ...session, reconnecting: false, lastError: error };
      return [!said && session.lastError !== error, next] as const;
    });
    if (news) yield* publish({ _tag: "Diagnostic", error });
  });

  /**
   * The connection the reconnect made ready on `generation` at `since`, on the monotonic clock,
   * stays up until it has been ready for `settle`, or this fails with why it went down sooner.
   * Once the session is on another generation, one a reconnect asked for began, this reconnect is
   * over, however late this sees it: it ends, and leaves that generation alone.
   */
  const holds = (generation: bigint, since: bigint) =>
    Effect.gen(function* () {
      const up = Duration.nanos((yield* Clock.monotonicTimeNanos) - since);
      const left = Duration.subtract(settle, up);
      if (!Duration.isPositive(left)) return;
      const down = yield* SubscriptionRef.changes(state).pipe(
        Stream.filter((session) => session.status !== "ready" || session.generation !== generation),
        Stream.runHead,
        Effect.timeoutOption(left),
        Effect.map(Option.flatten),
      );
      if (Option.isNone(down) || down.value.generation !== generation) return;
      const why =
        down.value.lastError ?? ReactorError.fromCode("Disconnected", "connection went down");
      return yield* why;
    });

  /**
   * Reconnects each connection the session drops: an attempt at once, then again on `schedule`,
   * which sees each failure, until a connection has stayed ready for `settle`. A connection that
   * drops sooner fails its attempt, so one that keeps dropping is tried on the schedule, within
   * `reconnectTimeout` of the drop that began the reconnect. It stops once the schedule stops, no
   * attempt can succeed or that time has passed; a connection ready then still has the rest of its
   * `settle`, counted from when it became ready, to stay up, and the reconnect stops where it drops
   * if it does not. Each attempt says why it failed as it fails, and a reconnect out of time stops
   * with a `Timeout` whose detail is the last attempt's failure, or why the connection ready as the
   * time ran out dropped. A reconnect asked for while a connection this made ready settles ends
   * this one, which leaves the generation the application began alone, whether ready, failed or
   * dropped. A defect ends that reconnect, is reported, and the next drop is reconnected again.
   */
  const reconnectEachDrop = (
    schedule: Schedule.Schedule<unknown, ReactorError>,
    acquisition: Tracer.AnySpan | undefined,
  ) =>
    Effect.gen(function* () {
      const dropped = yield* SubscriptionRef.changes(state).pipe(
        Stream.filter(beginsFrom.own),
        Stream.runHead,
      );
      // The drop's move wakes this inline, before the drop has published its own events: the
      // attempt waits for the scheduler, so its events come after them.
      yield* Effect.yieldNow;
      // The latest generation this reconnect began, the dropped one to begin with. A session on
      // any other is another's: a reconnect asked for as a connection settled began it.
      const began = yield* Ref.make(Option.getOrThrow(dropped).generation);
      // The session is still this reconnect's to try again: down, on the latest generation it
      // began.
      const ours = Effect.map(
        Effect.all([SubscriptionRef.get(state), Ref.get(began)]),
        ([session, mine]) => beginsFrom.own(session) && session.generation === mine,
      );
      // Why the last attempt failed, which it published as it did; the deadline's detail.
      const last = yield* Ref.make<ReactorError | undefined>(undefined);
      // The latest connection this reconnect made ready, and when, on the monotonic clock.
      const readied = yield* Ref.make<{ readonly generation: bigint; readonly at: bigint }>({
        generation: 0n,
        at: 0n,
      });
      const settles = Effect.gen(function* () {
        const generation = yield* Ref.get(began);
        const at = yield* Clock.monotonicTimeNanos;
        yield* Ref.set(readied, { generation, at });
        return yield* holds(generation, at);
      });
      const reconnected = yield* attempt("own", acquisition, began).pipe(
        Effect.andThen(settles),
        Effect.tapError((error) => Ref.set(last, error)),
        Effect.retry({
          schedule,
          while: (error) => (ends(error) ? Effect.succeed(false) : ours),
        }),
        Effect.timeoutOrElse({
          duration: settings.reconnectTimeout,
          orElse: () => Effect.flatMap(Ref.get(last), (failure) => Effect.fail(outOfTime(failure))),
        }),
        Effect.exit,
      );
      if (Exit.isSuccess(reconnected)) return;
      if (Cause.hasDies(reconnected.cause)) yield* ErrorReporter.report(reconnected.cause);
      const mine = yield* Ref.get(began);
      const now = yield* SubscriptionRef.get(state);
      // Another's generation: a reconnect asked for as this one's connection settled ended it.
      if (now.generation !== mine) return;
      const stopped = failureOr(() => ReactorError.fromCode("Aborted", "reconnecting stopped"));
      if (now.status === "ready") {
        // A connection ready as the time ran out has the rest of its `settle`, counted from when
        // it became ready, to stay up; from now, if the deadline came before this noted when.
        const ready = yield* Ref.get(readied);
        const since = ready.generation === mine ? ready.at : yield* Clock.monotonicTimeNanos;
        const held = yield* Effect.exit(holds(mine, since));
        if (Exit.isSuccess(held)) return;
        return yield* stop(held.cause.pipe(stopped, outOfTime), false, mine);
      }
      const error = stopped(reconnected.cause);
      // An attempt's own failure was published as it failed.
      yield* stop(error, error === (yield* Ref.get(last)), mine);
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
    const acquisition = Option.getOrUndefined(yield* Effect.option(Effect.currentParentSpan));
    yield* Effect.forkIn(reconnectEachDrop(settings.reconnect, acquisition), root);
  });

  return { allocate, connect: attempt("connect"), reconnect: attempt("reconnect"), arm };
};
