/**
 * Allocation and connection: create or identify the remote session, then
 * negotiate each connection generation (describe, prepare, register, trickle
 * ICE, offer, answer, ready) within its deadline. A failed attempt fails and
 * closes its generation; a reconnect replaces the previous one and never
 * replays a command.
 */
import * as Clock from "effect/Clock";
import * as Deferred from "effect/Deferred";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Queue from "effect/Queue";
import * as Ref from "effect/Ref";
import * as Schedule from "effect/Schedule";
import * as Scope from "effect/Scope";
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

  /** A new generation, with its event fiber; the previous one is retired. */
  const begin = Effect.fnUntraced(function* (reconnect: boolean) {
    const session = yield* SubscriptionRef.get(state);
    const allowed = reconnect
      ? session.status === "ready" || session.status === "disconnected"
      : session.status === "idle";
    if (!allowed)
      return yield* ReactorError.fromCode(
        "InvalidState",
        `${reconnect ? "reconnect" : "connect"} while ${session.status}`,
      );
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
    yield* SubscriptionRef.update(state, (held): State => ({
      ...held,
      generation: c.generation,
      connection: c,
      lastError: undefined,
      received: new Set(),
      sampler: Stats.initialSampler,
    }));
    yield* transition("connecting");
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
    if (previous !== undefined) {
      yield* fail(
        previous,
        ReactorError.fromCode("Disconnected", "connection retired for reconnect"),
      );
      yield* Scope.close(previous.scope, Exit.void);
    }
    return c;
  }, Effect.uninterruptible);

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

  /** A connection attempt: a failed one leaves its generation failed and closed. */
  const attempt = (reconnect: boolean) =>
    Effect.gen(function* () {
      const c = yield* begin(reconnect);
      yield* Effect.annotateCurrentSpan("reactor.connection.generation", c.generation);
      yield* negotiate(c, reconnect).pipe(
        Effect.timeoutOrElse({
          duration: reconnect ? settings.reconnectTimeout : settings.connectTimeout,
          orElse: timedOut(reconnect ? "reconnect" : "connect"),
        }),
        Effect.onExit((exit) =>
          Exit.isSuccess(exit)
            ? Effect.void
            : Effect.gen(function* () {
                yield* allocationUnknown;
                yield* fail(
                  c,
                  failureOr(() =>
                    ReactorError.fromCode("Aborted", "connection attempt interrupted"),
                  )(exit.cause),
                );
                yield* Scope.close(c.scope, Exit.void);
              }),
        ),
      );
    }).pipe(
      Effect.withSpan(
        reconnect ? "reactor.session.reconnect" : "reactor.session.connect",
        { kind: "client" },
        { captureStackTrace: false },
      ),
    );

  return { allocate, connect: attempt(false), reconnect: attempt(true) };
};
