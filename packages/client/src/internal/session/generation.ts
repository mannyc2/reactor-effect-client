/**
 * The live connection generation: the checks that fence work to it, and its
 * one failure, which fails its requests, closes its peer and, while it is
 * live, leaves the session disconnected, reconnecting if it does so on its own.
 */
import * as Deferred from "effect/Deferred";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Predicate from "effect/Predicate";
import * as Ref from "effect/Ref";
import * as Stream from "effect/Stream";
import * as SubscriptionRef from "effect/SubscriptionRef";
import { IceFailed, ReactorError, TransportFailed } from "../../ReactorError.js";
import type { Connection, Core, Link, State } from "./model.js";
import { isClosing, timedOut } from "./model.js";

/**
 * Why a connection failed, from its statistics: a candidate pair that
 * succeeded or was nominated means ICE worked and the DTLS or SCTP transport
 * above it failed; otherwise no pair worked.
 */
const connectionFailure = (stats: ReadonlyArray<unknown>, generation: bigint): ReactorError => {
  const entries = stats.filter(Predicate.isObject);
  const pairs = entries.filter((entry) => entry.type === "candidate-pair");
  if (pairs.some((pair) => pair.state === "succeeded" || pair.nominated === true))
    return ReactorError.make({
      reason: TransportFailed.make({
        message: "peer failed after ICE connectivity succeeded",
        pairs: pairs.length,
      }),
      context: { generation },
    });
  const candidateTypes = new Set(
    entries
      .filter((entry) => entry.type === "local-candidate")
      .map((entry) => entry.candidateType)
      .filter(Predicate.isString),
  );
  return ReactorError.make({
    reason: IceFailed.make({
      message: "peer found no working ICE candidate pair",
      pairs: pairs.length,
      candidateTypes: [...candidateTypes],
    }),
    context: { generation },
  });
};

export const make = (core: Core) => {
  const { state, data, control, publish } = core;

  /** Fails when `c` is not the live generation, preserving its own failure. */
  const current = Effect.fnUntraced(function* (c: Connection) {
    const link = yield* Ref.get(c.link);
    if (link.failure !== undefined) return yield* link.failure;
    const session = yield* SubscriptionRef.get(state);
    if (session.connection !== c || isClosing(session.status))
      return yield* ReactorError.fromCode("Aborted", "retired connection generation", {
        generation: c.generation,
      });
  });

  /** The ready generation and what it negotiated. */
  const currentReady = Effect.gen(function* () {
    const session = yield* SubscriptionRef.get(state);
    if (isClosing(session.status))
      return yield* ReactorError.fromCode("Closed", "session is closed", {
        outcome: "not-submitted",
      });
    const c = session.connection;
    if (session.status !== "ready" || c === undefined)
      return yield* ReactorError.fromCode(
        "InvalidState",
        `operation requires ready, not ${session.status}`,
        { outcome: "not-submitted" },
      );
    yield* current(c);
    const negotiated = (yield* Ref.get(c.link)).negotiated;
    if (negotiated === undefined) return yield* Effect.die("ready connection has no negotiation");
    return { c, negotiated };
  });

  /** Runs `effect` for `c`, failing with `c`'s failure if it retires meanwhile. */
  const guard = <A>(c: Connection, effect: Effect.Effect<A, ReactorError>) =>
    current(c).pipe(
      Effect.andThen(effect),
      Effect.raceFirst(Deferred.await(c.failed)),
      Effect.tap(() => current(c)),
      Effect.catch((error) =>
        Ref.get(c.link).pipe(Effect.flatMap((link) => Effect.fail(link.failure ?? error))),
      ),
    );

  const fail = Effect.fnUntraced(function* (c: Connection, error: ReactorError) {
    const first = yield* Ref.modify(c.link, (link) =>
      link.failure === undefined
        ? ([link, { ...link, failure: error }] as const)
        : ([undefined, link] as const),
    );
    if (first === undefined) return;
    yield* Deferred.fail(c.failed, error);
    yield* Deferred.fail(c.ready, error);
    yield* data.failGeneration(c.generation, error);
    yield* control.failGeneration(c.generation, error);
    yield* c.peer.close;
    yield* Effect.sync(() => {
      for (const track of first.sending.values()) track.stop();
    });
    yield* Ref.update(c.link, (link): Link => ({
      ...link,
      sending: new Map(),
      ice: [],
      iceBytes: 0,
      claimed: new Set(),
      paused: new Set(),
    }));
    const disconnected = yield* SubscriptionRef.modify(state, (session) => {
      if (session.connection !== c || isClosing(session.status)) return [false, session] as const;
      const next: State = {
        ...session,
        received: new Set<string>(),
        lastError: error,
        status: "disconnected",
        // With the drop itself, so no reader takes a drop the session reconnects for a lasting one.
        reconnecting: session.reconnects,
      };
      return [true, next] as const;
    });
    if (disconnected) {
      yield* publish({ _tag: "Status", status: "disconnected" }, c.generation);
      yield* publish({ _tag: "Diagnostic", error }, c.generation);
    }
  });

  /** A task of `c`'s whose failure fails `c`; a defect is a bug and stays one. */
  const background = (c: Connection, body: Effect.Effect<void, ReactorError>) =>
    body.pipe(
      Effect.raceFirst(Deferred.await(c.failed)),
      Effect.catch((error) => fail(c, error)),
      Effect.forkIn(c.scope),
      Effect.asVoid,
    );

  /** Completes `c`'s readiness once its peer and both channels are open. */
  const readyGate = (c: Connection) =>
    Ref.get(c.link).pipe(
      Effect.flatMap((link) =>
        link.peerConnected && link.controlOpen && link.dataOpen && link.failure === undefined
          ? Deferred.succeed(c.ready, undefined)
          : Effect.void,
      ),
      Effect.asVoid,
    );

  /** A stream of `c`'s track, fenced to `c`: it fails once `c` retires. */
  const fenced = <A>(
    c: Connection,
    source: Stream.Stream<A, ReactorError>,
  ): Stream.Stream<A, ReactorError> =>
    Stream.transformPull(source, (pull) =>
      Effect.succeed(
        current(c).pipe(
          Effect.andThen(pull),
          Effect.raceFirst(Deferred.await(c.failed)),
          Effect.tap(() => current(c)),
        ),
      ),
    );

  /**
   * A failed connection, classified from one statistics read on the
   * generation's event fiber, so later events wait behind it and the
   * connection's scope owns the read. A read that fails, or outlasts 2 s on
   * the fiber's Clock, leaves it `Disconnected`.
   */
  const failedConnection = (c: Connection): Effect.Effect<ReactorError> =>
    c.peer.stats.pipe(
      Effect.timeoutOrElse({
        duration: Duration.seconds(2),
        orElse: timedOut("failure classification"),
      }),
      Effect.map((stats) => connectionFailure(stats, c.generation)),
      Effect.catch((cause) =>
        Effect.succeed(
          ReactorError.fromCode("Disconnected", "peer state failed", {
            generation: c.generation,
            detail: cause,
          }),
        ),
      ),
    );

  return { current, currentReady, guard, fail, background, readyGate, fenced, failedConnection };
};

export type Generation = ReturnType<typeof make>;
