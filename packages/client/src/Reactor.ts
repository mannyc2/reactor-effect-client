/**
 * Acquires Reactor sessions. `create` allocates a new session this process
 * owns; `attach` joins an existing one. Either way the session lives in the
 * caller's scope, connected, and closing the scope closes it.
 */
import * as Cause from "effect/Cause";
import * as Context from "effect/Context";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as Random from "effect/Random";
import * as Schedule from "effect/Schedule";
import type * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import { Coordinator, notTerminated } from "./Coordinator.js";
import type { Tokens } from "./Coordinator.js";
import * as Internal from "./internal/session.js";
import { PeerFactory } from "./Peer.js";
import { AcquisitionFailure, isReactorFailure, ReactorError } from "./ReactorError.js";
import type { CloseReport, Session } from "./Session.js";

export interface Options {
  /** How long a command or control request waits for its reply; 10 seconds by default. */
  readonly replyTimeout?: Duration.Input | undefined;
  /** How long an upload may take in all; 60 seconds by default. */
  readonly uploadTimeout?: Duration.Input | undefined;
  /**
   * How long connecting may take, the wait for a GPU included; 3 minutes by default. Waiting for
   * a GPU is not billed. The deadline cuts the allocation and the negotiation, not the host's work
   * on either side of them: making the connection's peer, which every shipped host does at once,
   * and shutting it down after a failed attempt, which the native peer bounds by its
   * `shutdownTimeout` (10 seconds by default) and the browser's does at once.
   */
  readonly connectTimeout?: Duration.Input | undefined;
  /**
   * How long a reconnect may take, a session's own counted from the drop that began it, through
   * the drops of connections that did not stay up 10 seconds; 30 seconds by default, the time
   * Reactor keeps a session that has lost its last connection before it ends it. A connection a
   * session's own reconnect made ready as the time ran out is left up, and the session stops
   * trying if it drops in the 10 seconds after. The deadline cuts the negotiation, not the host's
   * work on either side of it: making the connection's peer, which every shipped host does at
   * once, and shutting down the peer it replaces, or its own after a failed attempt, which the
   * native peer bounds by its `shutdownTimeout` (10 seconds by default) and the browser's does at
   * once.
   */
  readonly reconnectTimeout?: Duration.Input | undefined;
  /**
   * How a session reconnects a connection it drops, on its own and whoever reads it, owned or
   * attached: one attempt at once, then another after each failure on this schedule, which gets the
   * failure as its input. A connection that drops within 10 seconds of being ready is such a
   * failure too, so a connection that keeps dropping is tried on the schedule, not at once, and
   * within the one `reconnectTimeout`. It stops once a connection has stayed ready 10 seconds, the
   * schedule stops, the session is closing or ended (by Reactor or its moderation), Reactor refuses
   * this client's protocol (`VersionMismatch`), or `reconnectTimeout` has passed. A refusal that
   * may lift is tried again, a 401 or 403 included, since the next attempt may carry a fresh token.
   * Each failed attempt is a `Diagnostic` event, and a reconnect that runs out of time stops with a
   * `Timeout` `lastError` whose `detail` is the last attempt's failure. Each attempt is a new
   * connection generation of the same session: it allocates nothing and never replays a command. By
   * default the second attempt comes 250 ms after the first fails and each wait doubles, to at most
   * 4 seconds, jittered by up to a fifth either way, and never sooner than a refusal's
   * `Retry-After`. A schedule with no delay of its own tries again at once, for all of
   * `reconnectTimeout`, each time with a new peer (a process of its own on the isolated native
   * host), so space its attempts as the default does. The session reconnects in its acquisition's
   * context, with the tracer, `ErrorReporter`s and clock `create` or `attach` ran with, and each
   * attempt's `Session.reconnect` span begins a trace of its own, linked to the acquisition's.
   * Reconnecting keeps a session alive, and billed: an owned session its application never closed,
   * or one a viewer holds after its owner has gone, runs on until its cap. `false` leaves a dropped
   * connection dropped until `session.reconnect`, so that Reactor ends a session 30 seconds after
   * its last connection drops.
   */
  readonly reconnect?: Schedule.Schedule<unknown, ReactorError> | false | undefined;
  /** How long the peer and both channels may take after the answer; 30 seconds by default. */
  readonly readyTimeout?: Duration.Input | undefined;
  /** Between heartbeats; 10 seconds by default, `"Infinity"` for none. */
  readonly heartbeatInterval?: Duration.Input | undefined;
  /** Requests awaiting a reply per channel; 128 by default, at most 4,096. */
  readonly maxPending?: number | undefined;
  /** The largest upload; 16 MiB by default, at most 64 MiB. */
  readonly maxUploadBytes?: number | undefined;
  /** The tokens an acquisition uses when it names none. */
  readonly tokens?: Tokens | undefined;
}

interface AcquisitionOptions {
  /**
   * Resume the session's receive-only tracks as each connection becomes
   * ready; true by default. Reactor sends a connection no media until it
   * does, so a viewer that starts paused resumes them itself through `Media`.
   */
  readonly resumeTracks?: boolean | undefined;
}

export interface CreateOptions<E = never, R = never> extends AcquisitionOptions {
  readonly model: string;
  readonly version?: string | undefined;
  /**
   * The session's tokens: `create`'s token allocates it, and before that
   * expires a token from `bind` carries its later calls on, so the session can
   * outlive any one token.
   */
  readonly tokens?: Tokens | undefined;
  readonly extraArgs?: Schema.Json | undefined;
  /**
   * Runs once the session is allocated and before it connects, so a
   * supervisor can record the owner first. Its failure closes the session and
   * fails the acquisition with an `AcquisitionFailure` carrying the close's
   * report, as any failure after allocation does: one of the client's failures
   * keeps its reason and context, and any other error becomes an `Aborted`
   * failure with that error as its `context.detail`. A defect stays a defect,
   * raised once the session is closed.
   */
  readonly onAllocated?: ((session: Session) => Effect.Effect<void, E, R>) | undefined;
}

export interface AttachOptions extends AcquisitionOptions {
  readonly sessionId: string;
  readonly connectionId?: number | undefined;
  /** Tokens bound to the session, minted when the attach starts and before each expires. */
  readonly tokens?: Pick<Tokens, "bind"> | undefined;
  /**
   * Take over the session's remote lifetime, as a process resuming a session
   * its dead owner recorded does: closing it, or a failed attach, terminates
   * it. Without it an attach never terminates the session it joined.
   */
  readonly adopt?: boolean | undefined;
}

export class Reactor extends Context.Service<
  Reactor,
  {
    readonly create: <E = never, R = never>(
      options: CreateOptions<E, R>,
    ) => Effect.Effect<Session, AcquisitionFailure, Scope.Scope | R>;
    readonly attach: (
      options: AttachOptions,
    ) => Effect.Effect<Session, AcquisitionFailure, Scope.Scope>;
  }
>()("reactor-effect-client/Reactor") {}

/** The report of an acquisition that allocated and attached nothing. */
export const noAcquisition: CloseReport = {
  localClosed: true,
  allocation: "none",
  remote: notTerminated,
  unpublishSubmitted: [],
  unresolvedPublications: [],
  localErrors: [],
};

const invalid = (message: string) =>
  ReactorError.fromCode("InvalidInput", message, { outcome: "not-submitted" });

const bounded = (
  input: Duration.Input | undefined,
  fallback: Duration.Input,
  name: string,
  infinite = false,
): Effect.Effect<Duration.Duration, ReactorError> => {
  const value = Duration.fromInput(input ?? fallback);
  if (value._tag === "None") return Effect.fail(invalid(`${name} is not a duration`));
  const duration = value.value;
  if (!Duration.isFinite(duration))
    return infinite ? Effect.succeed(duration) : Effect.fail(invalid(`${name} must be finite`));
  if (!Duration.isPositive(duration) || Duration.isGreaterThan(duration, Duration.minutes(10)))
    return Effect.fail(invalid(`${name} must be positive and at most 10 minutes`));
  return Effect.succeed(duration);
};

/**
 * A session's own reconnect by default: 250 ms after the first failed attempt, doubling to at
 * most 4 s, jittered so clients that dropped together do not try again together, and never
 * sooner than a refusal's `Retry-After`.
 */
const reconnectSchedule: Schedule.Schedule<unknown, ReactorError> = Schedule.min([
  Schedule.exponential("250 millis"),
  Schedule.spaced("4 seconds"),
]).pipe(
  Schedule.jittered,
  Schedule.modifyDelay(({ input, duration }) => {
    const asked = ReactorError.is(input) ? input.retryAfter : undefined;
    return Effect.succeed(asked === undefined ? duration : Duration.max(duration, asked));
  }),
);

const count = (value: number | undefined, fallback: number, maximum: number, name: string) =>
  Number.isSafeInteger(value ?? fallback) &&
  (value ?? fallback) >= 1 &&
  (value ?? fallback) <= maximum
    ? Effect.succeed(value ?? fallback)
    : Effect.fail(invalid(`${name} must be an integer in 1..${String(maximum)}`));

/** A Reactor over the current Coordinator and host. No session is allocated until `create`. */
export const make = Effect.fnUntraced(function* (options: Options = {}) {
  const coordinator = yield* Coordinator;
  const peers = yield* PeerFactory;
  const settings = {
    replyTimeout: yield* bounded(options.replyTimeout, "10 seconds", "reply timeout"),
    uploadTimeout: yield* bounded(options.uploadTimeout, "60 seconds", "upload timeout"),
    connectTimeout: yield* bounded(options.connectTimeout, "3 minutes", "connect timeout"),
    reconnectTimeout: yield* bounded(options.reconnectTimeout, "30 seconds", "reconnect timeout"),
    reconnect: options.reconnect === false ? undefined : (options.reconnect ?? reconnectSchedule),
    readyTimeout: yield* bounded(options.readyTimeout, "30 seconds", "ready timeout"),
    heartbeat: yield* bounded(options.heartbeatInterval, "10 seconds", "heartbeat interval", true),
    maxPending: yield* count(options.maxPending, 128, 4096, "maxPending"),
    maxUploadBytes: yield* count(
      options.maxUploadBytes,
      16_777_216,
      64 * 1024 * 1024,
      "maxUploadBytes",
    ),
  };

  const acquire = <E, R>(
    intent: Internal.Intent,
    tokens: (Pick<Tokens, "bind"> & Partial<Pick<Tokens, "create">>) | undefined,
    resumeTracks: boolean | undefined,
    onAllocated: ((session: Session) => Effect.Effect<void, E, R>) | undefined,
  ): Effect.Effect<Session, AcquisitionFailure, Scope.Scope | R> =>
    Effect.uninterruptibleMask((restore) =>
      Effect.gen(function* () {
        const rejected = (error: ReactorError) =>
          Effect.fail(AcquisitionFailure.from(error, noAcquisition));
        yield* restore(peers.check).pipe(Effect.catch(rejected));
        const words = yield* Effect.all(
          Array.from({ length: 4 }, () => Random.nextIntBetween(0, 0xffffffff)),
        );
        const namespace = words.map((word) => word.toString(16).padStart(8, "0")).join("");
        // A failed acquisition closes this scope at once, even when its caller
        // handles the failure in a much longer-lived one.
        const scope = yield* Scope.fork(yield* Effect.scope);
        const handle = yield* Internal.make({
          intent,
          coordinator,
          tokens: tokens ?? options.tokens,
          resumeTracks: resumeTracks ?? true,
          peers,
          settings: { ...settings, namespace },
        }).pipe(Scope.provide(scope));
        yield* Scope.addFinalizer(scope, handle.close);
        const release = handle.close.pipe(Effect.tap(() => Scope.close(scope, Exit.void)));
        const acquired = yield* restore(
          Effect.gen(function* () {
            const id = yield* handle.allocate;
            yield* Effect.annotateCurrentSpan("reactor.session.id", id);
            const session = handle.session(id);
            if (onAllocated !== undefined) {
              const recorded = yield* onAllocated(session).pipe(
                Effect.mapError((error) =>
                  isReactorFailure(error)
                    ? error
                    : ReactorError.fromCode("Aborted", "onAllocated failed", {
                        operation: "onAllocated",
                        sessionId: id,
                        detail: error,
                      }),
                ),
                Effect.exit,
              );
              if (Exit.isFailure(recorded)) return Exit.failCause(recorded.cause);
            }
            yield* handle.connect;
            return Exit.succeed<Session>(session);
          }),
        ).pipe(
          Effect.catch((error) =>
            release.pipe(
              Effect.flatMap((report) => Effect.fail(AcquisitionFailure.from(error, report))),
            ),
          ),
          Effect.onInterrupt(() => Scope.close(scope, Exit.void)),
        );
        if (Exit.isSuccess(acquired)) return acquired.value;
        // A failure from onAllocated releases the lease too, and carries the
        // release's report as any failure after allocation does: the session may
        // outlive a release that could not end it. A defect is raised once the
        // lease is released.
        const report = yield* release;
        const failure = Cause.findError(acquired.cause);
        return yield* failure._tag === "Success"
          ? Effect.fail(AcquisitionFailure.from(failure.success, report))
          : Effect.failCause(failure.failure);
      }),
    ).pipe(
      Effect.withSpan(
        intent._tag === "Create" ? "Reactor.create" : "Reactor.attach",
        {
          kind: "client",
          attributes:
            intent._tag === "Create"
              ? { "reactor.model.name": intent.model.name }
              : { "reactor.session.id": intent.sessionId, "reactor.session.adopt": intent.adopt },
        },
        { captureStackTrace: false },
      ),
    );

  return Reactor.of({
    create: (input) =>
      input.model.length === 0
        ? Effect.fail(AcquisitionFailure.from(invalid("a session needs a model"), noAcquisition))
        : acquire(
            {
              _tag: "Create",
              model: {
                name: input.model,
                ...(input.version === undefined ? {} : { version: input.version }),
              },
              ...(input.extraArgs === undefined ? {} : { extraArgs: input.extraArgs }),
            },
            input.tokens,
            input.resumeTracks,
            input.onAllocated,
          ),
    attach: (input) =>
      input.sessionId.length === 0 ||
      (input.connectionId !== undefined &&
        !(
          Number.isInteger(input.connectionId) &&
          input.connectionId >= 0 &&
          input.connectionId <= 0xffffffff
        ))
        ? Effect.fail(
            AcquisitionFailure.from(
              invalid("an attach needs a session id and a uint32 connection id"),
              noAcquisition,
            ),
          )
        : acquire(
            {
              _tag: "Attach",
              sessionId: input.sessionId,
              ...(input.connectionId === undefined ? {} : { connectionId: input.connectionId }),
              adopt: input.adopt === true,
            },
            input.tokens,
            input.resumeTracks,
            undefined,
          ),
  });
});

/** The Reactor service over a Coordinator and a host's `PeerFactory`. */
export const layer = (
  options: Options = {},
): Layer.Layer<Reactor, ReactorError, Coordinator | PeerFactory> =>
  Layer.effect(Reactor, make(options));
