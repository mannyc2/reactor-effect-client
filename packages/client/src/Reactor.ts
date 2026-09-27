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
import type * as Redacted from "effect/Redacted";
import type * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import { Coordinator, notTerminated } from "./Coordinator.js";
import * as Internal from "./internal/session.js";
import { PeerFactory } from "./Peer.js";
import { AcquisitionFailure, isReactorFailure, ReactorError } from "./ReactorError.js";
import type { CloseReport, Session } from "./Session.js";

export interface Options {
  /** How long a command or control request waits for its reply; 10 seconds by default. */
  readonly replyTimeout?: Duration.Input | undefined;
  /** How long an upload may take in all; 60 seconds by default. */
  readonly uploadTimeout?: Duration.Input | undefined;
  /** How long connecting or reconnecting may take in all; 3 minutes by default. */
  readonly connectTimeout?: Duration.Input | undefined;
  /** How long the peer and both channels may take after the answer; 30 seconds by default. */
  readonly readyTimeout?: Duration.Input | undefined;
  /** Between heartbeats; 10 seconds by default, `"Infinity"` for none. */
  readonly heartbeatInterval?: Duration.Input | undefined;
  /** Requests awaiting a reply per channel; 128 by default, at most 4,096. */
  readonly maxPending?: number | undefined;
  /** The largest upload; 16 MiB by default, at most 64 MiB. */
  readonly maxUploadBytes?: number | undefined;
  /** The session token used when an acquisition names none. */
  readonly credential?: Effect.Effect<Redacted.Redacted<string>, ReactorError> | undefined;
}

export interface CreateOptions<E = never, R = never> {
  readonly model: string;
  readonly version?: string | undefined;
  readonly jwt?: Redacted.Redacted<string> | undefined;
  readonly extraArgs?: Schema.Json | undefined;
  /**
   * Runs once the session is allocated and before it connects, so a
   * supervisor can record the owner first. Its failure closes the session and
   * is returned as it is.
   */
  readonly onAllocated?: ((session: Session) => Effect.Effect<void, E, R>) | undefined;
}

export interface AttachOptions {
  readonly sessionId: string;
  readonly connectionId?: number | undefined;
  readonly jwt?: Redacted.Redacted<string> | undefined;
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
    ) => Effect.Effect<Session, AcquisitionFailure | E, Scope.Scope | R>;
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
    jwt: Redacted.Redacted<string> | undefined,
    onAllocated: ((session: Session) => Effect.Effect<void, E, R>) | undefined,
  ): Effect.Effect<Session, AcquisitionFailure | E, Scope.Scope | R> =>
    Effect.uninterruptibleMask((restore) =>
      Effect.gen(function* () {
        const rejected = (error: ReactorError) =>
          Effect.fail(AcquisitionFailure.from(error, noAcquisition));
        yield* restore(peers.check).pipe(Effect.catch(rejected));
        const token =
          jwt ??
          (options.credential === undefined
            ? undefined
            : yield* restore(options.credential).pipe(Effect.catch(rejected)));
        const words = yield* Effect.all(
          Array.from({ length: 4 }, () => Random.nextIntBetween(0, 0xffffffff)),
        );
        const namespace = words.map((word) => word.toString(16).padStart(8, "0")).join("");
        // A failed acquisition closes this scope at once, even when its caller
        // handles the failure in a much longer-lived one.
        const scope = yield* Scope.fork(yield* Effect.scope);
        const handle = yield* Internal.make({
          intent,
          signaling: coordinator.signaling(token),
          peers,
          settings: { ...settings, namespace },
          apiUrl: coordinator.apiUrl,
        }).pipe(Scope.provide(scope));
        yield* Scope.addFinalizer(scope, handle.close);
        const release = handle.close.pipe(Effect.tap(() => Scope.close(scope, Exit.void)));
        const acquired = yield* restore(
          Effect.gen(function* () {
            const id = yield* handle.allocate;
            yield* Effect.annotateCurrentSpan("reactor.session.id", id);
            const session = handle.session(id);
            if (onAllocated !== undefined) {
              const recorded = yield* Effect.exit(onAllocated(session));
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
        // A failure from onAllocated releases the lease too. One of the client's
        // own failures carries the cleanup evidence; an application's own error
        // is returned as it was raised.
        const report = yield* release;
        const application = Cause.findError(acquired.cause);
        return yield* application._tag === "Success" && isReactorFailure(application.success)
          ? Effect.fail(AcquisitionFailure.from(application.success, report))
          : Effect.failCause(acquired.cause);
      }),
    ).pipe(
      Effect.withSpan(
        intent._tag === "Create" ? "reactor.session.create" : "reactor.session.attach",
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
            input.jwt,
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
            input.jwt,
            undefined,
          ),
  });
});

/** The Reactor service over a Coordinator and a host's `PeerFactory`. */
export const layer = (
  options: Options = {},
): Layer.Layer<Reactor, ReactorError, Coordinator | PeerFactory> =>
  Layer.effect(Reactor, make(options));
