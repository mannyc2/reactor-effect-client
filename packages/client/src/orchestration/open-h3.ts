import * as Clock from "effect/Clock";
import type * as Crypto from "effect/Crypto";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import type * as FileSystem from "effect/FileSystem";
import type * as Path from "effect/Path";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import type * as Http from "effect/unstable/http/HttpClient";
import type * as Redacted from "effect/Redacted";
import type { TokenGrant } from "../coordinator/_internal/schemas.js";
import { AcquisitionFailure, isReactorFailure, ReactorError } from "../errors.js";
import type { CommandFailure, PolicyFailure } from "../errors.js";
import { modelName } from "../h3/profile.js";
import { noAcquisition } from "../session/_internal/acquire.js";
import { Client } from "../session/index.js";
import type { CreateOptions, Session } from "../session/index.js";
import { fromH3Session } from "./h3-source.js";
import type { H3Source, SessionSourceOptions } from "./h3-source.js";
import type { Opened } from "./renewal.js";

/**
 * A durable owner record of an allocated session, without its token: enough to
 * find the session again and terminate it with the grant's token, which the
 * application stores as it decides.
 */
export const Allocation = Schema.Struct({
  sessionId: Schema.String,
  ownership: Schema.Literals(["owned", "attached"]),
  model: Schema.String,
  /** When the grant's token expires, in seconds since the epoch. */
  expiresAt: Schema.Finite,
  /**
   * When the session's granted length ends at the latest, in seconds since the
   * epoch: the grant's `maxSessionSeconds` from just before the create request,
   * so never later than the server's own end. `resumeH3` takes a resumed
   * session's lifetime from it. `openH3` always sets it; a record written by
   * 0.3.0 lacks it.
   */
  endsAt: Schema.optionalKey(Schema.Finite),
});
export interface Allocation extends Schema.Schema.Type<typeof Allocation> {}

/** A paid session just allocated, before it connects, and the grant it runs under. */
export interface Allocated {
  readonly session: Session;
  /** `grant.jwt` stays `Redacted`: how to store it is the application's decision. */
  readonly grant: TokenGrant;
  /** The session's owner record, ready to persist with `Allocation`. */
  readonly allocation: Allocation;
}

export interface OpenH3Options<E = never, R = never> {
  /** Mints the session's token. The API key never reaches the opener. */
  readonly mint: Effect.Effect<TokenGrant, ReactorError, R>;
  /**
   * Runs after allocation and before connect, for example to record the owner
   * durably, so a crash after it can still find and terminate the session:
   * terminate and inspect authenticate with the grant's token.
   */
  readonly onAllocated?: ((allocated: Allocated) => Effect.Effect<void, E, R>) | undefined;
  /** The rest of the create request; the model is H3's and the token is the grant's. */
  readonly create?: Omit<CreateOptions, "model" | "jwt"> | undefined;
  readonly source?: SessionSourceOptions | undefined;
}

/** An opened H3 source and its session's granted lifetime: a renewal `open` result. */
export interface OpenedH3 extends Opened {
  readonly source: H3Source;
}

type Bind = (
  session: Session,
  options?: SessionSourceOptions,
) => Effect.Effect<
  H3Source,
  ReactorError | PolicyFailure | CommandFailure,
  Scope.Scope | Crypto.Crypto | FileSystem.FileSystem | Path.Path | Http.HttpClient
>;

/** `openH3` over a given session binder; internal, so tests can supply media. */
export const openH3With =
  (bind: Bind) =>
  <E = never, R = never>(
    options: OpenH3Options<E, R>,
  ): Effect.Effect<
    OpenedH3,
    AcquisitionFailure | E,
    R | Client | Scope.Scope | Crypto.Crypto | FileSystem.FileSystem | Path.Path | Http.HttpClient
  > =>
    Effect.gen(function* () {
      const client = yield* Client;
      const grant = yield* options.mint.pipe(
        Effect.mapError((error) => AcquisitionFailure.from(error, noAcquisition)),
      );
      // A child of the caller's scope owns the paid session, so a failed open
      // releases it now rather than when a longer-lived caller scope closes.
      const scope = yield* Scope.fork(yield* Effect.scope);
      // The server starts the granted length no earlier than the request.
      const requested = yield* Clock.currentTimeMillis;
      const session = yield* client
        .create({ ...options.create, model: modelName, jwt: grant.jwt })
        .pipe(Scope.provide(scope));
      const open = Effect.gen(function* () {
        if (options.onAllocated !== undefined)
          yield* options.onAllocated({
            session,
            grant,
            allocation: {
              sessionId: session.id,
              ownership: session.ownership,
              model: modelName,
              expiresAt: grant.expiresAt,
              endsAt: requested / 1000 + grant.granted.maxSessionSeconds,
            },
          });
        yield* session.connect;
        const source = yield* bind(session, options.source);
        const opened: OpenedH3 = {
          source,
          // A unit, never a bare number: a bare number is milliseconds.
          lifetime: `${grant.granted.maxSessionSeconds} seconds`,
        };
        return opened;
      }).pipe(Scope.provide(scope));
      return yield* open.pipe(
        // A failure after allocation reports the session's cleanup, as a failed
        // `createConnected` does; an application error from onAllocated stays as it was.
        Effect.catch((error) =>
          session.close.pipe(
            Effect.flatMap((report) =>
              Effect.fail(isReactorFailure(error) ? AcquisitionFailure.from(error, report) : error),
            ),
          ),
        ),
        Effect.onExit((exit) => (Exit.isFailure(exit) ? Scope.close(scope, exit) : Effect.void)),
      );
    });

/**
 * Open a paid H3 session as an orchestration source: mint its token, allocate
 * it, run `onAllocated`, connect, and derive its source. The result is a
 * renewal `open` result, its lifetime the granted session length:
 * `Orchestration.make({ open: openH3(options) })`.
 */
export const openH3 = openH3With(fromH3Session);

export interface ResumeH3Options {
  /** The owner record `openH3` handed to `onAllocated`, with its `endsAt`. */
  readonly allocation: Allocation;
  /** The grant's token, stored beside the record: it authenticates the attach and the termination. */
  readonly jwt: Redacted.Redacted<string>;
  /**
   * The source's options, without `canvas`: a resumed session may be playing,
   * and H3 refuses a canvas change unless it is idle, so the session keeps the
   * canvas it has. `holdLastFrame`, which H3 accepts at any time, still applies.
   */
  readonly source?: Omit<SessionSourceOptions, "canvas"> | undefined;
}

/** `resumeH3` over a given session binder; internal, so tests can supply media. */
export const resumeH3With =
  (bind: Bind) =>
  (
    options: ResumeH3Options,
  ): Effect.Effect<
    OpenedH3,
    AcquisitionFailure,
    Client | Scope.Scope | Crypto.Crypto | FileSystem.FileSystem | Path.Path | Http.HttpClient
  > =>
    Effect.gen(function* () {
      const refuse = (message: string) =>
        AcquisitionFailure.from(
          ReactorError.fromCode("InvalidInput", message, {
            operation: "resume",
            outcome: "not-submitted",
          }),
          noAcquisition,
        );
      const { allocation } = options;
      if (allocation.model !== modelName)
        return yield* refuse("the allocation is not an H3 session");
      if (allocation.endsAt === undefined)
        return yield* refuse("the allocation has no endsAt, so its remaining lifetime is unknown");
      const endsAt = allocation.endsAt;
      if (!(endsAt * 1000 > (yield* Clock.currentTimeMillis)))
        return yield* refuse("the allocation's granted length has ended");
      if (options.source !== undefined && "canvas" in options.source)
        return yield* refuse("a resumed session keeps its canvas");
      const client = yield* Client;
      // As in openH3, a child scope owns the adopted session, so a failed
      // resume terminates it now rather than when the caller's scope closes.
      const scope = yield* Scope.fork(yield* Effect.scope);
      const resumed = Effect.gen(function* () {
        // A failed connected attach has already terminated what it adopted.
        const session = yield* client.attachConnected({
          sessionId: allocation.sessionId,
          jwt: options.jwt,
          adopt: true,
        });
        return yield* bind(session, options.source).pipe(
          // What is left once the source is ready, since renewal counts from then.
          Effect.flatMap((source) =>
            Clock.currentTimeMillis.pipe(
              Effect.map((now): OpenedH3 => ({
                source,
                lifetime: `${Math.max(1, Math.floor(endsAt * 1000 - now))} millis`,
              })),
            ),
          ),
          Effect.catch((error) =>
            session.close.pipe(
              Effect.flatMap((report) => Effect.fail(AcquisitionFailure.from(error, report))),
            ),
          ),
        );
      }).pipe(Scope.provide(scope));
      yield* Effect.annotateCurrentSpan("reactor.session.id", allocation.sessionId);
      return yield* resumed.pipe(
        Effect.onExit((exit) => (Exit.isFailure(exit) ? Scope.close(scope, exit) : Effect.void)),
      );
    }).pipe(
      Effect.withSpan(
        "reactor.orchestration.resume",
        { kind: "client" },
        { captureStackTrace: false },
      ),
    );

/**
 * Resume a paid H3 session that `openH3` allocated, from its owner record and
 * token, after its owner died: attach to it and adopt it, so this process owns
 * its remote lifetime and closing the source terminates it, then derive its
 * source. It sends no policy command a busy session refuses: the session keeps
 * its canvas, queue and playback, and only reads state. The lifetime is what
 * remains until the record's `endsAt`, so it is directly a renewal `open`, and
 * renewal terminates the session when it retires it. A record without
 * `endsAt`, for another model, or past its end is refused before anything is
 * sent; a failure after attaching terminates the adopted session and reports
 * that cleanup in the `AcquisitionFailure`.
 */
export const resumeH3 = resumeH3With(fromH3Session);
