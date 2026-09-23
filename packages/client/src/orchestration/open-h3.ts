import type * as Crypto from "effect/Crypto";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import type * as FileSystem from "effect/FileSystem";
import type * as Path from "effect/Path";
import * as Scope from "effect/Scope";
import type * as Http from "effect/unstable/http/HttpClient";
import type { TokenGrant } from "../coordinator/_internal/schemas.js";
import { AcquisitionFailure, isReactorFailure } from "../errors.js";
import type { CommandFailure, PolicyFailure, ReactorError } from "../errors.js";
import { modelName } from "../h3/profile.js";
import { noAcquisition } from "../session/_internal/acquire.js";
import { Client } from "../session/index.js";
import type { CreateOptions, Session } from "../session/index.js";
import { fromH3Session } from "./h3-source.js";
import type { H3Source, SessionSourceOptions } from "./h3-source.js";
import type { Opened } from "./renewal.js";

/** A paid session just allocated, before it connects, and the grant it runs under. */
export interface Allocated {
  readonly session: Session;
  /** `grant.jwt` stays `Redacted`: how to store it is the application's decision. */
  readonly grant: TokenGrant;
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
      const session = yield* client
        .create({ ...options.create, model: modelName, jwt: grant.jwt })
        .pipe(Scope.provide(scope));
      const open = Effect.gen(function* () {
        if (options.onAllocated !== undefined) yield* options.onAllocated({ session, grant });
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
