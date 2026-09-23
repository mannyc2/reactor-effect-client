import * as Cause from "effect/Cause";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Result from "effect/Result";
import * as Scope from "effect/Scope";
import type { CoordinatorClient, Termination } from "../../coordinator/_internal/client.js";
import { parsed, ReactorError } from "../../errors.js";
import type { CloseReport } from "../../SessionTypes.js";
import * as W from "../../wire.generated.js";
import type { Connection } from "./lifecycle.js";
import { isKnownRemote } from "./remote.js";
import type { Remote, RemoteSession } from "./remote.js";

const releasePublications = (
  connection: Connection | undefined,
  commandTimeout: number,
): Effect.Effect<{ readonly submitted: string[]; readonly errors: ReactorError[] }> =>
  Effect.gen(function* () {
    const submitted: string[] = [],
      errors: ReactorError[] = [];
    if (connection !== undefined && connection.failure === undefined)
      for (const name of connection.claimed) {
        // Closing has already fenced ordinary requests. These are the final
        // notifications on the still-open transport, not correlated commands.
        const result = yield* Effect.exit(
          Effect.suspend(() =>
            connection.peer.send(
              "control",
              W.ControlClientMessage.encode({
                request_id: "",
                kind: 3,
                payload: { case: "unpublish_track", value: { name } },
              }),
            ),
          ).pipe(
            Effect.timeoutOrElse({
              duration: Math.min(1000, commandTimeout),
              orElse: () =>
                Effect.fail(ReactorError.fromCode("Timeout", "close unpublish: deadline")),
            }),
          ),
        );
        if (Exit.isSuccess(result)) submitted.push(name);
        else {
          const failure = Cause.findError(result.cause);
          errors.push(
            failure._tag === "Success" && ReactorError.is(failure.success)
              ? failure.success
              : ReactorError.fromCode("Shutdown", "publication cleanup failed", {
                  detail: result.cause,
                }),
          );
        }
      }
    return { submitted, errors };
  });

const terminateOwnedRemote = (
  remote: Remote | undefined,
  http: CoordinatorClient,
): Effect.Effect<Termination> =>
  Effect.gen(function* () {
    if (!isKnownRemote(remote) || remote.ownership !== "owned")
      return {
        attempted: false,
        responseReceived: false,
        confirmed: false,
        evidence: null,
        deleteStatus: null,
        state: null,
      };
    const result = yield* Effect.exit(Effect.suspend(() => http.terminate(remote.id)));
    return Exit.isSuccess(result)
      ? result.value
      : {
          attempted: true,
          responseReceived: false,
          confirmed: false,
          evidence: null,
          deleteStatus: null,
          state: null,
          error: ReactorError.fromCode("Shutdown", "remote cleanup did not complete", {
            detail: result.cause,
            sessionId: remote.id,
            outcome: "unknown",
          }),
        };
  });

/** Run once inside Session.close's mask and gate: unpublish, local shutdown,
 * then owned termination. Failure in one phase cannot omit the later evidence. */
export const cleanupSession = (options: {
  readonly connection: Connection | undefined;
  readonly scope: Scope.Closeable;
  readonly remote: RemoteSession;
  readonly http: CoordinatorClient;
  readonly commandTimeout: number;
  readonly retire: (connection: Connection, error: ReactorError) => void;
}): Effect.Effect<CloseReport> =>
  Effect.gen(function* () {
    const { connection } = options;
    const { submitted, errors } = yield* releasePublications(connection, options.commandTimeout);
    if (connection !== undefined) {
      const retired = yield* Effect.result(
        parsed(() =>
          options.retire(connection, ReactorError.fromCode("Aborted", "session closed")),
        ),
      );
      if (Result.isFailure(retired)) errors.push(retired.failure);
    }
    const shutdown = yield* Effect.exit(Scope.close(options.scope, Exit.void));
    if (Exit.isFailure(shutdown))
      errors.push(
        ReactorError.fromCode("Shutdown", "local cleanup did not complete cleanly", {
          detail: shutdown.cause,
        }),
      );
    // Inspect ownership after joining local work: interrupted allocation may
    // have changed its evidence while the generation scope was closing.
    const remote = options.remote.current;
    const termination = yield* terminateOwnedRemote(remote, options.http);
    return Object.freeze({
      localClosed: errors.length === 0,
      allocation: remote === undefined ? "none" : isKnownRemote(remote) ? "known" : "unknown",
      ...(isKnownRemote(remote) ? { ownership: remote.ownership, sessionId: remote.id } : {}),
      remote: termination,
      unpublishSubmitted: Object.freeze(submitted),
      unresolvedPublications: Object.freeze([...(connection?.pendingClaims.values() ?? [])]),
      localErrors: Object.freeze(errors),
    });
  });
