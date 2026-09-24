import { Config, Context, Effect, FileSystem, Layer, Path, Schema } from "effect";
import { ReactorError } from "reactor-effect-client";
import * as Orchestration from "reactor-effect-client/orchestration";

/**
 * The channel's durable evidence, as JSON lines in `CHANNEL_EVIDENCE_DIR`:
 * every paid session's owner record, written after allocation and before the
 * session connects, and the cleanup report the orchestration returns at
 * shutdown. After a crash, the owner records name the sessions an operator
 * must still confirm terminated. Neither record holds a token.
 */
export class Ledger extends Context.Service<
  Ledger,
  {
    readonly allocated: (allocation: Orchestration.Allocation) => Effect.Effect<void, ReactorError>;
    readonly closed: (report: Orchestration.CleanupReport) => Effect.Effect<void>;
  }
>()("reactor-effect-example-livestream/Ledger") {
  static readonly layer = Layer.effect(
    Ledger,
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const directory = yield* Config.String("CHANNEL_EVIDENCE_DIR").pipe(
        Config.withDefault(".channel"),
      );
      yield* fs.makeDirectory(directory, { recursive: true });
      const append = (file: string, value: unknown) =>
        fs.writeFileString(path.join(directory, file), `${JSON.stringify(value)}\n`, {
          flag: "a",
        });
      const encodeAllocation = Schema.encodeEffect(Schema.toCodecJson(Orchestration.Allocation));
      const encodeReport = Schema.encodeEffect(Schema.toCodecJson(Orchestration.CleanupReport));

      // An owner that cannot be recorded fails the open, so the SDK closes the
      // session it just allocated instead of connecting an unrecorded one.
      const allocated = Effect.fn("Ledger.allocated")(function* (
        allocation: Orchestration.Allocation,
      ) {
        yield* encodeAllocation(allocation).pipe(
          Effect.flatMap((encoded) => append("allocations.jsonl", encoded)),
          Effect.mapError((cause) =>
            ReactorError.fromCode("InvalidState", "could not record the session owner", {
              detail: cause,
            }),
          ),
        );
        yield* Effect.logInfo("session allocated", {
          sessionId: allocation.sessionId,
          expiresAt: allocation.expiresAt,
        });
      });

      const closed = Effect.fn("Ledger.closed")(function* (report: Orchestration.CleanupReport) {
        // A session that may have been allocated and was not confirmed
        // terminated may still be billing: name it.
        const unconfirmed = report.sessions
          .filter((entry) => entry.lease.allocation !== "none" && !entry.lease.remote.confirmed)
          .map((entry) => entry.lease.sessionId ?? "unknown");
        if (unconfirmed.length > 0)
          yield* Effect.logWarning("sessions not confirmed terminated", { sessions: unconfirmed });
        yield* Effect.logInfo("channel closed", { sessions: report.sessions.length });
        yield* encodeReport(report).pipe(
          Effect.flatMap((encoded) => append("cleanup.jsonl", encoded)),
          Effect.catch((cause) => Effect.logError("could not record the cleanup report", cause)),
        );
      });

      return Ledger.of({ allocated, closed });
    }),
  );
}
