import { Config, Context, Effect, FileSystem, Layer, Path, Schema } from "effect";
import type { PlatformError } from "effect";
import { H3Source, Session } from "reactor-effect-client";
import type { Playout } from "reactor-effect-client";

/**
 * The channel's durable evidence, as JSON lines in `CHANNEL_EVIDENCE_DIR`:
 * every paid session's owner record, written after allocation and before the
 * session connects, and the cleanup report the playout keeps at
 * shutdown. After a crash, the owner records name the sessions an operator
 * must still confirm terminated. Neither record holds a token.
 */
export class Ledger extends Context.Service<
  Ledger,
  {
    readonly allocated: (
      allocation: H3Source.Allocation,
    ) => Effect.Effect<void, PlatformError.PlatformError | Schema.SchemaError>;
    readonly closed: (report: Playout.Cleanup) => Effect.Effect<void>;
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
      const encodeAllocation = Schema.encodeEffect(Schema.toCodecJson(H3Source.Allocation));
      const encodeReport = Schema.encodeEffect(
        Schema.toCodecJson(
          Schema.Struct({ sessions: Schema.Int, retained: Schema.Array(Session.CloseReport) }),
        ),
      );

      // An owner that cannot be recorded fails the open, so the SDK closes the
      // session it just allocated instead of connecting an unrecorded one. The
      // open fails with the close's report and this failure as its detail, which
      // logs redact, so the ledger logs the failure itself.
      const allocated = Effect.fn("Ledger.allocated")(function* (allocation: H3Source.Allocation) {
        yield* encodeAllocation(allocation).pipe(
          Effect.flatMap((encoded) => append("allocations.jsonl", encoded)),
          Effect.tapError((cause) => Effect.logError("could not record the session owner", cause)),
        );
        yield* Effect.logInfo("session allocated", {
          sessionId: allocation.sessionId,
          endsAt: allocation.endsAt,
        });
      });

      const closed = Effect.fn("Ledger.closed")(function* (report: Playout.Cleanup) {
        // A session that may still be billing, one not confirmed terminated or
        // one whose allocation was never identified, is named.
        const unconfirmed = report.retained
          .filter(Session.mayStillBill)
          .map((entry) => entry.sessionId ?? "unknown");
        if (unconfirmed.length > 0)
          yield* Effect.logWarning("sessions not confirmed terminated", { sessions: unconfirmed });
        yield* Effect.logInfo("channel closed", { sessions: report.sessions });
        yield* encodeReport(report).pipe(
          Effect.flatMap((encoded) => append("cleanup.jsonl", encoded)),
          Effect.catch((cause) => Effect.logError("could not record the cleanup report", cause)),
        );
      });

      return Ledger.of({ allocated, closed });
    }),
  );
}
