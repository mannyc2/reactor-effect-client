import { Config, Context, Effect, FileSystem, Layer, Path, Schema } from "effect";
import { Session } from "reactor-effect-client";
import type { Playout } from "reactor-effect-client";

/**
 * The channel's durable evidence, in `CHANNEL_EVIDENCE_DIR`: the ledger of its paid sessions,
 * which the SDK keeps there, and the cleanup report the playout keeps at shutdown, appended to
 * `cleanup.jsonl` as a line of JSON. Neither holds a token.
 */
export class Evidence extends Context.Service<
  Evidence,
  {
    /** Where the evidence is kept, created when the channel starts. */
    readonly directory: string;
    readonly closed: (report: Playout.Cleanup) => Effect.Effect<void>;
  }
>()("reactor-effect-example-livestream/Evidence") {
  static readonly layer = Layer.effect(
    Evidence,
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const directory = yield* Config.String("CHANNEL_EVIDENCE_DIR").pipe(
        Config.withDefault(".channel"),
      );
      yield* fs.makeDirectory(directory, { recursive: true });
      const encodeReport = Schema.encodeEffect(
        Schema.toCodecJson(
          Schema.Struct({ sessions: Schema.Int, retained: Schema.Array(Session.CloseReport) }),
        ),
      );

      const closed = Effect.fn("Evidence.closed")(function* (report: Playout.Cleanup) {
        // A session that may still be billing, one not confirmed terminated or
        // one whose allocation was never identified, is named.
        const unconfirmed = report.retained
          .filter(Session.mayStillBill)
          .map((entry) => entry.sessionId ?? "unknown");
        if (unconfirmed.length > 0)
          yield* Effect.logWarning("sessions not confirmed terminated", { sessions: unconfirmed });
        yield* Effect.logInfo("channel closed", { sessions: report.sessions });
        yield* encodeReport(report).pipe(
          Effect.flatMap((encoded) =>
            fs.writeFileString(
              path.join(directory, "cleanup.jsonl"),
              `${JSON.stringify(encoded)}\n`,
              { flag: "a" },
            ),
          ),
          Effect.catch((cause) => Effect.logError("could not record the cleanup report", cause)),
        );
      });

      return Evidence.of({ directory, closed });
    }),
  );
}
