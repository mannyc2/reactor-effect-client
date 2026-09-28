import { Context, Effect, Layer, Result, Schema } from "effect";
import { Playout } from "reactor-effect-client";

/** One item of a rundown: what to show, and for how long. */
export interface Segment {
  readonly prompt: string;
  readonly seconds: number;
}

/** What became of a segment, read from the playout's as-run evidence. */
export const Outcome = Schema.Union([
  /** The clip played to its end. */
  Schema.TaggedStruct("Played", { airedSeconds: Schema.Finite }),
  /** It was accepted, then failed, was dropped or stopped before its end. */
  Schema.TaggedStruct("Failed", { reason: Schema.String }),
  /** Refused before anything was sent. */
  Schema.TaggedStruct("Refused", { reason: Schema.String }),
  /** Sent, but whether it aired is unknown. The playout never resends it. */
  Schema.TaggedStruct("Unknown", {}),
]);
export type Outcome = typeof Outcome.Type;

const outcomeOf = (status: Playout.AsRunStatus): Outcome => {
  switch (status._tag) {
    case "Ended":
      return status.termination === "finished"
        ? { _tag: "Played", airedSeconds: status.airedSeconds }
        : { _tag: "Failed", reason: "stopped" };
    case "Failed":
      return { _tag: "Failed", reason: status.reason._tag };
    case "Dropped":
      return { _tag: "Failed", reason: status.reason };
    default:
      return { _tag: "Unknown" };
  }
};

/**
 * Plays an ordered list of prompts on a `Playout`, paid or simulated, and
 * reports what became of each. Ordering, pacing, retries and never resending
 * an uncertain request are the playout's job; the application only says what
 * to show and reads what aired.
 */
export class Rundown extends Context.Service<
  Rundown,
  { readonly play: (segments: ReadonlyArray<Segment>) => Effect.Effect<ReadonlyArray<Outcome>> }
>()("reactor-effect-client-examples/Rundown") {
  static readonly layer = Layer.effect(
    Rundown,
    Effect.gen(function* () {
      const playout = yield* Playout.Playout;
      const runs = { next: 0 };
      const play = Effect.fn("Rundown.play")(function* (segments: ReadonlyArray<Segment>) {
        const run = runs.next++;
        const handles = yield* Effect.forEach(segments, (segment, index) =>
          playout
            .submit({
              key: Playout.ItemKey.make(`run-${run}-segment-${index}`),
              lane: "show",
              request: { prompt: segment.prompt, seconds: segment.seconds },
            })
            .pipe(Effect.result),
        );
        return yield* Effect.forEach(handles, (handle) =>
          Result.isFailure(handle)
            ? Effect.succeed<Outcome>({ _tag: "Refused", reason: handle.failure._tag })
            : Effect.map(handle.success.outcome, outcomeOf),
        );
      });
      return Rundown.of({ play });
    }),
  );
}
