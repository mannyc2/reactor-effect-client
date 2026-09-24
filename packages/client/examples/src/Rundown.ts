import { Context, Effect, Layer, Result, Schedule, Schema, Stream } from "effect";
import type { ReactorError } from "reactor-effect-client";
import * as Orchestration from "reactor-effect-client/orchestration";

/** One item of a rundown: what to show, and for how long. */
export interface Segment {
  readonly prompt: string;
  readonly seconds: number;
}

/** What became of a segment. */
export const Outcome = Schema.Union([
  /** The clip played to its end. */
  Schema.TaggedStruct("Played", { clipId: Orchestration.ClipId }),
  /** The clip was accepted, then failed or was stopped before its end. */
  Schema.TaggedStruct("Failed", { clipId: Orchestration.ClipId }),
  /** Refused without being sent, and not worth retrying. */
  Schema.TaggedStruct("Refused", { reason: Schema.String }),
  /** Sent, but whether it was applied is unknown: it may still play. Never resent. */
  Schema.TaggedStruct("Unknown", {}),
]);
export type Outcome = typeof Outcome.Type;

/**
 * Plays an ordered list of prompts on any orchestration `Engine`, simulated
 * or paid, keeping a few clips ahead of playback, and reports what became of
 * each one. It is application policy the SDK leaves to its caller: pacing,
 * retrying backpressure, and never resending a request that may already have
 * been applied.
 */
export class Rundown extends Context.Service<
  Rundown,
  {
    readonly play: (
      segments: ReadonlyArray<Segment>,
    ) => Effect.Effect<ReadonlyArray<Outcome>, ReactorError>;
  }
>()("reactor-effect-client-examples/Rundown") {
  static readonly layer = (options: { readonly ahead?: number } = {}) =>
    Layer.effect(
      Rundown,
      Effect.gen(function* () {
        const engine = yield* Orchestration.Engine;
        const ahead = options.ahead ?? 2;

        // A refusal caused by backpressure, or a connection lost before
        // sending, may succeed later. `isRetryable` is never true for an
        // outcome of `unknown`, so a retry can never duplicate a clip.
        const enqueue = (segment: Segment) =>
          engine
            .enqueue(
              new Orchestration.ClipRequest({
                prompt: segment.prompt,
                references: [],
                durationSeconds: segment.seconds,
                metadata: { source: "rundown" },
              }),
            )
            .pipe(
              Effect.retry({
                while: (failure) => failure.isRetryable,
                // Doubling from 250 ms, never more than 5 s apart.
                schedule: Schedule.min([
                  Schedule.exponential("250 millis"),
                  Schedule.spaced("5 seconds"),
                ]),
                times: 10,
              }),
            );

        const play = Effect.fn("Rundown.play")(function* (segments: ReadonlyArray<Segment>) {
          // Subscribe before sending anything, so no clip's start or end can
          // fall between an enqueue and the events that follow it. Events wait
          // in the observation while an enqueue retries, so it holds more than
          // the default; one that still overflows fails the play.
          const observation = yield* engine.observe({ capacity: 1024 });
          const outcomes: Array<Outcome | undefined> = segments.map(() => undefined);
          const clips = new Map<Orchestration.ClipId, { index: number; started: boolean }>();
          let next = 0;
          let waiting = 0;

          // Keep `ahead` of our clips sent and not yet started.
          const send = Effect.gen(function* () {
            while (next < segments.length && waiting < ahead) {
              const index = next++;
              const sent = yield* Effect.result(enqueue(segments[index]!));
              if (Result.isSuccess(sent)) {
                clips.set(sent.success, { index, started: false });
                waiting++;
              } else
                outcomes[index] =
                  sent.failure.context.outcome === "unknown"
                    ? { _tag: "Unknown" }
                    : { _tag: "Refused", reason: sent.failure.reason._tag };
            }
          });

          // Events arrive in order on this one fiber, so an enqueue's reply
          // and the events after it are applied in the order they happened.
          const apply = Effect.fnUntraced(function* (event: Orchestration.EngineEvent) {
            const clip = "clipId" in event ? clips.get(event.clipId) : undefined;
            if (clip === undefined || !("clipId" in event)) return;
            if (!clip.started && (event._tag === "Started" || event._tag === "Failed")) {
              clip.started = true;
              waiting--;
            }
            if (event._tag === "Ended")
              outcomes[clip.index] =
                event.termination === "finished"
                  ? { _tag: "Played", clipId: event.clipId }
                  : { _tag: "Failed", clipId: event.clipId };
            if (event._tag === "Failed")
              outcomes[clip.index] = { _tag: "Failed", clipId: event.clipId };
            yield* send;
          });

          const done = () => outcomes.every((outcome) => outcome !== undefined);
          yield* send;
          if (!done())
            yield* observation.events.pipe(
              Stream.mapEffect((event) => apply(event).pipe(Effect.map(done))),
              Stream.takeUntil((finished) => finished),
              Stream.runDrain,
            );
          // Events stop only when the engine closes: whatever is still open
          // then has no known outcome.
          return outcomes.map((outcome): Outcome => outcome ?? { _tag: "Unknown" });
        }, Effect.scoped);

        return Rundown.of({ play });
      }),
    );
}
