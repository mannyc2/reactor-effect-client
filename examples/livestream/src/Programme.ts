import { Clock, Context, Duration, Effect, Layer, Semaphore, Stream } from "effect";
import * as Orchestration from "reactor-effect-client/orchestration";
import { ChannelBusy, ChannelUnavailable, PromptRejected } from "./Api.ts";
import type { Submitted } from "./Api.ts";
import { Settings } from "./Settings.ts";

/** What plays when no viewer has asked for anything. */
const house = [
  "A slow dolly shot through a sunlit greenhouse full of ferns",
  "Waves rolling onto a black sand beach at dusk, seen from a low angle",
  "A paper lantern drifting along a quiet canal at night",
  "A time-lapse of clouds pouring over a mountain ridge",
];

/**
 * The channel's scheduling policy, which the SDK leaves to the application.
 *
 * A viewer's prompt is admitted while the content already committed (the
 * rest of the playing clip and every upcoming one, across sources) stays
 * within the renewal lead less one clip. After a renewal's replacement is
 * prepared, new clips go to it, so the session being retired never holds
 * more than it can play before it expires: every renewal switches cleanly,
 * with no clip lost. A house rotation keeps one clip upcoming so the channel
 * never runs dry.
 *
 * Admission reads the engine's state, then enqueues. A paid enqueue is a
 * round trip, so two admissions at once would each see room the other is
 * about to take: every admission, the house rotation's included, holds one
 * permit from the read to the enqueue's outcome.
 */
export class Programme extends Context.Service<
  Programme,
  {
    readonly submit: (
      prompt: string,
    ) => Effect.Effect<Submitted, ChannelBusy | ChannelUnavailable | PromptRejected>;
  }
>()("reactor-effect-example-livestream/Programme") {
  static readonly layer = Layer.effect(
    Programme,
    Effect.gen(function* () {
      const engine = yield* Orchestration.Engine;
      const { clipSeconds, lead } = yield* Settings;
      const budget = Duration.toMillis(lead) - clipSeconds * 1000;
      const admission = yield* Semaphore.make(1);
      const clip = (prompt: string, source: "viewer" | "house") =>
        new Orchestration.ClipRequest({
          prompt,
          references: [],
          durationSeconds: clipSeconds,
          metadata: { source },
        });

      const submit = Effect.fn("Programme.submit")(function* (prompt: string) {
        const now = yield* Clock.currentTimeMillis;
        const excess =
          Orchestration.committedMs(yield* engine.state, now) + clipSeconds * 1000 - budget;
        if (excess > 0)
          return yield* new ChannelBusy({
            message: "The next clips are already lined up",
            retryAfterSeconds: Math.ceil(excess / 1000),
          });
        return yield* engine.enqueue(clip(prompt, "viewer")).pipe(
          Effect.map((clipId): Submitted => ({ _tag: "Accepted", clipId })),
          // Refusals were decided locally and never sent: route on their reason.
          Effect.catchReasons(
            "PolicyFailure",
            {
              QueueFull: () =>
                new ChannelBusy({
                  message: "The generation queue is full",
                  retryAfterSeconds: clipSeconds,
                }),
              InvalidRequest: () =>
                new PromptRejected({ message: "The prompt is outside the model's limits" }),
            },
            (reason) =>
              new ChannelUnavailable({
                message: `The channel is not taking prompts (${reason._tag})`,
              }),
          ),
          // A command's evidence says whether the provider may have it. An
          // unknown outcome may still play, so it is reported, never resent.
          Effect.catchTag("CommandFailure", (failure) =>
            failure.context.outcome === "unknown"
              ? Effect.succeed<Submitted>({ _tag: "Unconfirmed" })
              : new ChannelUnavailable({
                  message: `The channel could not send the prompt (${failure.reason._tag})`,
                }),
          ),
        );
      }, admission.withPermits(1));

      // The engine's events say when to look again; its state says what to
      // do. The slow tick covers a quiet engine, and makes the first clip.
      let turn = 0;
      const fill = Effect.gen(function* () {
        const state = yield* engine.state;
        if (state.availability !== "Ready" || Orchestration.pendingCount(state) > 0) return;
        const prompt = house[turn++ % house.length] ?? "";
        yield* engine
          .enqueue(clip(prompt, "house"))
          .pipe(
            Effect.catch((failure) =>
              Effect.logWarning("house clip refused", { reason: failure.reason._tag }),
            ),
          );
      }).pipe(admission.withPermits(1));
      yield* engine.events.pipe(
        Stream.merge(Stream.tick("2 seconds")),
        Stream.runForEach(() => fill),
        Effect.catch((failure) =>
          Effect.logWarning("house rotation stopped", { reason: failure.reason._tag }),
        ),
        Effect.forkScoped,
      );

      return Programme.of({ submit });
    }),
  );
}
