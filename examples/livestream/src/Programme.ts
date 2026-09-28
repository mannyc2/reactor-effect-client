import { Context, Duration, Effect, Layer, Ref } from "effect";
import { Playout } from "reactor-effect-client";
import { ChannelBusy, ChannelUnavailable, PromptRejected } from "./Api.ts";
import type { Submitted } from "./Api.ts";
import { Settings } from "./Settings.ts";

/**
 * Viewers' prompts, as the channel's playout admits them. A prompt must be able
 * to start before the renewal lead, less one clip, runs out, so a session being
 * retired never holds more than it can play before it ends: the playout refuses
 * one that could not (`WouldMissDeadline`) and the viewer is asked to wait.
 */
export class Programme extends Context.Service<
  Programme,
  {
    readonly submit: (
      prompt: string,
    ) => Effect.Effect<Submitted, ChannelBusy | ChannelUnavailable | PromptRejected>;
    /** The prompt a viewer's clip was submitted with. */
    readonly prompt: (key: string) => Effect.Effect<string | undefined>;
  }
>()("reactor-effect-example-livestream/Programme") {
  static readonly layer = Layer.effect(
    Programme,
    Effect.gen(function* () {
      const playout = yield* Playout.Playout;
      const { clipSeconds, lead } = yield* Settings;
      const prompts = yield* Ref.make(new Map<string, string>());
      const count = yield* Ref.make(0);
      const startBy = Duration.subtract(lead, Duration.seconds(clipSeconds));

      const submit = Effect.fn("Programme.submit")(function* (prompt: string) {
        const key = Playout.ItemKey.make(
          `viewer-${yield* Ref.getAndUpdate(count, (value) => value + 1)}`,
        );
        yield* playout
          .submit({
            key,
            lane: "viewer",
            request: { prompt, seconds: clipSeconds },
            window: { startBy, firm: true },
          })
          .pipe(
            Effect.catchTags({
              WouldMissDeadline: () =>
                new ChannelBusy({
                  message: "The next clips are already lined up",
                  retryAfterSeconds: Math.ceil(clipSeconds),
                }),
              InvalidItem: () =>
                new PromptRejected({ message: "The prompt is outside the model's limits" }),
              PlayoutClosed: () =>
                new ChannelUnavailable({ message: "The channel is not taking prompts" }),
              KeyMismatch: () =>
                new ChannelUnavailable({ message: "The channel could not take the prompt" }),
              LaneBusy: () =>
                new ChannelBusy({
                  message: "The channel is busy",
                  retryAfterSeconds: Math.ceil(clipSeconds),
                }),
            }),
          );
        yield* Ref.update(prompts, (all) => new Map(all).set(key, prompt));
        return { _tag: "Accepted", clipId: key } satisfies Submitted;
      });

      return Programme.of({
        submit,
        prompt: (key) => Effect.map(Ref.get(prompts), (all) => all.get(key)),
      });
    }),
  );
}
