import { Clock, Context, Duration, Effect, Layer, Ref } from "effect";
import { Playout } from "reactor-effect-client";
import { ChannelBusy, ChannelUnavailable, PromptRejected } from "./Api.ts";
import type { Submitted } from "./Api.ts";
import { Settings } from "./Settings.ts";

/**
 * Viewers' prompts, as the channel's playout admits them. A prompt must be able
 * to start before the renewal lead, less one clip, runs out, so a session being
 * retired never holds more than it can play before it ends. One that could not
 * is refused with how long to wait; one admitted that still can't start by
 * then is dropped by the playout as late.
 */
export class Programme extends Context.Service<
  Programme,
  {
    readonly submit: (
      prompt: string,
    ) => Effect.Effect<Submitted, ChannelBusy | ChannelUnavailable | PromptRejected>;
    /** The prompt a viewer's item was submitted with. */
    readonly prompt: (key: string) => Effect.Effect<string | undefined>;
    /** How long after it is sent a prompt must be able to start. */
    readonly startWithin: Duration.Duration;
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
      // Only prompts still lined up or in the as-run log are looked up, so the
      // latest thousand are kept.
      const remember = (key: string, prompt: string) =>
        Ref.update(prompts, (all) => {
          const kept = new Map(all).set(key, prompt);
          for (const oldest of kept.keys()) {
            if (kept.size <= 1000) break;
            kept.delete(oldest);
          }
          return kept;
        });

      // Seconds until a prompt sent now could start in time, 0 when it could
      // now. It joins the end of the viewers' lane, so it starts after the
      // rest of the clip on air and every viewer prompt the playout holds,
      // each a clip long; house clips give way to viewers' prompts.
      const wait = Effect.gen(function* () {
        const { playing, lanes } = yield* playout.state;
        const now = yield* Clock.currentTimeMillis;
        const rest =
          playing?.seconds === undefined
            ? 0
            : Math.max(0, playing.startedAt + playing.seconds * 1000 - now);
        const waiting = lanes.flatMap((lane) => lane.keys).filter((key) => key !== playing?.key);
        const late = rest + waiting.length * clipSeconds * 1000 - Duration.toMillis(startBy);
        return late > 0 ? Math.ceil(late / 1000) : 0;
      });
      const busy = (retryAfterSeconds: number) =>
        ChannelBusy.make({ message: "The next clips are already lined up", retryAfterSeconds });

      const submit = Effect.fn("Programme.submit")(function* (prompt: string) {
        // The playout counts the prompts ahead that it has not built yet by
        // their build, not their air, so it admits a burst and later drops
        // what runs late. A prompt that could not start in time is refused here.
        const late = yield* wait;
        if (late > 0) return yield* busy(late);
        const key = Playout.ItemKey.make(
          `viewer-${yield* Ref.getAndUpdate(count, (value) => value + 1)}`,
        );
        // Recorded first, so the playout's first report of the item has its words.
        yield* remember(key, prompt);
        return yield* playout
          .submit({
            key,
            lane: "viewer",
            request: { prompt, seconds: clipSeconds },
            window: { startBy, firm: true },
          })
          .pipe(
            Effect.as({ _tag: "Accepted", key } satisfies Submitted),
            Effect.tapError(() =>
              Ref.update(prompts, (all) => {
                const rest = new Map(all);
                rest.delete(key);
                return rest;
              }),
            ),
            Effect.catchTags({
              WouldMissDeadline: () =>
                Effect.flatMap(wait, (seconds) =>
                  busy(seconds > 0 ? seconds : Math.ceil(clipSeconds)),
                ),
              InvalidItem: () =>
                PromptRejected.make({ message: "The prompt is outside the model's limits" }),
              PlayoutClosed: () =>
                ChannelUnavailable.make({ message: "The channel is not taking prompts" }),
              KeyMismatch: () =>
                ChannelUnavailable.make({ message: "The channel could not take the prompt" }),
              LaneBusy: () =>
                ChannelBusy.make({
                  message: "The channel is busy",
                  retryAfterSeconds: Math.ceil(clipSeconds),
                }),
            }),
          );
      });

      return Programme.of({
        submit,
        prompt: (key) => Effect.map(Ref.get(prompts), (all) => all.get(key)),
        startWithin: startBy,
      });
    }),
  );
}
