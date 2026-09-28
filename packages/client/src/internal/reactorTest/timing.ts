/**
 * Draws each simulated delay from its range with one seeded `Random`, so a run
 * repeats exactly for the same seed. A range whose ends meet is fixed.
 */
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Random from "effect/Random";
import type { Timing } from "../../ReactorTest.js";

export type Delay =
  | "http"
  | "channel"
  | "allocation"
  | "negotiation"
  | "connect"
  | "seam"
  | "stop"
  | "moderation";

export interface Sampler {
  /** A delay in milliseconds. */
  readonly delay: (which: Delay) => Effect.Effect<number>;
  /** Seconds of video built per second of build time. */
  readonly buildSpeed: Effect.Effect<number>;
  /** The same for a clip that continues another. */
  readonly continuedBuildSpeed: Effect.Effect<number>;
}

export const make = Effect.fnUntraced(function* (timing: Timing) {
  const random = yield* Effect.withFiber((fiber) =>
    Effect.succeed(fiber.getRef(Random.Random)),
  ).pipe(Random.withSeed(timing.seed));
  const between = (min: number, max: number): Effect.Effect<number> =>
    min === max
      ? Effect.succeed(min)
      : Random.nextBetween(min, max).pipe(Effect.provideService(Random.Random, random));
  const sampler: Sampler = {
    delay: (which) =>
      between(Duration.toMillis(timing[which].min), Duration.toMillis(timing[which].max)),
    buildSpeed: between(timing.buildSpeed.min, timing.buildSpeed.max),
    continuedBuildSpeed: between(timing.continuedBuildSpeed.min, timing.continuedBuildSpeed.max),
  };
  return sampler;
});
