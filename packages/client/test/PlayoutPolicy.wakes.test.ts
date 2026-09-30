import { describe, it } from "@effect/vitest";
import { Effect, Schema } from "effect";
import { propertyRuns, steps, wakes } from "./PlayoutPolicy.js";

describe("PlayoutPolicy, any script", () => {
  // The plan wakes when something falls due and never polls. The first property's scripts stay
  // short, as their length grows with the arbitrary's size, 10 by default, and scarcely a clip
  // plays in them; these run to 80 steps, so that clips play, the runway falls and deadlines
  // come due.
  for (const { seed, runs } of propertyRuns(3_000))
    it.effect.prop(
      `wakes only when something falls due (seed ${seed ?? "drawn"}, ${String(runs)} scripts)`,
      [Schema.Array(Schema.Literals(steps)).check(Schema.isMaxLength(80))],
      ([script]) => Effect.sync(() => wakes(script)),
      { arbitrary: { runs, size: 80, ...(seed === undefined ? {} : { seed }) }, timeout: 600_000 },
    );
});
