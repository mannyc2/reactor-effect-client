import { describe, it } from "@effect/vitest";
import { Effect, Schema } from "effect";
import { check, propertyRuns, renewing, steps } from "./PlayoutPolicy.js";

describe("PlayoutPolicy, any script", () => {
  // Scripts of 60 to 80 steps on sessions of 20 s, renewed 10 s before their cap: sessions renew
  // and switch within a script, two lanes carry commands at once, and covers go out.
  for (const { seed, runs } of propertyRuns(1_000))
    it.effect.prop(
      `keeps every promise of the plan across renewals (seed ${seed ?? "drawn"}, ${String(runs)} scripts)`,
      [Schema.Array(Schema.Literals(steps)).check(Schema.isMinLength(60), Schema.isMaxLength(80))],
      ([script]) => Effect.sync(() => check(script, renewing)),
      { arbitrary: { runs, size: 80, ...(seed === undefined ? {} : { seed }) }, timeout: 600_000 },
    );
});
