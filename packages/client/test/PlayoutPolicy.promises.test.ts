import { describe, it } from "@effect/vitest";
import { Effect, Schema } from "effect";
import { check, propertyRuns, steps } from "./PlayoutPolicy.js";

describe("PlayoutPolicy, any script", () => {
  // Any sequence of edits, provider answers, builds, plays and losses keeps the plan's promises.
  for (const { seed, runs } of propertyRuns(3_000))
    it.effect.prop(
      `keeps every promise of the plan (seed ${seed ?? "drawn"}, ${String(runs)} scripts)`,
      [Schema.Array(Schema.Literals(steps)).check(Schema.isMaxLength(80))],
      ([script]) => Effect.sync(() => check(script)),
      { arbitrary: { runs, ...(seed === undefined ? {} : { seed }) }, timeout: 600_000 },
    );
});
