import { describe, it } from "@effect/vitest";
import { Effect, Schema } from "effect";
import { check, lasting, propertyRuns, steps, strictLine } from "./PlayoutPolicy.js";

describe("PlayoutPolicy, any script in a strict lane", () => {
  // The promises again with the line lane strict, on scripts of up to 80 steps, so that several of
  // its items wait behind one another and play. It refuses `Asap` and `Manual` starts, so the
  // scripts send those in turn instead.
  for (const { seed, runs } of propertyRuns(3_000))
    it.effect.prop(
      `keeps every promise of the plan in a strict lane (seed ${seed ?? "drawn"}, ${String(runs)} scripts)`,
      [Schema.Array(Schema.Literals(steps)).check(Schema.isMaxLength(80))],
      ([script]) => Effect.sync(() => check(script, lasting, strictLine)),
      { arbitrary: { runs, size: 80, ...(seed === undefined ? {} : { seed }) }, timeout: 600_000 },
    );
});
