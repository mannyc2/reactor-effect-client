/**
 * Admission under concurrency. Prompts that arrive together are admitted one
 * at a time, so together they never commit more than the renewal lead less
 * one clip, even when each enqueue is a round trip, as a paid one is. The
 * simulation's own enqueue returns at once, which hides the difference, so
 * the engine here takes 150 ms per enqueue.
 */
import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, describe, it } from "@effect/vitest";
import { Clock, ConfigProvider, Effect, Fiber, Layer, Result } from "effect";
import { TestClock } from "effect/testing";
import * as Orchestration from "reactor-effect-client/orchestration";
import * as Simulation from "reactor-effect-client/simulation";
import { Programme } from "../src/Programme.ts";
import { Settings } from "../src/Settings.ts";

/** The simulation's engine, with a round trip before every enqueue. */
const RoundTripEngine = Layer.effect(
  Orchestration.Engine,
  Effect.gen(function* () {
    const engine = yield* Orchestration.Engine;
    return Orchestration.Engine.of({
      ...engine,
      enqueue: (request) =>
        Effect.sleep("150 millis").pipe(Effect.andThen(engine.enqueue(request))),
    });
  }),
).pipe(Layer.provide(Simulation.layerSim().pipe(Layer.provide(NodeServices.layer))));

/** 40-second sessions, a 20-second lead and 5-second clips: at most 15 seconds committed. */
const TestSettings = Settings.layer.pipe(
  Layer.provide(
    ConfigProvider.layer(
      ConfigProvider.fromUnknown({
        CHANNEL_SESSION_LENGTH: "40 seconds",
        CHANNEL_RENEWAL_LEAD: "20 seconds",
        CHANNEL_CLIP_SECONDS: 5,
      }),
    ),
  ),
);

describe("Programme", () => {
  it.effect("admits prompts that arrive together one at a time, within the lead", () =>
    Effect.gen(function* () {
      const programme = yield* Programme;
      const engine = yield* Orchestration.Engine;
      const submitted = yield* Effect.forEach(
        Array.from({ length: 10 }, (_, index) => `prompt ${index}`),
        (prompt) => Effect.result(programme.submit(prompt)),
        { concurrency: "unbounded" },
      ).pipe(Effect.forkChild);
      yield* TestClock.adjust("3 seconds");
      const results = yield* Fiber.join(submitted);
      const committed = Orchestration.committedMs(
        yield* engine.state,
        yield* Clock.currentTimeMillis,
      );
      assert.isAtMost(committed, 15_000);
      assert.isAbove(results.filter(Result.isSuccess).length, 0);
      assert.isTrue(
        results.some((result) => Result.isFailure(result) && result.failure._tag === "ChannelBusy"),
      );
    }).pipe(
      Effect.provide(
        Programme.layer.pipe(Layer.provideMerge(Layer.merge(RoundTripEngine, TestSettings))),
      ),
    ),
  );
});
