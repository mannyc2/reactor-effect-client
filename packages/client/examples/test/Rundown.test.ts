/**
 * The Rundown against the SDK's simulation, on the test clock: minutes of
 * programme run in milliseconds, with no network and nothing paid. The
 * simulation's faults stand in for a build failure and a lost session.
 */
import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, describe, it } from "@effect/vitest";
import { Effect, Fiber, Layer, Stream } from "effect";
import { TestClock } from "effect/testing";
import * as Orchestration from "reactor-effect-client/orchestration";
import * as Simulation from "reactor-effect-client/simulation";
import { Rundown } from "../src/Rundown.ts";
import type { Segment } from "../src/Rundown.ts";

const segments: ReadonlyArray<Segment> = [
  { prompt: "one", seconds: 5 },
  { prompt: "two", seconds: 5 },
  { prompt: "three", seconds: 5 },
];

const simulated = (options: Simulation.SimOptions = {}) =>
  Layer.provideMerge(
    Rundown.layer(),
    Simulation.layerSim(options).pipe(Layer.provide(NodeServices.layer)),
  );

/** Plays the segments while the test clock runs, recording the engine's events alongside. */
const play = Effect.gen(function* () {
  const engine = yield* Orchestration.Engine;
  const events: Orchestration.EngineEvent[] = [];
  // Subscribed here, before the play is forked, so no event is missed.
  const observation = yield* engine.observe({ capacity: 1024 });
  yield* observation.events.pipe(
    Stream.runForEach((event) => Effect.sync(() => events.push(event))),
    Effect.forkChild,
  );
  const played = yield* (yield* Rundown).play(segments).pipe(Effect.forkChild);
  yield* TestClock.adjust("5 minutes");
  return { outcomes: yield* Fiber.join(played), events };
});

describe("Rundown", () => {
  it.effect("plays every segment, in order", () =>
    Effect.gen(function* () {
      const engine = yield* Orchestration.Engine;
      const started = yield* engine.events.pipe(
        Stream.filter((event) => event._tag === "Started"),
        Stream.take(3),
        Stream.runCollect,
        Effect.forkChild,
      );
      const { outcomes } = yield* play;
      assert.deepStrictEqual(
        outcomes.map((outcome) => outcome._tag),
        ["Played", "Played", "Played"],
      );
      assert.deepStrictEqual(
        (yield* Fiber.join(started)).map((event) => event.clipId),
        outcomes.map((outcome) => (outcome._tag === "Played" ? outcome.clipId : "")),
      );
    }).pipe(Effect.provide(simulated())),
  );

  it.effect("waits for room while the generation queue is full", () =>
    Effect.gen(function* () {
      const { outcomes } = yield* play;
      assert.deepStrictEqual(
        outcomes.map((outcome) => outcome._tag),
        ["Played", "Played", "Played"],
      );
    }).pipe(Effect.provide(simulated({ queueLimit: 1 }))),
  );

  it.effect("reports a failed clip and carries on", () =>
    Effect.gen(function* () {
      const { outcomes } = yield* play;
      assert.deepStrictEqual(
        outcomes.map((outcome) => outcome._tag),
        ["Played", "Failed", "Played"],
      );
    }).pipe(Effect.provide(simulated({ faults: { buildFails: (sequence) => sequence === 2 } }))),
  );

  it.effect("never resends a clip whose enqueue outcome is unknown", () =>
    Effect.gen(function* () {
      const { outcomes, events } = yield* play;
      assert.strictEqual(outcomes[1]?._tag, "Unknown");
      // Only the other two segments were ever queued: the second was not resent.
      assert.strictEqual(events.filter((event) => event._tag === "Queued").length, 2);
    }).pipe(Effect.provide(simulated({ faults: { sessionFails: (sequence) => sequence === 2 } }))),
  );
});
