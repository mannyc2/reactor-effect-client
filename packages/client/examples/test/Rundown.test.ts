/**
 * The Rundown against the simulated Reactor on the test clock: minutes of
 * programme run in milliseconds, with no network and nothing paid. Faults stand
 * in for a failed build and a reply lost on its way back.
 */
import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import { assert, layer } from "@effect/vitest";
import { Effect, Layer } from "effect";
import { Coordinator, H3, H3Source, Playout, Reactor, ReactorTest } from "reactor-effect-client";
import { Rundown } from "../src/Rundown.ts";

const mint = Effect.gen(function* () {
  const test = yield* ReactorTest.ReactorTest;
  const coordinator = yield* Coordinator.Coordinator;
  return yield* coordinator.mintToken({
    apiKey: test.apiKey,
    modelName: H3.modelName,
    maxSessionDuration: "10 minutes",
    expiresAfter: "15 minutes",
  });
});

const simulated = Rundown.layer.pipe(
  Layer.provideMerge(Playout.layer({ open: H3Source.open({ mint }), lanes: [{ name: "show" }] })),
  Layer.provideMerge(Reactor.layer()),
  Layer.provideMerge(Coordinator.layer()),
  Layer.provideMerge(
    ReactorTest.layer({ timing: ReactorTest.Timing.fixed({ buildSpeed: 2.4, seam: "70 millis" }) }),
  ),
  Layer.provide(NodeCrypto.layer),
);

const segments = [
  { prompt: "one", seconds: 5 },
  { prompt: "two", seconds: 5 },
  { prompt: "three", seconds: 5 },
];

layer(simulated)("Rundown", (it) => {
  it.effect("plays every segment to its end, in order", () =>
    Effect.gen(function* () {
      yield* Effect.forkScoped(ReactorTest.flow("20 millis"));
      const outcomes = yield* (yield* Rundown).play(segments);
      assert.deepStrictEqual(
        outcomes.map((outcome) => outcome._tag),
        ["Played", "Played", "Played"],
      );
    }),
  );

  it.effect("reports a failed build as Failed and plays the rest", () =>
    Effect.gen(function* () {
      yield* Effect.forkScoped(ReactorTest.flow("20 millis"));
      const test = yield* ReactorTest.ReactorTest;
      yield* test.inject({ _tag: "FailBuild", nth: 2 });
      const outcomes = yield* (yield* Rundown).play(segments);
      assert.deepStrictEqual(
        outcomes.map((outcome) => outcome._tag),
        ["Played", "Failed", "Played"],
      );
    }),
  );
});
