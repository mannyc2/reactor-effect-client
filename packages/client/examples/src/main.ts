import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import * as NodeRuntime from "@effect/platform-node/NodeRuntime";
import { Console, Effect, Layer } from "effect";
import { Coordinator, H3, H3Source, Playout, Reactor, ReactorTest } from "reactor-effect-client";
import { Rundown } from "./Rundown.ts";
import type { Segment } from "./Rundown.ts";

const show: ReadonlyArray<Segment> = [
  { prompt: "A hand-painted sign that reads OPENING NIGHT", seconds: 5 },
  { prompt: "A crowd filing into a small theatre, seen from the balcony", seconds: 6 },
  { prompt: "Stage lights warming up over an empty stage", seconds: 5 },
  { prompt: "A curtain rising on a painted forest", seconds: 8 },
];

/** Opens H3 on tokens minted with the simulated Reactor's key; a paid deployment mints with its own. */
const open = Effect.gen(function* () {
  const test = yield* ReactorTest.ReactorTest;
  const coordinator = yield* Coordinator.Coordinator;
  return yield* H3Source.open({
    tokens: coordinator.tokens({
      apiKey: test.apiKey,
      modelName: H3.modelName,
      maxSessionDuration: "10 minutes",
      expiresAfter: "15 minutes",
    }),
  });
});

/**
 * Offline and unpaid, in real time: the simulated Reactor plays the timing two
 * paid runs measured. A paid deployment swaps `ReactorTest.layer` for an HTTP
 * client and a host, and the Rundown does not change.
 */
const Offline = Rundown.layer.pipe(
  Layer.provide(Playout.layer({ open, lanes: [{ name: "show" }] })),
  Layer.provideMerge(Reactor.layer()),
  Layer.provideMerge(Coordinator.layer()),
  Layer.provideMerge(ReactorTest.layer({ timing: ReactorTest.Timing.hosted })),
  Layer.provide(NodeCrypto.layer),
);

Effect.gen(function* () {
  const rundown = yield* Rundown;
  const outcomes = yield* rundown.play(show);
  for (const [index, outcome] of outcomes.entries())
    yield* Console.log(`${index + 1}. ${outcome._tag.padEnd(8)} ${show[index]?.prompt ?? ""}`);
}).pipe(
  // The program's entry point, the one place a layer is provided.
  // @effect-diagnostics-next-line strictEffectProvide:off
  Effect.provide(Offline),
  NodeRuntime.runMain,
);
