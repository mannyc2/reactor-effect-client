import * as NodeRuntime from "@effect/platform-node/NodeRuntime";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { Console, Effect, Layer } from "effect";
import * as Simulation from "reactor-effect-client/simulation";
import { Rundown } from "./Rundown.ts";
import type { Segment } from "./Rundown.ts";

const show: ReadonlyArray<Segment> = [
  { prompt: "A hand-painted sign that reads OPENING NIGHT", seconds: 5 },
  { prompt: "A crowd filing into a small theatre, seen from the balcony", seconds: 6 },
  { prompt: "Stage lights warming up over an empty stage", seconds: 5 },
  { prompt: "A curtain rising on a painted forest", seconds: 8 },
];

/**
 * Offline and unpaid: the SDK's simulation stands in for the model, with the
 * same Engine contract a paid orchestration provides (`Orchestration.layer`
 * with `Orchestration.openH3`), so the Rundown runs unchanged against either.
 */
const Engine = Simulation.layerSim({ buildRatio: 0.2 }).pipe(Layer.provide(NodeServices.layer));

Effect.gen(function* () {
  const rundown = yield* Rundown;
  const outcomes = yield* rundown.play(show);
  for (const [index, outcome] of outcomes.entries())
    yield* Console.log(`${index + 1}. ${outcome._tag.padEnd(8)} ${show[index]?.prompt ?? ""}`);
}).pipe(Effect.provide(Rundown.layer().pipe(Layer.provide(Engine))), NodeRuntime.runMain);
