import * as Effect from "effect/Effect";
import * as Orchestration from "reactor-effect-client/orchestration";
import * as Simulation from "reactor-effect-client/simulation";
import { webCrypto } from "./web-crypto.mjs";

/** Fully offline: no coordinator, credentials, native library or provider allocation. */
export const simulateClip = Effect.scoped(
  Effect.gen(function* () {
    const handle = yield* Simulation.make({ buildRatio: 0, fixedBuildTime: 0 });
    const prepared = yield* handle.engine.prepare(
      new Orchestration.ClipRequest({
        prompt: "An unpaid local simulation",
        references: [],
        durationSeconds: 5,
        metadata: { example: "simulation" },
        sequence: { id: "example", memberId: "first", final: true },
      }),
    );
    // Preparation is inert. Joining the same committed handle does not dispatch twice.
    const clipId = yield* prepared.submit;
    const joinedClipId = yield* prepared.submit;
    const cleanup = yield* handle.close;
    const repeatedCleanup = yield* handle.close;
    return { clipId, joinedClipId, cleanup, repeatedCleanup };
  }),
).pipe(Effect.provide(webCrypto));
