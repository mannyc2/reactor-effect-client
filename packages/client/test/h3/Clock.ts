/**
 * How the H3 suites run on time. Every test runs on a `TestClock`, so no reply
 * deadline, reconcile window or hook bound can fire on a slow machine's
 * account. `run` holds the clock still: a test that needs time to pass says
 * how much. `runFlowing` moves it a millisecond each time every other ready
 * fiber has had its turn, so windows elapse in order, as in real time, but
 * only after the work before them has run.
 */
import { Effect, Layer } from "effect";
import type { Crypto, Scope } from "effect";
import { TestClock } from "effect/testing";
import * as NodeCrypto from "@effect/platform-node/NodeCrypto";

export const run = <A, E>(effect: Effect.Effect<A, E, Scope.Scope | Crypto.Crypto>) =>
  Effect.runPromise(
    Effect.scoped(effect.pipe(Effect.provide(Layer.merge(NodeCrypto.layer, TestClock.layer())))),
  );

export const runFlowing = <A, E>(effect: Effect.Effect<A, E, Scope.Scope | Crypto.Crypto>) =>
  run(
    Effect.gen(function* () {
      yield* Effect.forkScoped(
        Effect.forever(Effect.andThen(Effect.yieldNow, TestClock.adjust(1))),
      );
      return yield* effect;
    }),
  );

/** Yield until `predicate` holds; time does not pass while it waits. */
export const waitFor = (predicate: () => Effect.Effect<boolean>) =>
  Effect.gen(function* () {
    for (let turn = 0; !(yield* predicate()); turn++) {
      if (turn === 100_000)
        return yield* Effect.die(new Error("waitFor: the condition never held"));
      yield* Effect.yieldNow;
    }
  });
