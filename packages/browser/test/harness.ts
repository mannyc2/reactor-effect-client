import * as Effect from "effect/Effect";
import type { ReactorError } from "reactor-effect-client";
import { assert } from "reactor-effect-test-kit";

export { test } from "vitest";
export {
  assert,
  equal,
  eventually,
  hex,
  normalize,
  throws,
  unhex,
  withGlobals,
} from "reactor-effect-test-kit";

/**
 * Browser media policy tests need no platform services; hosts are faked per test.
 * Pass the test context's `signal` so a test that times out interrupts its
 * fiber. Cleanup in a `finally` block runs without it, since the signal is
 * already aborted by then.
 */
export const run = <A>(
  effect: Effect.Effect<A, ReactorError>,
  options?: Effect.RunOptions,
): Promise<A> => Effect.runPromise(effect, options);
export const failure = async <A>(
  effect: Effect.Effect<A, ReactorError>,
  options?: Effect.RunOptions,
): Promise<ReactorError> => {
  const result = await Effect.runPromise(Effect.result(effect), options);
  assert(result._tag === "Failure", "expected Effect failure");
  return result.failure;
};
