import * as Effect from "effect/Effect";
import type { ReactorError } from "reactor-effect-client";
import { assert } from "reactor-effect-test-kit";

export { test } from "bun:test";
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

/** Modeled-host regression tests of the integration helpers; no platform services. */
export const run = <A>(effect: Effect.Effect<A, ReactorError>): Promise<A> =>
  Effect.runPromise(effect);
export const failure = async <A>(effect: Effect.Effect<A, ReactorError>): Promise<ReactorError> => {
  const result = await Effect.runPromise(Effect.result(effect));
  assert(result._tag === "Failure", "expected Effect failure");
  return result.failure;
};
