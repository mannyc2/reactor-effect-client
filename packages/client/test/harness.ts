import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as PlatformHttp from "effect/unstable/http/HttpClient";
import * as FetchHttpClient from "effect/unstable/http/FetchHttpClient";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert } from "reactor-effect-test-kit";
import * as FetchHttp from "../src/FetchHttp.js";
import type { ReactorError } from "../src/errors.js";

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

type Services = PlatformHttp.HttpClient | NodeServices.NodeServices;
/** Concrete HTTP and host services belong to the test composition, not the SDK. */
const provided = <A, E>(effect: Effect.Effect<A, E, Services>) =>
  effect.pipe(
    Effect.provide(Layer.merge(FetchHttp.layer, NodeServices.layer)),
    Effect.provideService(FetchHttpClient.Fetch, globalThis.fetch),
  );
/**
 * Pass the test context's `signal` so a test that times out interrupts its
 * fiber. Cleanup in a `finally` block runs without it, since the signal is
 * already aborted by then.
 */
export const run = <A>(
  effect: Effect.Effect<A, ReactorError, Services>,
  options?: Effect.RunOptions,
): Promise<A> => Effect.runPromise(provided(effect), options);
export const failure = async <A>(
  effect: Effect.Effect<A, ReactorError, Services>,
  options?: Effect.RunOptions,
): Promise<ReactorError> => {
  const result = await Effect.runPromise(provided(Effect.result(effect)), options);
  assert(result._tag === "Failure", "expected Effect failure");
  return result.failure;
};
