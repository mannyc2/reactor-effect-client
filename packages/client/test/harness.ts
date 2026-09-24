import * as Clock from "effect/Clock";
import type * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as Scope from "effect/Scope";
import { TestClock } from "effect/testing";
import { onTestFinished } from "vitest";
import * as PlatformHttp from "effect/unstable/http/HttpClient";
import * as FetchHttpClient from "effect/unstable/http/FetchHttpClient";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert } from "reactor-effect-test-kit";
import * as FetchHttp from "../src/FetchHttp.js";
import type { ReactorFailure } from "../src/errors.js";
import { testFetch } from "./fixtures.js";

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
interface RunOptions extends Effect.RunOptions {
  /** The test's own clock: every fiber the run starts, the session's included, keeps it. */
  readonly clock?: TestClock.TestClock;
}
/** Concrete HTTP and host services belong to the test composition, not the SDK. */
const provided = <A, E>(effect: Effect.Effect<A, E, Services>, clock?: TestClock.TestClock) => {
  const composed = effect.pipe(
    Effect.provide(Layer.merge(FetchHttp.layer, NodeServices.layer)),
    Effect.provideService(FetchHttpClient.Fetch, testFetch),
  );
  return clock === undefined ? composed : Effect.provideService(composed, Clock.Clock, clock);
};
/**
 * Pass the test context's `signal` so a test that times out interrupts its
 * fiber. Cleanup in a `finally` block runs without it, since the signal is
 * already aborted by then.
 */
export const run = <A, E extends ReactorFailure>(
  effect: Effect.Effect<A, E, Services>,
  options?: RunOptions,
): Promise<A> => Effect.runPromise(provided(effect, options?.clock), options);
export const failure = async <A, E extends ReactorFailure>(
  effect: Effect.Effect<A, E, Services>,
  options?: RunOptions,
): Promise<E> => {
  const result = await Effect.runPromise(provided(Effect.result(effect), options?.clock), options);
  assert(result._tag === "Failure", "expected Effect failure");
  return result.failure;
};
/**
 * A clock that moves only when the test says so. Give it to every `run` of a
 * session, so the session's deadlines, polls and heartbeats use it, then
 * `advance` it: a deadline fires exactly when it is due, and nothing that is
 * not due can happen however long the test waits.
 */
export const testClock = (): Promise<TestClock.TestClock> => {
  const scope = Scope.makeUnsafe();
  onTestFinished(() => Effect.runPromise(Scope.close(scope, Exit.void)));
  return Effect.runPromise(TestClock.make().pipe(Scope.provide(scope)));
};
export const advance = (clock: TestClock.TestClock, duration: Duration.Input): Promise<void> =>
  Effect.runPromise(clock.adjust(duration));
/**
 * Advance `clock` a `step` at a time, letting the event loop run between
 * steps, until `pending` settles; then return its result. Time passes only
 * here, so a deadline fires on the step that reaches it, whenever the timer
 * was registered, and never on a slow runner's account.
 */
export const within = async <A>(
  clock: TestClock.TestClock,
  pending: Promise<A>,
  step: Duration.Input,
  steps = 1_000,
): Promise<A> => {
  let settled = false;
  const done = () => {
    settled = true;
  };
  void pending.then(done, done);
  for (let taken = 0; !settled && taken < steps; taken++) {
    await advance(clock, step);
    await new Promise<void>((resolve) => setImmediate(resolve));
  }
  return pending;
};
