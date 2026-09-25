import * as Clock from "effect/Clock";
import * as Effect from "effect/Effect";

/**
 * The current clock, with a wall time that a test can step forward or back as
 * a host's wall clock is stepped when it is corrected. Its monotonic time and
 * its timers run on untouched, as they do on a host.
 */
export const steppableWall = Effect.gen(function* () {
  const base = yield* Clock.Clock;
  let offset = 0;
  const millis = () => base.currentTimeMillisUnsafe() + offset;
  const nanos = () => base.currentTimeNanosUnsafe() + BigInt(offset) * 1_000_000n;
  const clock: Clock.Clock = {
    currentTimeMillisUnsafe: millis,
    currentTimeMillis: Effect.sync(millis),
    currentTimeNanosUnsafe: nanos,
    currentTimeNanos: Effect.sync(nanos),
    monotonicTimeNanosUnsafe: () => base.monotonicTimeNanosUnsafe(),
    monotonicTimeNanos: base.monotonicTimeNanos,
    sleep: (duration) => base.sleep(duration),
  };
  return {
    clock,
    /** Steps wall time by `millis`, forward or back. */
    step: (millis: number) =>
      Effect.sync(() => {
        offset += millis;
      }),
  };
});
