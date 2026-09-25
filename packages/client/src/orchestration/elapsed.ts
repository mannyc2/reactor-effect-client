import type * as Clock from "effect/Clock";

/**
 * The clock's monotonic time in whole milliseconds. Elapsed time, such as a
 * source's age or a build's length, is measured on it, so a correction of the
 * host's wall clock moves no deadline and bends no measurement. Its origin is
 * arbitrary: a reading is only ever compared with another reading.
 */
export const monotonicMillis = (clock: Clock.Clock): number =>
  Number(clock.monotonicTimeNanosUnsafe() / 1_000_000n);
