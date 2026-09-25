import * as Duration from "effect/Duration";
import * as Option from "effect/Option";
import { ReactorError } from "./errors.js";

/**
 * What one time option admits beyond a positive, finite duration no longer
 * than `maximum`.
 */
export interface DurationPolicy {
  /** The longest accepted duration. Omitted, any finite duration is accepted. */
  readonly maximum?: Duration.Input | undefined;
  /** Accept zero. */
  readonly allowZero?: boolean | undefined;
  /** Accept a signed relative offset, such as an already elapsed deadline. */
  readonly allowNegative?: boolean | undefined;
  /** Accept an infinite duration (`"Infinity"`). */
  readonly allowInfinite?: boolean | undefined;
  /** Accept only a whole number of seconds, for a value the wire carries in seconds. */
  readonly wholeSeconds?: boolean | undefined;
}

const containsNaN = (input: Duration.Input): boolean => {
  if (typeof input === "number") return Number.isNaN(input);
  if (typeof input !== "object" || Duration.isDuration(input)) return false;
  return Object.values(input).some((value) => typeof value === "number" && Number.isNaN(value));
};

/**
 * Decode a caller's time option, as `Duration.fromInput` does, and apply its
 * policy. A bare number is milliseconds, as everywhere in Effect. Input that
 * does not decode, NaN, a negative duration, and anything the policy excludes
 * throw `InvalidInput`, not submitted; run it under `parsed` or `parsedInput`.
 * NaN is rejected before decoding, because `Duration.fromInput` decodes it as
 * zero.
 */
export const duration = (
  input: Duration.Input,
  name: string,
  policy: DurationPolicy = {},
): Duration.Duration => {
  const reject = (requirement: string): never => {
    throw ReactorError.fromCode("InvalidInput", `${name} ${requirement}`, {
      outcome: "not-submitted",
    });
  };
  const decoded = containsNaN(input) ? Option.none() : Duration.fromInput(input);
  if (Option.isNone(decoded)) return reject("is not a duration");
  const value = decoded.value;
  if (Duration.isNegative(value) && policy.allowNegative !== true)
    return reject("must not be negative");
  if (Duration.isZero(value) && policy.allowZero !== true) return reject("must be positive");
  if (!Duration.isFinite(value)) {
    if (policy.allowInfinite === true) return value;
    return reject("must be finite");
  }
  if (policy.maximum !== undefined) {
    const maximum = Duration.fromInputUnsafe(policy.maximum);
    if (Duration.isGreaterThan(value, maximum))
      return reject(`must be at most ${Duration.format(maximum)}`);
  }
  if (policy.wholeSeconds === true && Duration.toMillis(value) % 1000 !== 0)
    return reject("must be a whole number of seconds");
  return value;
};
