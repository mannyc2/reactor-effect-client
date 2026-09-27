/**
 * Throwing validators lifted into Effect, for the modules the Effect-native
 * rewrite has not reached yet (H3, orchestration, the simulation and the host
 * packages). Rewritten code decodes with Schema and fails with
 * `return yield* error`; each rewrite step removes its uses of these.
 */
import * as Effect from "effect/Effect";
import * as Result from "effect/Result";
import { ReactorError } from "../ReactorError.js";
import type { MessageCode } from "../ReactorError.js";

/**
 * Runs a synchronous check that rejects by throwing a `ReactorError`. The
 * rejection becomes the failure; anything else thrown is a bug and stays one.
 */
export const parse = <A>(evaluate: () => A): Result.Result<A, ReactorError> => {
  try {
    return Result.succeed(evaluate());
  } catch (cause) {
    if (ReactorError.is(cause)) return Result.fail(cause);
    throw cause;
  }
};

/** `parse` as an Effect: a rejection fails it and a bug is a defect. */
export const parsed = <A>(evaluate: () => A): Effect.Effect<A, ReactorError> =>
  Effect.suspend(() => Effect.fromResult(parse(evaluate)));

/** A rejected caller input: `InvalidInput`, never submitted. */
export const invalidInput = (error: ReactorError, operation?: string): ReactorError =>
  ReactorError.fromCode("InvalidInput", error.message, {
    ...error.context,
    ...(operation === undefined ? {} : { operation }),
    outcome: "not-submitted",
  });

/** `parsed` for caller input: a rejection is `invalidInput`. */
export const parsedInput = <A>(
  evaluate: () => A,
  operation?: string,
): Effect.Effect<A, ReactorError> =>
  parsed(evaluate).pipe(Effect.mapError((error) => invalidInput(error, operation)));

/** A platform call's expected exception as a failure of `code`, the exception kept as detail. */
export const errorOf = (cause: unknown, code: MessageCode, operation?: string): ReactorError =>
  ReactorError.is(cause)
    ? cause
    : ReactorError.fromCode(code, operation === undefined ? code : `${operation} failed`, {
        ...(operation === undefined ? {} : { operation }),
        detail: cause,
      });

export const positiveLimit = (value: number, name: string, maximum = 0x7fffffff): number => {
  if (!Number.isSafeInteger(value) || value < 1 || value > maximum) {
    throw ReactorError.fromCode("InvalidInput", `${name} must be an integer in 1..${maximum}`, {
      outcome: "not-submitted",
    });
  }
  return value;
};
