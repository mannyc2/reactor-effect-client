/**
 * A deadline a caller names in an option, in any `Duration.Input` form,
 * decoded once where it comes in. `Duration` reads a NaN as zero and keeps a
 * negative or infinite number as it is, so every numeric form is checked
 * finite before it becomes a duration, and the duration must be finite and
 * not negative.
 */
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as SchemaGetter from "effect/SchemaGetter";
import * as SchemaIssue from "effect/SchemaIssue";
import * as SchemaTransformation from "effect/SchemaTransformation";
import { ReactorError } from "../ReactorError.js";

/** A tuple or object form, which `Duration` reads without a Schema of its own. */
const parts = <S extends Schema.Codec<Duration.Input, Duration.Input>>(form: S) =>
  form.pipe(
    Schema.decodeTo(Schema.Duration, {
      decode: SchemaGetter.transformEffect((input: Duration.Input) =>
        Effect.fromOption(Duration.fromInput(input)).pipe(
          Effect.mapError(() => new SchemaIssue.InvalidValue({ message: "not a duration" }, input)),
        ),
      ),
      encode: SchemaGetter.forbidden(() => "a deadline is only decoded"),
    }),
  );

const Finite = Schema.optional(Schema.Finite);

export const Deadline = Schema.Union([
  Schema.Duration,
  Schema.DurationFromString,
  Schema.DurationFromNanos,
  Schema.Finite.pipe(Schema.decodeTo(Schema.Duration, SchemaTransformation.durationFromMillis)),
  parts(Schema.Tuple([Schema.Finite, Schema.Finite])),
  parts(
    Schema.Struct({
      weeks: Finite,
      days: Finite,
      hours: Finite,
      minutes: Finite,
      seconds: Finite,
      milliseconds: Finite,
      microseconds: Finite,
      nanoseconds: Finite,
    }),
  ),
]).check(
  Schema.makeFilter(
    (duration) =>
      (Duration.isFinite(duration) && !Duration.isNegative(duration)) ||
      "a finite duration that is not negative",
  ),
);

/** A decoder of the option `name`: anything but a finite, non-negative duration is `InvalidInput`. */
export const decode =
  (name: string) =>
  (input: Duration.Input): Effect.Effect<Duration.Duration, ReactorError> =>
    Schema.decodeEffect(Deadline)(input).pipe(
      Effect.mapError((cause) =>
        ReactorError.fromCode("InvalidInput", `${name} is not a finite, non-negative duration`, {
          outcome: "not-submitted",
          detail: cause,
        }),
      ),
    );
