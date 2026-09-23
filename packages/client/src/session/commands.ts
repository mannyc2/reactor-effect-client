import * as Schema from "effect/Schema";
import { ErrorContext, ReactorError } from "../errors.js";

/** Dispatch evidence is independent of the failure's transport category. */
export const CommandContext = Schema.Union([
  Schema.Struct({
    ...ErrorContext.fields,
    outcome: Schema.Literal("not-submitted"),
    operation: Schema.String,
  }),
  Schema.Struct({
    ...ErrorContext.fields,
    outcome: Schema.Literals(["unknown", "replied"]),
    operation: Schema.String,
    requestId: Schema.String,
    generation: Schema.BigInt,
  }),
]);
export type CommandContext = typeof CommandContext.Type;

/** Every failed command carries the dispatch evidence established by its owner. */
export class CommandFailure extends ReactorError.extend<CommandFailure>(
  "reactor-effect-client/CommandFailure",
)({ context: CommandContext }) {
  /** `error`'s failure, with the dispatch evidence its command established. */
  static from(error: ReactorError, context: CommandContext): CommandFailure {
    return new CommandFailure({
      code: error.code,
      message: error.message,
      context,
      ...(error.nativeError === undefined ? {} : { nativeError: error.nativeError }),
    });
  }
}
