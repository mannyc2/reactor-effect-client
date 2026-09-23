import { ReactorError } from "../errors.js";
import type { ErrorContext } from "../errors.js";

type Details = Omit<ErrorContext, "outcome" | "operation" | "requestId" | "generation">;

/** Dispatch evidence is independent of the failure's transport category. */
export type CommandContext = Details &
  (
    | {
        readonly outcome: "not-submitted";
        readonly operation: string;
        readonly requestId?: string;
        readonly generation?: bigint;
      }
    | {
        readonly outcome: "unknown" | "replied";
        readonly operation: string;
        readonly requestId: string;
        readonly generation: bigint;
      }
  );

/** Every failed command carries the dispatch evidence established by its owner. */
export class CommandFailure extends ReactorError {
  declare readonly context: CommandContext;

  // Built from a ReactorError and its dispatch evidence; never decoded through
  // the ReactorError schema.
  // @effect-diagnostics-next-line overriddenSchemaConstructor:off
  constructor(error: ReactorError, context: CommandContext) {
    super({
      code: error.code,
      message: error.message,
      context,
      ...(error.nativeError === undefined ? {} : { nativeError: error.nativeError }),
    });
  }
}
