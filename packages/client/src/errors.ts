import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";

export const ErrorCode = Schema.Literals([
  "InvalidInput",
  "Protocol",
  "Http",
  "VersionMismatch",
  "UnsupportedHost",
  "UnsupportedCapability",
  "InvalidState",
  "TerminalSession",
  "Timeout",
  "Disconnected",
  "Aborted",
  "Overflow",
  "Remote",
  "UnexpectedReply",
  "Upload",
  "RecorderDisabled",
  "Native",
  "Closed",
  "AlreadyReading",
  "Shutdown",
  "SdpRejected",
  "IceFailed",
  "TransportFailed",
  "ChannelClosed",
]);
export type ErrorCode = typeof ErrorCode.Type;
export type RemoteOutcome = "not-submitted" | "unknown" | "replied";

export const ProviderFailure = Schema.Struct({
  code: Schema.String,
  message: Schema.String,
  recoverable: Schema.Boolean,
  operation: Schema.optionalKey(Schema.String),
  status: Schema.optionalKey(Schema.Finite),
  retryAfterMs: Schema.optionalKey(Schema.Finite),
});
export type ProviderFailure = typeof ProviderFailure.Type;

export const ErrorContext = Schema.Struct({
  operation: Schema.optionalKey(Schema.String),
  requestId: Schema.optionalKey(Schema.String),
  sessionId: Schema.optionalKey(Schema.String),
  generation: Schema.optionalKey(Schema.BigInt),
  outcome: Schema.optionalKey(Schema.Literals(["not-submitted", "unknown", "replied"])),
  status: Schema.optionalKey(Schema.Finite),
  retryAfterMs: Schema.optionalKey(Schema.Finite),
  remoteCode: Schema.optionalKey(Schema.String),
  /** Explicit inspection only. Diagnostic serialization excludes response bodies and causes. */
  body: Schema.optionalKey(Schema.String),
  detail: Schema.optionalKey(Schema.Unknown),
});
export type ErrorContext = typeof ErrorContext.Type;

/**
 * A failure category never implies a mutation was rolled back. Inspect context.outcome.
 *
 * Construct it from its fields, as with any Schema class; `context` defaults to empty.
 */
export class ReactorError extends Schema.TaggedError<ReactorError>(
  "reactor-effect-client/ReactorError",
)("ReactorError", {
  code: ErrorCode,
  message: Schema.String,
  context: ErrorContext.pipe(Schema.withConstructorDefault(Effect.succeed({}))),
  nativeError: Schema.optionalKey(ProviderFailure),
}) {
  override toJSON() {
    const { body: _body, detail: _detail, generation, ...context } = this.context;
    return {
      _tag: this._tag,
      code: this.code,
      message: this.message,
      context: {
        ...context,
        ...(generation === undefined ? {} : { generation: String(generation) }),
      },
      ...(this.nativeError === undefined
        ? {}
        : {
            provider: {
              code: this.nativeError.code,
              operation: this.nativeError.operation,
              status: this.nativeError.status,
            },
          }),
    };
  }
}

/** Keep the original cause for deliberate inspection without including it in the message. */
export const errorOf = (
  cause: unknown,
  code: ErrorCode = "Protocol",
  operation?: string,
): ReactorError =>
  cause instanceof ReactorError
    ? cause
    : new ReactorError({
        code,
        message: operation === undefined ? code : `${operation} failed`,
        context: { ...(operation === undefined ? {} : { operation }), detail: cause },
      });

export const positiveLimit = (value: number, name: string, maximum = 0x7fffffff): number => {
  if (!Number.isSafeInteger(value) || value < 1 || value > maximum) {
    throw new ReactorError({
      code: "InvalidInput",
      message: `${name} must be an integer in 1..${maximum}`,
      context: { outcome: "not-submitted" },
    });
  }
  return value;
};
