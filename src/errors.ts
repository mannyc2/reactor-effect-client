import * as Schema from "effect/Schema";

export const ErrorCode = Schema.Literals([
  "InvalidInput", "Protocol", "Http", "VersionMismatch", "UnsupportedHost",
  "UnsupportedCapability", "InvalidState", "TerminalSession", "Timeout",
  "Disconnected", "Aborted", "Overflow", "Remote", "UnexpectedReply", "Upload",
  "RecorderDisabled", "Native", "Closed", "AlreadyReading", "Shutdown",
]);
export type ErrorCode = typeof ErrorCode.Type;
export type RemoteOutcome = "not-submitted" | "unknown" | "replied";

export const ProviderFailure = Schema.Struct({
  code: Schema.String,
  message: Schema.String,
  recoverable: Schema.Boolean,
  operation: Schema.optionalKey(Schema.String),
  status: Schema.optionalKey(Schema.Number),
  retryAfterMs: Schema.optionalKey(Schema.Number),
});
export type ProviderFailure = typeof ProviderFailure.Type;

export const ErrorContext = Schema.Struct({
  operation: Schema.optionalKey(Schema.String),
  requestId: Schema.optionalKey(Schema.String),
  sessionId: Schema.optionalKey(Schema.String),
  generation: Schema.optionalKey(Schema.BigInt),
  outcome: Schema.optionalKey(Schema.Literals(["not-submitted", "unknown", "replied"])),
  status: Schema.optionalKey(Schema.Number),
  retryAfterMs: Schema.optionalKey(Schema.Number),
  remoteCode: Schema.optionalKey(Schema.String),
  /** Explicit inspection only. Diagnostic serialization excludes response bodies and causes. */
  body: Schema.optionalKey(Schema.String),
  detail: Schema.optionalKey(Schema.Unknown),
});
export type ErrorContext = typeof ErrorContext.Type;

interface Fields {
  readonly code: ErrorCode;
  readonly message: string;
  readonly context?: ErrorContext;
  readonly nativeError?: ProviderFailure;
}

/** A failure category never implies a mutation was rolled back. Inspect context.outcome. */
export class ReactorError extends Schema.TaggedError<ReactorError>(
  "reactor-effect-client/ReactorError",
)("ReactorError", {
  code: ErrorCode,
  message: Schema.String,
  context: ErrorContext,
  nativeError: Schema.optionalKey(ProviderFailure),
}) {
  constructor(fields: Fields);
  constructor(code: ErrorCode, message: string, context?: ErrorContext);
  constructor(code: ErrorCode | Fields, message?: string, context: ErrorContext = {}) {
    const fields = typeof code === "string" ? { code, message: message ?? code, context } : code;
    super({ ...fields, context: fields.context ?? {} });
  }

  override toJSON() {
    const { body: _body, detail: _detail, generation, ...context } = this.context;
    return {
      _tag: this._tag,
      code: this.code,
      message: this.message,
      context: { ...context, ...(generation === undefined ? {} : { generation: String(generation) }) },
      ...(this.nativeError === undefined ? {} : {
        provider: { code: this.nativeError.code, operation: this.nativeError.operation, status: this.nativeError.status },
      }),
    };
  }
}

/** Keep the original cause for deliberate inspection without including it in the message. */
export const errorOf = (cause: unknown, code: ErrorCode = "Protocol", operation?: string): ReactorError =>
  cause instanceof ReactorError ? cause : new ReactorError(code, operation === undefined ? code : `${operation} failed`, {
    ...(operation === undefined ? {} : { operation }), detail: cause,
  });

export const positiveLimit = (value: number, name: string, maximum = 0x7fffffff): number => {
  if (!Number.isSafeInteger(value) || value < 1 || value > maximum) {
    throw new ReactorError("InvalidInput", `${name} must be an integer in 1..${maximum}`, { outcome: "not-submitted" });
  }
  return value;
};
