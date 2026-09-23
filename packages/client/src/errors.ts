import * as Effect from "effect/Effect";
import * as Predicate from "effect/Predicate";
import * as Schema from "effect/Schema";
import type { CloseReport } from "./SessionTypes.js";

/**
 * The client's four failure classes share this marker so one guard recognizes
 * each of them, although each has its own `_tag` (the ClusterError precedent).
 */
const TypeId = "~reactor-effect-client/ReactorFailure" as const;

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
  /**
   * Provider text (an HTTP response body, a remote error's message, a refusal or
   * clip-failure reason), for explicit inspection only. `message` is written by
   * the library and never contains provider or payload text, so spans and logs
   * that record it stay payload-free. Diagnostic serialization excludes response
   * bodies and causes.
   */
  body: Schema.optionalKey(Schema.String),
  detail: Schema.optionalKey(Schema.Unknown),
});
export type ErrorContext = typeof ErrorContext.Type;

/** Dispatch evidence proving that a request was never handed to the remote. */
export const NotSubmitted = Schema.Struct({
  ...ErrorContext.fields,
  outcome: Schema.Literal("not-submitted"),
  operation: Schema.String,
});
export type NotSubmitted = typeof NotSubmitted.Type;

/** Dispatch evidence is independent of the failure's transport category. */
export const CommandContext = Schema.Union([
  NotSubmitted,
  Schema.Struct({
    ...ErrorContext.fields,
    outcome: Schema.Literals(["unknown", "replied"]),
    operation: Schema.String,
    requestId: Schema.String,
    generation: Schema.BigInt,
  }),
]);
export type CommandContext = typeof CommandContext.Type;

/** Diagnostic JSON: no response bodies and no causes. */
const diagnostic = (error: {
  readonly _tag: string;
  readonly code: ErrorCode;
  readonly message: string;
  readonly context: ErrorContext;
  readonly nativeError?: ProviderFailure;
}) => {
  const { body: _body, detail: _detail, generation, ...context } = error.context;
  return {
    _tag: error._tag,
    code: error.code,
    message: error.message,
    context: {
      ...context,
      ...(generation === undefined ? {} : { generation: String(generation) }),
    },
    ...(error.nativeError === undefined
      ? {}
      : {
          provider: {
            code: error.nativeError.code,
            operation: error.nativeError.operation,
            status: error.nativeError.status,
          },
        }),
  };
};

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
  readonly [TypeId] = TypeId;

  /** Whether `u` is a `ReactorError`, and not one of the other client failures. */
  static is(u: unknown): u is ReactorError {
    return Predicate.hasProperty(u, TypeId) && Predicate.isTagged(u, "ReactorError");
  }

  override toJSON() {
    return diagnostic(this);
  }
}

/** Every failed command carries the dispatch evidence established by its owner. */
export class CommandFailure extends Schema.TaggedError<CommandFailure>(
  "reactor-effect-client/CommandFailure",
)("CommandFailure", {
  code: ErrorCode,
  message: Schema.String,
  context: CommandContext,
  nativeError: Schema.optionalKey(ProviderFailure),
}) {
  readonly [TypeId] = TypeId;

  /** Whether `u` is a `CommandFailure`. */
  static is(u: unknown): u is CommandFailure {
    return Predicate.hasProperty(u, TypeId) && Predicate.isTagged(u, "CommandFailure");
  }

  /** `error`'s failure, with the dispatch evidence its command established. */
  static from(error: ReactorFailure, context: CommandContext): CommandFailure {
    return new CommandFailure({
      code: error.code,
      message: error.message,
      context,
      ...("nativeError" in error && error.nativeError !== undefined
        ? { nativeError: error.nativeError }
        : {}),
    });
  }

  override toJSON() {
    return diagnostic(this);
  }
}

/** The report a lease produced, kept by reference rather than copied. */
const Cleanup = Schema.declare(
  (input: unknown): input is CloseReport =>
    Predicate.isObject(input) && typeof input.localClosed === "boolean",
  { expected: "CloseReport" },
);

/** A failed acquisition still returns the lifetime evidence of its partial lease. */
export class AcquisitionFailure extends Schema.TaggedError<AcquisitionFailure>(
  "reactor-effect-client/AcquisitionFailure",
)("AcquisitionFailure", {
  code: ErrorCode,
  message: Schema.String,
  context: ErrorContext.pipe(Schema.withConstructorDefault(Effect.succeed({}))),
  nativeError: Schema.optionalKey(ProviderFailure),
  cleanup: Cleanup,
}) {
  readonly [TypeId] = TypeId;

  /** Whether `u` is an `AcquisitionFailure`. */
  static is(u: unknown): u is AcquisitionFailure {
    return Predicate.hasProperty(u, TypeId) && Predicate.isTagged(u, "AcquisitionFailure");
  }

  /** `error`'s failure, with the cleanup its partial lease reported. */
  static from(error: ReactorFailure, cleanup: CloseReport): AcquisitionFailure {
    return new AcquisitionFailure({
      code: error.code,
      message: error.message,
      context: error.context,
      ...("nativeError" in error && error.nativeError !== undefined
        ? { nativeError: error.nativeError }
        : {}),
      cleanup,
    });
  }

  override toJSON() {
    return diagnostic(this);
  }
}

/** Local admission is a policy decision, with explicit proof of no dispatch. */
export class PolicyFailure extends Schema.TaggedError<PolicyFailure>(
  "reactor-effect-client/PolicyFailure",
)("PolicyFailure", {
  code: ErrorCode,
  message: Schema.String,
  context: NotSubmitted,
  reason: Schema.String,
}) {
  readonly [TypeId] = TypeId;

  /** Whether `u` is a `PolicyFailure`. */
  static is(u: unknown): u is PolicyFailure {
    return Predicate.hasProperty(u, TypeId) && Predicate.isTagged(u, "PolicyFailure");
  }

  /** A local refusal of `operation`, which was therefore never dispatched. */
  static refuse(
    reason: string,
    message: string,
    operation = "enqueue",
    cause?: unknown,
  ): PolicyFailure {
    return new PolicyFailure({
      code: reason === "invalid_request" ? "InvalidInput" : "InvalidState",
      message,
      context: {
        operation,
        outcome: "not-submitted",
        ...(cause === undefined ? {} : { detail: cause }),
      },
      reason,
    });
  }

  override toJSON() {
    return { ...diagnostic(this), reason: this.reason };
  }
}

/** Any failure this client raises. Each class keeps its own `_tag`. */
export type ReactorFailure = ReactorError | CommandFailure | AcquisitionFailure | PolicyFailure;

/**
 * Whether `u` is one of the client's failures. A passthrough that re-raises a
 * known failure uses this guard, never `instanceof ReactorError`, so the
 * evidence a subclass carries (dispatch outcome, `AcquisitionFailure.cleanup`)
 * is kept instead of re-wrapped.
 */
export const isReactorFailure = (u: unknown): u is ReactorFailure =>
  Predicate.hasProperty(u, TypeId);

/** Keep the original cause for deliberate inspection without including it in the message. */
export const errorOf = (
  cause: unknown,
  code: ErrorCode = "Protocol",
  operation?: string,
): ReactorError =>
  ReactorError.is(cause)
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
