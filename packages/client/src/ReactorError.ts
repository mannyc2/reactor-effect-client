/**
 * The client's failures. `ReactorError` carries a tagged `reason` (route with
 * `Effect.catchReason`) and a `context` of dispatch evidence; `CommandFailure`
 * is the same with the evidence a dispatched command must have, and
 * `AcquisitionFailure` adds the cleanup report of a partial lease.
 *
 * `message` is written by the library and never holds provider or payload
 * text. That text, native backend text and diagnostic detail live in
 * `Redacted` fields, so logs, spans and `toJSON` show `<redacted>` for them.
 * Persisted evidence stores a `FailureSummary`, never the error itself.
 */
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Predicate from "effect/Predicate";
import * as Redacted from "effect/Redacted";
import * as Schema from "effect/Schema";
import type { CloseReport } from "./Session.js";

const TypeId = "~reactor-effect-client/ReactorFailure" as const;

/** The codes whose reason carries nothing beyond a library-written message. */
export const FailureCode = Schema.Literals([
  "InvalidInput",
  "Protocol",
  "VersionMismatch",
  "UnsupportedHost",
  "UnsupportedCapability",
  "InvalidState",
  "TerminalSession",
  "Timeout",
  "Disconnected",
  "Aborted",
  "Overflow",
  "UnexpectedReply",
  "Upload",
  "Closed",
  "AlreadyReading",
  "Shutdown",
  "SdpRejected",
  "ChannelClosed",
  "Indeterminate",
  /** Reactor's content moderation ended the session. */
  "Moderated",
]);
export type FailureCode = typeof FailureCode.Type;

/** A reason whose `_tag` is its code. */
export class Failure extends Schema.Error<Failure>("reactor-effect-client/ReactorError/Failure")({
  _tag: FailureCode,
  message: Schema.String,
}) {}

/** A coordinator HTTP failure; `status` is absent when no response arrived. */
export class Http extends Schema.Error<Http>("reactor-effect-client/ReactorError/Http")({
  _tag: Schema.tag("Http"),
  message: Schema.String,
  status: Schema.optionalKey(Schema.Int),
  /** The response's `Retry-After`, when it named a delay. */
  retryAfter: Schema.optionalKey(Schema.Duration),
  /** The response body, for explicit inspection only. */
  body: Schema.String.pipe(Schema.Redacted, Schema.optionalKey),
}) {}

/**
 * The provider replied with an error or a refusal. `RecorderDisabled` is a
 * clip failure recognized from its reason, the one documented classification
 * of provider text.
 */
export class Remote extends Schema.Error<Remote>("reactor-effect-client/ReactorError/Remote")({
  _tag: Schema.Literals(["Remote", "RecorderDisabled"]),
  message: Schema.String,
  /** The provider's own error code, verbatim, for explicit inspection only. */
  remoteCode: Schema.String.pipe(Schema.Redacted, Schema.optionalKey),
  /** The provider's error text, for explicit inspection only. */
  body: Schema.String.pipe(Schema.Redacted, Schema.optionalKey),
}) {}

/** A failure of the native WebRTC addon or its host process. */
export class Native extends Schema.Error<Native>("reactor-effect-client/ReactorError/Native")({
  _tag: Schema.tag("Native"),
  message: Schema.String,
  /** libwebrtc's own text; it can contain peer SDP or signaling material. */
  backendMessage: Schema.optionalKey(Schema.Redacted(Schema.String, { disallowJsonEncode: true })),
}) {}

/** No ICE candidate pair worked. */
export class IceFailed extends Schema.Error<IceFailed>(
  "reactor-effect-client/ReactorError/IceFailed",
)({
  _tag: Schema.tag("IceFailed"),
  message: Schema.String,
  /** Candidate pairs the connection's statistics listed. */
  pairs: Schema.Int,
  /** The local candidate types gathered, such as `host` or `relay`. */
  candidateTypes: Schema.Array(Schema.String),
}) {}

/** ICE connectivity succeeded and the DTLS or SCTP transport above it failed. */
export class TransportFailed extends Schema.Error<TransportFailed>(
  "reactor-effect-client/ReactorError/TransportFailed",
)({
  _tag: Schema.tag("TransportFailed"),
  message: Schema.String,
  pairs: Schema.Int,
}) {}

/** A clip ended, by `clip_failed` or `clip_popped`, before it reached the phase awaited. */
export class ClipEnded extends Schema.Error<ClipEnded>(
  "reactor-effect-client/ReactorError/ClipEnded",
)({
  _tag: Schema.tag("ClipEnded"),
  message: Schema.String,
  clipId: Schema.String,
  lifecycle: Schema.Literals(["clip_failed", "clip_popped"]),
  /** The transport generation whose evidence ended it. */
  transportGeneration: Schema.BigInt,
}) {}

export const ReactorErrorReason = Schema.Union([
  Failure,
  Http,
  Remote,
  Native,
  IceFailed,
  TransportFailed,
  ClipEnded,
]);
export type ReactorErrorReason = typeof ReactorErrorReason.Type;
/** Every reason tag, as the native host's IPC carries it. */
export const ErrorCode = Schema.Literals([
  ...FailureCode.literals,
  "Http",
  "Remote",
  "RecorderDisabled",
  "Native",
  "IceFailed",
  "TransportFailed",
  "ClipEnded",
]);
export type ErrorCode = ReactorErrorReason["_tag"];
/** The codes a reason can be built from with a message alone. */
export type MessageCode = Exclude<ErrorCode, "IceFailed" | "TransportFailed" | "ClipEnded">;

/** Whether a remote mutation may have happened: never sent, maybe sent, or answered. */
export const RemoteOutcome = Schema.Literals(["not-submitted", "unknown", "replied"]);
export type RemoteOutcome = typeof RemoteOutcome.Type;

export const ErrorContext = Schema.Struct({
  operation: Schema.optionalKey(Schema.String),
  requestId: Schema.optionalKey(Schema.String),
  sessionId: Schema.optionalKey(Schema.String),
  generation: Schema.optionalKey(Schema.BigInt),
  outcome: Schema.optionalKey(RemoteOutcome),
  /** A cause or diagnostic record; it may hold provider or payload text. */
  detail: Schema.Unknown.pipe(Schema.Redacted, Schema.optionalKey),
});
export type ErrorContext = typeof ErrorContext.Type;

/** An `ErrorContext` whose `detail` is still plain; `fromCode` redacts it. */
export type ContextInput = Omit<ErrorContext, "detail"> & { readonly detail?: unknown };

/** Dispatch evidence proving that a request was never handed to the remote. */
export const NotSubmitted = Schema.Struct({
  ...ErrorContext.fields,
  outcome: Schema.Literal("not-submitted"),
  operation: Schema.String,
});
export type NotSubmitted = typeof NotSubmitted.Type;

/** A command's dispatch evidence: never sent, or sent with its request identity. */
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

const redact = <C extends { readonly detail?: unknown }>(
  context: C,
): Omit<C, "detail"> & { readonly detail?: Redacted.Redacted<unknown> } => {
  if (context.detail === undefined || Redacted.isRedacted(context.detail))
    return context as Omit<C, "detail"> & { readonly detail?: Redacted.Redacted<unknown> };
  return { ...context, detail: Redacted.make(context.detail) };
};

const reasonFor = (code: MessageCode, message: string): ReactorErrorReason => {
  switch (code) {
    case "Http":
      return Http.make({ message });
    case "Remote":
    case "RecorderDisabled":
      return Remote.make({ _tag: code, message });
    case "Native":
      return Native.make({ message });
    default:
      return Failure.make({ _tag: code, message });
  }
};

/**
 * Whether a later attempt may succeed: backpressure, a lost connection, or an
 * HTTP refusal for now. Never when the first attempt's outcome is unknown,
 * since the remote may already have applied it.
 */
const retryable = (reason: ReactorErrorReason, outcome: RemoteOutcome | undefined): boolean => {
  if (outcome === "unknown") return false;
  switch (reason._tag) {
    case "Overflow":
    case "Disconnected":
    case "ChannelClosed":
      return true;
    case "Http":
      // Reactor classes a server error as recoverable; a refusal of authority or a conflict is not.
      return (
        reason.status === undefined ||
        reason.status === 408 ||
        reason.status === 429 ||
        reason.status >= 500 ||
        reason.retryAfter !== undefined
      );
    default:
      return false;
  }
};

const retryAfterOf = (reason: ReactorErrorReason): Duration.Duration | undefined =>
  reason._tag === "Http" ? reason.retryAfter : undefined;

/**
 * A failure category never implies a mutation was rolled back: inspect
 * `context.outcome`, and route on `reason._tag`.
 */
export class ReactorError extends Schema.TaggedError<ReactorError>(
  "reactor-effect-client/ReactorError",
)("ReactorError", {
  reason: ReactorErrorReason,
  context: ErrorContext.pipe(Schema.withConstructorDefault(Effect.succeed({}))),
}) {
  readonly [TypeId] = TypeId;
  /** The reason is library-authored, so it is the native cause. */
  override readonly cause = this.reason;

  override get message(): string {
    return this.reason.message;
  }

  get isRetryable(): boolean {
    return retryable(this.reason, this.context.outcome);
  }

  get retryAfter(): Duration.Duration | undefined {
    return retryAfterOf(this.reason);
  }

  static is(u: unknown): u is ReactorError {
    return Predicate.hasProperty(u, TypeId) && Predicate.isTagged(u, "ReactorError");
  }

  /** A failure whose reason is `code` with a library-written `message`. */
  static fromCode(code: MessageCode, message: string, context: ContextInput = {}): ReactorError {
    return ReactorError.make({ reason: reasonFor(code, message), context: redact(context) });
  }
}

/** Every failed command carries the dispatch evidence its owner established. */
export class CommandFailure extends Schema.TaggedError<CommandFailure>(
  "reactor-effect-client/CommandFailure",
)("CommandFailure", {
  reason: ReactorErrorReason,
  context: CommandContext,
}) {
  readonly [TypeId] = TypeId;
  override readonly cause = this.reason;

  override get message(): string {
    return this.reason.message;
  }

  get isRetryable(): boolean {
    return retryable(this.reason, this.context.outcome);
  }

  get retryAfter(): Duration.Duration | undefined {
    return retryAfterOf(this.reason);
  }

  static is(u: unknown): u is CommandFailure {
    return Predicate.hasProperty(u, TypeId) && Predicate.isTagged(u, "CommandFailure");
  }

  /** `error`'s reason with the dispatch evidence its command established. */
  static from(
    error: ReactorError | CommandFailure | AcquisitionFailure,
    context: Omit<CommandContext, "detail"> & { readonly detail?: unknown },
  ): CommandFailure {
    return CommandFailure.make({
      reason: error.reason,
      context: redact(context) as CommandContext,
    });
  }
}

/**
 * A partial lease's cleanup report. `Session` owns its Schema and imports this
 * module at run time, so the report is declared here by type alone.
 */
const Cleanup = Schema.declare((u: unknown): u is CloseReport => Predicate.isObject(u), {
  expected: "CloseReport",
});

/** A failed acquisition still returns the lifetime evidence of its partial lease. */
export class AcquisitionFailure extends Schema.TaggedError<AcquisitionFailure>(
  "reactor-effect-client/AcquisitionFailure",
)("AcquisitionFailure", {
  reason: ReactorErrorReason,
  context: ErrorContext.pipe(Schema.withConstructorDefault(Effect.succeed({}))),
  cleanup: Cleanup,
}) {
  readonly [TypeId] = TypeId;
  override readonly cause = this.reason;

  override get message(): string {
    return this.reason.message;
  }

  get isRetryable(): boolean {
    return retryable(this.reason, this.context.outcome);
  }

  get retryAfter(): Duration.Duration | undefined {
    return retryAfterOf(this.reason);
  }

  static is(u: unknown): u is AcquisitionFailure {
    return Predicate.hasProperty(u, TypeId) && Predicate.isTagged(u, "AcquisitionFailure");
  }

  /** `error`'s reason and context, with the cleanup its partial lease reported. */
  static from(
    error: ReactorError | CommandFailure | AcquisitionFailure,
    cleanup: CloseReport,
  ): AcquisitionFailure {
    return AcquisitionFailure.make({ reason: error.reason, context: error.context, cleanup });
  }
}

/** Any failure this client raises. Each class keeps its own `_tag`. */
export type ReactorFailure = ReactorError | CommandFailure | AcquisitionFailure;

/**
 * Whether `u` is one of the client's failures. A passthrough re-raises a known
 * failure through this guard or a class's `is`, so the evidence a class carries
 * is kept instead of re-wrapped.
 */
export const isReactorFailure = (u: unknown): u is ReactorFailure =>
  Predicate.hasProperty(u, TypeId);

/** A failure as persisted evidence: its routing facts, without any redacted text. */
export const FailureSummary = Schema.Struct({
  _tag: Schema.String,
  reason: Schema.String,
  message: Schema.String,
  operation: Schema.optionalKey(Schema.String),
  requestId: Schema.optionalKey(Schema.String),
  sessionId: Schema.optionalKey(Schema.String),
  generation: Schema.optionalKey(Schema.String),
  outcome: Schema.optionalKey(RemoteOutcome),
});
export type FailureSummary = typeof FailureSummary.Type;

/** The summary of any failure with a tagged reason and a context, the client's or a policy's. */
export const summarize = (error: {
  readonly _tag: string;
  readonly message: string;
  readonly reason: { readonly _tag: string };
  readonly context: Omit<ErrorContext, "detail">;
}): FailureSummary => {
  const { operation, requestId, sessionId, generation, outcome } = error.context;
  return {
    _tag: error._tag,
    reason: error.reason._tag,
    message: error.message,
    ...(operation === undefined ? {} : { operation }),
    ...(requestId === undefined ? {} : { requestId }),
    ...(sessionId === undefined ? {} : { sessionId }),
    ...(generation === undefined ? {} : { generation: String(generation) }),
    ...(outcome === undefined ? {} : { outcome }),
  };
};
