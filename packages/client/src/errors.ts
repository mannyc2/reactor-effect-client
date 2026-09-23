/**
 * The client's error algebra. Four wrapper classes, each with its own `_tag`,
 * carry a tagged `reason` (handled with `Effect.catchReason`, `catchReasons`
 * and `unwrapReason`) and a `context` of cross-cutting evidence: the operation
 * and its dispatch outcome.
 *
 * Privacy: `message` is written by the library and never contains provider or
 * payload text, so spans and logs that record it stay payload-free. Provider
 * text lives only in fields for explicit inspection: `body` on `Http` and
 * `Remote`, the Redacted `backendMessage` on `Native`, and `context.detail`.
 * Diagnostic JSON (`toJSON`) leaves all three out. Only a cause the library
 * authored becomes the native `cause`: each wrapper hoists its reason, and a
 * reason never holds a raw cause, because exporters such as OtlpTracer render
 * the whole cause chain. A response body, a wire message, native backend text
 * or a `JSON.parse` SyntaxError (whose message quotes its input) stays in
 * `detail` or a typed field.
 */
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Predicate from "effect/Predicate";
import * as Result from "effect/Result";
import * as Schema from "effect/Schema";
import * as SchemaIssue from "effect/SchemaIssue";
import * as SchemaTransformation from "effect/SchemaTransformation";
import { SequenceCode } from "./Sequence.js";
import type { CloseReport } from "./SessionTypes.js";

/**
 * The client's four failure classes share this marker so one guard recognizes
 * each of them, although each has its own `_tag` (the ClusterError precedent).
 */
const TypeId = "~reactor-effect-client/ReactorFailure" as const;

// ---------------------------------------------------------------------------
// ReactorError reasons
// ---------------------------------------------------------------------------

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
]);
export type FailureCode = typeof FailureCode.Type;

/** One reason class whose `_tag` is the code (the PlatformError.SystemError precedent). */
export class Failure extends Schema.Error<Failure>("reactor-effect-client/ReactorError/Failure")({
  _tag: FailureCode,
  message: Schema.String,
}) {
  /** Local backpressure (`Overflow`) and a lost connection may succeed on a later attempt. */
  get isRetryable(): boolean {
    return (
      this._tag === "Overflow" || this._tag === "Disconnected" || this._tag === "ChannelClosed"
    );
  }
}

/** A coordinator HTTP failure; `status` is absent when no response arrived. */
export class Http extends Schema.Error<Http>("reactor-effect-client/ReactorError/Http")({
  _tag: Schema.tag("Http"),
  message: Schema.String,
  status: Schema.optionalKey(Schema.Int),
  /** The response's `Retry-After`, when it named a delay. */
  retryAfter: Schema.optionalKey(Schema.Duration),
  /** The response body, for explicit inspection only. */
  body: Schema.optionalKey(Schema.String),
}) {
  /**
   * No response arrived, or the coordinator refused for now: 408, 429, 503, or
   * any status that named a Retry-After delay.
   */
  get isRetryable(): boolean {
    return (
      this.status === undefined ||
      this.status === 408 ||
      this.status === 429 ||
      this.status === 503 ||
      this.retryAfter !== undefined
    );
  }

  /** Diagnostic JSON: the body stays out. */
  override toJSON() {
    return reasonJson(this);
  }
}

/**
 * The provider replied with an error or a refusal. `RecorderDisabled` is a
 * clip failure recognized from its reason, the one documented classification
 * of provider text.
 */
export class Remote extends Schema.Error<Remote>("reactor-effect-client/ReactorError/Remote")({
  _tag: Schema.Literals(["Remote", "RecorderDisabled"]),
  message: Schema.String,
  /** The provider's own error code, verbatim. */
  remoteCode: Schema.optionalKey(Schema.String),
  /** The provider's error text, for explicit inspection only. */
  body: Schema.optionalKey(Schema.String),
}) {
  /** The provider refused; the same request would be refused again. */
  get isRetryable(): boolean {
    return false;
  }

  /** Diagnostic JSON: the body stays out. */
  override toJSON() {
    return reasonJson(this);
  }
}

/** A native bridge failure; `status` is the ABI's failure class when one was returned. */
export class Native extends Schema.Error<Native>("reactor-effect-client/ReactorError/Native")({
  _tag: Schema.tag("Native"),
  message: Schema.String,
  status: Schema.optionalKey(Schema.Int),
  /**
   * libwebrtc's own text. It can contain peer SDP or caller-supplied signaling
   * material, so it is Redacted and refuses JSON encoding.
   */
  backendMessage: Schema.optionalKey(Schema.Redacted(Schema.String, { disallowJsonEncode: true })),
}) {
  get isRetryable(): boolean {
    return false;
  }

  /** Diagnostic JSON: the backend text stays out. */
  override toJSON() {
    return reasonJson(this);
  }
}

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
}) {
  get isRetryable(): boolean {
    return false;
  }
}

/** ICE connectivity succeeded and the DTLS or SCTP transport above it failed. */
export class TransportFailed extends Schema.Error<TransportFailed>(
  "reactor-effect-client/ReactorError/TransportFailed",
)({
  _tag: Schema.tag("TransportFailed"),
  message: Schema.String,
  /** Candidate pairs the connection's statistics listed. */
  pairs: Schema.Int,
}) {
  get isRetryable(): boolean {
    return false;
  }
}

/**
 * A clip ended before it reached the phase awaited: it failed, or it was
 * popped from the queue, first.
 */
export class ClipEnded extends Schema.Error<ClipEnded>(
  "reactor-effect-client/ReactorError/ClipEnded",
)({
  _tag: Schema.tag("ClipEnded"),
  message: Schema.String,
  clipId: Schema.String,
  /** The lifecycle message that ended the clip. */
  lifecycle: Schema.Literals(["clip_failed", "clip_popped"]),
  /** The transport generation whose evidence ended it. */
  transportGeneration: Schema.BigInt,
}) {
  get isRetryable(): boolean {
    return false;
  }
}

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

/** Every reason tag a ReactorError, CommandFailure or AcquisitionFailure can carry. */
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

const reasonFor = (code: MessageCode, message: string): ReactorErrorReason => {
  switch (code) {
    case "Http":
      return new Http({ message });
    case "Remote":
    case "RecorderDisabled":
      return new Remote({ _tag: code, message });
    case "Native":
      return new Native({ message });
    default:
      return new Failure({ _tag: code, message });
  }
};

// ---------------------------------------------------------------------------
// Cross-cutting evidence
// ---------------------------------------------------------------------------

export type RemoteOutcome = "not-submitted" | "unknown" | "replied";

export const ErrorContext = Schema.Struct({
  operation: Schema.optionalKey(Schema.String),
  requestId: Schema.optionalKey(Schema.String),
  sessionId: Schema.optionalKey(Schema.String),
  generation: Schema.optionalKey(Schema.BigInt),
  outcome: Schema.optionalKey(Schema.Literals(["not-submitted", "unknown", "replied"])),
  /**
   * The underlying cause or diagnostic record, for explicit inspection only. It
   * may hold provider or payload text, so it is never the native `cause` and
   * diagnostic JSON leaves it out.
   */
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

// ---------------------------------------------------------------------------
// PolicyFailure reasons
// ---------------------------------------------------------------------------

/** Local admission refusals that carry only a message. */
export const RefusalCode = Schema.Literals([
  "QueueFull",
  "SessionRecovering",
  "SessionClosed",
  "SessionRetired",
  "NotFound",
  "OwnerConflict",
  "Busy",
  "InvalidRequest",
  "PositionConflict",
  "QueueChanged",
  "RouteChanged",
  "AnchorUnavailable",
  "ContinuationUnavailable",
  "AnnotationCapacity",
  "SubmissionCapacity",
  "IdentityExhausted",
]);
export type RefusalCode = typeof RefusalCode.Type;

/** A local refusal whose `_tag` is its code. */
export class Refusal extends Schema.Error<Refusal>("reactor-effect-client/PolicyFailure/Refusal")({
  _tag: RefusalCode,
  message: Schema.String,
}) {
  /** A full queue and a recovering session are backpressure: the request can wait. */
  get isRetryable(): boolean {
    return this._tag === "QueueFull" || this._tag === "SessionRecovering";
  }
}

/** A request field names a clip that no session is known to own. */
export class Missing extends Schema.Error<Missing>("reactor-effect-client/PolicyFailure/Missing")({
  _tag: Schema.tag("Missing"),
  /** The field: `before` (anchor), `sameSessionAs` (session_anchor) or `continueFrom`. */
  purpose: Schema.Literals(["anchor", "session_anchor", "continuation"]),
}) {
  override get message(): string {
    return `The ${this.purpose} clip has no known owning session`;
  }

  get isRetryable(): boolean {
    return false;
  }
}

/** Sequence affinity refused the request; `code` is the SequenceError code. */
export class SequenceRefusal extends Schema.Error<SequenceRefusal>(
  "reactor-effect-client/PolicyFailure/Sequence",
)({
  _tag: Schema.tag("Sequence"),
  sequenceId: Schema.String,
  code: SequenceCode,
}) {
  override get message(): string {
    return `Sequence ${this.sequenceId}: ${this.code}`;
  }

  get isRetryable(): boolean {
    return false;
  }
}

export const PolicyReason = Schema.Union([Refusal, Missing, SequenceRefusal]);
export type PolicyReason = typeof PolicyReason.Type;

// ---------------------------------------------------------------------------
// Diagnostic JSON
// ---------------------------------------------------------------------------

/** A reason's routing fields, without provider text. */
const reasonJson = (reason: ReactorErrorReason | PolicyReason) => {
  switch (reason._tag) {
    case "Http":
      return {
        _tag: reason._tag,
        message: reason.message,
        ...(reason.status === undefined ? {} : { status: reason.status }),
        ...(reason.retryAfter === undefined
          ? {}
          : { retryAfterMillis: Duration.toMillis(reason.retryAfter) }),
      };
    case "Remote":
    case "RecorderDisabled":
      return {
        _tag: reason._tag,
        message: reason.message,
        ...(reason.remoteCode === undefined ? {} : { remoteCode: reason.remoteCode }),
      };
    case "Native":
      return {
        _tag: reason._tag,
        message: reason.message,
        ...(reason.status === undefined ? {} : { status: reason.status }),
      };
    case "IceFailed":
      return {
        _tag: reason._tag,
        message: reason.message,
        pairs: reason.pairs,
        candidateTypes: reason.candidateTypes,
      };
    case "TransportFailed":
      return { _tag: reason._tag, message: reason.message, pairs: reason.pairs };
    case "ClipEnded":
      return {
        _tag: reason._tag,
        message: reason.message,
        clipId: reason.clipId,
        lifecycle: reason.lifecycle,
        transportGeneration: String(reason.transportGeneration),
      };
    case "Missing":
      return { _tag: reason._tag, message: reason.message, purpose: reason.purpose };
    case "Sequence":
      return {
        _tag: reason._tag,
        message: reason.message,
        sequenceId: reason.sequenceId,
        code: reason.code,
      };
    default:
      return { _tag: reason._tag, message: reason.message };
  }
};

const contextJson = ({ detail: _detail, generation, ...context }: ErrorContext) => ({
  ...context,
  ...(generation === undefined ? {} : { generation: String(generation) }),
});

const diagnostic = (error: {
  readonly _tag: string;
  readonly message: string;
  readonly reason: ReactorErrorReason | PolicyReason;
  readonly context: ErrorContext;
}) => ({
  _tag: error._tag,
  message: error.message,
  reason: reasonJson(error.reason),
  context: contextJson(error.context),
});

// ---------------------------------------------------------------------------
// Wrappers
// ---------------------------------------------------------------------------

/**
 * A failure category never implies a mutation was rolled back. Inspect
 * `context.outcome`, and route on `reason._tag`.
 *
 * Construct it from its fields, as with any Schema class (`context` defaults
 * to empty), or with `ReactorError.fromCode`.
 */
export class ReactorError extends Schema.TaggedError<ReactorError>(
  "reactor-effect-client/ReactorError",
)("ReactorError", {
  reason: ReactorErrorReason,
  context: ErrorContext.pipe(Schema.withConstructorDefault(Effect.succeed({}))),
}) {
  readonly [TypeId] = TypeId;
  /** The reason is library-authored, so it is the native cause (the AiError precedent). */
  override readonly cause = this.reason;

  override get message(): string {
    return this.reason.message;
  }

  /**
   * Whether a later attempt may succeed. Never true when `context.outcome` is
   * "unknown": the remote may already have applied the first attempt.
   */
  get isRetryable(): boolean {
    return this.context.outcome !== "unknown" && this.reason.isRetryable;
  }

  /** The delay the remote asked for, when it named one (the AiError precedent). */
  get retryAfter(): Duration.Duration | undefined {
    return this.reason._tag === "Http" ? this.reason.retryAfter : undefined;
  }

  /** Whether `u` is a `ReactorError`, and not one of the other client failures. */
  static is(u: unknown): u is ReactorError {
    return Predicate.hasProperty(u, TypeId) && Predicate.isTagged(u, "ReactorError");
  }

  /** A failure whose reason is `code` with a library-written `message`. */
  static fromCode(code: MessageCode, message: string, context: ErrorContext = {}): ReactorError {
    return new ReactorError({ reason: reasonFor(code, message), context });
  }

  override toJSON() {
    return diagnostic(this);
  }
}

/** Every failed command carries the dispatch evidence established by its owner. */
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

  /**
   * Whether a later attempt may succeed. Never true when `context.outcome` is
   * "unknown": the remote may already have applied the first attempt.
   */
  get isRetryable(): boolean {
    return this.context.outcome !== "unknown" && this.reason.isRetryable;
  }

  /** The delay the remote asked for, when it named one (the AiError precedent). */
  get retryAfter(): Duration.Duration | undefined {
    return this.reason._tag === "Http" ? this.reason.retryAfter : undefined;
  }

  /** Whether `u` is a `CommandFailure`. */
  static is(u: unknown): u is CommandFailure {
    return Predicate.hasProperty(u, TypeId) && Predicate.isTagged(u, "CommandFailure");
  }

  /** `error`'s reason, with the dispatch evidence its command established. */
  static from(error: ReactorFailure, context: CommandContext): CommandFailure {
    return new CommandFailure({ reason: wrapperReason(error), context });
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
  reason: ReactorErrorReason,
  context: ErrorContext.pipe(Schema.withConstructorDefault(Effect.succeed({}))),
  cleanup: Cleanup,
}) {
  readonly [TypeId] = TypeId;
  override readonly cause = this.reason;

  override get message(): string {
    return this.reason.message;
  }

  /**
   * Whether a later attempt may succeed. Never true when `context.outcome` is
   * "unknown": the remote may already have applied the first attempt.
   */
  get isRetryable(): boolean {
    return this.context.outcome !== "unknown" && this.reason.isRetryable;
  }

  /** The delay the remote asked for, when it named one (the AiError precedent). */
  get retryAfter(): Duration.Duration | undefined {
    return this.reason._tag === "Http" ? this.reason.retryAfter : undefined;
  }

  /** Whether `u` is an `AcquisitionFailure`. */
  static is(u: unknown): u is AcquisitionFailure {
    return Predicate.hasProperty(u, TypeId) && Predicate.isTagged(u, "AcquisitionFailure");
  }

  /** `error`'s reason and context, with the cleanup its partial lease reported. */
  static from(error: ReactorFailure, cleanup: CloseReport): AcquisitionFailure {
    return new AcquisitionFailure({
      reason: wrapperReason(error),
      context: error.context,
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
  reason: PolicyReason,
  context: NotSubmitted,
}) {
  readonly [TypeId] = TypeId;
  override readonly cause = this.reason;

  override get message(): string {
    return this.reason.message;
  }

  /**
   * Whether a later attempt may succeed: a full queue or a recovering session.
   * Nothing was dispatched, so the outcome never stands in the way.
   */
  get isRetryable(): boolean {
    return this.reason.isRetryable;
  }

  /** A local refusal names no delay. */
  get retryAfter(): Duration.Duration | undefined {
    return undefined;
  }

  /** Whether `u` is a `PolicyFailure`. */
  static is(u: unknown): u is PolicyFailure {
    return Predicate.hasProperty(u, TypeId) && Predicate.isTagged(u, "PolicyFailure");
  }

  /** A local refusal of `operation`, which was therefore never dispatched. */
  static refuse(
    code: RefusalCode,
    message: string,
    operation = "enqueue",
    cause?: unknown,
  ): PolicyFailure {
    return new PolicyFailure({
      reason: new Refusal({ _tag: code, message }),
      context: {
        operation,
        outcome: "not-submitted",
        ...(cause === undefined ? {} : { detail: cause }),
      },
    });
  }

  /** A refusal because the clip a request field names has no known owner. */
  static missing(purpose: Missing["purpose"], operation = "enqueue"): PolicyFailure {
    return new PolicyFailure({
      reason: new Missing({ purpose }),
      context: { operation, outcome: "not-submitted" },
    });
  }

  /** A refusal by sequence affinity, with its SequenceError code. */
  static sequence(sequenceId: string, code: SequenceCode, operation = "enqueue"): PolicyFailure {
    return new PolicyFailure({
      reason: new SequenceRefusal({ sequenceId, code }),
      context: { operation, outcome: "not-submitted" },
    });
  }

  override toJSON() {
    return diagnostic(this);
  }
}

// ---------------------------------------------------------------------------
// Evidence codecs
// ---------------------------------------------------------------------------

/** A bigint as diagnostic JSON writes it. */
const Digits = Schema.String.check(Schema.isPattern(/^(?:0|[1-9][0-9]*)$/));

/** A reason's diagnostic JSON, as `toJSON` writes it. */
const ReactorReasonJson = Schema.Union([
  Schema.Struct({ _tag: FailureCode, message: Schema.String }),
  Schema.TaggedStruct("Http", {
    message: Schema.String,
    status: Schema.optionalKey(Schema.Int),
    retryAfterMillis: Schema.optionalKey(Schema.Finite.check(Schema.isGreaterThanOrEqualTo(0))),
  }),
  Schema.Struct({
    _tag: Remote.fields._tag,
    message: Schema.String,
    remoteCode: Schema.optionalKey(Schema.String),
  }),
  Schema.TaggedStruct("Native", {
    message: Schema.String,
    status: Schema.optionalKey(Schema.Int),
  }),
  Schema.TaggedStruct("IceFailed", {
    message: Schema.String,
    pairs: IceFailed.fields.pairs,
    candidateTypes: IceFailed.fields.candidateTypes,
  }),
  Schema.TaggedStruct("TransportFailed", {
    message: Schema.String,
    pairs: TransportFailed.fields.pairs,
  }),
  Schema.TaggedStruct("ClipEnded", {
    message: Schema.String,
    clipId: ClipEnded.fields.clipId,
    lifecycle: ClipEnded.fields.lifecycle,
    transportGeneration: Digits,
  }),
]);

const PolicyReasonJson = Schema.Union([
  Schema.Struct({ _tag: RefusalCode, message: Schema.String }),
  Schema.TaggedStruct("Missing", {
    message: Schema.String,
    purpose: Missing.fields.purpose,
  }),
  Schema.TaggedStruct("Sequence", {
    message: Schema.String,
    sequenceId: SequenceRefusal.fields.sequenceId,
    code: SequenceRefusal.fields.code,
  }),
]);

const ContextJson = Schema.Struct({
  operation: Schema.optionalKey(Schema.String),
  requestId: Schema.optionalKey(Schema.String),
  sessionId: Schema.optionalKey(Schema.String),
  generation: Schema.optionalKey(Digits),
  outcome: ErrorContext.fields.outcome,
});

const reactorReason = (json: typeof ReactorReasonJson.Type): ReactorErrorReason => {
  switch (json._tag) {
    case "Http":
      return new Http({
        message: json.message,
        ...(json.status === undefined ? {} : { status: json.status }),
        ...(json.retryAfterMillis === undefined
          ? {}
          : { retryAfter: Duration.millis(json.retryAfterMillis) }),
      });
    case "Remote":
    case "RecorderDisabled":
      return new Remote({
        _tag: json._tag,
        message: json.message,
        ...(json.remoteCode === undefined ? {} : { remoteCode: json.remoteCode }),
      });
    case "Native":
      return new Native({
        message: json.message,
        ...(json.status === undefined ? {} : { status: json.status }),
      });
    case "IceFailed":
      return new IceFailed({
        message: json.message,
        pairs: json.pairs,
        candidateTypes: json.candidateTypes,
      });
    case "TransportFailed":
      return new TransportFailed({ message: json.message, pairs: json.pairs });
    case "ClipEnded":
      return new ClipEnded({
        message: json.message,
        clipId: json.clipId,
        lifecycle: json.lifecycle,
        transportGeneration: BigInt(json.transportGeneration),
      });
    default:
      return new Failure({ _tag: json._tag, message: json.message });
  }
};

const policyReason = (json: typeof PolicyReasonJson.Type): PolicyReason => {
  switch (json._tag) {
    case "Missing":
      return new Missing({ purpose: json.purpose });
    case "Sequence":
      return new SequenceRefusal({ sequenceId: json.sequenceId, code: json.code });
    default:
      return new Refusal({ _tag: json._tag, message: json.message });
  }
};

const context = ({ generation, ...rest }: typeof ContextJson.Type) => ({
  ...rest,
  ...(generation === undefined ? {} : { generation: BigInt(generation) }),
});

/**
 * A failure class encoded as its diagnostic JSON. Encoding is `toJSON`, so
 * provider text (`body`), native backend text and `context.detail` never
 * reach the encoded form, and a decoded failure is without them.
 */
const fromJson = <A extends { toJSON(): unknown }, J>(
  json: Schema.Codec<J, unknown>,
  target: abstract new (...args: never) => A,
  build: (json: J) => A,
) =>
  json.pipe(
    Schema.decodeTo(
      Schema.declare((u): u is A => u instanceof target, { expected: target.name }),
      SchemaTransformation.transformEffect({
        decode: (input: J, options) =>
          Effect.try({
            try: () => build(input),
            catch: () =>
              new SchemaIssue.InvalidValue(
                { message: `not a valid ${target.name} context` },
                input,
                options,
              ),
          }),
        encode: (error: A) => Effect.succeed(error.toJSON() as J),
      }),
    ),
  );

/** A `ReactorError` as its diagnostic JSON. */
export const ReactorErrorFromJson = fromJson(
  Schema.TaggedStruct("ReactorError", {
    message: Schema.String,
    reason: ReactorReasonJson,
    context: ContextJson,
  }),
  ReactorError,
  (json) =>
    new ReactorError({ reason: reactorReason(json.reason), context: context(json.context) }),
);

/** A `CommandFailure` as its diagnostic JSON. */
export const CommandFailureFromJson = fromJson(
  Schema.TaggedStruct("CommandFailure", {
    message: Schema.String,
    reason: ReactorReasonJson,
    context: ContextJson,
  }),
  CommandFailure,
  (json) =>
    new CommandFailure({
      reason: reactorReason(json.reason),
      context: context(json.context) as CommandContext,
    }),
);

/** A `PolicyFailure` as its diagnostic JSON. */
export const PolicyFailureFromJson = fromJson(
  Schema.TaggedStruct("PolicyFailure", {
    message: Schema.String,
    reason: PolicyReasonJson,
    context: ContextJson,
  }),
  PolicyFailure,
  (json) =>
    new PolicyFailure({
      reason: policyReason(json.reason),
      context: context(json.context) as NotSubmitted,
    }),
);

// ---------------------------------------------------------------------------
// Guards and helpers
// ---------------------------------------------------------------------------

/** Any failure this client raises. Each class keeps its own `_tag`. */
export type ReactorFailure = ReactorError | CommandFailure | AcquisitionFailure | PolicyFailure;

/**
 * Whether `u` is one of the client's failures. A passthrough that re-raises a
 * known failure uses this guard or a class's `is`, never `instanceof
 * ReactorError`, so the evidence a class carries (the dispatch outcome,
 * `AcquisitionFailure.cleanup`) is kept instead of re-wrapped.
 */
export const isReactorFailure = (u: unknown): u is ReactorFailure =>
  Predicate.hasProperty(u, TypeId);

/** A failure's reason as a ReactorError reason: a local refusal is invalid input or state. */
const wrapperReason = (error: ReactorFailure): ReactorErrorReason =>
  PolicyFailure.is(error)
    ? new Failure({
        _tag: error.reason._tag === "InvalidRequest" ? "InvalidInput" : "InvalidState",
        message: error.message,
      })
    : error.reason;

/**
 * Runs a synchronous parser or check that rejects by throwing a ReactorError,
 * and returns that rejection as the Result's failure. Anything else it throws
 * is a bug and propagates, so it stays a defect rather than becoming a typed
 * failure.
 */
export const parse = <A>(evaluate: () => A): Result.Result<A, ReactorError> => {
  try {
    return Result.succeed(evaluate());
  } catch (cause) {
    if (ReactorError.is(cause)) return Result.fail(cause);
    throw cause;
  }
};

/** `parse`, lifted with `Effect.fromResult`: a rejection fails it and a bug is a defect. */
export const parsed = <A>(evaluate: () => A): Effect.Effect<A, ReactorError> =>
  Effect.suspend(() => Effect.fromResult(parse(evaluate)));

/**
 * A rejected caller input: `InvalidInput`, never submitted, whatever category
 * the parser that rejected it uses for remote data.
 */
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

/**
 * A platform call's expected exception (a DOMException, a rejected browser
 * promise, a native library call), as a failure of the category `code`; the
 * exception stays in the inspection-only `detail`. Parsers and checks use
 * `parse` instead, so a bug in them stays a defect.
 */
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
