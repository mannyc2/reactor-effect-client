/**
 * Local admission refusals: orchestration declined a request before
 * dispatching anything, so every `PolicyFailure` proves `not-submitted`.
 */
import type * as Duration from "effect/Duration";
import * as Predicate from "effect/Predicate";
import * as Redacted from "effect/Redacted";
import * as Schema from "effect/Schema";
import { Failure, isReactorFailure, NotSubmitted, ReactorError } from "../ReactorError.js";
import type { ReactorFailure } from "../ReactorError.js";
import { SequenceCode } from "../Sequence.js";

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

/** Local admission is a policy decision, with explicit proof of no dispatch. */
export class PolicyFailure extends Schema.TaggedError<PolicyFailure>(
  "reactor-effect-client/PolicyFailure",
)("PolicyFailure", {
  reason: PolicyReason,
  context: NotSubmitted,
}) {
  override readonly cause = this.reason;

  override get message(): string {
    return this.reason.message;
  }

  /** A full queue or a recovering session; nothing was dispatched, so the outcome never stands in the way. */
  get isRetryable(): boolean {
    return this.reason.isRetryable;
  }

  /** A local refusal names no delay. */
  get retryAfter(): Duration.Duration | undefined {
    return undefined;
  }

  static is(u: unknown): u is PolicyFailure {
    return Predicate.isTagged(u, "PolicyFailure") && Predicate.hasProperty(u, "reason");
  }

  /** A local refusal of `operation`, which was therefore never dispatched. */
  static refuse(
    code: RefusalCode,
    message: string,
    operation = "enqueue",
    cause?: unknown,
  ): PolicyFailure {
    return PolicyFailure.make({
      reason: Refusal.make({ _tag: code, message }),
      context: {
        operation,
        outcome: "not-submitted",
        ...(cause === undefined ? {} : { detail: Redacted.make(cause) }),
      },
    });
  }

  /** A refusal because the clip a request field names has no known owner. */
  static missing(purpose: Missing["purpose"], operation = "enqueue"): PolicyFailure {
    return PolicyFailure.make({
      reason: Missing.make({ purpose }),
      context: { operation, outcome: "not-submitted" },
    });
  }

  /** A refusal by sequence affinity, with its SequenceError code. */
  static sequence(sequenceId: string, code: SequenceCode, operation = "enqueue"): PolicyFailure {
    return PolicyFailure.make({
      reason: SequenceRefusal.make({ sequenceId, code }),
      context: { operation, outcome: "not-submitted" },
    });
  }

  /** The refusal as a session-level failure: invalid input or invalid state, never sent. */
  toReactorError(): ReactorError {
    return ReactorError.make({
      reason: Failure.make({
        _tag: this.reason._tag === "InvalidRequest" ? "InvalidInput" : "InvalidState",
        message: this.message,
      }),
      context: this.context,
    });
  }
}

/** Any failure an orchestration raises: the client's, or a local refusal. */
export type OrchestrationFailure = ReactorFailure | PolicyFailure;

export const isOrchestrationFailure = (u: unknown): u is OrchestrationFailure =>
  isReactorFailure(u) || PolicyFailure.is(u);

/** A refusal as the session-level failure it stands for; any other failure as it is. */
export const asReactorFailure = (error: OrchestrationFailure): ReactorFailure =>
  PolicyFailure.is(error) ? error.toReactorError() : error;
