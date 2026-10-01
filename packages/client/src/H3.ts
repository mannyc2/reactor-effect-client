/**
 * The H3 Reference Turbo Realtime provider over a connected `Session`: H3's
 * state and queue as the model reports them, its commands, and the evidence
 * that an enqueued clip was accepted and what became of it.
 *
 * `make(session)` observes a session it neither allocates nor closes. Every
 * command's failure carries its dispatch outcome; an enqueue whose reply is
 * lost stays `unknown` and is never sent again, though later evidence may
 * still prove its clip.
 */
import type * as Crypto from "effect/Crypto";
import { dual } from "effect/Function";
import * as Predicate from "effect/Predicate";
import type * as Duration from "effect/Duration";
import type * as Effect from "effect/Effect";
import type * as Result from "effect/Result";
import type * as Scope from "effect/Scope";
import type * as Stream from "effect/Stream";
import type { Contract } from "./internal/h3/commands.js";
import type { DecodedMessage, Message, MessageType, Payload } from "./internal/h3/messages.js";
import type { ClipOperation } from "./internal/h3/operations.js";
import * as Provider_ from "./internal/h3/provider.js";
import { validateAudioReferenceEffect, validateReferenceEffect } from "./internal/h3/references.js";
import type { Reference } from "./internal/h3/references.js";
import type { Request } from "./internal/h3/request.js";
import type { ValidatedAudioReference, ValidatedReference } from "./internal/h3/references.js";
import type { Acceptance, ProviderSnapshot } from "./internal/h3/state.js";
import type { CanvasAspect } from "./internal/h3/profile.js";
import type { CommandFailure, ReactorError } from "./ReactorError.js";
import type { CommandReply, Session, SessionEvent } from "./Session.js";
import type { Submission } from "./internal/h3/submission.js";

export {
  alignFrames,
  audioReferenceLimits,
  canvases,
  documentedVersion,
  estimateTokens,
  h3ReferenceTurboRealtime,
  isRequestableSeconds,
  maxSeconds,
  minSeconds,
  modelName,
  referenceLimits,
  requestSeconds,
  source,
} from "./internal/h3/profile.js";
export type { CanvasAspect, ModelProfile } from "./internal/h3/profile.js";
export { Clip, Queue, State } from "./internal/h3/messages.js";
export type { DecodedMessage, Message, MessageType, Payload } from "./internal/h3/messages.js";
export type { CommandName, Contract } from "./internal/h3/commands.js";
export type {
  ClipFact,
  ClipOperation,
  ClipPhase,
  OperationFacts,
} from "./internal/h3/operations.js";
export type { Acceptance, ClipObservation, Facts, ProviderSnapshot } from "./internal/h3/state.js";
export type { State as SubmissionState, Submission } from "./internal/h3/submission.js";
export { Reference } from "./internal/h3/references.js";
export type { ValidatedAudioReference, ValidatedReference } from "./internal/h3/references.js";

export { Request } from "./internal/h3/request.js";

export interface Reply<K extends MessageType> {
  readonly value: Payload<K>;
  readonly source: CommandReply;
}

/** An ACK proves the command arrived, never a model payload. */
export type ControlResult =
  | { readonly _tag: "Acknowledged"; readonly source: CommandReply }
  | { readonly _tag: "Reply"; readonly message: Message; readonly source: CommandReply };

export type ProviderEvent =
  | {
      readonly _tag: "Message";
      readonly message: DecodedMessage;
      readonly source: CommandReply;
      readonly disposition: "applied" | "duplicate" | "stale";
    }
  | { readonly _tag: "Acknowledged"; readonly source: CommandReply }
  | { readonly _tag: "Acceptance"; readonly acceptance: Acceptance }
  | { readonly _tag: "Session"; readonly source: Exclude<SessionEvent, CommandReply> }
  | { readonly _tag: "Diagnostic"; readonly error: ReactorError; readonly source?: SessionEvent };

export interface ObservationOptions {
  /** Events an observer may hold unread before it fails with `Overflow`; 64 by default. */
  readonly capacity?: number | undefined;
}

/** A snapshot and every event after it: events at or below `revision` are already in it. */
export interface ProviderObservation {
  readonly initial: ProviderSnapshot;
  readonly revision: bigint;
  readonly events: Stream.Stream<ProviderEvent, ReactorError>;
}

/**
 * A commit hook may refuse locally with its own error (`E`); the submission
 * fails with it unchanged, since nothing was dispatched.
 */
export interface PrepareHooks<E = never> {
  readonly commit?: (submissionId: string) => Effect.Effect<void, CommandFailure | E>;
  /** Observes the result; its failure is diagnostic and never replaces the result. */
  readonly result?: (
    submissionId: string,
    result: Result.Result<Acceptance, CommandFailure | E>,
  ) => Effect.Effect<void>;
}

/**
 * H3 replies to a command before it broadcasts the state and queue the command
 * changed (hosted H3 broadcasts an enqueue's queue first). A command that
 * needs current facts therefore waits, within `replyTimeout`, for a
 * synchronizing provider before it sends, and after its reply for the
 * broadcasts the reply implies, so its caller reads its own effects.
 * `getState` and `getQueue` never wait. Each deadline is a finite, non-negative
 * `Duration.Input`; any other value fails `make` with `InvalidInput`.
 */
export interface Options {
  /** Each command through the observation of its reply; 15 seconds by default. */
  readonly replyTimeout?: Duration.Input | undefined;
  /** Each reference upload; 60 seconds by default. */
  readonly uploadTimeout?: Duration.Input | undefined;
  /** Reading the deployment schema, then the first state and queue; 60 seconds each by default. */
  readonly setupTimeout?: Duration.Input | undefined;
  /**
   * Bounds both waits of an enqueue its reply has not settled; 5 seconds by
   * default. Evidence of the clip that came before the reply decides once it
   * has waited this long without the reply, and an enqueue whose reply is lost
   * waits this long for evidence.
   */
  readonly reconcileWindow?: Duration.Input | undefined;
}

export interface Provider {
  readonly sessionId: string;
  readonly contract: Contract;
  readonly snapshot: Effect.Effect<ProviderSnapshot>;
  /** The snapshot at every change, starting with the current one. */
  readonly changes: Stream.Stream<ProviderSnapshot>;
  readonly observe: (
    options?: ObservationOptions,
  ) => Effect.Effect<ProviderObservation, ReactorError, Scope.Scope>;
  readonly events: (options?: ObservationOptions) => Stream.Stream<ProviderEvent, ReactorError>;
  /** The error that made the provider unavailable for good. */
  readonly failure: Effect.Effect<ReactorError>;
  readonly acceptance: (submissionId: string) => Effect.Effect<Acceptance | undefined>;
  /**
   * The facts of a committed submission's clip for as long as the scope holds
   * them; releasing the scope frees its slot. A submission that never
   * committed, or whose ended operation was released, fails with `InvalidState`.
   */
  readonly operation: (
    submission: Pick<Submission<Acceptance, unknown>, "id">,
  ) => Effect.Effect<ClipOperation, ReactorError, Scope.Scope>;
  readonly prepare: <E = never>(
    request: Request,
    hooks?: PrepareHooks<E>,
  ) => Effect.Effect<Submission<Acceptance, CommandFailure | E>, ReactorError | CommandFailure>;
  readonly enqueue: (request: Request) => Effect.Effect<Acceptance, CommandFailure>;
  readonly getState: Effect.Effect<Reply<"state_update">, CommandFailure>;
  readonly getQueue: Effect.Effect<Reply<"queue_update">, CommandFailure>;
  readonly refresh: Effect.Effect<void, CommandFailure>;
  readonly pop: (clipId: string) => Effect.Effect<Reply<"clip_popped">, CommandFailure>;
  readonly move: (
    clipId: string,
    position: number,
  ) => Effect.Effect<Reply<"clip_moved">, CommandFailure>;
  readonly play: (clipId?: string) => Effect.Effect<ControlResult, CommandFailure>;
  readonly stop: Effect.Effect<ControlResult, CommandFailure>;
  readonly setCanvas: (
    aspect: CanvasAspect,
  ) => Effect.Effect<Reply<"canvas_accepted">, CommandFailure>;
  readonly setAutoplay: (
    enabled: boolean,
  ) => Effect.Effect<Reply<"autoplay_accepted">, CommandFailure>;
  readonly setFlushOnClipEnd: (
    enabled: boolean,
  ) => Effect.Effect<Reply<"flush_accepted">, CommandFailure>;
  readonly reset: Effect.Effect<Reply<"session_reset">, CommandFailure>;
}

/**
 * The provider over an already connected session. It reads the deployment's
 * schema, then H3's state and queue; a failure releases what it acquired.
 */
export const make: {
  (
    options?: Options,
  ): (
    session: Session,
  ) => Effect.Effect<Provider, ReactorError | CommandFailure, Crypto.Crypto | Scope.Scope>;
  (
    session: Session,
    options?: Options,
  ): Effect.Effect<Provider, ReactorError | CommandFailure, Crypto.Crypto | Scope.Scope>;
} = dual(
  (args) => Predicate.hasProperty(args[0], "command"),
  (session: Session, options?: Options) => Provider_.make(session, options),
);

/** Validates an image reference once, so requests reuse it without checking it again. */
export const validateReference: (
  reference: Reference | ValidatedReference,
) => Effect.Effect<ValidatedReference, ReactorError> = validateReferenceEffect;

/**
 * Validates an audio reference once: its container, and for WAV and FLAC its
 * length and channels.
 */
export const validateAudioReference: (
  reference: Reference | ValidatedAudioReference,
) => Effect.Effect<ValidatedAudioReference, ReactorError> = validateAudioReferenceEffect;
