import type * as Duration from "effect/Duration";
import type * as Effect from "effect/Effect";
import type * as Result from "effect/Result";
import type * as Scope from "effect/Scope";
import type * as Stream from "effect/Stream";
import type { CommandFailure, PolicyFailure, ReactorError } from "../errors.js";
import type {
  CommandReply,
  ReplyTimeoutOptions,
  SessionEvent,
  UploadTimeoutOptions,
} from "../session/index.js";
import type { Submission } from "../Submission.js";
import type { UploadReference } from "../wire.generated.js";
import type {
  Clip,
  DecodedMessage,
  Message,
  MessageType,
  Payload,
  Queue,
  State,
} from "./messages.js";
import type {
  audioMimeTypes,
  CanvasAspect,
  documentedVersion,
  imageMimeTypes,
  modelName,
} from "./profile.js";

export type Aspect = CanvasAspect;
export type Reference =
  | { readonly _tag: "Bytes"; readonly bytes: Uint8Array }
  | { readonly _tag: "Uploaded"; readonly file: UploadReference };

declare const validatedReference: unique symbol;
/** Opaque validated input. Byte storage is private and detached from the caller. */
export interface ValidatedReference {
  readonly [validatedReference]: true;
  readonly _tag: "ValidatedReference";
  readonly mimeType: (typeof imageMimeTypes)[number];
  readonly size: number;
  readonly width: number | null;
  readonly height: number | null;
}

declare const validatedAudio: unique symbol;
/**
 * Opaque validated audio input. Byte storage is private and detached from the
 * caller. `seconds` and `channels` are read from a WAV or FLAC header; for the
 * other formats they are null, and H3 checks them when it receives the clip.
 */
export interface ValidatedAudioReference {
  readonly [validatedAudio]: true;
  readonly _tag: "ValidatedAudioReference";
  readonly mimeType: (typeof audioMimeTypes)[number];
  readonly size: number;
  readonly seconds: number | null;
  readonly channels: number | null;
}

export interface Request {
  readonly prompt: string;
  readonly references?: readonly (Reference | ValidatedReference)[];
  /**
   * Up to three audio references (`Audio 1`, `Audio 2`, ... in the prompt),
   * each 2–15 s of WAV, MP3, AAC/M4A, OGG/Opus, FLAC or WebM, mono or stereo,
   * at most 25 MiB. A clip with audio needs at least one image reference or a
   * `continueFrom`, and a continued clip takes at most two, because its
   * continuation uses the third for the previous clip's soundtrack. Sent as
   * `reference_audios` only when present, and only to a deployment whose
   * contract declares it (`Contract.referenceAudio`).
   */
  readonly audio?: readonly (Reference | ValidatedAudioReference)[];
  readonly seconds?: number;
  readonly seed?: number;
  readonly position?: number;
  readonly continueFrom?: string;
  /** User metadata is a provider string. Local acceptance wraps and preserves it. */
  readonly metadata?: string;
}

export interface Contract {
  readonly modelName: typeof modelName;
  readonly documentedVersion: typeof documentedVersion;
  readonly source: string;
  readonly subset: "prompt-and-images";
  /**
   * Whether the deployment's `enqueue` declares `reference_audios`. When it
   * does not, a request with audio is refused as `UnsupportedCapability`
   * before anything is uploaded or sent.
   */
  readonly referenceAudio: boolean;
  readonly deployment: { readonly title: string | null; readonly version: string | null };
}

export interface Facts {
  readonly state: State;
  readonly queue: Queue;
}
export interface ClipObservation {
  readonly clip: Clip;
  /** Last explicit clip lifecycle event; null means only a queue snapshot listed it. */
  readonly lifecycle: Exclude<MessageType, "queue_update" | "state_update"> | null;
  readonly source: CommandReply;
}
interface SnapshotBase {
  readonly sessionId: string;
  readonly transportGeneration: bigint;
  readonly revision: bigint;
  readonly clips: readonly ClipObservation[];
}
export type ProviderSnapshot = SnapshotBase &
  (
    | { readonly _tag: "Synchronizing"; readonly lastFacts: Facts | null }
    | { readonly _tag: "Ready"; readonly state: State; readonly queue: Queue }
    | {
        readonly _tag: "Unavailable";
        readonly cause: ReactorError;
        readonly lastFacts: Facts | null;
      }
  );

export interface Acceptance {
  readonly submissionId: string;
  readonly clip: Clip;
  readonly evidence: { readonly kind: "correlated" | "metadata"; readonly source: CommandReply };
}
/** A phase a clip can be awaited to reach. */
export type ClipPhase = "generated" | "started";

/** One piece of evidence about a clip, and the transport generation that carried it. */
export interface ClipFact {
  readonly clipId: string;
  /**
   * The lifecycle message that established it, or the `queue_update` or
   * `state_update` snapshot that listed the clip.
   */
  readonly message: MessageType;
  readonly transportGeneration: bigint;
  readonly source: CommandReply;
}

/** What one clip operation has established so far; monotone. */
export interface OperationFacts {
  readonly submissionId: string;
  readonly acceptance?: Acceptance;
  readonly generated?: ClipFact;
  readonly started?: ClipFact;
  readonly ended?: ClipFact;
  /** The provider retired before evidence decided the rest. */
  readonly indeterminate: boolean;
}

/**
 * A committed clip's facts as they arrive, each resolved at most once from the
 * provider's own evidence, and a phase only once the acceptance is decided. A
 * phase completes when it or a later one is observed, fails with `ClipEnded`
 * when the clip failed or was popped first, and fails `Indeterminate` if the
 * provider retires before evidence decides it. Every fact fails with the
 * enqueue's own failure when that failure was definite. Evidence from a later
 * transport generation of the same session still resolves the operation.
 */
export interface ClipOperation {
  readonly submissionId: string;
  readonly accepted: Effect.Effect<Acceptance, ReactorError | CommandFailure>;
  readonly reached: (phase: ClipPhase) => Effect.Effect<ClipFact, ReactorError | CommandFailure>;
  readonly ended: Effect.Effect<ClipFact, ReactorError | CommandFailure>;
  readonly facts: Effect.Effect<OperationFacts>;
}

export interface Reply<K extends MessageType> {
  readonly value: Payload<K>;
  readonly source: CommandReply;
}
/** An ACK establishes command receipt, never an invented model payload. */
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
  readonly capacity?: number;
  readonly maxBytes?: number;
}
export interface ProviderObservation {
  readonly initial: ProviderSnapshot;
  readonly revision: bigint;
  readonly events: Stream.Stream<ProviderEvent, ReactorError>;
}
/**
 * A commit hook may refuse locally with a `PolicyFailure` (`E`); the submission
 * fails with it unchanged, since nothing was dispatched.
 */
export interface PrepareHooks<E extends PolicyFailure = never> {
  readonly commit?: (submissionId: string) => Effect.Effect<void, CommandFailure | E>;
  /** A post-commit observer. Failure is diagnostic and cannot rewrite remote evidence. */
  readonly result?: (
    submissionId: string,
    result: Result.Result<Acceptance, CommandFailure | E>,
  ) => Effect.Effect<void>;
}
/**
 * The provider's options. `replyTimeout` bounds each H3 command through the
 * observation of its returned envelope, 15 seconds by default; `uploadTimeout`
 * bounds each reference upload, 60 seconds by default. Both are at most 10
 * minutes, and a bare number is milliseconds.
 *
 * H3 documents that a command replies before it broadcasts the state and queue
 * it changed, though hosted H3 broadcasts an enqueue's queue first. A command
 * that needs the provider's current facts therefore waits,
 * up to `replyTimeout` each time, for a synchronizing provider to become ready
 * before it sends (it is refused with `InvalidState`, not submitted, if the
 * provider does not), and after its reply for the broadcasts the reply implies,
 * so its caller reads its own effects. `getState` and `getQueue` never wait.
 */
export interface Options extends ReplyTimeoutOptions, UploadTimeoutOptions {
  /**
   * How long reading the deployment schema, and then the initial state and
   * queue, may each take; 60 seconds by default and at most 10 minutes. A bare
   * number is milliseconds.
   */
  readonly setupTimeout?: Duration.Input | undefined;
  /**
   * How long an uncertain enqueue waits for evidence that settles it; 5
   * seconds by default and at most 1 minute. A bare number is milliseconds.
   */
  readonly reconcileWindow?: Duration.Input | undefined;
  /**
   * How long a `PrepareHooks.result` observer may run; 1 second by default and
   * at most 1 minute. A bare number is milliseconds.
   */
  readonly resultHookTimeout?: Duration.Input | undefined;
  readonly maxPending?: number;
  readonly maxTrackedClips?: number;
  readonly maxAcceptances?: number;
  /**
   * Clip operations retained for `operation`, 1024 by default. A commit takes a
   * slot before it sends anything; when every slot holds an operation that has
   * not ended, the commit is refused with `Overflow`, not submitted.
   */
  readonly maxOperations?: number;
  readonly maxCachedUploads?: number;
  readonly maxRetainedBytes?: number;
  readonly maxPromptBytes?: number;
  readonly observation?: ObservationOptions;
}

export interface Provider {
  readonly sessionId: string;
  readonly contract: Contract;
  readonly current: Effect.Effect<ProviderSnapshot>;
  readonly observe: (
    options?: ObservationOptions,
  ) => Effect.Effect<ProviderObservation, ReactorError, Scope.Scope>;
  readonly events: (options?: ObservationOptions) => Stream.Stream<ProviderEvent, ReactorError>;
  readonly failure: Effect.Effect<ReactorError>;
  /** Local annotations are separate from the provider's authoritative state. */
  readonly acceptances: Effect.Effect<readonly Acceptance[]>;
  readonly acceptance: (submissionId: string) => Effect.Effect<Acceptance | undefined>;
  /**
   * The retained facts of a committed submission's clip, from acceptance to its
   * end, for as long as the scope holds them: releasing the scope acknowledges
   * the operation and frees its slot. Call it once `submit` has started; a
   * submission that never committed, or whose ended operation was evicted,
   * fails with `InvalidState`.
   */
  readonly operation: (
    submission: Pick<Submission<Acceptance, unknown>, "id">,
  ) => Effect.Effect<ClipOperation, ReactorError, Scope.Scope>;
  readonly prepare: <E extends PolicyFailure = never>(
    request: Request,
    hooks?: PrepareHooks<E>,
  ) => Effect.Effect<Submission<Acceptance, CommandFailure | E>, ReactorError | CommandFailure>;
  /**
   * Host IO belongs to the same provisional scope and commit owner as static
   * preparation. A `PolicyFailure` from the preparation fails the submission
   * unchanged; any other failure becomes a not-submitted `CommandFailure`.
   */
  readonly prepareFrom: <E extends PolicyFailure = never>(
    preparation: Effect.Effect<Request, ReactorError | CommandFailure | E, Scope.Scope>,
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
  readonly setSeed: (seed: number) => Effect.Effect<Reply<"seed_accepted">, CommandFailure>;
  readonly setClipSeconds: (
    seconds: number,
  ) => Effect.Effect<Reply<"clip_length_accepted">, CommandFailure>;
  readonly setCanvas: (aspect: Aspect) => Effect.Effect<Reply<"canvas_accepted">, CommandFailure>;
  readonly setAutoplay: (
    enabled: boolean,
  ) => Effect.Effect<Reply<"autoplay_accepted">, CommandFailure>;
  readonly setFlushOnClipEnd: (
    enabled: boolean,
  ) => Effect.Effect<Reply<"flush_accepted">, CommandFailure>;
  readonly reset: Effect.Effect<Reply<"session_reset">, CommandFailure>;
}
