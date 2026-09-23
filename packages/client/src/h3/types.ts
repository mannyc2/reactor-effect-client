import type * as Effect from "effect/Effect";
import type * as Result from "effect/Result";
import type * as Scope from "effect/Scope";
import type * as Stream from "effect/Stream";
import type { ReactorError } from "../errors.js";
import type { CommandFailure, CommandReply, SessionEvent } from "../session/index.js";
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
import type { CanvasAspect, documentedVersion, imageMimeTypes, modelName } from "./profile.js";

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

export interface Request {
  readonly prompt: string;
  readonly references?: readonly (Reference | ValidatedReference)[];
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
export interface PrepareHooks {
  readonly commit?: (submissionId: string) => Effect.Effect<void, CommandFailure>;
  /** A post-commit observer. Failure is diagnostic and cannot rewrite remote evidence. */
  readonly result?: (
    submissionId: string,
    result: Result.Result<Acceptance, CommandFailure>,
  ) => Effect.Effect<void>;
}
export interface Options {
  readonly commandTimeoutMs?: number;
  readonly setupTimeoutMs?: number;
  readonly reconcileWindowMs?: number;
  readonly resultHookTimeoutMs?: number;
  readonly maxPending?: number;
  readonly maxTrackedClips?: number;
  readonly maxAcceptances?: number;
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
  readonly prepare: (
    request: Request,
    hooks?: PrepareHooks,
  ) => Effect.Effect<Submission<Acceptance, CommandFailure>, ReactorError>;
  /** Host IO belongs to the same provisional scope and commit owner as static preparation. */
  readonly prepareFrom: (
    preparation: Effect.Effect<Request, ReactorError, Scope.Scope>,
    hooks?: PrepareHooks,
  ) => Effect.Effect<Submission<Acceptance, CommandFailure>, ReactorError>;
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
