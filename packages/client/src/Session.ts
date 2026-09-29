/**
 * One Reactor session: an allocation this process owns or an attachment it
 * joined, its connection generations, commands, events and cleanup evidence.
 * `Reactor.create` and `Reactor.attach` acquire one in a scope.
 */
import type * as Duration from "effect/Duration";
import type * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import type * as Scope from "effect/Scope";
import type * as Stream from "effect/Stream";
import type { Capabilities, Descriptor, Transport } from "./Coordinator.js";
import { Termination } from "./Coordinator.js";
import type { Correlation } from "./internal/correlator.js";
import type { Statistics } from "./internal/stats.js";
import type { DecodedMedia, TrackMedia } from "./Media.js";
import type { CommandFailure, ReactorError } from "./ReactorError.js";
import { FailureSummary } from "./ReactorError.js";

export type { Correlation } from "./internal/correlator.js";
export type { Statistics } from "./internal/stats.js";
/** A recording the session prepared, with the playlist to download it from. */
export const ClipReady = Schema.Struct({
  sessionId: Schema.String,
  kind: Schema.String,
  startMarker: Schema.Finite,
  endMarker: Schema.Finite,
  nowMarker: Schema.Finite,
  predictedReadyAtMs: Schema.BigInt,
  playlistUrl: Schema.String,
});
export type ClipReady = typeof ClipReady.Type;

/**
 * A control-channel message Reactor sent. Provider text (a clip failure's
 * reason, an error's code and message) stays `Redacted`.
 */
export const ControlMessage = Schema.Union([
  Schema.TaggedStruct("ModelSchema", { openapi: Schema.optionalKey(Schema.JsonObject) }),
  Schema.TaggedStruct("ClipReady", { clip: ClipReady }),
  Schema.TaggedStruct("ClipFailed", { reason: Schema.Redacted(Schema.String) }),
  Schema.TaggedStruct("TrackPublished", { name: Schema.String }),
  Schema.TaggedStruct("Error", {
    code: Schema.Redacted(Schema.String),
    message: Schema.Redacted(Schema.String),
  }),
]);
export type ControlMessage = typeof ControlMessage.Type;
/** An uploaded file, as a command refers to it. */
export interface UploadReference {
  readonly uploadId: string;
  readonly name: string;
  readonly mimeType: string;
  readonly size: bigint;
}

export type Status =
  | "idle"
  | "connecting"
  | "waiting"
  | "ready"
  | "disconnected"
  | "closing"
  | "closed";
export type Ownership = "owned" | "attached" | "allocating" | "unknown";

/**
 * A session's cleanup evidence, a Schema so it can be persisted. Termination
 * is confirmed by an independent read, never by a DELETE response alone.
 */
export const CloseReport = Schema.Struct({
  localClosed: Schema.Boolean,
  allocation: Schema.Literals(["none", "known", "unknown"]),
  ownership: Schema.optionalKey(Schema.Literals(["owned", "attached"])),
  sessionId: Schema.optionalKey(Schema.String),
  remote: Termination,
  unpublishSubmitted: Schema.Array(Schema.String),
  /** Claims possibly accepted remotely without an attributable response. */
  unresolvedPublications: Schema.Array(Schema.String),
  localErrors: Schema.Array(FailureSummary),
});
export type CloseReport = typeof CloseReport.Type;

/**
 * Whether the session a report closed may still bill: this process owned it
 * and no read confirmed it ended, or it asked for an allocation whose outcome,
 * and so whose id, never arrived. Nothing can confirm the second ended; the
 * session's cap bounds it.
 */
export const mayStillBill = (report: CloseReport): boolean =>
  report.allocation === "unknown" ||
  (report.allocation === "known" && report.ownership === "owned" && !report.remote.confirmed);

export interface ReadyDescriptor extends Descriptor {
  readonly capabilities: Capabilities;
  readonly selected_transport: typeof Transport.Type;
}

export interface ReadyState {
  readonly status: "ready";
  readonly generation: bigint;
  readonly remote: {
    readonly ownership: "owned" | "attached";
    readonly sessionId: string;
    readonly descriptor: ReadyDescriptor;
    readonly connectionId: number;
  };
}

interface SnapshotDetails {
  readonly generation: bigint;
  /**
   * The session is reconnecting a dropped connection on its own: from the drop until a
   * connection is ready or it stops trying, as `Reactor.Options.reconnect` says. `disconnected`
   * without it is lasting: only `session.reconnect` connects the session again.
   */
  readonly reconnecting: boolean;
  readonly pending: { readonly data: number; readonly control: number };
  readonly pausedLocally: ReadonlyArray<string>;
  readonly claimedTracks: ReadonlyArray<string>;
  readonly unresolvedPublications: ReadonlyArray<string>;
  readonly receivedTracks: ReadonlyArray<string>;
  readonly observationOverflows: bigint;
  readonly subscribers: number;
  readonly lastError?: ReactorError;
  readonly close?: CloseReport;
}

export type Snapshot = SnapshotDetails &
  (
    | ReadyState
    | {
        readonly status: Exclude<Status, "ready">;
        readonly remote?: {
          readonly ownership: Ownership;
          readonly sessionId?: string;
          readonly descriptor?: Descriptor;
          readonly connectionId?: number;
        };
      }
  );

export interface Attribution {
  readonly requestId: string;
  readonly sequence: bigint;
  readonly generation: bigint;
  readonly correlation: Correlation;
}

/** A command's result and its observation are the same attributed value. */
export type CommandReply = Attribution & {
  readonly _tag: "Model";
  readonly outcome: "replied";
} & (
    | { readonly kind: "ack" }
    | { readonly kind: "message"; readonly type: string; readonly data?: Schema.JsonObject }
  );

export interface UploadProgress {
  readonly allocation: "not-requested" | "unknown" | "confirmed";
  readonly transfer: "not-requested" | "unknown" | "confirmed";
  readonly notification: "not-submitted" | "unknown" | "submitted";
  readonly file?: UploadReference;
}

export interface Uploaded {
  readonly file: UploadReference;
  readonly transfer: "confirmed";
  readonly notification: "submitted";
}

export type EventPayload =
  | { readonly _tag: "Status"; readonly status: Status }
  | {
      readonly _tag: "CommandError";
      readonly error: ReactorError;
      readonly requestId: string;
      readonly correlation: Correlation;
    }
  /** A control message; a reply to this client's own request only when correlation says so. */
  | {
      readonly _tag: "Control";
      readonly message: ControlMessage;
      readonly requestId: string;
      readonly correlation: Correlation;
    }
  | { readonly _tag: "Track"; readonly name: string; readonly mid: string }
  | {
      readonly _tag: "Decoded";
      readonly kind: "audio" | "video";
      readonly name: string;
      readonly mid: string;
    }
  | { readonly _tag: "Diagnostic"; readonly error: ReactorError }
  /**
   * Reactor's content moderation flagged an input. `terminate` means Reactor
   * is ending the session, and it is not reconnected; `warn` is informational.
   * Its text stays out: the categories name what was flagged.
   */
  | {
      readonly _tag: "Moderation";
      readonly action: string;
      readonly categories: ReadonlyArray<string>;
      /** What was screened, such as `prompt` or `image`, when Reactor says. */
      readonly inputKind?: string;
      /** The command that carried it, when Reactor says. */
      readonly command?: string;
      /** The request it answers, when Reactor names one. */
      readonly requestId?: string;
    }
  | { readonly _tag: "Upload"; readonly progress: UploadProgress };

export type SessionEvent =
  | CommandReply
  | (EventPayload & { readonly sequence: bigint; readonly generation: bigint });

/** A state and every event after it, with no gap: events at or below `revision` are already in it. */
export interface Observation {
  readonly initial: Snapshot;
  readonly revision: bigint;
  readonly events: Stream.Stream<SessionEvent, ReactorError>;
}

export interface ObserveOptions {
  /** Events an observer may hold unread before it fails with `Overflow`; 64 by default. */
  readonly capacity?: number | undefined;
}

/**
 * The library's own reply deadline, not a caller's wait: after dispatch its
 * expiry fails with `Timeout`, outcome `unknown`, and the request stays
 * attributable. To only stop waiting, fork the command and bound the join.
 */
export interface CommandOptions {
  /** 10 seconds by default. */
  readonly replyTimeout?: Duration.Input | undefined;
  /** Uploaded files the command refers to, by the name the command uses. */
  readonly uploads?: ReadonlyMap<string, UploadReference> | undefined;
}

export interface UploadOptions {
  /** From allocation to notification; the session's upload timeout by default. */
  readonly uploadTimeout?: Duration.Input | undefined;
}

export interface Session {
  readonly id: string;
  readonly ownership: "owned" | "attached";
  readonly snapshot: Effect.Effect<Snapshot>;
  /**
   * The snapshot at every lifecycle change, starting with the current one; a reader that falls
   * behind still sees each status in turn.
   */
  readonly changes: Stream.Stream<Snapshot>;
  /** The negotiated connection, when the session is ready. */
  readonly ready: Effect.Effect<ReadyState, ReactorError>;
  /** Events from now on; see `observe` to pair them with a state. */
  readonly events: (options?: ObserveOptions) => Stream.Stream<SessionEvent, ReactorError>;
  readonly observe: (
    options?: ObserveOptions,
  ) => Effect.Effect<Observation, ReactorError, Scope.Scope>;
  readonly command: (
    name: string,
    data: unknown,
    options?: CommandOptions,
  ) => Effect.Effect<CommandReply, CommandFailure>;
  /** The deployment's OpenAPI document, when it publishes one. */
  readonly schema: Effect.Effect<{ readonly openapi?: Schema.JsonObject }, ReactorError>;
  readonly upload: (
    name: string,
    mimeType: string,
    bytes: Uint8Array,
    options?: UploadOptions,
  ) => Effect.Effect<Uploaded, ReactorError>;
  readonly requestRecordingClip: (seconds: number) => Effect.Effect<ClipReady, ReactorError>;
  readonly recording: Effect.Effect<ClipReady, ReactorError>;
  /** A sample of the connection's statistics; its `rates` run from the session's previous sample. */
  readonly stats: Effect.Effect<Statistics, ReactorError>;
  /** The current generation's decoded media, from a host that decodes it. */
  readonly decoded: Effect.Effect<DecodedMedia, ReactorError>;
  /** The current generation's platform tracks, from a host that has them. */
  readonly tracks: Effect.Effect<TrackMedia, ReactorError>;
  /**
   * A new connection generation now, from a ready or a dropped connection; it never replays a
   * command. A session reconnects a dropped connection on its own unless `Reactor.Options`
   * turns `reconnect` off.
   */
  readonly reconnect: Effect.Effect<void, ReactorError>;
  /** Idempotent. Closing an owned session attempts and then confirms remote termination. */
  readonly close: Effect.Effect<CloseReport>;
}
