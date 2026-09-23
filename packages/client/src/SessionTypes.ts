import type * as Duration from "effect/Duration";
import * as Schema from "effect/Schema";
import type * as Effect from "effect/Effect";
import type * as Scope from "effect/Scope";
import type * as Stream from "effect/Stream";
import type { Capabilities, Descriptor } from "./contract.js";
import { Termination } from "./coordinator/_internal/client.js";
import type { HttpOptions } from "./coordinator/_internal/client.js";
import type { Correlation } from "./correlation.js";
import { ReactorErrorFromJson } from "./errors.js";
import type { ReactorError } from "./errors.js";
import type { Json, JsonObject } from "./json.js";
import type * as W from "./wire.generated.js";

export type Status =
  | "idle"
  | "connecting"
  | "waiting"
  | "ready"
  | "disconnected"
  | "closing"
  | "closed";
export type Ownership = "owned" | "attached" | "allocating" | "unknown";

/** Constructed by the acquisition boundary after validating operation-specific input. */
export type AcquisitionIntent =
  | {
      readonly _tag: "Create";
      readonly model: { readonly name: string; readonly version?: string };
      readonly extraArgs?: Json;
    }
  | { readonly _tag: "Attach"; readonly sessionId: string; readonly connectionId?: number };

/**
 * The session-owned reply budget. The session's default and a call's override
 * share this key. It is the library's own deadline, not a caller's wait:
 * after dispatch, expiry fails with `Timeout`, outcome `"unknown"`, and the
 * command's `requestId` and generation, and the pending slot stays held until a
 * late reply or the generation retires. To only stop waiting, fork the command
 * and bound `Fiber.join` with `Effect.timeout`.
 */
export interface ReplyTimeoutOptions {
  /**
   * How long a command or control request waits for its reply; 10 seconds by
   * default and at most 10 minutes. A bare number is milliseconds.
   */
  readonly replyTimeout?: Duration.Input | undefined;
}

/** The whole-upload budget. The session's default and a call's override share this key. */
export interface UploadTimeoutOptions {
  /**
   * How long an upload may take from allocation to notification; 60 seconds by
   * default and at most 10 minutes. A bare number is milliseconds.
   */
  readonly uploadTimeout?: Duration.Input | undefined;
}

export interface SessionTimeouts extends ReplyTimeoutOptions, UploadTimeoutOptions {
  /**
   * How long connecting or reconnecting may take in all; 3 minutes by default
   * and at most 10 minutes. A bare number is milliseconds.
   */
  readonly connectTimeout?: Duration.Input | undefined;
  /**
   * How long the peer and both channels may take to become ready after the
   * answer; 30 seconds by default and at most 10 minutes. A bare number is
   * milliseconds.
   */
  readonly readyTimeout?: Duration.Input | undefined;
  /**
   * The interval between heartbeats; 10 seconds by default and at most 10
   * minutes. `"Infinity"` disables the heartbeat. A bare number is milliseconds.
   */
  readonly heartbeatInterval?: Duration.Input | undefined;
  /** @deprecated Removed in 0.3.0: use `readyTimeout` (a bare number is milliseconds). */
  readonly readyTimeoutMs?: never;
  /** @deprecated Removed in 0.3.0: use `connectTimeout` (a bare number is milliseconds). */
  readonly connectTimeoutMs?: never;
  /** @deprecated Removed in 0.3.0: use `replyTimeout` (a bare number is milliseconds). */
  readonly commandTimeoutMs?: never;
  /**
   * @deprecated Removed in 0.3.0: use `heartbeatInterval` (a bare number is
   * milliseconds, and `"Infinity"` disables the heartbeat).
   */
  readonly heartbeatMs?: never;
}

/** A command's options. */
export interface CommandOptions extends ReplyTimeoutOptions {
  /** Uploaded files the command refers to, by the name the command uses. */
  readonly uploads?: ReadonlyMap<string, W.UploadReference> | undefined;
}

export interface SessionOptions extends HttpOptions, SessionTimeouts {
  readonly intent: AcquisitionIntent;
  readonly autoResumeTracks?: boolean;
  readonly maxPending?: number;
  readonly requestNamespace?: string;
  readonly maxUploadBytes?: number;
  /** Completion does not imply remote termination or stopped billing. */
  readonly onClose?: (report: CloseReport) => void;
}

/**
 * A session's cleanup evidence. As a Schema it encodes for persistence; its
 * failures encode as diagnostic JSON, without provider text.
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
  localErrors: Schema.Array(ReactorErrorFromJson),
});
export interface CloseReport extends Schema.Schema.Type<typeof CloseReport> {}

export interface ReadyDescriptor extends Descriptor {
  readonly capabilities: Capabilities;
  readonly selected_transport: { readonly protocol: string; readonly version: string };
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
  readonly pending: { readonly data: number; readonly control: number };
  readonly pausedLocally: readonly string[];
  readonly claimedTracks: readonly string[];
  readonly unresolvedPublications: readonly string[];
  readonly receivedTracks: readonly string[];
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

/** The command result and its observation are the same attributed object. */
export type CommandReply = Attribution & {
  readonly _tag: "Model";
  readonly outcome: "replied";
} & (
    | { readonly kind: "ack"; readonly raw: W.DataServerMessage }
    | {
        readonly kind: "message";
        readonly type: string;
        readonly data?: JsonObject;
        readonly raw: W.DataServerMessage;
      }
  );

export type EventPayload =
  | { readonly _tag: "Status"; readonly status: Status }
  | {
      readonly _tag: "CommandError";
      readonly error: ReactorError;
      readonly requestId: string;
      readonly correlation: Correlation;
    }
  | {
      readonly _tag: "Control";
      readonly message: W.ControlServerMessage;
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
  | { readonly _tag: "Upload"; readonly progress: UploadProgress };

export type SessionEvent =
  | CommandReply
  | (EventPayload & { readonly sequence: bigint; readonly generation: bigint });
export interface UploadProgress {
  readonly allocation: "not-requested" | "unknown" | "confirmed";
  readonly transfer: "not-requested" | "unknown" | "confirmed";
  readonly notification: "not-submitted" | "unknown" | "submitted";
  readonly file?: W.UploadReference;
}
export interface Uploaded {
  readonly file: W.UploadReference;
  readonly transfer: "confirmed";
  readonly notification: "submitted";
}
export type ControlPayload = NonNullable<W.ControlClientMessage["payload"]>;
export type ControlReply = NonNullable<W.ControlServerMessage["payload"]>;

export interface Observation {
  readonly initial: Snapshot;
  readonly revision: bigint;
  readonly events: Stream.Stream<SessionEvent, ReactorError>;
}
export type Observe = (options?: {
  readonly capacity?: number;
  readonly maxBytes?: number;
}) => Effect.Effect<Observation, ReactorError, Scope.Scope>;
