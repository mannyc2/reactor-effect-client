import type * as Effect from "effect/Effect";
import type * as Scope from "effect/Scope";
import type * as Stream from "effect/Stream";
import type { Capabilities, Descriptor } from "./contract.js";
import type { HttpOptions, Termination } from "./coordinator/_internal/client.js";
import type { Correlation } from "./correlation.js";
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

export interface SessionOptions extends HttpOptions {
  readonly intent: AcquisitionIntent;
  readonly readyTimeoutMs?: number;
  readonly connectTimeoutMs?: number;
  readonly commandTimeoutMs?: number;
  readonly heartbeatMs?: number;
  readonly autoResumeTracks?: boolean;
  readonly maxPending?: number;
  readonly requestNamespace?: string;
  readonly maxUploadBytes?: number;
  /** Completion does not imply remote termination or stopped billing. */
  readonly onClose?: (report: CloseReport) => void;
}

export interface CloseReport {
  readonly localClosed: boolean;
  readonly allocation: "none" | "known" | "unknown";
  readonly ownership?: "owned" | "attached";
  readonly sessionId?: string;
  readonly remote: Termination;
  readonly unpublishSubmitted: readonly string[];
  /** Claims possibly accepted remotely without an attributable response. */
  readonly unresolvedPublications: readonly string[];
  readonly localErrors: readonly ReactorError[];
}

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
