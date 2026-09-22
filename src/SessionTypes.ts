import type * as Effect from "effect/Effect";
import type * as Scope from "effect/Scope";
import type * as Stream from "effect/Stream";
import type { Descriptor } from "./contract.js";
import type { HttpOptions, Termination } from "./http.js";
import type { Correlation } from "./correlation.js";
import type { ReactorError } from "./errors.js";
import type { Json, JsonObject } from "./json.js";
import type * as W from "./wire.generated.js";

export type Status = "idle" | "connecting" | "waiting" | "ready" | "disconnected" | "closing" | "closed";
export type Ownership = "owned" | "attached" | "allocating" | "unknown";

export interface SessionOptions extends HttpOptions {
  readonly model?: { readonly name: string; readonly version?: string };
  readonly attach?: { readonly sessionId: string; readonly connectionId?: number };
  readonly extraArgs?: Json;
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

export interface Snapshot {
  readonly status: Status;
  readonly generation: bigint;
  readonly remote?: {
    readonly ownership: Ownership;
    readonly sessionId?: string;
    readonly descriptor?: Descriptor;
    readonly connectionId?: number;
  };
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

export type CommandReply =
  | { readonly kind: "ack"; readonly raw: W.DataServerMessage }
  | { readonly kind: "message"; readonly type: string; readonly data?: JsonObject; readonly raw: W.DataServerMessage };

export type EventPayload =
  | { readonly type: "status"; readonly status: Status }
  | { readonly type: "model"; readonly reply: CommandReply; readonly correlation: Correlation }
  | { readonly type: "control"; readonly message: W.ControlServerMessage; readonly correlation: Correlation }
  | { readonly type: "track"; readonly name: string; readonly mid: string }
  | { readonly type: "decoded"; readonly kind: "audio" | "video"; readonly name: string; readonly mid: string }
  | { readonly type: "diagnostic"; readonly error: ReactorError }
  | { readonly type: "upload"; readonly progress: UploadProgress };

export type SessionEvent = EventPayload & { readonly sequence: bigint; readonly generation: bigint };
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
export type Observe = (options?: { readonly capacity?: number; readonly maxBytes?: number }) =>
  Effect.Effect<Observation, ReactorError, Scope.Scope>;
