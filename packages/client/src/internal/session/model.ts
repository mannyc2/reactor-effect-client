/**
 * What a session and each of its connection generations hold, and the core
 * the session's parts share. The parts are built from the core in
 * `internal/session.ts`, one concern each.
 */
import * as Cause from "effect/Cause";
import type * as Deferred from "effect/Deferred";
import type * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import type * as Queue from "effect/Queue";
import type * as Ref from "effect/Ref";
import type * as Schedule from "effect/Schedule";
import type * as Schema from "effect/Schema";
import type * as Scope from "effect/Scope";
import type * as SubscriptionRef from "effect/SubscriptionRef";
import type { MessageInitShape } from "@bufbuild/protobuf";
import type { Descriptor, IceCandidate, Mapping, Signaling } from "../../Coordinator.js";
import type { MediaTrack, Peer, PeerEvent } from "../../Peer.js";
import { type ContextInput, ReactorError } from "../../ReactorError.js";
import type {
  CloseReport,
  CommandReply,
  ControlMessage,
  EventPayload,
  ReadyState,
  SessionEvent,
  Status,
} from "../../Session.js";
import type * as Correlator from "../correlator.js";
import type * as Hub from "../hub.js";
import type * as Stats from "../stats.js";
import type * as Wire from "../wire.js";

export interface Settings {
  readonly replyTimeout: Duration.Duration;
  readonly uploadTimeout: Duration.Duration;
  readonly connectTimeout: Duration.Duration;
  /**
   * A reconnect's deadline, the session's own counted from the drop that began it: Reactor ends a
   * session 30 s after it loses its last connection.
   */
  readonly reconnectTimeout: Duration.Duration;
  /** When the session tries a dropped connection again; none leaves it dropped. */
  readonly reconnect: Schedule.Schedule<unknown, ReactorError> | undefined;
  readonly readyTimeout: Duration.Duration;
  /** Infinite disables the heartbeat. */
  readonly heartbeat: Duration.Duration;
  readonly maxPending: number;
  readonly maxUploadBytes: number;
  /** Distinguishes this client's request ids from another's on a shared session. */
  readonly namespace: string;
}

export type Intent =
  | {
      readonly _tag: "Create";
      readonly model: { readonly name: string; readonly version?: string | undefined };
      readonly extraArgs?: Schema.Json | undefined;
    }
  | {
      readonly _tag: "Attach";
      readonly sessionId: string;
      readonly connectionId?: number | undefined;
      /** The attached session's remote lifetime is taken over: it is owned. */
      readonly adopt: boolean;
    };

export interface Known {
  readonly ownership: "owned" | "attached";
  readonly id: string;
  readonly descriptor?: Descriptor | undefined;
  readonly connectionId?: number | undefined;
}
export type RemoteSession = { readonly ownership: "allocating" | "unknown" } | Known;
export const isKnown = (remote: RemoteSession | undefined): remote is Known =>
  remote?.ownership === "owned" || remote?.ownership === "attached";

/** What one connection generation has learned and holds. */
export interface Link {
  readonly failure?: ReactorError | undefined;
  readonly peerConnected: boolean;
  readonly controlOpen: boolean;
  readonly dataOpen: boolean;
  readonly ice: ReadonlyArray<IceCandidate>;
  readonly iceBytes: number;
  readonly iceDone: boolean;
  readonly finalSent: boolean;
  readonly connectionId?: number | undefined;
  readonly mapping: ReadonlyArray<Mapping>;
  readonly negotiated?: ReadyState["remote"] | undefined;
  readonly paused: ReadonlySet<string>;
  readonly claimed: ReadonlySet<string>;
  readonly sending: ReadonlyMap<string, MediaTrack>;
  /** Publication claims by request id, until their reply names the track. */
  readonly claims: ReadonlyMap<string, string>;
  readonly busy: ReadonlySet<string>;
}

export const newLink: Link = {
  peerConnected: false,
  controlOpen: false,
  dataOpen: false,
  ice: [],
  iceBytes: 0,
  iceDone: false,
  finalSent: false,
  mapping: [],
  paused: new Set(),
  claimed: new Set(),
  sending: new Map(),
  claims: new Map(),
  busy: new Set(),
};

/** One connection generation: its scope, peer, readiness and the events it applies in order. */
export interface Connection {
  readonly generation: bigint;
  readonly scope: Scope.Closeable;
  readonly peer: Peer;
  readonly ready: Deferred.Deferred<void, ReactorError>;
  readonly failed: Deferred.Deferred<never, ReactorError>;
  readonly events: Queue.Queue<PeerEvent>;
  readonly iceWake: Queue.Queue<void>;
  readonly link: Ref.Ref<Link>;
}

export interface State {
  readonly status: Status;
  /** Content moderation is ending the session: nothing reconnects it. */
  readonly moderated: boolean;
  /** A dropped connection is reconnected on its own: the session has a policy and was acquired. */
  readonly reconnects: boolean;
  /** The session is reconnecting on its own, from the drop until a connection is ready or it stops. */
  readonly reconnecting: boolean;
  readonly generation: bigint;
  readonly remote: RemoteSession | undefined;
  readonly connection: Connection | undefined;
  readonly lastError: ReactorError | undefined;
  readonly close: CloseReport | undefined;
  readonly received: ReadonlySet<string>;
  readonly bitrates: ReadonlyMap<string, number>;
  readonly sampler: Stats.SamplerState;
}

export const transitions: Record<Status, ReadonlyArray<Status>> = {
  idle: ["connecting", "closing"],
  connecting: ["waiting", "disconnected", "closing"],
  waiting: ["ready", "disconnected", "closing"],
  ready: ["connecting", "disconnected", "closing"],
  disconnected: ["connecting", "closing"],
  closing: ["closed"],
  closed: [],
};

export const isClosing = (status: Status): boolean => status === "closing" || status === "closed";

/** Whether no later attempt can reconnect the session after `error`: it ended, or is gone. */
export const ends = (error: ReactorError): boolean => {
  switch (error.reason._tag) {
    case "TerminalSession":
    case "Moderated":
      return true;
    case "Http":
      return error.reason.status === 404;
    default:
      return false;
  }
};

/** Why nothing connects again a session that content moderation ended. */
export const moderationEnded = (context: ContextInput) =>
  ReactorError.fromCode("Moderated", "content moderation ended the session", context);

/** A control message this client sends. */
export type ControlPayload = Exclude<
  MessageInitShape<typeof Wire.ControlClientMessageSchema>["payload"],
  { readonly case: undefined } | undefined
>;

/** What the session's parts share: its state, its event hub and its correlators. */
export interface Core {
  readonly intent: Intent;
  readonly settings: Settings;
  readonly signaling: Signaling;
  readonly state: SubscriptionRef.SubscriptionRef<State>;
  /** The last event sequence number published. */
  readonly sequence: Ref.Ref<bigint>;
  readonly hub: Hub.Hub<SessionEvent>;
  readonly data: Correlator.Correlator<CommandReply>;
  readonly control: Correlator.Correlator<ControlMessage>;
  /** Publishes an event on `generation`, the current one when omitted. */
  readonly publish: (payload: EventPayload, generation?: bigint) => Effect.Effect<void>;
  /** Moves the session to `status` if it is not there yet; an illegal move is a defect. */
  readonly transition: (status: Status) => Effect.Effect<void>;
}

/** The fallback of a `timeoutOrElse`: `operation`'s deadline passed. */
export const timedOut = (operation: string) => () =>
  Effect.fail(ReactorError.fromCode("Timeout", `${operation}: deadline`));

/** The typed failure in a cause, or `fallback`'s for a defect or an interruption. */
export const failureOr =
  (fallback: () => ReactorError) =>
  (cause: Cause.Cause<ReactorError>): ReactorError => {
    const error = Cause.findError(cause);
    return error._tag === "Success" ? error.success : fallback();
  };

/** `set` with `value` in it. */
export const including =
  <A>(value: A) =>
  (set: ReadonlySet<A>): ReadonlySet<A> =>
    new Set(set).add(value);

/** `set` without `value`. */
export const excluding =
  <A>(value: A) =>
  (set: ReadonlySet<A>): ReadonlySet<A> => {
    const next = new Set(set);
    next.delete(value);
    return next;
  };

/** `map` without `key`. */
export const withoutKey =
  <K>(key: K) =>
  <V>(map: ReadonlyMap<K, V>): ReadonlyMap<K, V> => {
    const next = new Map(map);
    next.delete(key);
    return next;
  };
