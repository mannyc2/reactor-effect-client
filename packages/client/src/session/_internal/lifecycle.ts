import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Queue from "effect/Queue";
import * as Scope from "effect/Scope";
import type { IceCandidate, Mapping } from "../../contract.js";
import { ReactorError } from "../../errors.js";
import type { MediaTrack, Peer } from "../../PeerTypes.js";
import type { ReadyState, Status } from "../../SessionTypes.js";

const transitions: Record<Status, readonly Status[]> = {
  idle: ["connecting", "closing"],
  connecting: ["waiting", "disconnected", "closing"],
  waiting: ["ready", "disconnected", "closing"],
  ready: ["connecting", "disconnected", "closing"],
  disconnected: ["connecting", "closing"],
  closing: ["closed"],
  closed: [],
};

/** Resources and protocol facts belonging to exactly one peer generation. */
export class Connection {
  // Queue creation is synchronous and allocates no asynchronous work.
  readonly iceWake = Effect.runSync(Queue.dropping<void, ReactorError>(1));
  readonly ready = Deferred.makeUnsafe<void, ReactorError>();
  readonly failed = Deferred.makeUnsafe<never, ReactorError>();
  readonly iceBuffer: IceCandidate[] = [];
  iceBytes = 0;
  iceDone = false;
  finalSent = false;
  peerConnected = false;
  controlOpen = false;
  dataOpen = false;
  connectionId?: number;
  failure?: ReactorError;
  mapping: readonly Mapping[] = [];
  negotiated?: ReadyState["remote"];
  readonly paused = new Set<string>();
  readonly claimed = new Set<string>();
  readonly sending = new Map<string, MediaTrack>();
  readonly pendingClaims = new Map<string, string>();
  readonly trackBusy = new Set<string>();

  constructor(
    readonly generation: bigint,
    readonly scope: Scope.Closeable,
    readonly peer: Peer,
  ) {}

  readyGate(): void {
    if (this.peerConnected && this.controlOpen && this.dataOpen && this.failure === undefined)
      Deferred.doneUnsafe(this.ready, Effect.void);
  }
}

export interface ReadyConnection extends Connection {
  readonly negotiated: ReadyState["remote"];
}

const isNegotiated = (connection: Connection): connection is ReadyConnection =>
  connection.negotiated !== undefined;

/** The phase and generation authority. Wire correlation remains in Session. */
export class SessionLifecycle {
  private phase: Status = "idle";
  private revision = 0n;
  private active: Connection | undefined;
  private error: ReactorError | undefined;
  readonly scope = Scope.makeUnsafe();
  readonly closing = Deferred.makeUnsafe<never, ReactorError>();

  constructor(private readonly onStatus: (status: Status) => void) {}

  get status(): Status {
    return this.phase;
  }

  get generation(): bigint {
    return this.revision;
  }

  get connection(): Connection | undefined {
    return this.active;
  }

  get lastError(): ReactorError | undefined {
    return this.error;
  }

  get isClosing(): boolean {
    return this.phase === "closing" || this.phase === "closed";
  }

  transition(status: Status): void {
    if (this.phase === status) return;
    if (!transitions[this.phase].includes(status))
      throw ReactorError.fromCode("InvalidState", `illegal transition ${this.phase} -> ${status}`);
    this.phase = status;
    this.onStatus(status);
  }

  /** Called synchronously with Session's sampling reset and connecting event. */
  begin(reconnect: boolean, hasKnownRemote: boolean, makePeer: () => Peer): Connection {
    if (reconnect ? this.phase !== "ready" && this.phase !== "disconnected" : this.phase !== "idle")
      throw ReactorError.fromCode(
        "InvalidState",
        `${reconnect ? "reconnect" : "connect"} while ${this.phase}`,
      );
    if (reconnect && !hasKnownRemote)
      throw ReactorError.fromCode("InvalidState", "cannot reconnect without a known session");
    const generation = ++this.revision,
      scope = Scope.forkUnsafe(this.scope),
      peer = makePeer();
    const connection = new Connection(generation, scope, peer);
    this.active = connection;
    this.error = undefined;
    return connection;
  }

  accepts(connection: Connection): boolean {
    return this.active === connection && connection.failure === undefined && !this.isClosing;
  }

  assertCurrent(connection: Connection): void {
    // Preserve the actual failure even when close or reconnect also retired it.
    if (connection.failure !== undefined) throw connection.failure;
    if (this.active !== connection || this.isClosing)
      throw ReactorError.fromCode("Aborted", "retired connection generation", {
        generation: connection.generation,
      });
  }

  currentReady(): ReadyConnection {
    const connection = this.active;
    if (this.isClosing)
      throw ReactorError.fromCode("Closed", "session is closed", { outcome: "not-submitted" });
    if (this.phase !== "ready" || connection === undefined)
      throw ReactorError.fromCode("InvalidState", `operation requires ready, not ${this.phase}`, {
        outcome: "not-submitted",
      });
    this.assertCurrent(connection);
    if (!isNegotiated(connection)) throw new Error("ready connection has no negotiated descriptor");
    return connection;
  }

  /** Retiring an old peer must not disconnect its replacement or overwrite its error. */
  disconnect(connection: Connection, error: ReactorError): boolean {
    if (this.active !== connection || this.isClosing) return false;
    this.error = error;
    this.transition("disconnected");
    return true;
  }
}
