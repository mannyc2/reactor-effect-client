/**
 * The session implementation behind `Reactor.create` and `Reactor.attach`,
 * composed from its parts in `internal/session/`: the live generation,
 * inbound events, trickled ICE, requests, tracks, uploads and connection.
 *
 * State lives in a `SubscriptionRef` (the session) and a `Ref` per connection
 * generation. Each generation owns a scope, a peer and one fiber that applies
 * the peer's events in order; retiring a generation closes its scope, so
 * nothing it started outlives it. Commands are correlated before they are
 * sent, and their failures carry the dispatch evidence.
 */
import * as Cause from "effect/Cause";
import * as Clock from "effect/Clock";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Ref from "effect/Ref";
import * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import * as SubscriptionRef from "effect/SubscriptionRef";
import type { Coordinator, Termination, Tokens } from "../Coordinator.js";
import { notTerminated, terminationAttributes } from "../Coordinator.js";
import type { PeerFactory } from "../Peer.js";
import { ReactorError, summarize } from "../ReactorError.js";
import type {
  CloseReport,
  CommandReply,
  ControlMessage,
  EventPayload,
  Observation,
  ObserveOptions,
  Session,
  SessionEvent,
  Snapshot,
  Status,
} from "../Session.js";
import * as Correlator from "./correlator.js";
import * as Hub from "./hub.js";
import * as Connect from "./session/connect.js";
import * as Generation from "./session/generation.js";
import * as Ice from "./session/ice.js";
import * as Inbound from "./session/inbound.js";
import type { Core, Intent, RemoteSession, Settings, State } from "./session/model.js";
import { isClosing, isKnown, transitions } from "./session/model.js";
import * as Requests from "./session/requests.js";
import * as Tracks from "./session/tracks.js";
import * as Upload from "./session/upload.js";
import * as Stats from "./stats.js";
import * as Token from "./token.js";

export type { Intent, Settings } from "./session/model.js";

export interface Handle {
  /** The session's public value, once its id is known. */
  readonly session: (id: string) => Session;
  /** Allocates, or identifies the attached session, without connecting. */
  readonly allocate: Effect.Effect<string, ReactorError>;
  readonly connect: Effect.Effect<void, ReactorError>;
  readonly close: Effect.Effect<CloseReport>;
}

/** What a close report says of the remote session. */
const allocationOf = (remote: RemoteSession | undefined): CloseReport["allocation"] => {
  if (remote === undefined) return "none";
  return isKnown(remote) ? "known" : "unknown";
};

/** The remote session as a snapshot shows it outside a ready connection. */
const remoteOf = (remote: RemoteSession) =>
  isKnown(remote)
    ? {
        ownership: remote.ownership,
        sessionId: remote.id,
        ...(remote.descriptor === undefined ? {} : { descriptor: remote.descriptor }),
        ...(remote.connectionId === undefined ? {} : { connectionId: remote.connectionId }),
      }
    : { ownership: remote.ownership };

export const make = Effect.fnUntraced(function* (input: {
  readonly intent: Intent;
  readonly coordinator: Coordinator["Service"];
  /** Where the session's tokens come from; none sends no credential. */
  readonly tokens: (Pick<Tokens, "bind"> & Partial<Pick<Tokens, "create">>) | undefined;
  /** Resume receive-only tracks as each connection becomes ready; Reactor sends no media until then. */
  readonly resumeTracks: boolean;
  readonly peers: PeerFactory["Service"];
  readonly settings: Settings;
}) {
  const { intent, coordinator, settings } = input;
  const root = yield* Scope.make();
  const state = yield* SubscriptionRef.make<State>({
    status: "idle",
    moderated: false,
    reconnects: false,
    reconnecting: false,
    generation: 0n,
    remote: undefined,
    connection: undefined,
    lastError: undefined,
    close: undefined,
    received: new Set(),
    bitrates: new Map(),
    sampler: Stats.initialSampler,
  });
  const sequence = yield* Ref.make(0n);
  const hub = yield* Hub.make<SessionEvent>();
  const data = yield* Correlator.make<CommandReply>({
    prefix: "data",
    limit: settings.maxPending,
    namespace: settings.namespace,
  });
  const control = yield* Correlator.make<ControlMessage>({
    prefix: "ctrl",
    limit: settings.maxPending,
    namespace: settings.namespace,
  });
  const closing = yield* Deferred.make<never, ReactorError>();
  const closed = yield* Deferred.make<CloseReport>();
  const closeStarted = yield* Ref.make(false);

  const publish = Effect.fnUntraced(function* (payload: EventPayload, generation?: bigint) {
    const next = yield* Ref.updateAndGet(sequence, (value) => value + 1n);
    const current = generation ?? (yield* SubscriptionRef.get(state)).generation;
    yield* hub.publish({ ...payload, sequence: next, generation: current });
  });
  const transition = Effect.fnUntraced(function* (status: Status) {
    const from = yield* SubscriptionRef.modify(state, (current) =>
      current.status !== status && transitions[current.status].includes(status)
        ? ([
            current.status,
            {
              ...current,
              status,
              // A ready connection, or the close, ends the session's own reconnect.
              reconnecting: current.reconnecting && status !== "ready" && status !== "closing",
            },
          ] as const)
        : ([current.status, current] as const),
    );
    if (from === status) return true;
    if (!transitions[from].includes(status)) {
      // Once the session closes only its close moves it on: an attempt that raced it is told so.
      if (isClosing(from)) return false;
      return yield* Effect.die(`illegal session transition ${from} -> ${status}`);
    }
    yield* publish({ _tag: "Status", status });
    return true;
  });
  const token = yield* Token.make({
    tokens: input.tokens,
    sessionId: intent._tag === "Attach" ? intent.sessionId : undefined,
    onRefreshFailure: (error) => publish({ _tag: "Diagnostic", error }),
  });
  const signaling = coordinator.signaling(token.current);

  const core: Core = {
    intent,
    settings,
    signaling,
    state,
    sequence,
    hub,
    data,
    control,
    publish,
    transition,
  };
  const generation = Generation.make(core);
  const ice = Ice.make({ core, generation });
  const inbound = Inbound.make({ core, generation, ice });
  const requests = Requests.make({ core, generation, apiUrl: coordinator.apiUrl });
  const tracks = Tracks.make({ core, generation, requests });
  const upload = Upload.make({ core, generation, requests });
  const connect = Connect.make({
    core,
    generation,
    ice,
    inbound,
    requests,
    tracks,
    peers: input.peers,
    root,
    token,
    closing,
    resumeTracks: input.resumeTracks,
  });

  const terminateOwned = (remote: RemoteSession | undefined): Effect.Effect<Termination> =>
    isKnown(remote) && remote.ownership === "owned"
      ? signaling.terminate(remote.id)
      : Effect.succeed(notTerminated);

  const shutdown = Effect.gen(function* () {
    yield* transition("closing");
    yield* Deferred.fail(closing, ReactorError.fromCode("Closed", "session closing"));
    const connection = (yield* SubscriptionRef.get(state)).connection;
    const { submitted, errors } = yield* tracks.releasePublications(connection);
    // The host's own failure is the evidence; its message says what did not
    // finish. Anything else is reported as an opaque shutdown. A host effect
    // that cannot carry a typed error dies with it.
    const keep = (cleanup: Exit.Exit<void>, what: string) => {
      if (Exit.isSuccess(cleanup)) return;
      const failure = Cause.squash(cleanup.cause);
      errors.push(
        ReactorError.is(failure)
          ? failure
          : ReactorError.fromCode("Shutdown", `${what} did not complete cleanly`, {
              detail: cleanup.cause,
            }),
      );
    };
    if (connection !== undefined)
      keep(
        yield* generation.failUnreported(
          connection,
          ReactorError.fromCode("Aborted", "session closed"),
        ),
        "connection teardown",
      );
    keep(yield* Effect.exit(Scope.close(root, Exit.void)), "local cleanup");
    // Ownership is read after local work has joined: an interrupted
    // allocation may have changed its evidence meanwhile.
    const remote = (yield* SubscriptionRef.get(state)).remote;
    const termination = yield* terminateOwned(remote);
    const unresolved =
      connection === undefined ? [] : [...(yield* Ref.get(connection.link)).claims.values()];
    const report: CloseReport = {
      localClosed: errors.length === 0,
      allocation: allocationOf(remote),
      ...(isKnown(remote) ? { ownership: remote.ownership, sessionId: remote.id } : {}),
      remote: termination,
      unpublishSubmitted: submitted,
      unresolvedPublications: unresolved,
      localErrors: errors.map(summarize),
    };
    yield* SubscriptionRef.update(state, (current): State => ({ ...current, close: report }));
    yield* transition("closed");
    yield* hub.end;
    yield* Deferred.succeed(closed, report);
    yield* Effect.annotateCurrentSpan({
      "reactor.close.local_closed": report.localClosed,
      "reactor.close.allocation": report.allocation,
      ...terminationAttributes(report.remote),
    });
    return report;
  }).pipe(Effect.withSpan("Session.close", { kind: "client" }, { captureStackTrace: false }));

  const close: Effect.Effect<CloseReport> = Effect.uninterruptible(
    Ref.getAndSet(closeStarted, true).pipe(
      Effect.flatMap((started) => (started ? Deferred.await(closed) : shutdown)),
    ),
  );

  /** `session` as its readers see it, with what its connection and queues hold now. */
  const snapshotOf = Effect.fnUntraced(function* (session: State): Effect.fn.Return<Snapshot> {
    const link =
      session.connection === undefined ? undefined : yield* Ref.get(session.connection.link);
    const details = {
      generation: session.generation,
      reconnecting: session.reconnecting,
      pending: { data: yield* data.size, control: yield* control.size },
      pausedLocally: [...(link?.paused ?? [])],
      claimedTracks: [...(link?.claimed ?? [])],
      receivedTracks: [...session.received],
      unresolvedPublications: [...(link?.claims.values() ?? [])],
      observationOverflows: yield* hub.overflows,
      subscribers: yield* hub.observers,
      ...(session.lastError === undefined ? {} : { lastError: session.lastError }),
      ...(session.close === undefined ? {} : { close: session.close }),
    };
    if (session.status === "ready" && link?.negotiated !== undefined)
      return { ...details, status: "ready", remote: link.negotiated };
    return {
      ...details,
      status: session.status === "ready" ? "waiting" : session.status,
      ...(session.remote === undefined ? {} : { remote: remoteOf(session.remote) }),
    };
  });
  const snapshot: Effect.Effect<Snapshot> = Effect.flatMap(SubscriptionRef.get(state), snapshotOf);

  const observe = Effect.fnUntraced(function* (
    options: ObserveOptions = {},
  ): Effect.fn.Return<Observation, ReactorError, Scope.Scope> {
    // Subscribe first, so nothing falls between the state and its events.
    const events = yield* hub.subscribe(options.capacity);
    const revision = yield* Ref.get(sequence);
    const initial = yield* snapshot;
    return {
      initial,
      revision,
      events: Stream.filter(events, (event) => event.sequence > revision),
    };
  });

  const stats = Effect.gen(function* () {
    const { c } = yield* generation.currentReady;
    const raw = yield* generation.guard(c, c.peer.stats);
    if (raw.length > 4096)
      return yield* ReactorError.fromCode("Protocol", "statistics exceed their bound");
    const atMs = Number(yield* Clock.monotonicTimeNanos) / 1_000_000;
    return yield* SubscriptionRef.modify(state, (current) => {
      const [statistics, sampler] = Stats.sample({
        state: current.sampler,
        raw,
        generation: c.generation,
        atMs,
      });
      return [statistics, { ...current, sampler }] as const;
    });
  });

  const session = (id: string): Session => ({
    id,
    ownership: intent._tag === "Create" || intent.adopt ? "owned" : "attached",
    snapshot,
    // Each change as it was, so a reader that falls behind still sees every status.
    changes: SubscriptionRef.changes(state).pipe(Stream.mapEffect(snapshotOf)),
    ready: Effect.map(generation.currentReady, ({ c, negotiated }) => ({
      status: "ready",
      generation: c.generation,
      remote: negotiated,
    })),
    events: (options) => Stream.unwrap(hub.subscribe(options?.capacity)),
    observe,
    command: requests.command,
    schema: requests.schema,
    upload,
    requestRecordingClip: requests.requestRecordingClip,
    recording: requests.recording,
    stats,
    decoded: tracks.decoded,
    tracks: tracks.tracks,
    reconnect: connect.reconnect,
    close,
  });

  return {
    session,
    allocate: connect.allocate,
    connect: Effect.andThen(connect.connect, connect.arm),
    close,
  } satisfies Handle;
});
