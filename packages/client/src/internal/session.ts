/**
 * The session implementation behind `Reactor.create` and `Reactor.attach`.
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
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Predicate from "effect/Predicate";
import * as Queue from "effect/Queue";
import * as Redacted from "effect/Redacted";
import * as Ref from "effect/Ref";
import * as Schedule from "effect/Schedule";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import * as SubscriptionRef from "effect/SubscriptionRef";
import type { MessageInitShape } from "@bufbuild/protobuf";
import type {
  Coordinator,
  Descriptor,
  IceCandidate,
  Mapping,
  Termination,
  Tokens,
  Track,
} from "../Coordinator.js";
import { notTerminated, terminationAttributes } from "../Coordinator.js";
import type { DecodedMedia, TrackMedia } from "../Media.js";
import type { MediaTrack, Peer, PeerEvent, PeerFactory } from "../Peer.js";
import type { CommandContext } from "../ReactorError.js";
import {
  CommandFailure,
  IceFailed,
  ReactorError,
  Remote,
  summarize,
  TransportFailed,
} from "../ReactorError.js";
import { ClipReady } from "../Session.js";
import type {
  CloseReport,
  CommandOptions,
  ControlMessage,
  CommandReply,
  EventPayload,
  Observation,
  ObserveOptions,
  ReadyDescriptor,
  ReadyState,
  Session,
  SessionEvent,
  Snapshot,
  Status,
  UploadOptions,
  UploadProgress,
  Uploaded,
  UploadReference,
} from "../Session.js";
import * as Correlator from "./correlator.js";
import * as Hub from "./hub.js";
import { take } from "./queue.js";
import * as Stats from "./stats.js";
import * as Token from "./token.js";
import * as Wire from "./wire.js";

export interface Settings {
  readonly replyTimeout: Duration.Duration;
  readonly uploadTimeout: Duration.Duration;
  readonly connectTimeout: Duration.Duration;
  /** A reconnect's own deadline: Reactor ends a session 30 s after it loses its last connection. */
  readonly reconnectTimeout: Duration.Duration;
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

interface Known {
  readonly ownership: "owned" | "attached";
  readonly id: string;
  readonly descriptor?: Descriptor | undefined;
  readonly connectionId?: number | undefined;
}
type RemoteSession = { readonly ownership: "allocating" | "unknown" } | Known;
const isKnown = (remote: RemoteSession | undefined): remote is Known =>
  remote?.ownership === "owned" || remote?.ownership === "attached";

/** What one connection generation has learned and holds. */
interface Link {
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

interface Connection {
  readonly generation: bigint;
  readonly scope: Scope.Closeable;
  readonly peer: Peer;
  readonly ready: Deferred.Deferred<void, ReactorError>;
  readonly failed: Deferred.Deferred<never, ReactorError>;
  readonly events: Queue.Queue<PeerEvent>;
  readonly iceWake: Queue.Queue<void>;
  readonly link: Ref.Ref<Link>;
}

interface State {
  readonly status: Status;
  /** Content moderation is ending the session: nothing reconnects it. */
  readonly moderated: boolean;
  readonly generation: bigint;
  readonly remote: RemoteSession | undefined;
  readonly connection: Connection | undefined;
  readonly lastError: ReactorError | undefined;
  readonly close: CloseReport | undefined;
  readonly received: ReadonlySet<string>;
  readonly bitrates: ReadonlyMap<string, number>;
  readonly sampler: Stats.SamplerState;
}

const transitions: Record<Status, ReadonlyArray<Status>> = {
  idle: ["connecting", "closing"],
  connecting: ["waiting", "disconnected", "closing"],
  waiting: ["ready", "disconnected", "closing"],
  ready: ["connecting", "disconnected", "closing"],
  disconnected: ["connecting", "closing"],
  closing: ["closed"],
  closed: [],
};

const isClosing = (status: Status): boolean => status === "closing" || status === "closed";

type ControlPayload = Exclude<
  MessageInitShape<typeof Wire.ControlClientMessageSchema>["payload"],
  { readonly case: undefined } | undefined
>;
type ControlPayloadIn = Exclude<
  Wire.ControlServerMessage["payload"],
  { readonly case: undefined | "moderation" }
>;

/** A control message as the library reads it; provider text stays redacted. */
const controlMessage = (payload: ControlPayloadIn): Effect.Effect<ControlMessage, ReactorError> => {
  switch (payload.case) {
    case "modelSchema": {
      const openapi = payload.value.openapi;
      return openapi === undefined
        ? Effect.succeed({ _tag: "ModelSchema" })
        : Effect.map(Wire.json(openapi), (json) => ({ _tag: "ModelSchema", openapi: json }));
    }
    case "clipReady":
      return Schema.decodeEffect(ClipReady)(payload.value).pipe(
        Effect.map((clip) => ({ _tag: "ClipReady", clip }) as const),
        Effect.mapError((cause) =>
          ReactorError.fromCode("Protocol", "invalid clip_ready", { detail: cause }),
        ),
      );
    case "clipFailed":
      return Effect.succeed({ _tag: "ClipFailed", reason: Redacted.make(payload.value.reason) });
    case "publishTrack":
      return Effect.succeed({ _tag: "TrackPublished", name: payload.value.name });
    case "error":
      return Effect.succeed({
        _tag: "Error",
        code: Redacted.make(payload.value.code),
        message: Redacted.make(payload.value.message),
      });
  }
};

const UploadReference = Schema.Struct({
  uploadId: Schema.NonEmptyString,
  name: Schema.NonEmptyString,
  mimeType: Schema.NonEmptyString,
  size: Schema.BigInt.check(
    Schema.isGreaterThanOrEqualToBigInt(0n),
    Schema.isLessThanBigInt(1n << 63n),
  ),
});
const CommandInput = Schema.Struct({
  name: Schema.NonEmptyString,
  data: Wire.StructJson,
  uploads: Schema.ReadonlyMap(Schema.NonEmptyString, UploadReference).check(
    Schema.makeFilter((uploads) => uploads.size <= 128 || "at most 128 upload references"),
  ),
});

const unsupported = (message: string) =>
  ReactorError.fromCode("UnsupportedCapability", message, { outcome: "not-submitted" });

const deadline = <A, R>(
  effect: Effect.Effect<A, ReactorError, R>,
  duration: Duration.Duration,
  operation: string,
): Effect.Effect<A, ReactorError, R> =>
  Effect.timeoutOrElse(effect, {
    duration,
    orElse: () => Effect.fail(ReactorError.fromCode("Timeout", `${operation}: deadline`)),
  });

/**
 * Why a connection failed, from its statistics: a candidate pair that
 * succeeded or was nominated means ICE worked and the DTLS or SCTP transport
 * above it failed; otherwise no pair worked.
 */
const connectionFailure = (stats: ReadonlyArray<unknown>, generation: bigint): ReactorError => {
  const entries = stats.filter(Predicate.isObject);
  const pairs = entries.filter((entry) => entry.type === "candidate-pair");
  if (pairs.some((pair) => pair.state === "succeeded" || pair.nominated === true))
    return ReactorError.make({
      reason: TransportFailed.make({
        message: "peer failed after ICE connectivity succeeded",
        pairs: pairs.length,
      }),
      context: { generation },
    });
  const candidateTypes = new Set(
    entries
      .filter((entry) => entry.type === "local-candidate")
      .map((entry) => entry.candidateType)
      .filter(Predicate.isString),
  );
  return ReactorError.make({
    reason: IceFailed.make({
      message: "peer found no working ICE candidate pair",
      pairs: pairs.length,
      candidateTypes: [...candidateTypes],
    }),
    context: { generation },
  });
};

/** Marks a connect phase on the current span, as SqlClient marks a transaction's. */
const phase = (name: string): Effect.Effect<void> =>
  Effect.currentSpan.pipe(
    Effect.flatMap((span) =>
      Clock.currentTimeNanos.pipe(Effect.map((now) => span.event(`reactor.connect.${name}`, now))),
    ),
    Effect.ignore,
  );

export interface Handle {
  /** The session's public value, once its id is known. */
  readonly session: (id: string) => Session;
  /** Allocates, or identifies the attached session, without connecting. */
  readonly allocate: Effect.Effect<string, ReactorError>;
  readonly connect: Effect.Effect<void, ReactorError>;
  readonly close: Effect.Effect<CloseReport>;
}

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
  const { intent, coordinator, peers, settings } = input;
  const root = yield* Scope.make();
  const state = yield* SubscriptionRef.make<State>({
    status: "idle",
    moderated: false,
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

  const publish = (payload: EventPayload, generation?: bigint) =>
    Effect.gen(function* () {
      const next = yield* Ref.updateAndGet(sequence, (value) => value + 1n);
      const current = generation ?? (yield* SubscriptionRef.get(state)).generation;
      yield* hub.publish({ ...payload, sequence: next, generation: current });
    });
  const token = yield* Token.make({
    tokens: input.tokens,
    sessionId: intent._tag === "Attach" ? intent.sessionId : undefined,
    onRefreshFailure: (error) => publish({ _tag: "Diagnostic", error }),
  });
  const signaling = coordinator.signaling(token.current);

  const transition = (status: Status) =>
    Effect.gen(function* () {
      const from = yield* SubscriptionRef.modify(state, (current) =>
        current.status !== status && transitions[current.status].includes(status)
          ? ([current.status, { ...current, status }] as const)
          : ([current.status, current] as const),
      );
      if (from === status) return;
      if (!transitions[from].includes(status))
        return yield* Effect.die(`illegal session transition ${from} -> ${status}`);
      yield* publish({ _tag: "Status", status });
    });

  /** Adds what a known remote session has learned; an unknown one stays as it is. */
  const learn = (patch: Partial<Omit<Known, "ownership" | "id">>) =>
    SubscriptionRef.update(state, (current): State => ({
      ...current,
      remote: isKnown(current.remote) ? { ...current.remote, ...patch } : current.remote,
    }));

  /** An allocation interrupted or failed without a verdict leaves an unknown session. */
  const allocationUnknown = SubscriptionRef.update(state, (current): State => ({
    ...current,
    remote: current.remote?.ownership === "allocating" ? { ownership: "unknown" } : current.remote,
  }));

  /** Fails when `c` is not the live generation, preserving its own failure. */
  const current = (c: Connection): Effect.Effect<void, ReactorError> =>
    Effect.gen(function* () {
      const link = yield* Ref.get(c.link);
      if (link.failure !== undefined) return yield* link.failure;
      const session = yield* SubscriptionRef.get(state);
      if (session.connection !== c || isClosing(session.status))
        return yield* ReactorError.fromCode("Aborted", "retired connection generation", {
          generation: c.generation,
        });
    });

  const currentReady = Effect.gen(function* () {
    const session = yield* SubscriptionRef.get(state);
    if (isClosing(session.status))
      return yield* ReactorError.fromCode("Closed", "session is closed", {
        outcome: "not-submitted",
      });
    const c = session.connection;
    if (session.status !== "ready" || c === undefined)
      return yield* ReactorError.fromCode(
        "InvalidState",
        `operation requires ready, not ${session.status}`,
        { outcome: "not-submitted" },
      );
    yield* current(c);
    const negotiated = (yield* Ref.get(c.link)).negotiated;
    if (negotiated === undefined) return yield* Effect.die("ready connection has no negotiation");
    return { c, negotiated };
  });

  /** Runs `effect` for `c`, failing with `c`'s failure if it retires meanwhile. */
  const guard = <A>(c: Connection, effect: Effect.Effect<A, ReactorError>) =>
    current(c).pipe(
      Effect.andThen(effect),
      Effect.raceFirst(Deferred.await(c.failed)),
      Effect.tap(() => current(c)),
      Effect.catch((error) =>
        Ref.get(c.link).pipe(Effect.flatMap((link) => Effect.fail(link.failure ?? error))),
      ),
    );

  const fail = (c: Connection, error: ReactorError): Effect.Effect<void> =>
    Effect.gen(function* () {
      const first = yield* Ref.modify(c.link, (link) =>
        link.failure === undefined
          ? ([link, { ...link, failure: error }] as const)
          : ([undefined, link] as const),
      );
      if (first === undefined) return;
      yield* Deferred.fail(c.failed, error);
      yield* Deferred.fail(c.ready, error);
      yield* data.failGeneration(c.generation, error);
      yield* control.failGeneration(c.generation, error);
      yield* c.peer.close;
      yield* Effect.sync(() => {
        for (const track of first.sending.values()) track.stop();
      });
      yield* Ref.update(c.link, (link): Link => ({
        ...link,
        sending: new Map(),
        ice: [],
        iceBytes: 0,
        claimed: new Set(),
        paused: new Set(),
      }));
      const disconnected = yield* SubscriptionRef.modify(state, (session) => {
        if (session.connection !== c || isClosing(session.status)) return [false, session] as const;
        return [
          true,
          {
            ...session,
            received: new Set<string>(),
            lastError: error,
            status: "disconnected" as const,
          },
        ] as const;
      });
      if (disconnected) {
        yield* publish({ _tag: "Status", status: "disconnected" }, c.generation);
        yield* publish({ _tag: "Diagnostic", error }, c.generation);
      }
    });

  /** A task of `c`'s whose failure fails `c`; a defect is a bug and stays one. */
  const background = (c: Connection, body: Effect.Effect<void, ReactorError>) =>
    body.pipe(
      Effect.raceFirst(Deferred.await(c.failed)),
      Effect.catch((error) => fail(c, error)),
      Effect.forkIn(c.scope),
      Effect.asVoid,
    );

  const readyGate = (c: Connection) =>
    Ref.get(c.link).pipe(
      Effect.flatMap((link) =>
        link.peerConnected && link.controlOpen && link.dataOpen && link.failure === undefined
          ? Deferred.succeed(c.ready, undefined)
          : Effect.void,
      ),
      Effect.asVoid,
    );

  const remoteError = (value: { readonly code: string; readonly message: string }) =>
    Remote.make({
      _tag: "Remote",
      message: "remote command error",
      remoteCode: Redacted.make(value.code),
      body: Redacted.make(value.message),
    });

  const receiveData = (c: Connection, bytes: Uint8Array) =>
    Effect.gen(function* () {
      const message = yield* Wire.decode(Wire.DataServerMessageSchema, bytes);
      const payload = message.payload;
      if (payload.case === "error") {
        const error = ReactorError.make({
          reason: remoteError(payload.value),
          context: {
            requestId: message.requestId,
            generation: c.generation,
            outcome: "replied",
            detail: Redacted.make(message),
          },
        });
        yield* data.settle(message.requestId, c.generation, (correlation) =>
          publish(
            { _tag: "CommandError", requestId: message.requestId, error, correlation },
            c.generation,
          ).pipe(Effect.andThen(Effect.fail(error))),
        );
        return;
      }
      const body =
        payload.case === undefined
          ? { kind: "ack" as const }
          : {
              kind: "message" as const,
              type: payload.value.type,
              ...(payload.value.data === undefined
                ? {}
                : { data: yield* Wire.json(payload.value.data) }),
            };
      yield* data.settle(
        message.requestId,
        c.generation,
        (correlation) =>
          Effect.gen(function* () {
            // The exact value that completes the request is the one observers see.
            const reply: CommandReply = {
              ...body,
              _tag: "Model",
              outcome: "replied",
              requestId: message.requestId,
              generation: c.generation,
              sequence: yield* Ref.updateAndGet(sequence, (value) => value + 1n),
              correlation,
            };
            yield* hub.publish(reply);
            return reply;
          }),
        body.kind === "ack" ? "acknowledged" : "replied",
      );
    });

  const receiveControl = (c: Connection, bytes: Uint8Array) =>
    Effect.gen(function* () {
      const message = yield* Wire.decode(Wire.ControlServerMessageSchema, bytes);
      const payload = message.payload;
      // Unlike a data reply, a bodyless control message acknowledges nothing.
      if (payload.case === undefined)
        return yield* publish(
          {
            _tag: "Diagnostic",
            error: ReactorError.fromCode(
              "Protocol",
              "bodyless control response resolves no request",
              {
                requestId: message.requestId,
              },
            ),
          },
          c.generation,
        );
      // A moderation verdict answers no request. `terminate` means Reactor is ending the session.
      if (payload.case === "moderation") {
        const verdict = payload.value;
        if (verdict.action === "terminate")
          yield* SubscriptionRef.update(state, (current): State => ({
            ...current,
            moderated: true,
          }));
        return yield* publish(
          {
            _tag: "Moderation",
            action: verdict.action,
            categories: verdict.categories,
            ...(verdict.inputKind === "" ? {} : { inputKind: verdict.inputKind }),
            ...(verdict.command === "" ? {} : { command: verdict.command }),
            ...(message.requestId === "" ? {} : { requestId: message.requestId }),
          },
          c.generation,
        );
      }
      const claim = (yield* Ref.get(c.link)).claims.get(message.requestId);
      if (claim !== undefined) {
        if (payload.case === "publishTrack" && payload.value.name === claim)
          // Remote ownership is recorded even after the publisher stops waiting.
          yield* Ref.update(c.link, (link): Link => ({
            ...link,
            claimed: toggled(link.claimed, claim, true),
            claims: withoutKey(link.claims, message.requestId),
          }));
        else if (payload.case === "error")
          yield* Ref.update(c.link, (link): Link => ({
            ...link,
            claims: withoutKey(link.claims, message.requestId),
          }));
        else
          return yield* fail(
            c,
            ReactorError.fromCode("UnexpectedReply", "publisher claim reply named another track", {
              operation: "publish_track",
              requestId: message.requestId,
              outcome: "unknown",
            }),
          );
      }
      const read = yield* Effect.exit(controlMessage(payload));
      const result =
        payload.case === "error"
          ? Effect.fail(
              ReactorError.make({
                reason: remoteError(payload.value),
                context: {
                  requestId: message.requestId,
                  outcome: "replied",
                  detail: Redacted.make(message),
                },
              }),
            )
          : read;
      const correlation = yield* control.settle(message.requestId, c.generation, () => result);
      if (Exit.isFailure(read)) return yield* read;
      if (payload.case !== "error" || correlation !== "matched")
        yield* publish(
          { _tag: "Control", message: read.value, requestId: message.requestId, correlation },
          c.generation,
        );
    });

  /**
   * A failed connection, classified from one statistics read on the
   * generation's event fiber, so later events wait behind it and the
   * connection's scope owns the read. A read that fails, or outlasts 2 s on
   * the fiber's Clock, leaves it `Disconnected`.
   */
  const failedConnection = (c: Connection): Effect.Effect<ReactorError> =>
    deadline(c.peer.stats, Duration.seconds(2), "failure classification").pipe(
      Effect.map((stats) => connectionFailure(stats, c.generation)),
      Effect.catch((cause) =>
        Effect.succeed(
          ReactorError.fromCode("Disconnected", "peer state failed", {
            generation: c.generation,
            detail: cause,
          }),
        ),
      ),
    );

  const onPeer = (c: Connection, event: PeerEvent): Effect.Effect<void> =>
    Effect.gen(function* () {
      const link = yield* Ref.get(c.link);
      const session = yield* SubscriptionRef.get(state);
      if (link.failure !== undefined || session.connection !== c || isClosing(session.status))
        return;
      switch (event.type) {
        case "state":
          if (event.state === "failed") return yield* fail(c, yield* failedConnection(c));
          if (event.state === "disconnected" || event.state === "closed")
            return yield* fail(
              c,
              ReactorError.fromCode("Disconnected", `peer state ${event.state}`, {
                generation: c.generation,
              }),
            );
          yield* Ref.update(c.link, (current): Link => ({
            ...current,
            peerConnected: event.state === "connected",
          }));
          return yield* readyGate(c);
        case "channel":
          if (!event.open)
            return yield* fail(
              c,
              ReactorError.fromCode("ChannelClosed", `${event.channel} channel closed`, {
                generation: c.generation,
                detail: { channel: event.channel },
              }),
            );
          yield* Ref.update(c.link, (current) =>
            event.channel === "control"
              ? { ...current, controlOpen: true }
              : { ...current, dataOpen: true },
          );
          return yield* readyGate(c);
        case "ice": {
          const candidate = event.candidate;
          const overflow = yield* Ref.modify(c.link, (current) => {
            if (candidate === undefined) return [false, { ...current, iceDone: true }] as const;
            const iceBytes = current.iceBytes + candidate.candidate.length * 2;
            if (current.ice.length >= 256 || iceBytes > 262_144 || current.finalSent)
              return [true, current] as const;
            return [false, { ...current, ice: [...current.ice, candidate], iceBytes }] as const;
          });
          if (overflow)
            return yield* fail(
              c,
              ReactorError.fromCode(
                "Overflow",
                "ICE buffer bound or candidate after the final batch",
              ),
            );
          yield* Queue.offer(c.iceWake, undefined);
          return;
        }
        case "decoded":
        case "track":
          yield* SubscriptionRef.update(state, (current): State => ({
            ...current,
            received: toggled(current.received, event.name, true),
          }));
          return yield* publish(
            event.type === "decoded"
              ? { _tag: "Decoded", kind: event.kind, name: event.name, mid: event.mid }
              : { _tag: "Track", name: event.name, mid: event.mid },
            c.generation,
          );
        case "error":
          return yield* fail(c, event.error);
        case "message":
          return yield* (
            event.channel === "data" ? receiveData(c, event.bytes) : receiveControl(c, event.bytes)
          ).pipe(Effect.catch((error) => publish({ _tag: "Diagnostic", error }, c.generation)));
      }
    });

  const flushIce = (c: Connection) =>
    Effect.gen(function* () {
      yield* current(c);
      const session = yield* SubscriptionRef.get(state);
      const remote = session.remote;
      const connectionId = (yield* Ref.get(c.link)).connectionId;
      if (!isKnown(remote) || connectionId === undefined) return;
      while (true) {
        const batch = yield* Ref.modify(c.link, (link) => {
          const final = link.iceDone && !link.finalSent;
          if (link.ice.length === 0 && !final) return [undefined, link] as const;
          return [
            { candidates: link.ice, final },
            { ...link, ice: [], iceBytes: 0 },
          ] as const;
        });
        if (batch === undefined) return;
        yield* guard(c, signaling.ice(remote.id, connectionId, batch.candidates, batch.final));
        if (batch.final) yield* Ref.update(c.link, (link): Link => ({ ...link, finalSent: true }));
      }
    });

  const allocate: Effect.Effect<string, ReactorError> = Effect.uninterruptibleMask((restore) =>
    Effect.gen(function* () {
      const session = yield* SubscriptionRef.get(state);
      if (isClosing(session.status))
        return yield* ReactorError.fromCode("Closed", "session is closed", {
          outcome: "not-submitted",
        });
      if (isKnown(session.remote)) return session.remote.id;
      if (session.remote !== undefined)
        return yield* ReactorError.fromCode(
          "InvalidState",
          "session allocation is already pending or unresolved",
          { outcome: "unknown" },
        );
      if (intent._tag === "Attach") {
        const remote: Known = {
          ownership: intent.adopt ? "owned" : "attached",
          id: intent.sessionId,
          ...(intent.connectionId === undefined ? {} : { connectionId: intent.connectionId }),
        };
        yield* SubscriptionRef.update(state, (current): State => ({ ...current, remote }));
        return remote.id;
      }
      yield* SubscriptionRef.update(state, (current): State => ({
        ...current,
        remote: { ownership: "allocating" },
      }));
      const allocation = yield* restore(
        signaling
          .create(intent.model, intent.extraArgs)
          .pipe(Effect.raceFirst(Deferred.await(closing))),
      ).pipe(
        Effect.tapError((error) =>
          // A request never sent, or a refusal (4xx) the coordinator answered, proves nothing was
          // allocated; a server error, a timeout or a lost reply does not.
          SubscriptionRef.update(state, (current): State => ({
            ...current,
            remote:
              current.remote?.ownership !== "allocating"
                ? current.remote
                : error.context.outcome === "not-submitted" ||
                    (error.context.outcome === "replied" &&
                      error.reason._tag === "Http" &&
                      error.reason.status !== undefined &&
                      error.reason.status >= 400 &&
                      error.reason.status < 500)
                  ? undefined
                  : { ownership: "unknown" },
          })),
        ),
        Effect.onInterrupt(() => allocationUnknown),
      );
      // Ownership rests on the id alone: a reply that cannot describe the
      // session still names one its owner must terminate.
      yield* SubscriptionRef.update(state, (current): State => ({
        ...current,
        remote: { ownership: "owned", id: allocation.sessionId },
      }));
      yield* token.bind(allocation.sessionId);
      const descriptor = yield* signaling.describe(allocation);
      yield* SubscriptionRef.update(state, (current): State => ({
        ...current,
        remote: { ownership: "owned", id: allocation.sessionId, descriptor },
      }));
      return allocation.sessionId;
    }),
  );

  const begin = (reconnect: boolean) =>
    Effect.gen(function* () {
      const session = yield* SubscriptionRef.get(state);
      if (
        reconnect
          ? session.status !== "ready" && session.status !== "disconnected"
          : session.status !== "idle"
      )
        return yield* ReactorError.fromCode(
          "InvalidState",
          `${reconnect ? "reconnect" : "connect"} while ${session.status}`,
        );
      if (reconnect && !isKnown(session.remote))
        return yield* ReactorError.fromCode(
          "InvalidState",
          "cannot reconnect without a known session",
        );
      if (reconnect && session.moderated)
        return yield* ReactorError.fromCode("Moderated", "content moderation ended the session", {
          outcome: "not-submitted",
        });
      const scope = yield* Scope.fork(root);
      const peer = yield* Scope.provide(peers.make, scope).pipe(
        Effect.onError((cause) => Scope.close(scope, Exit.failCause(cause))),
      );
      const c: Connection = {
        generation: session.generation + 1n,
        scope,
        peer,
        ready: yield* Deferred.make<void, ReactorError>(),
        failed: yield* Deferred.make<never, ReactorError>(),
        events: yield* Queue.unbounded<PeerEvent>(),
        iceWake: yield* Queue.dropping<void>(1),
        link: yield* Ref.make<Link>({
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
        }),
      };
      const previous = session.connection;
      yield* SubscriptionRef.update(state, (current): State => ({
        ...current,
        generation: c.generation,
        connection: c,
        lastError: undefined,
        received: new Set(),
        sampler: Stats.initialSampler,
      }));
      yield* transition("connecting");
      yield* Scope.addFinalizer(
        scope,
        fail(
          c,
          ReactorError.fromCode("Aborted", "connection scope closed", { generation: c.generation }),
        ),
      );
      yield* take(c.events).pipe(
        Effect.flatMap((event) => onPeer(c, event)),
        Effect.forever,
        Effect.forkIn(scope),
      );
      if (previous !== undefined) {
        yield* fail(
          previous,
          ReactorError.fromCode("Disconnected", "connection retired for reconnect"),
        );
        yield* Scope.close(previous.scope, Exit.void);
      }
      return c;
    }).pipe(Effect.uninterruptible);

  const connectAttempt = (reconnect: boolean) =>
    Effect.gen(function* () {
      const c = yield* begin(reconnect);
      yield* Effect.annotateCurrentSpan("reactor.connection.generation", c.generation);
      const work = Effect.gen(function* () {
        if (!reconnect) yield* guard(c, allocate);
        yield* current(c);
        yield* transition("waiting");
        const known = (yield* SubscriptionRef.get(state)).remote;
        if (!isKnown(known))
          return yield* ReactorError.fromCode("InvalidState", "no known session id");
        yield* Effect.annotateCurrentSpan("reactor.session.id", known.id);
        const descriptor = yield* guard(
          c,
          signaling.ready(known.id, reconnect ? undefined : known.descriptor),
        );
        yield* learn({ descriptor });
        yield* phase("described");
        const capabilities = descriptor.capabilities;
        const transport = descriptor.selected_transport;
        if (capabilities === undefined || transport === undefined)
          return yield* ReactorError.fromCode(
            "Protocol",
            "missing ready capabilities or transport",
          );
        if (transport.protocol !== "webrtc" || transport.version !== "1.0")
          return yield* ReactorError.fromCode(
            "VersionMismatch",
            `unsupported transport ${transport.protocol}/${transport.version}`,
          );
        const ready: ReadyDescriptor = {
          ...descriptor,
          capabilities,
          selected_transport: transport,
        };
        const servers = yield* guard(c, signaling.iceServers(known.id));
        const prepared = yield* guard(
          c,
          Scope.provide(
            c.peer.prepare(servers, capabilities.tracks, (event) => {
              // The host boundary: hosts emit from platform callbacks; the
              // generation's fiber applies the events in order.
              Queue.offerUnsafe(c.events, event);
            }),
            c.scope,
          ),
        );
        yield* Ref.update(c.link, (link): Link => ({ ...link, mapping: prepared.mapping }));
        yield* phase("prepared");
        const previousId = known.connectionId;
        const connectionId = previousId ?? (yield* guard(c, signaling.register(known.id)));
        yield* Ref.update(c.link, (link): Link => ({ ...link, connectionId }));
        yield* learn({ connectionId });
        yield* phase("registered");
        // Registration, then buffered ICE (the empty final batch included), then offer, then answer.
        yield* flushIce(c);
        yield* background(c, take(c.iceWake).pipe(Effect.andThen(flushIce(c)), Effect.forever));
        yield* guard(
          c,
          signaling.offer(
            known.id,
            connectionId,
            prepared.sdp,
            prepared.mapping,
            reconnect && previousId !== undefined,
          ),
        );
        yield* phase("offered");
        const answer = yield* guard(c, signaling.answer(known.id, connectionId));
        const negotiatedId = answer.connection_id ?? connectionId;
        yield* Ref.update(c.link, (link): Link => ({ ...link, connectionId: negotiatedId }));
        yield* learn({ connectionId: negotiatedId });
        yield* guard(c, c.peer.answer(answer.sdp_answer));
        yield* phase("answered");
        yield* deadline(
          guard(c, Deferred.await(c.ready)),
          settings.readyTimeout,
          "peer and both channels ready",
        );
        yield* current(c);
        yield* phase("ready");
        yield* Ref.update(c.link, (link): Link => ({
          ...link,
          negotiated: {
            ownership: known.ownership,
            sessionId: known.id,
            descriptor: ready,
            connectionId: negotiatedId,
          },
        }));
        yield* transition("ready");
        // Hosted Reactor holds a connection's media until that connection
        // resumes its receive-only tracks, attached or not.
        for (const track of capabilities.tracks)
          if (track.direction === "recvonly" && input.resumeTracks) {
            const resumed = yield* Effect.result(setTrackActive(track.name, true, c));
            if (resumed._tag === "Failure")
              yield* publish({ _tag: "Diagnostic", error: resumed.failure }, c.generation);
          }
        for (const [name, bitrate] of (yield* SubscriptionRef.get(state)).bitrates) {
          const applied = yield* Effect.result(guard(c, c.peer.maxBitrate(name, bitrate)));
          if (applied._tag === "Failure")
            yield* publish({ _tag: "Diagnostic", error: applied.failure }, c.generation);
        }
        if (Duration.isFinite(settings.heartbeat))
          yield* background(
            c,
            notification(c, { case: "ping", value: {} }).pipe(
              Effect.repeat(Schedule.spaced(settings.heartbeat)),
              Effect.asVoid,
            ),
          );
      });
      yield* deadline(
        work,
        reconnect ? settings.reconnectTimeout : settings.connectTimeout,
        reconnect ? "reconnect" : "connect",
      ).pipe(
        Effect.onExit((exit) =>
          Exit.isSuccess(exit)
            ? Effect.void
            : Effect.gen(function* () {
                yield* allocationUnknown;
                yield* fail(
                  c,
                  failureOf(exit.cause, () =>
                    ReactorError.fromCode("Aborted", "connection attempt interrupted"),
                  ),
                );
                yield* Scope.close(c.scope, Exit.void);
              }),
        ),
      );
    }).pipe(
      Effect.withSpan(
        reconnect ? "reactor.session.reconnect" : "reactor.session.connect",
        { kind: "client" },
        { captureStackTrace: false },
      ),
    );

  const request = <A>(
    c: Connection,
    correlator: Correlator.Correlator<A>,
    operation: string,
    encode: (id: string) => Effect.Effect<Uint8Array<ArrayBuffer>, ReactorError>,
    channel: "control" | "data",
    wait: Duration.Duration,
    publication?: string,
  ): Effect.Effect<A, ReactorError> =>
    Effect.uninterruptibleMask((restore) =>
      Effect.gen(function* () {
        yield* current(c);
        const pending = yield* correlator.register(c.generation, operation);
        const forget = Effect.all([
          correlator.cancel(pending),
          Ref.update(c.link, (link): Link => ({
            ...link,
            claims: withoutKey(link.claims, pending.id),
          })),
        ]);
        if (publication !== undefined)
          yield* Ref.update(c.link, (link): Link => ({
            ...link,
            claims: new Map(link.claims).set(pending.id, publication),
          }));
        const encoded = yield* Effect.result(encode(pending.id));
        if (encoded._tag === "Failure") {
          yield* forget;
          return yield* ReactorError.make({
            reason: encoded.failure.reason,
            context: {
              ...encoded.failure.context,
              operation,
              requestId: pending.id,
              generation: c.generation,
              outcome: "not-submitted",
            },
          });
        }
        const attribute = (error: ReactorError) =>
          correlator.isSubmitted(pending).pipe(
            Effect.map((submitted) =>
              ReactorError.make({
                reason: error.reason,
                context: {
                  ...error.context,
                  operation,
                  requestId: pending.id,
                  generation: c.generation,
                  outcome: error.context.outcome ?? (submitted ? "unknown" : "not-submitted"),
                },
              }),
            ),
          );
        const sending = current(c).pipe(
          // The record exists before the peer could reply, however fast.
          Effect.andThen(correlator.submitted(pending)),
          Effect.andThen(c.peer.send(channel, encoded.success)),
          Effect.catch((error) =>
            Effect.gen(function* () {
              if (!(yield* correlator.isPending(pending))) return;
              if (error.context.outcome === "not-submitted") yield* forget;
              yield* Deferred.fail(pending.deferred, yield* attribute(error));
            }),
          ),
        );
        const execution = Effect.gen(function* () {
          // The reply can precede the transport's acknowledgement of the send.
          yield* Effect.forkIn(sending, yield* Effect.scope, { startImmediately: true });
          return yield* Deferred.await(pending.deferred);
        }).pipe(
          Effect.scoped,
          Effect.interruptible,
          (effect) => deadline(effect, wait, operation),
          Effect.catch((error) => Effect.flatMap(attribute(error), Effect.fail)),
          Effect.onExit((exit) =>
            Effect.gen(function* () {
              // A submitted request stays attributable until a late reply or its
              // generation retires; its slot is not released by the deadline.
              const submitted = yield* correlator.isSubmitted(pending);
              if (!submitted) yield* forget;
              else if (Exit.isFailure(exit)) yield* correlator.abandon(pending);
              // The span covers the owned execution, which ends with the request's
              // own outcome even after its caller stops waiting.
              const error = Exit.findError(exit);
              yield* Effect.annotateCurrentSpan(
                Exit.isSuccess(exit)
                  ? { "reactor.command.outcome": "replied" }
                  : error._tag === "Success"
                    ? {
                        "reactor.command.outcome": error.success.context.outcome,
                        "error.type": error.success.reason._tag,
                      }
                    : { "reactor.command.outcome": submitted ? "unknown" : "not-submitted" },
              );
            }),
          ),
          Effect.withSpan(
            channel === "data" ? "reactor.session.command" : "reactor.session.control",
            {
              kind: "client",
              attributes: {
                "reactor.operation": operation,
                "reactor.request.id": pending.id,
                "reactor.connection.generation": c.generation,
              },
            },
            { captureStackTrace: false },
          ),
        );
        const owner = yield* Effect.forkIn(execution, c.scope, { startImmediately: true });
        return yield* restore(Fiber.join(owner));
      }),
    );

  const command = (
    name: string,
    input: unknown,
    options: CommandOptions = {},
  ): Effect.Effect<CommandReply, CommandFailure> =>
    Effect.gen(function* () {
      const { c } = yield* currentReady;
      const payload = yield* Schema.decodeUnknownEffect(CommandInput)({
        name,
        data: input,
        uploads: options.uploads ?? new Map(),
      }).pipe(
        Effect.mapError((cause) =>
          ReactorError.fromCode("InvalidInput", "invalid command", {
            operation: name,
            outcome: "not-submitted",
            detail: cause,
          }),
        ),
      );
      const wait =
        options.replyTimeout === undefined
          ? settings.replyTimeout
          : Duration.fromInputUnsafe(options.replyTimeout);
      return yield* request(
        c,
        data,
        name,
        (id) =>
          Wire.encode(Wire.DataClientMessageSchema, {
            requestId: id,
            kind: Wire.MessageKind.REQUEST,
            payload: {
              case: "command",
              value: {
                type: payload.name,
                data: payload.data,
                uploads: Object.fromEntries(payload.uploads),
              },
            },
          }),
        "data",
        wait,
      );
    }).pipe(
      Effect.mapError((error): CommandFailure => {
        const { outcome, requestId, generation } = error.context;
        if (
          (outcome === "unknown" || outcome === "replied") &&
          requestId !== undefined &&
          generation !== undefined
        ) {
          const context: CommandContext = {
            ...error.context,
            operation: name,
            outcome,
            requestId,
            generation,
          };
          return CommandFailure.from(error, context);
        }
        return CommandFailure.from(error, {
          ...error.context,
          operation: name,
          outcome: "not-submitted",
        });
      }),
    );

  const controlRequest = (operation: string, payload: ControlPayload, expected?: Connection) =>
    Effect.gen(function* () {
      const c = expected ?? (yield* currentReady).c;
      yield* current(c);
      return yield* request(
        c,
        control,
        operation,
        (id) =>
          Wire.encode(Wire.ControlClientMessageSchema, {
            requestId: id,
            kind: Wire.MessageKind.REQUEST,
            payload,
          }),
        "control",
        settings.replyTimeout,
        payload.case === "publishTrack" ? payload.value.name : undefined,
      );
    });

  const notification = (c: Connection, payload: ControlPayload) =>
    current(c).pipe(
      Effect.andThen(
        Wire.encode(Wire.ControlClientMessageSchema, {
          kind: Wire.MessageKind.NOTIFICATION,
          payload,
        }),
      ),
      Effect.flatMap((bytes) => guard(c, c.peer.send("control", bytes))),
      (effect) => deadline(effect, settings.replyTimeout, "control notification"),
    );

  const unexpected = (message: string) =>
    ReactorError.fromCode("UnexpectedReply", message, { outcome: "replied" });

  const clip = (operation: string, payload: ControlPayload) =>
    controlRequest(operation, payload).pipe(
      Effect.flatMap((reply) => {
        if (reply._tag === "ClipFailed")
          return Effect.fail(
            ReactorError.make({
              reason: Remote.make({
                // The one classification of provider text: a clip failure carries
                // only a reason string, and a disabled recorder must be told apart.
                _tag: /recorder disabled|encoder crashed/i.test(Redacted.value(reply.reason))
                  ? "RecorderDisabled"
                  : "Remote",
                message: "clip failed",
                body: reply.reason,
              }),
              context: { outcome: "replied" },
            }),
          );
        if (reply._tag !== "ClipReady")
          return Effect.fail(unexpected(`clip reply was ${reply._tag}`));
        const playlist = URL.parse(reply.clip.playlistUrl, `${coordinator.apiUrl}/`);
        return playlist === null
          ? Effect.fail(
              ReactorError.fromCode("Protocol", "clip playlist URL is malformed", {
                outcome: "replied",
              }),
            )
          : Effect.succeed({ ...reply.clip, playlistUrl: playlist.href });
      }),
    );

  const trackOperation = <A>(
    name: string,
    body: (c: Connection, track: Track) => Effect.Effect<A, ReactorError>,
    expected?: Connection,
  ) =>
    Effect.acquireUseRelease(
      Effect.gen(function* () {
        const c = expected ?? (yield* currentReady).c;
        yield* current(c);
        const link = yield* Ref.get(c.link);
        const track = link.negotiated?.descriptor.capabilities.tracks.find((t) => t.name === name);
        if (track === undefined)
          return yield* ReactorError.fromCode("InvalidState", `unknown track: ${name}`);
        const free = yield* Ref.modify(c.link, (current) =>
          current.busy.has(name)
            ? ([false, current] as const)
            : ([true, { ...current, busy: toggled(current.busy, name, true) }] as const),
        );
        if (!free)
          return yield* ReactorError.fromCode(
            "InvalidState",
            `another track operation is in flight: ${name}`,
          );
        return { c, track };
      }),
      ({ c, track }) => guard(c, body(c, track)),
      ({ c }) =>
        Ref.update(c.link, (link): Link => ({ ...link, busy: toggled(link.busy, name, false) })),
    );

  const setTrackActive = (name: string, active: boolean, expected?: Connection) =>
    trackOperation(
      name,
      (c) =>
        c.peer.direction(name, active).pipe(
          Effect.andThen(
            Ref.update(c.link, (link): Link => ({
              ...link,
              paused: toggled(link.paused, name, !active),
            })),
          ),
          Effect.andThen(
            notification(
              c,
              active
                ? { case: "resumeTrack", value: { name } }
                : { case: "pauseTrack", value: { name } },
            ),
          ),
        ),
      expected,
    );

  /**
   * A platform track replacement cannot be cancelled. Recording its owner is
   * masked, and an abandoned wait retires the generation so a late
   * replacement cannot race another sender operation.
   */
  const replaceSender = (c: Connection, name: string, source: MediaTrack | null) =>
    Effect.uninterruptibleMask((restore) =>
      Effect.gen(function* () {
        const media = c.peer.media;
        if (media._tag !== "Tracks") return yield* unsupported("this peer publishes no tracks");
        yield* current(c);
        const clone = source === null ? null : source.clone();
        const invoked = yield* Ref.make(false);
        const expired = yield* Ref.make(false);
        yield* restore(
          current(c).pipe(
            Effect.andThen(Ref.set(invoked, true)),
            Effect.andThen(media.replace(name, clone)),
            Effect.timeoutOrElse({
              duration: settings.replyTimeout,
              orElse: () =>
                Ref.set(expired, true).pipe(
                  Effect.andThen(
                    Effect.fail(
                      ReactorError.fromCode(
                        "Timeout",
                        "sender replacement deadline; generation retired",
                        {
                          operation: name,
                          outcome: "unknown",
                        },
                      ),
                    ),
                  ),
                ),
            }),
          ),
        ).pipe(
          Effect.onExit((exit) =>
            Exit.isSuccess(exit)
              ? Effect.void
              : Effect.gen(function* () {
                  clone?.stop();
                  const deadlined = yield* Ref.get(expired);
                  if ((yield* Ref.get(invoked)) && (deadlined || Cause.hasInterrupts(exit.cause)))
                    yield* fail(
                      c,
                      ReactorError.fromCode(
                        deadlined ? "Timeout" : "Disconnected",
                        "sender replacement abandoned; reconnect before reusing the publication",
                        { operation: name, outcome: "unknown" },
                      ),
                    );
                }),
          ),
        );
        const live = yield* Effect.exit(current(c));
        if (Exit.isFailure(live)) {
          clone?.stop();
          return yield* live;
        }
        const previous = yield* Ref.modify(c.link, (link) => {
          const sending = new Map(link.sending);
          const before = sending.get(name);
          if (clone === null) sending.delete(name);
          else sending.set(name, clone);
          return [before, { ...link, sending }] as const;
        });
        previous?.stop();
      }),
    );

  const publishTrack = (name: string, source: MediaTrack, expected?: Connection) =>
    trackOperation(
      name,
      (c, track) =>
        Effect.gen(function* () {
          if (
            track.direction !== "sendonly" ||
            source.kind !== track.kind ||
            source.readyState !== "live"
          )
            return yield* ReactorError.fromCode(
              "InvalidState",
              "publish requires a live matching input track",
            );
          if (!(yield* Ref.get(c.link)).claimed.has(name)) {
            const reply = yield* controlRequest(
              "publish_track",
              { case: "publishTrack", value: { name } },
              c,
            );
            if (reply._tag !== "TrackPublished" || reply.name !== name)
              return yield* unexpected("publisher claim reply mismatch");
            yield* Ref.update(c.link, (link): Link => ({
              ...link,
              claimed: toggled(link.claimed, name, true),
            }));
          }
          yield* replaceSender(c, name, source);
        }),
      expected,
    );

  const unpublishTrack = (name: string, expected?: Connection) =>
    trackOperation(
      name,
      (c) =>
        replaceSender(c, name, null).pipe(
          Effect.andThen(notification(c, { case: "unpublishTrack", value: { name } })),
          Effect.andThen(
            Ref.update(c.link, (link): Link => ({
              ...link,
              claimed: toggled(link.claimed, name, false),
            })),
          ),
        ),
      expected,
    );

  const setMaxBitrate = (name: string, bitsPerSecond: number, expected?: Connection) =>
    trackOperation(
      name,
      (c) =>
        c.peer.maxBitrate(name, bitsPerSecond).pipe(
          Effect.andThen(
            SubscriptionRef.update(state, (session): State => ({
              ...session,
              bitrates: new Map(session.bitrates).set(name, bitsPerSecond),
            })),
          ),
        ),
      expected,
    );

  const releasePublications = (c: Connection | undefined) =>
    Effect.gen(function* () {
      const submitted: Array<string> = [];
      const errors: Array<ReactorError> = [];
      if (c === undefined) return { submitted, errors };
      const link = yield* Ref.get(c.link);
      if (link.failure !== undefined) return { submitted, errors };
      for (const name of link.claimed) {
        // Closing has fenced ordinary requests; these are the last notifications.
        const sent = yield* Effect.exit(
          Wire.encode(Wire.ControlClientMessageSchema, {
            kind: Wire.MessageKind.NOTIFICATION,
            payload: { case: "unpublishTrack", value: { name } },
          }).pipe(
            Effect.flatMap((bytes) => c.peer.send("control", bytes)),
            (effect) =>
              deadline(
                effect,
                Duration.min(Duration.seconds(1), settings.replyTimeout),
                "close unpublish",
              ),
          ),
        );
        if (Exit.isSuccess(sent)) submitted.push(name);
        else
          errors.push(
            failureOf(sent.cause, () =>
              ReactorError.fromCode("Shutdown", "publication cleanup failed", {
                detail: sent.cause,
              }),
            ),
          );
      }
      return { submitted, errors };
    });

  const terminateOwned = (remote: RemoteSession | undefined): Effect.Effect<Termination> =>
    isKnown(remote) && remote.ownership === "owned"
      ? signaling.terminate(remote.id)
      : Effect.succeed(notTerminated);

  const close: Effect.Effect<CloseReport> = Effect.uninterruptible(
    Effect.gen(function* () {
      if (yield* Ref.getAndSet(closeStarted, true)) return yield* Deferred.await(closed);
      return yield* Effect.gen(function* () {
        yield* transition("closing");
        yield* Deferred.fail(closing, ReactorError.fromCode("Closed", "session closing"));
        const connection = (yield* SubscriptionRef.get(state)).connection;
        const { submitted, errors } = yield* releasePublications(connection);
        if (connection !== undefined)
          yield* fail(connection, ReactorError.fromCode("Aborted", "session closed"));
        const shutdown = yield* Effect.exit(Scope.close(root, Exit.void));
        if (Exit.isFailure(shutdown)) {
          // The host's own failure is the evidence; its message says what did
          // not finish. Anything else is reported as an opaque shutdown.
          // A host finalizer that cannot carry a typed error dies with it.
          const failure = Cause.squash(shutdown.cause);
          errors.push(
            ReactorError.is(failure)
              ? failure
              : ReactorError.fromCode("Shutdown", "local cleanup did not complete cleanly", {
                  detail: shutdown.cause,
                }),
          );
        }
        // Ownership is read after local work has joined: an interrupted
        // allocation may have changed its evidence meanwhile.
        const remote = (yield* SubscriptionRef.get(state)).remote;
        const termination = yield* terminateOwned(remote);
        const unresolved =
          connection === undefined ? [] : [...(yield* Ref.get(connection.link)).claims.values()];
        const report: CloseReport = {
          localClosed: errors.length === 0,
          allocation: remote === undefined ? "none" : isKnown(remote) ? "known" : "unknown",
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
      }).pipe(
        Effect.withSpan("reactor.session.close", { kind: "client" }, { captureStackTrace: false }),
      );
    }),
  );

  const snapshot: Effect.Effect<Snapshot> = Effect.gen(function* () {
    const session = yield* SubscriptionRef.get(state);
    const link =
      session.connection === undefined ? undefined : yield* Ref.get(session.connection.link);
    const details = {
      generation: session.generation,
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
    const remote = session.remote;
    return {
      ...details,
      status: session.status === "ready" ? "waiting" : session.status,
      ...(remote === undefined
        ? {}
        : {
            remote: {
              ownership: remote.ownership,
              ...(isKnown(remote)
                ? {
                    sessionId: remote.id,
                    ...(remote.descriptor === undefined ? {} : { descriptor: remote.descriptor }),
                    ...(remote.connectionId === undefined
                      ? {}
                      : { connectionId: remote.connectionId }),
                  }
                : {}),
            },
          }),
    };
  });

  const observe = (
    options: ObserveOptions = {},
  ): Effect.Effect<Observation, ReactorError, Scope.Scope> =>
    Effect.gen(function* () {
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

  /** A stream of `c`'s track, fenced to `c`: it fails once `c` retires. */
  const fenced = <A>(
    c: Connection,
    source: Stream.Stream<A, ReactorError>,
  ): Stream.Stream<A, ReactorError> =>
    Stream.transformPull(source, (pull) =>
      Effect.succeed(
        current(c).pipe(
          Effect.andThen(pull),
          Effect.raceFirst(Deferred.await(c.failed)),
          Effect.tap(() => current(c)),
        ),
      ),
    );

  const decoded: Effect.Effect<DecodedMedia, ReactorError> = Effect.gen(function* () {
    const { c, negotiated } = yield* currentReady;
    const media = c.peer.media;
    if (media._tag !== "Decoded") return yield* unsupported("this peer has no decoded media");
    return {
      generation: c.generation,
      tracks: negotiated.descriptor.capabilities.tracks,
      retired: Deferred.await(c.failed),
      video: (name) => fenced(c, media.video(name)),
      audio: (name) => fenced(c, media.audio(name)),
      pressure: media.pressure,
    };
  });

  const tracks: Effect.Effect<TrackMedia, ReactorError> = Effect.gen(function* () {
    const { c, negotiated } = yield* currentReady;
    const media = c.peer.media;
    if (media._tag !== "Tracks") return yield* unsupported("this peer has no platform tracks");
    return {
      generation: c.generation,
      tracks: negotiated.descriptor.capabilities.tracks,
      retired: Deferred.await(c.failed),
      track: (name) => current(c).pipe(Effect.andThen(media.lease(name))),
      publish: (name, source) => publishTrack(name, source, c),
      unpublish: (name) => unpublishTrack(name, c),
      setTrackActive: (name, active) => setTrackActive(name, active, c),
      setMaxBitrate: (name, bits) => setMaxBitrate(name, bits, c),
    };
  });

  const upload = (name: string, mimeType: string, bytes: Uint8Array, options: UploadOptions = {}) =>
    Effect.gen(function* () {
      const progress = yield* Ref.make<UploadProgress>({
        allocation: "not-requested",
        transfer: "not-requested",
        notification: "not-submitted",
      });
      const reach = (patch: Partial<UploadProgress>) =>
        Ref.update(progress, (p): UploadProgress => ({ ...p, ...patch }));
      const operation = Effect.gen(function* () {
        const { c } = yield* currentReady;
        const known = (yield* SubscriptionRef.get(state)).remote;
        if (!isKnown(known))
          return yield* ReactorError.fromCode("InvalidState", "no known session id");
        if (
          name.length === 0 ||
          mimeType.length === 0 ||
          bytes.byteLength < 1 ||
          bytes.byteLength > settings.maxUploadBytes
        )
          return yield* ReactorError.fromCode(
            "InvalidInput",
            `an upload needs a name, a MIME type and 1..${String(settings.maxUploadBytes)} bytes`,
            { outcome: "not-submitted" },
          );
        const copy = new Uint8Array(bytes);
        yield* reach({ allocation: "unknown" });
        const slot = yield* guard(
          c,
          signaling.allocateUpload(known.id, name, mimeType, copy.length),
        );
        const file: UploadReference = {
          uploadId: slot.presigned_id,
          name,
          mimeType,
          size: BigInt(copy.length),
        };
        yield* reach({ allocation: "confirmed", file, transfer: "unknown" });
        yield* guard(c, signaling.putUpload(slot, copy, mimeType));
        yield* reach({ transfer: "confirmed", notification: "unknown" });
        yield* notification(c, { case: "fileUploaded", value: file }).pipe(
          Effect.tapError((error) =>
            error.context.outcome === "not-submitted"
              ? reach({ notification: "not-submitted" })
              : Effect.void,
          ),
        );
        yield* reach({ notification: "submitted" });
        return { file, transfer: "confirmed", notification: "submitted" } satisfies Uploaded;
      });
      const wait =
        options.uploadTimeout === undefined
          ? settings.uploadTimeout
          : Duration.fromInputUnsafe(options.uploadTimeout);
      return yield* deadline(operation, wait, "upload").pipe(
        Effect.catch((error) =>
          Ref.get(progress).pipe(
            Effect.flatMap((reached) =>
              Effect.fail(
                ReactorError.fromCode("Upload", error.message, {
                  ...error.context,
                  operation: "upload",
                  detail: { cause: error, progress: reached },
                }),
              ),
            ),
          ),
        ),
        Effect.ensuring(
          Ref.get(progress).pipe(
            Effect.flatMap((reached) => publish({ _tag: "Upload", progress: reached })),
          ),
        ),
      );
    }).pipe(
      // The MIME type and size only: never the name or the bytes.
      Effect.withSpan(
        "reactor.session.upload",
        {
          kind: "client",
          attributes: {
            "reactor.upload.mime_type": mimeType,
            "reactor.upload.size": bytes.byteLength,
          },
        },
        { captureStackTrace: false },
      ),
    );

  const session = (id: string): Session => ({
    id,
    ownership: intent._tag === "Create" || intent.adopt ? "owned" : "attached",
    snapshot,
    changes: SubscriptionRef.changes(state).pipe(Stream.mapEffect(() => snapshot)),
    ready: Effect.map(currentReady, ({ c, negotiated }) => ({
      status: "ready",
      generation: c.generation,
      remote: negotiated,
    })),
    events: (options) => Stream.unwrap(hub.subscribe(options?.capacity)),
    observe,
    command,
    schema: controlRequest("request_schema", { case: "requestSchema", value: {} }).pipe(
      Effect.flatMap((reply) =>
        reply._tag === "ModelSchema"
          ? Effect.succeed(reply.openapi === undefined ? {} : { openapi: reply.openapi })
          : Effect.fail(unexpected(`schema reply was ${reply._tag}`)),
      ),
    ),
    upload,
    requestRecordingClip: (seconds) =>
      Number.isFinite(seconds) && seconds > 0
        ? clip("request_clip", { case: "requestClip", value: { durationSeconds: seconds } })
        : Effect.fail(
            ReactorError.fromCode("InvalidInput", "clip duration must be finite and positive", {
              outcome: "not-submitted",
            }),
          ),
    recording: clip("request_recording", { case: "requestRecording", value: {} }),
    stats: Effect.gen(function* () {
      const { c } = yield* currentReady;
      const raw = yield* guard(c, c.peer.stats);
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
    }),
    decoded,
    tracks,
    reconnect: connectAttempt(true),
    close,
  });

  return { session, allocate, connect: connectAttempt(false), close } satisfies Handle;
});

const withoutKey = <K, V>(map: ReadonlyMap<K, V>, key: K): ReadonlyMap<K, V> => {
  const next = new Map(map);
  next.delete(key);
  return next;
};

/** `set` with `value` in it or not. */
const toggled = <A>(set: ReadonlySet<A>, value: A, present: boolean): ReadonlySet<A> => {
  const next = new Set(set);
  if (present) next.add(value);
  else next.delete(value);
  return next;
};

/** The typed failure in `cause`, or `fallback` for a defect or an interruption. */
const failureOf = (
  cause: Cause.Cause<ReactorError>,
  fallback: () => ReactorError,
): ReactorError => {
  const error = Cause.findError(cause);
  return error._tag === "Success" ? error.success : fallback();
};
