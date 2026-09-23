import * as Effect from "effect/Effect";
import * as Deferred from "effect/Deferred";
import * as Fiber from "effect/Fiber";
import * as Scope from "effect/Scope";
import * as Exit from "effect/Exit";
import * as Queue from "effect/Queue";
import * as Cause from "effect/Cause";
import * as Clock from "effect/Clock";
import * as Stream from "effect/Stream";
import { errorOf, positiveLimit, ReactorError } from "./errors.js";
import { nonempty, uint32, structFromObject, objectFromStruct } from "./json.js";
import type { JsonObject } from "./json.js";
import type { Track } from "./contract.js";
import { CoordinatorClient } from "./coordinator/_internal/client.js";
import { Correlator } from "./correlation.js";
import { Observations } from "./observation.js";
import type { Peer, PeerEvent, MediaTrack } from "./PeerTypes.js";
import * as W from "./wire.generated.js";
import { StatsSampler } from "./stats.js";
import type { Statistics } from "./stats.js";

import type {
  SessionOptions,
  CloseReport,
  Snapshot,
  ReadyState,
  ReadyDescriptor,
  CommandReply,
  EventPayload,
  SessionEvent,
  UploadProgress,
  Uploaded,
  ControlPayload,
  ControlReply,
} from "./SessionTypes.js";
import { CommandFailure } from "./session/commands.js";
import type { CommandContext } from "./session/commands.js";
import type { MediaGeneration, RawMedia, TrackGeneration } from "./session/media.js";
import { captureUploads } from "./session/_internal/uploads.js";
import { SessionLifecycle } from "./session/_internal/lifecycle.js";
import type { Connection, ReadyConnection } from "./session/_internal/lifecycle.js";
import { isKnownRemote, RemoteSession } from "./session/_internal/remote.js";
import type { KnownRemote } from "./session/_internal/remote.js";
import { cleanupSession } from "./session/_internal/cleanup.js";
export type {
  Status,
  SessionOptions,
  CloseReport,
  Snapshot,
  CommandReply,
  SessionEvent,
  UploadProgress,
  Uploaded,
} from "./SessionTypes.js";
const pure = <A>(body: () => A): Effect.Effect<A, ReactorError> =>
  Effect.try({ try: body, catch: errorOf });
const withDeadline = <A, R>(
  effect: Effect.Effect<A, ReactorError, R>,
  ms: number,
  operation: string,
): Effect.Effect<A, ReactorError, R> =>
  effect.pipe(
    Effect.timeoutOrElse({
      duration: ms,
      orElse: () =>
        Effect.fail(new ReactorError({ code: "Timeout", message: `${operation}: deadline` })),
    }),
  );

/** The canonical Reactor state and wire engine for both public host exports.
 * Platform and peer dependencies are supplied by the configured client factory. */
export class Session {
  private readonly lifecycle = new SessionLifecycle((status) =>
    this.emit({ _tag: "Status", status }),
  );
  private readonly remote = new RemoteSession();
  private readonly data: Correlator<CommandReply>;
  private readonly control: Correlator<ControlReply>;
  private readonly observations = new Observations<SessionEvent>();
  private sequence = 0n;
  private readonly sampler = new StatsSampler();
  private closeGate: Deferred.Deferred<CloseReport> | undefined;
  private closeReport: CloseReport | undefined;
  private readonly received = new Set<string>();
  private readonly bitrates = new Map<string, number>();
  private readonly commandTimeout: number;
  private readonly connectTimeout: number;
  private readonly readyTimeout: number;
  private readonly heartbeat: number;
  private readonly uploadBound: number;
  readonly http: CoordinatorClient;
  constructor(
    readonly options: SessionOptions,
    private readonly makePeer: () => Peer,
    http: CoordinatorClient,
  ) {
    if (options.intent._tag === "Attach") {
      nonempty(options.intent.sessionId, "attach sessionId");
      if (options.intent.connectionId !== undefined)
        uint32(options.intent.connectionId, "attach connectionId");
    } else nonempty(options.intent.model.name, "model name");
    this.http = http;
    this.commandTimeout = positiveLimit(
      options.commandTimeoutMs ?? 10_000,
      "command timeout",
      600_000,
    );
    this.connectTimeout = positiveLimit(
      options.connectTimeoutMs ?? 180_000,
      "connect timeout",
      600_000,
    );
    this.readyTimeout = positiveLimit(options.readyTimeoutMs ?? 30_000, "ready timeout", 600_000);
    this.heartbeat = options.heartbeatMs ?? 10_000;
    if (this.heartbeat !== 0) positiveLimit(this.heartbeat, "heartbeat interval", 600_000);
    this.uploadBound = positiveLimit(
      options.maxUploadBytes ?? 16_777_216,
      "upload byte bound",
      64 * 1024 * 1024,
    );
    this.data = new Correlator("data", options.maxPending ?? 128, options.requestNamespace);
    this.control = new Correlator("ctrl", options.maxPending ?? 128, options.requestNamespace);
  }
  get snapshot(): Snapshot {
    const r = this.remote.current,
      c = this.lifecycle.connection;
    const details = {
      generation: this.lifecycle.generation,
      pending: Object.freeze({ data: this.data.size, control: this.control.size }),
      pausedLocally: Object.freeze([...(c?.paused ?? [])]),
      claimedTracks: Object.freeze([...(c?.claimed ?? [])]),
      receivedTracks: Object.freeze([...this.received]),
      unresolvedPublications: Object.freeze([...(c?.pendingClaims.values() ?? [])]),
      observationOverflows: this.observations.overflowCount,
      subscribers: this.observations.size,
      ...(this.lifecycle.lastError === undefined ? {} : { lastError: this.lifecycle.lastError }),
      ...(this.closeReport === undefined ? {} : { close: this.closeReport }),
    };
    if (this.lifecycle.status === "ready")
      return Object.freeze({ ...details, status: "ready", remote: this.currentReady().negotiated });
    return Object.freeze({
      ...details,
      status: this.lifecycle.status,
      ...(r === undefined
        ? {}
        : {
            remote: Object.freeze({
              ownership: r.ownership,
              ...(isKnownRemote(r)
                ? {
                    sessionId: r.id,
                    ...(r.descriptor === undefined ? {} : { descriptor: r.descriptor }),
                    ...(r.connectionId === undefined ? {} : { connectionId: r.connectionId }),
                  }
                : {}),
            }),
          }),
    });
  }
  events(options?: {
    readonly capacity?: number;
    readonly maxBytes?: number;
  }): Stream.Stream<SessionEvent, ReactorError> {
    return this.observations.stream(options);
  }
  observe(options?: { readonly capacity?: number; readonly maxBytes?: number }): Effect.Effect<
    {
      readonly initial: Snapshot;
      readonly revision: bigint;
      readonly events: Stream.Stream<SessionEvent, ReactorError>;
    },
    ReactorError,
    Scope.Scope
  > {
    const self = this;
    return Effect.gen(function* () {
      const events = yield* self.observations.subscribe(options);
      const initial = self.snapshot;
      const revision = self.sequence;
      return {
        initial,
        revision,
        events: events.pipe(Stream.filter((event) => event.sequence > revision)),
      };
    });
  }
  private emit(event: EventPayload, bytes?: number): void {
    this.observations.emit(
      { ...event, sequence: ++this.sequence, generation: this.lifecycle.generation },
      bytes,
    );
  }
  private assertCurrent(c: Connection): void {
    this.lifecycle.assertCurrent(c);
  }
  private currentReady(): ReadyConnection {
    return this.lifecycle.currentReady();
  }
  private currentRemote(): KnownRemote {
    return this.remote.requireKnown();
  }
  private fail(c: Connection, error: ReactorError): void {
    if (c.failure !== undefined) return;
    c.failure = error;
    Deferred.doneUnsafe(c.failed, Effect.fail(error));
    Deferred.doneUnsafe(c.ready, Effect.fail(error));
    Queue.failCauseUnsafe(c.iceWake, Cause.fail(error));
    this.data.failGeneration(c.generation, error);
    this.control.failGeneration(c.generation, error);
    c.peer.close();
    for (const track of c.sending.values()) track.stop();
    c.sending.clear();
    c.iceBuffer.length = 0;
    c.iceBytes = 0;
    c.claimed.clear();
    c.paused.clear();
    if (this.lifecycle.connection === c) this.received.clear();
    if (this.lifecycle.disconnect(c, error)) {
      this.emit({ _tag: "Diagnostic", error });
    }
  }
  private guard<A>(
    c: Connection,
    effect: Effect.Effect<A, ReactorError>,
  ): Effect.Effect<A, ReactorError> {
    return pure(() => this.assertCurrent(c)).pipe(
      Effect.andThen(effect),
      Effect.raceFirst(Deferred.await(c.failed)),
      Effect.tap(() => pure(() => this.assertCurrent(c))),
      Effect.mapError((e) => c.failure ?? e),
    );
  }
  private onPeer(c: Connection, event: PeerEvent): void {
    if (!this.lifecycle.accepts(c)) return;
    switch (event.type) {
      case "state":
        if (event.state === "failed" || event.state === "disconnected" || event.state === "closed")
          this.fail(
            c,
            new ReactorError({
              code: "Disconnected",
              message: `peer state ${event.state}`,
              context: { generation: c.generation },
            }),
          );
        else {
          c.peerConnected = event.state === "connected";
          c.readyGate();
        }
        break;
      case "channel":
        if (!event.open) {
          this.fail(
            c,
            new ReactorError({
              code: "ChannelClosed",
              message: `${event.channel} channel closed`,
              context: { generation: c.generation, detail: { channel: event.channel } },
            }),
          );
          break;
        }
        if (event.channel === "control") c.controlOpen = true;
        else c.dataOpen = true;
        c.readyGate();
        break;
      case "ice":
        if (event.candidate === undefined) c.iceDone = true;
        else {
          c.iceBytes += event.candidate.candidate.length * 2;
          if (c.iceBuffer.length >= 256 || c.iceBytes > 262_144 || c.finalSent) {
            this.fail(
              c,
              new ReactorError({
                code: "Overflow",
                message: "ICE buffer bound or candidate after final batch",
              }),
            );
            break;
          }
          c.iceBuffer.push(event.candidate);
        }
        Queue.offerUnsafe(c.iceWake, undefined);
        break;
      case "decoded":
        this.received.add(event.name);
        this.emit({ _tag: "Decoded", kind: event.kind, name: event.name, mid: event.mid });
        break;
      case "track":
        this.received.add(event.name);
        this.emit({ _tag: "Track", name: event.name, mid: event.mid });
        break;
      case "error":
        this.fail(c, event.error);
        break;
      case "message":
        this.receive(c, event.channel, event.bytes);
        break;
    }
  }
  private receive(c: Connection, channel: "data" | "control", bytes: Uint8Array): void {
    try {
      if (channel === "data") {
        const message = W.DataServerMessage.decode(bytes),
          payload = message.payload;
        if (payload?.case === "error") {
          const error = new ReactorError({
            code: "Remote",
            message: `remote command error ${payload.value.code}`,
            context: {
              remoteCode: payload.value.code,
              body: payload.value.message,
              requestId: message.request_id,
              generation: c.generation,
              outcome: "replied",
              detail: message,
            },
          });
          this.data.settleWith(message.request_id, c.generation, (correlation) => {
            this.emit(
              { _tag: "CommandError", requestId: message.request_id, error, correlation },
              bytes.length,
            );
            return Effect.fail(error);
          });
          return;
        }
        const value =
          payload === undefined
            ? { kind: "ack" as const, raw: message }
            : {
                kind: "message" as const,
                type: payload.value.type,
                ...(payload.value.data === undefined
                  ? {}
                  : { data: objectFromStruct(payload.value.data) }),
                raw: message,
              };
        this.data.settleWith(
          message.request_id,
          c.generation,
          (correlation) => {
            const reply: CommandReply = Object.freeze({
              ...value,
              _tag: "Model",
              outcome: "replied",
              requestId: message.request_id,
              generation: c.generation,
              sequence: ++this.sequence,
              correlation,
            });
            // Publish the exact value that completes the request. Observers cannot
            // delay correlation; overflow terminates only that observation.
            this.observations.emit(reply, bytes.length);
            return Effect.succeed(reply);
          },
          value.kind === "ack" ? "acknowledged" : "replied",
        );
      } else {
        const message = W.ControlServerMessage.decode(bytes),
          payload = message.payload;
        // Unlike the data correlator, a bodyless control message is NOT an acknowledgement.
        if (payload === undefined) {
          this.emit(
            {
              _tag: "Diagnostic",
              error: new ReactorError({
                code: "Protocol",
                message: "bodyless control response does not resolve a request",
                context: { requestId: message.request_id },
              }),
            },
            bytes.length,
          );
          return;
        }
        const result =
          payload.case === "error"
            ? Effect.fail(
                new ReactorError({
                  code: "Remote",
                  message: `remote command error ${payload.value.code}`,
                  context: {
                    remoteCode: payload.value.code,
                    body: payload.value.message,
                    requestId: message.request_id,
                    outcome: "replied",
                    detail: message,
                  },
                }),
              )
            : Effect.succeed(payload);
        const claim = c.pendingClaims.get(message.request_id);
        if (claim !== undefined) {
          if (payload.case === "publish_track" && payload.value.name === claim) {
            // Record remote ownership even after the local publish caller stops
            // waiting. Attaching a source still requires that caller to continue.
            c.claimed.add(claim);
            c.pendingClaims.delete(message.request_id);
          } else if (payload.case === "error") c.pendingClaims.delete(message.request_id);
          else {
            this.fail(
              c,
              new ReactorError({
                code: "UnexpectedReply",
                message: "publisher claim reply did not identify the requested track",
                context: {
                  operation: "publish_track",
                  requestId: message.request_id,
                  outcome: "unknown",
                },
              }),
            );
            return;
          }
        }
        const correlation = this.control.settle(message.request_id, c.generation, result);
        if (payload.case !== "error" || correlation !== "matched")
          this.emit({ _tag: "Control", message, correlation }, bytes.length);
      }
    } catch (cause) {
      this.emit({ _tag: "Diagnostic", error: errorOf(cause, "Protocol") }, bytes.length);
    }
  }
  private flushIce(c: Connection): Effect.Effect<void, ReactorError> {
    const self = this;
    return Effect.gen(function* () {
      self.assertCurrent(c);
      if (c.connectionId === undefined) return;
      while (c.iceBuffer.length > 0 || (c.iceDone && !c.finalSent)) {
        const candidates = c.iceBuffer.splice(0);
        c.iceBytes = 0;
        const isFinal = c.iceDone && !c.finalSent;
        yield* self.guard(
          c,
          self.http.ice(self.currentRemote().id, c.connectionId, candidates, isFinal),
        );
        if (isFinal) c.finalSent = true;
      }
    });
  }
  private background(c: Connection, body: Effect.Effect<void, ReactorError>): Effect.Effect<void> {
    return Effect.forkIn(
      body.pipe(
        Effect.raceFirst(Deferred.await(c.failed)),
        Effect.catchCause((cause) =>
          Effect.sync(() => {
            const failure = Cause.findError(cause);
            this.fail(
              c,
              failure._tag === "Success" && failure.success instanceof ReactorError
                ? failure.success
                : new ReactorError({
                    code: "Protocol",
                    message: "session task failed",
                    context: { detail: cause, generation: c.generation },
                  }),
            );
          }),
        ),
      ),
      c.scope,
    ).pipe(Effect.asVoid);
  }
  private begin(reconnect: boolean): Effect.Effect<Connection, ReactorError> {
    const self = this;
    return Effect.gen(function* () {
      const previous = self.lifecycle.connection;
      const c = yield* pure(() => {
        const connection = self.lifecycle.begin(reconnect, self.remote.isKnown, () =>
          self.makePeer(),
        );
        self.sampler.reset();
        self.received.clear();
        self.lifecycle.transition("connecting");
        return connection;
      });
      yield* Scope.addFinalizer(
        c.scope,
        Effect.gen(function* () {
          if (c.failure === undefined)
            self.fail(
              c,
              new ReactorError({
                code: "Aborted",
                message: "connection scope closed",
                context: { generation: c.generation },
              }),
            );
          if (c.peer.shutdown !== undefined) yield* c.peer.shutdown.pipe(Effect.orDie);
        }),
      );
      if (previous !== undefined) {
        self.fail(
          previous,
          new ReactorError({ code: "Disconnected", message: "connection retired for reconnect" }),
        );
        yield* Scope.close(previous.scope, Exit.void);
      }
      return c;
    }).pipe(Effect.uninterruptible);
  }
  /** Allocate or identify one remote session, without opening a peer connection. */
  allocate(): Effect.Effect<string, ReactorError> {
    return this.remote.allocate(this.options, this.http, this.lifecycle);
  }

  get id(): string | undefined {
    return this.remote.id;
  }
  get rawMedia(): RawMedia | undefined {
    return this.lifecycle.connection?.peer.rawMedia;
  }

  readyState(): Effect.Effect<ReadyState, ReactorError> {
    return pure(() => {
      const connection = this.currentReady();
      return Object.freeze({
        status: "ready",
        generation: connection.generation,
        remote: connection.negotiated,
      });
    });
  }

  mediaGeneration(): Effect.Effect<MediaGeneration, ReactorError> {
    return pure(() => {
      const connection = this.currentReady();
      const media = connection.peer.rawMedia;
      if (media === undefined)
        throw new ReactorError({
          code: "UnsupportedCapability",
          message: "peer has no decoded media capability",
          context: { outcome: "not-submitted" },
        });
      const owned = <A>(source: Stream.Stream<A, ReactorError>): Stream.Stream<A, ReactorError> =>
        Stream.transformPull(source, (pull) =>
          Effect.succeed(
            pure(() => this.assertCurrent(connection)).pipe(
              Effect.andThen(pull),
              Effect.raceFirst(Deferred.await(connection.failed)),
              Effect.tap(() => pure(() => this.assertCurrent(connection))),
            ),
          ),
        );
      return Object.freeze({
        generation: connection.generation,
        tracks: connection.negotiated.descriptor.capabilities.tracks,
        retired: Deferred.await(connection.failed),
        video: (name: string) => owned(media.video(name)),
        audio: (name: string) => owned(media.audio(name)),
        snapshot: media.snapshot,
      });
    });
  }

  trackGeneration(): Effect.Effect<TrackGeneration, ReactorError> {
    return pure(() => {
      const connection = this.currentReady();
      if (connection.peer.nativeTracks !== true) {
        throw new ReactorError({
          code: "UnsupportedCapability",
          message: "peer has no browser track capability",
          context: { outcome: "not-submitted" },
        });
      }
      return Object.freeze({
        generation: connection.generation,
        tracks: connection.negotiated.descriptor.capabilities.tracks,
        retired: Deferred.await(connection.failed),
        track: (name: string) => this.track(name, connection),
        publish: (name: string, source: MediaTrack) => this.publish(name, source, connection),
        unpublish: (name: string) => this.unpublish(name, connection),
        setTrackActive: (name: string, active: boolean) =>
          this.setTrackActive(name, active, connection),
        setMaxBitrate: (name: string, bits: number) => this.setMaxBitrate(name, bits, connection),
      });
    });
  }

  /** Internal initial acquisition. Reconnect is a distinct operation and never replays a model command. */
  start(): Effect.Effect<void, ReactorError> {
    return this.connectAttempt(false);
  }
  reconnect(): Effect.Effect<void, ReactorError> {
    return this.connectAttempt(true);
  }
  private connectAttempt(reconnect: boolean): Effect.Effect<void, ReactorError> {
    const self = this;
    return Effect.gen(function* () {
      const c = yield* self.begin(reconnect);
      const work = Effect.gen(function* () {
        if (!reconnect) yield* self.guard(c, self.allocate());
        self.assertCurrent(c);
        self.lifecycle.transition("waiting");
        const remote = self.currentRemote();
        const descriptor = yield* self.guard(
          c,
          self.http.ready(remote.id, reconnect ? undefined : remote.descriptor),
        );
        remote.descriptor = descriptor;
        const capabilities = descriptor.capabilities,
          transport = descriptor.selected_transport;
        if (capabilities === undefined || transport === undefined)
          return yield* new ReactorError({
            code: "Protocol",
            message: "missing ready capabilities/transport",
          });
        if (transport.protocol !== "webrtc" || transport.version !== "1.0")
          return yield* new ReactorError({
            code: "VersionMismatch",
            message: `unsupported transport ${transport.protocol}/${transport.version}`,
          });
        const readyDescriptor: ReadyDescriptor = Object.freeze({
          ...descriptor,
          capabilities,
          selected_transport: transport,
        });
        const servers = yield* self.guard(c, self.http.iceServers(remote.id));
        const prepared = yield* self.guard(
          c,
          Scope.provide(
            c.peer.prepare(servers, capabilities.tracks, (event) => self.onPeer(c, event)),
            c.scope,
          ),
        );
        c.mapping = prepared.mapping;
        const previousId = remote.connectionId;
        const cid = previousId ?? (yield* self.guard(c, self.http.register(remote.id)));
        c.connectionId = cid;
        remote.connectionId = cid;
        // Source order: registration -> buffered ICE (including empty final) -> offer -> poll answer.
        yield* self.flushIce(c);
        yield* self.background(
          c,
          Effect.gen(function* () {
            while (true) {
              yield* Queue.take(c.iceWake);
              yield* self.flushIce(c);
            }
          }),
        );
        yield* self.guard(
          c,
          self.http.offer(
            remote.id,
            cid,
            prepared.sdp,
            prepared.mapping,
            reconnect && previousId !== undefined,
          ),
        );
        const answer = yield* self.guard(c, self.http.answer(remote.id, cid));
        if (answer.connection_id !== undefined) {
          c.connectionId = answer.connection_id;
          remote.connectionId = answer.connection_id;
        }
        yield* self.guard(c, c.peer.answer(answer.sdp_answer));
        yield* withDeadline(
          self.guard(c, Deferred.await(c.ready)),
          self.readyTimeout,
          "peer and both channels ready",
        );
        self.assertCurrent(c);
        c.negotiated = Object.freeze({
          ownership: remote.ownership,
          sessionId: remote.id,
          descriptor: readyDescriptor,
          connectionId: c.connectionId,
        });
        self.lifecycle.transition("ready");
        if (self.options.autoResumeTracks ?? self.options.intent._tag === "Create")
          for (const track of capabilities.tracks)
            if (track.direction === "recvonly") {
              const outcome = yield* Effect.result(self.setTrackActive(track.name, true));
              if (outcome._tag === "Failure")
                self.emit({ _tag: "Diagnostic", error: outcome.failure });
            }
        for (const [name, bitrate] of self.bitrates) {
          const outcome = yield* Effect.result(self.guard(c, c.peer.maxBitrate(name, bitrate)));
          if (outcome._tag === "Failure") self.emit({ _tag: "Diagnostic", error: outcome.failure });
        }
        if (self.heartbeat !== 0)
          yield* self.background(
            c,
            Effect.gen(function* () {
              while (true) {
                yield* self.ping();
                yield* Effect.sleep(self.heartbeat);
              }
            }),
          );
      });
      yield* withDeadline(work, self.connectTimeout, reconnect ? "reconnect" : "connect").pipe(
        Effect.onExit((exit) =>
          Exit.isFailure(exit)
            ? Effect.gen(function* () {
                self.remote.allocationLost();
                const error = Cause.findError(exit.cause);
                self.fail(
                  c,
                  error._tag === "Success" && error.success instanceof ReactorError
                    ? error.success
                    : new ReactorError({
                        code: "Aborted",
                        message: "connection attempt interrupted",
                      }),
                );
                yield* Scope.close(c.scope, Exit.void);
              })
            : Effect.void,
        ),
      );
    });
  }
  private request<A>(
    c: Connection,
    correlator: Correlator<A>,
    operation: string,
    encode: (id: string) => Uint8Array<ArrayBuffer>,
    channel: "control" | "data",
    timeoutMs: number,
    publication?: string,
  ): Effect.Effect<A, ReactorError> {
    const self = this;
    return Effect.uninterruptibleMask((restore) =>
      Effect.gen(function* () {
        const pending = yield* pure(() => {
          self.assertCurrent(c);
          return correlator.register(c.generation, operation);
        });
        if (publication !== undefined) c.pendingClaims.set(pending.id, publication);
        const encoded = yield* Effect.result(pure(() => encode(pending.id)));
        if (encoded._tag === "Failure") {
          correlator.cancel(pending);
          c.pendingClaims.delete(pending.id);
          return yield* new ReactorError({
            code: encoded.failure.code,
            message: encoded.failure.message,
            context: {
              ...encoded.failure.context,
              operation,
              requestId: pending.id,
              generation: c.generation,
              outcome: "not-submitted",
            },
          });
        }
        const failure = (error: ReactorError) =>
          new ReactorError({
            code: error.code,
            message: error.message,
            context: {
              ...error.context,
              operation,
              requestId: pending.id,
              generation: c.generation,
              outcome: error.context.outcome ?? (pending.submitted ? "unknown" : "not-submitted"),
            },
          });
        const sending = pure(() => {
          self.assertCurrent(c);
          // The record already exists when a synchronous peer reply arrives.
          pending.submitted = true;
        }).pipe(
          Effect.andThen(Effect.suspend(() => c.peer.send(channel, encoded.success))),
          Effect.catch((error) =>
            Effect.sync(() => {
              if (!correlator.has(pending)) return;
              if (error.context.outcome === "not-submitted") {
                correlator.cancel(pending);
                c.pendingClaims.delete(pending.id);
              }
              Deferred.doneUnsafe(pending.deferred, Effect.fail(failure(error)));
            }),
          ),
        );
        const execution = Effect.scoped(
          Effect.gen(function* () {
            // The application reply can precede the transport's acknowledgement.
            // Ending the reply wait joins this sender's local ACK waiter, while the
            // native backend remains responsible for any foreign operation slot.
            yield* Effect.forkIn(sending, yield* Effect.scope, { startImmediately: true });
            return yield* Deferred.await(pending.deferred);
          }),
        ).pipe(
          Effect.interruptible,
          (effect) => withDeadline(effect, timeoutMs, operation),
          Effect.mapError(failure),
          Effect.onExit((exit) =>
            Effect.sync(() => {
              if (!pending.submitted) {
                correlator.cancel(pending);
                c.pendingClaims.delete(pending.id);
              } else if (Exit.isFailure(exit)) pending.waiting = false;
              // Unresolved submitted records remain bounded by maxPending. A late
              // reply or retiring this generation releases the slot; a timeout does not.
            }),
          ),
        );
        const owner = yield* Effect.forkIn(execution, c.scope, { startImmediately: true });
        return yield* restore(Fiber.join(owner));
      }),
    );
  }
  command(
    type: string,
    data: unknown,
    uploads: ReadonlyMap<string, W.UploadReference> = new Map(),
    timeoutMs = this.commandTimeout,
  ): Effect.Effect<CommandReply, CommandFailure> {
    return pure(() => {
      const c = this.currentReady(),
        payload = {
          type: nonempty(type, "command type"),
          data: structFromObject(data),
          uploads: captureUploads(uploads),
        };
      positiveLimit(timeoutMs, "command timeout", 600_000);
      return { c, payload };
    }).pipe(
      Effect.mapError((error) =>
        CommandFailure.from(error, {
          ...error.context,
          operation: type,
          outcome: "not-submitted",
        }),
      ),
      Effect.flatMap(({ c, payload }) =>
        this.request(
          c,
          this.data,
          type,
          (id) =>
            W.DataClientMessage.encode({
              request_id: id,
              kind: 1,
              payload: { case: "command", value: payload },
            }),
          "data",
          timeoutMs,
        ).pipe(
          Effect.mapError((error) => {
            const { outcome, requestId, generation } = error.context;
            if (outcome === "unknown" || outcome === "replied") {
              if (requestId === undefined || generation === undefined)
                throw new Error("dispatched command lost its attribution");
              const context: CommandContext = {
                ...error.context,
                operation: type,
                outcome,
                requestId,
                generation,
              };
              return CommandFailure.from(error, context);
            }
            return CommandFailure.from(error, {
              ...error.context,
              operation: type,
              outcome: "not-submitted",
            });
          }),
        ),
      ),
    );
  }
  private controlRequest(
    operation: string,
    payload: ControlPayload,
    expected?: Connection,
  ): Effect.Effect<ControlReply, ReactorError> {
    return pure(() => {
      const connection = expected ?? this.currentReady();
      this.assertCurrent(connection);
      return connection;
    }).pipe(
      Effect.flatMap((c) =>
        this.request(
          c,
          this.control,
          operation,
          (id) => W.ControlClientMessage.encode({ request_id: id, kind: 1, payload }),
          "control",
          this.commandTimeout,
          payload.case === "publish_track" ? payload.value.name : undefined,
        ),
      ),
    );
  }
  private notification(c: Connection, payload: ControlPayload): Effect.Effect<void, ReactorError> {
    return pure(() => {
      this.assertCurrent(c);
      return W.ControlClientMessage.encode({ request_id: "", kind: 3, payload });
    }).pipe(
      Effect.flatMap((bytes) => this.guard(c, c.peer.send("control", bytes))),
      (effect) => withDeadline(effect, this.commandTimeout, "control notification"),
    );
  }
  ping(): Effect.Effect<void, ReactorError> {
    return pure(() => this.currentReady()).pipe(
      Effect.flatMap((c) => this.notification(c, { case: "ping", value: {} })),
    );
  }
  schema(): Effect.Effect<
    { readonly openapi?: JsonObject; readonly raw: W.ModelSchema },
    ReactorError
  > {
    return this.controlRequest("request_schema", { case: "request_schema", value: {} }).pipe(
      Effect.flatMap((reply) =>
        pure(() => {
          if (reply.case !== "model_schema")
            throw new ReactorError({
              code: "UnexpectedReply",
              message: `schema reply was ${reply.case}`,
              context: { outcome: "replied" },
            });
          return {
            raw: reply.value,
            ...(reply.value.openapi === undefined
              ? {}
              : { openapi: objectFromStruct(reply.value.openapi) }),
          };
        }),
      ),
    );
  }
  private clip(payload: ControlPayload): Effect.Effect<W.ClipReady, ReactorError> {
    return this.controlRequest(payload.case, payload).pipe(
      Effect.flatMap((reply) =>
        pure(() => {
          if (reply.case === "clip_failed")
            throw new ReactorError({
              // The one regex classification of provider free text, a deliberate exception:
              // ClipFailed carries only a reason string, and a caller must tell a disabled
              // recorder from other clip failures. Add no others; drop this once the wire
              // carries a code.
              code: /recorder disabled|encoder crashed/i.test(reply.value.reason)
                ? "RecorderDisabled"
                : "Remote",
              message: "clip failed",
              context: { outcome: "replied", body: reply.value.reason },
            });
          if (reply.case !== "clip_ready")
            throw new ReactorError({
              code: "UnexpectedReply",
              message: `clip reply was ${reply.case}`,
              context: { outcome: "replied" },
            });
          return {
            ...reply.value,
            playlist_url: new URL(reply.value.playlist_url, `${this.http.apiUrl}/`).href,
          };
        }),
      ),
    );
  }
  requestClip(seconds: number): Effect.Effect<W.ClipReady, ReactorError> {
    return pure(() => {
      if (!Number.isFinite(seconds) || seconds <= 0)
        throw new ReactorError({
          code: "Protocol",
          message: "clip duration must be finite and positive",
        });
    }).pipe(
      Effect.andThen(this.clip({ case: "request_clip", value: { duration_seconds: seconds } })),
    );
  }
  recording(): Effect.Effect<W.ClipReady, ReactorError> {
    return this.clip({ case: "request_recording", value: {} });
  }
  private namedTrack(name: string, connection: ReadyConnection): Track {
    if (connection.peer.mediaSupported === false)
      throw new ReactorError({
        code: "UnsupportedCapability",
        message: "This peer is data-only; track/media operations are unavailable",
        context: { outcome: "not-submitted" },
      });
    const track = connection.negotiated.descriptor.capabilities.tracks.find((t) => t.name === name);
    if (track === undefined)
      throw new ReactorError({ code: "InvalidState", message: `unknown track: ${name}` });
    return track;
  }
  private trackOperation<A>(
    name: string,
    body: (c: Connection, track: Track) => Effect.Effect<A, ReactorError>,
    expected?: ReadyConnection,
  ): Effect.Effect<A, ReactorError> {
    return Effect.acquireUseRelease(
      pure(() => {
        const c = expected ?? this.currentReady();
        this.assertCurrent(c);
        const track = this.namedTrack(name, c);
        if (c.trackBusy.has(name))
          throw new ReactorError({
            code: "InvalidState",
            message: `another track operation is in flight: ${name}`,
          });
        c.trackBusy.add(name);
        return { c, track };
      }),
      ({ c, track }) => this.guard(c, body(c, track)),
      ({ c }) =>
        Effect.sync(() => {
          c.trackBusy.delete(name);
        }),
    );
  }
  setTrackActive(
    name: string,
    active: boolean,
    expected?: ReadyConnection,
  ): Effect.Effect<void, ReactorError> {
    return this.trackOperation(
      name,
      (c) =>
        c.peer.direction(name, active).pipe(
          Effect.andThen(
            pure(() => {
              if (active) c.paused.delete(name);
              else c.paused.add(name);
            }),
          ),
          Effect.andThen(
            this.notification(
              c,
              active
                ? { case: "resume_track", value: { name } }
                : { case: "pause_track", value: { name } },
            ),
          ),
        ),
      expected,
    );
  }
  /** Native replaceTrack cannot be cancelled. Atomic local ownership transfer is
   * masked; only the bounded native wait is interruptible. An abandoned wait retires
   * this generation, preventing late replacement from racing another sender operation. */
  private replaceSender(
    c: Connection,
    name: string,
    source: MediaTrack | null,
  ): Effect.Effect<void, ReactorError> {
    const self = this;
    return Effect.uninterruptibleMask((restore) =>
      Effect.gen(function* () {
        const clone = yield* pure(() => {
          self.assertCurrent(c);
          return source === null ? null : source.clone();
        });
        let invoked = false,
          deadline = false;
        const mutation = pure(() => {
          self.assertCurrent(c);
          invoked = true;
        }).pipe(
          Effect.andThen(Effect.suspend(() => c.peer.replace(name, clone))),
          Effect.timeoutOrElse({
            duration: self.commandTimeout,
            orElse: () => {
              deadline = true;
              return Effect.fail(
                new ReactorError({
                  code: "Timeout",
                  message: "sender replacement deadline; local generation retired",
                  context: { operation: name, outcome: "unknown" },
                }),
              );
            },
          }),
        );
        yield* restore(mutation).pipe(
          Effect.onExit((exit) =>
            Exit.isFailure(exit)
              ? Effect.sync(() => {
                  clone?.stop();
                  if (invoked && (deadline || Cause.hasInterrupts(exit.cause)))
                    self.fail(
                      c,
                      new ReactorError({
                        code: deadline ? "Timeout" : "Disconnected",
                        message:
                          "native sender replacement abandoned; reconnect explicitly before reusing publication",
                        context: { operation: name, outcome: "unknown" },
                      }),
                    );
                })
              : Effect.void,
          ),
        );
        // No interruptible boundary between successful mutation and recording its owner.
        // If another task retired the generation, do not resurrect a sending clone.
        yield* pure(() => {
          try {
            self.assertCurrent(c);
            c.sending.get(name)?.stop();
            if (clone === null) c.sending.delete(name);
            else c.sending.set(name, clone);
          } catch (error) {
            clone?.stop();
            throw error;
          }
        });
      }),
    );
  }
  /** Claims the publisher slot, then attaches a session-owned clone. Caller retains its source track. */
  publish(
    name: string,
    source: MediaTrack,
    expected?: ReadyConnection,
  ): Effect.Effect<void, ReactorError> {
    const self = this;
    return this.trackOperation(
      name,
      (c, track) =>
        Effect.gen(function* () {
          if (c.peer.nativeTracks === false)
            return yield* new ReactorError({
              code: "UnsupportedCapability",
              message: "this peer does not accept browser media tracks",
              context: { outcome: "not-submitted" },
            });
          if (
            track.direction !== "sendonly" ||
            source.kind !== track.kind ||
            source.readyState !== "live"
          )
            return yield* new ReactorError({
              code: "InvalidState",
              message: "publish requires a live matching input track",
            });
          if (!c.claimed.has(name)) {
            const reply = yield* self.controlRequest(
              "publish_track",
              { case: "publish_track", value: { name } },
              c,
            );
            if (reply.case !== "publish_track" || reply.value.name !== name)
              return yield* new ReactorError({
                code: "UnexpectedReply",
                message: "publisher claim reply mismatch",
                context: { outcome: "replied" },
              });
            c.claimed.add(name);
          }
          yield* self.replaceSender(c, name, source);
        }),
      expected,
    );
  }
  unpublish(name: string, expected?: ReadyConnection): Effect.Effect<void, ReactorError> {
    return this.trackOperation(
      name,
      (c) =>
        c.peer.nativeTracks === false
          ? Effect.fail(
              new ReactorError({
                code: "UnsupportedCapability",
                message: "this peer does not expose browser track publication",
                context: { outcome: "not-submitted" },
              }),
            )
          : this.replaceSender(c, name, null).pipe(
              Effect.andThen(this.notification(c, { case: "unpublish_track", value: { name } })),
              Effect.tap(() =>
                Effect.sync(() => {
                  c.claimed.delete(name);
                }),
              ),
            ),
      expected,
    );
  }
  track(
    name: string,
    expected?: ReadyConnection,
  ): Effect.Effect<MediaTrack, ReactorError, Scope.Scope> {
    return Effect.acquireRelease(
      pure(() => {
        const c = expected ?? this.currentReady();
        this.assertCurrent(c);
        if (c.peer.nativeTracks === false)
          throw new ReactorError({
            code: "UnsupportedCapability",
            message: "this peer exposes owned decoded frames instead of browser tracks",
            context: { outcome: "not-submitted" },
          });
        return { peer: c.peer, track: c.peer.lease(name) };
      }),
      ({ peer, track }) => Effect.sync(() => peer.release(track)),
    ).pipe(Effect.map(({ track }) => track));
  }
  setMaxBitrate(
    name: string,
    bitsPerSecond: number,
    expected?: ReadyConnection,
  ): Effect.Effect<void, ReactorError> {
    return this.trackOperation(
      name,
      (c) =>
        c.peer.maxBitrate(name, bitsPerSecond).pipe(
          Effect.tap(() =>
            Effect.sync(() => {
              this.bitrates.set(name, bitsPerSecond);
            }),
          ),
        ),
      expected,
    );
  }
  stats(): Effect.Effect<Statistics, ReactorError> {
    const self = this;
    return Effect.gen(function* () {
      const connection = yield* pure(() => self.currentReady());
      const raw = yield* self.guard(connection, connection.peer.stats);
      // Elapsed sampling time belongs to this Effect boundary, not the pure sampler.
      const atNanos = yield* Clock.monotonicTimeNanos;
      const atMs = Number(atNanos) / 1_000_000;
      return yield* pure(() => self.sampler.sample(raw, connection.generation, atMs));
    });
  }
  rawStats(): Effect.Effect<readonly unknown[], ReactorError> {
    return pure(() => this.currentReady()).pipe(Effect.flatMap((c) => this.guard(c, c.peer.stats)));
  }
  upload(
    name: string,
    mimeType: string,
    bytes: Uint8Array,
    timeoutMs = 60_000,
  ): Effect.Effect<Uploaded, ReactorError> {
    const self = this;
    return Effect.suspend(() => {
      let progress: UploadProgress = {
        allocation: "not-requested",
        transfer: "not-requested",
        notification: "not-submitted",
      };
      const operation = Effect.gen(function* () {
        const { c, remote, copy } = yield* pure(() => {
          const c = self.currentReady(),
            remote = self.currentRemote();
          nonempty(name, "upload name");
          nonempty(mimeType, "upload MIME type");
          positiveLimit(bytes.byteLength, "upload size", self.uploadBound);
          positiveLimit(timeoutMs, "upload timeout", 600_000);
          return { c, remote, copy: new Uint8Array(bytes) };
        });
        progress = { ...progress, allocation: "unknown" };
        const allocation = yield* self.guard(
          c,
          self.http.allocateUpload(remote.id, name, mimeType, copy.length),
        );
        const file: W.UploadReference = {
          upload_id: allocation.presigned_id,
          name,
          mime_type: mimeType,
          size: BigInt(copy.length),
        };
        progress = { ...progress, allocation: "confirmed", file, transfer: "unknown" };
        yield* self.guard(c, self.http.putUpload(allocation, copy, mimeType));
        progress = { ...progress, transfer: "confirmed" };
        progress = { ...progress, notification: "unknown" };
        yield* self.notification(c, { case: "file_uploaded", value: file }).pipe(
          Effect.tapError((error) =>
            Effect.sync(() => {
              if (error.context.outcome === "not-submitted")
                progress = { ...progress, notification: "not-submitted" };
            }),
          ),
        );
        progress = { ...progress, notification: "submitted" };
        return { file, transfer: "confirmed" as const, notification: "submitted" as const };
      });
      return withDeadline(operation, timeoutMs, "upload").pipe(
        Effect.mapError(
          (e) =>
            new ReactorError({
              code: "Upload",
              message: e.message,
              context: {
                ...e.context,
                operation: "upload",
                detail: { cause: e, progress: { ...progress } },
              },
            }),
        ),
        Effect.ensuring(
          Effect.sync(() =>
            self.emit({ _tag: "Upload", progress: Object.freeze({ ...progress }) }),
          ),
        ),
      );
    });
  }
  /** Idempotent close. Owned-session termination is attempted once; response is not terminal proof. */
  close(): Effect.Effect<CloseReport> {
    const self = this;
    return Effect.uninterruptible(
      Effect.suspend(() => {
        if (self.closeGate !== undefined) return Deferred.await(self.closeGate);
        const gate = Deferred.makeUnsafe<CloseReport>();
        self.closeGate = gate;
        return Effect.gen(function* () {
          self.lifecycle.transition("closing");
          Deferred.doneUnsafe(
            self.lifecycle.closing,
            Effect.fail(new ReactorError({ code: "Closed", message: "session closing" })),
          );
          const report = yield* cleanupSession({
            connection: self.lifecycle.connection,
            scope: self.lifecycle.scope,
            remote: self.remote,
            http: self.http,
            commandTimeout: self.commandTimeout,
            retire: (connection, error) => self.fail(connection, error),
          });
          self.closeReport = report;
          self.lifecycle.transition("closed");
          self.observations.end();
          Deferred.doneUnsafe(gate, Effect.succeed(report));
          // Consumer callbacks cannot undo or stall cleanup.
          yield* Effect.ignore(Effect.try(() => self.options.onClose?.(report)));
          return report;
        });
      }),
    );
  }
  isKnownTerminal(): boolean {
    return this.remote.isKnownTerminal();
  }
}
