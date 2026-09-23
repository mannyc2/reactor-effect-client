/**
 * The parent side of the isolated native host. Each peer forks its own child
 * process when the factory makes it and drives the child's `NativePeer` over
 * Effect RPC on one NodeWorker that never respawns. The parent keeps the
 * session's credentials, allocation, correlation and termination; a child that
 * crashes takes only its own connection generation with it.
 */
import { fork } from "node:child_process";
import type { ChildProcess } from "node:child_process";
import { fileURLToPath } from "node:url";
import * as NodeWorker from "@effect/platform-node/NodeWorker";
import * as Cause from "effect/Cause";
import * as Context from "effect/Context";
import * as Deferred from "effect/Deferred";
import type * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Predicate from "effect/Predicate";
import * as Queue from "effect/Queue";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import * as RpcClient from "effect/unstable/rpc/RpcClient";
import { RpcClientError } from "effect/unstable/rpc/RpcClientError";
import type { FromClientEncoded, FromServerEncoded } from "effect/unstable/rpc/RpcMessage";
import * as Worker from "effect/unstable/workers/Worker";
import { WorkerReceiveError, WorkerSendError } from "effect/unstable/workers/WorkerError";
import { PeerFactory, ReactorError } from "reactor-effect-client";
import type { PeerFactoryShape, Track } from "reactor-effect-client";
import { Observations, parsed } from "reactor-effect-client/host";
import type {
  AudioFrame,
  Channel,
  IceServer,
  MediaPressure,
  MediaTrack,
  Peer,
  PeerEvent,
  Prepared,
  RawMedia,
  VideoFrame,
} from "reactor-effect-client/host";
import { iceServers, validateNativeTracks } from "../peer.js";
import { eventFromWire, exact, fromWire, IsolatedRpcs } from "./protocol.js";
import type { PrepareItem, WireAudio, WireEvent, WireFailure, WireVideo } from "./protocol.js";

/**
 * The compiled child entry. The path climbs to the package root, so it
 * resolves identically from `dist/` and, in the package's own tests, from
 * `src/`, which therefore run the built child.
 */
const childEntry = fileURLToPath(
  new URL("../../../dist/_internal/isolated/child.js", import.meta.url),
);

/** Per-reader bounds, as the in-process host sets them. */
const VIDEO_READER = { capacity: 4, maxBytes: 64 * 1024 * 1024 } as const;
const AUDIO_READER = { capacity: 128, maxBytes: 4 * 1024 * 1024 } as const;
/** Chunks the RPC client buffers per stream: a frame per chunk keeps credit at one frame. */
const FRAME_BUFFER = 1;
const EVENT_BUFFER = 16;

const SHUTDOWN = "shutdown native WebRTC";

/**
 * Whether the protocol has handed the current call's request to its child.
 * The client runs the protocol's send in the caller's context, so a call
 * learns there whether a failure came before or after dispatch.
 */
class Dispatch extends Context.Service<Dispatch, { sent: boolean }>()(
  "reactor-effect-native/isolated/Dispatch",
) {}

type Client = RpcClient.FromGroup<typeof IsolatedRpcs, RpcClientError>;
type CallError = WireFailure | RpcClientError;

const isClientError = (u: unknown): u is RpcClientError => Predicate.isTagged(u, "RpcClientError");

/**
 * One child process and what its protocol observed. The host fences on it,
 * and the package's tests read it; it is not a package export.
 */
export class Link {
  child: ChildProcess | undefined;
  /**
   * Set once the child can take no request. Every later request fails before
   * dispatch: `Worker.send` would otherwise buffer it without bound for a
   * worker that will never become ready again.
   */
  fence: RpcClientError | undefined;
  exit: { readonly code: number | null; readonly signal: NodeJS.Signals | null } | undefined;
  readonly exited = Deferred.makeUnsafe<void>();
  spawns = 0;
  dispatched = 0;
  /** Replies and chunks that arrived for requests nobody awaits any longer. */
  late = 0;
  /** Events and frames that arrived after the peer was retired. */
  retired = 0;
  /** The most chunks of one stream the parent held unacknowledged. */
  maxUnacked = 0;
  /** The most frames one chunk of a track stream carried. */
  maxFramesPerChunk = 0;
  private readonly unacked = new Map<string, number>();
  private readonly frameStreams = new Set<string>();

  constructor(private readonly entry: string) {}

  spawn(): ChildProcess {
    this.spawns++;
    const child = fork(this.entry, [], {
      serialization: "advanced",
      // Neither the parent's runtime flags nor its environment, which may hold
      // credentials, reach the child.
      execArgv: [],
      env: {},
      stdio: ["ignore", "ignore", "inherit", "ipc"],
    });
    this.child = child;
    child.once("exit", (code, signal) => {
      this.exit = { code, signal };
      this.refuse();
      Deferred.doneUnsafe(this.exited, Exit.void);
    });
    // A closed channel cannot carry another reply; the process must end too.
    child.once("disconnect", () => {
      this.refuse();
      this.kill();
    });
    child.on("error", () => {
      this.refuse();
      // A child that never started emits no exit.
      if (child.pid === undefined) Deferred.doneUnsafe(this.exited, Exit.void);
    });
    return child;
  }

  refuse(): void {
    this.fence ??= new RpcClientError({
      reason: new WorkerSendError({ message: "native WebRTC child process is gone" }),
    });
  }

  kill(): void {
    if (this.child !== undefined && this.exit === undefined) this.child.kill("SIGKILL");
  }

  request(id: string, tag: string): void {
    if (tag === "Video" || tag === "Audio") this.frameStreams.add(id);
  }

  chunk(id: string, values: number): void {
    if (this.frameStreams.has(id) && values > this.maxFramesPerChunk)
      this.maxFramesPerChunk = values;
    const held = (this.unacked.get(id) ?? 0) + 1;
    this.unacked.set(id, held);
    if (held > this.maxUnacked) this.maxUnacked = held;
  }

  ack(id: string): void {
    const held = this.unacked.get(id);
    if (held !== undefined) this.unacked.set(id, held - 1);
  }

  settle(id: string): void {
    this.unacked.delete(id);
    this.frameStreams.delete(id);
  }
}

/**
 * A client protocol over one child that is spawned once and never again: a
 * dead child fails every pending request and fences every later one, and no
 * initial message is replayed. Replies route to the client id that `send`
 * named, never a fixed one, so each generation's client receives its own.
 */
const protocol = (link: Link) =>
  RpcClient.Protocol.make(
    Effect.fnUntraced(function* (writeResponse, clientIds) {
      const platform = yield* Worker.WorkerPlatform;
      const worker = yield* platform
        .spawn<FromServerEncoded, FromClientEncoded>(0)
        .pipe(Effect.provideService(Worker.Spawner, () => link.spawn()));
      const routes = new Map<string, number>();
      const broadcast = (response: FromServerEncoded) =>
        Effect.forEach(clientIds, (clientId) => writeResponse(clientId, response), {
          discard: true,
        });
      yield* worker
        .run((response) => {
          if (response._tag !== "Chunk" && response._tag !== "Exit") return broadcast(response);
          const id = String(response.requestId);
          const clientId = routes.get(id);
          if (clientId === undefined) {
            link.late++;
            return Effect.void;
          }
          if (response._tag === "Exit") {
            routes.delete(id);
            link.settle(id);
          } else link.chunk(id, response.values.length);
          return writeResponse(clientId, response);
        })
        .pipe(
          Effect.onExit(() =>
            Effect.suspend(() => {
              link.refuse();
              link.kill();
              routes.clear();
              return broadcast({
                _tag: "ClientProtocolError",
                error: new RpcClientError({
                  reason: new WorkerReceiveError({ message: "native WebRTC child process exited" }),
                }),
              });
            }),
          ),
          Effect.interruptible,
          // Fork the child now, inside `make`, so it starts while the session allocates.
          Effect.forkScoped({ startImmediately: true }),
        );
      const toClientError = (error: { readonly reason: RpcClientError["reason"] }) =>
        new RpcClientError({ reason: error.reason });
      const send = (clientId: number, request: FromClientEncoded) =>
        Effect.suspend(() => {
          if (link.fence !== undefined)
            return request._tag === "Request" ? Effect.fail(link.fence) : Effect.void;
          switch (request._tag) {
            case "Request": {
              const id = String(request.id);
              routes.set(id, clientId);
              link.request(id, request.tag);
              return worker.send(request).pipe(
                Effect.mapError((error) => {
                  routes.delete(id);
                  return toClientError(error);
                }),
                Effect.andThen(Effect.serviceOption(Dispatch)),
                Effect.map((mark) => {
                  link.dispatched++;
                  if (Option.isSome(mark)) mark.value.sent = true;
                }),
              );
            }
            case "Ack":
              link.ack(String(request.requestId));
              break;
            case "Interrupt":
              // A reply to an interrupted request is late: nobody awaits it.
              routes.delete(String(request.requestId));
              link.settle(String(request.requestId));
              break;
            default:
              break;
          }
          return Effect.mapError(worker.send(request), toClientError);
        });
      return {
        send,
        supportsAck: true,
        supportsTransferables: false,
        // The child's worker runner encodes with Schema's JSON codec, as every
        // worker protocol does; the byte fields are declarations it passes on.
        codecFor: Schema.toCodecJson,
      };
    }),
  );

export interface Environment {
  readonly platform: Context.Context<Worker.WorkerPlatform>;
  readonly libraryPath: string | undefined;
  readonly shutdownTimeout: Duration.Duration;
  readonly entry: string;
  readonly release: (peer: IsolatedPeer) => void;
}

const closedError = (operation: string): ReactorError =>
  ReactorError.fromCode("Closed", "native WebRTC peer is closed", {
    operation,
    outcome: "not-submitted",
  });

const videoFrame = (track: string, frame: WireVideo): VideoFrame =>
  Object.freeze({
    _tag: "VideoFrame",
    track,
    format: "BGRA",
    width: frame.width,
    height: frame.height,
    frameId: frame.frameId,
    timestampMicros: frame.timestampMicros,
    data: exact(frame.data),
    metadata: exact(frame.metadata),
  });

const audioFrame = (track: string, frame: WireAudio): AudioFrame =>
  Object.freeze({
    _tag: "AudioFrame",
    track,
    sampleRate: frame.sampleRate,
    channels: frame.channels,
    samples: exact(frame.samples),
  });

/**
 * One peer in a child process of its own. Its child is spawned as the peer is
 * made, so the child starts while the session allocates; every call waits for
 * the child to open its native peer.
 */
export class IsolatedPeer implements Peer {
  readonly nativeTracks = false;
  readonly rawMedia: RawMedia;
  readonly link: Link;
  private readonly scope = Scope.makeUnsafe();
  private readonly client = Deferred.makeUnsafe<Client, ReactorError>();
  private readonly opening: Fiber.Fiber<void>;
  private readonly video = new Map<string, Observations<VideoFrame>>();
  private readonly audio = new Map<string, Observations<AudioFrame>>();
  private readonly incoming = new Map<string, "video" | "audio">();
  private media: Scope.Scope | undefined;
  private emit: ((event: PeerEvent) => void) | undefined;
  private closed = false;
  private failureEmitted = false;
  private failure: ReactorError | undefined;
  private closing: Fiber.Fiber<void> | undefined;
  private finished: Deferred.Deferred<void, ReactorError> | undefined;

  constructor(private readonly environment: Environment) {
    this.link = new Link(environment.entry);
    this.opening = Effect.runForkWith(environment.platform)(this.open);
    this.rawMedia = Object.freeze({
      video: (name: string) =>
        this.track(name, "video", this.video, (client) =>
          Stream.map(
            client.Video({ name }, { streamBufferSize: FRAME_BUFFER }),
            (frame): readonly [VideoFrame, number] => {
              const owned = videoFrame(name, frame);
              return [owned, owned.data.byteLength + owned.metadata.byteLength + name.length * 2];
            },
          ),
        ).pipe(Stream.unwrap),
      audio: (name: string) =>
        this.track(name, "audio", this.audio, (client) =>
          Stream.map(
            client.Audio({ name }, { streamBufferSize: FRAME_BUFFER }),
            (frame): readonly [AudioFrame, number] => {
              const owned = audioFrame(name, frame);
              return [owned, owned.samples.byteLength + name.length * 2];
            },
          ),
        ).pipe(Stream.unwrap),
      snapshot: Effect.suspend(() => this.snapshot()),
    });
  }

  /** The child's native peer is open: succeeds once `Open` has replied. */
  get opened(): Effect.Effect<void, ReactorError> {
    return Effect.asVoid(Deferred.await(this.client));
  }

  private get open(): Effect.Effect<void, never, Worker.WorkerPlatform> {
    const self = this;
    return Effect.gen(function* () {
      const client = yield* RpcClient.make(IsolatedRpcs, { disableTracing: true }).pipe(
        Effect.provideServiceEffect(RpcClient.Protocol, protocol(self.link)),
        Scope.provide(self.scope),
      );
      const libraryPath = self.environment.libraryPath;
      yield* client.Open(libraryPath === undefined ? {} : { libraryPath });
      return client;
    }).pipe(
      Effect.onExit((exit) =>
        Deferred.done(
          self.client,
          Exit.isSuccess(exit) ? Exit.succeed(exit.value) : Exit.fail(self.openFailure(exit.cause)),
        ),
      ),
      // The outcome belongs to the peer's calls, which await it.
      Effect.ignoreCause,
    );
  }

  /** Nothing remote was dispatched while a child opened its peer, whatever failed. */
  private openFailure(cause: Cause.Cause<unknown>): ReactorError {
    const found = Cause.findError(cause);
    if (
      found._tag === "Success" &&
      !isClientError(found.success) &&
      !Predicate.isTagged(found.success, "WorkerError")
    )
      return fromWire(found.success as WireFailure);
    if (this.closed && Cause.hasInterruptsOnly(cause)) return closedError("open native WebRTC");
    return ReactorError.fromCode("Native", "native WebRTC child process failed to start", {
      operation: "open native WebRTC",
      outcome: "not-submitted",
      detail: Cause.squash(cause),
    });
  }

  private connected(operation: string): Effect.Effect<Client, ReactorError> {
    return Effect.suspend(() =>
      this.closed ? Effect.fail(closedError(operation)) : Deferred.await(this.client),
    );
  }

  /**
   * A failure after the call reached the protocol. The child's own failure
   * keeps its evidence. Otherwise the child or its channel failed, or the
   * wait ended from the RPC side: after dispatch the outcome is unknown,
   * before it the request was not submitted.
   */
  private lost(
    cause: Cause.Cause<CallError | Cause.Done>,
    operation: string,
    sent: boolean,
  ): ReactorError {
    const found = Cause.findError(cause);
    if (found._tag === "Success" && !isClientError(found.success) && !Cause.isDone(found.success))
      return fromWire(found.success);
    const outcome = sent ? "unknown" : "not-submitted";
    const detail = found._tag === "Success" ? found.success : Cause.squash(cause);
    if (this.closed && this.link.exit === undefined)
      return ReactorError.fromCode("Closed", "native WebRTC peer is closed", {
        operation,
        outcome,
        detail,
      });
    return ReactorError.fromCode(
      "Native",
      this.link.fence === undefined
        ? "native WebRTC child call failed"
        : "native WebRTC child process exited",
      {
        operation,
        outcome,
        detail: this.link.exit === undefined ? detail : { ...this.link.exit, cause: detail },
      },
    );
  }

  private call<A>(
    operation: string,
    run: (client: Client) => Effect.Effect<A, CallError>,
  ): Effect.Effect<A, ReactorError> {
    const self = this;
    return Effect.gen(function* () {
      const client = yield* self.connected(operation);
      const mark = { sent: false };
      const exit = yield* Effect.exit(run(client).pipe(Effect.provideService(Dispatch, mark)));
      if (Exit.isSuccess(exit)) return exit.value;
      return yield* self.lost(exit.cause, operation, mark.sent);
    });
  }

  private readerOverflows(): bigint {
    let overflows = 0n;
    for (const feed of this.video.values()) overflows += feed.overflowCount;
    for (const feed of this.audio.values()) overflows += feed.overflowCount;
    return overflows;
  }

  private snapshot(): Effect.Effect<MediaPressure, ReactorError> {
    return this.call("native media snapshot", (client) => client.Snapshot()).pipe(
      Effect.map((pressure): MediaPressure =>
        Object.freeze({
          ...pressure,
          readerOverflows: pressure.readerOverflows + this.readerOverflows(),
        }),
      ),
    );
  }

  /**
   * A track's readers share one observation, fed by one RPC stream from the
   * child for this generation, which the first reader opens.
   */
  private track<A>(
    name: string,
    kind: "video" | "audio",
    feeds: Map<string, Observations<A>>,
    open: (client: Client) => Stream.Stream<readonly [A, number], CallError>,
  ): Effect.Effect<Stream.Stream<A, ReactorError>, ReactorError> {
    const self = this;
    const bounds = kind === "video" ? VIDEO_READER : AUDIO_READER;
    return Effect.gen(function* () {
      yield* parsed(() => {
        if (self.incoming.get(name) !== kind)
          throw ReactorError.fromCode(
            "InvalidInput",
            "native media requires a declared receive track of the requested kind",
            { outcome: "not-submitted" },
          );
      });
      const existing = feeds.get(name);
      if (existing !== undefined) return existing.stream(bounds);
      const feed = new Observations<A>();
      feeds.set(name, feed);
      if (self.failure !== undefined) feed.fail(self.failure);
      else if (self.closed || self.media === undefined) feed.end();
      else yield* Effect.forkIn(self.pump(feed, open), self.media, { startImmediately: true });
      return feed.stream(bounds);
    });
  }

  private pump<A>(
    feed: Observations<A>,
    open: (client: Client) => Stream.Stream<readonly [A, number], CallError>,
  ): Effect.Effect<void> {
    const self = this;
    const operation = "native media";
    return Effect.gen(function* () {
      const client = yield* self.connected(operation);
      const mark = { sent: false };
      const exit = yield* Effect.exit(
        open(client).pipe(
          Stream.runForEach(([frame, bytes]) =>
            Effect.sync(() => {
              if (self.closed) self.link.retired++;
              else feed.emit(frame, bytes);
            }),
          ),
          Effect.provideService(Dispatch, mark),
        ),
      );
      // The child ended the track because its peer closed.
      if (Exit.isSuccess(exit)) feed.end();
      else if (!Cause.hasInterruptsOnly(exit.cause))
        feed.fail(self.lost(exit.cause, operation, mark.sent));
    }).pipe(Effect.catch((error) => Effect.sync(() => feed.fail(error))));
  }

  prepare(
    servers: readonly IceServer[],
    tracks: readonly Track[],
    emit: (event: PeerEvent) => void,
  ): Effect.Effect<Prepared, ReactorError, Scope.Scope> {
    const self = this;
    const operation = "prepare native WebRTC";
    return Effect.gen(function* () {
      // The in-process host's own checks, before anything reaches the child.
      const request = yield* parsed(() => {
        validateNativeTracks(tracks);
        return { servers: iceServers(servers), tracks: [...tracks] };
      });
      for (const track of tracks)
        if (track.direction === "recvonly") self.incoming.set(track.name, track.kind);
      self.emit = emit;
      self.media = yield* Effect.scope;
      const client = yield* self.connected(operation);
      const mark = { sent: false };
      const items = yield* client
        .Prepare(request, { asQueue: true, streamBufferSize: EVENT_BUFFER })
        .pipe(Effect.provideService(Dispatch, mark));
      const first = yield* Effect.exit(Queue.take(items));
      if (Exit.isFailure(first)) return yield* self.lost(first.cause, operation, mark.sent);
      const head = first.value;
      if (head._tag !== "Prepared")
        return yield* ReactorError.fromCode(
          "Protocol",
          "isolated native child sent an event before its offer",
        );
      yield* Effect.forkScoped(self.deliver(items));
      return Object.freeze({
        sdp: head.sdp,
        mapping: Object.freeze(head.mapping.map((entry) => Object.freeze({ ...entry }))),
      });
    });
  }

  /** The child's peer events, in order, until the peer retires. */
  private deliver(items: Queue.Dequeue<PrepareItem, CallError | Cause.Done>): Effect.Effect<void> {
    return Queue.takeAll(items).pipe(
      Effect.flatMap((batch) =>
        Effect.sync(() => {
          for (const item of batch) if (item._tag === "Event") this.receive(item.event);
        }),
      ),
      Effect.forever,
      Effect.catchCause((cause) =>
        Effect.sync(() => {
          if (Cause.hasInterruptsOnly(cause)) return;
          const found = Cause.findError(cause);
          // The child ended its events after shutdown.
          if (found._tag === "Success" && Cause.isDone(found.success)) return;
          this.fail(this.lost(cause, "native WebRTC events", true));
        }),
      ),
    );
  }

  private receive(wire: WireEvent): void {
    if (this.closed) {
      this.link.retired++;
      return;
    }
    const event = eventFromWire(wire);
    if (event.type === "error") this.fail(event.error);
    else this.emit?.(event);
  }

  private fail(error: ReactorError): void {
    if (this.closed) return;
    this.failure = error;
    try {
      if (!this.failureEmitted) {
        this.failureEmitted = true;
        this.emit?.({ type: "error", error });
      }
    } finally {
      this.close();
    }
  }

  answer(sdp: string): Effect.Effect<void, ReactorError> {
    return this.call("apply native SDP answer", (client) => client.Answer({ sdp }));
  }
  send(channel: Channel, bytes: Uint8Array<ArrayBuffer>): Effect.Effect<void, ReactorError> {
    return this.call(`send native ${channel}`, (client) => client.Send({ channel, bytes }));
  }
  direction(name: string, active: boolean): Effect.Effect<void, ReactorError> {
    return this.call("set native transceiver direction", (client) =>
      client.Direction({ name, active }),
    );
  }
  maxBitrate(name: string, bitsPerSecond: number): Effect.Effect<void, ReactorError> {
    return this.call("set native sender bitrate", (client) =>
      client.MaxBitrate({ name, bitsPerSecond }),
    );
  }
  get stats(): Effect.Effect<readonly unknown[], ReactorError> {
    return this.call("native WebRTC statistics", (client) => client.Stats()).pipe(
      Effect.map((stats) => Object.freeze([...stats])),
    );
  }

  lease(): MediaTrack {
    throw ReactorError.fromCode(
      "UnsupportedCapability",
      "native WebRTC exposes owned decoded samples, not browser MediaStreamTrack leases",
      { outcome: "not-submitted" },
    );
  }
  release(): void {
    /* lease never succeeds on this host. */
  }
  /** Only the absence of a track crosses the process boundary. */
  replace(name: string, track: MediaTrack | null): Effect.Effect<void, ReactorError> {
    if (track !== null)
      return Effect.fail(
        ReactorError.fromCode(
          "UnsupportedCapability",
          "native WebRTC does not accept browser MediaStreamTrack publication",
          { outcome: "not-submitted" },
        ),
      );
    return this.call("replace native track", (client) => client.Replace({ name }));
  }

  /**
   * Retire the peer in this process at once: no later event or frame reaches
   * the session, and every later call fails before dispatch. The child's
   * native peer closes when `shutdown` runs.
   */
  close(): void {
    if (this.closed) return;
    this.closed = true;
    this.emit = undefined;
    for (const feed of this.video.values()) {
      if (this.failure === undefined) feed.end();
      else feed.fail(this.failure);
    }
    for (const feed of this.audio.values()) {
      if (this.failure === undefined) feed.end();
      else feed.fail(this.failure);
    }
  }

  /**
   * Close, then ask the child to close and join its native peer, bounded by
   * `shutdownTimeout`. A child that has not exited by then is killed, and the
   * shutdown fails with `Shutdown`; so does a join the child reports failed.
   * Either way the child is gone once this completes, and its native state
   * with it. Idempotent: later runs await the first.
   */
  get shutdown(): Effect.Effect<void, ReactorError> {
    return Effect.suspend(() => {
      if (this.finished !== undefined) return Deferred.await(this.finished);
      const finished = Deferred.makeUnsafe<void, ReactorError>();
      this.finished = finished;
      return this.windDown.pipe(
        Effect.exit,
        Effect.flatMap((exit) => Deferred.done(finished, exit)),
        Effect.andThen(Deferred.await(finished)),
        Effect.ensuring(Effect.sync(() => this.environment.release(this))),
        Effect.uninterruptible,
      );
    });
  }

  private get windDown(): Effect.Effect<void, ReactorError> {
    const self = this;
    return Effect.gen(function* () {
      self.close();
      const joined = yield* self.join.pipe(
        Effect.exit,
        Effect.interruptible,
        Effect.timeoutOption(self.environment.shutdownTimeout),
      );
      yield* self.terminate;
      if (Option.isNone(joined))
        return yield* ReactorError.fromCode(
          "Shutdown",
          "native child shutdown exceeded its deadline; child process killed",
          { operation: SHUTDOWN },
        );
      if (Exit.isFailure(joined.value)) return yield* Effect.failCause(joined.value.cause);
    });
  }

  /** The graceful path: the child joins its native peer, then exits when its runner closes. */
  private get join(): Effect.Effect<void, ReactorError> {
    const self = this;
    return Effect.gen(function* () {
      const opened = yield* Effect.exit(Deferred.await(self.client));
      // A child that never opened, or is gone, holds no native peer to join.
      if (Exit.isFailure(opened) || self.link.fence !== undefined) return;
      const mark = { sent: false };
      const shut = yield* Effect.exit(
        opened.value.Shutdown().pipe(Effect.provideService(Dispatch, mark)),
      );
      if (Exit.isFailure(shut)) {
        const error = self.lost(shut.cause, SHUTDOWN, mark.sent);
        // Refused before dispatch: the child was already gone.
        if (!mark.sent && self.link.exit !== undefined) return;
        return yield* ReactorError.fromCode(
          "Shutdown",
          self.link.exit === undefined
            ? "native child shutdown failed; child process killed"
            : "native child process exited before its shutdown completed",
          { operation: SHUTDOWN, detail: error },
        );
      }
      self.closeScope();
      yield* Deferred.await(self.link.exited);
    });
  }

  /** The child is gone, by exit or by kill, and its client and worker are released. */
  private get terminate(): Effect.Effect<void> {
    const self = this;
    return Effect.gen(function* () {
      yield* Fiber.interrupt(self.opening);
      self.link.kill();
      if (self.link.child !== undefined) yield* Deferred.await(self.link.exited);
      yield* Fiber.await(self.closeScope());
    });
  }

  private closeScope(): Fiber.Fiber<void> {
    this.closing ??= Effect.runFork(Scope.close(this.scope, Exit.void));
    return this.closing;
  }
}

/**
 * The isolated factory: preflight by spawning a probe child that loads and
 * verifies the library and opens a native peer, then shuts it down.
 */
export const factory = (options: {
  readonly libraryPath: string | undefined;
  readonly shutdownTimeout: Duration.Duration;
}): Effect.Effect<PeerFactoryShape, ReactorError, Scope.Scope> =>
  Effect.gen(function* () {
    const platform = yield* Layer.build(NodeWorker.layerPlatform);
    const live = new Set<IsolatedPeer>();
    const environment: Environment = {
      platform,
      libraryPath: options.libraryPath,
      shutdownTimeout: options.shutdownTimeout,
      entry: childEntry,
      release: (peer) => live.delete(peer),
    };
    const probe = new IsolatedPeer(environment);
    const opened = yield* Effect.exit(probe.opened);
    yield* probe.shutdown;
    if (Exit.isFailure(opened)) return yield* Effect.failCause(opened.cause);
    // A peer its owner never shut down still ends with the layer.
    yield* Effect.addFinalizer(() =>
      Effect.forEach([...live], (peer) => Effect.exit(peer.shutdown), {
        concurrency: "unbounded",
        discard: true,
      }),
    );
    return PeerFactory.of({
      make: () => {
        const peer = new IsolatedPeer(environment);
        live.add(peer);
        return peer;
      },
    });
  });
