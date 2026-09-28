/**
 * The parent side of the isolated native host. Each peer forks its own child
 * process when it is made and drives the child's native peer over Effect RPC,
 * on a worker that never respawns. The parent keeps credentials, allocation,
 * correlation and termination; a child that crashes takes only its own
 * connection generation with it.
 */
// @effect-diagnostics-next-line nodeBuiltinImport:off -- NodeWorker's spawner takes a forked ChildProcess
import { fork } from "node:child_process";
// @effect-diagnostics-next-line nodeBuiltinImport:off -- NodeWorker's spawner takes a forked ChildProcess
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
import type * as Queue from "effect/Queue";
import * as Ref from "effect/Ref";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import * as RpcClient from "effect/unstable/rpc/RpcClient";
import { RpcClientError } from "effect/unstable/rpc/RpcClientError";
import type { FromClientEncoded, FromServerEncoded } from "effect/unstable/rpc/RpcMessage";
import * as Worker from "effect/unstable/workers/Worker";
import { WorkerReceiveError, WorkerSendError } from "effect/unstable/workers/WorkerError";
import type { AudioFrame, VideoFrame } from "reactor-effect-client/Media";
import { PeerFactory, trackFeed } from "reactor-effect-client/Peer";
import type { PeerEvent, TrackFeed } from "reactor-effect-client/Peer";
import { ReactorError } from "reactor-effect-client/ReactorError";
import * as Events from "../events.js";
import type { NativePeer } from "../peer.js";
import { take, takeAll } from "../queue.js";
import { eventFromWire, exact, fromWire, IsolatedRpcs } from "./protocol.js";
import type { PrepareItem, WireAudio, WireEvent, WireFailure, WireVideo } from "./protocol.js";

/**
 * The compiled child entry. The path climbs to the package root, so it resolves
 * identically from `dist/` and, in the package's own tests, from `src/`, which
 * therefore run the built child.
 */
const childEntry = fileURLToPath(
  new URL("../../../dist/internal/isolated/child.js", import.meta.url),
);

/** Per-reader bounds, as the in-process host sets them. */
const videoBounds = {
  capacity: 4,
  maxBytes: 64 * 1024 * 1024,
  bytes: (frame: VideoFrame) =>
    frame.data.byteLength + frame.metadata.byteLength + frame.track.length * 2,
};
const audioBounds = {
  capacity: 128,
  maxBytes: 4 * 1024 * 1024,
  bytes: (frame: AudioFrame) => frame.samples.byteLength + frame.track.length * 2,
};
/** Chunks the RPC client buffers per stream: a frame per chunk keeps credit at one frame. */
const frameBuffer = 1;
const eventBuffer = 16;

const SHUTDOWN = "shutdown native WebRTC";

/**
 * What the protocol did, for the package's tests to wait on; production
 * passes nothing. `Late` is a reply or chunk for a request nobody awaits any
 * longer, and `Retired` an event or frame that arrived after the peer closed.
 */
export type Trace =
  | { readonly _tag: "Spawned" }
  | { readonly _tag: "Dispatched"; readonly request: string }
  | {
      readonly _tag: "Chunk";
      readonly id: string;
      readonly request: string;
      readonly values: number;
    }
  | { readonly _tag: "Ack"; readonly id: string }
  | { readonly _tag: "Late" }
  | { readonly _tag: "Retired" };

/**
 * Whether the current call's request reached the child. RpcClient runs the
 * protocol's send in the caller's context, so a call learns there whether its
 * failure came before or after dispatch.
 */
const Dispatched = Context.Reference<Ref.Ref<boolean> | undefined>(
  "reactor-effect-native/internal/isolated/host/Dispatched",
  { defaultValue: () => undefined },
);

type Client = RpcClient.FromGroup<typeof IsolatedRpcs, RpcClientError>;
type CallError = WireFailure | RpcClientError;

const isClientError = (u: unknown): u is RpcClientError => Predicate.isTagged(u, "RpcClientError");

/**
 * One child process, as Node reports it. Its callbacks set these fields, so
 * the record is the child-process seam; the host reads it to fence calls.
 */
export interface Link {
  child: ChildProcess | undefined;
  /** Set once the child can take no request; every later request fails before dispatch. */
  fence: RpcClientError | undefined;
  exit: { readonly code: number | null; readonly signal: NodeJS.Signals | null } | undefined;
  readonly exited: Deferred.Deferred<void>;
}

const refuse = (link: Link): void => {
  link.fence ??= RpcClientError.make({
    reason: WorkerSendError.make({ message: "native WebRTC child process is gone" }),
  });
};

const kill = (link: Link): void => {
  if (link.child !== undefined && link.exit === undefined) link.child.kill("SIGKILL");
};

const spawn = (link: Link, entry: string, observe: (trace: Trace) => void): ChildProcess => {
  observe({ _tag: "Spawned" });
  const child = fork(entry, [], {
    serialization: "advanced",
    // Neither the parent's runtime flags nor its environment, which may hold
    // credentials, reach the child. Its own process group keeps a terminal's
    // interrupt from reaching it: the parent decides when it ends.
    execArgv: [],
    env: {},
    detached: true,
    stdio: ["ignore", "ignore", "inherit", "ipc"],
  });
  link.child = child;
  child.once("exit", (code, signal) => {
    link.exit = { code, signal };
    refuse(link);
    Deferred.doneUnsafe(link.exited, Exit.void);
  });
  // A closed channel cannot carry another reply; the process must end too.
  child.once("disconnect", () => {
    refuse(link);
    kill(link);
  });
  child.on("error", () => {
    refuse(link);
    // A child that never started emits no exit.
    if (child.pid === undefined) Deferred.doneUnsafe(link.exited, Exit.void);
  });
  return child;
};

/**
 * A client protocol over one child that is spawned once and never again: a
 * dead child fails every pending request and fences every later one, and no
 * message is replayed. Replies route to the client id that `send` named.
 */
const protocol = (link: Link, entry: string, observe: (trace: Trace) => void) =>
  RpcClient.Protocol.make(
    Effect.fnUntraced(function* (writeResponse, clientIds) {
      const platform = yield* Worker.WorkerPlatform;
      const worker = yield* platform
        .spawn<FromServerEncoded, FromClientEncoded>(0)
        .pipe(Effect.provideService(Worker.Spawner, () => spawn(link, entry, observe)));
      const routes = new Map<string, { readonly clientId: number; readonly request: string }>();
      const broadcast = (response: FromServerEncoded) =>
        Effect.forEach(clientIds, (clientId) => writeResponse(clientId, response), {
          discard: true,
        });
      yield* worker
        .run((response) => {
          if (response._tag !== "Chunk" && response._tag !== "Exit") return broadcast(response);
          const id = String(response.requestId);
          const route = routes.get(id);
          if (route === undefined) {
            observe({ _tag: "Late" });
            return Effect.void;
          }
          if (response._tag === "Exit") routes.delete(id);
          else
            observe({ _tag: "Chunk", id, request: route.request, values: response.values.length });
          return writeResponse(route.clientId, response);
        })
        .pipe(
          Effect.onExit(() =>
            Effect.suspend(() => {
              refuse(link);
              kill(link);
              routes.clear();
              return broadcast({
                _tag: "ClientProtocolError",
                error: RpcClientError.make({
                  reason: WorkerReceiveError.make({
                    message: "native WebRTC child process exited",
                  }),
                }),
              });
            }),
          ),
          Effect.interruptible,
          // Fork the child now, as the peer is made, so it starts while the session allocates.
          Effect.forkScoped({ startImmediately: true }),
        );
      const toClientError = (error: { readonly reason: RpcClientError["reason"] }) =>
        RpcClientError.make({ reason: error.reason });
      const send = (clientId: number, request: FromClientEncoded) =>
        Effect.gen(function* () {
          if (link.fence !== undefined) {
            if (request._tag === "Request") return yield* link.fence;
            return;
          }
          switch (request._tag) {
            case "Request": {
              const id = String(request.id);
              routes.set(id, { clientId, request: request.tag });
              yield* worker.send(request).pipe(
                Effect.mapError((error) => {
                  routes.delete(id);
                  return toClientError(error);
                }),
              );
              const mark = yield* Dispatched;
              if (mark !== undefined) yield* Ref.set(mark, true);
              observe({ _tag: "Dispatched", request: request.tag });
              return;
            }
            case "Ack":
              observe({ _tag: "Ack", id: String(request.requestId) });
              break;
            case "Interrupt":
              // A reply to an interrupted request is late: nobody awaits it.
              routes.delete(String(request.requestId));
              break;
            default:
              break;
          }
          yield* Effect.mapError(worker.send(request), toClientError);
        });
      return {
        send,
        supportsAck: true,
        supportsTransferables: false,
        // The child's runner encodes with Schema's JSON codec, as every worker
        // protocol does; the byte fields are declarations it passes on.
        codecFor: Schema.toCodecJson,
      };
    }),
  );

export interface Environment {
  readonly platform: Context.Context<Worker.WorkerPlatform>;
  readonly libraryPath: string | undefined;
  readonly shutdownTimeout: Duration.Duration;
  readonly entry: string;
  readonly observe: (trace: Trace) => void;
}

/** An isolated peer, the child it drives and whether that child opened its native peer. */
export interface IsolatedPeer extends NativePeer {
  readonly link: Readonly<Link>;
  readonly opened: Effect.Effect<void, ReactorError>;
}

interface State {
  readonly closed: boolean;
  readonly emit: ((event: PeerEvent) => void) | undefined;
  readonly video: ReadonlyMap<string, TrackFeed<VideoFrame>>;
  readonly audio: ReadonlyMap<string, TrackFeed<AudioFrame>>;
  readonly pumping: ReadonlySet<string>;
  readonly media: Scope.Scope | undefined;
  readonly failure: ReactorError | undefined;
}

const closedError = (operation: string): ReactorError =>
  ReactorError.fromCode("Closed", "native WebRTC peer is closed", {
    operation,
    outcome: "not-submitted",
  });

const videoFrame = (track: string, frame: WireVideo): VideoFrame => ({
  _tag: "VideoFrame",
  track,
  format: "BGRA",
  width: frame.width,
  height: frame.height,
  frameId: frame.frameId,
  timestampMicros: frame.timestampMicros,
  // The native admission sequence, so a frame the child's handoff evicted is a
  // gap here as it is for the in-process host.
  sequence: frame.sequence,
  data: exact(frame.data),
  metadata: exact(frame.metadata),
});

const audioFrame = (track: string, frame: WireAudio): AudioFrame => ({
  _tag: "AudioFrame",
  track,
  sampleRate: frame.sampleRate,
  channels: frame.channels,
  sequence: frame.sequence,
  samples: exact(frame.samples),
});

/** A stream's chunks, read with the queue workaround: rc.117's own read can miss a wakeup. */
const chunks = <A, E>(
  queue: Effect.Effect<Queue.Dequeue<A, E>, never, Scope.Scope>,
): Stream.Stream<A, Exclude<E, Cause.Done>> =>
  Stream.unwrap(Effect.map(queue, (items) => takeAll(items).pipe(Effect.succeed, Stream.fromPull)));

/**
 * One peer in a child process of its own. The child is spawned as the peer is
 * made, so it starts while the session allocates; every call waits for it to
 * open its native peer.
 */
export const make = Effect.fnUntraced(function* (
  environment: Environment,
): Effect.fn.Return<IsolatedPeer, never, Scope.Scope> {
  const scope = yield* Effect.scope;
  const link: Link = {
    child: undefined,
    fence: undefined,
    exit: undefined,
    exited: yield* Deferred.make<void>(),
  };
  const client = yield* Deferred.make<Client, ReactorError>();
  const clientScope = yield* Scope.fork(scope);
  const state = yield* Ref.make<State>({
    closed: false,
    emit: undefined,
    video: new Map(),
    audio: new Map(),
    pumping: new Set(),
    media: undefined,
    failure: undefined,
  });

  /** Nothing remote was dispatched while a child opened its peer, whatever failed. */
  const openFailure = (cause: Cause.Cause<unknown>, closed: boolean): ReactorError => {
    const found = Cause.findError(cause);
    if (
      found._tag === "Success" &&
      !isClientError(found.success) &&
      !Predicate.isTagged(found.success, "WorkerError")
    )
      // The child's own failures are the protocol's WireFailure records.
      return fromWire(found.success as WireFailure);
    if (closed && Cause.hasInterruptsOnly(cause)) return closedError("open native WebRTC");
    return ReactorError.fromCode("Native", "native WebRTC child process failed to start", {
      operation: "open native WebRTC",
      outcome: "not-submitted",
      detail: Cause.squash(cause),
    });
  };

  const opening = yield* Effect.gen(function* () {
    const rpc = yield* RpcClient.make(IsolatedRpcs, { disableTracing: true }).pipe(
      Effect.provideServiceEffect(
        RpcClient.Protocol,
        protocol(link, environment.entry, environment.observe),
      ),
      Scope.provide(clientScope),
    );
    const libraryPath = environment.libraryPath;
    yield* rpc.Open(libraryPath === undefined ? {} : { libraryPath });
    return rpc;
  }).pipe(
    Effect.onExit((exit) =>
      Effect.flatMap(Ref.get(state), (current) =>
        Deferred.done(
          client,
          Exit.isSuccess(exit)
            ? Exit.succeed(exit.value)
            : Exit.fail(openFailure(exit.cause, current.closed)),
        ),
      ),
    ),
    // The outcome belongs to the peer's calls, which await it.
    Effect.ignoreCause,
    Effect.provideContext(environment.platform),
    Effect.forkScoped({ startImmediately: true }),
  );

  const connected = (operation: string): Effect.Effect<Client, ReactorError> =>
    Effect.flatMap(Ref.get(state), (current) =>
      current.closed ? Effect.fail(closedError(operation)) : Deferred.await(client),
    );

  /**
   * A failure after the call reached the protocol. The child's own failure
   * keeps its evidence. Otherwise the child or its channel failed: after
   * dispatch the outcome is unknown, before it the request was not submitted.
   */
  const lost = (
    cause: Cause.Cause<CallError | Cause.Done>,
    operation: string,
    sent: boolean,
  ): Effect.Effect<ReactorError> =>
    Effect.map(Ref.get(state), (current) => {
      const found = Cause.findError(cause);
      if (found._tag === "Success" && !isClientError(found.success) && !Cause.isDone(found.success))
        return fromWire(found.success);
      const outcome = sent ? "unknown" : "not-submitted";
      const detail = found._tag === "Success" ? found.success : Cause.squash(cause);
      // IPC may close before Node reports the child's exit. A fenced link is
      // already lost, even if that exit has not reached this process yet.
      if (current.closed && link.exit === undefined && link.fence === undefined)
        return ReactorError.fromCode("Closed", "native WebRTC peer is closed", {
          operation,
          outcome,
          detail,
        });
      return ReactorError.fromCode(
        "Native",
        link.fence === undefined
          ? "native WebRTC child call failed"
          : "native WebRTC child process exited",
        {
          operation,
          outcome,
          detail: link.exit === undefined ? detail : { ...link.exit, cause: detail },
        },
      );
    });

  /** One call, and whether its request reached the child if it failed. */
  const call = <A>(
    operation: string,
    run: (rpc: Client) => Effect.Effect<A, CallError>,
  ): Effect.Effect<A, ReactorError> =>
    Effect.gen(function* () {
      const rpc = yield* connected(operation);
      const mark = yield* Ref.make(false);
      const exit = yield* Effect.exit(run(rpc).pipe(Effect.provideService(Dispatched, mark)));
      if (Exit.isSuccess(exit)) return exit.value;
      const error = yield* lost(exit.cause, operation, yield* Ref.get(mark));
      return yield* error;
    });

  /** Retire the peer here at once: no later event or frame reaches the session. */
  const close: Effect.Effect<void> = Effect.gen(function* () {
    const previous = yield* Ref.getAndUpdate(state, (current) => ({
      ...current,
      closed: true,
      emit: undefined,
    }));
    if (previous.closed) return;
    const failure = previous.failure;
    yield* Effect.forEach(
      [...previous.video.values(), ...previous.audio.values()],
      (feed) => (failure === undefined ? feed.end : feed.fail(failure)),
      { discard: true },
    );
  });

  const fail = (error: ReactorError): Effect.Effect<void> =>
    Effect.gen(function* () {
      const previous = yield* Ref.getAndUpdate(state, (current) =>
        current.closed || current.failure !== undefined ? current : { ...current, failure: error },
      );
      if (previous.closed || previous.failure !== undefined) return;
      previous.emit?.({ type: "error", error });
      yield* close;
    });

  const receive = (wire: WireEvent): Effect.Effect<void> =>
    Effect.flatMap(Ref.get(state), (current) => {
      if (current.closed) return Effect.sync(() => environment.observe({ _tag: "Retired" }));
      const event = eventFromWire(wire);
      if (event.type === "error") return fail(event.error);
      return Effect.sync(() => current.emit?.(event));
    });

  /** The child's peer events, in order, until the peer retires. */
  const deliver = (
    items: Queue.Dequeue<PrepareItem, CallError | Cause.Done>,
  ): Effect.Effect<void> =>
    Effect.suspend(() => takeAll(items)).pipe(
      Effect.flatMap((batch) =>
        Effect.forEach(
          batch,
          (item) => (item._tag === "Event" ? receive(item.event) : Effect.void),
          {
            discard: true,
          },
        ),
      ),
      Effect.forever,
      Effect.catchCause((cause) => {
        if (Cause.hasInterruptsOnly(cause)) return Effect.void;
        const found = Cause.findError(cause);
        // The child ended its events after shutdown.
        if (found._tag === "Success" && Cause.isDone(found.success)) return Effect.void;
        return Effect.flatMap(lost(cause, "native WebRTC events", true), fail);
      }),
    );

  /** A track's frames: its first reader opens one RPC stream from the child for this generation. */
  const track = <A, W>(
    kind: "video" | "audio",
    name: string,
    select: (current: State) => ReadonlyMap<string, TrackFeed<A>>,
    open: (rpc: Client) => Stream.Stream<W, CallError>,
    own: (frame: W) => A,
  ): Stream.Stream<A, ReactorError> =>
    Stream.unwrap(
      Effect.gen(function* () {
        const current = yield* Ref.get(state);
        const feed = select(current).get(name);
        if (feed === undefined)
          return yield* ReactorError.fromCode(
            "InvalidInput",
            `native ${kind} needs a declared receive track of that kind: ${name}`,
            { outcome: "not-submitted" },
          );
        const key = `${kind}:${name}`;
        const media = current.media;
        if (!current.pumping.has(key) && media !== undefined && !current.closed) {
          yield* Ref.update(state, (latest) => ({
            ...latest,
            pumping: new Set(latest.pumping).add(key),
          }));
          yield* pump(feed, open, own).pipe(Effect.forkIn(media, { startImmediately: true }));
        }
        return feed.stream;
      }),
    );

  const pump = <A, W>(
    feed: TrackFeed<A>,
    open: (rpc: Client) => Stream.Stream<W, CallError>,
    own: (frame: W) => A,
  ): Effect.Effect<void> =>
    Effect.gen(function* () {
      const operation = "native media";
      const rpc = yield* connected(operation);
      const mark = yield* Ref.make(false);
      const exit = yield* Effect.exit(
        open(rpc).pipe(
          Stream.runForEach((frame) =>
            Effect.flatMap(Ref.get(state), (current) =>
              current.closed
                ? Effect.sync(() => environment.observe({ _tag: "Retired" }))
                : feed.publish(own(frame)),
            ),
          ),
          Effect.provideService(Dispatched, mark),
        ),
      );
      // The child ended the track because its peer closed.
      if (Exit.isSuccess(exit)) return yield* feed.end;
      if (Cause.hasInterruptsOnly(exit.cause)) return;
      const sent = yield* Ref.get(mark);
      yield* feed.fail(yield* lost(exit.cause, operation, sent));
    }).pipe(Effect.catch((error) => feed.fail(error)));

  const pressure = Effect.gen(function* () {
    const snapshot = yield* call("native media snapshot", (rpc) => rpc.Snapshot());
    const current = yield* Ref.get(state);
    let readerOverflows = snapshot.readerOverflows;
    for (const feed of [...current.video.values(), ...current.audio.values()])
      readerOverflows += yield* feed.overflows;
    return { ...snapshot, readerOverflows };
  });

  /** The graceful path: the child joins its native peer, then exits when its runner closes. */
  const join = Effect.gen(function* () {
    const opened = yield* Deferred.await(client).pipe(Effect.exit);
    // A child that never opened, or is gone, holds no native peer to join.
    if (Exit.isFailure(opened) || link.fence !== undefined) return;
    const mark = yield* Ref.make(false);
    const shut = yield* Effect.exit(
      opened.value.Shutdown().pipe(Effect.provideService(Dispatched, mark)),
    );
    if (Exit.isFailure(shut)) {
      const sent = yield* Ref.get(mark);
      const error = yield* lost(shut.cause, SHUTDOWN, sent);
      // Refused before dispatch: the child was already gone.
      if (!sent && link.exit !== undefined) return;
      return yield* ReactorError.fromCode(
        "Shutdown",
        link.exit === undefined
          ? "native child shutdown failed; child process killed"
          : "native child process exited before its shutdown completed",
        { operation: SHUTDOWN, detail: error },
      );
    }
    yield* Effect.forkChild(Scope.close(clientScope, Exit.void));
    yield* Deferred.await(link.exited);
  });

  /** The child is gone, by exit or by kill, and its client and worker are released. */
  const terminate = Effect.gen(function* () {
    yield* Fiber.interrupt(opening);
    kill(link);
    if (link.child !== undefined) yield* Deferred.await(link.exited);
    yield* Scope.close(clientScope, Exit.void);
  });

  const finished = yield* Deferred.make<void, ReactorError>();
  const shutdownStarted = yield* Ref.make(false);
  const explicit = yield* Ref.make(false);
  /**
   * Close, then ask the child to close and join its native peer within
   * `shutdownTimeout`. A child that has not exited by then is killed and the
   * shutdown fails with `Shutdown`; so does a join the child reports failed.
   * Either way the child is gone once this completes. Later runs await the first.
   */
  const run: Effect.Effect<void, ReactorError> = Effect.gen(function* () {
    if (!(yield* Ref.getAndSet(shutdownStarted, true))) {
      const outcome = yield* Effect.exit(
        Effect.gen(function* () {
          yield* close;
          const joined = yield* join.pipe(
            Effect.exit,
            Effect.interruptible,
            Effect.timeoutOption(environment.shutdownTimeout),
          );
          yield* terminate;
          if (Option.isNone(joined))
            return yield* ReactorError.fromCode(
              "Shutdown",
              "native child shutdown exceeded its deadline; child process killed",
              { operation: SHUTDOWN },
            );
          if (Exit.isFailure(joined.value)) return yield* Effect.failCause(joined.value.cause);
        }),
      );
      yield* Deferred.done(finished, outcome);
    }
    return yield* Deferred.await(finished);
  }).pipe(Effect.uninterruptible);
  const shutdown = Ref.set(explicit, true).pipe(Effect.andThen(run));
  // A shutdown nobody ran explicitly reports a failure by dying, so the
  // session's close report keeps it; one a caller ran already reported to it.
  yield* Effect.addFinalizer(() =>
    Effect.flatMap(Ref.get(explicit), (reported) =>
      reported ? Effect.ignore(run) : Effect.orDie(run),
    ),
  );

  return {
    link,
    opened: Deferred.await(client).pipe(Effect.asVoid),
    media: {
      _tag: "Decoded",
      video: (name) =>
        track(
          "video",
          name,
          (current) => current.video,
          (rpc) => chunks(rpc.Video({ name }, { streamBufferSize: frameBuffer, asQueue: true })),
          (frame: WireVideo) => videoFrame(name, frame),
        ),
      audio: (name) =>
        track(
          "audio",
          name,
          (current) => current.audio,
          (rpc) => chunks(rpc.Audio({ name }, { streamBufferSize: frameBuffer, asQueue: true })),
          (frame: WireAudio) => audioFrame(name, frame),
        ),
      pressure,
    },
    prepare: (servers, tracks, emit) =>
      Effect.gen(function* () {
        const operation = "prepare native WebRTC";
        // The in-process host's own checks, before anything reaches the child.
        const input = yield* Events.prepareInput({ servers, tracks });
        const incoming = <A>(kind: "video" | "audio", bounds: Parameters<typeof trackFeed<A>>[0]) =>
          Effect.map(
            Effect.forEach(
              tracks.filter((entry) => entry.direction === "recvonly" && entry.kind === kind),
              (entry) => Effect.map(trackFeed<A>(bounds), (feed) => [entry.name, feed] as const),
            ),
            (entries): ReadonlyMap<string, TrackFeed<A>> => new Map(entries),
          );
        const video = yield* incoming<VideoFrame>("video", videoBounds);
        const audio = yield* incoming<AudioFrame>("audio", audioBounds);
        const media = yield* Effect.scope;
        yield* Ref.update(state, (current) =>
          current.closed ? current : { ...current, emit, video, audio, media },
        );
        const rpc = yield* connected(operation);
        const mark = yield* Ref.make(false);
        const items = yield* rpc
          .Prepare(input, { asQueue: true, streamBufferSize: eventBuffer })
          .pipe(Effect.provideService(Dispatched, mark));
        const first = yield* Effect.exit(take(items));
        if (Exit.isFailure(first)) {
          const error = yield* lost(first.cause, operation, yield* Ref.get(mark));
          return yield* error;
        }
        const head = first.value;
        if (head._tag !== "Prepared")
          return yield* ReactorError.fromCode(
            "Protocol",
            "isolated native child sent an event before its offer",
          );
        yield* Effect.forkScoped(deliver(items));
        return { sdp: head.sdp, mapping: head.mapping };
      }),
    answer: (sdp) => call("apply native SDP answer", (rpc) => rpc.Answer({ sdp })),
    send: (channel, bytes) => call(`send native ${channel}`, (rpc) => rpc.Send({ channel, bytes })),
    direction: (name, active) =>
      call("set native transceiver direction", (rpc) => rpc.Direction({ name, active })),
    maxBitrate: (name, bitsPerSecond) =>
      call("set native sender bitrate", (rpc) => rpc.MaxBitrate({ name, bitsPerSecond })),
    stats: call("native WebRTC statistics", (rpc) => rpc.Stats()),
    close,
    shutdown,
  };
});

export interface EnvironmentOptions {
  readonly libraryPath: string | undefined;
  readonly shutdownTimeout: Duration.Duration;
  readonly observe?: ((trace: Trace) => void) | undefined;
}

/** The platform, library and deadline every peer of one isolated host shares. */
export const environment = Effect.fnUntraced(function* (
  options: EnvironmentOptions,
): Effect.fn.Return<Environment, never, Scope.Scope> {
  return {
    platform: yield* Layer.build(NodeWorker.layerPlatform),
    libraryPath: options.libraryPath,
    shutdownTimeout: options.shutdownTimeout,
    entry: childEntry,
    observe: options.observe ?? (() => undefined),
  };
});

/**
 * The isolated factory. Building it spawns a probe child that loads and
 * verifies the library and opens a native peer, then shuts it down, so an
 * unsupported host fails before any session is allocated.
 */
export const factory = Effect.fnUntraced(function* (
  options: EnvironmentOptions,
): Effect.fn.Return<PeerFactory["Service"], ReactorError, Scope.Scope> {
  const shared = yield* environment(options);
  yield* Effect.scoped(
    Effect.gen(function* () {
      const probe = yield* make(shared);
      const opened = yield* Effect.exit(probe.opened);
      yield* probe.shutdown;
      if (Exit.isFailure(opened)) return yield* Effect.failCause(opened.cause);
    }),
  );
  return PeerFactory.of({ check: Effect.void, make: make(shared) });
});
