/**
 * The parent side of the isolated native peer. Each peer forks its own child
 * process as it is made, and the same peer that runs in process drives the
 * child's addon over Effect RPC, on a worker that never respawns. The parent
 * keeps credentials, allocation, correlation and termination; a child that
 * crashes takes only its own connection generation with it.
 */
// NodeWorker's spawner takes the ChildProcess Node's fork returns, with its IPC channel.
// @effect-diagnostics-next-line nodeBuiltinImport:off
import { type ChildProcess, fork } from "node:child_process";
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
import { PeerFactory } from "reactor-effect-client/Peer";
import { ReactorError } from "reactor-effect-client/ReactorError";
import * as Peer from "../peer.js";
import { takeAll } from "../queue.js";
import { Failure, IsolatedRpcs, OpenFailure } from "./protocol.js";

/**
 * The compiled child entry. The path climbs to the package root, so it resolves
 * identically from `dist/` and, in the package's own tests, from `src/`, which
 * therefore run the built child.
 */
const childEntry = fileURLToPath(
  new URL("../../../dist/internal/isolated/child.js", import.meta.url),
);

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
type CallError = typeof Failure.Type | RpcClientError;

const isFailure = Schema.is(Failure);
const isOpenFailure = Schema.is(OpenFailure);

/**
 * One child process, as Node reports it. Its callbacks set these fields, so
 * the record is the child-process seam; the peer reads it to fence calls.
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

const spawn = (link: Link, entry: string): ChildProcess => {
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
 * message is replayed.
 */
const protocol = (link: Link, entry: string) =>
  RpcClient.Protocol.make(
    Effect.fnUntraced(function* (writeResponse, clientIds) {
      const platform = yield* Worker.WorkerPlatform;
      const worker = yield* platform
        .spawn<FromServerEncoded, FromClientEncoded>(0)
        .pipe(Effect.provideService(Worker.Spawner, () => spawn(link, entry)));
      const broadcast = (response: FromServerEncoded) =>
        Effect.forEach(clientIds, (clientId) => writeResponse(clientId, response), {
          discard: true,
        });
      yield* worker.run(broadcast).pipe(
        Effect.onExit(() =>
          Effect.suspend(() => {
            refuse(link);
            kill(link);
            return broadcast({
              _tag: "ClientProtocolError",
              error: RpcClientError.make({
                reason: WorkerReceiveError.make({ message: "native WebRTC child process exited" }),
              }),
            });
          }),
        ),
        Effect.interruptible,
        // Fork the child now, as the peer is made, so it starts while the session allocates.
        Effect.forkScoped({ startImmediately: true }),
      );
      const send = (_clientId: number, message: FromClientEncoded) =>
        Effect.gen(function* () {
          if (link.fence !== undefined) {
            if (message._tag === "Request") return yield* link.fence;
            return;
          }
          yield* Effect.mapError(worker.send(message), (error) =>
            RpcClientError.make({ reason: error.reason }),
          );
          const mark = yield* Dispatched;
          if (message._tag === "Request" && mark !== undefined) yield* Ref.set(mark, true);
        });
      return {
        send,
        supportsAck: true,
        supportsTransferables: false,
        // The child's runner encodes with Schema's JSON codec, as every worker
        // protocol does; the addon's values are declarations it passes on.
        codecFor: Schema.toCodecJson,
      };
    }),
  );

export interface Environment {
  readonly platform: Context.Context<Worker.WorkerPlatform>;
  readonly addon: string | undefined;
  readonly shutdownTimeout: Duration.Duration;
  readonly entry: string;
}

/** A child's addon peer, and the child, which the package's own tests watch. */
export interface Remote extends Peer.NativeHandle {
  readonly link: Readonly<Link>;
  readonly opened: Effect.Effect<void, ReactorError>;
}

const SHUTDOWN = "shutdown native WebRTC";

/** Chunks the client buffers per queue: media credit stays one frame. */
const buffers = { events: 16, media: 1 };

/**
 * A handle on the addon peer in a child process of its own. The child is
 * spawned as the handle is made; every call waits for it to open its peer.
 */
export const remote = Effect.fnUntraced(function* (
  environment: Environment,
): Effect.fn.Return<Remote, never, Scope.Scope> {
  const link: Link = {
    child: undefined,
    fence: undefined,
    exit: undefined,
    exited: yield* Deferred.make<void>(),
  };
  const client = yield* Deferred.make<Client, ReactorError>();
  const clientScope = yield* Scope.fork(yield* Effect.scope);
  const closed = yield* Ref.make(false);

  /** Nothing was dispatched while the child opened its peer, whatever failed. */
  const openFailure = (cause: Cause.Cause<unknown>): ReactorError => {
    const context = { operation: "open native WebRTC", outcome: "not-submitted" } as const;
    const found = Cause.findError(cause);
    if (found._tag === "Success" && isOpenFailure(found.success))
      return ReactorError.fromCode(found.success.code, found.success.message, context);
    if (Cause.hasInterruptsOnly(cause))
      return ReactorError.fromCode("Closed", "native WebRTC peer is closed", context);
    return ReactorError.fromCode("Native", "native WebRTC child process failed to start", {
      ...context,
      detail: Cause.squash(cause),
    });
  };

  const opening = yield* Effect.gen(function* () {
    const rpc = yield* RpcClient.make(IsolatedRpcs, { disableTracing: true }).pipe(
      Effect.provideServiceEffect(RpcClient.Protocol, protocol(link, environment.entry)),
      Scope.provide(clientScope),
    );
    yield* rpc.Open(environment.addon === undefined ? {} : { addon: environment.addon });
    return rpc;
  }).pipe(
    Effect.onExit((exit) =>
      Deferred.done(
        client,
        Exit.isSuccess(exit) ? Exit.succeed(exit.value) : exit.cause.pipe(openFailure, Exit.fail),
      ),
    ),
    // The outcome belongs to the peer's calls, which await it.
    Effect.ignoreCause,
    Effect.provideContext(environment.platform),
    Effect.forkScoped({ startImmediately: true }),
  );

  /**
   * A failed call. The addon's own failure keeps its class. Otherwise the
   * child or its channel failed: after dispatch the outcome is unknown, before
   * it the request was not submitted.
   */
  const lost = (cause: Cause.Cause<unknown>, operation: string, sent: boolean): ReactorError => {
    const found = Cause.findError(cause);
    if (found._tag === "Success" && isFailure(found.success))
      return Peer.nativeFailure(operation)(found.success);
    const detail = found._tag === "Success" ? found.success : Cause.squash(cause);
    return ReactorError.fromCode(
      "Native",
      link.fence === undefined
        ? "native WebRTC child call failed"
        : "native WebRTC child process exited",
      {
        operation,
        outcome: sent ? "unknown" : "not-submitted",
        detail: link.exit === undefined ? detail : { ...link.exit, cause: detail },
      },
    );
  };

  /** One call, and whether its request reached the child if it failed. */
  const call = <A>(
    operation: string,
    run: (rpc: Client) => Effect.Effect<A, CallError>,
  ): Effect.Effect<A, ReactorError> =>
    Effect.gen(function* () {
      if (yield* Ref.get(closed))
        return yield* ReactorError.fromCode("Closed", "native WebRTC peer is closed", {
          operation,
          outcome: "not-submitted",
        });
      const rpc = yield* Deferred.await(client);
      const mark = yield* Ref.make(false);
      const exit = yield* run(rpc).pipe(Effect.provideService(Dispatched, mark), Effect.exit);
      if (Exit.isSuccess(exit)) return exit.value;
      return yield* lost(exit.cause, operation, yield* Ref.get(mark));
    });

  /** One queue's items, as the peer pulls them; its end is the child's peer closing. */
  const queue = <A>(
    open: (
      rpc: Client,
    ) => Effect.Effect<Queue.Dequeue<A, CallError | Cause.Done>, never, Scope.Scope>,
  ): Stream.Stream<A, ReactorError> =>
    Deferred.await(client).pipe(
      Effect.flatMap(open),
      Effect.map((items) =>
        takeAll(items).pipe(
          Effect.mapError((error) =>
            Cause.isDone(error) ? error : lost(Cause.fail(error), "native WebRTC queue", true),
          ),
          Effect.succeed,
          Stream.fromPull,
        ),
      ),
      Stream.unwrap,
    );

  /** The child is gone, by exit or by kill, and its client and worker are released. */
  const terminate = Effect.gen(function* () {
    yield* Fiber.interrupt(opening);
    kill(link);
    if (link.child !== undefined) yield* Deferred.await(link.exited);
    yield* Scope.close(clientScope, Exit.void);
  });

  return {
    link,
    opened: Deferred.await(client).pipe(Effect.asVoid),
    prepare: (servers, tracks) =>
      call("prepare native WebRTC", (rpc) => rpc.Prepare({ servers, tracks })),
    answer: (sdp) => call("apply native SDP answer", (rpc) => rpc.Answer({ sdp })),
    direction: (name, active) =>
      call("set native transceiver direction", (rpc) => rpc.Direction({ name, active })),
    maxBitrate: (name, bitsPerSecond) =>
      call("set native sender bitrate", (rpc) => rpc.MaxBitrate({ name, bitsPerSecond })),
    send: (channel, bytes) => call(`send native ${channel}`, (rpc) => rpc.Send({ channel, bytes })),
    stats: call("native WebRTC statistics", (rpc) => rpc.Stats()),
    pressure: call("native media snapshot", (rpc) => rpc.Pressure()),
    events: queue((rpc) =>
      rpc.Events(undefined, { asQueue: true, streamBufferSize: buffers.events }),
    ),
    video: queue((rpc) => rpc.Video(undefined, { asQueue: true, streamBufferSize: buffers.media })),
    audio: queue((rpc) => rpc.Audio(undefined, { asQueue: true, streamBufferSize: buffers.media })),
    close: Ref.set(closed, true),
    // The graceful path: the child joins its addon peer, then exits once its
    // runner closes. A child that never opened, or is gone, has none to join.
    shutdown: Effect.gen(function* () {
      yield* Ref.set(closed, true);
      const opened = yield* Deferred.await(client).pipe(Effect.exit);
      if (Exit.isSuccess(opened) && link.fence === undefined) {
        const mark = yield* Ref.make(false);
        const joined = yield* Effect.exit(
          opened.value.Shutdown().pipe(Effect.provideService(Dispatched, mark)),
        );
        if (Exit.isFailure(joined)) {
          const error = lost(joined.cause, SHUTDOWN, yield* Ref.get(mark));
          yield* terminate;
          return yield* ReactorError.fromCode(
            "Shutdown",
            "native child shutdown failed; child process killed",
            { operation: SHUTDOWN, detail: error },
          );
        }
        yield* Effect.forkChild(Scope.close(clientScope, Exit.void));
        yield* Deferred.await(link.exited);
      }
      yield* terminate;
    }),
    abandon: Effect.as(
      terminate,
      ReactorError.fromCode(
        "Shutdown",
        "native child shutdown exceeded its deadline; child process killed",
        { operation: SHUTDOWN },
      ),
    ),
  };
});

/** An isolated peer, the child it drives and whether that child opened its addon peer. */
export interface IsolatedPeer extends Peer.NativePeer {
  readonly link: Readonly<Link>;
  readonly opened: Effect.Effect<void, ReactorError>;
}

export const make = (environment: Environment): Effect.Effect<IsolatedPeer, never, Scope.Scope> =>
  Effect.flatMap(remote(environment), (handle) =>
    Effect.map(Peer.make(handle, environment.shutdownTimeout), (peer) => ({
      ...peer,
      link: handle.link,
      opened: handle.opened,
    })),
  );

/** The worker platform, addon and deadline every peer of one isolated factory shares. */
export const environment = (options: {
  readonly addon: string | undefined;
  readonly shutdownTimeout: Duration.Duration;
}) =>
  Effect.map(Layer.build(NodeWorker.layerPlatform), (platform): Environment => ({
    ...options,
    platform,
    entry: childEntry,
  }));

/**
 * The isolated factory. Building it spawns a probe child that loads the addon
 * and opens a peer, then shuts it down, so an unsupported host fails before
 * any session is allocated.
 */
export const factory = Effect.fnUntraced(function* (options: {
  readonly addon: string | undefined;
  readonly shutdownTimeout: Duration.Duration;
}): Effect.fn.Return<PeerFactory["Service"], ReactorError, Scope.Scope> {
  const shared = yield* environment(options);
  yield* Effect.scoped(
    Effect.gen(function* () {
      const probe = yield* make(shared);
      const opened = yield* Effect.exit(probe.opened);
      yield* probe.shutdown;
      if (Exit.isFailure(opened)) return yield* opened;
    }),
  );
  return PeerFactory.of({ check: Effect.void, make: make(shared) });
});
