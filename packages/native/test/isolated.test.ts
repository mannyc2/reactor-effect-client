import { spawn, spawnSync } from "node:child_process";
import { createInterface } from "node:readline";
import { fileURLToPath } from "node:url";
import * as Cause from "effect/Cause";
import * as Context from "effect/Context";
import type * as Crypto from "effect/Crypto";
import * as Deferred from "effect/Deferred";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Redacted from "effect/Redacted";
import * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import * as FetchHttpClient from "effect/unstable/http/FetchHttpClient";
import type * as PlatformHttp from "effect/unstable/http/HttpClient";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { afterAll, beforeAll, describe, expect, test } from "vitest";
import * as Reactor from "reactor-effect-client/Reactor";
import { PeerFactory } from "reactor-effect-client/Peer";
import type { ReactorError } from "reactor-effect-client/ReactorError";
import * as Coordinator from "reactor-effect-client/Coordinator";
import type { IceCandidate } from "reactor-effect-client/Coordinator";
import type { PeerEvent } from "reactor-effect-client/Peer";
import type { VideoFrame } from "reactor-effect-client/Media";
import type { IsolatedPeer } from "../src/internal/isolated/host.js";
import { defaultShutdownTimeout } from "../src/internal/peer.js";
import * as NativePeer from "../src/NativePeer.js";
import { assertExactFrames, decoded, FarPeer, makeFakeAddon, record, until } from "./support.js";

/*
 * The isolated host forks the built child entry, dist/internal/isolated/child.js,
 * from source and package alike, so these tests need `bun run build` first. Its
 * parent must be Node; under Bun only the refusal runs.
 */
const isBun = process.versions.bun !== undefined;
const onNode = !isBun && process.platform !== "win32";

const packageRoot = fileURLToPath(new URL("..", import.meta.url));

const tracks = [
  { name: "main_video", kind: "video", direction: "recvonly" },
  { name: "main_audio", kind: "audio", direction: "recvonly" },
  { name: "input_audio", kind: "audio", direction: "sendonly" },
] as const;

/** A healthy close, the child's join and exit included, takes under a fifth of the default deadline. */
const closeBound = Duration.toMillis(defaultShutdownTimeout) / 5;

/** Whether `pid` names a live process: a zombie awaiting its reaper does not count. */
const alive = (pid: number): boolean => {
  try {
    process.kill(pid, 0);
  } catch {
    return false;
  }
  const state = spawnSync("ps", ["-o", "stat=", "-p", String(pid)], { encoding: "utf8" });
  const stat = state.stdout.trim();
  return stat !== "" && !stat.startsWith("Z");
};

const sessionId = "sess_isolated_fixture";
const descriptor = (id: string) => ({
  session_id: id,
  state: "ACTIVE",
  capabilities: {
    protocol_version: "1.0",
    tracks,
    commands: [{ name: "echo", schema: {} }],
  },
  selected_transport: { protocol: "webrtc", version: "1.0" },
});

/** A coordinator that allocates one session per POST; a deleted session reads as absent. */
const coordinator = () => {
  const allocated: string[] = [],
    deleted = new Set<string>();
  const fetch = async (input: string | Request | URL, init?: RequestInit): Promise<Response> => {
    const request = new Request(input, init);
    const path = new URL(request.url).pathname;
    const id = /^\/sessions\/([^/]+)/.exec(path)?.[1] ?? sessionId;
    if (path === "/sessions" && request.method === "POST") {
      const allocation = allocated.length === 0 ? sessionId : `${sessionId}_${allocated.length}`;
      allocated.push(allocation);
      return Response.json(descriptor(allocation));
    }
    if (path === `/sessions/${id}` && request.method === "GET")
      return deleted.has(id)
        ? Response.json({ error: "session not found" }, { status: 404 })
        : Response.json(descriptor(id));
    if (path.endsWith("/ice_servers")) return Response.json({ ice_servers: [] });
    if (path.endsWith("/connections")) return Response.json({ connection_id: 1001 });
    if (path.endsWith("/ice_candidates")) return new Response(null, { status: 204 });
    if (path.endsWith("/sdp_params")) {
      if (request.method !== "GET") return new Response(null, { status: 204 });
      return Response.json({ sdp_answer: "fixture native answer" });
    }
    if (path === `/sessions/${id}` && request.method === "DELETE") {
      deleted.add(id);
      return new Response(null, { status: 202 });
    }
    return Response.json({ error: `unhandled fixture route ${path}` }, { status: 404 });
  };
  return { fetch, allocated, deleted };
};

const runClient = <A, E>(
  effect: Effect.Effect<A, E, PlatformHttp.HttpClient | Crypto.Crypto>,
  fetch: typeof globalThis.fetch,
): Promise<A> =>
  Effect.runPromise(
    effect.pipe(
      Effect.provide(Layer.merge(FetchHttpClient.layer, NodeServices.layer)),
      Effect.provideService(FetchHttpClient.Fetch, fetch),
    ),
  );

/** The isolated factory, built in the caller's scope. */
const isolatedFactory = (options: NativePeer.Options) =>
  Layer.build(NativePeer.layerIsolated(options)).pipe(
    Effect.map((context) => Context.get(context, PeerFactory)),
  );

/** Isolated peers over the environment the factory builds, so a test can drive one child directly. */
const isolatedPeers = (options: NativePeer.Options) =>
  Effect.gen(function* () {
    const host = yield* Effect.promise(() => import("../src/internal/isolated/host.js"));
    const environment = yield* host.environment({
      addon: options.addon,
      shutdownTimeout: Duration.fromInputUnsafe(options.shutdownTimeout ?? "10 seconds"),
    });
    return { make: host.make(environment) };
  });

/** The canonical factory over the isolated host, recording each peer it makes. */
const isolatedClient = (
  settings: Coordinator.Options & Reactor.Options,
  options: NativePeer.Options,
  peers: IsolatedPeer[],
) =>
  Effect.gen(function* () {
    const factory = yield* isolatedFactory(options);
    const made = yield* isolatedPeers(options);
    const services = yield* Layer.build(Coordinator.layer(settings));
    return yield* Reactor.make(settings).pipe(
      Effect.provide(services),
      Effect.provideService(
        PeerFactory,
        PeerFactory.of({
          check: factory.check,
          make: Effect.tap(made.make, (peer) => Effect.sync(() => peers.push(peer))),
        }),
      ),
    );
  });

const create = { model: "fixture/native-session", jwt: Redacted.make("fixture-token") };

describe("isolated native host", () => {
  test.runIf(isBun)("refuses to build under Bun, before any child exists", async () => {
    const result = await Effect.runPromise(
      Effect.scoped(Effect.result(Layer.build(NativePeer.layerIsolated()))),
    );
    expect(result._tag).toBe("Failure");
    if (result._tag === "Failure")
      expect(result.failure).toMatchObject({
        reason: { _tag: "UnsupportedCapability" },
        context: expect.objectContaining({ outcome: "not-submitted" }),
      });
  });

  test.runIf(onNode)(
    "rejects an invalid deadline and an unloadable addon while the layer builds",
    async () => {
      for (const shutdownTimeout of [0, -1, Number.NaN, "soon"]) {
        const result = await Effect.runPromise(
          Effect.scoped(
            Effect.result(isolatedFactory({ shutdownTimeout: shutdownTimeout as Duration.Input })),
          ),
        );
        expect(result._tag).toBe("Failure");
        if (result._tag === "Failure")
          expect(result.failure).toMatchObject({
            reason: { _tag: "InvalidInput" },
            context: expect.objectContaining({ outcome: "not-submitted" }),
          });
      }
      // The probe child cannot load it, so the layer fails before any Client exists.
      const missing = await Effect.runPromise(
        Effect.scoped(
          Effect.result(isolatedFactory({ addon: "/nonexistent/reactor-effect-native.node" })),
        ),
      );
      expect(missing._tag).toBe("Failure");
      if (missing._tag === "Failure")
        expect(missing.failure).toMatchObject({
          reason: expect.objectContaining({ _tag: "Native" }),
          context: expect.objectContaining({ outcome: "not-submitted" }),
        });
    },
  );

  test.runIf(onNode)(
    "serves successive generations from one parent, each child answering its own client",
    async () => {
      const addon = makeFakeAddon();
      const remote = coordinator();
      const peers: IsolatedPeer[] = [];
      try {
        const result = await runClient(
          Effect.scoped(
            Effect.gen(function* () {
              const factory = yield* isolatedClient(
                { apiUrl: "https://coordinator.fixture" },
                { addon: addon.path },
                peers,
              );
              const client = yield* factory.create(create);
              const first = (yield* client.ready).generation;
              const before = yield* (yield* client.decoded).pressure;
              yield* client.reconnect;
              const second = (yield* client.ready).generation;
              const media = yield* client.decoded;
              const reader = yield* Effect.forkChild(
                media.video("main_video").pipe(Stream.runHead),
                { startImmediately: true },
              );
              // The snapshot releases the fake's frame, which waits in its queue until taken.
              const after = yield* media.pressure;
              const frame = Option.getOrThrow(yield* Fiber.join(reader));
              const report = yield* client.close;
              return { first, second, before, after, frame, report };
            }),
          ),
          remote.fetch,
        );
        expect(result.second).toBeGreaterThan(result.first);
        expect(result.before.closed).toBe(false);
        expect(result.after.closed).toBe(false);
        expect(result.frame).toMatchObject({
          _tag: "VideoFrame",
          track: "main_video",
          format: "BGRA",
          width: 1,
          height: 1,
          frameId: 18446744073709551615n,
          timestampMicros: 9007199254740993n,
        });
        expect([...result.frame.data]).toEqual([1, 2, 3, 4]);
        expect([...result.frame.metadata]).toEqual([9, 8, 7]);
        expect(result.report.localClosed).toBe(true);
        expect(result.report.localErrors).toEqual([]);
        // One child per generation, each ended by its own shutdown.
        expect(peers).toHaveLength(2);
        const [a, b] = peers;
        expect(a!.link.child!.pid).not.toBe(b!.link.child!.pid);
        for (const peer of peers) expect(peer.link.exit).toEqual({ code: 0, signal: null });
      } finally {
        addon.remove();
      }
    },
    30_000,
  );

  test.runIf(onNode)(
    "fails a dispatched call as unknown when its child dies, fences later calls and never respawns",
    async () => {
      const addon = makeFakeAddon();
      try {
        const result = await Effect.runPromise(
          Effect.scoped(
            Effect.gen(function* () {
              const peers = yield* isolatedPeers({ addon: addon.path });
              const peer = yield* peers.make;
              // Making the peer forks its child at once, while a session allocates.
              const spawnedAtMake = peer.link.child?.pid !== undefined;
              const pid = peer.link.child?.pid;
              yield* peer.opened;
              const events: PeerEvent[] = [];
              yield* peer.prepare([], tracks, (event) => events.push(event));
              // The fake holds a data-channel send for 50 ms.
              const sending = yield* Effect.forkChild(
                Effect.result(peer.send("data", Uint8Array.of(1, 2))),
                { startImmediately: true },
              );
              yield* addon.reached("send");
              process.kill(peer.link.child!.pid!, "SIGKILL");
              const pending = yield* Fiber.join(sending);
              yield* Deferred.await(peer.link.exited);
              yield* Effect.promise(() =>
                until(() => events.some((event) => event.type === "error"), "no failure event"),
              );
              const started = performance.now();
              const later = yield* Effect.result(peer.stats);
              const laterMs = performance.now() - started;
              const shutdown = yield* Effect.exit(peer.shutdown);
              // A peer with no events to learn of the death from is still refused
              // before dispatch, rather than buffered for a worker that is gone.
              const solo = yield* peers.make;
              yield* solo.opened;
              solo.link.child?.kill("SIGKILL");
              yield* Deferred.await(solo.link.exited);
              const fenced = yield* Effect.result(solo.stats).pipe(
                Effect.timeoutOption("2 seconds"),
              );
              yield* solo.shutdown;
              return {
                spawnedAtMake,
                pid,
                pending,
                later,
                laterMs,
                events,
                shutdown,
                link: peer.link,
                fenced,
              };
            }),
          ),
        );
        expect(result.spawnedAtMake).toBe(true);
        expect(result.pending._tag).toBe("Failure");
        if (result.pending._tag === "Failure")
          expect(result.pending.failure).toMatchObject({
            reason: { _tag: "Native" },
            message: "native WebRTC child process exited",
            context: expect.objectContaining({ outcome: "unknown" }),
          });
        // Later calls are refused before dispatch; nothing waits on a dead child,
        // and no child replaces it.
        expect(result.later._tag).toBe("Failure");
        if (result.later._tag === "Failure")
          expect(result.later.failure.context.outcome).toBe("not-submitted");
        expect(result.laterMs).toBeLessThan(500);
        expect(result.link.child?.pid).toBe(result.pid);
        expect(addon.calls().filter((call) => call === "prepare")).toHaveLength(1);
        // The session hears of the death once, as a connection failure.
        expect(result.events.filter((event) => event.type === "error")).toHaveLength(1);
        expect(result.events.at(-1)).toMatchObject({
          type: "error",
          error: { reason: { _tag: "Native" }, message: "native WebRTC child process exited" },
        });
        expect(result.link.exit).toEqual({ code: null, signal: "SIGKILL" });
        // Nothing of the child is left to join.
        expect(Exit.isSuccess(result.shutdown)).toBe(true);
        expect(Option.isSome(result.fenced)).toBe(true);
        if (Option.isSome(result.fenced) && result.fenced.value._tag === "Failure")
          expect(result.fenced.value.failure).toMatchObject({
            reason: { _tag: "Native" },
            message: "native WebRTC child process exited",
            context: expect.objectContaining({ outcome: "not-submitted" }),
          });
        else expect.fail("a call to a dead child was not refused");
      } finally {
        addon.remove();
      }
    },
    20_000,
  );

  test.runIf(onNode)(
    "ends a child whose parent dies, even with a native call in flight",
    async () => {
      // The held statistics call keeps the child's event loop alive: without
      // its own exit on disconnect, it would outlive the parent as an orphan.
      const addon = makeFakeAddon();
      addon.hold("stats", true);
      const script = `
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as host from "./dist/internal/isolated/host.js";
Effect.runFork(
  Effect.scoped(
    Effect.gen(function* () {
      const environment = yield* host.environment({
        addon: process.env.FIXTURE,
        shutdownTimeout: Duration.seconds(10),
      });
      const peer = yield* host.make(environment);
      yield* peer.opened;
      yield* Effect.forkChild(peer.stats, { startImmediately: true });
      process.stdout.write(JSON.stringify({ child: peer.link.child.pid }) + "\\n");
      yield* Effect.never;
    }),
  ),
);
`;
      const parent = spawn(process.execPath, ["--input-type=module", "-e", script], {
        cwd: packageRoot,
        env: { ...process.env, FIXTURE: addon.path },
        stdio: ["ignore", "pipe", "inherit"],
      });
      let child: number | undefined;
      try {
        const line = await new Promise<string>((resolve, reject) => {
          createInterface({ input: parent.stdout }).once("line", resolve);
          parent.once("exit", (code) => reject(new Error(`parent exited early with ${code}`)));
        });
        child = (JSON.parse(line) as { readonly child: number }).child;
        const orphan = child;
        await until(() => addon.calls().includes("stats"), "statistics never reached the child");
        expect(alive(orphan)).toBe(true);
        parent.kill("SIGKILL");
        await until(() => !alive(orphan), "the child outlived its parent", 5_000);
      } finally {
        parent.kill("SIGKILL");
        if (child !== undefined && alive(child)) process.kill(child, "SIGKILL");
        addon.remove();
      }
    },
    20_000,
  );

  test.runIf(onNode)(
    "kills a child whose shutdown outlives its deadline, reports Shutdown and still terminates the remote session",
    async () => {
      const addon = makeFakeAddon();
      const remote = coordinator();
      const peers: IsolatedPeer[] = [];
      try {
        const result = await runClient(
          Effect.scoped(
            Effect.gen(function* () {
              const factory = yield* isolatedClient(
                { apiUrl: "https://coordinator.fixture" },
                { addon: addon.path, shutdownTimeout: "250 millis" },
                peers,
              );
              // The probe has shut down; every child from here on holds its join.
              addon.hold("shutdown", true);
              const client = yield* factory.create(create);
              const started = performance.now();
              const report = yield* client.close;
              return { report, closeMs: performance.now() - started };
            }),
          ),
          remote.fetch,
        );
        expect(result.closeMs).toBeGreaterThanOrEqual(200);
        expect(result.closeMs).toBeLessThan(closeBound);
        expect(result.report.localClosed).toBe(false);
        expect(result.report.localErrors).toHaveLength(1);
        const [shutdown] = result.report.localErrors;
        expect(shutdown?.reason).toBe("Shutdown");
        expect(shutdown?.message).toBe(
          "native child shutdown exceeded its deadline; child process killed",
        );
        // Cleanup went on to terminate the owned remote session.
        expect(result.report.remote).toMatchObject({
          attempted: true,
          confirmed: true,
          evidence: "absent",
        });
        expect(remote.deleted.has(sessionId)).toBe(true);
        expect(peers).toHaveLength(1);
        expect(peers[0]!.link.exit).toEqual({ code: null, signal: "SIGKILL" });
        expect(alive(peers[0]!.link.child!.pid!)).toBe(false);
      } finally {
        addon.remove();
      }
    },
    20_000,
  );

  test.runIf(onNode)(
    "never delivers a closed or killed child's late events and frames, to it or to a later peer",
    async () => {
      const addon = makeFakeAddon();
      try {
        const result = await Effect.runPromise(
          Effect.scoped(
            Effect.gen(function* () {
              const peers = yield* isolatedPeers({ addon: addon.path });

              // Closed while its answer's three events and a frame are on their way.
              const a = yield* peers.make;
              yield* a.opened;
              const aEvents: PeerEvent[] = [];
              const aFrames: VideoFrame[] = [];
              const aScope = yield* Scope.make();
              yield* a
                .prepare([], tracks, (event) => aEvents.push(event))
                .pipe(Scope.provide(aScope));
              const aReader = yield* Effect.forkChild(
                decoded(a)
                  .video("main_video")
                  .pipe(Stream.runForEach((frame) => Effect.sync(() => aFrames.push(frame)))),
                { startImmediately: true },
              );
              const answering = yield* Effect.forkChild(a.answer("fixture answer"), {
                startImmediately: true,
              });
              const releasing = yield* Effect.forkChild(decoded(a).pressure, {
                startImmediately: true,
              });
              // Both requests are on the channel: whatever they release is late.
              yield* a.close;
              yield* Fiber.join(answering);
              yield* Fiber.join(releasing);
              yield* addon.reached("answer");
              // The child queued three events and a frame; none reaches anyone.
              yield* Effect.sleep("100 millis");
              const aReaderExit = yield* Fiber.await(aReader);
              yield* a.shutdown;
              yield* Scope.close(aScope, Exit.void);

              // Killed while connected: one failure event, then nothing.
              const b = yield* peers.make;
              yield* b.opened;
              const bEvents: PeerEvent[] = [];
              const bScope = yield* Scope.make();
              yield* b
                .prepare([], tracks, (event) => bEvents.push(event))
                .pipe(Scope.provide(bScope));
              yield* b.answer("fixture answer");
              yield* Effect.promise(() => until(() => bEvents.length === 3, "b never connected"));
              b.link.child!.kill("SIGKILL");
              yield* Deferred.await(b.link.exited);
              yield* Effect.promise(() =>
                until(() => bEvents.length === 4, "b's failure never came"),
              );
              yield* b.close;
              yield* b.shutdown;
              yield* Scope.close(bScope, Exit.void);

              // A later peer hears only its own child.
              const c = yield* peers.make;
              yield* c.opened;
              const cEvents: PeerEvent[] = [];
              const cScope = yield* Scope.make();
              yield* c
                .prepare([], tracks, (event) => cEvents.push(event))
                .pipe(Scope.provide(cScope));
              yield* c.answer("fixture answer");
              yield* Effect.promise(() => until(() => cEvents.length === 3, "c never connected"));
              yield* c.close;
              yield* c.shutdown;
              yield* Scope.close(cScope, Exit.void);
              return { aEvents, aFrames, aReaderExit, bEvents, cEvents };
            }),
          ),
        );
        expect(result.aEvents).toEqual([]);
        expect(result.aFrames).toEqual([]);
        // Its readers ended with the close.
        expect(Exit.isSuccess(result.aReaderExit)).toBe(true);
        expect(result.bEvents.map((event) => event.type)).toEqual([
          "state",
          "channel",
          "channel",
          "error",
        ]);
        expect(result.cEvents).toEqual([
          { type: "state", state: "connected" },
          { type: "channel", channel: "control", open: true },
          { type: "channel", channel: "data", open: true },
        ]);
      } finally {
        addon.remove();
      }
    },
    30_000,
  );

  test.runIf(onNode)(
    "reports a dispatched call whose wait is cancelled as unknown, and never attributes a late reply to a later call",
    async () => {
      const addon = makeFakeAddon();
      try {
        const result = await Effect.runPromise(
          Effect.scoped(
            Effect.gen(function* () {
              const peers = yield* isolatedPeers({
                addon: addon.path,
                shutdownTimeout: "300 millis",
              });
              const peer = yield* peers.make;
              yield* peer.opened;
              // The caller stops waiting for a send the child holds for 50 ms.
              const sending = yield* Effect.forkChild(peer.send("data", Uint8Array.of(7)), {
                startImmediately: true,
              });
              yield* addon.reached("send");
              yield* Fiber.interrupt(sending);
              const interrupted = yield* Fiber.await(sending);
              const snapshot = yield* decoded(peer).pressure;
              // The abandoned send's reply arrives while the next call waits.
              const direction = yield* Effect.exit(
                Effect.andThen(Effect.sleep("60 millis"), peer.direction("main_video", true)),
              );
              // A statistics read the child never completes: its join waits for it,
              // and the shutdown deadline kills the child under it.
              addon.hold("stats", true);
              const reading = yield* Effect.forkChild(Effect.result(peer.stats), {
                startImmediately: true,
              });
              yield* addon.reached("stats");
              const shutdown = yield* Effect.exit(peer.shutdown);
              const stats = yield* Fiber.join(reading);
              return { interrupted, snapshot, direction, stats, shutdown, link: peer.link };
            }),
          ),
        );
        expect(Exit.hasInterrupts(result.interrupted)).toBe(true);
        // Each later call received its own reply, of its own shape.
        expect(result.snapshot).toMatchObject({ closed: false, readerOverflows: 0n });
        expect(Exit.isSuccess(result.direction)).toBe(true);
        expect(result.stats._tag).toBe("Failure");
        if (result.stats._tag === "Failure")
          expect(result.stats.failure).toMatchObject({
            reason: { _tag: "Native" },
            context: expect.objectContaining({ outcome: "unknown" }),
          });
        expect(Exit.isFailure(result.shutdown)).toBe(true);
        if (Exit.isFailure(result.shutdown))
          expect(Cause.squash(result.shutdown.cause)).toMatchObject({
            reason: { _tag: "Shutdown" },
            message: "native child shutdown exceeded its deadline; child process killed",
          });
        expect(result.link.exit).toEqual({ code: null, signal: "SIGKILL" });
      } finally {
        addon.remove();
      }
    },
    20_000,
  );
});

describe.runIf(onNode)("isolated native host over real libwebrtc", () => {
  const farTracks = [
    { name: "main_video", kind: "video", direction: "recvonly" },
    { name: "main_audio", kind: "audio", direction: "recvonly" },
  ] as const;
  let far: FarPeer;
  beforeAll(async () => {
    far = await FarPeer.start();
  });
  afterAll(async () => {
    await far?.quit();
  });

  test("fails only the reader that stops consuming", async () => {
    const id = "isolated-overflow";
    try {
      const result = await Effect.runPromise(
        Effect.scoped(
          Effect.gen(function* () {
            const peers = yield* isolatedPeers({});
            const peer = yield* peers.make;
            const failures: ReactorError[] = [];
            const prepared = yield* peer.prepare([], farTracks, (event) => {
              if (event.type === "ice" && event.candidate !== undefined)
                far.candidate(id, event.candidate);
              else if (event.type === "error") failures.push(event.error);
            });
            let fast = 0;
            const gate = yield* Deferred.make<void>();
            const stalled = yield* Effect.forkChild(
              decoded(peer)
                .video("main_video")
                .pipe(Stream.runForEach(() => Deferred.await(gate))),
              { startImmediately: true },
            );
            const reader = yield* Effect.forkChild(
              decoded(peer)
                .video("main_video")
                .pipe(
                  Stream.runForEach(() =>
                    Effect.sync(() => {
                      fast++;
                    }),
                  ),
                ),
              { startImmediately: true },
            );
            yield* peer.answer(yield* Effect.promise(() => far.answer(id, prepared.sdp)));
            yield* Effect.promise(() =>
              until(() => fast >= 24, "no frames reached the reader", 20_000),
            );
            yield* Deferred.succeed(gate, undefined);
            const stalledExit = yield* Fiber.await(stalled);
            const reached = fast;
            yield* Effect.promise(() =>
              until(() => fast >= reached + 24, "the consuming reader stopped", 20_000),
            );
            const pressure = yield* decoded(peer).pressure;
            yield* peer.close;
            yield* Fiber.interrupt(reader);
            yield* peer.shutdown;
            return { stalledExit, pressure, failures, link: peer.link };
          }),
        ),
      );
      expect(result.failures).toEqual([]);
      expect(Exit.isFailure(result.stalledExit)).toBe(true);
      if (Exit.isFailure(result.stalledExit))
        expect(Cause.squash(result.stalledExit.cause)).toMatchObject({
          reason: { _tag: "Overflow" },
        });
      expect(result.pressure.readerOverflows).toBe(1n);
      expect(result.link.exit).toEqual({ code: 0, signal: null });
    } finally {
      await far.close(id);
    }
  }, 60_000);

  test("delivers exact frames through a canonical session over real libwebrtc in a child process and closes it cleanly", async () => {
    const id = "isolated-canonical";
    const described = {
      session_id: id,
      state: "ACTIVE",
      capabilities: {
        protocol_version: "1.0",
        tracks: farTracks,
        commands: [{ name: "echo", schema: {} }],
      },
      selected_transport: { protocol: "webrtc", version: "1.0" },
    };
    let answer: Promise<string> | undefined;
    let deleted = false;
    // A coordinator that relays signaling to the far peer.
    const relay = async (input: string | Request | URL, init?: RequestInit): Promise<Response> => {
      const request = new Request(input, init);
      const path = new URL(request.url).pathname;
      if (path === "/sessions" && request.method === "POST") return Response.json(described);
      if (path === `/sessions/${id}` && request.method === "GET")
        return deleted
          ? Response.json({ error: "session not found" }, { status: 404 })
          : Response.json(described);
      if (path === `/sessions/${id}` && request.method === "DELETE") {
        deleted = true;
        return new Response(null, { status: 202 });
      }
      if (path.endsWith("/ice_servers")) return Response.json({ ice_servers: [] });
      if (path.endsWith("/connections")) return Response.json({ connection_id: 1 });
      if (path.endsWith("/ice_candidates")) {
        const body = record(await request.json());
        for (const candidate of Array.isArray(body.candidates) ? body.candidates : [])
          far.candidate(id, candidate as IceCandidate);
        return new Response(null, { status: 204 });
      }
      if (path.endsWith("/sdp_params")) {
        if (request.method === "GET") return Response.json({ sdp_answer: await answer });
        answer = far.answer(id, String(record(await request.json()).sdp_offer));
        return new Response(null, { status: 204 });
      }
      return Response.json({ error: `unhandled route ${path}` }, { status: 404 });
    };
    const peers: IsolatedPeer[] = [];
    try {
      const result = await runClient(
        Effect.scoped(
          Effect.gen(function* () {
            const factory = yield* isolatedClient(
              { apiUrl: "https://coordinator.far-peer" },
              {},
              peers,
            );
            const client = yield* factory.create({
              model: "far-peer",
              jwt: Redacted.make("far-peer-token"),
            });
            const media = yield* client.decoded;
            const video = yield* media.video("main_video").pipe(Stream.take(8), Stream.runCollect);
            const audio = yield* media.audio("main_audio").pipe(Stream.take(8), Stream.runCollect);
            const pressure = yield* media.pressure;
            const started = performance.now();
            const report = yield* client.close;
            return { video, audio, pressure, report, closeMs: performance.now() - started };
          }),
        ),
        relay,
      );
      expect(result.video).toHaveLength(8);
      expect(result.audio).toHaveLength(8);
      // Each frame's bytes are the whole of their own buffer after the IPC hop.
      assertExactFrames(result.video, (frame) => frame.data);
      assertExactFrames(result.video, (frame) => frame.metadata);
      assertExactFrames(result.audio, (frame) => frame.samples);
      for (const frame of result.video) {
        expect(frame.format).toBe("BGRA");
        expect(frame.data.byteLength).toBe(frame.width * frame.height * 4);
      }
      // The native admission sequence crosses the process boundary: it only rises.
      for (const frames of [result.video, result.audio])
        for (let index = 1; index < frames.length; index++)
          expect(frames[index]!.sequence).toBeGreaterThan(frames[index - 1]!.sequence);
      expect(result.pressure.deliveredVideo).toBeGreaterThanOrEqual(8n);
      expect(result.report.localClosed).toBe(true);
      expect(result.report.localErrors).toEqual([]);
      expect(result.report.remote).toMatchObject({ attempted: true, confirmed: true });
      expect(result.closeMs).toBeLessThan(closeBound);
      expect(peers).toHaveLength(1);
      expect(peers[0]!.link.exit).toEqual({ code: 0, signal: null });
    } finally {
      await far.close(id);
    }
  }, 60_000);
});
