/** The isolated native host: one child process per connection, over the fake addon and real libwebrtc. */
import { fileURLToPath } from "node:url";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, layer } from "@effect/vitest";
import * as Cause from "effect/Cause";
import * as Context from "effect/Context";
import * as Deferred from "effect/Deferred";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Redacted from "effect/Redacted";
import * as Schedule from "effect/Schedule";
import * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import * as HttpClient from "effect/unstable/http/HttpClient";
import * as ChildProcess from "effect/unstable/process/ChildProcess";
import { ChildProcessSpawner } from "effect/unstable/process/ChildProcessSpawner";
import * as Coordinator from "reactor-effect-client/Coordinator";
import type { VideoFrame } from "reactor-effect-client/Media";
import { PeerFactory } from "reactor-effect-client/Peer";
import type { PeerEvent } from "reactor-effect-client/Peer";
import * as Reactor from "reactor-effect-client/Reactor";
import type { ReactorError } from "reactor-effect-client/ReactorError";
import { describe, expect } from "vitest";
import type { IsolatedPeer } from "../src/internal/isolated/host.js";
import { defaultShutdownTimeout } from "../src/internal/peer.js";
import * as NativePeer from "../src/NativePeer.js";
import {
  assertExactFrames,
  candidateRelay,
  coordinator,
  decoded,
  eventually,
  fakeAddon,
  FarPeer,
} from "./support.js";

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
const alive = (pid: number) =>
  Effect.gen(function* () {
    const signalled = yield* Effect.try(() => process.kill(pid, 0)).pipe(
      Effect.as(true),
      Effect.orElseSucceed(() => false),
    );
    if (!signalled) return false;
    const spawner = yield* ChildProcessSpawner;
    const stat = yield* spawner
      .string(ChildProcess.make("ps", ["-o", "stat=", "-p", String(pid)]))
      .pipe(Effect.orElseSucceed(() => ""));
    return stat.trim() !== "" && !stat.trim().startsWith("Z");
  });

const sessionId = "sess_isolated_fixture";
const settings = { apiUrl: "https://coordinator.fixture" } as const;
const create = { model: "fixture/native-session", jwt: Redacted.make("fixture-token") };
const fixture = coordinator({ sessionId, tracks });

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
const isolatedClient = (input: {
  readonly settings: Coordinator.Options & Reactor.Options;
  readonly options: NativePeer.Options;
  readonly peers: Array<IsolatedPeer>;
}) =>
  Effect.gen(function* () {
    const factory = yield* isolatedFactory(input.options);
    const made = yield* isolatedPeers(input.options);
    const services = yield* Layer.build(Coordinator.layer(input.settings));
    return yield* Reactor.make(input.settings).pipe(
      Effect.provide(services),
      Effect.provideService(
        PeerFactory,
        PeerFactory.of({
          check: factory.check,
          make: Effect.tap(made.make, (peer) => Effect.sync(() => input.peers.push(peer))),
        }),
      ),
    );
  });

/** A peer's events, recorded as they arrive. */
const recorded = () => {
  const events: Array<PeerEvent> = [];
  return { events, emit: (event: PeerEvent) => events.push(event) };
};

layer(NodeServices.layer, { excludeTestServices: true })("isolated native host", (it) => {
  it.effect.runIf(isBun)("refuses to build under Bun, before any child exists", () =>
    Effect.gen(function* () {
      const result = yield* Layer.build(NativePeer.layerIsolated()).pipe(
        Effect.result,
        Effect.scoped,
      );
      expect(result).toMatchObject({
        _tag: "Failure",
        failure: {
          reason: { _tag: "UnsupportedCapability" },
          context: { outcome: "not-submitted" },
        },
      });
    }),
  );

  it.effect.runIf(onNode)(
    "rejects an invalid deadline and an unloadable addon while the layer builds",
    () =>
      Effect.gen(function* () {
        for (const shutdownTimeout of [0, -1, Number.NaN]) {
          const result = yield* isolatedFactory({ shutdownTimeout }).pipe(
            Effect.result,
            Effect.scoped,
          );
          expect(result).toMatchObject({
            _tag: "Failure",
            failure: { reason: { _tag: "InvalidInput" }, context: { outcome: "not-submitted" } },
          });
        }
        // The probe child cannot load it, so the layer fails before any Client exists.
        const missing = yield* isolatedFactory({
          addon: "/nonexistent/reactor-effect-native.node",
        }).pipe(Effect.result, Effect.scoped);
        expect(missing).toMatchObject({
          _tag: "Failure",
          failure: { reason: { _tag: "Native" }, context: { outcome: "not-submitted" } },
        });
      }),
  );

  it.effect.runIf(onNode)(
    "serves successive generations from one parent, each child answering its own client",
    () =>
      Effect.gen(function* () {
        const addon = yield* fakeAddon;
        const remote = yield* fixture;
        const peers: Array<IsolatedPeer> = [];
        const result = yield* Effect.scoped(
          Effect.gen(function* () {
            const factory = yield* isolatedClient({
              settings,
              options: { addon: addon.path },
              peers,
            });
            const client = yield* factory.create(create);
            const first = (yield* client.ready).generation;
            const before = yield* (yield* client.decoded).pressure;
            yield* client.reconnect;
            const second = (yield* client.ready).generation;
            const media = yield* client.decoded;
            const reader = yield* Effect.forkChild(media.video("main_video").pipe(Stream.runHead), {
              startImmediately: true,
            });
            // The snapshot releases the fake's frame, which waits in its queue until taken.
            const after = yield* media.pressure;
            const frame = Option.getOrThrow(yield* Fiber.join(reader));
            const report = yield* client.close;
            return { first, second, before, after, frame, report };
          }),
        ).pipe(Effect.provideService(HttpClient.HttpClient, remote.client));
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
        expect(peers.map((peer) => peer.link.exit)).toEqual([
          { code: 0, signal: null },
          { code: 0, signal: null },
        ]);
        expect(new Set(peers.map((peer) => peer.link.child?.pid)).size).toBe(2);
      }),
    30_000,
  );

  it.effect.runIf(onNode)(
    "fails a dispatched call as unknown when its child dies, fences later calls and never respawns",
    () =>
      Effect.gen(function* () {
        const addon = yield* fakeAddon;
        const result = yield* Effect.scoped(
          Effect.gen(function* () {
            const peers = yield* isolatedPeers({ addon: addon.path });
            const peer = yield* peers.make;
            // Making the peer forks its child at once, while a session allocates.
            const spawnedAtMake = peer.link.child?.pid !== undefined;
            const pid = peer.link.child?.pid;
            yield* peer.opened;
            const { events, emit } = recorded();
            yield* peer.prepare([], tracks, emit);
            // The fake holds a data-channel send for 50 ms.
            const sending = yield* Effect.forkChild(
              Effect.result(peer.send("data", Uint8Array.of(1, 2))),
              { startImmediately: true },
            );
            yield* addon.reached("send");
            peer.link.child?.kill("SIGKILL");
            const pending = yield* Fiber.join(sending);
            yield* Deferred.await(peer.link.exited);
            yield* eventually({
              condition: () => events.some((event) => event.type === "error"),
              message: "no failure event",
            });
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
            const fenced = yield* Effect.result(solo.stats).pipe(Effect.timeoutOption("2 seconds"));
            yield* solo.shutdown;
            return {
              spawnedAtMake,
              pid,
              pending,
              later,
              laterMs,
              events,
              shutdown,
              fenced,
              link: peer.link,
            };
          }),
        );
        expect(result.spawnedAtMake).toBe(true);
        expect(result.pending).toMatchObject({
          _tag: "Failure",
          failure: {
            reason: { _tag: "Native" },
            message: "native WebRTC child process exited",
            context: { outcome: "unknown" },
          },
        });
        // Later calls are refused before dispatch; nothing waits on a dead child,
        // and no child replaces it.
        expect(result.later).toMatchObject({
          _tag: "Failure",
          failure: { context: { outcome: "not-submitted" } },
        });
        expect(result.laterMs).toBeLessThan(500);
        expect(result.link.child?.pid).toBe(result.pid);
        expect((yield* addon.calls).filter((call) => call === "prepare")).toHaveLength(1);
        // The session hears of the death once, as a connection failure.
        expect(result.events.filter((event) => event.type === "error")).toHaveLength(1);
        expect(result.events.at(-1)).toMatchObject({
          type: "error",
          error: { reason: { _tag: "Native" }, message: "native WebRTC child process exited" },
        });
        expect(result.link.exit).toEqual({ code: null, signal: "SIGKILL" });
        // Nothing of the child is left to join.
        expect(Exit.isSuccess(result.shutdown)).toBe(true);
        expect(result.fenced).toMatchObject({
          _tag: "Some",
          value: {
            _tag: "Failure",
            failure: {
              reason: { _tag: "Native" },
              message: "native WebRTC child process exited",
              context: { outcome: "not-submitted" },
            },
          },
        });
      }),
    20_000,
  );

  it.effect.runIf(onNode)(
    "ends a child whose parent dies, even with a native call in flight",
    () =>
      Effect.gen(function* () {
        // The held statistics call keeps the child's event loop alive: without
        // its own exit on disconnect, it would outlive the parent as an orphan.
        const addon = yield* fakeAddon;
        yield* addon.hold("stats", true);
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
        const spawner = yield* ChildProcessSpawner;
        const parent = yield* spawner.spawn(
          ChildProcess.make(process.execPath, ["--input-type=module", "-e", script], {
            cwd: packageRoot,
            env: { FIXTURE: addon.path },
            extendEnv: true,
            stdout: "pipe",
            stderr: "inherit",
          }),
        );
        const line = yield* parent.stdout.pipe(
          Stream.decodeText,
          Stream.splitLines,
          Stream.runHead,
        );
        const child = Number(/"child":(\d+)/.exec(Option.getOrThrow(line))?.[1]);
        yield* Effect.addFinalizer(() =>
          alive(child).pipe(
            Effect.flatMap((live) =>
              live ? Effect.sync(() => process.kill(child, "SIGKILL")) : Effect.void,
            ),
            Effect.orDie,
          ),
        );
        yield* addon.reached("stats");
        expect(yield* alive(child)).toBe(true);
        yield* parent.kill({ killSignal: "SIGKILL" });
        yield* alive(child).pipe(
          Effect.flatMap((live) => (live ? Effect.fail("alive") : Effect.void)),
          Effect.retry({ schedule: Schedule.spaced("20 millis") }),
          Effect.timeoutOrElse({
            duration: "5 seconds",
            orElse: () => Effect.die(new Error("the child outlived its parent")),
          }),
        );
      }),
    20_000,
  );

  it.effect.runIf(onNode)(
    "kills a child whose shutdown outlives its deadline, reports Shutdown and still terminates the remote session",
    () =>
      Effect.gen(function* () {
        const addon = yield* fakeAddon;
        const remote = yield* fixture;
        const peers: Array<IsolatedPeer> = [];
        const result = yield* Effect.scoped(
          Effect.gen(function* () {
            const factory = yield* isolatedClient({
              settings,
              options: { addon: addon.path, shutdownTimeout: "250 millis" },
              peers,
            });
            // The probe has shut down; every child from here on holds its join.
            yield* addon.hold("shutdown", true);
            const client = yield* factory.create(create);
            const started = performance.now();
            const report = yield* client.close;
            return { report, closeMs: performance.now() - started };
          }),
        ).pipe(Effect.provideService(HttpClient.HttpClient, remote.client));
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
        expect((yield* remote.deleted).has(sessionId)).toBe(true);
        expect(peers.map((peer) => peer.link.exit)).toEqual([{ code: null, signal: "SIGKILL" }]);
        const pid = peers[0]?.link.child?.pid;
        assert(pid !== undefined, "the child was spawned");
        expect(yield* alive(pid)).toBe(false);
      }),
    20_000,
  );

  it.effect.runIf(onNode)(
    "never delivers a closed or killed child's late events and frames, to it or to a later peer",
    () =>
      Effect.gen(function* () {
        const addon = yield* fakeAddon;
        const result = yield* Effect.scoped(
          Effect.gen(function* () {
            const peers = yield* isolatedPeers({ addon: addon.path });

            // Closed while its answer's three events and a frame are on their way.
            const a = yield* peers.make;
            yield* a.opened;
            const aEvents = recorded();
            const aFrames: Array<VideoFrame> = [];
            const aScope = yield* Scope.make();
            yield* a.prepare([], tracks, aEvents.emit).pipe(Scope.provide(aScope));
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
            // The child took the three events and the frame they released and
            // sent them on; once it has joined and exited, nothing more can come.
            yield* addon.reached("take event", 3);
            yield* addon.reached("take video");
            yield* a.shutdown;
            const aReaderExit = yield* Fiber.await(aReader);
            yield* Scope.close(aScope, Exit.void);

            // Killed while connected: one failure event, then nothing.
            const b = yield* peers.make;
            yield* b.opened;
            const bEvents = recorded();
            const bScope = yield* Scope.make();
            yield* b.prepare([], tracks, bEvents.emit).pipe(Scope.provide(bScope));
            yield* b.answer("fixture answer");
            yield* eventually({
              condition: () => bEvents.events.length === 3,
              message: "b never connected",
            });
            b.link.child?.kill("SIGKILL");
            yield* Deferred.await(b.link.exited);
            yield* eventually({
              condition: () => bEvents.events.length === 4,
              message: "b's failure never came",
            });
            yield* b.close;
            yield* b.shutdown;
            yield* Scope.close(bScope, Exit.void);

            // A later peer hears only its own child.
            const c = yield* peers.make;
            yield* c.opened;
            const cEvents = recorded();
            const cScope = yield* Scope.make();
            yield* c.prepare([], tracks, cEvents.emit).pipe(Scope.provide(cScope));
            yield* c.answer("fixture answer");
            yield* eventually({
              condition: () => cEvents.events.length === 3,
              message: "c never connected",
            });
            yield* c.close;
            yield* c.shutdown;
            yield* Scope.close(cScope, Exit.void);
            return {
              aEvents: aEvents.events,
              aFrames,
              aReaderExit,
              bEvents: bEvents.events,
              cEvents: cEvents.events,
            };
          }),
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
      }),
    30_000,
  );

  it.effect.runIf(onNode)(
    "reports a dispatched call whose wait is cancelled as unknown, and never attributes a late reply to a later call",
    () =>
      Effect.gen(function* () {
        const addon = yield* fakeAddon;
        const result = yield* Effect.scoped(
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
            // The abandoned send's reply is on its way as the next call is made.
            yield* addon.reached("send answered");
            const direction = yield* Effect.exit(peer.direction("main_video", true));
            // A statistics read the child never completes: its join waits for it,
            // and the shutdown deadline kills the child under it.
            yield* addon.hold("stats", true);
            const reading = yield* Effect.forkChild(Effect.result(peer.stats), {
              startImmediately: true,
            });
            yield* addon.reached("stats");
            const shutdown = yield* Effect.exit(peer.shutdown);
            const stats = yield* Fiber.join(reading);
            return { interrupted, snapshot, direction, stats, shutdown, link: peer.link };
          }),
        );
        expect(Exit.hasInterrupts(result.interrupted)).toBe(true);
        // Each later call received its own reply, of its own shape.
        expect(result.snapshot).toMatchObject({ closed: false, readerOverflows: 0n });
        expect(Exit.isSuccess(result.direction)).toBe(true);
        expect(result.stats).toMatchObject({
          _tag: "Failure",
          failure: { reason: { _tag: "Native" }, context: { outcome: "unknown" } },
        });
        expect(Exit.isFailure(result.shutdown)).toBe(true);
        if (Exit.isFailure(result.shutdown))
          expect(Cause.squash(result.shutdown.cause)).toMatchObject({
            reason: { _tag: "Shutdown" },
            message: "native child shutdown exceeded its deadline; child process killed",
          });
        expect(result.link.exit).toEqual({ code: null, signal: "SIGKILL" });
      }),
    20_000,
  );
});

const farTracks = [
  { name: "main_video", kind: "video", direction: "recvonly" },
  { name: "main_audio", kind: "audio", direction: "recvonly" },
] as const;

describe.runIf(onNode)("isolated native host over real libwebrtc", () => {
  layer(Layer.merge(NodeServices.layer, FarPeer.layer), { excludeTestServices: true })((it) => {
    it.effect(
      "fails only the reader that stops consuming",
      () =>
        Effect.gen(function* () {
          const far = yield* FarPeer;
          const id = "isolated-overflow";
          yield* Effect.addFinalizer(() => far.close(id));
          const result = yield* Effect.scoped(
            Effect.gen(function* () {
              const peers = yield* isolatedPeers({});
              const peer = yield* peers.make;
              const failures: Array<ReactorError> = [];
              const relay = yield* candidateRelay({ far, id });
              const prepared = yield* peer.prepare([], farTracks, (event) => {
                if (event.type === "ice" && event.candidate !== undefined) relay(event.candidate);
                else if (event.type === "error") failures.push(event.error);
              });
              const counted = { fast: 0 };
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
                  .pipe(Stream.runForEach(() => Effect.sync(() => counted.fast++))),
                { startImmediately: true },
              );
              yield* peer.answer(yield* far.answer(id, prepared.sdp));
              // Long enough for the stalled reader to pass its 24-frame bound.
              yield* eventually({
                condition: () => counted.fast >= 72,
                message: "no frames reached the reader",
                timeout: "20 seconds",
              });
              yield* Deferred.succeed(gate, undefined);
              const stalledExit = yield* Fiber.await(stalled);
              const reached = counted.fast;
              yield* eventually({
                condition: () => counted.fast >= reached + 24,
                message: "the consuming reader stopped",
                timeout: "20 seconds",
              });
              const pressure = yield* decoded(peer).pressure;
              yield* peer.close;
              yield* Fiber.interrupt(reader);
              yield* peer.shutdown;
              return { stalledExit, pressure, failures, link: peer.link };
            }),
          );
          expect(result.failures).toEqual([]);
          expect(Exit.isFailure(result.stalledExit)).toBe(true);
          if (Exit.isFailure(result.stalledExit))
            expect(Cause.squash(result.stalledExit.cause)).toMatchObject({
              reason: { _tag: "Overflow" },
            });
          expect(result.pressure.readerOverflows).toBe(1n);
          expect(result.link.exit).toEqual({ code: 0, signal: null });
        }),
      60_000,
    );

    it.effect(
      "delivers exact frames through a canonical session over real libwebrtc in a child process and closes it cleanly",
      () =>
        Effect.gen(function* () {
          const far = yield* FarPeer;
          const id = "isolated-canonical";
          yield* Effect.addFinalizer(() => far.close(id));
          const relay = yield* coordinator({
            sessionId: id,
            tracks: farTracks,
            answer: (sdp) => far.answer(id, sdp),
            candidate: (candidate) => far.candidate(id, candidate),
          });
          const peers: Array<IsolatedPeer> = [];
          const result = yield* Effect.scoped(
            Effect.gen(function* () {
              const factory = yield* isolatedClient({
                settings: { apiUrl: "https://coordinator.far-peer" },
                options: {},
                peers,
              });
              const client = yield* factory.create({
                model: "far-peer",
                tokens: Coordinator.fixedTokens({
                  jwt: Redacted.make("far-peer-token"),
                  expiresAt: Number.MAX_SAFE_INTEGER,
                  maxSessionSeconds: undefined,
                }),
              });
              const media = yield* client.decoded;
              const video = yield* media
                .video("main_video")
                .pipe(Stream.take(8), Stream.runCollect);
              const audio = yield* media
                .audio("main_audio")
                .pipe(Stream.take(8), Stream.runCollect);
              const pressure = yield* media.pressure;
              const started = performance.now();
              const report = yield* client.close;
              return { video, audio, pressure, report, closeMs: performance.now() - started };
            }),
          ).pipe(Effect.provideService(HttpClient.HttpClient, relay.client));
          expect(result.video).toHaveLength(8);
          expect(result.audio).toHaveLength(8);
          // Each frame's bytes are the whole of their own buffer after the IPC hop.
          assertExactFrames({ frames: result.video, bytes: (frame) => frame.data });
          assertExactFrames({ frames: result.video, bytes: (frame) => frame.metadata });
          assertExactFrames({ frames: result.audio, bytes: (frame) => frame.samples });
          for (const frame of result.video) {
            expect(frame.format).toBe("BGRA");
            expect(frame.data.byteLength).toBe(frame.width * frame.height * 4);
          }
          // The native admission sequence crosses the process boundary: it only rises.
          for (const frames of [result.video, result.audio]) {
            const sequences = frames.map((frame) => frame.sequence);
            expect(sequences).toEqual([...sequences].sort((a, b) => (a < b ? -1 : 1)));
            expect(new Set(sequences).size).toBe(sequences.length);
          }
          expect(result.pressure.deliveredVideo).toBeGreaterThanOrEqual(8n);
          expect(result.report.localClosed).toBe(true);
          expect(result.report.localErrors).toEqual([]);
          expect(result.report.remote).toMatchObject({ attempted: true, confirmed: true });
          expect(result.closeMs).toBeLessThan(closeBound);
          expect(peers.map((peer) => peer.link.exit)).toEqual([{ code: 0, signal: null }]);
        }),
      60_000,
    );
  });
});
