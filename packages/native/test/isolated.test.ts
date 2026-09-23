import { spawn, spawnSync } from "node:child_process";
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
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
import { FetchHttp, PeerFactory, make as makeClient } from "reactor-effect-client";
import type { Configuration, ReactorError, ReactorFailure } from "reactor-effect-client";
import type { IceCandidate, PeerEvent, VideoFrame } from "reactor-effect-client/host";
import { assertExactFrames } from "reactor-effect-test-kit/frames";
import type { IsolatedPeer } from "../src/_internal/isolated/host.js";
import { defaultShutdownTimeout } from "../src/_internal/peer.js";
import * as Native from "../src/index.js";
import { FarPeer, compileLibrary, libraryPath, record, until } from "./support.js";

/*
 * The isolated host forks the built child entry, dist/_internal/isolated/child.js,
 * from source and package alike, so these tests need `bun run build` first. Its
 * parent must be Node; under Bun only the refusal runs.
 */
const isBun = process.versions.bun !== undefined;
const onNode = !isBun && process.platform !== "win32";

const packageRoot = fileURLToPath(new URL("..", import.meta.url));
const fixtureSource = fileURLToPath(new URL("./session-fixture.c", import.meta.url));

/** The scripted ABI 3 fixture with `suffix` appended to its source. */
const fixture = (suffix = "") =>
  compileLibrary(`${readFileSync(fixtureSource, "utf8")}${suffix}`, "fixture");

/**
 * Each child process loads its own copy of the fixture, so a test holds a
 * native call from the library itself: from load when `marker` exists, which
 * a test creates once the probe child has shut down.
 */
const holdShutdownWhen = (marker: string) => `
__attribute__((constructor)) static void fixture_hold_shutdown_when_marked(void) {
  if (access(${JSON.stringify(marker)}, F_OK) == 0) atomic_store(&shutdown_held, 1);
}
`;
const holdStats = `
__attribute__((constructor)) static void fixture_hold_stats(void) { atomic_store(&stats_held, 1); }
`;

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

const runClient = <A, E extends ReactorFailure>(
  effect: Effect.Effect<A, E, PlatformHttp.HttpClient | Crypto.Crypto>,
  fetch: typeof globalThis.fetch,
): Promise<A> =>
  Effect.runPromise(
    effect.pipe(
      Effect.provide(Layer.merge(FetchHttp.layer, NodeServices.layer)),
      Effect.provideService(FetchHttpClient.Fetch, fetch),
    ),
  );

/** The isolated factory, built in the caller's scope. */
const isolatedFactory = (options: Native.Isolated.IsolatedOptions) =>
  Layer.build(Native.Isolated.layer(options)).pipe(
    Effect.map((context) => Context.get(context, PeerFactory)),
  );

/** The canonical factory over the isolated host, recording each peer it makes. */
const isolatedClient = (
  configuration: Configuration,
  options: Native.Isolated.IsolatedOptions,
  peers: IsolatedPeer[],
) =>
  isolatedFactory(options).pipe(
    Effect.flatMap((factory) =>
      makeClient(configuration).pipe(
        Effect.provideService(
          PeerFactory,
          PeerFactory.of({
            make: () => {
              const peer = factory.make() as IsolatedPeer;
              peers.push(peer);
              return peer;
            },
          }),
        ),
      ),
    ),
  );

const create = { model: "fixture/native-session", jwt: Redacted.make("fixture-token") };

describe("isolated native host", () => {
  test.runIf(isBun)("refuses to build under Bun, before any child exists", async () => {
    const result = await Effect.runPromise(
      Effect.scoped(Effect.result(Layer.build(Native.Isolated.layer()))),
    );
    expect(result._tag).toBe("Failure");
    if (result._tag === "Failure")
      expect(result.failure).toMatchObject({
        reason: { _tag: "UnsupportedCapability" },
        context: expect.objectContaining({ outcome: "not-submitted" }),
      });
  });

  test.runIf(onNode)(
    "rejects an invalid deadline and an unloadable library while the layer builds",
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
          Effect.result(
            isolatedFactory({ libraryPath: "/nonexistent/libreactor_effect_native.so" }),
          ),
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
      const compiled = fixture();
      const remote = coordinator();
      const peers: IsolatedPeer[] = [];
      try {
        const result = await runClient(
          Effect.scoped(
            Effect.gen(function* () {
              const factory = yield* isolatedClient(
                { apiUrl: "https://coordinator.fixture" },
                { libraryPath: compiled.path },
                peers,
              );
              const client = yield* factory.create(create);
              yield* client.connect;
              const first = (yield* client.ready).generation;
              const before = yield* (yield* Native.media(client)).snapshot;
              yield* client.reconnect;
              const second = (yield* client.ready).generation;
              const media = yield* Native.media(client);
              const current = peers[1]!;
              const dispatched = current.link.dispatched;
              const reader = yield* Effect.forkChild(
                media.video("main_video").pipe(Stream.runHead),
                { startImmediately: true },
              );
              // The child serves requests in order: the track stream is open
              // before the snapshot releases the fixture's frame.
              yield* Effect.promise(() =>
                until(() => current.link.dispatched > dispatched, "the video stream never opened"),
              );
              const after = yield* media.snapshot;
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
        // One child per generation, each spawned once and ended by its own shutdown.
        expect(peers).toHaveLength(2);
        const [a, b] = peers;
        expect(a!.link.child!.pid).not.toBe(b!.link.child!.pid);
        for (const peer of peers) {
          expect(peer.link.spawns).toBe(1);
          expect(peer.link.exit).toEqual({ code: 0, signal: null });
        }
      } finally {
        rmSync(compiled.directory, { recursive: true, force: true });
      }
    },
    30_000,
  );

  test.runIf(onNode)(
    "fails a dispatched call as unknown when its child dies, fences later calls and never respawns",
    async () => {
      const compiled = fixture();
      try {
        const result = await Effect.runPromise(
          Effect.scoped(
            Effect.gen(function* () {
              const factory = yield* isolatedFactory({ libraryPath: compiled.path });
              const peer = factory.make() as IsolatedPeer;
              // Making the peer forks its child at once, while a session allocates.
              const spawnedAtMake = peer.link.child?.pid !== undefined;
              yield* peer.opened;
              const events: PeerEvent[] = [];
              yield* peer.prepare([], tracks, (event) => events.push(event));
              // The fixture holds a data-channel send for 50 ms.
              const dispatched = peer.link.dispatched;
              const sending = yield* Effect.forkChild(
                Effect.result(peer.send("data", Uint8Array.of(1, 2))),
                { startImmediately: true },
              );
              expect(peer.link.dispatched).toBe(dispatched + 1);
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
              const solo = factory.make() as IsolatedPeer;
              yield* solo.opened;
              solo.link.kill();
              yield* Deferred.await(solo.link.exited);
              const fenced = yield* Effect.result(solo.stats).pipe(
                Effect.timeoutOption("2 seconds"),
              );
              yield* solo.shutdown;
              return {
                spawnedAtMake,
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
        // Later calls are refused before dispatch; nothing waits on a dead child.
        expect(result.later._tag).toBe("Failure");
        if (result.later._tag === "Failure")
          expect(result.later.failure.context.outcome).toBe("not-submitted");
        expect(result.laterMs).toBeLessThan(500);
        // The session hears of the death once, as a connection failure.
        expect(result.events.filter((event) => event.type === "error")).toHaveLength(1);
        expect(result.events.at(-1)).toMatchObject({
          type: "error",
          error: { reason: { _tag: "Native" }, message: "native WebRTC child process exited" },
        });
        expect(result.link.exit).toEqual({ code: null, signal: "SIGKILL" });
        expect(result.link.spawns).toBe(1);
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
        rmSync(compiled.directory, { recursive: true, force: true });
      }
    },
    20_000,
  );

  test.runIf(onNode)(
    "ends a child whose parent dies, even with a native call in flight",
    async () => {
      // The held statistics call keeps the child's event loop alive: without
      // its own exit on disconnect, it would outlive the parent as an orphan.
      const compiled = fixture(holdStats);
      const script = `
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import { PeerFactory } from "reactor-effect-client";
import * as Native from "./dist/index.js";
Effect.runFork(
  Effect.scoped(
    Effect.gen(function* () {
      const context = yield* Layer.build(Native.Isolated.layer({ libraryPath: process.env.FIXTURE }));
      const peer = Context.get(context, PeerFactory).make();
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
        env: { ...process.env, FIXTURE: compiled.path },
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
        expect(alive(orphan)).toBe(true);
        parent.kill("SIGKILL");
        await until(() => !alive(orphan), "the child outlived its parent", 5_000);
      } finally {
        parent.kill("SIGKILL");
        if (child !== undefined && alive(child)) process.kill(child, "SIGKILL");
        rmSync(compiled.directory, { recursive: true, force: true });
      }
    },
    20_000,
  );

  test.runIf(onNode)(
    "kills a child whose shutdown outlives its deadline, reports Shutdown and still terminates the remote session",
    async () => {
      const directory = mkdtempSync(join(tmpdir(), "reactor-isolated-"));
      const marker = join(directory, "hold-shutdown");
      const compiled = fixture(holdShutdownWhen(marker));
      const remote = coordinator();
      const peers: IsolatedPeer[] = [];
      try {
        const result = await runClient(
          Effect.scoped(
            Effect.gen(function* () {
              const factory = yield* isolatedClient(
                { apiUrl: "https://coordinator.fixture" },
                { libraryPath: compiled.path, shutdownTimeout: "250 millis" },
                peers,
              );
              // The probe has shut down; every child from here on holds its native join.
              writeFileSync(marker, "");
              const client = yield* factory.create(create);
              yield* client.connect;
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
        expect(shutdown?.reason._tag).toBe("Shutdown");
        expect(Cause.squash(shutdown?.context.detail as Cause.Cause<unknown>)).toMatchObject({
          reason: { _tag: "Shutdown" },
          message: "native child shutdown exceeded its deadline; child process killed",
        });
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
        rmSync(directory, { recursive: true, force: true });
        rmSync(compiled.directory, { recursive: true, force: true });
      }
    },
    20_000,
  );

  test.runIf(onNode)(
    "never delivers a retired, shut down or killed child's late events and frames, to it or to a later peer",
    async () => {
      const compiled = fixture();
      try {
        const result = await Effect.runPromise(
          Effect.scoped(
            Effect.gen(function* () {
              const factory = yield* isolatedFactory({ libraryPath: compiled.path });

              // Retired with its answer's three events and a frame still to come.
              const a = factory.make() as IsolatedPeer;
              yield* a.opened;
              const aEvents: PeerEvent[] = [];
              const aFrames: VideoFrame[] = [];
              const aScope = yield* Scope.make();
              yield* a
                .prepare([], tracks, (event) => aEvents.push(event))
                .pipe(Scope.provide(aScope));
              const opening = a.link.dispatched;
              const aReader = yield* Effect.forkChild(
                a.rawMedia
                  .video("main_video")
                  .pipe(Stream.runForEach((frame) => Effect.sync(() => aFrames.push(frame)))),
                { startImmediately: true },
              );
              expect(a.link.dispatched).toBe(opening + 1);
              const dispatched = a.link.dispatched;
              const answering = yield* Effect.forkChild(a.answer("fixture answer"), {
                startImmediately: true,
              });
              const releasing = yield* Effect.forkChild(a.rawMedia.snapshot, {
                startImmediately: true,
              });
              // Both requests are on the channel: whatever they release is late.
              expect(a.link.dispatched).toBe(dispatched + 2);
              a.close();
              yield* Fiber.join(answering);
              yield* Fiber.join(releasing);
              yield* Effect.promise(() =>
                until(() => a.link.retired >= 4, "the retired child's late items never arrived"),
              );
              const aReaderExit = yield* Fiber.await(aReader);
              yield* a.shutdown;
              yield* Scope.close(aScope, Exit.void);
              const aRetired = a.link.retired;

              // Killed while connected: one failure event, then nothing.
              const b = factory.make() as IsolatedPeer;
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
              b.close();
              yield* b.shutdown;
              yield* Scope.close(bScope, Exit.void);

              // A later peer hears only its own child.
              const c = factory.make() as IsolatedPeer;
              yield* c.opened;
              const cEvents: PeerEvent[] = [];
              const cScope = yield* Scope.make();
              yield* c
                .prepare([], tracks, (event) => cEvents.push(event))
                .pipe(Scope.provide(cScope));
              yield* c.answer("fixture answer");
              yield* Effect.promise(() => until(() => cEvents.length === 3, "c never connected"));
              c.close();
              yield* c.shutdown;
              yield* Scope.close(cScope, Exit.void);
              return { aEvents, aFrames, aReaderExit, aRetired, a, bEvents, cEvents };
            }),
          ),
        );
        expect(result.aEvents).toEqual([]);
        expect(result.aFrames).toEqual([]);
        // Its readers ended with the retirement, and nothing arrived after shutdown.
        expect(Exit.isSuccess(result.aReaderExit)).toBe(true);
        expect(result.aRetired).toBe(4);
        expect(result.a.link.retired).toBe(result.aRetired);
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
        rmSync(compiled.directory, { recursive: true, force: true });
      }
    },
    30_000,
  );

  test.runIf(onNode)(
    "reports a dispatched call whose wait is cancelled as unknown, and never attributes a late reply to a later call",
    async () => {
      const compiled = fixture(holdStats);
      try {
        const result = await Effect.runPromise(
          Effect.scoped(
            Effect.gen(function* () {
              const factory = yield* isolatedFactory({
                libraryPath: compiled.path,
                shutdownTimeout: "300 millis",
              });
              const peer = factory.make() as IsolatedPeer;
              yield* peer.opened;
              // The caller stops waiting for a send the child holds for 50 ms.
              const dispatched = peer.link.dispatched;
              const sending = yield* Effect.forkChild(peer.send("data", Uint8Array.of(7)), {
                startImmediately: true,
              });
              expect(peer.link.dispatched).toBe(dispatched + 1);
              yield* Fiber.interrupt(sending);
              const interrupted = yield* Fiber.await(sending);
              const snapshot = yield* peer.rawMedia.snapshot;
              yield* Effect.promise(() =>
                until(() => peer.link.late >= 1, "the abandoned send's reply never arrived"),
              );
              const direction = yield* Effect.exit(peer.direction("main_video", true));
              // A statistics read the child never completes: the shutdown deadline
              // kills the child under it.
              const reading = yield* Effect.forkChild(Effect.result(peer.stats), {
                startImmediately: true,
              });
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
        expect(result.link.late).toBeGreaterThanOrEqual(1);
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
        rmSync(compiled.directory, { recursive: true, force: true });
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

  test("fails only the reader that stops consuming, while each stream's credit stays one chunk", async () => {
    const id = "isolated-overflow";
    try {
      const result = await Effect.runPromise(
        Effect.scoped(
          Effect.gen(function* () {
            const factory = yield* isolatedFactory({ libraryPath });
            const peer = factory.make() as IsolatedPeer;
            const failures: ReactorError[] = [];
            const prepared = yield* peer.prepare([], farTracks, (event) => {
              if (event.type === "ice" && event.candidate !== undefined)
                far.candidate(id, event.candidate);
              else if (event.type === "error") failures.push(event.error);
            });
            let fast = 0;
            const gate = yield* Deferred.make<void>();
            const stalled = yield* Effect.forkChild(
              peer.rawMedia.video("main_video").pipe(Stream.runForEach(() => Deferred.await(gate))),
              { startImmediately: true },
            );
            const reader = yield* Effect.forkChild(
              peer.rawMedia.video("main_video").pipe(
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
            const pressure = yield* peer.rawMedia.snapshot;
            peer.close();
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
      // The child sends a stream's next chunk only once the last is acknowledged,
      // and a track's chunk is one frame, so a stream's credit is one frame.
      expect(result.link.maxUnacked).toBe(1);
      expect(result.link.maxFramesPerChunk).toBe(1);
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
              { libraryPath },
              peers,
            );
            const client = yield* factory.create({
              model: "far-peer",
              jwt: Redacted.make("far-peer-token"),
            });
            yield* client.connect;
            const media = yield* Native.media(client);
            const video = yield* media.video("main_video").pipe(Stream.take(8), Stream.runCollect);
            const audio = yield* media.audio("main_audio").pipe(Stream.take(8), Stream.runCollect);
            const pressure = yield* media.snapshot;
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
