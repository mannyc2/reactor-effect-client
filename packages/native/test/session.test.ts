import { rmSync } from "node:fs";
import * as Cause from "effect/Cause";
import type * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Redacted from "effect/Redacted";
import * as Stream from "effect/Stream";
import * as PlatformHttp from "effect/unstable/http/HttpClient";
import type * as Crypto from "effect/Crypto";
import * as FetchHttpClient from "effect/unstable/http/FetchHttpClient";
import * as NodeServices from "@effect/platform-node/NodeServices";
import koffi from "koffi";
import { describe, expect, test, vi } from "vitest";
import { FetchHttp } from "reactor-effect-client";
import type { ReactorFailure } from "reactor-effect-client";
import * as Native from "../src/index.js";
import type { UploadReference } from "reactor-effect-client/wire";
import { compileFixture, nativeClient, until } from "./support.js";

const sessionId = "sess_native_fixture";
const descriptor = (id: string) => ({
  session_id: id,
  state: "ACTIVE",
  capabilities: {
    protocol_version: "1.0",
    tracks: [
      { name: "main_video", kind: "video", direction: "recvonly" },
      { name: "main_audio", kind: "audio", direction: "recvonly" },
      { name: "input_audio", kind: "audio", direction: "sendonly" },
    ],
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
    if (path === "/tokens") return Response.json({ jwt: "fixture-jwt" });
    if ((path === "/sessions" && request.method === "POST") || path === "/start_session") {
      const allocation = allocated.length === 0 ? sessionId : `${sessionId}_${allocated.length}`;
      allocated.push(allocation);
      return Response.json(descriptor(allocation));
    }
    if ((path === `/sessions/${id}` || path === "/session") && request.method === "GET")
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
    if ((path === `/sessions/${id}` && request.method === "DELETE") || path === "/stop_session") {
      deleted.add(id);
      return new Response(null, { status: 202 });
    }
    return Response.json({ error: `unhandled native fixture route ${path}` }, { status: 404 });
  };
  return { fetch, allocated, deleted };
};

const runClient = <A, E extends ReactorFailure>(
  effect: Effect.Effect<A, E, PlatformHttp.HttpClient | Crypto.Crypto>,
  fetch: typeof globalThis.fetch = coordinator().fetch,
): Promise<A> =>
  Effect.runPromise(
    effect.pipe(
      Effect.provide(Layer.merge(FetchHttp.layer, NodeServices.layer)),
      Effect.provideService(FetchHttpClient.Fetch, fetch),
    ),
  );

describe("native canonical session boundary", () => {
  test("dispatches owned media and preserves bounded submitted requests across caller cancellation", async () => {
    if (process.platform === "win32") return;
    const compiled = compileFixture();
    try {
      const result = await runClient(
        Effect.scoped(
          Effect.gen(function* () {
            const factory = yield* nativeClient(
              {
                apiUrl: "https://coordinator.fixture",
                session: { maxPending: 32 },
              },
              { libraryPath: compiled.path },
            );
            const client = yield* factory.create({
              model: "fixture/native-session",
              jwt: Redacted.make("fixture-token"),
            });
            yield* client.connect;
            const ready = yield* client.ready;
            expect(ready.remote.ownership).toBe("owned");

            for (const size of [Number.NaN, 1.5, -1, -1n]) {
              // Exercise JavaScript callers with values outside the declared
              // bigint contract as well as a negative bigint within that type.
              const reference = {
                upload_id: "fixture-upload",
                name: "image.png",
                mime_type: "image/png",
                size,
              } as unknown as UploadReference;
              const invalid = yield* Effect.result(
                client.command("echo", {}, { uploads: new Map([["picture", reference]]) }),
              );
              expect(invalid._tag).toBe("Failure");
              if (invalid._tag === "Failure")
                expect(invalid.failure).toMatchObject({
                  reason: { _tag: "InvalidInput" },
                  context: expect.objectContaining({ outcome: "not-submitted" }),
                });
            }
            expect((yield* client.current).pending.data).toBe(0);

            // The fixture uses the snapshot call as an explicit test-only gate so
            // both generation readers subscribe before native media is released.
            const media = yield* Native.media(client);
            expect(media.generation).toBe(ready.generation);
            const videoReader = yield* Effect.forkChild(
              media.video("main_video").pipe(Stream.runHead),
            );
            const audioReader = yield* Effect.forkChild(
              media.audio("main_audio").pipe(Stream.runHead),
            );
            yield* Effect.yieldNow;
            yield* media.snapshot;
            const video = Option.getOrThrow(yield* Fiber.join(videoReader));
            const audio = Option.getOrThrow(yield* Fiber.join(audioReader));

            for (let index = 0; index < 32; index++) {
              const fiber = yield* Effect.forkChild(client.command("echo", { index }));
              yield* Effect.sleep(5);
              yield* Fiber.interrupt(fiber);
            }

            const pressure = yield* client.current;
            const overflow = yield* Effect.result(client.command("echo", { index: 32 }));
            const closeReport = yield* client.close;
            const afterClose = yield* Effect.result(client.command("echo", {}));
            const closed = yield* client.current;
            return { video, audio, pressure, overflow, afterClose, closed, closeReport };
          }),
        ),
      );

      expect(result.video).toMatchObject({
        _tag: "VideoFrame",
        format: "BGRA",
        track: "main_video",
        width: 1,
        height: 1,
        frameId: 18446744073709551615n,
        timestampMicros: 9007199254740993n,
        // The admission sequence crosses the ABI as a full 64-bit value.
        sequence: 9007199254740993n,
      });
      expect([...result.video.data]).toEqual([1, 2, 3, 4]);
      expect([...result.video.metadata]).toEqual([9, 8, 7]);
      expect(result.audio).toMatchObject({
        _tag: "AudioFrame",
        track: "main_audio",
        sampleRate: 48_000,
        channels: 2,
        sequence: 3n,
      });
      expect([...result.audio.samples]).toEqual([1, -2, 300, -400]);

      expect(result.pressure.pending.data).toBe(32);
      expect(result.overflow._tag).toBe("Failure");
      if (result.overflow._tag === "Failure") {
        expect(result.overflow.failure).toMatchObject({
          reason: { _tag: "Overflow" },
          context: expect.objectContaining({ outcome: "not-submitted" }),
        });
      }
      expect(result.afterClose._tag).toBe("Failure");
      if (result.afterClose._tag === "Failure") {
        expect(result.afterClose.failure).toMatchObject({
          reason: { _tag: "Closed" },
          context: expect.objectContaining({ outcome: "not-submitted" }),
        });
      }
      expect(result.closed.status).toBe("closed");
      expect(result.closeReport.localClosed).toBe(true);
      expect(result.closeReport.localErrors).toEqual([]);
    } finally {
      rmSync(compiled.directory, { recursive: true, force: true });
    }
  }, 15_000);

  test("bounds a native owner join that never completes, retains its bridge and still terminates the remote session", async () => {
    if (process.platform === "win32") return;
    const compiled = compileFixture();
    const library = koffi.load(compiled.path);
    const hold: (held: number) => void = library.func("void fixture_shutdown_hold(int held)");
    const stat: (which: number) => number = library.func("int fixture_lifetime_stat(int which)");
    const unregister = vi.spyOn(koffi, "unregister");
    const remote = coordinator();
    const options = { libraryPath: compiled.path, shutdownTimeout: "250 millis" } as const;
    const create = { model: "fixture/native-session", jwt: Redacted.make("fixture-token") };
    try {
      const result = await runClient(
        Effect.scoped(
          Effect.gen(function* () {
            const factory = yield* nativeClient({ apiUrl: "https://coordinator.fixture" }, options);
            const client = yield* factory.create(create);
            yield* client.connect;
            hold(1);
            const started = performance.now();
            const report = yield* client.close;
            const closeMs = performance.now() - started;
            // The wedged join keeps its handle and callback, and the process
            // admits no new owner, so nothing is allocated for one.
            const retained = {
              entered: stat(8),
              destroyed: stat(9),
              unregistered: unregister.mock.calls.length,
            };
            const degraded = yield* Effect.result(factory.create(create));
            const allocatedWhileDegraded = remote.allocated.length;
            hold(0);
            yield* Effect.promise(() =>
              until(() => stat(9) === 1, "the released join never destroyed its handle"),
            );
            const recovered = yield* factory.create(create);
            yield* recovered.connect;
            const recoveredReport = yield* recovered.close;
            return { report, closeMs, retained, degraded, allocatedWhileDegraded, recoveredReport };
          }),
        ),
        remote.fetch,
      );

      expect(result.closeMs).toBeGreaterThanOrEqual(200);
      expect(result.closeMs).toBeLessThan(2_000);
      expect(result.report.localClosed).toBe(false);
      expect(result.report.localErrors).toHaveLength(1);
      const [shutdown] = result.report.localErrors;
      expect(shutdown?.reason._tag).toBe("Shutdown");
      // The connection finalizer dies with the typed deadline failure, which
      // cleanup records before it goes on to terminate the owned session.
      expect(Cause.squash(shutdown?.context.detail as Cause.Cause<unknown>)).toMatchObject({
        reason: { _tag: "Shutdown" },
        message: "native owner join exceeded its deadline; handle retained",
      });
      expect(result.report.remote).toMatchObject({
        attempted: true,
        confirmed: true,
        evidence: "absent",
      });
      expect(remote.deleted.has(sessionId)).toBe(true);
      expect(result.retained).toEqual({ entered: 1, destroyed: 0, unregistered: 0 });

      expect(result.degraded._tag).toBe("Failure");
      if (result.degraded._tag === "Failure")
        expect(result.degraded.failure).toMatchObject({
          reason: { _tag: "Native" },
          context: expect.objectContaining({ outcome: "not-submitted" }),
        });
      expect(result.allocatedWhileDegraded).toBe(1);

      // Once the join completes it destroys and unregisters, and the process
      // admits peers again.
      expect(unregister).toHaveBeenCalledTimes(2);
      expect(result.recoveredReport.localClosed).toBe(true);
      expect(result.recoveredReport.localErrors).toEqual([]);
      expect(remote.allocated).toHaveLength(2);
    } finally {
      hold(0);
      unregister.mockRestore();
      rmSync(compiled.directory, { recursive: true, force: true });
    }
  }, 15_000);

  test("rejects a shutdown deadline that is not a positive duration when the layer is built", async () => {
    if (process.platform === "win32") return;
    const compiled = compileFixture();
    const remote = coordinator();
    try {
      for (const shutdownTimeout of [0, -1, Number.NaN, "soon"]) {
        // No Client can exist, so nothing can allocate a remote session.
        const result = await runClient(
          Effect.scoped(
            Effect.result(
              nativeClient(
                { apiUrl: "https://coordinator.fixture" },
                { libraryPath: compiled.path, shutdownTimeout: shutdownTimeout as Duration.Input },
              ),
            ),
          ),
          remote.fetch,
        );
        expect(result._tag).toBe("Failure");
        if (result._tag === "Failure")
          expect(result.failure).toMatchObject({
            reason: { _tag: "InvalidInput" },
            context: expect.objectContaining({ outcome: "not-submitted" }),
          });
      }
      expect(remote.allocated).toEqual([]);
    } finally {
      rmSync(compiled.directory, { recursive: true, force: true });
    }
  });

  test("fails to build the layer, before any allocation, when the library cannot load", async () => {
    const remote = coordinator();
    const result = await runClient(
      Effect.scoped(
        Effect.result(
          nativeClient(
            { apiUrl: "https://coordinator.fixture" },
            { libraryPath: "/nonexistent/libreactor_effect_native.so" },
          ),
        ),
      ),
      remote.fetch,
    );
    expect(result._tag).toBe("Failure");
    if (result._tag === "Failure")
      expect(result.failure).toMatchObject({
        reason: expect.objectContaining({ _tag: "Native" }),
        context: expect.objectContaining({ outcome: "not-submitted" }),
      });
    expect(remote.allocated).toEqual([]);
  });
});
