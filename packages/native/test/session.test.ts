import type * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Redacted from "effect/Redacted";
import * as Schedule from "effect/Schedule";
import * as Stream from "effect/Stream";
import * as PlatformHttp from "effect/unstable/http/HttpClient";
import type * as Crypto from "effect/Crypto";
import * as FetchHttpClient from "effect/unstable/http/FetchHttpClient";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { describe, expect, test } from "vitest";
import type { ReactorFailure } from "reactor-effect-client/ReactorError";
import type { UploadReference } from "reactor-effect-client/Session";
import { makeFakeAddon, nativeClient } from "./support.js";

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
      Effect.provide(Layer.merge(FetchHttpClient.layer, NodeServices.layer)),
      Effect.provideService(FetchHttpClient.Fetch, fetch),
    ),
  );

describe("native canonical session boundary", () => {
  test("dispatches owned media and preserves bounded submitted requests across caller cancellation", async () => {
    const addon = makeFakeAddon();
    try {
      const result = await runClient(
        Effect.scoped(
          Effect.gen(function* () {
            const factory = yield* nativeClient(
              {
                apiUrl: "https://coordinator.fixture",
                maxPending: 32,
              },
              { addon: addon.path },
            );
            const client = yield* factory.create({
              model: "fixture/native-session",
              jwt: Redacted.make("fixture-token"),
            });
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
            expect((yield* client.snapshot).pending.data).toBe(0);

            // The fake addon's snapshot is a test gate: both generation readers
            // subscribe before it releases media.
            const media = yield* client.decoded;
            expect(media.generation).toBe(ready.generation);
            const videoReader = yield* Effect.forkChild(
              media.video("main_video").pipe(Stream.runHead),
            );
            const audioReader = yield* Effect.forkChild(
              media.audio("main_audio").pipe(Stream.runHead),
            );
            yield* Effect.yieldNow;
            yield* media.pressure;
            const video = Option.getOrThrow(yield* Fiber.join(videoReader));
            const audio = Option.getOrThrow(yield* Fiber.join(audioReader));

            for (let index = 0; index < 32; index++) {
              const fiber = yield* Effect.forkChild(client.command("echo", { index }));
              yield* Effect.sleep(5);
              yield* Fiber.interrupt(fiber);
            }

            const pressure = yield* client.snapshot;
            const overflow = yield* Effect.result(client.command("echo", { index: 32 }));
            const closeReport = yield* client.close;
            const afterClose = yield* Effect.result(client.command("echo", {}));
            const closed = yield* client.snapshot;
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
        // The admission sequence crosses the binding as a full 64-bit value.
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
      addon.remove();
    }
  }, 15_000);

  test("bounds a native owner join that never completes, retains its peer and still terminates the remote session", async () => {
    const addon = makeFakeAddon();
    const remote = coordinator();
    const options = { addon: addon.path, shutdownTimeout: "250 millis" } as const;
    const create = { model: "fixture/native-session", jwt: Redacted.make("fixture-token") };
    try {
      const result = await runClient(
        Effect.scoped(
          Effect.gen(function* () {
            const factory = yield* nativeClient({ apiUrl: "https://coordinator.fixture" }, options);
            const client = yield* factory.create(create);
            addon.hold("shutdown", true);
            const started = performance.now();
            const report = yield* client.close;
            const closeMs = performance.now() - started;
            // The wedged join keeps its peer, and the process admits no new
            // owner, so nothing is allocated for one.
            const degraded = yield* Effect.result(factory.create(create));
            const allocatedWhileDegraded = remote.allocated.length;
            addon.hold("shutdown", false);
            // Once the join completes, the process admits peers again.
            const recovered = yield* factory
              .create(create)
              .pipe(Effect.retry({ schedule: Schedule.spaced("10 millis"), times: 200 }));
            const recoveredReport = yield* recovered.close;
            return { report, closeMs, degraded, allocatedWhileDegraded, recoveredReport };
          }),
        ),
        remote.fetch,
      );

      expect(result.closeMs).toBeGreaterThanOrEqual(200);
      expect(result.closeMs).toBeLessThan(2_000);
      expect(result.report.localClosed).toBe(false);
      expect(result.report.localErrors).toHaveLength(1);
      const [shutdown] = result.report.localErrors;
      expect(shutdown?.reason).toBe("Shutdown");
      // The connection finalizer dies with the typed deadline failure, which
      // cleanup records before it goes on to terminate the owned session.
      expect(shutdown?.message).toBe("native owner join exceeded its deadline; handle retained");
      expect(result.report.remote).toMatchObject({
        attempted: true,
        confirmed: true,
        evidence: "absent",
      });
      expect(remote.deleted.has(sessionId)).toBe(true);

      expect(result.degraded._tag).toBe("Failure");
      if (result.degraded._tag === "Failure")
        expect(result.degraded.failure).toMatchObject({
          reason: { _tag: "Native" },
          context: expect.objectContaining({ outcome: "not-submitted" }),
        });
      expect(result.allocatedWhileDegraded).toBe(1);
      expect(result.recoveredReport.localClosed).toBe(true);
      expect(result.recoveredReport.localErrors).toEqual([]);
      expect(remote.allocated).toHaveLength(2);
    } finally {
      addon.hold("shutdown", false);
      addon.remove();
    }
  }, 15_000);

  test("rejects a shutdown deadline that is not a positive duration when the layer is built", async () => {
    const addon = makeFakeAddon();
    const remote = coordinator();
    try {
      for (const shutdownTimeout of [0, -1, Number.NaN, "soon"]) {
        // No Client can exist, so nothing can allocate a remote session.
        const result = await runClient(
          Effect.scoped(
            Effect.result(
              nativeClient(
                { apiUrl: "https://coordinator.fixture" },
                { addon: addon.path, shutdownTimeout: shutdownTimeout as Duration.Input },
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
      addon.remove();
    }
  });

  test("fails to build the layer, before any allocation, when the addon cannot load", async () => {
    const remote = coordinator();
    const result = await runClient(
      Effect.scoped(
        Effect.result(
          nativeClient(
            { apiUrl: "https://coordinator.fixture" },
            { addon: "/nonexistent/reactor-effect-native.node" },
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
