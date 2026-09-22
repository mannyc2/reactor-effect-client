import { spawnSync } from "node:child_process";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Redacted from "effect/Redacted";
import * as Stream from "effect/Stream";
import * as PlatformHttp from "effect/unstable/http/HttpClient";
import * as FetchHttpClient from "effect/unstable/http/FetchHttpClient";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { describe, expect, test } from "vitest";
import { ReactorError } from "../src/errors.js";
import * as FetchHttp from "../src/FetchHttp.js";
import { checkNativeBridge } from "../src/native-bridge.js";
import { NativePeer } from "../src/native-peer.js";
import type { PeerFactoryShape } from "../src/PeerFactory.js";
import * as SessionClient from "../src/SessionClient.js";

const fixtureSource = fileURLToPath(new URL("./native-session-fixture.c", import.meta.url));
const sessionId = "sess_native_fixture";
const descriptor = {
  session_id: sessionId,
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
};

const coordinatorFetch = async (input: RequestInfo | URL, init?: RequestInit): Promise<Response> => {
  const request = new Request(input, init);
  const path = new URL(request.url).pathname;
  if (path === "/tokens") return Response.json({ jwt: "fixture-jwt" });
  if ((path === "/sessions" && request.method === "POST") || path === "/start_session") return Response.json(descriptor);
  if (path === `/sessions/${sessionId}` || path === "/session") return Response.json(descriptor);
  if (path.endsWith("/ice_servers")) return Response.json({ ice_servers: [] });
  if (path.endsWith("/connections")) return Response.json({ connection_id: 1001 });
  if (path.endsWith("/ice_candidates")) return new Response(null, { status: 204 });
  if (path.endsWith("/sdp_params")) {
    if (request.method !== "GET") return new Response(null, { status: 204 });
    return Response.json({ sdp_answer: "fixture native answer" });
  }
  if ((path === `/sessions/${sessionId}` && request.method === "DELETE") || path === "/stop_session") return new Response(null, { status: 202 });
  return Response.json({ error: `unhandled native fixture route ${path}` }, { status: 404 });
};

const runClient = <A>(effect: Effect.Effect<A, ReactorError, PlatformHttp.HttpClient | NodeServices.NodeServices>): Promise<A> =>
  Effect.runPromise(effect.pipe(
    Effect.provide(Layer.merge(FetchHttp.layer, NodeServices.layer)),
    Effect.provideService(FetchHttpClient.Fetch, coordinatorFetch as typeof fetch),
  ));

const compileFixture = (): { readonly directory: string; readonly path: string } => {
  const directory = mkdtempSync(join(tmpdir(), "reactor-native-session-"));
  const extension = process.platform === "darwin" ? "dylib" : "so";
  const path = join(directory, `libreactor_native_session_fixture.${extension}`);
  const compiler = process.env.CC ?? "cc";
  const platformFlags = process.platform === "darwin" ? ["-dynamiclib"] : ["-shared", "-fPIC"];
  const result = spawnSync(compiler, ["-std=c11", "-D_DEFAULT_SOURCE", ...platformFlags, fixtureSource, "-o", path], { encoding: "utf8" });
  if (result.error !== undefined) throw result.error;
  if (result.status !== 0) throw new Error(`${compiler} failed: ${result.stderr}`);
  return { directory, path };
};

describe("native SessionClient boundary", () => {
  test("dispatches owned media and preserves bounded submitted requests across caller cancellation", async () => {
    if (process.platform === "win32") return;
    const compiled = compileFixture();
    try {
      const peers: PeerFactoryShape = {
        check: Effect.tryPromise({
          try: () => checkNativeBridge(compiled.path),
          catch: (cause) => cause instanceof ReactorError ? cause : new ReactorError("Native", "fixture preflight failed", { detail: cause }),
        }),
        make: () => new NativePeer(compiled.path),
      };

      const result = await runClient(Effect.scoped(Effect.gen(function* () {
          const client = yield* SessionClient.make(peers, {
            modelName: "fixture/native-session",
            jwt: Redacted.make("fixture-token"),
            apiUrl: "https://coordinator.fixture",
          });
          yield* client.connect();

          for (const size of [Number.NaN, 1.5, -1]) {
            const invalid = yield* Effect.result(client.send("echo", {}, {
              picture: { upload_id: "fixture-upload", name: "image.png", mime_type: "image/png", size },
            }));
            expect(invalid._tag).toBe("Failure");
            if (invalid._tag === "Failure") expect(invalid.failure).toMatchObject({
              code: "InvalidInput", context: expect.objectContaining({ outcome: "not-submitted" }),
            });
          }
          expect((yield* client.snapshot).pendingRequests).toBe(0);

          // The fixture uses the snapshot call as an explicit test-only gate so
          // media cannot race ahead of SessionClient's raw-media subscriptions.
          yield* client.snapshot;
          const video = Option.getOrThrow(yield* client.video.pipe(Stream.runHead));
          const audio = Option.getOrThrow(yield* client.audio.pipe(Stream.runHead));

          for (let index = 0; index < 32; index++) {
            const fiber = yield* Effect.forkChild(client.send("echo", { index }));
            yield* Effect.sleep(5);
            yield* Fiber.interrupt(fiber);
          }

          const pressure = yield* client.snapshot;
          const overflow = yield* Effect.result(client.send("echo", { index: 32 }));
          yield* client.close;
          const afterClose = yield* Effect.result(client.send("echo", {}));
          const closed = yield* client.snapshot;
          return { video, audio, pressure, overflow, afterClose, closed };
        })));

      expect(result.video).toMatchObject({
        _tag: "VideoFrame",
        track: "main_video",
        width: 1,
        height: 1,
        frameId: 18446744073709551615n,
        timestampMicros: 9007199254740993n,
      });
      expect([...result.video.data]).toEqual([1, 2, 3, 4]);
      expect([...result.video.metadata]).toEqual([9, 8, 7]);
      expect(result.audio).toMatchObject({ _tag: "AudioFrame", track: "main_audio", sampleRate: 48_000, channels: 2 });
      expect([...result.audio.samples]).toEqual([1, -2, 300, -400]);

      expect(result.pressure.pendingRequests).toBe(32);
      expect(result.overflow._tag).toBe("Failure");
      if (result.overflow._tag === "Failure") {
        expect(result.overflow.failure).toMatchObject({ code: "Overflow", context: expect.objectContaining({ outcome: "not-submitted" }) });
      }
      expect(result.afterClose._tag).toBe("Failure");
      if (result.afterClose._tag === "Failure") {
        expect(result.afterClose.failure).toMatchObject({ code: "Closed", context: expect.objectContaining({ outcome: "not-submitted" }) });
      }
      expect(result.closed.closed).toBe(true);
    } finally {
      rmSync(compiled.directory, { recursive: true, force: true });
    }
  }, 15_000);
});
