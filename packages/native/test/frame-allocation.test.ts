import { Buffer } from "node:buffer";
import { rmSync } from "node:fs";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Redacted from "effect/Redacted";
import * as Stream from "effect/Stream";
import type * as Crypto from "effect/Crypto";
import type * as PlatformHttp from "effect/unstable/http/HttpClient";
import * as FetchHttpClient from "effect/unstable/http/FetchHttpClient";
import * as NodeServices from "@effect/platform-node/NodeServices";
import koffi from "koffi";
import { describe, expect, test, vi } from "vitest";
import { FetchHttp } from "reactor-effect-client";
import type { ReactorFailure } from "reactor-effect-client";
import { assertExactFrames } from "reactor-effect-test-kit/frames";
import { checkNativeBridge, NativeBridge } from "../src/_internal/bridge.js";
import type { NativeVideo } from "../src/_internal/bridge.js";
import * as Native from "../src/index.js";
import { compileFrameFixture, expectedPixel } from "./frame-fixture.js";
import { compileFixture } from "./support.js";

/*
 * The allocation budget for decoded video on the JavaScript side. The native
 * take copies each frame once into caller memory; from there to a Stream
 * consumer the host adds no pixel copy and hands over one exclusively owned,
 * exact-size allocation.
 */

interface Ledger {
  /** Byte sizes of typed arrays and Buffers allocated by length. */
  readonly allocations: number[];
  /** Byte counts of every copy between JavaScript-visible buffers. */
  readonly copies: number[];
}

const TypedArrayPrototype = Object.getPrototypeOf(Uint8Array.prototype) as object;
const TypedArrayConstructor = Object.getPrototypeOf(Uint8Array) as object;

const byteSize = (value: unknown): number =>
  ArrayBuffer.isView(value) || value instanceof ArrayBuffer
    ? value.byteLength
    : Array.isArray(value)
      ? value.length
      : 0;

/** Record the typed-array allocations and byte copies `body` makes, synchronously. */
const measure = <A>(body: () => A): { readonly value: A; readonly ledger: Ledger } => {
  const ledger: Ledger = { allocations: [], copies: [] };
  const restore: (() => void)[] = [];
  const wrap = (
    target: object,
    key: string,
    record: (result: unknown, args: readonly unknown[]) => void,
  ): void => {
    const descriptor = Object.getOwnPropertyDescriptor(target, key);
    if (descriptor === undefined || typeof descriptor.value !== "function")
      throw new Error(`cannot instrument ${key}`);
    const original = descriptor.value as (...args: unknown[]) => unknown;
    Object.defineProperty(target, key, {
      ...descriptor,
      value: function (this: unknown, ...args: unknown[]): unknown {
        const result = original.apply(this, args);
        record(result, args);
        return result;
      },
    });
    restore.push(() => Object.defineProperty(target, key, descriptor));
  };
  const copied = (result: unknown): void => {
    ledger.copies.push(byteSize(result));
  };
  const allocated = (result: unknown): void => {
    ledger.allocations.push(byteSize(result));
  };
  wrap(TypedArrayPrototype, "slice", copied);
  wrap(TypedArrayPrototype, "set", (_, args) => ledger.copies.push(byteSize(args[0])));
  wrap(TypedArrayConstructor, "from", copied);
  wrap(ArrayBuffer.prototype, "slice", copied);
  wrap(Buffer, "from", copied);
  wrap(Buffer, "concat", copied);
  wrap(Buffer, "alloc", allocated);
  wrap(Buffer, "allocUnsafe", allocated);
  wrap(Buffer, "allocUnsafeSlow", allocated);
  const OriginalUint8Array = globalThis.Uint8Array;
  globalThis.Uint8Array = new Proxy(OriginalUint8Array, {
    construct(target, args: unknown[], newTarget: new (...args: never[]) => object) {
      const result: unknown = Reflect.construct(target, args, newTarget);
      if (typeof args[0] === "number") allocated(result);
      return result as object;
    },
  });
  restore.push(() => {
    globalThis.Uint8Array = OriginalUint8Array;
  });
  try {
    return { value: body(), ledger };
  } finally {
    for (const undo of restore.reverse()) undo();
  }
};

describe("decoded video allocation budget", () => {
  test("each take hands the caller the one exact-size allocation native code copied the frame into", async () => {
    if (process.platform === "win32") return;
    const fixture = compileFrameFixture();
    const library = koffi.load(fixture.path);
    const push = library.func(
      "void fixture_push(uint32_t width, uint32_t height, uint32_t metadata, uint32_t count)",
    ) as (width: number, height: number, metadata: number, count: number) => void;
    let bridge: NativeBridge | undefined;
    try {
      await checkNativeBridge(fixture.path);
      bridge = new NativeBridge(fixture.path, () => undefined);
      const ownedBridge = bridge;
      // First frame, two steady frames (one without metadata), a resolution
      // drop, a steady frame at the smaller size, then a resolution rise.
      const script = [
        { width: 64, height: 32, metadata: 5, kind: "first" },
        { width: 64, height: 32, metadata: 5, kind: "steady" },
        { width: 64, height: 32, metadata: 0, kind: "steady" },
        { width: 32, height: 16, metadata: 7, kind: "shrink" },
        { width: 32, height: 16, metadata: 7, kind: "steady" },
        { width: 64, height: 32, metadata: 5, kind: "grow" },
      ] as const;
      for (const frame of script) push(frame.width, frame.height, frame.metadata, 1);

      const taken: { readonly frame: NativeVideo; readonly ledger: Ledger }[] = [];
      for (;;) {
        const { value, ledger } = measure(() => ownedBridge.takeVideo());
        if (value === undefined || value === null) break;
        taken.push({ frame: value, ledger });
      }
      console.log(
        JSON.stringify({
          ledgers: taken.map(({ frame, ledger }, index) => ({
            kind: script[index]?.kind,
            dataBytes: frame.data.byteLength,
            metadataBytes: frame.metadata.byteLength,
            ...ledger,
          })),
        }),
      );
      expect(taken).toHaveLength(script.length);

      // One exclusively owned, exact-size allocation per plane: no pooled
      // slab, no retained slack, nothing else reachable through `.buffer`.
      const frames = taken.map(({ frame }) => frame);
      assertExactFrames(frames, (frame) => frame.data);
      assertExactFrames(frames, (frame) => frame.metadata);
      taken.forEach(({ frame, ledger }, index) => {
        const spec = script[index];
        if (spec === undefined) throw new Error("unscripted frame");
        const size = spec.width * spec.height * 4;
        expect(frame).toMatchObject({ width: spec.width, height: spec.height, track: 0 });
        expect(frame.frameId).toBe(BigInt(index + 1));
        // The declared format: BGRA, four bytes per pixel.
        expect(frame.data.byteLength).toBe(size);
        expect(frame.metadata.byteLength).toBe(spec.metadata);
        expect(frame.metadata.buffer).not.toBe(frame.data.buffer);

        // No JavaScript copy of pixel bytes except the one deliberate trim
        // when a smaller frame lands in its larger predecessor's buffer.
        const pixelCopies = ledger.copies.filter((bytes) => bytes > spec.metadata);
        expect(pixelCopies).toEqual(spec.kind === "shrink" ? [size] : []);
        const pixelAllocations = ledger.allocations.filter((bytes) => bytes > spec.metadata);
        if (spec.kind === "steady" || spec.kind === "shrink") {
          // Only the next frame's buffer, sized from this one.
          expect(pixelAllocations).toEqual([size]);
        } else {
          // Grow to fit this frame after BUFFER_TOO_SMALL, then the next buffer.
          expect(pixelAllocations).toEqual([size, size]);
        }
      });

      // The bridge never writes into a buffer it has handed over.
      taken.forEach(({ frame }, index) => {
        for (let offset = 0; offset < frame.data.byteLength; offset++) {
          if (frame.data[offset] !== expectedPixel(index + 1, offset))
            throw new Error(`frame ${index + 1} byte ${offset} changed after hand-off`);
        }
        expect([...frame.metadata]).toEqual(
          Array.from({ length: frame.metadata.byteLength }, () => 0xa0 + index),
        );
      });
    } finally {
      if (bridge !== undefined) await bridge.shutdown();
      rmSync(fixture.directory, { recursive: true, force: true });
    }
  });

  test("a public generation's Stream consumer receives the very buffers the bridge took", async () => {
    if (process.platform === "win32") return;
    const compiled = compileFixture();
    const takeVideo = vi.spyOn(NativeBridge.prototype, "takeVideo");
    try {
      const video = await Effect.runPromise(
        Effect.scoped(
          Effect.gen(function* () {
            const factory = yield* Native.make(
              { apiUrl: "https://coordinator.fixture" },
              { libraryPath: compiled.path },
            );
            const client = yield* factory.create({
              model: "fixture/native-session",
              jwt: Redacted.make("fixture-token"),
            });
            yield* client.connect;
            const media = yield* Native.media(client);
            const reader = yield* Effect.forkChild(media.video("main_video").pipe(Stream.runHead));
            yield* Effect.yieldNow;
            // The session fixture releases media only after this snapshot call.
            yield* media.snapshot;
            const frame = Option.getOrThrow(yield* Fiber.join(reader));
            yield* client.close;
            return frame;
          }),
        ).pipe(withCoordinator),
      );
      const taken = takeVideo.mock.results
        .map((result) => (result.type === "return" ? result.value : undefined))
        .filter((value): value is NativeVideo => value !== undefined && value !== null);
      expect(taken).toHaveLength(1);
      const [source] = taken;
      // Identity, not equality: bridge -> peer -> Observations -> Queue ->
      // Stream -> generation guard adds no copy.
      expect(video.data).toBe(source?.data);
      expect(video.metadata).toBe(source?.metadata);
      assertExactFrames([video], (frame) => frame.data);
      expect(video.data.byteLength).toBe(4);
      expect([...video.data]).toEqual([1, 2, 3, 4]);
      expect([...video.metadata]).toEqual([9, 8, 7]);
    } finally {
      takeVideo.mockRestore();
      rmSync(compiled.directory, { recursive: true, force: true });
    }
  }, 15_000);
});

const sessionId = "sess_native_frames";
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

const coordinatorFetch = async (
  input: string | Request | URL,
  init?: RequestInit,
): Promise<Response> => {
  const request = new Request(input, init);
  const path = new URL(request.url).pathname;
  if (path === "/tokens") return Response.json({ jwt: "fixture-jwt" });
  if ((path === "/sessions" && request.method === "POST") || path === "/start_session")
    return Response.json(descriptor);
  if ((path === `/sessions/${sessionId}` || path === "/session") && request.method === "GET")
    return Response.json(descriptor);
  if (path.endsWith("/ice_servers")) return Response.json({ ice_servers: [] });
  if (path.endsWith("/connections")) return Response.json({ connection_id: 1001 });
  if (path.endsWith("/ice_candidates")) return new Response(null, { status: 204 });
  if (path.endsWith("/sdp_params")) {
    if (request.method !== "GET") return new Response(null, { status: 204 });
    return Response.json({ sdp_answer: "fixture native answer" });
  }
  if (
    (path === `/sessions/${sessionId}` && request.method === "DELETE") ||
    path === "/stop_session"
  )
    return new Response(null, { status: 202 });
  return Response.json({ error: `unhandled native fixture route ${path}` }, { status: 404 });
};

const withCoordinator = <A, E extends ReactorFailure>(
  effect: Effect.Effect<A, E, PlatformHttp.HttpClient | Crypto.Crypto>,
): Effect.Effect<A, E> =>
  effect.pipe(
    Effect.provide(Layer.merge(FetchHttp.layer, NodeServices.layer)),
    Effect.provideService(FetchHttpClient.Fetch, coordinatorFetch),
  );
