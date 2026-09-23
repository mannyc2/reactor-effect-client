import { rmSync } from "node:fs";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Stream from "effect/Stream";
import * as TestClock from "effect/testing/TestClock";
import koffi from "koffi";
import type { ReactorError } from "reactor-effect-client";
import { describe, expect, test, vi } from "vitest";
import { checkNativeBridge } from "../src/_internal/bridge.js";
import { NativePeer } from "../src/_internal/peer.js";
import { compileFixture, until } from "./support.js";

const compile = () => {
  const fixture = compileFixture();
  return { ...fixture, library: koffi.load(fixture.path) };
};

const tracks = [
  { name: "main_video", kind: "video", direction: "recvonly" },
  { name: "main_audio", kind: "audio", direction: "recvonly" },
  { name: "input_audio", kind: "audio", direction: "sendonly" },
] as const;

describe("native foreign-call ownership", () => {
  test("joins active and queued Koffi work after Effect interruption before one handle destruction", async () => {
    if (process.platform === "win32") return;
    const fixture = compile();
    const begin = fixture.library.func("void fixture_lifetime_begin(int expected)");
    const stat: (which: number) => number = fixture.library.func(
      "int fixture_lifetime_stat(int which)",
    );
    const release = fixture.library.func("void fixture_lifetime_release(int which)");
    const dispose = fixture.library.func("int fixture_lifetime_dispose(void)");
    const expected = 96;
    let peer: NativePeer | undefined;
    begin(expected);
    try {
      await checkNativeBridge(fixture.path);
      peer = new NativePeer(fixture.path);
      const ownedPeer = peer;
      const inputs = Array.from({ length: expected }, (_, index) =>
        Uint8Array.of(index, 255 - index),
      );
      let dispatched = 0;
      const fibers = inputs.map((input) =>
        Effect.runFork(
          Effect.gen(function* () {
            dispatched++;
            yield* ownedPeer.send("data", input);
          }),
        ),
      );
      await until(
        () => dispatched === expected && stat(0) > 0,
        "foreign calls never entered the fixture",
      );
      // All effects have dispatched, but blocked native workers cannot have
      // entered every call. This establishes queued work in the actual executor.
      expect(stat(0)).toBeLessThan(expected);
      expect(stat(1)).toBe(0);
      for (const input of inputs) input.fill(0);
      await Promise.all(fibers.map((fiber) => Effect.runPromise(Fiber.interrupt(fiber))));
      for (const fiber of fibers) {
        const exit = await Effect.runPromise(Fiber.await(fiber));
        expect(exit._tag).toBe("Failure");
      }

      let finished = false;
      const shutdown = Effect.runPromise(peer.shutdown).then(() => {
        finished = true;
      });
      const concurrent = Effect.runPromise(peer.shutdown);
      release(0); // Drain the queued work while the oldest foreign call stays held.
      await until(
        () => stat(1) === expected - 1,
        "queued native calls did not drain after release",
      );
      expect(stat(0)).toBe(expected);
      expect(stat(2)).toBe(0);
      expect(stat(3)).toBe(0);
      expect(finished).toBe(false);

      release(1);
      await Promise.all([shutdown, concurrent]);
      expect(stat(1)).toBe(expected);
      expect(stat(2)).toBe(1);
      expect(stat(3)).toBe(1);
      expect(stat(4)).toBe(0); // Destroy with queued/active calls.
      expect(stat(5)).toBe(0); // Native access after the destruction marker.
      expect(stat(6)).toBe(0); // FFI inputs survived mutation of caller buffers.
      expect(dispose()).toBe(0);
      peer = undefined;
    } finally {
      release(2);
      if (peer !== undefined) {
        await Effect.runPromise(peer.shutdown);
        dispose();
      }
      rmSync(fixture.directory, { recursive: true, force: true });
    }
  }, 15_000);

  test("unregisters the readiness callback only after shutdown joined the notifier thread", async () => {
    if (process.platform === "win32") return;
    const fixture = compile();
    const stat: (which: number) => number = fixture.library.func(
      "int fixture_lifetime_stat(int which)",
    );
    const unregister = koffi.unregister;
    const joinsAtUnregister: number[] = [];
    const spy = vi.spyOn(koffi, "unregister").mockImplementation((callback) => {
      joinsAtUnregister.push(stat(7));
      unregister(callback);
    });
    try {
      await checkNativeBridge(fixture.path);
      const joins = stat(7);
      const peer = new NativePeer(fixture.path);
      await Effect.runPromise(peer.shutdown);
      await Effect.runPromise(peer.shutdown);
      expect(joinsAtUnregister).toEqual([joins + 1]);
    } finally {
      spy.mockRestore();
      rmSync(fixture.directory, { recursive: true, force: true });
    }
  });

  test("source failure reaches existing and future decoded-media readers", async () => {
    if (process.platform === "win32") return;
    const fixture = compile();
    const fault = fixture.library.func("void fixture_media_fault(int enabled)");
    let peer: NativePeer | undefined;
    try {
      await checkNativeBridge(fixture.path);
      peer = new NativePeer(fixture.path);
      const ownedPeer = peer;
      const errors: unknown[] = [];
      await Effect.runPromise(
        Effect.scoped(
          Effect.gen(function* () {
            // Finalizers have no typed error channel; a shutdown defect must still fail this test.
            yield* Effect.addFinalizer(() => ownedPeer.shutdown.pipe(Effect.orDie));
            yield* ownedPeer.prepare([], tracks, (event) => {
              if (event.type === "error") errors.push(event.error);
            });
            const reader = yield* Effect.forkChild(
              Effect.result(ownedPeer.rawMedia.video("main_video").pipe(Stream.runHead)),
            );
            yield* Effect.yieldNow;
            // The fixture's next frame names a track index that is not a video receiver.
            fault(1);
            yield* ownedPeer.rawMedia.snapshot;
            const current = yield* Fiber.join(reader);
            const future = yield* Effect.result(
              ownedPeer.rawMedia.audio("main_audio").pipe(Stream.runHead),
            );
            expect(current).toMatchObject({ _tag: "Failure", failure: { code: "Protocol" } });
            expect(future).toMatchObject({ _tag: "Failure", failure: { code: "Protocol" } });
            expect(errors).toHaveLength(1);
          }),
        ),
      );
    } finally {
      fault(0);
      if (peer !== undefined) await Effect.runPromise(peer.shutdown);
      rmSync(fixture.directory, { recursive: true, force: true });
    }
  });

  test("normal shutdown ends media readers and unknown tracks fail without creating feeds", async () => {
    if (process.platform === "win32") return;
    const fixture = compile();
    let peer: NativePeer | undefined;
    try {
      await checkNativeBridge(fixture.path);
      peer = new NativePeer(fixture.path);
      const ownedPeer = peer;
      await Effect.runPromise(
        Effect.scoped(
          Effect.gen(function* () {
            yield* Effect.addFinalizer(() => ownedPeer.shutdown.pipe(Effect.orDie));
            yield* ownedPeer.prepare([], tracks, () => {});
            for (const name of ["missing", "main_audio", "input_audio"]) {
              const result = yield* Effect.result(
                ownedPeer.rawMedia.video(name).pipe(Stream.runHead),
              );
              expect(result).toMatchObject({
                _tag: "Failure",
                failure: { code: "InvalidInput", context: { outcome: "not-submitted" } },
              });
            }
            const reader = yield* Effect.forkChild(
              ownedPeer.rawMedia.video("main_video").pipe(Stream.runCollect),
            );
            yield* Effect.yieldNow;
            yield* ownedPeer.shutdown;
            expect(yield* Fiber.join(reader)).toEqual([]);
            expect(yield* ownedPeer.rawMedia.audio("main_audio").pipe(Stream.runCollect)).toEqual(
              [],
            );
          }),
        ),
      );
    } finally {
      if (peer !== undefined) await Effect.runPromise(peer.shutdown);
      rmSync(fixture.directory, { recursive: true, force: true });
    }
  });

  test("classifies a failed connection on the fiber's Clock when statistics never return", async () => {
    if (process.platform === "win32") return;
    const fixture = compile();
    const stat: (which: number) => number = fixture.library.func(
      "int fixture_lifetime_stat(int which)",
    );
    const hold: (held: number) => void = fixture.library.func("void fixture_stats_hold(int held)");
    const failConnection: () => void = fixture.library.func("void fixture_connection_fail(void)");
    let peer: NativePeer | undefined;
    try {
      await checkNativeBridge(fixture.path);
      peer = new NativePeer(fixture.path);
      const ownedPeer = peer;
      const errors: ReactorError[] = [];
      const waited = await Effect.runPromise(
        Effect.scoped(
          Effect.gen(function* () {
            yield* ownedPeer.prepare([], tracks, (event) => {
              if (event.type === "error") errors.push(event.error);
            });
            hold(1);
            failConnection();
            yield* Effect.promise(() =>
              until(() => stat(10) === 1, "classification never read statistics"),
            );
            // The native call never returns, and no host timer ends the wait.
            yield* Effect.promise(() => new Promise((resolve) => setTimeout(resolve, 50)));
            expect(errors).toEqual([]);
            const started = performance.now();
            yield* TestClock.adjust("2 seconds");
            yield* Effect.promise(() => until(() => errors.length > 0, "no classification"));
            return performance.now() - started;
          }),
        ).pipe(Effect.provide(TestClock.layer())),
      );
      expect(waited).toBeLessThan(1_000);
      expect(errors).toHaveLength(1);
      expect(errors[0]).toMatchObject({
        code: "Disconnected",
        message: "peer state failed",
        context: {
          detail: { code: "Timeout", message: "native failure classification timed out" },
        },
      });
    } finally {
      // The abandoned statistics call still owns its lease; shutdown drains it.
      hold(0);
      if (peer !== undefined) await Effect.runPromise(peer.shutdown);
      rmSync(fixture.directory, { recursive: true, force: true });
    }
  });
});
