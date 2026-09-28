/** The in-process peer over the addon's queues, driven through the scripted fake addon. */
import { assert, describe, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Stream from "effect/Stream";
import * as TestClock from "effect/testing/TestClock";
import type { ReactorError } from "reactor-effect-client/ReactorError";
import { expect } from "vitest";
import { decoded, eventually, fakeAddon, nativePeer, revealed } from "./support.js";

const tracks = [
  { name: "main_video", kind: "video", direction: "recvonly" },
  { name: "main_audio", kind: "audio", direction: "recvonly" },
  { name: "input_audio", kind: "audio", direction: "sendonly" },
] as const;

describe("native peer over the addon's queues", () => {
  it.live("fails existing and future decoded-media readers with the source's failure", () =>
    Effect.gen(function* () {
      const addon = yield* fakeAddon;
      const peer = yield* nativePeer(addon.module);
      const media = decoded(peer);
      const errors: Array<ReactorError> = [];
      yield* peer.prepare([], tracks, (event) => {
        if (event.type === "error") errors.push(event.error);
      });
      const reader = yield* media
        .video("main_video")
        .pipe(Stream.runHead, Effect.result, Effect.forkChild);
      yield* Effect.yieldNow;
      // The next frame names a track index that is not a video receiver.
      addon.module.controls.fault = true;
      yield* media.pressure;
      const current = yield* Fiber.join(reader);
      const future = yield* Effect.result(media.audio("main_audio").pipe(Stream.runHead));
      expect(current).toMatchObject({ _tag: "Failure", failure: { reason: { _tag: "Protocol" } } });
      expect(future).toMatchObject({ _tag: "Failure", failure: { reason: { _tag: "Protocol" } } });
      assert.strictEqual(errors.length, 1);
    }),
  );

  it.live("ends media readers on shutdown, and refuses tracks it does not receive", () =>
    Effect.gen(function* () {
      const addon = yield* fakeAddon;
      const peer = yield* nativePeer(addon.module);
      const media = decoded(peer);
      yield* peer.prepare([], tracks, () => {});
      for (const name of ["missing", "main_audio", "input_audio"]) {
        const result = yield* Effect.result(media.video(name).pipe(Stream.runHead));
        expect(result).toMatchObject({
          _tag: "Failure",
          failure: { reason: { _tag: "InvalidInput" }, context: { outcome: "not-submitted" } },
        });
      }
      const reader = yield* Effect.forkChild(media.video("main_video").pipe(Stream.runCollect));
      yield* Effect.yieldNow;
      yield* peer.shutdown;
      assert.deepStrictEqual(yield* Fiber.join(reader), []);
      assert.deepStrictEqual(yield* media.audio("main_audio").pipe(Stream.runCollect), []);
    }),
  );

  it.effect(
    "classifies a failed connection on the fiber's Clock when statistics never return",
    () =>
      Effect.gen(function* () {
        const addon = yield* fakeAddon;
        const peer = yield* nativePeer(addon.module);
        const errors: Array<ReactorError> = [];
        yield* peer.prepare([], tracks, (event) => {
          if (event.type === "error") errors.push(event.error);
        });
        addon.hold("stats", true);
        // Release the held read before the scope's shutdown joins it.
        yield* Effect.addFinalizer(() => Effect.sync(() => addon.hold("stats", false)));
        addon.module.controls.peers[0]?.fail();
        yield* TestClock.withLive(addon.reached("stats"));
        // The call never answers, and no host timer ends the wait.
        yield* TestClock.withLive(Effect.sleep("50 millis"));
        assert.deepStrictEqual(errors, []);
        yield* TestClock.adjust("2 seconds");
        yield* TestClock.withLive(
          eventually(() => errors.length > 0, "no classification", "1 second"),
        );
        assert.strictEqual(errors.length, 1);
        expect(errors[0] === undefined ? undefined : revealed(errors[0])).toMatchObject({
          reason: { _tag: "Disconnected" },
          message: "peer state failed",
          context: {
            detail: {
              reason: { _tag: "Timeout" },
              message: "native failure classification timed out",
            },
          },
        });
      }),
  );
});
