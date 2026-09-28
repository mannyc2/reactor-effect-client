/** The in-process peer over the addon's queues, driven through the scripted fake addon. */
import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, layer } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Stream from "effect/Stream";
import type { PeerEvent } from "reactor-effect-client/Peer";
import type { ReactorError } from "reactor-effect-client/ReactorError";
import { expect } from "vitest";
import { decoded, eventually, fakeAddon, nativePeer } from "./support.js";

const tracks = [
  { name: "main_video", kind: "video", direction: "recvonly" },
  { name: "main_audio", kind: "audio", direction: "recvonly" },
  { name: "input_audio", kind: "audio", direction: "sendonly" },
] as const;

layer(NodeServices.layer, { excludeTestServices: true })(
  "native peer over the addon's queues",
  (it) => {
    it.effect("fails existing and future decoded-media readers with the source's failure", () =>
      Effect.gen(function* () {
        const addon = yield* fakeAddon;
        const peer = yield* nativePeer({ addon: addon.module });
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
        expect(current).toMatchObject({
          _tag: "Failure",
          failure: { reason: { _tag: "Protocol" } },
        });
        expect(future).toMatchObject({
          _tag: "Failure",
          failure: { reason: { _tag: "Protocol" } },
        });
        assert.strictEqual(errors.length, 1);
      }),
    );

    it.effect("ends media readers on shutdown, and refuses tracks it does not receive", () =>
      Effect.gen(function* () {
        const addon = yield* fakeAddon;
        const peer = yield* nativePeer({ addon: addon.module });
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
      "reports a failed connection as its state and stays open for the session to read",
      () =>
        Effect.gen(function* () {
          const addon = yield* fakeAddon;
          const peer = yield* nativePeer({ addon: addon.module });
          const events: Array<PeerEvent> = [];
          yield* peer.prepare([], tracks, (event) => {
            events.push(event);
          });
          const [made] = addon.module.controls.peers;
          assert(made !== undefined, "the peer opened an addon peer");
          made.fail();
          yield* eventually({ condition: () => events.length > 0, message: "no event" });
          assert.deepStrictEqual(events, [{ type: "state", state: "failed" }]);
          assert.notInclude(yield* addon.calls, "stats");
          assert.deepStrictEqual(yield* peer.stats, []);
        }),
    );
  },
);
