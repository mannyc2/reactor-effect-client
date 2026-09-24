/**
 * An encoder run that ends is replaced, never handed to a new viewer: a frame
 * of another format ends the run its viewers are on, and a viewer that joins
 * afterwards gets a new run, starting with a new initialization segment. The
 * media here is synthetic, so no orchestration is involved; ffmpeg is real.
 */
import { spawnSync } from "node:child_process";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, describe, it } from "@effect/vitest";
import { Effect, Layer, Result, Stream } from "effect";
import * as Orchestration from "reactor-effect-client/orchestration";
import type { VideoFrame } from "reactor-effect-native";
import { Broadcast } from "../src/Broadcast.ts";

/** Two seconds of 64x36 frames, then 48x48 frames: a canvas change. */
const frames = Stream.fromEffectRepeat(Effect.sleep("40 millis")).pipe(
  Stream.mapAccum(
    () => 0,
    (index): readonly [number, ReadonlyArray<VideoFrame>] => {
      const [width, height] = index < 48 ? [64, 36] : [48, 48];
      return [
        index + 1,
        [
          {
            _tag: "VideoFrame",
            track: "main_video",
            width,
            height,
            frameId: BigInt(index + 1),
            timestampMicros: 0n,
            sequence: BigInt(index),
            format: "BGRA",
            data: new Uint8Array(width * height * 4).fill(index % 200),
            metadata: new Uint8Array(0),
          },
        ],
      ];
    },
  ),
);

const SyntheticMedia = Layer.succeed(
  Orchestration.Media,
  Orchestration.Media.of({
    video: frames,
    audio: Stream.never,
    videoFramesPerSecond: 24,
    pressure: Effect.succeed({
      closed: false,
      queuedControl: 0,
      queuedVideo: 0,
      queuedAudio: 0,
      queuedBytes: 0,
      droppedVideo: 0n,
      droppedAudio: 0n,
      pendingRequests: 0,
      deliveredVideo: 0n,
      deliveredAudio: 0n,
      readerOverflows: 0n,
    }),
  }),
);

const boxType = (bytes: Uint8Array) => String.fromCharCode(...bytes.subarray(4, 8));

const hasFfmpeg = spawnSync("ffmpeg", ["-version"]).status === 0;

describe.skipIf(!hasFfmpeg)("Broadcast", () => {
  it.live("gives a viewer who reconnects after the format changed a new run", () =>
    Effect.gen(function* () {
      const broadcast = yield* Broadcast;
      const first = yield* broadcast.viewer.pipe(Stream.runCollect, Effect.result);
      assert.isTrue(Result.isFailure(first));
      const next = yield* broadcast.viewer.pipe(
        Stream.take(1),
        Stream.runCollect,
        Effect.timeout("10 seconds"),
      );
      assert.strictEqual(boxType(next[0] ?? new Uint8Array(8)), "ftyp");
    }).pipe(
      Effect.provide(Broadcast.layer.pipe(Layer.provide([SyntheticMedia, NodeServices.layer]))),
    ),
  );
});
