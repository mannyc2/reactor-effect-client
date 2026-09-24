/**
 * The recorder offline: synthetic frames with a gap where a host would have
 * dropped some, written through a real ffmpeg, counted back with ffprobe.
 */
import { spawnSync } from "node:child_process";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, describe, it } from "@effect/vitest";
import { Effect, FileSystem, Option, Path, Stream } from "effect";
import { ChildProcess, ChildProcessSpawner } from "effect/unstable/process";
import type { AudioFrame, VideoFrame } from "reactor-effect-native";
import { toMp4 } from "../src/Recording.ts";

const frame = (sequence: number): VideoFrame => ({
  _tag: "VideoFrame",
  track: "main_video",
  width: 64,
  height: 36,
  frameId: BigInt(sequence + 1),
  timestampMicros: 0n,
  sequence: BigInt(sequence),
  format: "BGRA",
  data: new Uint8Array(64 * 36 * 4).fill(sequence * 10),
  metadata: new Uint8Array(0),
});

const block = (sequence: number): AudioFrame => ({
  _tag: "AudioFrame",
  track: "main_audio",
  sampleRate: 48_000,
  channels: 1,
  sequence: BigInt(sequence),
  samples: new Int16Array(2000).fill(1000),
});

const hasFfmpeg = ["ffmpeg", "ffprobe"].every((tool) => spawnSync(tool, ["-version"]).status === 0);

describe.skipIf(!hasFfmpeg)("Recording", () => {
  it.live("fills the frames and audio a host dropped, so the file keeps its timing", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const out = path.join(yield* fs.makeTempDirectoryScoped(), "clip.mp4");
      // Frames 3 and 4 and audio block 2 never arrived.
      const written = yield* toMp4({
        path: out,
        video: Stream.fromIterable([0, 1, 2, 5, 6, 7].map(frame)),
        audio: Option.some(Stream.fromIterable([0, 1, 3, 4, 5, 6, 7].map(block))),
      });
      assert.deepStrictEqual(written, { frames: 8, filledFrames: 2, filledBlocks: 1 });
      const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
      const counted = yield* spawner.string(
        ChildProcess.make("ffprobe", [
          ...["-v", "error", "-count_frames", "-select_streams", "v:0"],
          ...["-show_entries", "stream=nb_read_frames", "-of", "csv=p=0", out],
        ]),
      );
      assert.strictEqual(counted.trim(), "8");
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );
});
