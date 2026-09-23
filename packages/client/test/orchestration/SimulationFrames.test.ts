/** Simulated media hands its readers exactly owned buffers, whatever views a renderer passes. */
import { expect, test } from "vitest";
import * as Effect from "effect/Effect";
import * as Stream from "effect/Stream";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { assertExactFrames } from "reactor-effect-test-kit/frames";
import { ClipRequest } from "../../src/orchestration/request.js";
import type { AudioFrame, VideoFrame } from "../../src/session/media.js";
import * as Simulation from "../../src/simulation/index.js";

test("simulation media gives each frame its own exact buffer when the renderer passes views into one slab", ({
  signal,
}) =>
  Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        // Pooled or IPC-delivered bytes arrive as views into a larger buffer.
        const slab = new ArrayBuffer(4096);
        const video = (index: number): VideoFrame => ({
          _tag: "VideoFrame",
          format: "BGRA",
          track: "video",
          width: 2,
          height: 2,
          frameId: BigInt(index + 1),
          timestampMicros: 0n,
          data: new Uint8Array(slab, 64 + index * 16, 16).fill(index + 1),
          metadata: new Uint8Array(slab, 1024 + index * 4, 4),
        });
        const audio = (index: number): AudioFrame => ({
          _tag: "AudioFrame",
          track: "audio",
          sampleRate: 48_000,
          channels: 1,
          samples: new Int16Array(slab, 2048 + index * 64, 32).fill(index + 1),
        });
        const handle = yield* Simulation.make({
          fixedBuildTime: 0,
          buildRatio: 0,
          present: (_, __, sink) =>
            Effect.gen(function* () {
              for (let index = 0; index < 3; index++) {
                yield* sink.video(video(index));
                yield* sink.audio(audio(index));
              }
              return yield* Effect.never;
            }),
        });
        yield* handle.engine.enqueue(
          new ClipRequest({
            prompt: "A host speaks at a desk.",
            references: [],
            durationSeconds: 5,
            metadata: {},
          }),
        );
        const videos = yield* handle.media.video.pipe(Stream.take(3), Stream.runCollect);
        const audios = yield* handle.media.audio.pipe(Stream.take(3), Stream.runCollect);
        assertExactFrames(videos, (frame) => frame.data);
        assertExactFrames(videos, (frame) => frame.metadata);
        assertExactFrames(audios, (frame) => frame.samples);
        // The declared formats: four bytes per BGRA pixel, one sample per frame per channel.
        for (const frame of videos)
          expect(frame.data.byteLength).toBe(frame.width * frame.height * 4);
        expect(videos.map((frame) => frame.data[0])).toEqual([1, 2, 3]);
        expect(audios.map((frame) => [frame.samples.length, frame.samples[0]])).toEqual([
          [32, 1],
          [32, 2],
          [32, 3],
        ]);
        yield* handle.close;
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
    { signal },
  ));
