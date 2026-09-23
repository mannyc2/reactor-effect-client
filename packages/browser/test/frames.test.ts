/** Copied browser samples are exactly owned buffers. Simulated media host; no browser is involved. */
import { expect, test } from "bun:test";
import * as Effect from "effect/Effect";
import * as Stream from "effect/Stream";
import { FakeTrack, withGlobals } from "reactor-effect-test-kit";
import { assertExactFrames } from "reactor-effect-test-kit/frames";
import { audioSamples, videoFrames } from "../src/_internal/media.js";

class Frame {
  readonly displayWidth = 4;
  readonly displayHeight = 2;
  readonly codedWidth = 4;
  readonly codedHeight = 2;
  readonly visibleRect = { x: 0, y: 0, width: 4, height: 2 };
  constructor(readonly timestamp: number) {}
  allocationSize(): number {
    return 4 * 2 * 4;
  }
  copyTo(destination: Uint8Array): Promise<void> {
    destination.fill(this.timestamp);
    return Promise.resolve();
  }
  close(): void {}
}

class Audio {
  readonly numberOfFrames = 8;
  readonly numberOfChannels = 2;
  readonly sampleRate = 48_000;
  constructor(readonly timestamp: number) {}
  copyTo(plane: Float32Array, options: { readonly planeIndex: number }): void {
    plane.fill(this.timestamp + options.planeIndex / 10);
  }
  close(): void {}
}

/** A track processor whose readable yields `samples` in order. */
const processor = (samples: readonly unknown[]) =>
  class {
    readonly readable = new ReadableStream({
      start(controller) {
        for (const sample of samples) controller.enqueue(sample);
      },
    });
  };

test("copied video frames are each the whole of their own RGBA buffer", () =>
  withGlobals(
    { VideoFrame: Frame, MediaStreamTrackProcessor: processor([1, 2, 3].map((t) => new Frame(t))) },
    async () => {
      const samples = await Effect.runPromise(
        videoFrames(new FakeTrack("video")).pipe(Stream.take(3), Stream.runCollect),
      );
      assertExactFrames(samples, (sample) => sample.data);
      // The declared format: four bytes per RGBA pixel.
      for (const sample of samples)
        expect(sample.data.byteLength).toBe(sample.width * sample.height * 4);
      expect(samples.map((sample) => sample.data[0])).toEqual([1, 2, 3]);
    },
  ));

test("copied audio planes are each the whole of their own buffer", () =>
  withGlobals(
    { AudioData: Audio, MediaStreamTrackProcessor: processor([1, 2, 3].map((t) => new Audio(t))) },
    async () => {
      const samples = await Effect.runPromise(
        audioSamples(new FakeTrack("audio")).pipe(Stream.take(3), Stream.runCollect),
      );
      const planes = samples.flatMap((sample) => sample.planes);
      assertExactFrames(planes, (plane) => plane);
      // The declared format: one f32 plane of `frames` samples per channel.
      expect(planes.map((plane) => plane.length)).toEqual([8, 8, 8, 8, 8, 8]);
      expect(planes.map((plane) => Math.round((plane[0] ?? 0) * 10))).toEqual([
        10, 11, 20, 21, 30, 31,
      ]);
    },
  ));
