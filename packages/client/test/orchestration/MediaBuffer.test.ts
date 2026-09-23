import { expect, test } from "vitest";
import { Effect, Fiber, Result, Stream } from "effect";
import { ReactorError } from "../../src/errors.js";
import * as MediaBuffer from "../../src/orchestration/media-buffer.js";
import { audioFrame, gate, runClock, videoFrame } from "./SourceFixture.js";

test("cancelling a video reader retains unconsumed frames and their byte accounting", () =>
  runClock(
    Effect.gen(function* () {
      const buffer = yield* MediaBuffer.make;
      const received = yield* gate;
      yield* buffer.offerVideo(videoFrame(1));
      yield* buffer.offerVideo({ ...videoFrame(2), metadata: new Uint8Array([3, 4]) });
      const reader = yield* buffer.video.pipe(
        Stream.runForEach(() => received.release.pipe(Effect.andThen(Effect.never))),
        Effect.forkScoped,
      );
      yield* received.wait;
      yield* Fiber.interrupt(reader);
      expect(buffer.pressure()).toEqual({ queuedVideo: 1, queuedAudio: 0, queuedBytes: 6 });
      const remaining = yield* buffer.video.pipe(Stream.take(1), Stream.runCollect);
      expect(remaining.map((frame) => frame.data[0])).toEqual([2]);
      expect(buffer.pressure()).toEqual({ queuedVideo: 0, queuedAudio: 0, queuedBytes: 0 });
      buffer.end();
    }),
  ));

test("cancelling an audio reader preserves the next packet and exact sample counts", () =>
  runClock(
    Effect.gen(function* () {
      const buffer = yield* MediaBuffer.make;
      const received = yield* gate;
      yield* buffer.offerAudio(audioFrame(7, 1));
      yield* buffer.offerAudio(audioFrame(11, 2));
      const reader = yield* buffer.audio.pipe(
        Stream.runForEach(() => received.release.pipe(Effect.andThen(Effect.never))),
        Effect.forkScoped,
      );
      yield* received.wait;
      yield* Fiber.interrupt(reader);
      expect(buffer.forwarded()).toEqual({ queuedVideoFrames: 0, queuedAudioSamples: 11 });
      expect(buffer.pressure()).toEqual({ queuedVideo: 0, queuedAudio: 1, queuedBytes: 22 });
      const remaining = yield* buffer.audio.pipe(Stream.take(1), Stream.runCollect);
      expect([...remaining[0]!.samples]).toEqual(new Array<number>(11).fill(2));
      expect(buffer.forwarded().queuedAudioSamples).toBe(0);
      buffer.end();
    }),
  ));

test("queue admission refuses overflow without changing accounting or accepting late output", () =>
  runClock(
    Effect.gen(function* () {
      const buffer = yield* MediaBuffer.make;
      for (let index = 0; index < 96; index++) yield* buffer.offerVideo(videoFrame());
      yield* buffer.offerAudio(audioFrame(192_000));
      const before = buffer.pressure();
      for (const excess of [buffer.offerVideo(videoFrame()), buffer.offerAudio(audioFrame(1))]) {
        const result = yield* Effect.result(excess);
        expect(Result.isFailure(result) && result.failure.reason._tag).toBe("Overflow");
        expect(buffer.pressure()).toEqual(before);
      }
      const failure = ReactorError.fromCode("Overflow", "fixture queue is full");
      buffer.fail(failure);
      buffer.end();
      yield* buffer.offerVideo(videoFrame());
      yield* buffer.offerAudio(audioFrame(1));
      expect(buffer.pressure()).toEqual(before);
      const outcome = yield* Effect.result(buffer.video.pipe(Stream.runDrain));
      expect(Result.isFailure(outcome) && outcome.failure).toBe(failure);
    }),
  ));
