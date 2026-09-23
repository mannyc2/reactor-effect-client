import { expect, test } from "vitest";
import { Effect, Fiber, Result, Stream } from "effect";
import { ReactorError } from "../../src/errors.js";
import type { ReactorFailure } from "../../src/errors.js";
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

/** Two retained elements; a second concurrent reader is refused, and a later one resumes. */
const exclusive = <A>(
  output: Stream.Stream<A, ReactorFailure>,
  offerTwo: Effect.Effect<void, ReactorError>,
) =>
  Effect.gen(function* () {
    yield* offerTwo;
    const received = yield* gate;
    const first = yield* output.pipe(
      Stream.runForEach(() => received.release.pipe(Effect.andThen(Effect.never))),
      Effect.forkScoped,
    );
    yield* received.wait;
    const second = yield* Effect.result(output.pipe(Stream.take(1), Stream.runCollect));
    expect(Result.isFailure(second) && second.failure.reason._tag).toBe("AlreadyReading");
    // The first reader's end releases the output to a later reader, which
    // resumes from the retained second element.
    yield* Fiber.interrupt(first);
    return yield* output.pipe(Stream.take(1), Stream.runCollect);
  });

test("a second concurrent reader fails with AlreadyReading instead of splitting the output", () =>
  runClock(
    Effect.gen(function* () {
      const buffer = yield* MediaBuffer.make;
      const video = yield* exclusive(
        buffer.video,
        Effect.andThen(buffer.offerVideo(videoFrame(1)), buffer.offerVideo(videoFrame(2))),
      );
      expect(video.map((frame) => frame.data[0])).toEqual([2]);
      const audio = yield* exclusive(
        buffer.audio,
        Effect.andThen(buffer.offerAudio(audioFrame(1, 1)), buffer.offerAudio(audioFrame(1, 2))),
      );
      expect(audio.map((frame) => frame.samples[0])).toEqual([2]);
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
