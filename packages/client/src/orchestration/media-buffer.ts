import * as Cause from "effect/Cause";
import * as Effect from "effect/Effect";
import * as Queue from "effect/Queue";
import * as Stream from "effect/Stream";
import { ReactorError } from "../errors.js";
import type { ReactorFailure } from "../errors.js";
import type { AudioFrame, VideoFrame } from "../session/media.js";

/**
 * One reader at a time drains a queue. A second concurrent reader fails with
 * `AlreadyReading` instead of silently splitting the frames with the first; a
 * reader that ends releases the output, and a later one resumes from the
 * retained frames.
 */
const singleReader = <A, E>(
  kind: string,
  queue: Queue.Queue<A, E | Cause.Done>,
  taken: (value: A) => void,
): Stream.Stream<A, E | ReactorError> => {
  let reading = false;
  return Stream.unwrap(
    Effect.acquireRelease(
      Effect.suspend(() => {
        if (reading)
          return Effect.fail(
            ReactorError.fromCode(
              "AlreadyReading",
              `orchestration ${kind} already has an active reader`,
              { outcome: "not-submitted" },
            ),
          );
        reading = true;
        return Effect.void;
      }),
      () =>
        Effect.sync(() => {
          reading = false;
        }),
    ).pipe(
      Effect.as(
        Stream.fromEffectRepeat(
          Effect.uninterruptibleMask((restore) =>
            restore(Queue.take(queue)).pipe(
              Effect.map((value) => {
                taken(value);
                return value;
              }),
            ),
          ),
        ),
      ),
    ),
  );
};

/** The output queue owns admission and accounting together; source readers cannot bypass its bounds. */
export const make = Effect.gen(function* () {
  const video = yield* Queue.unbounded<VideoFrame, ReactorFailure | Cause.Done>();
  const audio = yield* Queue.unbounded<AudioFrame, ReactorFailure | Cause.Done>();
  let ended = false;
  let queuedFrames = 0;
  let queuedSamples = 0;
  let queuedAudioFrames = 0;
  let queuedVideoBytes = 0;

  const offerVideo = (frame: VideoFrame): Effect.Effect<void, ReactorError> =>
    Effect.suspend(() => {
      if (ended) return Effect.void;
      if (queuedFrames >= 96)
        return Effect.fail(
          ReactorError.fromCode("Overflow", "Orchestration video receiver overflow"),
        );
      // The private queues are nonblocking. Admission and counters change in one synchronous turn.
      if (Queue.offerUnsafe(video, frame)) {
        queuedFrames++;
        queuedVideoBytes += frame.data.byteLength + frame.metadata.byteLength;
      }
      return Effect.void;
    });
  const offerAudio = (frame: AudioFrame): Effect.Effect<void, ReactorError> =>
    Effect.suspend(() => {
      if (ended) return Effect.void;
      if (queuedSamples + frame.samples.length > 48_000 * 4)
        return Effect.fail(
          ReactorError.fromCode("Overflow", "Orchestration audio receiver overflow"),
        );
      if (Queue.offerUnsafe(audio, frame)) {
        queuedSamples += frame.samples.length;
        queuedAudioFrames++;
      }
      return Effect.void;
    });

  return {
    offerVideo,
    offerAudio,
    video: singleReader("video", video, (frame) => {
      queuedFrames--;
      queuedVideoBytes -= frame.data.byteLength + frame.metadata.byteLength;
    }),
    audio: singleReader("audio", audio, (frame) => {
      queuedSamples -= frame.samples.length;
      queuedAudioFrames--;
    }),
    pressure: () => ({
      queuedVideo: queuedFrames,
      queuedAudio: queuedAudioFrames,
      queuedBytes: queuedVideoBytes + queuedSamples * 2,
    }),
    forwarded: () => ({ queuedVideoFrames: queuedFrames, queuedAudioSamples: queuedSamples }),
    fail: (cause: ReactorFailure): void => {
      if (ended) return;
      ended = true;
      Queue.failCauseUnsafe(video, Cause.fail(cause));
      Queue.failCauseUnsafe(audio, Cause.fail(cause));
    },
    end: (): void => {
      if (ended) return;
      ended = true;
      Queue.endUnsafe(video);
      Queue.endUnsafe(audio);
    },
  };
});
