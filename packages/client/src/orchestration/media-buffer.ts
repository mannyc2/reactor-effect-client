import * as Cause from "effect/Cause";
import * as Effect from "effect/Effect";
import * as Queue from "effect/Queue";
import * as Stream from "effect/Stream";
import { ReactorError } from "../errors.js";
import type { AudioFrame, VideoFrame } from "../session/media.js";

/** The output queue owns admission and accounting together; source readers cannot bypass its bounds. */
export const make = Effect.gen(function* () {
  const video = yield* Queue.unbounded<VideoFrame, ReactorError | Cause.Done>();
  const audio = yield* Queue.unbounded<AudioFrame, ReactorError | Cause.Done>();
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
          new ReactorError({ code: "Overflow", message: "Orchestration video receiver overflow" }),
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
          new ReactorError({ code: "Overflow", message: "Orchestration audio receiver overflow" }),
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
    video: Stream.fromEffectRepeat(
      Effect.uninterruptibleMask((restore) =>
        restore(Queue.take(video)).pipe(
          Effect.map((frame) => {
            queuedFrames--;
            queuedVideoBytes -= frame.data.byteLength + frame.metadata.byteLength;
            return frame;
          }),
        ),
      ),
    ),
    audio: Stream.fromEffectRepeat(
      Effect.uninterruptibleMask((restore) =>
        restore(Queue.take(audio)).pipe(
          Effect.map((frame) => {
            queuedSamples -= frame.samples.length;
            queuedAudioFrames--;
            return frame;
          }),
        ),
      ),
    ),
    pressure: () => ({
      queuedVideo: queuedFrames,
      queuedAudio: queuedAudioFrames,
      queuedBytes: queuedVideoBytes + queuedSamples * 2,
    }),
    forwarded: () => ({ queuedVideoFrames: queuedFrames, queuedAudioSamples: queuedSamples }),
    fail: (cause: ReactorError): void => {
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
