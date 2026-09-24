import { Cause, Effect, Option, Queue, Schema, Stream } from "effect";
import { ChildProcess, ChildProcessSpawner } from "effect/unstable/process";
import { recorder } from "reactor-effect-client";
import type { AudioFrame, VideoFrame } from "reactor-effect-native";

export class RecordingError extends Schema.TaggedError<RecordingError>()("RecordingError", {
  message: Schema.String,
}) {}

/** What a recording wrote, and what it had to make up for. */
export interface Written {
  readonly frames: number;
  /** Video frames the host dropped, each filled with the frame before it. */
  readonly filledFrames: number;
  /** Audio blocks the host dropped, each filled with silence. */
  readonly filledBlocks: number;
}

/**
 * One track with its gaps made good. `recorder` marks every run of frames
 * the host dropped between two it delivered; each missing frame is filled
 * with the last one delivered, so the file keeps the source's timing.
 */
const fillGaps = <F extends { readonly sequence: bigint }, E>(
  frames: Stream.Stream<F, E>,
  fill: (last: F) => F,
  onFill: (count: number) => void,
) =>
  recorder(frames).pipe(
    Stream.mapAccum(
      (): F | undefined => undefined,
      (last, item): readonly [F | undefined, ReadonlyArray<F>] => {
        if (item._tag === "Frame") return [item.frame, [item.frame]];
        if (last === undefined) return [last, []];
        const count = Number(item.count);
        onFill(count);
        return [last, Array.from({ length: count }, () => fill(last))];
      },
    ),
  );

const silence = (block: AudioFrame): AudioFrame => ({
  ...block,
  samples: new Int16Array(block.samples.length),
});

/**
 * Writes a video track, and an audio track when there is one, to an MP4 file
 * through ffmpeg, and resolves when ffmpeg has finished the file. The tracks
 * must end for the file to finish; a track that fails ends the file there.
 *
 * Each track runs into a queue of its own, both readers started, and
 * subscribed, before this waits for either, so neither loses its opening.
 * The first frame and audio block then set the encoder's input formats. The
 * queues hold two seconds (48 frames, 200 blocks of 10 ms); an encoder slower
 * than real time holds up the readers, and a reader that falls behind its
 * host's bound ends the file.
 *
 * The pipes' writers run detached: Effect's Node spawner leaves the child's
 * stdin, and any input fd, with no error listener once its writer is
 * interrupted, so a write still buffered when the child exits would surface
 * as an uncaught EPIPE. A writer that is never interrupted ends its pipe
 * itself, and ffmpeg then exits on its own.
 */
export const toMp4 = Effect.fn("Recording.toMp4")(function* <E>(options: {
  readonly path: string;
  readonly video: Stream.Stream<VideoFrame, E>;
  readonly audio: Option.Option<Stream.Stream<AudioFrame, E>>;
}) {
  const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
  let frames = 0;
  let filledFrames = 0;
  let filledBlocks = 0;
  /** Starts reading a track into a queue; the reader has subscribed when this returns. */
  const read = Effect.fnUntraced(function* <F extends { readonly sequence: bigint }>(
    track: Stream.Stream<F, E>,
    capacity: number,
    fill: (last: F) => F,
    onFill: (count: number) => void,
  ) {
    const queue = yield* Queue.bounded<F, Cause.Done>(capacity);
    yield* fillGaps(track, fill, onFill).pipe(
      Stream.catch(() => Stream.empty),
      Stream.runIntoQueue(queue),
      Effect.forkScoped({ startImmediately: true }),
    );
    return queue;
  });
  /** A track's first element, or none when it ended empty, and all of it. */
  const opened = <F>(queue: Queue.Queue<F, Cause.Done>) =>
    Queue.take(queue).pipe(
      Effect.option,
      Effect.map(
        Option.map((first) => ({
          first,
          all: Stream.concat(Stream.succeed(first), Stream.fromQueue(queue)),
        })),
      ),
    );
  const videoQueue = yield* read(
    options.video,
    48,
    (last) => last,
    (count) => (filledFrames += count),
  );
  const audioQueue = yield* Option.match(options.audio, {
    onNone: () => Effect.succeedNone,
    onSome: (track) =>
      read(track, 200, silence, (count) => (filledBlocks += count)).pipe(Effect.asSome),
  });
  const video = yield* opened(videoQueue);
  if (Option.isNone(video)) return yield* new RecordingError({ message: "no video frame arrived" });
  const audio = Option.flatten(
    yield* Option.match(audioQueue, {
      onNone: () => Effect.succeedNone,
      onSome: (queue) => opened(queue).pipe(Effect.asSome),
    }),
  );
  const first = video.value.first;
  const encoder = yield* spawner.spawn(
    ChildProcess.make(
      "ffmpeg",
      [
        ...["-hide_banner", "-loglevel", "error", "-y"],
        ...["-f", "rawvideo", "-pix_fmt", first.format === "BGRA" ? "bgra" : "rgba"],
        ...["-s", `${first.width}x${first.height}`, "-r", "24", "-i", "pipe:0"],
        ...Option.match(audio, {
          onNone: () => [],
          onSome: ({ first: block }) => [
            ...["-f", "s16le", "-ar", String(block.sampleRate), "-ac", String(block.channels)],
            ...["-i", "pipe:3"],
          ],
        }),
        ...["-c:v", "libx264", "-preset", "veryfast", "-crf", "20", "-pix_fmt", "yuv420p"],
        ...["-c:a", "aac", "-b:a", "128k", "-movflags", "+faststart", options.path],
      ],
      {
        stderr: "inherit",
        additionalFds: Option.isSome(audio) ? { fd3: { type: "input" } } : {},
      },
    ),
  );
  yield* video.value.all.pipe(
    Stream.map((frame) => {
      frames++;
      return frame.data;
    }),
    Stream.run(encoder.stdin),
    Effect.ignore,
    Effect.forkDetach,
  );
  if (Option.isSome(audio))
    yield* audio.value.all.pipe(
      Stream.map((block) => new Uint8Array(block.samples.buffer)),
      Stream.run(encoder.getInputFd(3)),
      Effect.ignore,
      Effect.forkDetach,
    );
  const code = yield* encoder.exitCode;
  if (code !== ChildProcessSpawner.ExitCode(0))
    return yield* new RecordingError({ message: `ffmpeg exited with ${code}` });
  return { frames, filledFrames, filledBlocks } satisfies Written;
});
