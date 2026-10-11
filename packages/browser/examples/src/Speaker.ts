/**
 * Plays decoded PCM on the page's Web Audio clock, and measures what the
 * output is playing, so whatever follows the voice follows what is heard
 * rather than when blocks arrived.
 */
import { Deferred, Effect, Stream } from "effect";
import type { Scope } from "effect";
import type { AudioFrame } from "reactor-effect-client/Media";

/** Seconds of audio scheduled ahead of the output before the stream must wait. */
const lead = 1;

/** Seconds of room for blocks arriving on a busy event loop, at the start and after a gap. */
const headroom = 0.1;

export interface Speaker {
  /**
   * Plays one utterance's blocks back to back as they arrive and succeeds when
   * the output has played the last one. It schedules at most a second ahead,
   * so a stream generated faster than real time waits rather than buffering.
   * One that arrives slower plays with gaps. Interrupting it, or the stream
   * failing, silences everything it scheduled at once.
   */
  readonly play: <E>(frames: Stream.Stream<AudioFrame, E>) => Effect.Effect<void, E>;
  /** The root-mean-square level of what the output is playing now, from 0 to 1. */
  readonly level: Effect.Effect<number>;
}

interface Scheduled {
  /** When the block finishes, on the audio context's clock. */
  readonly end: number;
  readonly ended: Deferred.Deferred<void>;
}

interface Queued {
  /** When the last block scheduled so far finishes. */
  readonly end: number;
  /** Blocks the output may not have finished yet, oldest first. */
  readonly scheduled: ReadonlyArray<Scheduled>;
}

/** A speaker for the scope's lifetime: closing the scope closes its audio context. */
export const make: Effect.Effect<Speaker, never, Scope.Scope> = Effect.gen(function* () {
  const context = yield* Effect.acquireRelease(
    Effect.sync(() => new AudioContext()),
    (context) => Effect.promise(() => context.close()),
  );
  const analyser = context.createAnalyser();
  analyser.fftSize = 1024;
  analyser.connect(context.destination);
  const window = new Float32Array(analyser.fftSize);

  const level = Effect.sync(() => {
    analyser.getFloatTimeDomainData(window);
    let sum = 0;
    for (const sample of window) sum += sample * sample;
    return Math.sqrt(sum / window.length);
  });

  /**
   * Starts one block at `at`. The node leaves `playing`, the utterance's set
   * of nodes not yet heard, when the output has played it.
   */
  const schedule = Effect.fnUntraced(function* (
    block: AudioFrame,
    at: number,
    playing: Set<AudioBufferSourceNode>,
  ) {
    const ended = yield* Deferred.make<void>();
    const length = block.samples.length / block.channels;
    const buffer = context.createBuffer(block.channels, length, block.sampleRate);
    for (let channel = 0; channel < block.channels; channel++) {
      const samples = buffer.getChannelData(channel);
      for (let index = 0; index < length; index++)
        samples[index] = (block.samples[index * block.channels + channel] ?? 0) / 32768;
    }
    const source = context.createBufferSource();
    source.buffer = buffer;
    source.connect(analyser);
    // The DOM calls back from outside any fiber.
    source.onended = () => {
      playing.delete(source);
      source.disconnect();
      Deferred.doneUnsafe(ended, Effect.void);
    };
    playing.add(source);
    source.start(at);
    return { end: at + buffer.duration, ended } satisfies Scheduled;
  });

  /** Waits until no more than `lead` seconds are scheduled past the output. */
  const pace = (
    end: number,
    scheduled: ReadonlyArray<Scheduled>,
  ): Effect.Effect<ReadonlyArray<Scheduled>> =>
    Effect.suspend(() => {
      const first = scheduled.at(0);
      if (first === undefined || end - context.currentTime <= lead)
        return Effect.succeed(scheduled.filter((block) => block.end > context.currentTime));
      return Effect.andThen(Deferred.await(first.ended), pace(end, scheduled.slice(1)));
    });

  const play = <E>(frames: Stream.Stream<AudioFrame, E>): Effect.Effect<void, E> =>
    Effect.scoped(
      Effect.gen(function* () {
        // A page may only start sound from a person's gesture; the caller runs this from one.
        yield* Effect.promise(() => context.resume());
        const playing = new Set<AudioBufferSourceNode>();
        yield* Effect.addFinalizer(() =>
          Effect.sync(() => {
            for (const source of playing) {
              source.onended = null;
              source.stop();
              source.disconnect();
            }
            playing.clear();
          }),
        );
        const { scheduled } = yield* frames.pipe(
          Stream.runFoldEffect(
            (): Queued => ({ end: 0, scheduled: [] }),
            (queued, block) =>
              Effect.gen(function* () {
                const waiting = yield* pace(queued.end, queued.scheduled);
                const at =
                  queued.end > context.currentTime ? queued.end : context.currentTime + headroom;
                const next = yield* schedule(block, at, playing);
                return { end: next.end, scheduled: [...waiting, next] } satisfies Queued;
              }),
          ),
        );
        const last = scheduled.at(-1);
        if (last !== undefined) yield* Deferred.await(last.ended);
      }),
    );

  return { play, level };
});
