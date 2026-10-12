/** Browser PCM playback and a real audio track for recording or publishing. */
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Redacted from "effect/Redacted";
import * as Ref from "effect/Ref";
import * as Schema from "effect/Schema";
import type * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import { PlaybackError } from "reactor-effect-client/AudioPlayback";
import type * as AudioPlayback from "reactor-effect-client/AudioPlayback";
import type { AudioFrame } from "reactor-effect-client/Media";

export interface Playback extends AudioPlayback.Playback {
  /** Borrowed output; closing the playback scope stops it. Do not stop it yourself. */
  readonly track: MediaStreamTrack;
}

const failed = (message: string, detail: unknown) =>
  PlaybackError.make({ reason: { _tag: "Playback", message }, detail: Redacted.make(detail) });
const closed = PlaybackError.make({
  reason: { _tag: "Closed", message: "The playback scope is closed" },
});

/** PCM metadata and channel alignment are checked before Web Audio consumes them. */
const Format = Schema.Struct({
  sampleRate: Schema.Int.check(Schema.isGreaterThan(0)),
  channels: Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 32 })),
  length: Schema.Int.check(Schema.isGreaterThan(0)),
}).check(
  Schema.makeFilter((value) =>
    value.length % value.channels === 0 ? undefined : "PCM must contain complete sample frames",
  ),
);

interface Scheduled {
  readonly end: number;
  readonly ended: Deferred.Deferred<void>;
}
interface Queued {
  readonly end: number;
  readonly scheduled: ReadonlyArray<Scheduled>;
}

/**
 * Acquires one locally audible output and its capture track. Call play from a user
 * gesture when the browser requires it; resume fails after ten seconds. At most
 * one second of PCM is scheduled ahead, including when a provider emits a large
 * block. The bound concerns device scheduling, not the input stream's own buffers.
 */
export const make: Effect.Effect<Playback, PlaybackError, Scope.Scope> = Effect.gen(function* () {
  if (typeof AudioContext === "undefined")
    return yield* PlaybackError.make({
      reason: { _tag: "UnsupportedHost", message: "This host has no AudioContext" },
    });
  const context = yield* Effect.acquireRelease(
    Effect.try({
      try: () => new AudioContext(),
      catch: (cause) => failed("Opening audio failed", cause),
    }),
    (context) => Effect.promise(() => context.close()),
  );
  const output = yield* Effect.acquireRelease(
    Effect.try({
      try: () => context.createMediaStreamDestination(),
      catch: (cause) => failed("Creating the audio output track failed", cause),
    }),
    (output) =>
      Effect.sync(() => {
        for (const track of output.stream.getTracks()) track.stop();
        output.disconnect();
      }),
  );
  const analyser = yield* Effect.acquireRelease(
    Effect.try({
      try: () => context.createAnalyser(),
      catch: (cause) => failed("Creating the audio meter failed", cause),
    }),
    (analyser) => Effect.sync(() => analyser.disconnect()),
  );
  yield* Effect.try({
    try: () => {
      analyser.fftSize = 1024;
      analyser.connect(context.destination);
      analyser.connect(output);
    },
    catch: (cause) => failed("Connecting audio failed", cause),
  });
  const track = output.stream.getAudioTracks()[0];
  if (track === undefined) return yield* failed("The audio output has no track", undefined);
  const state = yield* Ref.make<"idle" | "playing" | "closed">("idle");
  const released = yield* Deferred.make<void>();
  // Web Audio owns these callbacks and nodes; finalizers silence them synchronously.
  const playing = new Set<AudioBufferSourceNode>();
  const stop = Effect.sync(() => {
    for (const source of playing) {
      source.onended = null;
      source.stop();
      source.disconnect();
    }
    playing.clear();
  });
  yield* Effect.addFinalizer(() =>
    Effect.andThen(Ref.set(state, "closed"), Deferred.succeed(released, undefined)).pipe(
      Effect.andThen(stop),
    ),
  );
  const window = new Float32Array(analyser.fftSize);
  const level = Effect.gen(function* () {
    if ((yield* Ref.get(state)) === "closed") return yield* closed;
    return yield* Effect.try({
      try: () => {
        analyser.getFloatTimeDomainData(window);
        let sum = 0;
        for (const sample of window) sum += sample * sample;
        return Math.min(1, Math.sqrt(sum / window.length));
      },
      catch: (cause) => failed("Reading the audio meter failed", cause),
    });
  });

  const schedule = Effect.fnUntraced(function* (
    block: AudioFrame,
    offset: number,
    length: number,
    at: number,
  ) {
    const ended = yield* Deferred.make<void>();
    return yield* Effect.try({
      try: () => {
        const buffer = context.createBuffer(block.channels, length, block.sampleRate);
        for (let channel = 0; channel < block.channels; channel++) {
          const data = buffer.getChannelData(channel);
          for (let index = 0; index < length; index++)
            data[index] = (block.samples[(offset + index) * block.channels + channel] ?? 0) / 32768;
        }
        const source = context.createBufferSource();
        source.buffer = buffer;
        source.connect(analyser);
        source.onended = () => {
          playing.delete(source);
          source.disconnect();
          // Completion enters Effect through Web Audio's callback boundary.
          Deferred.doneUnsafe(ended, Effect.void);
        };
        source.start(at);
        playing.add(source);
        return { end: at + buffer.duration, ended };
      },
      catch: (cause) => failed("Scheduling audio failed", cause),
    });
  });
  const append = Effect.fnUntraced(function* (
    queued: Queued,
    block: AudioFrame,
  ): Effect.fn.Return<Queued, PlaybackError> {
    yield* Schema.decodeEffect(Format)({
      sampleRate: block.sampleRate,
      channels: block.channels,
      length: block.samples.length,
    }).pipe(
      Effect.mapError(() =>
        PlaybackError.make({
          reason: {
            _tag: "InvalidAudio",
            message: "PCM needs a positive sample rate and complete frames of 1–32 channels",
          },
        }),
      ),
    );
    let end = queued.end;
    let scheduled = queued.scheduled;
    const frames = block.samples.length / block.channels;
    const slice = Math.max(1, Math.floor(block.sampleRate / 10));
    for (let offset = 0; offset < frames; offset += slice) {
      const length = Math.min(slice, frames - offset);
      const duration = length / block.sampleRate;
      // Wait before admitting the next slice, so even a multi-second input block is bounded.
      while (end + duration - context.currentTime > 1) {
        const first = scheduled[0];
        if (first === undefined) break;
        yield* Deferred.await(first.ended);
        scheduled = scheduled.slice(1);
      }
      const at =
        end > context.currentTime ? end : context.currentTime + Math.min(0.1, 1 - duration);
      const next = yield* schedule(block, offset, length, at);
      end = next.end;
      scheduled = [...scheduled.filter((item) => item.end > context.currentTime), next];
    }
    return { end, scheduled };
  });
  const reserve = Effect.gen(function* () {
    const previous = yield* Ref.modify(state, (value) => [
      value,
      value === "idle" ? "playing" : value,
    ]);
    if (previous === "closed") return yield* closed;
    if (previous === "playing")
      return yield* PlaybackError.make({
        reason: { _tag: "Busy", message: "An utterance is already playing" },
      });
  });
  const play = Effect.fnUntraced(function* <E, R>(frames: Stream.Stream<AudioFrame, E, R>) {
    yield* Effect.acquireUseRelease(
      reserve,
      () =>
        Effect.gen(function* () {
          yield* Effect.tryPromise({
            try: () => context.resume(),
            catch: (cause) => failed("Starting audio failed", cause),
          }).pipe(
            Effect.timeoutOrElse({
              duration: "10 seconds",
              orElse: () =>
                Effect.fail(
                  PlaybackError.make({
                    reason: { _tag: "Timeout", message: "Audio playback did not start in time" },
                  }),
                ),
            }),
          );
          const queued = yield* Stream.runFoldEffect(
            frames,
            (): Queued => ({ end: 0, scheduled: [] }),
            append,
          );
          const last = queued.scheduled.at(-1);
          if (last !== undefined) yield* Deferred.await(last.ended);
        }).pipe(Effect.raceFirst(Effect.andThen(Deferred.await(released), Effect.fail(closed)))),
      () =>
        stop.pipe(
          Effect.andThen(Ref.update(state, (value) => (value === "closed" ? value : "idle"))),
        ),
    );
  });
  return { play, level, track };
});
