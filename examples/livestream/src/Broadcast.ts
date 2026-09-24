import {
  Cause,
  Clock,
  Context,
  Deferred,
  Effect,
  Exit,
  Layer,
  Option,
  Queue,
  Ref,
  Schema,
  Scope,
  Stream,
  SynchronizedRef,
} from "effect";
import { ChildProcess, ChildProcessSpawner } from "effect/unstable/process";
import * as Orchestration from "reactor-effect-client/orchestration";
import type { AudioFrame, VideoFrame } from "reactor-effect-native";
import * as Fmp4 from "./Fmp4.ts";

/** The H3 profile's rate: 24 frames per second, and 48 kHz audio. */
const fps = 24;
const sampleRate = 48_000;
const samplesPerFrame = sampleRate / fps;

export class BroadcastError extends Schema.TaggedError<BroadcastError>()("BroadcastError", {
  message: Schema.String,
}) {}

/**
 * Mono PCM between the orchestration and the encoder: at most 200 ms, the
 * oldest dropped past that. A take shorter than what is held is padded with
 * silence, so the encoder always receives exactly one frame's worth.
 */
class Pcm {
  private readonly ring = new Int16Array(sampleRate / 5);
  private start = 0;
  private size = 0;

  push(frame: AudioFrame): void {
    if (frame.sampleRate !== sampleRate) return;
    const count = Math.floor(frame.samples.length / frame.channels);
    for (let index = 0; index < count; index++) {
      let sum = 0;
      for (let channel = 0; channel < frame.channels; channel++)
        sum += frame.samples[index * frame.channels + channel] ?? 0;
      if (this.size === this.ring.length) this.start = (this.start + 1) % this.ring.length;
      else this.size++;
      this.ring[(this.start + this.size - 1) % this.ring.length] = Math.round(sum / frame.channels);
    }
  }

  take(count: number): Uint8Array<ArrayBuffer> {
    const out = new Int16Array(count);
    const available = Math.min(count, this.size);
    for (let index = 0; index < available; index++)
      out[index] = this.ring[(this.start + index) % this.ring.length] ?? 0;
    this.start = (this.start + available) % this.ring.length;
    this.size -= available;
    return new Uint8Array(out.buffer);
  }

  /** Start 60 ms behind with silence, so arrival jitter does not become gaps. */
  reset(): void {
    this.ring.fill(0);
    this.start = 0;
    this.size = (sampleRate * 60) / 1000;
  }
}

/**
 * Raw frames in (BGRA or RGBA on stdin, 48 kHz mono PCM on fd 3), fragmented
 * MP4 out: H.264 with a keyframe every second, so each fragment is a place a
 * viewer can join, and AAC audio. Raw inputs are described, not probed:
 * probing reads seconds of one input before the other, and both are fed in
 * lockstep, so it would wait forever.
 */
const encoderArgs = (frame: VideoFrame): readonly string[] => [
  ...["-hide_banner", "-loglevel", "error"],
  ...["-f", "rawvideo", "-pix_fmt", frame.format === "BGRA" ? "bgra" : "rgba"],
  ...["-s", `${frame.width}x${frame.height}`, "-r", String(fps)],
  ...["-thread_queue_size", "64", "-nofind_stream_info", "-i", "pipe:0"],
  ...["-f", "s16le", "-ar", String(sampleRate), "-ac", "1"],
  ...["-thread_queue_size", "64", "-nofind_stream_info", "-i", "pipe:3"],
  ...["-c:v", "libx264", "-preset", "veryfast", "-tune", "zerolatency"],
  ...["-g", String(fps), "-pix_fmt", "yuv420p", "-c:a", "aac", "-b:a", "96k"],
  ...["-f", "mp4", "-movflags", "frag_keyframe+empty_moov+default_base_moof", "pipe:1"],
];

/**
 * A steady 24 ticks per second on the Effect clock. A late tick is followed at
 * once by the ones it missed, so the encoder's frame count keeps pace with
 * time; after a stall of more than a second the clock starts over instead.
 */
const ticks: Stream.Stream<void> = Stream.unwrap(
  Effect.gen(function* () {
    let origin = yield* Clock.currentTimeMillis;
    let count = 0;
    return Stream.fromEffectRepeat(
      Effect.gen(function* () {
        count++;
        const now = yield* Clock.currentTimeMillis;
        const due = origin + (count * 1000) / fps;
        if (now - due > 1000) origin = now - (count * 1000) / fps;
        else if (due > now) yield* Effect.sleep(due - now);
      }),
    );
  }),
);

export class Broadcast extends Context.Service<
  Broadcast,
  {
    /**
     * The channel as fragmented MP4 for one viewer: the initialization
     * segment, then every fragment from the next keyframe on. It fails when
     * its encoder run ends (the channel went off air, the frame format
     * changed, or ffmpeg failed); a viewer that reconnects joins a new run.
     */
    readonly viewer: Stream.Stream<Uint8Array, BroadcastError | Fmp4.Fmp4Error>;
    /** Fails once the channel is off air: its media ended with the orchestration's terminal failure. */
    readonly onAir: Effect.Effect<void, BroadcastError>;
  }
>()("reactor-effect-example-livestream/Broadcast") {
  /**
   * Fails when ffmpeg is not on PATH. The application builds it before the
   * orchestration, so a host without an encoder never opens a paid session.
   */
  static readonly preflight = Layer.effectDiscard(
    Effect.gen(function* () {
      const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
      yield* spawner
        .string(ChildProcess.make("ffmpeg", ["-hide_banner", "-version"]))
        .pipe(
          Effect.mapError(
            () => new BroadcastError({ message: "the broadcast needs ffmpeg on PATH" }),
          ),
        );
    }),
  );

  static readonly layer = Layer.effect(
    Broadcast,
    Effect.gen(function* () {
      const media = yield* Orchestration.Media;
      const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
      const latest = yield* Ref.make(Option.none<VideoFrame>());
      const firstFrame = yield* Deferred.make<void>();
      const offAir = yield* Deferred.make<never, BroadcastError>();
      const pcm = new Pcm();
      const ended = (kind: string) => (failure: { readonly message: string }) =>
        Effect.logWarning(`channel ${kind} ended`, { reason: failure.message }).pipe(
          Effect.andThen(
            Deferred.fail(offAir, new BroadcastError({ message: "the channel is off air" })),
          ),
        );

      // Consumption is mandatory: the orchestration fails with Overflow once
      // its output holds four seconds nobody took. So both outputs are drained
      // for the channel's whole life, watched or not, each by its one reader.
      yield* media.video.pipe(
        Stream.runForEach((frame) =>
          Ref.set(latest, Option.some(frame)).pipe(
            Effect.andThen(Deferred.succeed(firstFrame, undefined)),
          ),
        ),
        Effect.catch(ended("video")),
        Effect.forkScoped,
      );
      yield* media.audio.pipe(
        Stream.runForEach((frame) => Effect.sync(() => pcm.push(frame))),
        Effect.catch(ended("audio")),
        Effect.forkScoped,
      );

      // One encoder for all viewers. It samples the newest frame 24 times a
      // second (repeating it while the model is between clips) together with
      // a frame's worth of audio (silence when there is none), so ffmpeg sees
      // a steady stream whatever the source's timing.
      const encode = Stream.unwrap(
        Effect.gen(function* () {
          yield* Effect.raceFirst(Deferred.await(firstFrame), Deferred.await(offAir));
          const first = Option.getOrUndefined(yield* Ref.get(latest));
          if (first === undefined) return Stream.empty;
          const encoder = yield* spawner.spawn(
            ChildProcess.make("ffmpeg", encoderArgs(first), {
              stderr: "inherit",
              additionalFds: { fd3: { type: "input" } },
              // ffmpeg catches SIGTERM and, blocked reading a pipe, exits only
              // on the fourth; without forceKillAfter the spawner then waits
              // for it with no bound, hanging shutdown. A live encoder has
              // nothing to finish, so it is killed outright.
              killSignal: "SIGKILL",
            }),
          );
          const video = yield* Queue.bounded<Uint8Array, Cause.Done>(2);
          const audio = yield* Queue.bounded<Uint8Array, Cause.Done>(24);
          const failed = yield* Deferred.make<never, BroadcastError>();
          const fail = (message: string) =>
            Deferred.fail(failed, new BroadcastError({ message })).pipe(Effect.asVoid);
          // The writers are detached, not scoped. Effect's Node spawner leaves
          // the child's stdin, and any input fd, with no error listener once
          // its writer is interrupted, so a write still buffered when the
          // encoder is killed would surface as an uncaught EPIPE and end the
          // process. Instead the finalizer below
          // ends their queues before the kill: a writer then flushes and ends
          // its pipe, or fails on the EPIPE with its own listener in place.
          const write = (queue: Queue.Queue<Uint8Array, Cause.Done>, pipe: typeof encoder.stdin) =>
            Stream.fromQueue(queue).pipe(
              Stream.run(pipe),
              Effect.catch(() => fail("the encoder stopped reading")),
              Effect.forkDetach,
            );
          yield* write(video, encoder.stdin);
          yield* write(audio, encoder.getInputFd(3));
          yield* Effect.addFinalizer(() => Effect.all([Queue.end(video), Queue.end(audio)]));
          pcm.reset();
          const tick = Effect.gen(function* () {
            const frame = Option.getOrUndefined(yield* Ref.get(latest));
            if (frame === undefined) return;
            if (
              frame.width !== first.width ||
              frame.height !== first.height ||
              frame.format !== first.format
            )
              return yield* fail("the frame format changed");
            yield* Queue.offer(audio, pcm.take(samplesPerFrame));
            yield* Queue.offer(video, frame.data);
          });
          // Scoped, so it stops before the queues end.
          yield* ticks.pipe(
            Stream.runForEach(() => tick),
            Effect.forkScoped,
          );
          yield* Effect.logInfo("encoder started", { width: first.width, height: first.height });
          return encoder.stdout.pipe(
            Fmp4.segments,
            Stream.interruptWhen(Effect.raceFirst(Deferred.await(failed), Deferred.await(offAir))),
            Stream.ensuring(Effect.logInfo("encoder stopped")),
          );
        }),
      ).pipe(
        Stream.mapError((error) =>
          error._tag === "PlatformError"
            ? new BroadcastError({ message: "the encoder process failed" })
            : error,
        ),
      );

      // Viewers share one encoder run: started by the first viewer, stopped 30
      // seconds after the last one leaves. A viewer that falls 8 fragments
      // behind loses the oldest, and no viewer slows another or the encoder.
      // A shared stream does not replay its end to a viewer that joins after
      // it, so a run that has ended is replaced rather than handed out: the
      // viewer who reconnects after a failure or a format change starts the
      // next run, with a new initialization segment.
      interface Run {
        readonly segments: Stream.Stream<Fmp4.Segment, BroadcastError | Fmp4.Fmp4Error>;
        readonly ended: Deferred.Deferred<void>;
        readonly scope: Scope.Closeable;
      }
      const scope = yield* Effect.scope;
      const start = Effect.gen(function* () {
        const ended = yield* Deferred.make<void>();
        const owner = yield* Scope.fork(scope);
        const segments = yield* Stream.share(
          encode.pipe(Stream.ensuring(Deferred.succeed(ended, undefined))),
          { capacity: 8, strategy: "sliding", idleTimeToLive: "30 seconds" },
        ).pipe(Scope.provide(owner));
        return { segments, ended, scope: owner } satisfies Run;
      });
      const runs = yield* SynchronizedRef.make<Run>(yield* start);
      const current = SynchronizedRef.modifyEffect(runs, (run) =>
        Deferred.isDone(run.ended).pipe(
          Effect.flatMap((ended) =>
            ended
              ? Scope.close(run.scope, Exit.void).pipe(
                  Effect.andThen(start),
                  Effect.map((next) => [next.segments, next] as const),
                )
              : Effect.succeed([run.segments, run] as const),
          ),
        ),
      );
      const viewer = Stream.unwrap(current).pipe(
        Stream.mapAccum(
          () => true,
          (first, segment: Fmp4.Segment) =>
            [false, first ? [segment.init, segment.fragment] : [segment.fragment]] as const,
        ),
      );

      const onAir = Deferred.isDone(offAir).pipe(
        Effect.flatMap((done) => (done ? Deferred.await(offAir) : Effect.void)),
      );

      return Broadcast.of({ viewer, onAir });
    }),
  );
}
