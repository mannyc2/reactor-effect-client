/** Browser animation cadence, audio-driven mouth movement and canvas capture. */
import * as Effect from "effect/Effect";
import * as Predicate from "effect/Predicate";
import * as Redacted from "effect/Redacted";
import * as Schema from "effect/Schema";
import type * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import { CharacterError } from "reactor-effect-client/Character";
import type { Renderer } from "reactor-effect-client/Character";

/** The next animation frame's timestamp, on the browser's monotonic clock. */
const frame = Effect.callback<number, CharacterError>((resume) => {
  if (
    !Predicate.isFunction(globalThis.requestAnimationFrame) ||
    !Predicate.isFunction(globalThis.cancelAnimationFrame)
  ) {
    resume(
      Effect.fail(
        CharacterError.make({
          reason: { _tag: "UnsupportedHost", message: "Animation frames need a browser window" },
        }),
      ),
    );
    return;
  }
  try {
    const id = requestAnimationFrame((time) => resume(Effect.succeed(time)));
    return Effect.sync(() => cancelAnimationFrame(id));
  } catch (detail) {
    resume(
      Effect.fail(
        CharacterError.make({
          reason: { _tag: "Render", message: "The browser could not schedule an animation frame" },
          detail: Redacted.make(detail),
        }),
      ),
    );
  }
});

/**
 * Elapsed seconds between browser animation frames. Each consumer owns its
 * request; interruption cancels it. A background tab may suspend this cadence.
 */
export const frames: Stream.Stream<number, CharacterError> = Stream.fromEffectRepeat(frame).pipe(
  Stream.mapAccum(
    (): number | undefined => undefined,
    (previous, now) => [now, previous === undefined ? [] : [(now - previous) / 1000]],
  ),
);

const validLevel = Schema.is(Schema.Finite.check(Schema.isGreaterThanOrEqualTo(0)));

/**
 * Animates until interrupted or a renderer/level read fails. `level` reads the
 * output's current nonnegative RMS amplitude, including zero during silence.
 * Speech usually peaks below 0.25, so four times its level opens the mouth fully.
 * The mouth opens quickly and settles more slowly as output becomes silent.
 */
export const animate = Effect.fnUntraced(function* <E, R>(
  renderer: Renderer,
  level: Effect.Effect<number, E, R>,
): Effect.fn.Return<never, CharacterError | E, R> {
  yield* frames.pipe(
    Stream.runFoldEffect(
      () => 0,
      (mouth, seconds) =>
        Effect.gen(function* () {
          const amplitude = yield* level;
          if (!validLevel(amplitude))
            return yield* CharacterError.make({
              reason: {
                _tag: "InvalidInput",
                message: "The audio level must be finite and nonnegative",
              },
            });
          const target = Math.min(1, amplitude * 4);
          const rate = target > mouth ? 30 : 12;
          const next = mouth + (target - mouth) * Math.min(1, rate * seconds);
          yield* renderer.render({ seconds, mouth: next });
          return next;
        }),
    ),
  );
  return yield* Effect.never;
});

export interface CaptureOptions {
  /** Maximum capture frame rate, finite and positive; 30 frames per second by default. */
  readonly frameRate?: number | undefined;
}

const validFrameRate = Schema.is(Schema.Finite.check(Schema.isGreaterThan(0)));

/**
 * Captures the caller's canvas as a video track, stopped when the scope closes.
 * The caller owns drawing, mixing with audio, publishing and recording the track.
 * A captured track is not evidence that a recording or audience received frames.
 */
export const capture = Effect.fnUntraced(function* (
  canvas: HTMLCanvasElement,
  options: CaptureOptions = {},
): Effect.fn.Return<MediaStreamTrack, CharacterError, Scope.Scope> {
  const frameRate = options.frameRate ?? 30;
  if (!validFrameRate(frameRate))
    return yield* CharacterError.make({
      reason: {
        _tag: "InvalidInput",
        message: "The capture frame rate must be finite and positive",
      },
    });
  if (typeof canvas.captureStream !== "function")
    return yield* CharacterError.make({
      reason: { _tag: "UnsupportedHost", message: "This browser cannot capture a canvas" },
    });
  const stream = yield* Effect.acquireRelease(
    Effect.try({
      try: () => canvas.captureStream(frameRate),
      catch: (detail) =>
        CharacterError.make({
          reason: { _tag: "Capture", message: "The browser could not capture the canvas" },
          detail: Redacted.make(detail),
        }),
    }),
    (stream) => Effect.sync(() => stream.getTracks().forEach((track) => track.stop())),
  );
  const track = stream.getVideoTracks()[0];
  if (track?.readyState !== "live")
    return yield* CharacterError.make({
      reason: { _tag: "Capture", message: "The canvas did not produce a live video track" },
    });
  return track;
});
