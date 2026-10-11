/**
 * A persistent Live2D character, alive between utterances, its mouth following
 * what the speaker plays. The page owns the character's lifetime and its frame
 * loop; drawing it is the Live2D adapter's job, which the person running the
 * page builds from their own copy of the Cubism SDK for Web, because neither
 * the SDK nor any model can ship in this repository.
 */
import { Effect, Predicate, Schema, Stream } from "effect";
import type { Scope } from "effect";

/** One loaded model, drawn into the canvas it was loaded with. */
export interface Character {
  /**
   * Advances the model's own idle motion, blinking, breathing and physics by
   * `seconds`, with the mouth open by `mouth`, from 0 to 1.
   */
  readonly update: (seconds: number, mouth: number) => void;
  /** Draws the current pose, sized to the canvas as it is now laid out. */
  readonly draw: () => void;
  /** Releases the model, its textures and its renderer. */
  readonly release: () => void;
}

/** What `/live2d/adapter.js` exports; `live2d/adapter.ts` implements it. */
export interface Adapter {
  readonly load: (options: {
    readonly canvas: HTMLCanvasElement;
    /** Addresses of the Cubism Core script, the Framework's WebGL shaders and the model. */
    readonly core: string;
    readonly shaders: string;
    readonly model: string;
  }) => Promise<Character>;
}

export class AvatarError extends Schema.TaggedError<AvatarError>()("AvatarError", {
  message: Schema.String,
  cause: Schema.Defect(),
}) {}

/** The next animation frame's time, in milliseconds on the page's clock. */
const frame = Effect.callback<number>((resume) => {
  const id = requestAnimationFrame((time) => resume(Effect.succeed(time)));
  return Effect.sync(() => cancelAnimationFrame(id));
});

/** The seconds since the previous frame, once per animation frame, for as long as it is read. */
export const frames: Stream.Stream<number> = Stream.fromEffectRepeat(frame).pipe(
  Stream.mapAccum(
    (): number | undefined => undefined,
    (previous, now) => [now, previous === undefined ? [] : [(now - previous) / 1000]],
  ),
);

/**
 * Speech's output level is mostly under 0.25; this maps it onto the mouth's
 * range, which opens quickly and closes a little more slowly, as a mouth does.
 */
const gain = 4;
const opening = 30;
const closing = 12;

/** The mouth `seconds` after it was open by `mouth`, following `level`. */
const follow = (mouth: number, level: number, seconds: number): number => {
  const target = Math.min(1, level * gain);
  const rate = target > mouth ? opening : closing;
  return mouth + (target - mouth) * Math.min(1, rate * seconds);
};

/**
 * Loads the adapter and the model, then animates the character until
 * interrupted, which releases it. Silence closes the mouth, so the character
 * settles back to idle after every utterance, finished or interrupted.
 */
export const show = Effect.fnUntraced(function* (
  canvas: HTMLCanvasElement,
  model: string,
  level: Effect.Effect<number>,
): Effect.fn.Return<never, AvatarError, Scope.Scope> {
  const failed = (message: string) => (cause: unknown) => AvatarError.make({ message, cause });
  // A computed address keeps the bundler from resolving the adapter, which is built separately.
  const address = new URL("/live2d/adapter.js", window.location.href).href;
  const module = yield* Effect.tryPromise({
    try: (): Promise<unknown> => import(address),
    catch: failed("The Live2D adapter did not load"),
  });
  if (!Predicate.hasProperty(module, "load") || !Predicate.isFunction(module.load))
    return yield* AvatarError.make({
      message: "The Live2D adapter exports no load function",
      cause: undefined,
    });
  // The adapter is a separately built module; this is the one place its contract is assumed.
  // oxlint-disable-next-line typescript/no-unsafe-type-assertion
  const adapter = module as unknown as Adapter;
  const character = yield* Effect.acquireRelease(
    Effect.tryPromise({
      try: () =>
        adapter.load({
          canvas,
          core: "/live2d/core.js",
          shaders: "/live2d/shaders/",
          model,
        }),
      catch: failed("The Live2D model did not load"),
    }),
    (character) => Effect.sync(() => character.release()),
  );
  yield* frames.pipe(
    Stream.runFoldEffect(
      () => 0,
      (mouth, seconds) =>
        Effect.map(level, (level) => {
          const next = follow(mouth, level, seconds);
          character.update(seconds, next);
          character.draw();
          return next;
        }),
    ),
  );
  return yield* Effect.never;
});
