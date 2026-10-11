/**
 * A persistent Live2D character, alive between utterances, its mouth following
 * what the speaker plays. The page selects and loads its own Cubism adapter
 * and model. BrowserCharacter owns the reusable animation and lip-sync loop.
 */
import { Effect, Predicate, Redacted } from "effect";
import type { Scope } from "effect";
import * as BrowserCharacter from "reactor-effect-browser/BrowserCharacter";
import { CharacterError } from "reactor-effect-client/Character";
import type { Renderer } from "reactor-effect-client/Character";

/** One loaded model, drawn into the canvas it was loaded with. */
export interface Live2DModel {
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
  }) => Promise<Live2DModel>;
}

/**
 * Loads the adapter and the model, then animates the character until
 * interrupted, which releases it. Silence closes the mouth, so the character
 * settles back to idle after every utterance, finished or interrupted.
 */
export const show = Effect.fnUntraced(function* <E, R>(
  canvas: HTMLCanvasElement,
  model: string,
  level: Effect.Effect<number, E, R>,
): Effect.fn.Return<never, CharacterError | E, Scope.Scope | R> {
  const failed = (message: string) => (detail: unknown) =>
    CharacterError.make({ reason: { _tag: "Load", message }, detail: Redacted.make(detail) });
  // A computed address keeps the bundler from resolving the adapter, which is built separately.
  const address = new URL("/live2d/adapter.js", window.location.href).href;
  const module = yield* Effect.tryPromise({
    try: (): Promise<unknown> => import(address),
    catch: failed("The Live2D adapter did not load"),
  });
  if (!Predicate.hasProperty(module, "load") || !Predicate.isFunction(module.load))
    return yield* CharacterError.make({
      reason: { _tag: "Load", message: "The Live2D adapter exports no load function" },
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
  const renderer: Renderer = {
    render: ({ seconds, mouth }) =>
      Effect.try({
        try: () => {
          character.update(seconds, mouth);
          character.draw();
        },
        catch: (detail) =>
          CharacterError.make({
            reason: { _tag: "Render", message: "The Live2D model could not render a frame" },
            detail: Redacted.make(detail),
          }),
      }),
  };
  return yield* BrowserCharacter.animate(renderer, level);
});
