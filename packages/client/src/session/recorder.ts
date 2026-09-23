import * as Stream from "effect/Stream";

/** A recorder's view of one track: each frame, and each run the host dropped, in place. */
export type Recorded<F> =
  | { readonly _tag: "Frame"; readonly frame: F }
  | {
      readonly _tag: "Lost";
      /** The sequence of the last frame received before the loss. */
      readonly after: bigint;
      /** How many frames the host dropped there. */
      readonly count: bigint;
    };

/**
 * Every frame a track stream delivers, with a `Lost` element wherever the host
 * dropped frames between two it delivered: their admission sequences are not
 * consecutive. The view starts at the first frame received, so frames before
 * a reader subscribed are not loss. A sequence that goes back marks a new
 * generation, such as an orchestration's next source, and starts a new run.
 * Read one track: each track numbers its own frames.
 */
export const recorder = <F extends { readonly sequence: bigint }, E, R>(
  frames: Stream.Stream<F, E, R>,
): Stream.Stream<Recorded<F>, E, R> =>
  frames.pipe(
    Stream.mapAccum(
      (): bigint | undefined => undefined,
      (last, frame): readonly [bigint, ReadonlyArray<Recorded<F>>] => {
        const received: Recorded<F> = { _tag: "Frame", frame };
        if (last === undefined || frame.sequence <= last || frame.sequence === last + 1n)
          return [frame.sequence, [received]];
        return [
          frame.sequence,
          [{ _tag: "Lost", after: last, count: frame.sequence - last - 1n }, received],
        ];
      },
    ),
  );
