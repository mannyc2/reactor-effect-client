/** The recorder view: every delivered frame, and each dropped run at its position. */
import { assert, it } from "@effect/vitest";
import { Effect, Stream } from "effect";
import { recorder } from "../src/Media.js";

const view = (...sequences: ReadonlyArray<number>) =>
  Stream.fromIterable(sequences.map((sequence) => ({ sequence: BigInt(sequence) }))).pipe(
    recorder,
    Stream.map((entry) =>
      entry._tag === "Frame"
        ? Number(entry.frame.sequence)
        : `lost ${String(entry.count)} after ${String(entry.after)}`,
    ),
    Stream.runCollect,
  );

it.effect("a gap in admission sequences becomes a Lost run at its position", () =>
  Effect.gen(function* () {
    assert.deepStrictEqual(yield* view(0, 1, 4, 5, 7), [
      0,
      1,
      "lost 2 after 1",
      4,
      5,
      "lost 1 after 5",
      7,
    ]);
  }),
);

it.effect("frames before the first received are not loss, and a restart is a new run", () =>
  Effect.gen(function* () {
    assert.deepStrictEqual(yield* view(40, 41, 0, 1, 3), [40, 41, 0, 1, "lost 1 after 1", 3]);
  }),
);
