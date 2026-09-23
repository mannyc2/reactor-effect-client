/** The recorder view: every delivered frame, and each dropped run at its position. */
import { expect, test } from "vitest";
import { Effect, Stream } from "effect";
import { recorder } from "../src/session/recorder.js";
import type { Recorded } from "../src/session/recorder.js";

const frames = (...sequences: number[]) =>
  Stream.fromIterable(sequences.map((sequence) => ({ sequence: BigInt(sequence) })));
const view = (stream: Stream.Stream<{ readonly sequence: bigint }>) =>
  Effect.runPromise(Stream.runCollect(recorder(stream)));
const shape = (recorded: ReadonlyArray<Recorded<{ readonly sequence: bigint }>>) =>
  recorded.map((entry) =>
    entry._tag === "Frame"
      ? Number(entry.frame.sequence)
      : `lost ${entry.count} after ${entry.after}`,
  );

test("a gap in admission sequences becomes a Lost run at its position", async () => {
  expect(shape(await view(frames(0, 1, 4, 5, 7)))).toEqual([
    0,
    1,
    "lost 2 after 1",
    4,
    5,
    "lost 1 after 5",
    7,
  ]);
});

test("frames before the first one received are not loss, and a restart is a new run", async () => {
  expect(shape(await view(frames(40, 41, 0, 1, 3)))).toEqual([40, 41, 0, 1, "lost 1 after 1", 3]);
});
