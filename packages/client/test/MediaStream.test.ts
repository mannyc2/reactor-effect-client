/** An owned readable stream releases its reader even when the host's cancel never settles. */
import { expect, test } from "vitest";
import { Effect, Fiber, Stream } from "effect";
import { TestClock } from "effect/testing";
import { ReactorError } from "../src/errors.js";
import { fromOwnedReadableStream } from "../src/media-stream.js";

test("a cancel that never settles holds the finalizer for its bound, then the lock is released", () =>
  Effect.runPromise(
    Effect.gen(function* () {
      let cancels = 0;
      const readable = new ReadableStream<number>({
        pull: (controller) => controller.enqueue(1),
        cancel: () => {
          cancels++;
          return new Promise<void>(() => {});
        },
      });
      const reading = yield* fromOwnedReadableStream({
        evaluate: () => readable,
        onError: (cause) =>
          ReactorError.fromCode("Protocol", "fixture read failed", { detail: cause }),
      }).pipe(Stream.take(1), Stream.runCollect, Effect.forkChild);
      // Let the reader take its element and reach the finalizer's cancel.
      while (cancels === 0) yield* Effect.yieldNow;
      expect(readable.locked).toBe(true);
      yield* TestClock.adjust("1 second");
      expect(yield* Fiber.join(reading)).toEqual([1]);
      expect(readable.locked).toBe(false);
    }).pipe(Effect.provide(TestClock.layer())),
  ));
