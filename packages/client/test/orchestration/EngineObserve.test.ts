/** A state paired with every later event, with no gap between them. */
import { expect, test } from "vitest";
import { Cause, Effect, Exit, Result, Stream } from "effect";
import { Observations } from "../../src/observation.js";
import { ClipId } from "../../src/orchestration/request.js";
import type { EngineEvent } from "../../src/orchestration/types.js";
import { renewalFixture } from "./RenewalFixture.js";
import { run } from "./SourceFixture.js";

const queued = (clipId: string): EngineEvent => ({
  _tag: "Queued",
  clipId: ClipId.make(clipId),
  durationSeconds: 5,
});

test("observeWith subscribes before reading, so an event emitted during the read is kept", () =>
  run(
    Effect.gen(function* () {
      const observations = new Observations<string>();
      let applied = 0;
      // The state read itself races an emission, as a live producer can.
      const state = Effect.sync(() => {
        observations.emit("during-read");
        return ++applied;
      });
      const { initial, events } = yield* observations.observeWith(state);
      observations.emit("after-read");
      observations.end();
      expect(initial).toBe(1);
      expect(yield* Stream.runCollect(events)).toEqual(["during-read", "after-read"]);

      // Reading first and subscribing afterwards loses the same event.
      const naive = new Observations<string>();
      yield* Effect.sync(() => naive.emit("during-read"));
      const later = naive.stream();
      naive.emit("after-read");
      naive.end();
      expect(yield* Stream.runCollect(later)).toEqual([]);
    }),
  ));

test("an engine observation pairs its state with later events and re-syncs after overflow", () =>
  run(
    Effect.gen(function* () {
      const { handle, sources } = yield* renewalFixture();
      const first = yield* handle.engine.observe({ capacity: 1 });
      expect(first.initial.availability).toBe((yield* handle.engine.state).availability);
      // Two events past a one-event bound fail this observer, loudly.
      yield* sources[0]!.emit(queued("a"));
      yield* sources[0]!.emit(queued("b"));
      const overflowed = yield* Effect.result(Stream.runCollect(first.events));
      expect(Result.isFailure(overflowed) && overflowed.failure.reason._tag).toBe("Overflow");
      // A fresh observation starts from the current state and keeps later events.
      const again = yield* handle.engine.observe();
      yield* sources[0]!.emit(queued("c"));
      const next = yield* again.events.pipe(Stream.take(1), Stream.runCollect);
      expect(next.map((event) => event._tag)).toEqual(["Queued"]);
    }),
  ));

for (const failure of ["defect", "interruption"] as const)
  test(`observation ${failure} reaches active and later subscribers with its original Cause`, () =>
    run(
      Effect.gen(function* () {
        const observations = new Observations<string>();
        const active = yield* observations.subscribe();
        const cause =
          failure === "defect"
            ? Cause.die(new Error("controlled observation defect"))
            : Cause.interrupt();
        observations.emit("before-failure");
        observations.failCause(cause);
        observations.failCause(Cause.die(new Error("later failure")));
        observations.end();
        observations.emit("after-failure");
        const seen: string[] = [];
        expect(
          yield* Effect.exit(
            Stream.runForEach(active, (value) =>
              Effect.sync(() => {
                seen.push(value);
              }),
            ),
          ),
        ).toEqual(Exit.failCause(cause));
        expect(seen).toEqual(["before-failure"]);
        const later = yield* observations.subscribe();
        expect(yield* Effect.exit(Stream.runCollect(later))).toEqual(Exit.failCause(cause));
      }),
    ));
