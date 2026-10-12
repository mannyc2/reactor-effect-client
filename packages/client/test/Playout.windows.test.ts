import { assert, describe, it } from "@effect/vitest";
import { Array, Clock, Duration, Effect, Stream } from "effect";
import * as H3 from "../src/H3.js";
import * as LocalSource from "../src/LocalSource.js";
import * as Playout from "../src/Playout.js";
import * as ReactorTest from "../src/ReactorTest.js";

const key = (name: string) => Playout.ItemKey.make(name);
const idle = H3.alignFrames(H3.h3ReferenceTurboRealtime, 5) / H3.h3ReferenceTurboRealtime.fps;
const parts = (...names: readonly [string, ...Array<string>]) =>
  Array.map(names, (name): Playout.GroupPart => ({
    key: key(name),
    request: { prompt: name, seconds: 5 },
  }));

const start = Effect.fnUntraced(function* (lane: Playout.LaneSpec) {
  yield* Effect.forkScoped(ReactorTest.flow("20 millis"));
  const playout = yield* Playout.make({
    open: LocalSource.open({
      build: (clip) =>
        clip.tag._tag === "Filler"
          ? Effect.succeed({ value: undefined })
          : Effect.as(Effect.sleep("300 millis"), { value: undefined }),
    }),
    lanes: [lane],
    filler: {
      runway: { floor: "15 seconds", target: "15 seconds" },
      clip: ({ index }) => ({ prompt: `idle ${index}`, seconds: idle }),
      lengths: { min: idle, max: idle },
    },
  });
  yield* playout.forecasts.pipe(
    Stream.mapEffect(() => playout.state),
    Stream.filter(
      (state) =>
        state.sessions.flatMap((session) => session.ready).filter((item) => item === "filler")
          .length >= 3,
    ),
    Stream.runHead,
  );
  return playout;
});

describe("firm windows beside timed items", () => {
  // Ready filler does not all air before a timed item held behind a playing group. Both
  // admission and the check before dispatch must place the firm insert behind the timed item.
  it.effect.each([30, 36])("airs an insert behind a waiting group with startBy %s s", (startBy) =>
    Effect.gen(function* () {
      const playout = yield* start({ name: "line", strict: true });
      const a = yield* playout.submitGroup({
        key: key("A"),
        lane: "line",
        parts: parts("a1", "a2", "a3"),
      });
      yield* a.started;
      const now = yield* Clock.currentTimeMillis;
      yield* playout.submitGroup({
        key: key("B"),
        lane: "line",
        parts: parts("b1"),
        start: { _tag: "At", time: now + 500, late: { _tag: "readyBy", by: "60 seconds" } },
      });
      const ready = (yield* playout.state).sessions.flatMap((session) => session.ready);
      assert.strictEqual(ready.filter((item) => item === "filler").length, 3);
      const n = yield* playout.edit([
        {
          _tag: "InsertGroup",
          insert: {
            key: key("N"),
            after: key("b1"),
            parts: parts("n1"),
            window: { startBy: Duration.seconds(startBy), firm: true },
          },
        },
      ]);
      const result = n.results[0];
      assert.strictEqual(result?._tag, "AddedGroup");
      if (result?._tag !== "AddedGroup") return;
      const started = yield* result.handle.started;
      assert.strictEqual(started._tag, "Started");
      if (started._tag === "Started") assert.approximately(started.at - now, 20_720, 100);
    }),
  );

  it.effect("admits a single insert behind a timed item outside a strict lane", () =>
    Effect.gen(function* () {
      const playout = yield* start({ name: "line" });
      const a = yield* playout.submit({
        key: key("a"),
        lane: "line",
        request: { prompt: "a", seconds: 15 },
      });
      yield* a.started;
      const now = yield* Clock.currentTimeMillis;
      yield* playout.submit({
        key: key("b"),
        lane: "line",
        request: { prompt: "b", seconds: 5 },
        start: { _tag: "At", time: now + 500, late: { _tag: "nextBoundary" } },
      });
      const n = yield* playout.insert({
        key: key("n"),
        after: key("b"),
        request: { prompt: "n", seconds: 5 },
        window: { startBy: "30 seconds", firm: true },
      });
      const started = yield* n.started;
      assert.strictEqual(started._tag, "Started");
      if (started._tag === "Started") assert.approximately(started.at - now, 20_280, 100);
    }),
  );

  it.effect.each([false, true])("admits an insert that inherits a future start, cut %s", (cut) =>
    Effect.gen(function* () {
      const playout = yield* start({ name: "line", strict: true, cut });
      const now = yield* Clock.currentTimeMillis;
      yield* playout.submitGroup({
        key: key("B"),
        lane: "line",
        parts: parts("b1"),
        start: { _tag: "At", time: now + 2_000, late: { _tag: "readyBy", by: "2 seconds" } },
      });
      yield* playout.asRun.pipe(
        Stream.filter((event) => event.key === key("b1") && event.status._tag === "Ready"),
        Stream.runHead,
      );
      const editAt = yield* Clock.currentTimeMillis;
      const n = yield* playout.insertGroup({
        key: key("N"),
        before: key("b1"),
        parts: [{ key: key("n1"), request: { prompt: "n1", seconds: 15 } }],
        window: { startBy: "10 seconds", firm: true },
      });
      const started = yield* n.started;
      assert.strictEqual(started._tag, "Started");
      if (started._tag === "Started") {
        assert.isAtLeast(started.at, now + 2_000);
        assert.isBelow(started.at - editAt, 10_000);
      }
    }),
  );
});
