import { assert, describe, it } from "@effect/vitest";
import { Array, Clock, Deferred, Effect, Result, Stream } from "effect";
import { H3Source, LocalSource, Playout, ReactorTest } from "../src/index.js";

const key = (name: string) => Playout.ItemKey.make(name);
const parts = (...names: readonly [string, ...Array<string>]) =>
  Array.map(names, (name) => ({ key: key(name), request: { prompt: name, seconds: 5 } }));

// Filler builds immediately while speech takes 300 ms. Planning must account for the cut,
// including an item held to a time and an insert behind a playing member of its strict lane.
const start = Effect.fnUntraced(function* (
  cut: boolean,
  strict: boolean = true,
  buildMs: number = 300,
) {
  yield* Effect.forkScoped(ReactorTest.flow("20 millis"));
  const fillerStarted = yield* Deferred.make<void>();
  const ready = yield* Deferred.make<void>();
  const idle = H3Source.model.builtSeconds(5);
  const playout = yield* Playout.make({
    model: H3Source.model,
    evidence: LocalSource.evidence,
    open: LocalSource.open({
      model: H3Source.model,
      build: (clip) =>
        clip.tag._tag === "Filler"
          ? Effect.succeed({ value: undefined })
          : Effect.as(Effect.sleep(buildMs), { value: undefined }),
    }).pipe(
      Effect.map((source) => ({
        ...source,
        events: source.events.pipe(
          Stream.tap((event) =>
            event._tag === "Started" &&
            event.clip.tag?._tag === "Filler" &&
            event.clip.tag.index === 2
              ? Deferred.succeed(fillerStarted, undefined)
              : Effect.void,
          ),
        ),
      })),
    ),
    lanes: [{ name: "line", strict, cut }],
    filler: {
      runway: { floor: "15 seconds", target: "15 seconds" },
      clip: ({ index }) => ({ prompt: `idle ${String(index)}`, seconds: idle }),
      lengths: { min: idle, max: idle },
    },
  });
  yield* playout.events.pipe(
    Stream.runForEach((event) =>
      event._tag === "AsRun" && event.event.status._tag === "Ready"
        ? Deferred.succeed(ready, undefined)
        : Effect.void,
    ),
    Effect.forkScoped({ startImmediately: true }),
  );
  yield* Deferred.await(fillerStarted);
  // Enter the middle of the clip, with over four seconds still on air.
  yield* Effect.sleep("1 second");
  return { playout, ready };
});

it.effect("forecasts a cut whose held time falls inside the next filler seam", () =>
  Effect.gen(function* () {
    const { playout, ready } = yield* start(true);
    const end = (yield* playout.forecast).clips[0]?.endsAt;
    if (end === undefined) return yield* Effect.die("no filler on air");
    const item = yield* playout.submit({
      key: key("seam"),
      lane: "line",
      request: { prompt: "seam", seconds: 5 },
      start: { _tag: "At", time: end + 20, late: { _tag: "readyBy", by: "2 seconds" } },
    });
    yield* Deferred.await(ready);
    const placed = Playout.forecastFor(yield* playout.forecast, key("seam"));
    assert.strictEqual(placed._tag, "Airs");
    if (placed._tag !== "Airs") return;
    const started = yield* item.started;
    assert.strictEqual(started._tag, "Started");
    if (started._tag === "Started")
      assert.isAtMost(Math.abs(placed.clip.startsAt - started.at), 100);
  }),
);

describe.each([true, false])("cutting lane planning, cut %s", (cut) => {
  it.effect("forecasts a Ready item held until a time inside a filler clip", () =>
    Effect.gen(function* () {
      const { playout, ready } = yield* start(cut);
      const now = yield* Clock.currentTimeMillis;
      const group = yield* playout.submitGroup({
        key: key("B"),
        lane: "line",
        parts: parts("b1"),
        start: { _tag: "At", time: now + 2000, late: { _tag: "readyBy", by: "2 seconds" } },
      });
      yield* Deferred.await(ready);
      const forecast = yield* playout.forecast;
      const placed = Playout.forecastFor(forecast, key("b1"));
      assert.strictEqual(placed._tag, "Airs");
      if (placed._tag !== "Airs") return;
      const started = yield* group.started;
      assert.strictEqual(started._tag, "Started");
      if (started._tag !== "Started") return;
      assert.isAtMost(Math.abs(placed.clip.startsAt - started.at), 100);
      const previous = forecast.clips[0];
      assert.isAtMost(previous?.endsAt ?? Infinity, placed.clip.startsAt);
    }),
  );

  it.effect("places a submission while filler is playing", () =>
    Effect.gen(function* () {
      const { playout } = yield* start(cut, false);
      const placement = yield* playout.place({ key: key("n1"), seconds: 5 });
      assert.isNotNull(placement);
      assert.strictEqual(placement.anchor, "next");
      const item = yield* playout.submit({
        key: key("n1"),
        lane: "line",
        request: { prompt: "n1", seconds: 5 },
        start: { _tag: "Asap" },
        follows: cut ? undefined : placement.after,
      });
      const started = yield* item.started;
      assert.strictEqual(started._tag, "Started");
      if (started._tag !== "Started") return;
      assert.isAtMost(Math.abs(placement.startsAt - started.at), 500);
    }),
  );

  it.effect("admits a firm item only when it can cut the filler before its deadline", () =>
    Effect.gen(function* () {
      const { playout } = yield* start(cut);
      const now = yield* Clock.currentTimeMillis;
      const result = yield* playout
        .submitGroup({
          key: key("N"),
          lane: "line",
          parts: parts("n1"),
          window: { startBy: "2 seconds", firm: true },
        })
        .pipe(Effect.result);
      if (!cut) {
        assert.isTrue(Result.isFailure(result));
        if (Result.isFailure(result)) assert.strictEqual(result.failure._tag, "WouldMissDeadline");
        return;
      }
      assert.isTrue(Result.isSuccess(result));
      if (Result.isFailure(result)) return;
      const started = yield* result.success.started;
      assert.strictEqual(started._tag, "Started");
      if (started._tag === "Started") assert.isBelow(started.at - now, 2000);
    }),
  );

  it.effect("admits a firm group inserted before a held item behind a playing group", () =>
    Effect.gen(function* () {
      const { playout } = yield* start(cut);
      const now = yield* Clock.currentTimeMillis;
      const a = yield* playout.submitGroup({
        key: key("A"),
        lane: "line",
        parts: parts("a1", "a2"),
      });
      yield* playout.submitGroup({
        key: key("B"),
        lane: "line",
        parts: parts("b1", "b2"),
        start: { _tag: "At", time: now + 3000, late: { _tag: "readyBy", by: "20 seconds" } },
      });
      yield* a.started;
      const from = yield* Clock.currentTimeMillis;
      const inserted = yield* playout.insertGroup({
        key: key("N"),
        before: key("b1"),
        parts: parts("n1"),
        window: { startBy: "15 seconds", firm: true },
      });
      const started = yield* inserted.started;
      assert.strictEqual(started._tag, "Started");
      if (started._tag !== "Started") return;
      assert.isBelow(started.at - from, 15_000);
      const a2 = yield* a.parts[1]?.started ?? Effect.die("no second group part");
      assert.strictEqual(a2._tag, "Started");
      if (a2._tag === "Started") assert.isAtLeast(started.at - a2.at, 5000);
    }),
  );

  it.effect("keeps late-drop behavior when the build finishes in the next filler clip", () =>
    Effect.gen(function* () {
      const { playout } = yield* start(cut, true, 3000);
      // Leave two seconds of filler: the three-second build finishes in the next clip.
      yield* Effect.sleep(H3Source.model.builtSeconds(5) * 1000 - 3000);
      const now = yield* Clock.currentTimeMillis;
      const group = yield* playout.submitGroup({
        key: key("N"),
        lane: "line",
        parts: parts("n1"),
        window: { startBy: "5 seconds", firm: true },
      });
      const started = yield* group.parts[0].started;
      if (!cut) assert.deepStrictEqual(started, { _tag: "Dropped", reason: "late" });
      else {
        assert.strictEqual(started._tag, "Started");
        if (started._tag === "Started") assert.isBelow(started.at - now, 5000);
      }
    }),
  );
});
