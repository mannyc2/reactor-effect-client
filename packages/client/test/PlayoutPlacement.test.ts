/**
 * Placement must keep its predecessor even when the air ahead ends earlier than projected, and
 * start when it said with a clip ahead at a length not asked for before.
 */
import { assert, layer } from "@effect/vitest";
import { Deferred, Effect, Ref, Stream } from "effect";
import { H3Source, LocalSource, Playout, ReactorTest } from "../src/index.js";
import { environment, tokens } from "./fixtures/Simulated.js";

const key = (value: string) => Playout.ItemKey.make(value);
const simulated = environment({
  timing: ReactorTest.Timing.fixed({
    buildSpeed: 2.4,
    seam: "40 millis",
    http: "40 millis",
    channel: "20 millis",
  }),
});

// A covering clip can stop while its follower builds. H3's automatic start can beat removal,
// so the predecessor must still be kept through the actual command/message boundary.
layer(simulated)("Playout placement", (it) => {
  it.effect("fences a follower before a reorder whose reply is still pending at the boundary", () =>
    Effect.gen(function* () {
      yield* Effect.forkScoped(ReactorTest.flow("20 millis"));
      const source = yield* Deferred.make<Playout.Source>();
      const predecessorId = yield* Deferred.make<string>();
      const fenced = yield* Deferred.make<void>();
      const restored = yield* Deferred.make<void>();
      const moved = yield* Deferred.make<void>();
      const playout = yield* Playout.make({
        open: H3Source.open({ tokens: yield* tokens }).pipe(
          Effect.map((opened) => ({
            ...opened,
            events: opened.events.pipe(
              Stream.tap((event) =>
                event._tag === "Started" &&
                event.clip.tag?._tag === "Item" &&
                event.clip.tag.key === "predecessor"
                  ? Deferred.succeed(predecessorId, event.clip.clipId)
                  : Effect.void,
              ),
            ),
            setAutoplay: Effect.fnUntraced(function* (enabled: boolean) {
              yield* opened.setAutoplay(enabled);
              if (!enabled) yield* Deferred.succeed(fenced, undefined);
              else if (yield* Deferred.isDone(fenced)) yield* Deferred.succeed(restored, undefined);
            }),
            move: Effect.fnUntraced(function* (clipId: string, position: number) {
              yield* opened.move(clipId, position);
              yield* Deferred.succeed(moved, undefined);
              // The real queue changes now, but its successful reply reaches playout later.
              yield* Effect.sleep("10 seconds");
            }),
          })),
          Effect.tap((opened) => Deferred.succeed(source, opened)),
        ),
        lanes: [{ name: "urgent" }, { name: "line" }],
      });
      const predecessor = yield* playout.submit({
        key: key("predecessor"),
        lane: "line",
        request: { prompt: "predecessor", seconds: 15 },
      });
      yield* predecessor.started;
      const after = yield* playout.submit({
        key: key("after"),
        lane: "line",
        request: { prompt: "after", seconds: 5 },
        follows: { _tag: "Item", key: key("predecessor") },
      });
      yield* Deferred.await(restored);
      yield* playout.submit({
        key: key("urgent"),
        lane: "urgent",
        request: { prompt: "urgent", seconds: 5 },
      });
      yield* Deferred.await(moved);
      yield* (yield* Deferred.await(source)).stop(yield* Deferred.await(predecessorId));
      assert.deepStrictEqual(yield* after.outcome, { _tag: "Dropped", reason: "displaced" });
      assert.deepStrictEqual(yield* after.started, { _tag: "Dropped", reason: "displaced" });
    }),
  );

  it.effect("never airs a follower before its predecessor when the covering clip stops early", () =>
    Effect.gen(function* () {
      yield* Effect.forkScoped(ReactorTest.flow("20 millis"));
      const source = yield* Deferred.make<Playout.Source>();
      const playout = yield* Playout.make({
        open: H3Source.open({ tokens: yield* tokens }).pipe(
          Effect.tap((opened) => Deferred.succeed(source, opened)),
        ),
        lanes: [{ name: "line" }],
      });
      const starts = yield* Ref.make<ReadonlyArray<string>>([]);
      const building = yield* Deferred.make<void>();
      yield* playout.events.pipe(
        Stream.runForEach((event) => {
          if (event._tag !== "AsRun") return Effect.void;
          if (event.event.key === "after" && event.event.status._tag === "Building")
            return Deferred.succeed(building, undefined);
          return event.event.status._tag === "Started"
            ? Ref.update(starts, (all) => [...all, event.event.key])
            : Effect.void;
        }),
        Effect.forkScoped({ startImmediately: true }),
      );
      const submit = (name: string, seconds = 5) =>
        playout.submit({ key: key(name), lane: "line", request: { prompt: name, seconds } });
      const measured = yield* Effect.forEach(["a", "b", "c"], (name) => submit(name));
      for (const handle of measured) yield* handle.outcome;
      const cover = yield* submit("cover", 15);
      yield* cover.started;
      yield* playout.submit({
        key: key("predecessor"),
        lane: "line",
        request: { prompt: "predecessor", seconds: 5 },
        start: { _tag: "Manual" },
      });
      const after = yield* playout.submit({
        key: key("after"),
        lane: "line",
        request: { prompt: "after", seconds: 5 },
        start: { _tag: "Asap" },
        follows: { _tag: "Item", key: key("predecessor") },
      });
      yield* Deferred.await(building);
      const test = yield* ReactorTest.ReactorTest;
      const clipId = (yield* test.log).findLast(
        (entry) => entry.kind === "message" && entry.name === "clip_started",
      )?.clipId;
      if (clipId === undefined) return yield* Effect.die("the covering clip never started");
      yield* (yield* Deferred.await(source)).stop(clipId);
      yield* playout.release(key("predecessor"));
      assert.deepStrictEqual(yield* after.outcome, { _tag: "Dropped", reason: "displaced" });
      assert.notInclude(yield* Ref.get(starts), "after");
    }),
  );

  it.effect("drops a placed follower whose build misses the end of its predecessor", () =>
    Effect.gen(function* () {
      yield* Effect.forkScoped(ReactorTest.flow("20 millis"));
      const playout = yield* Playout.make({
        open: LocalSource.open({
          build: Effect.fnUntraced(function* (clip) {
            // The real renderer's next build is slower than the samples used by place.
            yield* Effect.sleep(
              clip.tag._tag === "Item" && clip.tag.key === "after" ? "15 seconds" : "500 millis",
            );
            return { value: undefined };
          }),
        }),
        lanes: [{ name: "line" }],
      });
      const submit = (name: string, seconds = 5) =>
        playout.submit({ key: key(name), lane: "line", request: { prompt: name, seconds } });
      const measured = yield* Effect.forEach(["a", "b", "c"], (name) => submit(name));
      for (const handle of measured) yield* handle.outcome;
      const predecessor = yield* submit("predecessor", 10);
      yield* predecessor.started;
      const placement = yield* playout.place({ key: key("after") });
      if (placement === null || placement.anchor === "next")
        return yield* Effect.die("the playing predecessor has no makeable boundary");
      assert.deepStrictEqual(placement.after, { _tag: "Item", key: key("predecessor") });
      const after = yield* playout.insert({
        key: key("after"),
        request: { prompt: "after", seconds: 5 },
        after: placement.anchor,
        follows: placement.after,
      });
      assert.deepStrictEqual(yield* after.outcome, { _tag: "Dropped", reason: "displaced" });
      assert.deepStrictEqual(yield* after.started, { _tag: "Dropped", reason: "displaced" });
    }),
  );

  // H3 builds a 7.3 s request as 192 frames, 8 s, a length no clip measured so far asked for.
  it.effect("place holds its startsAt with a clip ahead at a length not asked for before", () =>
    Effect.gen(function* () {
      yield* Effect.forkScoped(ReactorTest.flow("20 millis"));
      const playout = yield* Playout.make({
        open: H3Source.open({ tokens: yield* tokens }),
        lanes: [{ name: "line" }],
      });
      const building = yield* Deferred.make<void>();
      yield* playout.events.pipe(
        Stream.runForEach((event) =>
          event._tag === "AsRun" &&
          event.event.key === "ahead" &&
          event.event.status._tag === "Building"
            ? Deferred.succeed(building, undefined)
            : Effect.void,
        ),
        Effect.forkScoped({ startImmediately: true }),
      );
      const submit = (name: string, seconds = 5) =>
        playout.submit({ key: key(name), lane: "line", request: { prompt: name, seconds } });
      const measured = yield* Effect.forEach(["a", "b", "c"], (name) => submit(name));
      for (const handle of measured) yield* handle.outcome;
      const long = yield* submit("long", 15);
      yield* long.started;
      // About 5 s of the long clip is left: the clip ahead is built within it, and the probe,
      // built after it, could not be Ready before it ends.
      yield* Effect.sleep("10 seconds");
      yield* submit("ahead", 7.3);
      yield* Deferred.await(building);
      const placement = yield* playout.place({ key: key("probe") });
      if (placement === null || placement.anchor === "next")
        return yield* Effect.die("the clip ahead has no makeable boundary after it");
      assert.deepStrictEqual(placement.after, { _tag: "Item", key: key("ahead") });
      assert.strictEqual(placement.basis, "projected");
      const probe = yield* playout.insert({
        key: key("probe"),
        request: { prompt: "probe", seconds: 5 },
        after: placement.anchor,
        follows: placement.after,
      });
      const started = yield* probe.started;
      if (started._tag !== "Started") return yield* Effect.die(`the probe ${started._tag}`);
      const errorMs = started.at - placement.startsAt;
      assert.isAtMost(Math.abs(errorMs), 100, `started ${errorMs.toFixed(0)} ms off`);
    }),
  );
});
