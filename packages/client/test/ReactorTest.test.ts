/** The simulated Reactor behind the real client, the H3 provider and a playout. */
import { assert, layer } from "@effect/vitest";
import { Effect, Fiber, Ref, Stream } from "effect";
import * as H3 from "../src/H3.js";
import { H3Source, Playout, ReactorTest } from "../src/index.js";
import type { VideoFrame } from "../src/Media.js";
import { connect, environment, mint } from "./fixtures/Simulated.js";

const frameMs = 1000 / 24;

/** Plays two 5 s clips back to back and returns each clip's frames in arrival order. */
const playTwo = Effect.gen(function* () {
  const session = yield* connect;
  const provider = yield* H3.make(session);
  const media = yield* session.decoded;
  const frames = yield* Ref.make<ReadonlyArray<VideoFrame>>([]);
  yield* media.video(H3.h3ReferenceTurboRealtime.tracks.video).pipe(
    Stream.runForEach((frame) => Ref.update(frames, (all) => [...all, frame])),
    Effect.forkScoped,
  );
  yield* provider.setAutoplay(true);
  const first = yield* provider.enqueue({ prompt: "first", seconds: 5 });
  const second = yield* provider.enqueue({ prompt: "second", seconds: 5 });
  yield* Effect.sleep("30 seconds");
  const played = (yield* Ref.get(frames)).flatMap((frame) => {
    const decoded = ReactorTest.frameOf(frame);
    return decoded === undefined ? [] : [{ ...decoded, at: Number(frame.timestampMicros) / 1000 }];
  });
  const of = (clipId: string) => played.filter((frame) => frame.clipId === clipId);
  return { first: of(first.clip.clip_id), second: of(second.clip.clip_id), frames };
});

layer(environment({ timing: ReactorTest.Timing.fixed({ buildSpeed: 2.4, seam: "70 millis" }) }))(
  "playback",
  (it) => {
    it.effect("plays each 5 s clip as 124 frames, its seam as timed, and nothing when idle", () =>
      Effect.gen(function* () {
        yield* Effect.forkScoped(ReactorTest.flow());
        const { first, second, frames } = yield* playTwo;
        assert.deepStrictEqual(
          first.map((frame) => frame.index),
          Array.from({ length: 124 }, (_, index) => index),
        );
        assert.strictEqual(second.length, 124);
        // The next clip starts the seam's 70 ms after the last one ends, one frame after its last frame.
        const seam = (second[0]?.at ?? 0) - (first[123]?.at ?? 0);
        assert.approximately(seam, frameMs + 70, 1);
        // Hosted H3 sends no frames while nothing plays.
        yield* Effect.sleep("10 seconds");
        assert.strictEqual((yield* Ref.get(frames)).length, 248);
      }),
    );
  },
);

layer(environment({ timing: ReactorTest.Timing.hosted }))("the hosted trace", (it) => {
  it.effect("plays back to back within the seams two paid runs measured", () =>
    Effect.gen(function* () {
      yield* Effect.forkScoped(ReactorTest.flow());
      const { first, second } = yield* playTwo;
      assert.strictEqual(first.length, 124);
      assert.strictEqual(second.length, 124);
      const seam = (second[0]?.at ?? 0) - (first[123]?.at ?? 0);
      assert.isTrue(seam >= frameMs + 30 && seam <= frameMs + 110, `seam ${seam} ms`);
    }),
  );
});

layer(environment({ timing: ReactorTest.Timing.fixed({ buildSpeed: 2.4 }) }))("billing", (it) => {
  it.effect("bills whole minutes from ready until close confirms termination", () =>
    Effect.gen(function* () {
      yield* Effect.forkScoped(ReactorTest.flow("50 millis"));
      const test = yield* ReactorTest.ReactorTest;
      const session = yield* connect;
      yield* Effect.sleep("61 seconds");
      const report = yield* session.close;
      assert.isTrue(report.remote.confirmed);
      assert.strictEqual(report.remote.evidence, "terminal");
      const billing = yield* test.billing;
      assert.strictEqual(billing.minutes, 2);
      assert.strictEqual(billing.usd, 1.5);
      assert.deepStrictEqual(
        (yield* test.sessions).map((info) => [info.state, info.deletes]),
        [["CLOSED", 1]],
      );
    }),
  );
});

layer(
  environment({
    timing: ReactorTest.Timing.fixed({ buildSpeed: 2.4 }),
    faults: [{ _tag: "RefuseAllocation", nth: 1 }],
  }),
)("allocation", (it) => {
  it.effect("a refused allocation fails acquisition and allocates nothing", () =>
    Effect.gen(function* () {
      yield* Effect.forkScoped(ReactorTest.flow());
      const test = yield* ReactorTest.ReactorTest;
      const failure = yield* Effect.flip(connect);
      assert.strictEqual(failure._tag, "AcquisitionFailure");
      assert.deepStrictEqual(
        failure.reason._tag === "Http" ? [failure.reason.status, failure.context.outcome] : [],
        [403, "replied"],
      );
      assert.deepStrictEqual(yield* test.sessions, []);
    }),
  );
});

layer(environment({ timing: ReactorTest.Timing.fixed({ buildSpeed: 2.4 }) }))("playout", (it) => {
  it.effect(
    "a playout opened with H3Source plays on the simulator and terminates what it opened",
    () =>
      Effect.gen(function* () {
        yield* Effect.forkScoped(ReactorTest.flow("10 millis"));
        const { cleanup, frames } = yield* Effect.scoped(
          Effect.gen(function* () {
            const playout = yield* Playout.make({
              open: H3Source.open({ mint }),
              lanes: [{ name: "line" }],
            });
            const video = yield* playout.video.pipe(
              Stream.take(124),
              Stream.runCollect,
              Effect.forkScoped,
            );
            const item = yield* playout.submit({
              key: Playout.ItemKey.make("one"),
              lane: "line",
              request: { prompt: "one", seconds: 5 },
            });
            yield* item.outcome;
            return { cleanup: playout.cleanup, frames: yield* Fiber.join(video) };
          }),
        );
        assert.strictEqual(frames.length, 124);
        const report = yield* cleanup;
        assert.deepStrictEqual(
          report.retained.map((entry) => entry.remote.confirmed),
          [true],
        );
      }),
  );
});
