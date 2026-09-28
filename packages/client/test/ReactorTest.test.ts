/** The simulated Reactor behind the real client, the H3 provider and a playout. */
import { assert, layer } from "@effect/vitest";
import { Duration, Effect, Fiber, Ref, Stream } from "effect";
import * as H3 from "../src/H3.js";
import { Coordinator, H3Source, Playout, ReactorTest } from "../src/index.js";
import type { VideoFrame } from "../src/Media.js";
import { connect, environment, tokens } from "./fixtures/Simulated.js";

const frameMs = 1000 / 24;

/** A connected session's H3 provider, and every video frame it receives in arrival order. */
const watch = Effect.gen(function* () {
  const session = yield* connect;
  const provider = yield* H3.make(session);
  const media = yield* session.decoded;
  const frames = yield* Ref.make<ReadonlyArray<VideoFrame>>([]);
  yield* media.video(H3.h3ReferenceTurboRealtime.tracks.video).pipe(
    Stream.runForEach((frame) => Ref.update(frames, (all) => [...all, frame])),
    Effect.forkScoped,
  );
  return { provider, frames };
});

/** One session's log entries of one kind and name: a block's tests share a simulated Reactor. */
const logged = (sessionId: string, kind: "build" | "command" | "message", name: string) =>
  Effect.map(ReactorTest.ReactorTest.pipe(Effect.flatMap((test) => test.log)), (log) =>
    log.filter(
      (entry) => entry.sessionId === sessionId && entry.kind === kind && entry.name === name,
    ),
  );

/** Plays two 5 s clips back to back and returns each clip's frames, and the black ones. */
const playTwo = (flush?: boolean) =>
  Effect.gen(function* () {
    const { provider, frames } = yield* watch;
    if (flush !== undefined) yield* provider.setFlushOnClipEnd(flush);
    yield* provider.setAutoplay(true);
    const first = yield* provider.enqueue({ prompt: "first", seconds: 5 });
    const second = yield* provider.enqueue({ prompt: "second", seconds: 5 });
    yield* Effect.sleep("30 seconds");
    const all = yield* Ref.get(frames);
    const played = all.flatMap((frame) => {
      const decoded = ReactorTest.frameOf(frame);
      return decoded === undefined
        ? []
        : [{ ...decoded, at: Number(frame.timestampMicros) / 1000 }];
    });
    const of = (clipId: string) => played.filter((frame) => frame.clipId === clipId);
    // A black frame's position in arrival order, so a test can place it between clips.
    const black = all.flatMap((frame, position) =>
      ReactorTest.frameOf(frame) === undefined ? [position] : [],
    );
    return { first: of(first.clip.clip_id), second: of(second.clip.clip_id), black, frames };
  });

layer(environment({ timing: ReactorTest.Timing.fixed({ buildSpeed: 2.4, seam: "70 millis" }) }))(
  "playback",
  (it) => {
    it.effect("plays each 5 s clip as 124 frames, its seam as timed, and nothing when idle", () =>
      Effect.gen(function* () {
        yield* Effect.forkScoped(ReactorTest.flow());
        const { first, second, frames } = yield* playTwo();
        assert.deepStrictEqual(
          first.map((frame) => frame.index),
          Array.from({ length: 124 }, (_, index) => index),
        );
        assert.strictEqual(second.length, 124);
        // The next clip starts the seam's 70 ms after the last one ends, one frame after its last frame.
        const seam = (second[0]?.at ?? 0) - (first[123]?.at ?? 0);
        assert.approximately(seam, frameMs + 70, 1);
        // Hosted H3 sends no frames while nothing plays.
        const idle = (yield* Ref.get(frames)).length;
        yield* Effect.sleep("10 seconds");
        assert.strictEqual((yield* Ref.get(frames)).length, idle);
      }),
    );
  },
);

layer(environment({ timing: ReactorTest.Timing.hosted }))("the hosted trace", (it) => {
  it.effect("plays back to back within the seams two paid runs measured", () =>
    Effect.gen(function* () {
      yield* Effect.forkScoped(ReactorTest.flow());
      const { first, second } = yield* playTwo();
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
              open: H3Source.open({ tokens: yield* tokens }),
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

layer(environment({ timing: ReactorTest.Timing.fixed({ buildSpeed: 2.4, seam: "70 millis" }) }))(
  "the documented boundary",
  (it) => {
    it.effect(
      "flushes to black at each boundary by default, and holds the last frame without",
      () =>
        Effect.gen(function* () {
          yield* Effect.forkScoped(ReactorTest.flow());
          const flushed = yield* playTwo();
          // One black frame after each clip's last frame, the first before the next clip starts.
          assert.deepStrictEqual(flushed.black, [124, 249]);
          const held = yield* playTwo(false);
          assert.deepStrictEqual(held.black, []);
          assert.strictEqual(held.first.length + held.second.length, 248);
        }),
    );
  },
);

layer(environment({ timing: ReactorTest.Timing.fixed({ buildSpeed: 2.4, seam: "1 second" }) }))(
  "provider semantics",
  (it) => {
    const unknown = "00000000-0000-4000-8000-0000000000ff";

    it.effect("continues only from a clip the session holds, and otherwise builds alone", () =>
      Effect.gen(function* () {
        yield* Effect.forkScoped(ReactorTest.flow());
        const { provider } = yield* watch;
        const source = yield* provider.enqueue({ prompt: "source", seconds: 5 });
        yield* Effect.sleep("5 seconds");
        const continued = yield* provider.enqueue({
          prompt: "continued",
          seconds: 5,
          continueFrom: source.clip.clip_id,
        });
        // An unknown id is not refused: H3 builds the clip without the continuation.
        const alone = yield* provider.enqueue({
          prompt: "alone",
          seconds: 5,
          continueFrom: unknown,
        });
        yield* Effect.sleep("10 seconds");
        assert.deepStrictEqual(
          (yield* logged(provider.sessionId, "build", "continued")).map((entry) => entry.clipId),
          [continued.clip.clip_id],
        );
        assert.deepStrictEqual(
          (yield* logged(provider.sessionId, "build", "independent")).map((entry) => entry.clipId),
          [alone.clip.clip_id],
        );
        // Audio with no image cannot fall back, so an unknown continuation refuses it.
        const refused = yield* Effect.flip(
          provider.enqueue({
            prompt: "a voice alone",
            audio: [{ _tag: "Bytes", bytes: ReactorTest.wavBytes({ seconds: 3 }) }],
            continueFrom: unknown,
          }),
        );
        assert.strictEqual(refused.context.outcome, "replied");
      }),
    );

    it.effect("counts an armed clip as playing, and stop cuts it before it starts", () =>
      Effect.gen(function* () {
        yield* Effect.forkScoped(ReactorTest.flow());
        const { provider, frames } = yield* watch;
        yield* provider.setFlushOnClipEnd(false);
        yield* provider.setAutoplay(true);
        yield* provider.enqueue({ prompt: "first", seconds: 5 });
        const second = yield* provider.enqueue({ prompt: "second", seconds: 5 });
        // The first starts after its build (about 2.15 s) and its 1 s seam, and ends 5.17 s later;
        // the second is then armed for its own 1 s seam.
        yield* Effect.sleep("8800 millis");
        const armed = (yield* provider.getState).value;
        assert.deepStrictEqual([armed.playing, armed.playing_clip_id], [true, second.clip.clip_id]);
        yield* provider.stop;
        yield* Effect.sleep("10 seconds");
        assert.deepStrictEqual(
          (yield* logged(provider.sessionId, "message", "clip_stopped")).map(
            (entry) => entry.clipId,
          ),
          [second.clip.clip_id],
        );
        const aired = (yield* Ref.get(frames)).filter(
          (frame) => ReactorTest.frameOf(frame)?.clipId === second.clip.clip_id,
        );
        assert.strictEqual(aired.length, 0);
      }),
    );

    it.effect("fails a clip whose prompt is past the model's text budget when it would build", () =>
      Effect.gen(function* () {
        yield* Effect.forkScoped(ReactorTest.flow());
        const { provider } = yield* watch;
        // H3's estimate is 3.5 characters a token, so 7,001 characters is over 2,000 tokens.
        const long = yield* provider.enqueue({ prompt: "x".repeat(7_001), seconds: 5 });
        const short = yield* provider.enqueue({ prompt: "short", seconds: 5 });
        yield* Effect.sleep("5 seconds");
        assert.deepStrictEqual(
          (yield* logged(provider.sessionId, "message", "clip_failed")).map(
            (entry) => entry.clipId,
          ),
          [long.clip.clip_id],
        );
        assert.deepStrictEqual(
          (yield* logged(provider.sessionId, "message", "clip_generated")).map(
            (entry) => entry.clipId,
          ),
          [short.clip.clip_id],
        );
      }),
    );

    it.effect("refuses an enqueue whose reference image the fault marks invalid", () =>
      Effect.gen(function* () {
        yield* Effect.forkScoped(ReactorTest.flow());
        const test = yield* ReactorTest.ReactorTest;
        const { provider } = yield* watch;
        yield* test.inject({ _tag: "InvalidImage", nth: 1 });
        const image = {
          _tag: "Bytes",
          bytes: ReactorTest.pngBytes({ width: 64, height: 64 }),
        } as const;
        const refused = yield* Effect.flip(
          provider.enqueue({ prompt: "one", references: [image] }),
        );
        assert.strictEqual(refused.context.outcome, "replied");
        const accepted = yield* provider.enqueue({ prompt: "two", references: [image] });
        assert.isTrue(accepted.clip.has_reference_image);
      }),
    );

    it.effect("publishes its rate in credits a minute, as the pricing API does", () =>
      Effect.gen(function* () {
        const coordinator = yield* Coordinator.Coordinator;
        const rate = yield* Coordinator.modelRate(yield* coordinator.pricing, H3.modelName);
        assert.strictEqual((rate.creditsPerSecond * 60) / rate.creditsPerDollar, 0.75);
      }),
    );
  },
);

layer(
  environment({
    timing: ReactorTest.Timing.fixed({ buildSpeed: 2.4, seam: "70 millis", stop: "100 millis" }),
  }),
)("a stop's landing", (it) => {
  it.effect(
    "answers a stop before its clip ends, and holds a stop sent meanwhile for the next clip",
    () =>
      Effect.gen(function* () {
        yield* Effect.forkScoped(ReactorTest.flow());
        const { provider } = yield* watch;
        yield* provider.setAutoplay(true);
        const first = yield* provider.enqueue({ prompt: "first", seconds: 15 });
        const second = yield* provider.enqueue({ prompt: "second", seconds: 5 });
        const third = yield* provider.enqueue({ prompt: "third", seconds: 5 });
        yield* Effect.sleep("9 seconds");
        yield* provider.stop;
        // Acknowledged, not landed: the clip still plays, so a play is refused.
        assert.strictEqual((yield* provider.getState).value.playing_clip_id, first.clip.clip_id);
        const refused = yield* Effect.flip(provider.play(third.clip.clip_id));
        assert.strictEqual(refused.context.outcome, "replied");
        // H3's stop names no clip: this one waits for the landing, then stops the next clip.
        yield* provider.stop;
        yield* Effect.sleep("10 seconds");
        assert.deepStrictEqual(
          (yield* logged(provider.sessionId, "message", "clip_stopped")).map(
            (entry) => entry.clipId,
          ),
          [first.clip.clip_id, second.clip.clip_id],
        );
        assert.deepStrictEqual(
          (yield* logged(provider.sessionId, "message", "clip_started")).map(
            (entry) => entry.clipId,
          ),
          [first.clip.clip_id, second.clip.clip_id, third.clip.clip_id],
        );
      }),
  );
});

// Reactor's docs: a session that loses its last connection lives 30 seconds, then ends.
layer(environment({ timing: ReactorTest.Timing.fixed({ buildSpeed: 2.4 }) }))(
  "the reconnect window",
  (it) => {
    it.effect("ends a session 30 s after its connection drops with none back", () =>
      Effect.gen(function* () {
        yield* Effect.forkScoped(ReactorTest.flow("50 millis"));
        const test = yield* ReactorTest.ReactorTest;
        yield* test.inject({ _tag: "Disconnect", nth: 1, after: Duration.seconds(1) });
        const session = yield* connect;
        yield* Effect.sleep("20 seconds");
        const state = (id: string) =>
          Effect.map(test.sessions, (all) => all.find((info) => info.id === id)?.state);
        assert.strictEqual(yield* state(session.id), "ACTIVE");
        yield* Effect.sleep("15 seconds");
        assert.strictEqual(yield* state(session.id), "CLOSED");
        const late = yield* Effect.flip(session.reconnect);
        assert.strictEqual(late.reason._tag, "TerminalSession");
      }),
    );
  },
);

// Reactor's docs: 5 concurrent sessions, and 10 a minute with 3 back to back; 429 says when to retry.
layer(environment({ timing: ReactorTest.Timing.fixed({ buildSpeed: 2.4 }) }))("quotas", (it) => {
  it.effect("refuses a fourth session at once, retryable after the delay it names", () =>
    Effect.gen(function* () {
      yield* Effect.forkScoped(ReactorTest.flow("50 millis"));
      yield* Effect.replicateEffect(connect, 3);
      const refused = yield* Effect.flip(connect);
      assert.deepStrictEqual(
        [refused.reason._tag, refused.reason._tag === "Http" ? refused.reason.status : undefined],
        ["Http", 429],
      );
      assert.isTrue(refused.isRetryable);
      assert.isDefined(refused.retryAfter);
      assert.strictEqual(refused.cleanup.allocation, "none");
      yield* Effect.sleep("6 seconds");
      yield* connect;
    }),
  );
});
