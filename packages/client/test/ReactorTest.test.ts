/** The simulated Reactor behind the real client, H3 provider and orchestration. */
import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import { assert, layer } from "@effect/vitest";
import { Effect, Fiber, FileSystem, Layer, Path, Ref, Stream } from "effect";
import * as H3 from "../src/h3/index.js";
import type { VideoFrame } from "../src/host.js";
import { mediaGeneration } from "../src/host.js";
import * as Reactor from "../src/index.js";
import * as Orchestration from "../src/orchestration/index.js";
import { ReactorTest } from "../src/testing/index.js";

/** Each block gets its own simulated Reactor, so no session or bill carries over. */
const environment = (options?: Parameters<typeof ReactorTest.layer>[0]) =>
  Reactor.layer().pipe(
    Layer.provideMerge(ReactorTest.layer(options)),
    Layer.provideMerge(Layer.mergeAll(NodeCrypto.layer, FileSystem.layerNoop({}), Path.layer)),
  );

const mint = Effect.gen(function* () {
  const test = yield* ReactorTest.ReactorTest;
  const coordinator = yield* Reactor.Coordinator.make();
  return yield* coordinator.mintToken({
    apiKey: test.apiKey,
    modelName: H3.modelName,
    maxSessionDuration: "120 seconds",
    expiresAfter: "10 minutes",
  });
});

const connect = Effect.gen(function* () {
  const grant = yield* mint;
  const client = yield* Reactor.Client;
  return yield* client.createConnected({ model: H3.modelName, jwt: grant.jwt });
});

layer(environment())("playback", (it) => {
  it.effect("plays each 5 s clip as 124 frames with a measured seam, and nothing when idle", () =>
    Effect.gen(function* () {
      yield* Effect.forkScoped(ReactorTest.flow());
      const session = yield* connect;
      const provider = yield* H3.make(session);
      const media = yield* mediaGeneration(session);
      const frames = yield* Ref.make<ReadonlyArray<VideoFrame>>([]);
      yield* media.video(H3.h3ReferenceTurboRealtime.tracks.video).pipe(
        Stream.runForEach((frame) => Ref.update(frames, (all) => [...all, frame])),
        Effect.forkScoped,
      );
      yield* provider.setAutoplay(true);
      const first = yield* provider.enqueue({ prompt: "first", seconds: 5 });
      const second = yield* provider.enqueue({ prompt: "second", seconds: 5 });
      yield* Effect.sleep("20 seconds");

      const played = (yield* Ref.get(frames)).flatMap((frame) => {
        const decoded = ReactorTest.frameOf(frame);
        return decoded === undefined ? [] : [{ ...decoded, at: Number(frame.timestampMicros) }];
      });
      const of = (clipId: string) => played.filter((frame) => frame.clipId === clipId);
      assert.deepStrictEqual(
        of(first.clip.clip_id).map((frame) => frame.index),
        Array.from({ length: 124 }, (_, index) => index),
      );
      assert.strictEqual(of(second.clip.clip_id).length, 124);
      // clip_started follows clip_finished by 30–110 ms, so the seam adds that to one frame's spacing.
      const lastOfFirst = of(first.clip.clip_id)[123]?.at ?? 0;
      const seamMs = ((of(second.clip.clip_id)[0]?.at ?? 0) - lastOfFirst) / 1000;
      assert.isTrue(seamMs >= 1000 / 24 + 30 && seamMs <= 1000 / 24 + 110, `seam ${seamMs} ms`);

      // Hosted H3 sends no frames while nothing plays.
      yield* Effect.sleep("10 seconds");
      assert.strictEqual((yield* Ref.get(frames)).length, 248);
    }),
  );
});

layer(environment())("billing", (it) => {
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

layer(environment())("lost replies", (it) => {
  it.effect("an enqueue whose reply is lost fails as unknown and is never resent", () =>
    Effect.gen(function* () {
      yield* Effect.forkScoped(ReactorTest.flow("20 millis"));
      const test = yield* ReactorTest.ReactorTest;
      const provider = yield* H3.make(yield* connect);
      yield* test.inject({ _tag: "DropReply", command: "enqueue" });
      const failure = yield* Effect.flip(provider.enqueue({ prompt: "lost", seconds: 5 }));
      assert.strictEqual(failure.context.outcome, "unknown");
      yield* Effect.sleep("30 seconds");
      const enqueues = (yield* test.log).filter(
        (entry) => entry.kind === "command" && entry.name === "enqueue",
      );
      assert.deepStrictEqual(
        enqueues.map((entry) => entry.dropped),
        ["command"],
      );
    }),
  );
});

layer(environment({ faults: [{ _tag: "RefuseAllocation", nth: 1 }] }))("allocation", (it) => {
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

layer(environment())("orchestration", (it) => {
  it.effect("an orchestration opened with openH3 plays and terminates on the simulator", () =>
    Effect.gen(function* () {
      yield* Effect.forkScoped(ReactorTest.flow("10 millis"));
      const handle = yield* Orchestration.make({ open: Orchestration.openH3({ mint }) });
      const video = yield* handle.media.video.pipe(
        Stream.take(124),
        Stream.runCollect,
        Effect.forkScoped,
      );
      yield* handle.media.audio.pipe(Stream.runDrain, Effect.forkScoped);
      yield* handle.engine.setAutoplay(true);
      yield* handle.engine.enqueue(
        Orchestration.ClipRequest.make({
          prompt: "one",
          references: [],
          durationSeconds: 5,
          metadata: {},
        }),
      );
      assert.strictEqual((yield* Fiber.join(video)).length, 124);
      const report = yield* handle.close;
      assert.deepStrictEqual(
        report.sessions.map((cleanup) => cleanup.lease.remote.confirmed),
        [true],
      );
    }),
  );
});
