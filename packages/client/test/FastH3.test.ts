/** Requested FastH3 input boundaries and provider flows, written before the model support. */
import { assert, describe, it, layer } from "@effect/vitest";
import { Effect, Result, Schema, type Layer, type Scope } from "effect";
import * as FastH3 from "../src/FastH3.js";
import * as H3 from "../src/H3.js";
import { CoordinatorClient, Reactor, ReactorTest } from "../src/index.js";
import { capture, encode } from "../src/internal/fastH3/request.js";
import { snapFrames } from "../src/internal/fastH3/profile.js";
import { connect as connectH3, commands, environment } from "./fixtures/Simulated.js";

const id = "00000000-0000-4000-8000-000000000001";
const frame: H3.Reference = {
  _tag: "Bytes",
  bytes: ReactorTest.pngBytes({ width: 64, height: 64 }),
};

describe("FastH3 request", () => {
  it("accepts a starting frame with a closing clip", () => {
    const result = Schema.decodeResult(FastH3.Request)({
      prompt: "a painting comes alive",
      start: frame,
      end: { endFrom: id },
    });
    assert.isTrue(Result.isSuccess(result));
  });

  it("accepts a hold between the same clip's ending and starting frame", () => {
    const result = Schema.decodeResult(FastH3.Request)({
      prompt: "hold the view",
      start: { continueFrom: id },
      end: { endFrom: id },
    });
    assert.isTrue(Result.isSuccess(result));
  });

  it("refuses five seconds and accepts the maximum request length", () => {
    assert.isTrue(
      Result.isFailure(Schema.decodeResult(FastH3.Request)({ prompt: "short", seconds: 5 })),
    );
    assert.isTrue(
      Result.isSuccess(Schema.decodeResult(FastH3.Request)({ prompt: "long", seconds: 14.375 })),
    );
  });

  it("refuses a continuation that is not a clip UUID", () => {
    assert.isTrue(
      Result.isFailure(
        Schema.decodeResult(FastH3.Request)({
          prompt: "continue",
          start: { continueFrom: "not-a-uuid" },
        }),
      ),
    );
  });

  it.effect("encodes a starting frame without H3's reference properties", () =>
    Effect.gen(function* () {
      const request = yield* capture({ prompt: "a frame", start: frame });
      const args = encode({
        request,
        uploaded: [
          {
            uploadId: id,
            name: "frame.png",
            mimeType: "image/png",
            size: BigInt(frame.bytes.length),
          },
        ],
        metadata: "caller",
      });
      assert.property(args, "starting_frame");
      assert.deepStrictEqual(args.starting_frame, {
        upload_id: id,
        name: "frame.png",
        mime_type: "image/png",
        size: frame.bytes.length,
      });
      assert.notProperty(args, "reference_images");
      assert.notProperty(args, "reference_audios");
    }),
  );

  it("aligns the observed requests upward and preserves the rounded minimum", () => {
    assert.deepStrictEqual([5.167, 5.5, 6, 10, 14.375].map(snapFrames), [124, 141, 158, 243, 345]);
  });
});

const timing = ReactorTest.Timing.fixed({ buildSpeed: 2, http: "20 millis", channel: "10 millis" });
const scenario = <E>(
  name: string,
  body: () => Effect.Effect<void, E, Layer.Success<ReturnType<typeof environment>> | Scope.Scope>,
) => layer(environment({ timing }))(name, (it) => it.effect(name, body));

const connect = Effect.gen(function* () {
  const test = yield* ReactorTest.ReactorTest;
  const coordinator = yield* CoordinatorClient.CoordinatorClient;
  const reactor = yield* Reactor.Reactor;
  const session = yield* reactor.create({
    model: FastH3.modelName,
    tokens: coordinator.tokens({
      apiKey: test.apiKey,
      modelName: FastH3.modelName,
      maxSessionDuration: "5 minutes",
      expiresAfter: "10 minutes",
    }),
  });
  return { session, provider: yield* FastH3.make(session) };
});

scenario("an independent clip is accepted, built and played with FastH3's facts", () =>
  Effect.gen(function* () {
    yield* Effect.forkScoped(ReactorTest.flow());
    const { provider } = yield* connect;
    yield* provider.setAutoplay(true);
    const submission = yield* provider.prepare({ prompt: "a harbour at dawn", seconds: 5.167 });
    const accepted = yield* submission.submit;
    assert.strictEqual(accepted.clip.has_starting_frame, false);
    assert.strictEqual(accepted.clip.continue_from_clip_id, null);
    const operation = yield* provider.operation(submission);
    yield* operation.reached("generated");
    yield* operation.reached("started");
    assert.strictEqual((yield* operation.ended).message, "clip_finished");
  }),
);

scenario("a starting frame uploads once and is reported in the accepted clip", () =>
  Effect.gen(function* () {
    yield* Effect.forkScoped(ReactorTest.flow());
    const test = yield* ReactorTest.ReactorTest;
    const { session, provider } = yield* connect;
    const accepted = yield* provider.enqueue({ prompt: "from the painting", start: frame });
    assert.strictEqual(accepted.clip.has_starting_frame, true);
    const uploads = (yield* test.log).filter(
      (entry) =>
        entry.sessionId === session.id &&
        entry.kind === "upload" &&
        entry.name.startsWith("stored"),
    );
    assert.strictEqual(uploads.length, 1);
  }),
);

scenario("a history clip can continue until it is evicted, then the model refuses it", () =>
  Effect.gen(function* () {
    yield* Effect.forkScoped(ReactorTest.flow());
    const { provider } = yield* connect;
    yield* provider.setAutoplay(true);
    const original = yield* provider.prepare({ prompt: "the first view", seconds: 5.167 });
    const first = yield* original.submit;
    yield* (yield* provider.operation(original)).ended;
    assert.include(
      (yield* provider.getQueue).value.history.map((clip) => clip.clip_id),
      first.clip.clip_id,
    );
    const continuation = yield* provider.prepare({
      prompt: "continue the view",
      seconds: 5.167,
      start: { continueFrom: first.clip.clip_id },
    });
    const continued = yield* continuation.submit;
    assert.strictEqual(continued.clip.continue_from_clip_id, first.clip.clip_id);
    yield* (yield* provider.operation(continuation)).ended;
    // ReactorTest's permitted default retains eight clips; this flow crosses that boundary.
    for (let index = 0; index < 8; index++) {
      const next = yield* provider.prepare({ prompt: `another view ${index}`, seconds: 5.167 });
      yield* next.submit;
      yield* (yield* provider.operation(next)).ended;
    }
    assert.notInclude(
      (yield* provider.getQueue).value.history.map((clip) => clip.clip_id),
      first.clip.clip_id,
    );
    const failure = yield* Effect.flip(
      provider.enqueue({ prompt: "the old view", start: { continueFrom: first.clip.clip_id } }),
    );
    assert.strictEqual(failure.reason._tag, "Remote");
    assert.strictEqual(failure.context.outcome, "replied");
  }),
);

scenario("a six-second request builds the next grid length up", () =>
  Effect.gen(function* () {
    yield* Effect.forkScoped(ReactorTest.flow());
    const { provider } = yield* connect;
    const submission = yield* provider.prepare({ prompt: "a longer view", seconds: 6 });
    const accepted = yield* submission.submit;
    yield* (yield* provider.operation(submission)).reached("generated");
    const clip = (yield* provider.getQueue).value.playout.find(
      (entry) => entry.clip_id === accepted.clip.clip_id,
    );
    assert.isDefined(clip);
    assert.strictEqual(clip.frames, 124 + 2 * 17);
  }),
);

scenario("FastH3 refuses an H3 deployment before reading model facts", () =>
  Effect.gen(function* () {
    yield* Effect.forkScoped(ReactorTest.flow());
    const failure = yield* Effect.flip(FastH3.make(yield* connectH3));
    assert.strictEqual(failure.reason._tag, "UnsupportedCapability");
    assert.strictEqual(failure.context.outcome, "not-submitted");
    assert.deepStrictEqual(yield* commands("get_state"), []);
    assert.deepStrictEqual(yield* commands("get_queue"), []);
  }),
);
