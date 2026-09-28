/**
 * The H3 provider through its public API, over the real session and a
 * simulated Reactor. Each block states the timing it relies on; the last runs
 * one flow across seeded random timings and checks what must hold for any.
 */
import { assert, describe, it, layer } from "@effect/vitest";
import { Effect, Exit, type Layer, Result, Schema, Scope } from "effect";
import * as H3 from "../src/H3.js";
import { ReactorTest } from "../src/index.js";
import { deploymentContract } from "../src/internal/h3/commands.js";
import { decodeMessage } from "../src/internal/h3/messages.js";
import { deployment } from "../src/internal/reactorTest/h3.js";
import { commands, connect, environment } from "./fixtures/Simulated.js";

const timing = ReactorTest.Timing.fixed({ buildSpeed: 2, http: "20 millis", channel: "10 millis" });

/** One test on a simulated Reactor of its own, so no fault, session or log carries over. */
const scenario = <E>(
  name: string,
  body: () => Effect.Effect<void, E, Layer.Success<ReturnType<typeof environment>> | Scope.Scope>,
  options: Omit<Parameters<typeof environment>[0], "timing"> = {},
) => layer(environment({ timing, ...options }))(name, (it) => it.effect(name, body));

scenario("a correlated reply accepts the clip, and the caller then finds it queued", () =>
  Effect.gen(function* () {
    yield* Effect.forkScoped(ReactorTest.flow());
    const provider = yield* H3.make(yield* connect);
    const acceptance = yield* provider.enqueue({ prompt: "a harbour at dawn", seconds: 5 });
    assert.strictEqual(acceptance.evidence.kind, "correlated");
    const snapshot = yield* provider.snapshot;
    assert.strictEqual(snapshot._tag, "Ready");
    const queued =
      snapshot._tag === "Ready"
        ? [...snapshot.queue.generation, ...snapshot.queue.playout].map((clip) => clip.clip_id)
        : [];
    assert.include(queued, acceptance.clip.clip_id);
  }),
);

scenario("a lost reply is proven by the queue that lists the clip, and nothing is resent", () =>
  Effect.gen(function* () {
    yield* Effect.forkScoped(ReactorTest.flow());
    const test = yield* ReactorTest.ReactorTest;
    const provider = yield* H3.make(yield* connect);
    yield* test.inject({ _tag: "DropReply", command: "enqueue", applied: true });
    const acceptance = yield* provider.enqueue({ prompt: "only the broadcast", seconds: 5 });
    assert.strictEqual(acceptance.evidence.kind, "metadata");
    assert.deepStrictEqual(
      (yield* commands("enqueue")).map((entry) => entry.dropped),
      ["reply"],
    );
  }),
);

scenario("an enqueue with no evidence fails as unknown and is never sent again", () =>
  Effect.gen(function* () {
    yield* Effect.forkScoped(ReactorTest.flow());
    const test = yield* ReactorTest.ReactorTest;
    const provider = yield* H3.make(yield* connect);
    yield* test.inject({ _tag: "DropReply", command: "enqueue" });
    const failure = yield* Effect.flip(provider.enqueue({ prompt: "lost", seconds: 5 }));
    assert.strictEqual(failure.context.outcome, "unknown");
    yield* Effect.sleep("30 seconds");
    assert.deepStrictEqual(
      (yield* commands("enqueue")).map((entry) => entry.dropped),
      ["command"],
    );
  }),
);

/** An application's own local refusal. */
class Full extends Schema.TaggedError<Full>()("Full", {}) {}

scenario("a commit hook's refusal sends nothing and reaches the caller unchanged", () =>
  Effect.gen(function* () {
    yield* Effect.forkScoped(ReactorTest.flow());
    const provider = yield* H3.make(yield* connect);
    const submission = yield* provider.prepare(
      { prompt: "refused locally" },
      { commit: () => Full.make({}) },
    );
    const failure = yield* Effect.flip(submission.submit);
    assert.strictEqual(failure._tag, "Full");
    assert.deepStrictEqual(yield* commands("enqueue"), []);
  }),
);

scenario("a clip's facts arrive in order: generated, started, then finished", () =>
  Effect.gen(function* () {
    yield* Effect.forkScoped(ReactorTest.flow());
    const provider = yield* H3.make(yield* connect);
    yield* provider.setAutoplay(true);
    const submission = yield* provider.prepare({ prompt: "watched", seconds: 5 });
    yield* submission.submit;
    const operation = yield* provider.operation(submission);
    const generated = yield* operation.reached("generated");
    const started = yield* operation.reached("started");
    const ended = yield* operation.ended;
    assert.isTrue(generated.source.sequence <= started.source.sequence);
    assert.isTrue(started.source.sequence < ended.source.sequence);
    assert.strictEqual(ended.message, "clip_finished");
  }),
);

scenario("a failed build ends the clip, and the phases it never reached fail ClipEnded", () =>
  Effect.gen(function* () {
    yield* Effect.forkScoped(ReactorTest.flow());
    const test = yield* ReactorTest.ReactorTest;
    const provider = yield* H3.make(yield* connect);
    yield* test.inject({ _tag: "FailBuild" });
    const submission = yield* provider.prepare({ prompt: "fails to build", seconds: 5 });
    yield* submission.submit;
    const operation = yield* provider.operation(submission);
    assert.strictEqual((yield* operation.ended).message, "clip_failed");
    const started = yield* Effect.flip(operation.reached("started"));
    assert.strictEqual(started.reason._tag, "ClipEnded");
  }),
);

scenario("closing the provider leaves undecided facts Indeterminate and the session open", () =>
  Effect.gen(function* () {
    yield* Effect.forkScoped(ReactorTest.flow());
    const test = yield* ReactorTest.ReactorTest;
    const session = yield* connect;
    const scope = yield* Scope.make();
    const provider = yield* H3.make(session).pipe(Scope.provide(scope));
    yield* test.inject({ _tag: "StallBuild" });
    const submission = yield* provider.prepare({ prompt: "never built", seconds: 5 });
    yield* submission.submit;
    const operation = yield* provider.operation(submission);
    yield* Scope.close(scope, Exit.void);
    assert.strictEqual((yield* Effect.flip(operation.ended)).reason._tag, "Indeterminate");
    assert.strictEqual((yield* session.snapshot).status, "ready");
  }),
);

scenario("identical image bytes upload once for the clips that share them", () =>
  Effect.gen(function* () {
    yield* Effect.forkScoped(ReactorTest.flow());
    const test = yield* ReactorTest.ReactorTest;
    const provider = yield* H3.make(yield* connect);
    const image = {
      _tag: "Bytes" as const,
      bytes: ReactorTest.pngBytes({ width: 64, height: 64 }),
    };
    const first = yield* provider.enqueue({ prompt: "one", references: [image] });
    const second = yield* provider.enqueue({ prompt: "two", references: [image] });
    assert.strictEqual(first.clip.reference_image_count, 1);
    assert.strictEqual(second.clip.reference_image_count, 1);
    const stored = (yield* test.log).filter(
      (entry) => entry.kind === "upload" && entry.name.startsWith("stored"),
    );
    assert.strictEqual(stored.length, 1);
  }),
);

scenario("bytes that are not an image are refused before anything is uploaded or sent", () =>
  Effect.gen(function* () {
    yield* Effect.forkScoped(ReactorTest.flow());
    const test = yield* ReactorTest.ReactorTest;
    const provider = yield* H3.make(yield* connect);
    const failure = yield* Effect.flip(
      provider.enqueue({
        prompt: "a broken reference",
        references: [{ _tag: "Bytes", bytes: new Uint8Array([1, 2, 3]) }],
      }),
    );
    assert.strictEqual(failure.reason._tag, "InvalidInput");
    assert.strictEqual(failure.context.outcome, "not-submitted");
    const sent = (yield* test.log).filter(
      (entry) => entry.kind === "upload" || (entry.kind === "command" && entry.name === "enqueue"),
    );
    assert.deepStrictEqual(sent, []);
  }),
);

scenario(
  "a deployment without reference audio refuses audio, uploading nothing",
  () =>
    Effect.gen(function* () {
      yield* Effect.forkScoped(ReactorTest.flow());
      const test = yield* ReactorTest.ReactorTest;
      const provider = yield* H3.make(yield* connect);
      assert.isFalse(provider.contract.referenceAudio);
      const failure = yield* Effect.flip(
        provider.enqueue({
          prompt: "Audio 1 over a still",
          references: [{ _tag: "Bytes", bytes: ReactorTest.pngBytes({ width: 64, height: 64 }) }],
          audio: [{ _tag: "Bytes", bytes: ReactorTest.wavBytes({ seconds: 3 }) }],
        }),
      );
      assert.strictEqual(failure.reason._tag, "UnsupportedCapability");
      assert.deepStrictEqual(
        (yield* test.log).filter((entry) => entry.kind === "upload"),
        [],
      );
    }),
  { referenceAudio: false },
);

/**
 * One flow under timings drawn from wide ranges: builds from a quarter of real
 * time to ten times it, and every request and message delayed up to seconds.
 * Whatever the timing, each clip is accepted once, reaches its end in order,
 * and no enqueue is sent twice. A failing block names its seed.
 */
for (const seed of [1, 2, 3, 4, 5, 6, 7, 8]) {
  layer(environment({ timing: ReactorTest.Timing.random({ seed }) }))(
    `random timing, seed ${seed}`,
    (it) => {
      it.effect("each clip is accepted once and ends in order, and nothing is resent", () =>
        Effect.gen(function* () {
          yield* Effect.forkScoped(ReactorTest.flow("50 millis"));
          const provider = yield* H3.make(yield* connect);
          yield* provider.setAutoplay(true);
          const submissions = yield* Effect.forEach(["first", "second", "third"], (prompt) =>
            provider.prepare({ prompt, seconds: 5 }),
          );
          const acceptances = yield* Effect.forEach(submissions, (submission) => submission.submit);
          assert.strictEqual(
            new Set(acceptances.map((acceptance) => acceptance.clip.clip_id)).size,
            3,
          );
          for (const submission of submissions) {
            const operation = yield* provider.operation(submission);
            const ended = yield* operation.ended;
            const facts = yield* operation.facts;
            assert.strictEqual(ended.message, "clip_finished", `seed ${seed}`);
            assert.isTrue(
              (facts.generated?.source.sequence ?? Infinity) <=
                (facts.started?.source.sequence ?? -Infinity),
              `seed ${seed}: generated before started`,
            );
          }
          assert.strictEqual((yield* commands("enqueue")).length, 3, `seed ${seed}`);
        }),
      );
    },
  );
}

describe("the deployment and its messages", () => {
  /** The simulated deployment's document without one command, as the session reads it. */
  const withoutCommand = (name: string) => {
    const document = deployment(true);
    const { [`/events/${name}`]: _removed, ...paths } = document.paths;
    return Result.flatMap(
      Schema.decodeUnknownResult(Schema.Json)({ ...document, paths }),
      deploymentContract,
    );
  };

  it("admits a deployment that lacks a command the provider can start without", () => {
    const contract = withoutCommand("set_seed");
    assert.isTrue(Result.isSuccess(contract));
    if (Result.isSuccess(contract)) {
      assert.isFalse(contract.success.commands.has("set_seed"));
      assert.isTrue(contract.success.commands.has("enqueue"));
    }
  });

  it("refuses a deployment that lacks a command the provider needs to start", () => {
    const contract = withoutCommand("get_queue");
    assert.isTrue(Result.isFailure(contract));
    const failure = Result.isFailure(contract) ? contract.failure : undefined;
    assert.strictEqual(
      failure?._tag === "ReactorError" ? failure.reason._tag : failure?._tag,
      "UnsupportedCapability",
    );
  });

  it("decodes a state that is playing an armed clip it does not name yet", () => {
    const decoded = decodeMessage({
      type: "state_update",
      data: {
        clip_seconds: 5.167,
        clip_seconds_min: 5,
        clip_seconds_max: 15.084,
        seed: 1000,
        autoplay: true,
        flush_on_clip_end: true,
        aspect: "16:9",
        width: 1344,
        height: 768,
        playing: true,
        playing_clip_id: null,
        generation_queued: 0,
        generation_capacity: 20,
        playout_queued: 1,
        playout_capacity: 10,
        clips_played: 1,
        seconds_sent: 5.167,
        valid_commands: ["stop"],
      },
    });
    assert.isTrue(Result.isSuccess(decoded));
  });
});

describe("references are refused before anything is uploaded", () => {
  const bytes = (...parts: ReadonlyArray<string | ReadonlyArray<number>>) =>
    new Uint8Array(
      parts.flatMap((part) =>
        typeof part === "string" ? Array.from(part, (char) => char.charCodeAt(0)) : [...part],
      ),
    );
  const le32 = (value: number) => [
    value & 255,
    (value >> 8) & 255,
    (value >> 16) & 255,
    value >>> 24,
  ];
  const be32 = (value: number) => [
    value >>> 24,
    (value >> 16) & 255,
    (value >> 8) & 255,
    value & 255,
  ];
  const webp = (type: string, body: ReadonlyArray<number>) =>
    bytes("RIFF", le32(22), "WEBP", type, le32(10), body);
  const images: ReadonlyArray<readonly [string, Uint8Array, string | undefined]> = [
    ["PNG", ReactorTest.pngBytes({ width: 64, height: 64 }), "image/png"],
    [
      "JPEG",
      bytes([255, 216, 255, 192, 0, 11, 8, 0, 64, 0, 64, 1, 1, 17, 0, 255, 217]),
      "image/jpeg",
    ],
    ["lossy WebP", webp("VP8 ", [0, 0, 0, 157, 1, 42, 64, 0, 64, 0]), "image/webp"],
    ["lossless WebP", webp("VP8L", [47, ...le32(63 | (63 << 14)), 0, 0, 0, 0, 0]), "image/webp"],
    ["extended WebP", webp("VP8X", [0, 0, 0, 0, 63, 0, 0, 63, 0, 0]), "image/webp"],
    ["a truncated PNG", ReactorTest.pngBytes({ width: 64, height: 64 }).slice(0, 40), undefined],
    ["a JPEG with no dimensions", bytes([255, 216, 255, 217]), undefined],
    ["a truncated WebP", webp("VP8X", [0, 0, 0, 0]), undefined],
    ["an image four times too wide", ReactorTest.pngBytes({ width: 1000, height: 100 }), undefined],
    ["text", bytes("not an image at all, only words"), undefined],
  ];
  it.effect.each(images)("%s", ([, image, mimeType]) =>
    Effect.gen(function* () {
      const result = yield* Effect.result(H3.validateReference({ _tag: "Bytes", bytes: image }));
      assert.strictEqual(
        Result.isSuccess(result) ? result.success.mimeType : result.failure.context.outcome,
        mimeType ?? "not-submitted",
      );
    }),
  );

  const flac = (rate: number, channels: number, samples: number) =>
    bytes(
      "fLaC",
      [0, 0, 0, 34],
      [0, 16, 0, 16, 0, 0, 0, 0, 0, 0],
      [rate >> 12, (rate >> 4) & 255, ((rate & 15) << 4) | ((channels - 1) << 1), 0],
      be32(samples),
      new Array<number>(16).fill(0),
    );
  const audio: ReadonlyArray<readonly [string, Uint8Array, string | undefined]> = [
    ["WAV", ReactorTest.wavBytes({ seconds: 3 }), "audio/wav"],
    ["stereo WAV", ReactorTest.wavBytes({ seconds: 3, channels: 2 }), "audio/wav"],
    ["FLAC", flac(48_000, 2, 48_000 * 3), "audio/flac"],
    [
      "Opus in Ogg",
      bytes("OggS", new Array<number>(22).fill(0), [1, 0], "OpusHead", [1, 2]),
      "audio/ogg",
    ],
    ["M4A", bytes([0, 0, 0, 24], "ftypM4A ", new Array<number>(8).fill(0)), "audio/mp4"],
    ["WebM", bytes([26, 69, 223, 163], new Array<number>(8).fill(0)), "audio/webm"],
    ["MP3", bytes("ID3", new Array<number>(8).fill(0)), "audio/mpeg"],
    ["AAC", bytes([255, 241], new Array<number>(8).fill(0)), "audio/aac"],
    ["a WAV under 2 s", ReactorTest.wavBytes({ seconds: 1 }), undefined],
    ["a WAV over 15 s", ReactorTest.wavBytes({ seconds: 16 }), undefined],
    ["a FLAC over 15 s", flac(48_000, 2, 48_000 * 20), undefined],
    ["a FLAC with no sample rate", flac(0, 2, 48_000 * 3), undefined],
    ["text", bytes("not audio at all, only words"), undefined],
  ];
  it.effect.each(audio)("%s", ([, clip, mimeType]) =>
    Effect.gen(function* () {
      const result = yield* Effect.result(
        H3.validateAudioReference({ _tag: "Bytes", bytes: clip }),
      );
      assert.strictEqual(
        Result.isSuccess(result) ? result.success.mimeType : result.failure.context.outcome,
        mimeType ?? "not-submitted",
      );
    }),
  );
});
