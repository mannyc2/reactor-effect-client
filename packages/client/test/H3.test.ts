/**
 * The H3 provider through its public API, over the real session and a
 * simulated Reactor. Each block states the timing it relies on; the last runs
 * one flow across seeded random timings and checks what must hold for any.
 */
import { assert, layer } from "@effect/vitest";
import { Effect, Exit, type Layer, Scope } from "effect";
import * as H3 from "../src/H3.js";
import { ReactorTest } from "../src/index.js";
import { PolicyFailure } from "../src/orchestration/policy.js";
import { pngBytes } from "../src/testing/Png.js";
import { wavBytes } from "../src/testing/Wav.js";
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

scenario("a commit hook's refusal sends nothing and reaches the caller unchanged", () =>
  Effect.gen(function* () {
    yield* Effect.forkScoped(ReactorTest.flow());
    const provider = yield* H3.make(yield* connect);
    const submission = yield* provider.prepare(
      { prompt: "refused locally" },
      { commit: () => PolicyFailure.refuse("QueueFull", "the application is full") },
    );
    const failure = yield* Effect.flip(submission.submit);
    assert.isTrue(PolicyFailure.is(failure));
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
    const image = { _tag: "Bytes" as const, bytes: pngBytes(64, 64) };
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
          references: [{ _tag: "Bytes", bytes: pngBytes(64, 64) }],
          audio: [{ _tag: "Bytes", bytes: wavBytes(3) }],
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
