/** Clip operations: a committed submission's retained facts, bounded and scope-released. */
import { describe, expect, test } from "vitest";
import { Effect, Exit, Result, Scope } from "effect";
import { CommandFailure, ReactorError } from "../../src/errors.js";
import * as H3 from "../../src/h3/index.js";
import { fixture, fixtureClip, textArg } from "./ProviderSession.js";
import type { Fixture, Script } from "./ProviderSession.js";
import { run, runFlowing } from "./Clock.js";

const options: H3.Options = { replyTimeout: 100, setupTimeout: 1000, reconcileWindow: 20 };
const request: H3.Request = { prompt: "A blue paper boat on clear water.", seconds: 7 };

const setup = (script: Script = {}, bounds: H3.Options = {}) =>
  Effect.gen(function* () {
    const fake = yield* fixture(script);
    const provider = yield* H3.make(fake.session, { ...options, ...bounds });
    return { fake, provider };
  });

/** The clip the fixture accepted for the latest enqueue. */
const acceptedClip = (fake: Fixture) => ({ ...fake.accepted.at(-1)! });

/** An enqueue whose outcome stays unknown: the fixture never replies to it. */
const unknownEnqueue: Script = {
  command: { enqueue: ({ fail }) => Effect.fail(fail("unknown")) },
};

describe("H3 clip operations", () => {
  test("each phase resolves once, with the transport generation of its evidence", () =>
    run(
      Effect.gen(function* () {
        const { fake, provider } = yield* setup();
        const submission = yield* provider.prepare(request);
        const acceptance = yield* submission.submit;
        const op = yield* provider.operation(submission);
        expect(yield* op.accepted).toBe(acceptance);
        const clip = acceptedClip(fake);
        yield* fake.emit("clip_generated", { clip });
        expect(yield* op.reached("generated")).toMatchObject({
          clipId: clip.clip_id,
          message: "clip_generated",
          transportGeneration: 1n,
        });
        yield* fake.emit("clip_started", { clip });
        yield* fake.emit("clip_finished", { clip, seconds_sent: 7 });
        expect(yield* op.reached("started")).toMatchObject({ message: "clip_started" });
        expect(yield* op.ended).toMatchObject({ message: "clip_finished" });
        const facts = yield* op.facts;
        expect(facts.generated?.message).toBe("clip_generated");
        expect(facts.indeterminate).toBe(false);
      }),
    ));

  test("a start observed before the reply resolves generated as well", () =>
    run(
      Effect.gen(function* () {
        const { fake, provider } = yield* setup({
          command: {
            enqueue: ({ fake, defaults }) =>
              defaults.pipe(
                Effect.tap(() => fake.emit("clip_started", { clip: acceptedClip(fake) })),
              ),
          },
        });
        const submission = yield* provider.prepare(request);
        yield* submission.submit;
        const op = yield* provider.operation(submission);
        const generated = yield* op.reached("generated");
        expect(generated.message).toBe("clip_started");
        expect(yield* op.reached("started")).toBe(generated);
        expect(fake.accepted).toHaveLength(1);
      }),
    ));

  test("a clip that fails before it starts fails started with ClipEnded", () =>
    run(
      Effect.gen(function* () {
        const { fake, provider } = yield* setup();
        const submission = yield* provider.prepare(request);
        yield* submission.submit;
        const op = yield* provider.operation(submission);
        const clip = acceptedClip(fake);
        yield* fake.emit("clip_failed", { clip, reason: "fixture failure" });
        const started = yield* Effect.flip(op.reached("started"));
        expect(started).toMatchObject({
          reason: { _tag: "ClipEnded", lifecycle: "clip_failed", clipId: clip.clip_id },
        });
        expect(yield* op.ended).toMatchObject({ message: "clip_failed" });
      }),
    ));

  test("a queue snapshot after reconnect advances the facts, naming the new generation", () =>
    run(
      Effect.gen(function* () {
        const { fake, provider } = yield* setup();
        const submission = yield* provider.prepare(request);
        yield* submission.submit;
        const op = yield* provider.operation(submission);
        const clip = acceptedClip(fake);
        yield* fake.status("disconnected");
        yield* fake.status("ready", 2n);
        yield* fake.emit(
          "queue_update",
          { generation: [], playout: [{ ...clip, ready: true }], history: [] },
          { generation: 2n },
        );
        expect(yield* op.reached("generated")).toMatchObject({
          message: "queue_update",
          transportGeneration: 2n,
        });
      }),
    ));

  test("an unknown enqueue is attributed by later evidence, and acceptances stay unchanged", () =>
    runFlowing(
      Effect.gen(function* () {
        const { fake, provider } = yield* setup(unknownEnqueue);
        const submission = yield* provider.prepare(request);
        const exit = yield* Effect.exit(submission.submit);
        // The submission's own result is unchanged: its outcome is unknown.
        expect(Exit.isFailure(exit)).toBe(true);
        const op = yield* provider.operation(submission);
        const sent = fake.calls.find((call) => call.command === "enqueue")!;
        const clip = fixtureClip({
          prompt: textArg(sent.args.prompt),
          metadata: textArg(sent.args.metadata),
        });
        yield* fake.status("disconnected");
        yield* fake.status("ready", 2n);
        yield* fake.emit(
          "queue_update",
          { generation: [{ ...clip }], playout: [], history: [] },
          { generation: 2n },
        );
        const acceptance = yield* op.accepted;
        expect(acceptance.clip.clip_id).toBe(clip.clip_id);
        expect(acceptance.evidence.kind).toBe("metadata");
        expect(acceptance.evidence.source.generation).toBe(2n);
        expect(yield* provider.acceptance(submission.id)).toBeUndefined();
      }),
    ));

  test("a definite enqueue failure fails every fact with that failure", () =>
    run(
      Effect.gen(function* () {
        const { provider } = yield* setup({
          command: { enqueue: ({ fail }) => Effect.fail(fail("replied", "Remote")) },
        });
        const submission = yield* provider.prepare(request);
        const failure = yield* Effect.flip(submission.submit);
        const op = yield* provider.operation(submission);
        expect(yield* Effect.flip(op.accepted)).toBe(failure);
        expect(CommandFailure.is(yield* Effect.flip(op.ended))).toBe(true);
      }),
    ));

  test("retirement resolves what evidence did not decide as Indeterminate", () =>
    run(
      Effect.gen(function* () {
        const providerScope = yield* Scope.make();
        const { provider } = yield* setup().pipe(Scope.provide(providerScope));
        const submission = yield* provider.prepare(request);
        yield* submission.submit;
        const op = yield* provider.operation(submission);
        yield* Scope.close(providerScope, Exit.void);
        const ended = yield* Effect.flip(op.ended);
        expect(ReactorError.is(ended) && ended.reason._tag).toBe("Indeterminate");
        expect((yield* op.facts).indeterminate).toBe(true);
      }),
    ));

  test("a table full of unresolved operations refuses a commit before sending it", () =>
    runFlowing(
      Effect.gen(function* () {
        const { fake, provider } = yield* setup(unknownEnqueue, { maxOperations: 1 });
        const first = yield* provider.prepare(request);
        yield* Effect.exit(first.submit);
        const second = yield* provider.prepare(request);
        const refused = yield* Effect.flip(second.submit);
        expect(refused).toMatchObject({
          reason: { _tag: "Overflow" },
          context: { outcome: "not-submitted" },
        });
        expect(fake.calls.filter((call) => call.command === "enqueue")).toHaveLength(1);
      }),
    ));

  test("releasing an operation's scope acknowledges it and frees its slot", () =>
    runFlowing(
      Effect.gen(function* () {
        const { fake, provider } = yield* setup(unknownEnqueue, { maxOperations: 1 });
        const first = yield* provider.prepare(request);
        yield* Effect.exit(first.submit);
        yield* Effect.scoped(provider.operation(first));
        const again = yield* Effect.result(provider.operation(first));
        expect(Result.isFailure(again) && again.failure.reason._tag).toBe("InvalidState");
        const second = yield* provider.prepare(request);
        yield* Effect.exit(second.submit);
        expect(fake.calls.filter((call) => call.command === "enqueue")).toHaveLength(2);
      }),
    ));
});
