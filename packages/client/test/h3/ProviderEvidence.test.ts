import { expect, test } from "vitest";
import { Effect, Result } from "effect";
import { ReactorError } from "../../src/errors.js";
import * as H3 from "../../src/h3/index.js";
import { CommandFailure } from "../../src/session/commands.js";
import { fixture, fixtureClip, textArg } from "./ProviderSession.js";
import { runFlowing } from "./Clock.js";

const options: H3.Options = { replyTimeout: 100, setupTimeout: 1000, reconcileWindow: 20 };
const observed = (provider: H3.Provider, revision: bigint) =>
  Effect.gen(function* () {
    while ((yield* provider.current).revision < revision) yield* Effect.yieldNow;
  }).pipe(Effect.timeout(1000));
const failure = <A>(result: Result.Result<A, CommandFailure>): CommandFailure => {
  expect(Result.isFailure(result)).toBe(true);
  if (Result.isSuccess(result)) throw new Error("Expected uncertain dispatch evidence");
  return result.failure;
};

test("matching submission fields still require the exact captured prompt and metadata bytes", () =>
  runFlowing(
    Effect.gen(function* () {
      const fake = yield* fixture({
        command: {
          enqueue: ({ fake, call }) =>
            Effect.gen(function* () {
              const clip = fixtureClip({
                prompt: textArg(call.args.prompt),
                metadata: textArg(call.args.metadata),
              });
              const annotation = JSON.parse(clip.metadata) as Record<string, unknown>;
              yield* fake.emit("clip_queued", { clip: { ...clip, prompt: "changed prompt" } });
              // Same parsed values, different string identity: metadata must be echoed exactly.
              const reordered = JSON.stringify({
                caller: annotation.caller,
                submission: annotation.submission,
                namespace: annotation.namespace,
                reactor_effect_h3: annotation.reactor_effect_h3,
              });
              yield* fake.emit("clip_queued", {
                clip: { ...clip, clip_id: fixtureClip().clip_id, metadata: reordered },
              });
              yield* fake.emit("clip_generated", {
                clip: { ...clip, metadata: JSON.stringify({ ...annotation, extra: true }) },
              });
              return { type: "clip_queued", data: { clip: { ...clip } } };
            }),
        },
      });
      const provider = yield* H3.make(fake.session, options);
      const prepared = yield* provider.prepare({
        prompt: "captured prompt",
        metadata: "opaque caller",
      });
      const acceptance = yield* prepared.submit;
      expect(acceptance.submissionId).toBe(prepared.id);
      expect(acceptance.clip.prompt).toBe("captured prompt");
      expect(acceptance.clip.metadata).toBe(textArg(fake.calls[2]!.args.metadata));
      expect(acceptance.evidence.kind).toBe("correlated");
      expect(acceptance.evidence.source).toBe(fake.returns[2]!);
      expect(yield* provider.acceptances).toEqual([acceptance]);
      expect(yield* prepared.submit).toBe(acceptance);
      expect(fake.calls.filter((call) => call.command === "enqueue")).toHaveLength(1);
    }),
  ));

test("expired acceptance tokens cannot be revived by late matching clips and release their pending capacity", () =>
  runFlowing(
    Effect.gen(function* () {
      const fake = yield* fixture({ command: { enqueue: () => Effect.undefined } });
      const provider = yield* H3.make(fake.session, { ...options, maxPending: 1 });
      const prepared = yield* provider.prepare({ prompt: "expired request" });
      const first = failure(yield* Effect.result(prepared.submit));
      expect(first.context.outcome).toBe("unknown");
      const call = fake.calls[2]!;
      const clip = fixtureClip({
        prompt: textArg(call.args.prompt),
        metadata: textArg(call.args.metadata),
      });
      const source = yield* fake.emit(
        "clip_generated",
        { clip: { ...clip } },
        { requestId: call.requestId, correlation: "late" },
      );
      yield* observed(provider, source.sequence);
      expect((yield* provider.current).clips.map((entry) => entry.clip.clip_id)).toContain(
        clip.clip_id,
      );
      expect(yield* provider.acceptance(prepared.id)).toBeUndefined();
      expect(yield* provider.acceptances).toEqual([]);
      expect(failure(yield* Effect.result(prepared.submit))).toBe(first);
      expect(fake.calls.filter((entry) => entry.command === "enqueue")).toHaveLength(1);
      yield* provider.refresh;
      const next = yield* provider.prepare({ prompt: "next request" });
      const second = failure(yield* Effect.result(next.submit));
      expect(second.context.outcome).toBe("unknown");
      expect(second.reason._tag).toBe("UnexpectedReply");
      expect(next.id).not.toBe(prepared.id);
      expect(fake.calls.filter((entry) => entry.command === "enqueue")).toHaveLength(2);
      expect(yield* provider.acceptances).toEqual([]);
    }),
  ));

test("bounded reconciliation retains the exact unknown failure, original code and structured cause", () =>
  runFlowing(
    Effect.gen(function* () {
      const cause = new Error("independent transport failure");
      let original: CommandFailure | undefined;
      const fake = yield* fixture({
        command: {
          enqueue: ({ call }) => {
            original = CommandFailure.from(ReactorError.fromCode("Native", "fixture uncertainty"), {
              operation: "enqueue",
              outcome: "unknown",
              requestId: call.requestId,
              generation: call.generation,
              detail: cause,
            });
            return Effect.fail(original);
          },
        },
      });
      const provider = yield* H3.make(fake.session, options);
      const prepared = yield* provider.prepare({ prompt: "one uncertain dispatch" });
      const error = failure(yield* Effect.result(prepared.submit));
      expect(error).toBe(original!);
      expect(error.reason._tag).toBe("Native");
      expect(error.context.detail).toBe(cause);
      expect(error.context).toMatchObject({
        operation: "enqueue",
        outcome: "unknown",
        requestId: fake.calls[2]!.requestId,
        generation: 1n,
      });
      expect(failure(yield* Effect.result(prepared.submit))).toBe(error);
      expect(fake.calls.filter((call) => call.command === "enqueue")).toHaveLength(1);
      expect(yield* provider.acceptances).toEqual([]);
    }),
  ));
