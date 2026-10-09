/** The requested candidate check uses the paid runner and real ReactorTest boundary. */
import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, layer } from "@effect/vitest";
import { Effect, FileSystem, Layer, Schema } from "effect";
import { ReactorTest } from "reactor-effect-client";
import { execute } from "../Qualify.js";
import { Check } from "../Spend.js";
import * as Target from "../Target.js";

const candidate = (faults: ReadonlyArray<ReactorTest.Fault> = []) =>
  Target.rehearsal({ faults, candidate: "host" }).pipe(Layer.provideMerge(NodeServices.layer));

const run = Effect.gen(function* () {
  const check = yield* Schema.decodeEffect(Check)("candidate");
  yield* ReactorTest.flow().pipe(Effect.forkScoped);
  const fs = yield* FileSystem.FileSystem;
  const ledger = yield* fs.makeTempDirectoryScoped({ prefix: "candidate-test-" });
  return yield* execute({
    authorization: { check, budgetUsd: 8.4, totalUsd: 8.4 },
    ledger,
  });
});

layer(candidate())("candidate", (it) => {
  it.effect("qualifies both models in one capped run", () =>
    Effect.gen(function* () {
      const evidence = yield* run;
      assert.strictEqual(evidence.verdict, "pass", evidence.reasons.join("; "));
      assert.lengthOf(evidence.sessions, 2);
      assert.isTrue(evidence.sessions.every((session) => session.close?.confirmed === true));
    }),
  );
});

layer(candidate([{ _tag: "Video", video: "black" }]))("candidate media failure", (it) => {
  it.effect("ends its first session before refusing to allocate the second", () =>
    Effect.gen(function* () {
      const evidence = yield* run;
      assert.strictEqual(evidence.verdict, "fail");
      assert.lengthOf(evidence.sessions, 1);
      assert.isTrue(evidence.sessions[0]?.close?.confirmed);
    }),
  );
});

layer(candidate([{ _tag: "IgnoreDelete" }]))("candidate unconfirmed end", (it) => {
  it.effect("keeps an unconfirmed first end and allocates no second session", () =>
    Effect.gen(function* () {
      const evidence = yield* run;
      assert.strictEqual(evidence.verdict, "fail");
      assert.lengthOf(evidence.sessions, 1);
      assert.isFalse(evidence.sessions[0]?.close?.confirmed);
    }),
  );
});

layer(candidate([{ _tag: "RefuseConnect" }]))("candidate partial acquisition", (it) => {
  it.effect("records the cleanup report of a session that never connected", () =>
    Effect.gen(function* () {
      const evidence = yield* run;
      assert.strictEqual(evidence.verdict, "fail");
      assert.lengthOf(evidence.sessions, 1);
      assert.isTrue(evidence.sessions[0]?.close?.confirmed);
    }),
  );
});

layer(candidate())("candidate absent end", (it) => {
  it.effect("independently confirms a gone session only after two consecutive reads", () =>
    Effect.gen(function* () {
      const test = yield* ReactorTest.ReactorTest;
      yield* Effect.gen(function* () {
        while (!(yield* test.log).some((entry) => entry.name.startsWith("DELETE ")))
          yield* Effect.sleep("5 millis");
        // The source's confirmation reads first; the candidate's two independent reads follow.
        yield* test.inject({ _tag: "MissingSession", nth: 2 });
        yield* test.inject({ _tag: "MissingSession", nth: 3 });
      }).pipe(Effect.forkScoped);
      const evidence = yield* run;
      assert.strictEqual(evidence.verdict, "pass", evidence.reasons.join("; "));
      assert.lengthOf(evidence.sessions, 2);
      assert.deepStrictEqual(
        evidence.candidate?.phases[0]?.endReads.map((read) => read.state),
        ["absent", "absent"],
      );
    }),
  );
});
