/**
 * Every check and every failure path, rehearsed on ReactorTest through the
 * same program a paid run uses. Nothing here reaches the network.
 */
import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, layer } from "@effect/vitest";
import { Duration, Effect, FileSystem, Layer } from "effect";
import { ReactorTest } from "reactor-effect-client";
import type { Evidence } from "../Evidence.js";
import { cleanupInstructions } from "../Evidence.js";
import { execute } from "../Qualify.js";
import type { Check } from "../Spend.js";
import { ceilingFor, maxTotalUsd } from "../Spend.js";
import * as Target from "../Target.js";

const rehearse = (
  name: string,
  input: {
    readonly check: Check;
    readonly faults?: ReadonlyArray<ReactorTest.Fault>;
    readonly judge: (evidence: Evidence) => void;
  },
) =>
  layer(
    Target.rehearsal({
      faults: input.faults ?? [],
      candidate: input.check === "turn" ? "relay" : "host",
    }).pipe(Layer.provideMerge(NodeServices.layer)),
  )(name, (it) =>
    it.effect(name, () =>
      Effect.gen(function* () {
        yield* ReactorTest.flow().pipe(Effect.forkScoped);
        const fs = yield* FileSystem.FileSystem;
        const evidence = yield* execute({
          authorization: {
            check: input.check,
            budgetUsd: ceilingFor(input.check),
            totalUsd: maxTotalUsd,
          },
          ledger: yield* fs.makeTempDirectoryScoped({ prefix: "rehearsal-test-" }),
        });
        input.judge(evidence);
      }),
    ),
  );

const passes = (evidence: Evidence) =>
  assert.strictEqual(evidence.verdict, "pass", evidence.reasons.join("; "));
const failed = (criterion: string) => (evidence: Evidence) => {
  assert.strictEqual(evidence.verdict, "fail");
  assert.isFalse(
    evidence.criteria.find((judged) => judged.name === criterion)?.passed ?? true,
    evidence.reasons.join("; "),
  );
};

for (const check of ["vertical", "turn", "audio", "takeover", "queue", "renewal", "cut"] as const)
  rehearse(`${check} passes`, { check, judge: passes });

// A resumed source's first state names no playing clip: H3Source.resume knows only the
// clips its own provider has seen, and H3 keeps no history of the one playing.
rehearse("resume identifies no playing clip yet", {
  check: "resume",
  judge: (evidence) => {
    failed("clip identified")(evidence);
    assert.deepStrictEqual(
      evidence.criteria.filter((criterion) => !criterion.passed).map((criterion) => criterion.name),
      ["clip identified"],
    );
  },
});

// Playout refuses an insert after a clip that has started, which 0.7.0 took as the next boundary.
rehearse("edits cannot insert after the playing clip yet", {
  check: "edits",
  judge: (evidence) => {
    assert.strictEqual(evidence.verdict, "fail");
    assert.isTrue(
      evidence.reasons.some((reason) => reason.includes("the anchor is not waiting to air")),
      evidence.reasons.join("; "),
    );
  },
});

rehearse("a lost enqueue reply stops the check", {
  check: "vertical",
  faults: [{ _tag: "DropReply", command: "enqueue" }],
  judge: (evidence) => {
    assert.strictEqual(evidence.verdict, "fail");
    assert.include(evidence.outcomes, "unknown");
  },
});

rehearse("a termination confirmed only after the library's read stays unconfirmed", {
  check: "vertical",
  faults: [{ _tag: "SlowDelete", for: Duration.seconds(5) }],
  judge: (evidence) => {
    assert.strictEqual(evidence.verdict, "fail");
    const session = evidence.sessions[0];
    assert.isFalse(session?.close?.confirmed ?? true);
    assert.isDefined(session?.terminalMs, "the trail shows when it ended");
    assert.lengthOf(cleanupInstructions(evidence), 1);
  },
});

rehearse("black video fails", {
  check: "vertical",
  faults: [{ _tag: "Video", video: "black" }],
  judge: failed("live video"),
});

rehearse("frozen video fails", {
  check: "vertical",
  faults: [{ _tag: "Video", video: "frozen" }],
  judge: failed("live video"),
});

rehearse("missing audio fails", {
  check: "vertical",
  faults: [{ _tag: "NoAudio" }],
  judge: failed("audio when offered"),
});

rehearse("an over-granting token is refused before anything is allocated", {
  check: "vertical",
  faults: [{ _tag: "OverGrant" }],
  judge: (evidence) => {
    assert.strictEqual(evidence.verdict, "fail");
    assert.lengthOf(evidence.grants, 0);
    assert.lengthOf(evidence.sessions, 0);
  },
});
