/**
 * Every check and every failure path, rehearsed on ReactorTest through the
 * same program a paid run uses. Nothing here reaches the network.
 */
import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, layer } from "@effect/vitest";
import { Duration, Effect, FileSystem, Layer, Redacted } from "effect";
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
    readonly moderationPrompt?: string;
    readonly judge: (evidence: Evidence) => void;
  },
) =>
  layer(
    Target.rehearsal({
      faults: input.faults ?? [],
      candidate: input.check === "turn" ? "relay" : "host",
      moderationPrompt:
        input.moderationPrompt === undefined ? undefined : Redacted.make(input.moderationPrompt),
    }).pipe(Layer.provideMerge(NodeServices.layer)),
  )(name, (it) =>
    it.effect(
      name,
      () =>
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
      // A rehearsal plays a whole session at a moving test clock: moments, or more under load.
      60_000,
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

for (const check of [
  "vertical",
  "turn",
  "audio",
  "takeover",
  "resume",
  "queue",
  "renewal",
  "edits",
  "cut",
  "tokens",
] as const)
  rehearse(`${check} passes`, { check, judge: passes });

rehearse("tokens records the free probes and the documented refusals", {
  check: "tokens",
  judge: (evidence) => {
    passes(evidence);
    const tokens = evidence.tokens;
    assert.deepStrictEqual(
      tokens?.probes.map((probe) => probe.status),
      [200, 200, 200, 200, 403, 404, 404],
    );
    // ReactorTest clamps a 7 h token to six hours, as Reactor documents; `expires_at` is whole
    // seconds, so the lifetime measured from the request is good to a second.
    assert.closeTo(tokens?.probes[1]?.lifetimeSeconds ?? 0, 21_600, 1);
    // A token is base64url JSON, so it would begin "eyJ": no probe keeps one.
    assert.notInclude(JSON.stringify(tokens?.probes), "eyJ");
    assert.deepStrictEqual(
      [
        tokens?.expiredTokenStatus,
        tokens?.unboundTokenStatus,
        tokens?.apiKeyTermination?.confirmed,
      ],
      [401, 403, true],
    );
    assert.isAtLeast(tokens?.mints.filter((mint) => mint.kind === "bind").length ?? 0, 2);
  },
});

const flagged = "a prompt the rehearsal's moderation flags";

rehearse("cut records a moderation verdict, and the playout ends on it", {
  check: "cut",
  moderationPrompt: flagged,
  faults: [{ _tag: "Moderate", prompt: flagged }],
  judge: (evidence) => {
    passes(evidence);
    const moderation = evidence.moderation;
    assert.deepStrictEqual(
      [moderation?.flagged, moderation?.aired, moderation?.verdict?.action],
      [true, false, "terminate"],
    );
    assert.include(moderation?.statuses.at(-1)?.detail ?? "", "moderated");
    assert.isTrue(moderation?.playout.some((entry) => entry.event.startsWith("failed")));
  },
});

rehearse("cut records a session ended with no verdict, and opens no second one", {
  check: "cut",
  moderationPrompt: flagged,
  faults: [{ _tag: "Moderate", prompt: flagged, verdict: false }],
  judge: (evidence) => {
    passes(evidence);
    assert.deepStrictEqual(
      [evidence.moderation?.flagged, evidence.moderation?.verdict, evidence.sessions.length],
      [true, undefined, 1],
    );
  },
});

rehearse("cut withdraws a flagged item moderation let through before it airs", {
  check: "cut",
  moderationPrompt: flagged,
  // Screening flags some other prompt, never this one.
  faults: [{ _tag: "Moderate", prompt: "another prompt" }],
  judge: (evidence) => {
    passes(evidence);
    assert.deepStrictEqual(
      [evidence.moderation?.flagged, evidence.moderation?.aired],
      [false, false],
    );
    assert.notInclude(evidence.playout?.startOrder ?? [], "flagged");
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
