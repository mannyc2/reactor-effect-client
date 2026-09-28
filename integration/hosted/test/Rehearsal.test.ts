/**
 * Every check and every failure path, rehearsed on ReactorTest through the
 * same program a paid run uses. Nothing here reaches the network.
 */
import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, layer } from "@effect/vitest";
import { Duration, Effect, FileSystem, Layer, Path, Redacted } from "effect";
import { ReactorTest } from "reactor-effect-client";
import type { Evidence } from "../Evidence.js";
import { cleanupInstructions } from "../Evidence.js";
import { execute, staleBuild } from "../Qualify.js";
import type { Check } from "../Spend.js";
import { ceilingFor, checks, maxTotalUsd } from "../Spend.js";
import * as Target from "../Target.js";

const rehearse = (
  name: string,
  input: {
    readonly check: Check;
    readonly faults?: ReadonlyArray<ReactorTest.Fault>;
    readonly moderationPrompt?: string;
    readonly adoptAfterMs?: number;
    readonly recorder?: boolean;
    readonly judge: (evidence: Evidence) => void;
  },
) =>
  layer(
    Target.rehearsal({
      faults: input.faults ?? [],
      candidate: input.check === "turn" ? "relay" : "host",
      moderationPrompt:
        input.moderationPrompt === undefined ? undefined : Redacted.make(input.moderationPrompt),
      adoptAfterMs: input.adoptAfterMs,
      recorder: input.recorder,
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

for (const check of checks) rehearse(`${check} passes`, { check, judge: passes });

// Paid run tokens 83d17eb7: hosted Reactor read INACTIVE 9 s after the owner was killed, and the
// session was still there. Reactor ends a session 30 s after its last connection drops.
for (const check of ["takeover", "resume", "tokens"] as const) {
  rehearse(`${check} adopts a session left without a connection for 9 s`, {
    check,
    adoptAfterMs: 9_000,
    judge: (evidence) => {
      passes(evidence);
      const states = (evidence.adopterReads ?? []).map((read) => read.state);
      assert.include(states, "INACTIVE", JSON.stringify(evidence.adopterReads));
    },
  });
  rehearse(`${check} fails cleanly when its adopter comes 31 s after the owner died`, {
    check,
    adoptAfterMs: 31_000,
    judge: (evidence) => {
      assert.strictEqual(evidence.verdict, "fail");
      // A bind must name an open session (authentication), so the adopter's mint is refused.
      assert.isTrue(
        evidence.reasons.some(
          (reason) => reason.includes("HTTP 403") || reason.includes("TerminalSession"),
        ),
        evidence.reasons.join("; "),
      );
      assert.isTrue(
        evidence.sessions.every((session) => session.close?.termination?.confirmed === true),
        JSON.stringify(evidence.sessions.map((session) => session.close)),
      );
    },
  });
}

// Paid run tokens 7bc779d4 adopted after the owner's first clip had ended: the
// clip queued behind it was playing, and it is the owner's too.
rehearse("tokens names the owner's queued clip when the first has ended", {
  check: "tokens",
  adoptAfterMs: 14_000,
  judge: (evidence) => {
    // So late an adoption leaves no time to refresh inside the work budget; only
    // the clip is judged here.
    assert.isTrue(
      evidence.criteria.find((judged) => judged.name === "clip identified")?.passed,
      evidence.reasons.join("; "),
    );
    const tokens = evidence.tokens;
    assert.isDefined(tokens?.ownerClipIds);
    assert.strictEqual(tokens.playingClipId, tokens.ownerClipIds.queued);
  },
});

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

// Reactor leaves recording to each deployment, so the paid tour may never download one.
rehearse("tour downloads the recording clip when the deployment records", {
  check: "tour",
  recorder: true,
  judge: (evidence) => {
    passes(evidence);
    const [clip] = evidence.tour?.recordings ?? [];
    assert.deepStrictEqual(
      [clip?.request, clip?.outcome, clip?.download?.outcome],
      ["clip", "ClipReady", "downloaded"],
    );
    assert.isAbove(clip?.download?.segments ?? 0, 0);
  },
});

// A phase that fails is a failed criterion, and the phases after it still run: the session
// still refreshes its token, reconnects after the creating token expired, and ends.
rehearse("tour goes on after clip 1 is refused, and still ends its session", {
  check: "tour",
  faults: [{ _tag: "InvalidImage", nth: 1 }],
  judge: (evidence) => {
    assert.strictEqual(evidence.verdict, "fail");
    const passed = (name: string) =>
      evidence.criteria.find((criterion) => criterion.name === name)?.passed;
    assert.deepStrictEqual(
      [
        passed("clip 1"),
        passed("the session refreshes to a token bound to itself before its token expires"),
        passed("a call after the creating token expired succeeds"),
        passed("the reconnect makes the next generation"),
        passed("the API key ends the session"),
        passed("attaching to the ended session is refused"),
        passed("confirmed termination"),
      ],
      [false, true, true, true, true, true, true],
      evidence.reasons.join("; "),
    );
  },
});

// The refresh runs on its own: clip 1's build never finishes, which holds every phase after it
// up past the refresh point, and the session still refreshes its token in time.
rehearse("tour refreshes its token on time while a stalled build holds up its phases", {
  check: "tour",
  faults: [{ _tag: "StallBuild", nth: 1 }],
  judge: (evidence) => {
    const passed = (name: string) =>
      evidence.criteria.find((criterion) => criterion.name === name)?.passed;
    const heldUntil = evidence.milestones.find(
      (milestone) => milestone.step === "clip 1 on air failed",
    )?.atMs;
    const tour = evidence.tour;
    assert.deepStrictEqual(
      [
        passed("clip 1 on air"),
        passed("the session refreshes to a token bound to itself before its token expires"),
      ],
      [false, true],
      evidence.reasons.join("; "),
    );
    assert.isBelow(tour?.refreshDueMs ?? Infinity, heldUntil ?? 0);
    assert.isBelow(tour?.refreshCall?.startedMs ?? Infinity, tour?.createExpiresMs ?? 0);
  },
});

// A session whose end was not confirmed may still be live: nothing probes it as ended, and the
// check's cleanup ends it.
rehearse("tour probes nothing after an end it could not confirm", {
  check: "tour",
  faults: [{ _tag: "IgnoreDelete" }],
  judge: (evidence) => {
    const passed = (name: string) =>
      evidence.criteria.find((criterion) => criterion.name === name)?.passed;
    assert.deepStrictEqual(
      [passed("the API key ends the session"), passed("after the end")],
      [false, false],
      evidence.reasons.join("; "),
    );
    assert.isUndefined(evidence.tour?.afterEnd);
    assert.isFalse(evidence.sessions[0]?.close?.confirmed ?? true);
  },
});

// A phase that fails is a failed criterion, and the phases after it still run: the refusals are
// still read, and closing the resumed source still ends the session.
rehearse("adoption records a refused clip and still ends the session it resumed", {
  check: "adoption",
  faults: [{ _tag: "InvalidImage" }],
  judge: (evidence) => {
    assert.strictEqual(evidence.verdict, "fail");
    const passed = (name: string) =>
      evidence.criteria.find((criterion) => criterion.name === name)?.passed;
    assert.deepStrictEqual(
      [
        passed("the refreshed clip"),
        passed("an expired token is refused"),
        passed("an unbound token is refused"),
        passed("closing the resumed source ends the session"),
        passed("confirmed termination"),
      ],
      [false, true, true, true, true],
      evidence.reasons.join("; "),
    );
  },
});

// The owner allocates and then cannot connect, so it never streams: the session it allocated
// must still be known, and ended with the key, rather than left to run to its cap.
rehearse("adoption ends the session of an owner that fails before it streams", {
  check: "adoption",
  faults: [{ _tag: "RefuseConnect" }],
  judge: (evidence) => {
    assert.strictEqual(evidence.verdict, "fail");
    assert.strictEqual(evidence.sessions.length, 1);
    assert.isTrue(evidence.sessions[0]?.close?.confirmed, evidence.reasons.join("; "));
    assert.isTrue(
      evidence.criteria.find((criterion) => criterion.name === "confirmed termination")?.passed,
      evidence.reasons.join("; "),
    );
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

// Without a prompt, `show passes` above ends the second session with the API key.
rehearse("show loses its second session to a moderation verdict, and goes on", {
  check: "show",
  moderationPrompt: flagged,
  faults: [{ _tag: "Moderate", prompt: flagged }],
  judge: (evidence) => {
    passes(evidence);
    assert.deepStrictEqual(
      [evidence.show?.loss?.by, evidence.moderation?.verdict?.action, evidence.sessions.length],
      ["moderation", "terminate", 3],
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

layer(NodeServices.layer)("a build older than its sources", (it) => {
  it.effect(
    "counts a package built before its sources changed as stale, and one installed as built",
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const fs = yield* FileSystem.FileSystem;
          const path = yield* Path.Path;
          const directory = yield* fs.makeTempDirectoryScoped({ prefix: "stale-build-test-" });
          const built = path.join(directory, "dist", "internal", "a.js");
          const source = path.join(directory, "src", "internal", "a.ts");
          yield* fs.makeDirectory(path.dirname(built), { recursive: true });
          yield* fs.writeFileString(built, "");
          assert.isFalse(yield* staleBuild(directory), "without sources, as installed");
          yield* fs.makeDirectory(path.dirname(source), { recursive: true });
          yield* fs.writeFileString(source, "");
          yield* fs.utimes(source, 1_000, 1_000);
          yield* fs.utimes(built, 2_000, 2_000);
          assert.isFalse(yield* staleBuild(directory), "built after its sources");
          yield* fs.utimes(source, 3_000, 3_000);
          assert.isTrue(yield* staleBuild(directory), "a source changed after the build");
        }),
      ),
  );
});
