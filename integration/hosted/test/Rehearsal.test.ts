/**
 * Every check and every failure path, rehearsed on ReactorTest through the
 * same program a paid run uses. Nothing here reaches the network.
 */
// Vitest decides which suites to run while it collects them, synchronously, so whether ffmpeg
// is on the PATH, and what a reel holds, are asked with a synchronous child process.
// @effect-diagnostics-next-line nodeBuiltinImport:off
import { spawnSync } from "node:child_process";
// @effect-diagnostics-next-line nodeBuiltinImport:off
import { readdirSync } from "node:fs";
// @effect-diagnostics-next-line nodeBuiltinImport:off
import { join } from "node:path";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, layer } from "@effect/vitest";
import { Duration, Effect, FileSystem, Layer, Path, Redacted } from "effect";
import { ReactorTest } from "reactor-effect-client";
import type { Evidence } from "../Evidence.js";
import { cleanupInstructions } from "../Evidence.js";
import { execute, staleBuild } from "../Qualify.js";
import type { Check } from "../Spend.js";
import { ceilingFor, checks, holdsFor, maxTotalUsd } from "../Spend.js";
import { summarize } from "../Summary.js";
import * as Target from "../Target.js";

const rehearse = (
  name: string,
  input: {
    readonly check: Check;
    readonly faults?: ReadonlyArray<ReactorTest.Fault>;
    readonly moderationPrompt?: string;
    readonly adoptAfterMs?: number;
    readonly recorder?: boolean;
    readonly timing?: ReactorTest.Timing;
    readonly judge: (evidence: Evidence, ledger: string) => void;
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
      timing: input.timing,
    }).pipe(Layer.provideMerge(NodeServices.layer)),
  )(name, (it) =>
    it.effect(
      name,
      () =>
        Effect.gen(function* () {
          yield* ReactorTest.flow().pipe(Effect.forkScoped);
          const fs = yield* FileSystem.FileSystem;
          const ledger = yield* fs.makeTempDirectoryScoped({ prefix: "rehearsal-test-" });
          const evidence = yield* execute({
            authorization: {
              check: input.check,
              budgetUsd: ceilingFor(input.check),
              totalUsd: maxTotalUsd,
            },
            ledger,
          });
          input.judge(evidence, ledger);
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

const hasFfprobe = spawnSync("ffprobe", ["-version"]).status === 0;

// The rehearsal's simulated picture and sound go through a real ffmpeg wherever one can record
// them; CI's runtime jobs have none, and the check then says why it recorded nothing.
rehearse("showreel records its reel, poster and loop when ffmpeg can", {
  check: "showreel",
  judge: (evidence, ledger) => {
    passes(evidence);
    const showreel = evidence.showreel;
    if (showreel?.notRecorded !== undefined) return;
    assert.deepStrictEqual(
      showreel?.files.map((file) => file.name),
      ["reel.mp4", "poster.png", "loop.gif"],
    );
    const span = showreel?.reel;
    assert.closeTo(
      ((showreel?.recording?.frames ?? 0) * 1000) / 24,
      (span?.toMs ?? 0) - (span?.fromMs ?? 0),
      1000 / 24,
    );
    if (!hasFfprobe) return;
    const directory = readdirSync(ledger).find((name) => name.endsWith(evidence.runId));
    assert.isDefined(directory);
    const counted = spawnSync("ffprobe", [
      ...["-v", "error", "-count_frames", "-select_streams", "v:0"],
      ...["-show_entries", "stream=nb_read_frames", "-of", "csv=p=0"],
      join(ledger, directory, "reel.mp4"),
    ]);
    // The file holds every frame the recorder wrote.
    assert.strictEqual(Number(counted.stdout.toString().trim()), showreel?.recording?.frames);
  },
});

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

// The opening scene's build fails, so it never starts: the check must judge that and close its
// session, rather than wait for the start with no deadline while the session runs to its cap.
rehearse("showreel fails, and ends its session, when its first scene never starts", {
  check: "showreel",
  faults: [{ _tag: "FailBuild", nth: 1 }],
  judge: (evidence) => {
    assert.strictEqual(evidence.verdict, "fail");
    assert.isFalse(
      evidence.criteria.find(
        (criterion) => criterion.name === "every scene accepted, built, started and ended",
      )?.passed ?? true,
      evidence.reasons.join("; "),
    );
    assert.lengthOf(evidence.sessions, 1);
    assert.isTrue(evidence.sessions[0]?.close?.confirmed, evidence.reasons.join("; "));
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

// ReactorTest ends a session at its cap, counted from ACTIVE, whether or not anything connected,
// and refuses a spent single-session token 403 session_limit. Whether hosted Reactor does either
// is what unconnected asks.
rehearse("unconnected follows a session nothing connected to until its cap ends it", {
  check: "unconnected",
  judge: (evidence) => {
    passes(evidence);
    const probe = evidence.unconnected;
    const spent = probe?.spentToken;
    const [session, spending] = evidence.sessions;
    assert.deepStrictEqual(
      probe?.states.slice(-2).map((entry) => entry.state),
      ["ACTIVE", "CLOSED"],
    );
    assert.strictEqual(probe?.ended?.by, "reactor");
    assert.isUndefined(probe?.unanswered);
    // The cap ends it 60 s after it went ACTIVE, half a second at most after the reply, and a
    // read every 2 s finds that end.
    const endedAfter = (probe?.ended?.atMs ?? 0) - (session?.allocatedMs ?? 0);
    assert.isAtLeast(endedAfter, 60_000);
    assert.isAtMost(endedAfter, 62_500);
    assert.isDefined(probe?.connectableMs);
    // A token each: the second allocates one session, which spends it, and nothing more.
    assert.lengthOf(evidence.grants, 2);
    assert.lengthOf(evidence.sessions, 2);
    assert.deepStrictEqual([spent?.create?.answer, spent?.sessionId], ["allocated", spending?.id]);
    const second = spent?.second;
    assert.deepStrictEqual(
      [second?.answer, second?.outcome, second?.status, second?.codes?.["error.code"]],
      ["Http", "replied", 403, "session_limit"],
    );
    // After the second create, the key reads the spent token's session until it is ready, and
    // ends it 10 s later, well within its hold.
    const held = spent?.held?.[0];
    assert.strictEqual(held?.sessionId, spending?.id);
    assert.isAtLeast(held?.states[0]?.firstMs ?? 0, second?.answeredMs ?? Infinity);
    assert.isTrue(spending?.close?.termination?.confirmed);
    const heldMs = spending.close.requestedMs - (held?.connectableMs ?? Infinity);
    assert.isAtLeast(heldMs, 10_000);
    assert.isBelow(heldMs, 10_100);
    assert.isAtMost(
      spending.close.reportedMs - (spent?.requestedMs ?? 0),
      (holdsFor("unconnected")[1] ?? 0) * 1000,
    );
    // The time the summary gives is the refusal's.
    assert.match(
      summarize([evidence]),
      /\*\*Spent token:\*\* its first create allocated \S+; a second create failed in \d+\.\d\d s with Http 403, outcome replied;/,
    );
    assert.match(
      summarize([evidence]),
      /> CLOSED from [\d.]+ s after allocation to [\d.]+ s after allocation \(1 read\)$/m,
    );
    assert.isTrue(session?.close?.termination?.confirmed);
  },
});

rehearse("unconnected ends with the key a session its cap did not end", {
  check: "unconnected",
  faults: [{ _tag: "IgnoreCap" }],
  judge: (evidence) => {
    passes(evidence);
    const probe = evidence.unconnected;
    const session = evidence.sessions[0];
    assert.deepStrictEqual(probe?.states.at(-1)?.state, "ACTIVE");
    assert.strictEqual(probe?.ended?.by, "key");
    // The window ends 120 s after the request: 15 s for allocation, ACTIVE and ready, the cap,
    // 30 s more and a 15 s margin. It ends past the cap and 30 s from each by 15 s at least. The
    // check adds those to a time with a fraction of a millisecond, which a loaded machine's test
    // clock reaches, so the difference can miss 120 s by a rounding error.
    const windowEndsMs = probe?.windowEndsMs ?? 0;
    assert.closeTo(windowEndsMs - (probe?.requestedMs ?? 0), 120_000, 0.001);
    const active = probe?.states.find((entry) => entry.state === "ACTIVE")?.firstMs;
    for (const startMs of [session?.allocatedMs, active, probe?.connectableMs])
      assert.isAtLeast(windowEndsMs - (startMs ?? Infinity) - 90_000, 15_000);
    // Its last read came at the window's end, and the key's end right after: within the 9 s of
    // the last read and the read at the end, and within the 155 s the session is reserved for.
    assert.isAtLeast(probe?.states.at(-1)?.lastMs ?? 0, windowEndsMs);
    assert.isAtLeast(session?.close?.requestedMs ?? 0, windowEndsMs);
    assert.isAtMost((session?.close?.requestedMs ?? Infinity) - windowEndsMs, 9_000);
    assert.isAtMost(
      (session?.close?.reportedMs ?? Infinity) - (probe?.requestedMs ?? 0),
      (holdsFor("unconnected")[0] ?? 0) * 1000,
    );
    assert.isTrue(session?.close?.termination?.confirmed);
    assert.match(
      summarize([evidence]),
      /^- \*\*Window:\*\* reads until 120\.00 s after the request, past the cap and 30 s by \d+\.\d\d s from allocation, \d+\.\d\d s from ACTIVE and \d+\.\d\d s from ready$/m,
    );
  },
});

rehearse("unconnected ends together the spent token's session and one its second create made", {
  check: "unconnected",
  faults: [{ _tag: "IgnoreSessionLimit" }],
  judge: (evidence) => {
    passes(evidence);
    const probe = evidence.unconnected;
    const spent = probe?.spentToken;
    const [session, spending, extra] = evidence.sessions;
    assert.lengthOf(evidence.sessions, 3);
    assert.isDefined(spending);
    assert.isDefined(extra);
    assert.deepStrictEqual(
      [spent?.second?.answer, spent?.second?.sessionId],
      ["allocated", extra.id],
    );
    // Each is held 10 s past its own ready, so neither waits on the other's reads or tries, and
    // each session ends within its hold.
    const [, spendingHold = 0, extraHold = 0] = holdsFor("unconnected");
    for (const [ended, holdSeconds, sentMs] of [
      [spending, spendingHold, spent?.requestedMs],
      [extra, extraHold, spent?.second?.sentMs],
    ] as const) {
      const held = spent?.held?.find((entry) => entry.sessionId === ended.id);
      assert.isTrue(ended.close?.termination?.confirmed);
      const heldMs = ended.close.requestedMs - (held?.connectableMs ?? Infinity);
      assert.isAtLeast(heldMs, 10_000);
      assert.isBelow(heldMs, 10_100);
      assert.isAtMost(ended.close.reportedMs - (sentMs ?? 0), holdSeconds * 1000);
    }
    // The watched session is none of the spent token's: its cap still ends it.
    assert.strictEqual(probe?.ended?.by, "reactor");
    assert.isAtLeast((probe?.ended?.atMs ?? 0) - (session?.allocatedMs ?? 0), 60_000);
    // The time the summary gives is the allocation's; the termination line says how it ended.
    const summary = summarize([evidence]);
    assert.include(summary, `a second create allocated ${extra.id} in `);
    // A row a session for the dashboard's duration and charge, which the maintainer fills in.
    assert.include(
      summary,
      "| Session | Made by | Requested | Allocated | First ACTIVE read | Ready | Held | Ended by | Ended | Allocated to ended | Ready to ended | Dashboard duration | Dashboard charge |",
    );
    const rows = summary.split("\n").flatMap((line) =>
      evidence.sessions.some((held) => line.startsWith(`| ${held.id} |`))
        ? [
            line
              .split("|")
              .slice(1, -1)
              .map((cell) => cell.trim()),
          ]
        : [],
    );
    assert.deepStrictEqual(
      rows.map((cells) => [
        cells[0],
        cells[1],
        cells[4] === "–" || cells[5] === "–",
        cells[6],
        cells[7],
        cells[11],
        cells[12],
      ]),
      [
        [session?.id, "the watched token's create", false, "–", "Reactor", "", ""],
        [
          spending.id,
          "the spent token's first create",
          false,
          "10.00 s past ready",
          "the key",
          "",
          "",
        ],
        [extra.id, "its second create", false, "10.00 s past ready", "the key", "", ""],
      ],
    );
    // A DELETE that found no session came after the session had ended, whatever ended it.
    const gone = {
      ...evidence,
      sessions: evidence.sessions.map((held) =>
        held.id === spending.id && held.close?.termination !== undefined
          ? {
              ...held,
              close: {
                ...held.close,
                termination: { ...held.close.termination, deleteStatus: 404 },
              },
            }
          : held,
      ),
    };
    assert.include(
      summarize([gone]),
      `| ${spending.id} | the spent token's first create | ${rows[1]?.slice(2, 7).join(" | ") ?? "?"} | before the key's DELETE |`,
    );
    // Reactor's end lies between the last read that found the session running and the first
    // that found it ended, 2 s apart; the times count from the watched session's request.
    assert.match(
      summary,
      /^\| \S+ \| the watched token's create \| 0\.00 s \| \d+\.\d\d s \| \d+\.\d\d s \| \d+\.\d\d s \| – \| Reactor \| \d+\.\d\d–\d+\.\d\d s \| 6\d\.\d\d–6\d\.\d\d s \| \d+\.\d\d–\d+\.\d\d s \|  \|  \|$/m,
    );
  },
});

// Nothing ended the sessions, so their cap cannot be counted on to: the cleanup says to end each.
rehearse("unconnected fails, and says to end the session, when the key cannot end it", {
  check: "unconnected",
  faults: [{ _tag: "IgnoreCap" }, { _tag: "IgnoreDelete" }],
  judge: (evidence) => {
    failed("confirmed termination")(evidence);
    const instructions = cleanupInstructions(evidence);
    assert.deepStrictEqual(
      instructions.map(
        (line) => evidence.sessions.find((session) => line.includes(session.id))?.id,
      ),
      evidence.sessions.map((session) => session.id),
    );
    assert.include(instructions[0] ?? "", "may not end it");
  },
});

// A spent token that answers with the session it made allocated nothing more: the key ends that
// session alone, and the watch goes on.
rehearse("unconnected ends only the spent token's session when its second create names it", {
  check: "unconnected",
  faults: [{ _tag: "RepeatSession" }],
  judge: (evidence) => {
    passes(evidence);
    const probe = evidence.unconnected;
    const spent = probe?.spentToken;
    assert.lengthOf(evidence.sessions, 2);
    assert.deepStrictEqual(
      [spent?.second?.answer, spent?.second?.sessionId],
      ["the same session", spent?.sessionId],
    );
    assert.isTrue(evidence.sessions[1]?.close?.termination?.confirmed);
    assert.strictEqual(probe?.ended?.by, "reactor");
    assert.isAtLeast((probe?.ended?.atMs ?? 0) - (evidence.sessions[0]?.allocatedMs ?? 0), 60_000);
    assert.include(
      summarize([evidence]),
      `a second create answered with ${spent?.sessionId ?? "?"}, the session it made, in `,
    );
  },
});

// Nothing here trusts the cap to end a session, so an end the key cannot confirm is tried again.
rehearse("unconnected ends its session with the key again until the end is confirmed", {
  check: "unconnected",
  faults: [{ _tag: "IgnoreCap" }, { _tag: "SlowDelete", for: Duration.seconds(3) }],
  judge: (evidence) => {
    passes(evidence);
    const session = evidence.sessions[0];
    assert.strictEqual(evidence.unconnected?.ended?.by, "key");
    assert.isTrue(session?.close?.termination?.confirmed);
    // Each try the key could not confirm is in the timeline; the close keeps the first DELETE's time.
    const unconfirmed = evidence.milestones.filter(
      (milestone) =>
        milestone.step === "end unconfirmed" &&
        (milestone.detail ?? "").startsWith(`${session.id}:`),
    );
    assert.lengthOf(unconfirmed, 2);
    assert.include(unconfirmed[0]?.detail ?? "", `${session.id}: DELETE 200, then STOPPING`);
    assert.isBelow(session.close.requestedMs, unconfirmed[0]?.atMs ?? 0);
  },
});

// A create whose outcome is unknown may have allocated a session the run never learned of. The
// watched session's create stops the check, before the spent token is used.
rehearse("unconnected says where to look when its create's outcome is unknown", {
  check: "unconnected",
  faults: [{ _tag: "RefuseAllocation", nth: 1, status: 503 }],
  judge: (evidence) => {
    assert.strictEqual(evidence.verdict, "fail");
    assert.lengthOf(evidence.sessions, 0);
    assert.isUndefined(evidence.unconnected?.spentToken);
    const [instruction] = cleanupInstructions(evidence);
    assert.include(instruction ?? "", "may have allocated a session");
    assert.include(instruction ?? "", evidence.unconnected?.requestedAt ?? "no request");
    // Nothing else in the summary may say that no session was allocated.
    const reasons = evidence.reasons.join("; ");
    assert.include(reasons, "an outcome is unknown, so the run fails and is not repeated");
    assert.include(reasons, "confirmed termination: no session was recorded");
    assert.notInclude(reasons, "no session was allocated");
  },
});

// A refused create allocated nothing and spent nothing, so no second create goes out; the watch
// runs on as if the spent token had never been used.
rehearse("unconnected watches on when the spent token's first create is refused", {
  check: "unconnected",
  faults: [{ _tag: "RefuseAllocation", nth: 2 }],
  judge: (evidence) => {
    failed("the spent token's second create was answered")(evidence);
    const probe = evidence.unconnected;
    const spent = probe?.spentToken;
    assert.deepStrictEqual(
      [spent?.create?.answer, spent?.create?.status, spent?.create?.outcome, spent?.sessionId],
      ["Http", 403, "replied", undefined],
    );
    assert.isUndefined(spent?.second);
    assert.include(
      evidence.criteria.find(
        (criterion) => criterion.name === "the spent token's second create was answered",
      )?.detail ?? "",
      "its first create failed with Http 403, outcome replied",
    );
    assert.isTrue(
      evidence.criteria.find((criterion) => criterion.name === "the watch completed")?.passed,
    );
    assert.strictEqual(probe?.ended?.by, "reactor");
    assert.isAtLeast((probe?.ended?.atMs ?? 0) - (evidence.sessions[0]?.allocatedMs ?? 0), 60_000);
    assert.lengthOf(evidence.sessions, 1);
    assert.isTrue(evidence.sessions[0]?.close?.termination?.confirmed);
    assert.deepStrictEqual(cleanupInstructions(evidence), []);
  },
});

rehearse("unconnected watches on, and says where to look, when the second create is unknown", {
  check: "unconnected",
  faults: [{ _tag: "IgnoreSessionLimit" }, { _tag: "RefuseAllocation", nth: 3, status: 503 }],
  judge: (evidence) => {
    failed("the spent token's second create was answered")(evidence);
    const probe = evidence.unconnected;
    assert.strictEqual(probe?.ended?.by, "reactor");
    assert.lengthOf(evidence.sessions, 2);
    assert.isTrue(
      evidence.sessions.every((session) => session.close?.termination?.confirmed === true),
    );
    assert.include(
      evidence.criteria.find(
        (criterion) => criterion.name === "the spent token's second create was answered",
      )?.detail ?? "",
      "has no known answer: Http 503, outcome unknown",
    );
    const [instruction] = cleanupInstructions(evidence);
    assert.include(instruction ?? "", "The spent token's second create has an unknown outcome");
    assert.include(instruction ?? "", probe?.spentToken?.requestedAt ?? "no request");
  },
});

// A second create may hang until its 15 s deadline. The watch runs beside it, so its reads keep
// their schedule meanwhile, and the key ends the spent token's session once the create gives up.
rehearse("unconnected keeps its watch on schedule while the second create hangs", {
  check: "unconnected",
  faults: [{ _tag: "StallAllocation", nth: 3 }],
  judge: (evidence) => {
    failed("the spent token's second create was answered")(evidence);
    const probe = evidence.unconnected;
    const second = probe?.spentToken?.second;
    const [session, spending] = evidence.sessions;
    assert.deepStrictEqual([second?.answer, second?.outcome], ["Timeout", "unknown"]);
    assert.isAtLeast((second?.answeredMs ?? 0) - (second?.sentMs ?? 0), 15_000);
    // Read at allocation and 2 s later, the session was found ACTIVE while the create still hung.
    const active = probe?.states.find((entry) => entry.state === "ACTIVE")?.firstMs;
    assert.isAtMost((active ?? Infinity) - (session?.allocatedMs ?? 0), 2_500);
    assert.strictEqual(probe?.ended?.by, "reactor");
    const endedAfter = (probe?.ended?.atMs ?? 0) - (session?.allocatedMs ?? 0);
    assert.isAtLeast(endedAfter, 60_000);
    assert.isAtMost(endedAfter, 62_500);
    assert.isTrue(
      evidence.criteria.find((criterion) => criterion.name === "the watch completed")?.passed,
    );
    assert.lengthOf(evidence.sessions, 2);
    assert.isTrue(spending?.close?.termination?.confirmed);
    assert.isAtLeast(spending.close.requestedMs, second?.answeredMs ?? Infinity);
    const [instruction] = cleanupInstructions(evidence);
    assert.include(instruction ?? "", "The spent token's second create has an unknown outcome");
  },
});

// A session allocated more slowly than the 5 s the key waits for it to be ready is held 10 s
// from the end of that wait instead, so its dashboard duration is still a known one.
rehearse("unconnected holds a spent token's session 10 s past a wait it never became ready in", {
  check: "unconnected",
  timing: {
    ...ReactorTest.Timing.hosted,
    allocation: { min: Duration.seconds(7), max: Duration.seconds(7) },
  },
  judge: (evidence) => {
    passes(evidence);
    const spent = evidence.unconnected?.spentToken;
    const held = spent?.held?.[0];
    const spending = evidence.sessions[1];
    assert.isUndefined(held?.connectableMs);
    assert.deepStrictEqual([...new Set(held?.states.map((entry) => entry.state))], ["PENDING"]);
    // Compared as the check computes it: a difference of times with a fraction of a millisecond,
    // which a loaded machine's test clock reaches, can fall short of 5 s by a rounding error.
    assert.isAtLeast(held?.heldFromMs ?? 0, (spent?.second?.answeredMs ?? Infinity) + 5_000);
    assert.closeTo((held?.endsMs ?? 0) - (held?.heldFromMs ?? Infinity), 10_000, 0.001);
    assert.isAtLeast(spending?.close?.requestedMs ?? 0, held?.endsMs ?? Infinity);
    assert.include(summarize([evidence]), " | 10.00 s past the wait | ");
  },
});

// Whatever ends the spent token's session after its second create, the reads after that create
// find it ended: it is not held, and the key ends it at once. Here each session ends as it goes
// ACTIVE, so neither is ever ready.
rehearse("unconnected ends at once a spent token's session found ended after the second create", {
  check: "unconnected",
  faults: [{ _tag: "Expire", after: Duration.zero }],
  judge: (evidence) => {
    passes(evidence);
    const spent = evidence.unconnected?.spentToken;
    const held = spent?.held?.[0];
    const spending = evidence.sessions[1];
    assert.strictEqual(held?.states.at(-1)?.state, "CLOSED");
    assert.isUndefined(held?.connectableMs);
    assert.isUndefined(held?.heldFromMs);
    assert.isBelow(
      (spending?.close?.requestedMs ?? Infinity) - (held?.states.at(-1)?.firstMs ?? 0),
      100,
    );
    assert.match(
      summarize([evidence]),
      new RegExp(
        `^\\| ${spending?.id ?? "?"} \\| the spent token's first create \\|.*\\| none: found ended \\| Reactor \\|`,
        "m",
      ),
    );
  },
});

// A run that stops hard, as a crash stops it, leaves the evidence as last saved: a create sent
// and no answer recorded says where to look too, whichever of the three it was.
rehearse("unconnected says where to look when the run stops with a create unanswered", {
  check: "unconnected",
  judge: (evidence) => {
    passes(evidence);
    const { finishedAt: _finished, verdict: _verdict, ...unfinished } = evidence;
    const probe = evidence.unconnected;
    const spent = probe?.spentToken;
    assert.isDefined(probe);
    assert.isDefined(spent);
    const { sessionId: _session, create: _create, spentToken: _spent, ...sent } = probe;
    const [first] = cleanupInstructions({
      ...unfinished,
      sessions: [],
      milestones: evidence.milestones.filter((milestone) => milestone.step === "create sent"),
      unconnected: { ...sent, states: [] },
    });
    assert.include(first ?? "", "The create went unanswered, as the run stopped");
    assert.include(first ?? "", probe.requestedAt);
    const { sessionId: _spending, create: _answer, second: _second, ...spending } = spent;
    const [second] = cleanupInstructions({
      ...unfinished,
      unconnected: { ...probe, spentToken: spending },
    });
    assert.include(second ?? "", "The spent token's first create went unanswered");
    assert.include(second ?? "", spent.requestedAt);
    const { second: _retry, ...retrying } = spent;
    const [third] = cleanupInstructions({
      ...unfinished,
      unconnected: { ...probe, spentToken: retrying },
    });
    assert.include(third ?? "", "The spent token's second create went unanswered");
  },
});

// Nothing but the key's reads touches the watched session, so an end Reactor makes about 30 s in,
// the time it gives a session once its last connection drops, answers Q1 as any other end does.
rehearse("unconnected answers Q1 when Reactor ends its session 30 s after it went ACTIVE", {
  check: "unconnected",
  faults: [{ _tag: "Expire", after: Duration.seconds(30) }],
  judge: (evidence) => {
    passes(evidence);
    const probe = evidence.unconnected;
    assert.strictEqual(probe?.ended?.by, "reactor");
    assert.isUndefined(probe?.unanswered);
    const endedAfter = (probe?.ended?.atMs ?? 0) - (evidence.sessions[0]?.allocatedMs ?? 0);
    assert.isAtLeast(endedAfter, 30_000);
    assert.isAtMost(endedAfter, 33_000);
    assert.notInclude(summarize([evidence]), "**Q1:**");
  },
});

// A coordinator may lose a session for one read: only a second read in a row answered 404, or a
// read at the end that agrees, ends the watch.
rehearse("unconnected takes a single read answered 404 for no end", {
  check: "unconnected",
  faults: [{ _tag: "MissingSession", nth: 10 }],
  judge: (evidence) => {
    passes(evidence);
    const probe = evidence.unconnected;
    assert.include(probe?.states.map((entry) => entry.state) ?? [], "gone");
    assert.strictEqual(probe?.ended?.by, "reactor");
    assert.isAtLeast((probe?.ended?.atMs ?? 0) - (evidence.sessions[0]?.allocatedMs ?? 0), 60_000);
    assert.isUndefined(probe?.unanswered);
  },
});

rehearse("unconnected credits no end the read at the end contradicts", {
  check: "unconnected",
  faults: [
    { _tag: "MissingSession", nth: 10 },
    { _tag: "MissingSession", nth: 11 },
  ],
  judge: (evidence) => {
    passes(evidence);
    const probe = evidence.unconnected;
    assert.strictEqual(probe?.read?.state, "ACTIVE");
    assert.strictEqual(probe?.ended?.by, "key");
    assert.include(probe?.unanswered ?? "", "the read at the end found it running");
    assert.match(summarize([evidence]), /^- \*\*Q1:\*\* unanswered by this run\. The watch /m);
  },
});

// One read answered 404 may be the coordinator's slip at the end as in the watch: alone, it
// credits Reactor with no end, and the key's end is the one recorded. The read at the end is the
// 64th of any session: after the watch's 61, and the spent token's session's read that found it
// ready and the read that confirmed its end.
rehearse("unconnected credits no end to a lone 404 on the read at the end", {
  check: "unconnected",
  faults: [{ _tag: "IgnoreCap" }, { _tag: "MissingSession", nth: 64 }],
  judge: (evidence) => {
    passes(evidence);
    const probe = evidence.unconnected;
    assert.strictEqual(probe?.states.at(-1)?.state, "ACTIVE");
    assert.strictEqual(probe?.read?.status, 404);
    assert.strictEqual(probe?.ended?.by, "key");
    assert.include(probe?.unanswered ?? "", "the read at the end alone answered 404");
  },
});

// A rate limit refuses a create before it asks anything of the token, so a 429 says nothing of a
// spent one.
rehearse("unconnected takes a second create refused 429 for no answer", {
  check: "unconnected",
  faults: [{ _tag: "IgnoreSessionLimit" }, { _tag: "RefuseAllocation", nth: 3, status: 429 }],
  judge: (evidence) => {
    failed("the spent token's second create was answered")(evidence);
    assert.include(
      evidence.criteria.find(
        (criterion) => criterion.name === "the spent token's second create was answered",
      )?.detail ?? "",
      "refused 429",
    );
    assert.strictEqual(evidence.unconnected?.ended?.by, "reactor");
  },
});

// A create answered 2xx without naming a session may have made one all the same: the codes of
// its body are kept, for whoever must find that session.
rehearse("unconnected keeps the codes of a second create's reply that names no session", {
  check: "unconnected",
  faults: [{ _tag: "IgnoreSessionLimit" }, { _tag: "UnnamedAllocation", nth: 3 }],
  judge: (evidence) => {
    failed("the spent token's second create was answered")(evidence);
    const second = evidence.unconnected?.spentToken?.second;
    assert.deepStrictEqual(
      [second?.answer, second?.outcome, second?.keys, second?.codes],
      ["Protocol", "unknown", ["state"], { state: "PENDING" }],
    );
    const [instruction] = cleanupInstructions(evidence);
    assert.include(instruction ?? "", "The spent token's second create has an unknown outcome");
    assert.include(instruction ?? "", "Its reply's codes: state PENDING.");
  },
});

rehearse("unconnected keeps the codes of a create reply that names no session", {
  check: "unconnected",
  faults: [{ _tag: "UnnamedAllocation", nth: 1 }],
  judge: (evidence) => {
    assert.strictEqual(evidence.verdict, "fail");
    assert.lengthOf(evidence.sessions, 0);
    const probe = evidence.unconnected;
    assert.isUndefined(probe?.sessionId);
    assert.isUndefined(probe?.spentToken);
    assert.deepStrictEqual(
      [probe?.create?.answer, probe?.create?.outcome, probe?.create?.codes],
      ["Protocol", "unknown", { state: "PENDING" }],
    );
    const [instruction] = cleanupInstructions(evidence);
    assert.include(instruction ?? "", "The create has an unknown outcome");
    assert.include(instruction ?? "", probe?.requestedAt ?? "no request");
    assert.include(instruction ?? "", "Its reply's codes: state PENDING.");
    assert.include(
      summarize([evidence]),
      "**Unconnected:** no session named; its create failed with Protocol",
    );
  },
});

// Reactor may end the session after the watch's last read and before the key's: the read at the
// end tells which. Counted from ACTIVE, 119.8 s lands between those two reads; 119.7 to 119.9 s
// do.
rehearse("unconnected credits Reactor with an end only its read at the end found", {
  check: "unconnected",
  faults: [{ _tag: "IgnoreCap" }, { _tag: "Expire", after: Duration.millis(119_800) }],
  judge: (evidence) => {
    passes(evidence);
    const probe = evidence.unconnected;
    assert.strictEqual(probe?.states.at(-1)?.state, "ACTIVE");
    assert.strictEqual(probe?.read?.state, "CLOSED");
    assert.strictEqual(probe?.ended?.by, "reactor");
  },
});

// ReactorTest answers a payload its schema refuses, or one carrying a null, with an error frame, and
// what its model refuses with a command_error broadcast before the acknowledgement, as hosted Vidu
// did. A call's picture reaches the connection once it resumes main_video after the call is live.
// The record keeps codes and lengths: no transcript, persona, URL or voice description.
rehearse("avatar judges both calls and keeps what each refused command met, and no text", {
  check: "avatar",
  judge: (evidence) => {
    passes(evidence);
    assert.includeMembers(
      evidence.criteria.map((criterion) => criterion.name),
      [
        "the session's first snapshot arrived",
        "the avatar became ready",
        "the first call went live",
        "character video arrived during the first call",
        "character audio arrived during the first call",
        "end_call was answered with call_ended",
        "the second call went live with video",
        "confirmed termination",
      ],
    );
    const avatar = evidence.avatar;
    assert.deepStrictEqual(
      avatar?.refusals.map((refusal) => [
        refusal.command,
        refusal.wire,
        refusal.code ?? refusal.commandError?.code,
        refusal.commandError?.beforeAnswer,
      ]),
      [
        ["clone_voice", "ack", "CLONING_DISABLED", true],
        ["say", "ack", "NOT_LIVE", true],
        ["update_call", "ack", "INVALID_INPUT", true],
        ["say", "error", "invalid_command", undefined],
        ["update_call", "error", "invalid_command", undefined],
      ],
    );
    assert.deepStrictEqual(avatar?.refusals.at(-1)?.changed, []);
    // The snapshot sent on connect is heard, though it comes before any command.
    assert.isBelow(avatar?.first?.atMs ?? Infinity, avatar?.getState?.sentMs ?? 0);
    assert.deepStrictEqual(
      avatar?.calls.map((call) => [
        call.picture?.map((stage) => [stage.action, stage.firstFrameMs !== undefined]),
        call.end?.type,
      ]),
      [
        [[["resume", true]], "call_ended"],
        [
          [
            ["wait", false],
            ["resume", true],
          ],
          "call_ended",
        ],
      ],
    );
    const kept = JSON.stringify(evidence);
    for (const text of [
      "You are Probe",
      "count slowly",
      "Greeting 1",
      "example.invalid",
      "adult male",
    ])
      assert.notInclude(kept, text);
    assert.match(summarize([evidence]), /^\| say before any call \| ack \| [\d.]+ s \| NOT_LIVE /m);
  },
});

// With no picture at all, each call does all it can for one and finds none: the video criteria
// fail, the rest is still asked, and the session still ends.
rehearse("avatar fails without the character's picture, and still ends its session", {
  check: "avatar",
  faults: [{ _tag: "Video", video: "absent" }],
  judge: (evidence) => {
    assert.strictEqual(evidence.verdict, "fail");
    const passed = (name: string) =>
      evidence.criteria.find((criterion) => criterion.name === name)?.passed;
    assert.deepStrictEqual(
      [
        passed("character video arrived during the first call"),
        passed("the second call went live with video"),
        passed("the first call went live"),
        passed("character audio arrived during the first call"),
        passed("end_call was answered with call_ended"),
        passed("confirmed termination"),
      ],
      [false, false, true, true, true, true],
      evidence.reasons.join("; "),
    );
    assert.deepStrictEqual(
      evidence.avatar?.calls.map((call) =>
        call.picture?.map((stage) => [stage.action, stage.firstFrameMs]),
      ),
      [
        [
          ["resume", undefined],
          ["cycle", undefined],
        ],
        [
          ["wait", undefined],
          ["resume", undefined],
          ["cycle", undefined],
        ],
      ],
    );
    assert.isTrue(evidence.sessions[0]?.close?.confirmed);
  },
});

// With no picture, the provider's call still goes live, is answered and ends: only the picture's
// criterion fails, the session still ends, and the record keeps lengths, never what was said.
rehearse("character fails without the character's picture, and keeps no text", {
  check: "character",
  faults: [{ _tag: "Video", video: "absent" }],
  judge: (evidence) => {
    assert.strictEqual(evidence.verdict, "fail");
    assert.deepStrictEqual(
      evidence.criteria.filter((criterion) => !criterion.passed).map((criterion) => criterion.name),
      ["the character's picture came after live"],
    );
    assert.isTrue(evidence.sessions[0]?.close?.confirmed);
    assert.isNotEmpty(evidence.character?.transcripts);
    const kept = JSON.stringify(evidence);
    for (const text of ["You are Probe", "Say hello", "about the sea", "Greeting 1"])
      assert.notInclude(kept, text);
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
