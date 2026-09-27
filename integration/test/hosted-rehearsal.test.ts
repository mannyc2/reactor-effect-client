/**
 * The hosted qualification, rehearsed end to end against the local twin: the
 * same script and checks a paid run executes, through its command line, with
 * every failure path the stop rules exist for. Nothing leaves loopback.
 */
import { spawn, spawnSync } from "node:child_process";
import { mkdtempSync, readFileSync, readdirSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { expect, test } from "bun:test";
import * as Schema from "effect/Schema";
import { Evidence } from "../hosted/evidence.js";

const script = fileURLToPath(new URL("../hosted/qualify.ts", import.meta.url));

const rehearse = (
  check: string,
  faults: readonly string[] = [],
  constructor?: "legacy" | "continuous",
) => {
  const ledger = mkdtempSync(join(tmpdir(), "hosted-rehearsal-"));
  const result = spawnSync(
    process.execPath,
    [
      script,
      "rehearse",
      check,
      `--ledger=${ledger}`,
      ...(constructor === undefined ? [] : [`--constructor=${constructor}`]),
      ...(faults.length === 0 ? [] : [`--faults=${faults.join(",")}`]),
    ],
    { encoding: "utf8", timeout: 150_000, env: { PATH: process.env.PATH ?? "" } },
  );
  const files = readdirSync(ledger).filter((name) => name.endsWith(".json"));
  expect(files, `${result.stdout}\n${result.stderr}`).toHaveLength(1);
  const text = readFileSync(join(ledger, files[0]!), "utf8");
  return {
    status: result.status,
    output: `${result.stdout}\n${result.stderr}`,
    text,
    evidence: Schema.decodeUnknownSync(Evidence)(JSON.parse(text)),
  };
};

/** No credential of any kind reaches the evidence. */
const credentialFree = (text: string) => {
  expect(text).not.toContain("twin-api-key");
  expect(text).not.toMatch(/eyJ[\w-]+\.eyJ[\w-]+/);
  expect(text.toLowerCase()).not.toContain("bearer");
};

test("a rehearsed vertical passes with complete evidence and no credential", () => {
  const run = rehearse("vertical");
  expect(run.status, run.output).toBe(0);
  const evidence = run.evidence;
  expect(evidence.verdict).toBe("pass");
  expect(evidence.missing).toEqual([]);
  expect(evidence.criteria.map((criterion) => [criterion.name, criterion.passed])).toEqual([
    ["correlated acceptance", true],
    ["lifecycle progression", true],
    ["live video", true],
    ["audio when offered", true],
    ["metadata preserved", true],
    ["ICE pair selected", true],
    ["confirmed termination", true],
  ]);
  expect(evidence.mode).toBe("rehearsal");
  expect(evidence.outcomes).toEqual(["replied"]);
  expect(evidence.termination?.confirmed).toBe(true);
  // A session shorter than a minute bills the whole minute its worst case reserved.
  expect(evidence.budget.estimatedUsd).toBeGreaterThan(0);
  expect(evidence.budget.estimatedUsd).toBeLessThanOrEqual(evidence.budget.worstCaseUsd!);
  const connect = evidence.spans.find((span) => span.name === "reactor.session.connect");
  expect(connect?.events.map((event) => event.name)).toContain("reactor.connect.ready");
  expect(evidence.milestones.map((milestone) => milestone.step)).toEqual([
    "admitted",
    "minted",
    "allocated",
    "connected",
    "accepted",
    "clip started",
    "observed",
    "closed",
    "trail",
  ]);
  credentialFree(run.text);
}, 180_000);

test("a rehearsed takeover kills the owner, attaches in time and ends the session by its record", () => {
  const run = rehearse("takeover");
  expect(run.status, run.output).toBe(0);
  const takeover = run.evidence.takeover!;
  expect(takeover.attachMs).toBeLessThanOrEqual(5_000);
  expect(takeover.clipIdentified).toBe(true);
  expect(takeover.metadataPreserved).toBe(true);
  expect(takeover.enqueuesAfterAttach).toBe(0);
  expect(takeover.video?.frames).toBeGreaterThan(1);
  expect(run.evidence.termination?.remote?.confirmed).toBe(true);
  expect(run.evidence.missing).toEqual([]);
  credentialFree(run.text);
}, 180_000);

test("a rehearsed audio check sends an image and an audio reference, and the clip reports both", () => {
  const run = rehearse("audio");
  expect(run.status, run.output).toBe(0);
  expect(run.evidence.missing).toEqual([]);
  expect(run.evidence.contract?.referenceAudio).toBe(true);
  expect(run.evidence.acceptance?.references).toEqual({
    images: 1,
    audio: 1,
    reportedImages: 1,
    reportedAudio: 1,
    hasReferenceAudio: true,
  });
  expect(run.evidence.criteria.map((criterion) => criterion.name)).toContain(
    "reference audio reported",
  );
  // Two reference uploads, then the enqueue: the audio went up as audio.
  expect(run.evidence.spans.filter((span) => span.name === "reactor.session.upload")).toHaveLength(
    2,
  );
  credentialFree(run.text);
}, 180_000);

test("a rehearsed resume adopts the killed owner's session through resumeH3, only reads it, and its close ends it", () => {
  const run = rehearse("resume");
  expect(run.status, run.output).toBe(0);
  expect(run.evidence.missing).toEqual([]);
  const takeover = run.evidence.takeover!;
  expect(takeover.clipIdentified).toBe(true);
  expect(takeover.metadataPreserved).toBe(true);
  expect(takeover.enqueuesAfterAttach).toBe(0);
  expect(takeover.video?.frames).toBeGreaterThan(1);
  // The library's close of the adopted session is the termination, not the record.
  expect(run.evidence.termination?.close).toMatchObject({
    ownership: "owned",
    remote: { attempted: true, confirmed: true },
  });
  expect(run.evidence.termination?.remote).toBeUndefined();
  expect(
    run.evidence.criteria
      .filter((criterion) => !criterion.passed)
      .map((criterion) => criterion.name),
  ).toEqual([]);
  const names = run.evidence.criteria.map((criterion) => criterion.name);
  expect(names).toContain("only reads on resume");
  expect(names).toContain("adopted close terminates");
  credentialFree(run.text);
}, 180_000);

test("a rehearsed scheduler records two bounded sessions and measured queue and decoded-media facts", () => {
  const run = rehearse("scheduler");
  expect(run.status, run.output).toBe(0);
  const evidence = run.evidence;
  const scheduler = evidence.scheduler!;
  expect(evidence.budget.worstCaseUsd).toBeGreaterThan(0);
  expect(evidence.budget.worstCaseUsd).toBeLessThanOrEqual(1.5);
  expect(evidence.missing).toEqual([]);
  expect(evidence.termination?.confirmed).toBe(true);
  expect(scheduler.replacement.termination?.confirmed).toBe(true);
  expect(scheduler.replacement.session?.id).not.toBe(evidence.session?.id);
  expect(
    scheduler.builds
      .filter((build) => build.readyMs !== undefined)
      .map((build) => build.requestedSeconds),
  ).toEqual([5, 15]);
  expect(scheduler.latencyByRequestedSeconds.map((bucket) => bucket.requestedSeconds)).toEqual([
    5, 15,
  ]);
  expect(scheduler.builds[0]?.readySeconds).toBeGreaterThanOrEqual(5);
  expect(scheduler.readyMove).toMatchObject({ queue: "playout", position: 0 });
  expect(scheduler.positionZero?.generationOrder.slice(0, 2)).toEqual([
    scheduler.positionZero!.buildingClipId,
    scheduler.positionZero!.requestedClipId,
  ]);
  expect(scheduler.poppedBuild?.wasGeneration).toBe(true);
  expect(scheduler.poppedBuild?.startedAfterPop).toBe(false);
  expect(scheduler.decodedHandoff?.replacementFirstFrameMs).toBeGreaterThan(
    scheduler.decodedHandoff?.oldLastFrameMs ?? 0,
  );
  expect(Object.keys(scheduler.metadata.observed).length).toBeGreaterThan(0);
  expect(scheduler.metadata.mismatched).toEqual({});
  credentialFree(run.text);
}, 180_000);

test("an uncertain scheduler enqueue stops before opening the replacement session", () => {
  const run = rehearse("scheduler", ["dropEnqueueReply"]);
  expect(run.status, run.output).toBe(1);
  expect(run.evidence.outcomes).toContain("unknown");
  expect(run.evidence.scheduler?.replacement.session).toBeUndefined();
  expect(run.evidence.termination?.confirmed).toBe(true);
  expect(run.evidence.budget.worstCaseUsd).toBeGreaterThan(0);
  credentialFree(run.text);
}, 180_000);

test("the relay check passes only on a relay pair", () => {
  const run = rehearse("turn");
  expect(run.status, run.output).toBe(0);
  expect([run.evidence.network?.pair?.local, run.evidence.network?.pair?.remote]).toContain(
    "relay",
  );
}, 180_000);

test("a lost enqueue reply stops the check as unknown, and the session still ends", () => {
  const run = rehearse("vertical", ["dropEnqueueReply"]);
  expect(run.status, run.output).toBe(1);
  expect(run.evidence.outcomes).toContain("unknown");
  expect(run.evidence.reasons[0]).toContain("an outcome is unknown");
  expect(run.evidence.termination?.confirmed).toBe(true);
}, 180_000);

test("a termination the library cannot confirm fails with cleanup instructions and a trail", () => {
  const run = rehearse("vertical", ["slowDelete"]);
  expect(run.status, run.output).toBe(1);
  const termination = run.evidence.termination!;
  expect(termination.confirmed).toBe(false);
  // The coordinator ended it later: the trail records when, which is what
  // decides whether `terminate` needs a bounded confirmation poll.
  expect(termination.terminalMs).toBeGreaterThan(termination.requestedMs);
  expect(run.evidence.reasons.join("\n")).toContain("remote termination is unconfirmed");
  expect(run.evidence.cleanup).toContain("was not confirmed ended");
  expect(run.output).toContain("was not confirmed ended");
}, 180_000);

test("black video, frozen video and missing audio each fail their criterion", () => {
  for (const [fault, criterion] of [
    ["blackFrames", "live video"],
    ["frozenFrames", "live video"],
    ["noAudio", "audio when offered"],
  ] as const) {
    const run = rehearse("vertical", [fault]);
    expect(run.status, `${fault}\n${run.output}`).toBe(1);
    expect(run.evidence.criteria.find((entry) => entry.name === criterion)?.passed, fault).toBe(
      false,
    );
    expect(run.evidence.termination?.confirmed, fault).toBe(true);
  }
}, 400_000);

test("a token that grants more than asked is refused before any session exists", () => {
  const run = rehearse("vertical", ["overgrant"]);
  expect(run.status, run.output).toBe(1);
  // The library refuses the grant itself; the script's own gate is a second check.
  expect(run.evidence.reasons[0]).toContain("invalid or unbounded session grant");
  expect(run.evidence.session).toBeUndefined();
  expect(run.evidence.budget.worstCaseUsd).toBeGreaterThan(0);
}, 180_000);

for (const constructor of ["legacy", "continuous"] as const)
  test(`public renewal ${constructor} uses the real scheduler, planned switch and accepted drain`, () => {
    const run = rehearse("scheduler-renewal", [], constructor);
    expect(run.status, run.output).toBe(0);
    const renewal = run.evidence.schedulerRenewal!;
    expect(renewal.configuration.constructor).toBe(constructor);
    if (constructor === "continuous") {
      expect(renewal.cleanup?._tag).toBe("Continuous");
      if (renewal.cleanup?._tag !== "Continuous") throw new Error("Missing continuous cleanup");
      expect(renewal.cleanup.summary).toMatchObject({
        totalRetirements: 2n,
        omittedComplete: { ownedTerminated: 1n, noAllocation: 0n, attachedDetached: 0n },
        exhausted: false,
      });
      expect(renewal.cleanup.summary?.retained).toHaveLength(1);
      expect(renewal.cleanup.summary?.retained[0]).toMatchObject({
        source: { sessionId: renewal.allocations[1]!.sessionId },
        disposition: "complete",
        retirement: { unknownSubmissions: 0n },
      });
    }
    expect(renewal.openAttempts).toBe(2);
    expect(renewal.switches).toHaveLength(1);
    expect(renewal.drain?.outcome).toBe("completed");
    expect(renewal.media.attributionComplete).toBe(true);
    expect(renewal.media.decodedBoundary?.gapMs).toBeGreaterThanOrEqual(0);
    expect(renewal.allocations.every((slot) => slot.cleanup?.lease.remote.confirmed === true)).toBe(
      true,
    );
    expect(run.evidence.scheduler).toBeUndefined();
    credentialFree(run.text);
  }, 180_000);

for (const constructor of ["legacy", "continuous"] as const)
  for (const fault of [
    "refuseFirstAllocation",
    "refuseSecondAllocation",
    "failConnect",
    "failSecondConnect",
    "overgrantSecondToken",
    "slowDelete",
    "ignoreDelete",
    "dropSecondEnqueueReply",
    "noFirstVideo",
    "noSecondVideo",
    "stallSecondBuild",
  ])
    test(`public renewal ${constructor} retains failed evidence for ${fault}`, () => {
      const run = rehearse("scheduler-renewal", [fault], constructor);
      expect(run.status, run.output).toBe(1);
      expect(run.evidence.verdict).toBe("fail");
      const renewal = run.evidence.schedulerRenewal!;
      expect(
        renewal.allocations.filter((slot) => slot.sessionId !== undefined).length,
      ).toBeLessThanOrEqual(2);
      expect(run.evidence.budget.worstCaseUsd).toBeGreaterThan(0);
      if (fault === "overgrantSecondToken" || fault === "refuseFirstAllocation")
        expect(renewal.allocations.some((slot) => slot.sessionId !== undefined)).toBe(false);
      if (fault === "refuseSecondAllocation") {
        expect(renewal.allocations.filter((slot) => slot.sessionId !== undefined)).toHaveLength(1);
        // The refused open's unknown allocation stops the run before the SDK's
        // retry, so neither a third open nor continuous capacity is ever reached.
        expect(renewal.openAttempts).toBe(2);
        if (constructor === "continuous") {
          // The failed acquisition keeps its reserved incomplete record.
          if (renewal.cleanup?._tag !== "Continuous") throw new Error("Missing continuous cleanup");
          expect(renewal.cleanup.summary).toMatchObject({
            totalRetirements: 2n,
            exhausted: false,
            omittedComplete: { noAllocation: 0n, ownedTerminated: 0n, attachedDetached: 0n },
          });
          expect(renewal.cleanup.summary?.retained).toHaveLength(2);
          expect(
            renewal.cleanup.summary?.retained.find((row) => row.disposition === "incomplete"),
          ).toMatchObject({
            cleanup: { lease: { allocation: "unknown" } },
            retirement: {
              accounting: "not-applicable",
              scope: "closed",
              affinity: "not-applicable",
            },
          });
          // The refused open names no session, so its allocation stays unknown: the
          // slot records it with its own report, the stop rule sees it, and the
          // operator is sent to the dashboard with the grant's expiry.
          expect(renewal.allocations[1]).toMatchObject({
            slot: 2,
            allocation: "unknown",
            leaseCleanup: { allocation: "unknown" },
          });
          expect(run.evidence.outcomes).toContain("unknown");
          expect(run.evidence.cleanup).toContain("Source 2's allocation outcome is unknown");
          expect(run.output).toContain("Source 2's allocation outcome is unknown");
          // Cleanup starts as soon as that outcome is recorded, well inside the 5 s
          // the SDK waits before it would try another allocation.
          expect(renewal.cleanup.requestedMs - renewal.allocations[1]!.closedMs!).toBeLessThan(
            2_500,
          );
        }
      }
      if (fault === "failConnect")
        expect(renewal.allocations[0]?.leaseCleanup?.remote.confirmed).toBe(true);
      if (fault === "failSecondConnect") {
        // The second session is known and closed, so its failed open is one the SDK
        // retries: the harness's guard must refuse that third open before allocating.
        expect(run.output).toContain("rehearsal twin: 2 created, 0 not closed");
        expect(renewal.openAttempts).toBe(3);
        expect(run.evidence.reasons).toContain(
          "InvalidState: scheduler renewal refused a third open attempt",
        );
        expect(renewal.allocations[1]?.leaseCleanup).toMatchObject({
          allocation: "known",
          sessionId: renewal.allocations[1]?.sessionId,
          remote: { confirmed: true },
        });
        expect(renewal.allocations.some((slot) => slot.allocation === "unknown")).toBe(false);
        expect(run.evidence.outcomes).not.toContain("unknown");
      }
      if (fault === "dropSecondEnqueueReply") {
        expect(run.evidence.outcomes).toContain("unknown");
        expect(renewal.items[1]?.sessionId).toBe(renewal.allocations[1]?.sessionId);
        expect(renewal.items[1]?.statuses.some((status) => status._tag === "Unknown")).toBe(true);
        if (constructor === "continuous") {
          expect(renewal.cleanup?._tag).toBe("Continuous");
          if (renewal.cleanup?._tag !== "Continuous") throw new Error("Missing continuous cleanup");
          const unresolved = renewal.cleanup.summary?.retained.find(
            (row) => row.source?.sessionId === renewal.items[1]?.sessionId,
          );
          expect(unresolved?.disposition).toBe("incomplete");
          expect(unresolved?.retirement.unknownSubmissions).toBeGreaterThan(0n);
        }
      }
      if (fault === "slowDelete" || fault === "ignoreDelete") {
        // The retiring source's DELETE is accepted, but its end is never confirmed.
        expect(run.evidence.reasons).toContain("Shutdown: source cleanup was not confirmed");
        expect(renewal.allocations[0]?.cleanup?.lease.remote.confirmed).toBe(false);
        expect(run.evidence.cleanup).toContain("was not confirmed ended");
      }
      if (fault === "noFirstVideo" || fault === "noSecondVideo") {
        expect(run.evidence.reasons.join("\n")).toContain(
          "logical video from both sources and complete frame attribution are required",
        );
        expect(run.evidence.missing).toContain("schedulerRenewal.media.decodedBoundary");
      }
      if (fault === "stallSecondBuild") {
        expect(renewal.switches).toEqual([]);
        expect(run.evidence.reasons).toContain(
          "Timeout: scheduler renewal exceeded its shared deadline",
        );
      }
      credentialFree(run.text);
    }, 180_000);

/** The parent process is the explicit external emergency owner for these interruption fixtures. */
const stopAtCheckpoint = async (
  constructor: "legacy" | "continuous",
  fault: string | undefined,
  signal: NodeJS.Signals,
  reached: (renewal: NonNullable<Evidence["schedulerRenewal"]>) => boolean,
) => {
  const ledger = mkdtempSync(join(tmpdir(), "hosted-renewal-interrupt-"));
  const child = spawn(
    process.execPath,
    [
      script,
      "rehearse",
      "scheduler-renewal",
      `--constructor=${constructor}`,
      `--ledger=${ledger}`,
      ...(fault === undefined ? [] : [`--faults=${fault}`]),
    ],
    { env: { PATH: process.env.PATH ?? "" }, stdio: ["ignore", "pipe", "pipe"] },
  );
  let output = "";
  child.stdout.setEncoding("utf8").on("data", (chunk: string) => (output += chunk));
  child.stderr.setEncoding("utf8").on("data", (chunk: string) => (output += chunk));
  const exited = new Promise<number | null>((resolve, reject) => {
    child.once("close", resolve);
    child.once("error", reject);
  });
  const deadline = Date.now() + 90_000;
  let checkpoint: Evidence | undefined;
  let file: string | undefined;
  try {
    while (Date.now() < deadline) {
      const name = readdirSync(ledger).find((entry) => entry.endsWith(".json"));
      if (name !== undefined) {
        file = join(ledger, name);
        checkpoint = Schema.decodeUnknownSync(Evidence)(JSON.parse(readFileSync(file, "utf8")));
        const renewal = checkpoint.schedulerRenewal;
        if (renewal !== undefined && reached(renewal)) break;
      }
      await new Promise<void>((resolve) => setTimeout(resolve, 10));
    }
    expect(file).toBeDefined();
    expect(checkpoint?.schedulerRenewal?.allocations[0]?.sessionId).toBeDefined();
    child.kill(signal);
    const emergency = setTimeout(() => child.kill("SIGKILL"), 30_000);
    let status: number | null;
    try {
      status = await exited;
    } finally {
      clearTimeout(emergency);
    }
    return {
      evidence: Schema.decodeUnknownSync(Evidence)(JSON.parse(readFileSync(file!, "utf8"))),
      status,
      output,
    };
  } finally {
    if (child.exitCode === null && child.signalCode === null) child.kill("SIGKILL");
  }
};

for (const constructor of ["legacy", "continuous"] as const) {
  test(`public renewal ${constructor} interruption after allocation preserves the canonical lease report`, async () => {
    const { evidence } = await stopAtCheckpoint(
      constructor,
      undefined,
      "SIGINT",
      (renewal) => renewal.allocations[0]?.sessionId !== undefined,
    );
    expect(evidence.verdict).toBe("fail");
    expect(evidence.budget.worstCaseUsd).toBeGreaterThan(0);
    const source = evidence.schedulerRenewal!.allocations[0]!;
    expect(source.cleanup?.lease.remote.confirmed ?? source.leaseCleanup?.remote.confirmed).toBe(
      true,
    );
  }, 180_000);

  test(`public renewal ${constructor} stalled close leaves a durable failed checkpoint after emergency termination`, async () => {
    const { evidence } = await stopAtCheckpoint(
      constructor,
      "stallClose",
      "SIGKILL",
      (renewal) => renewal.allocations[0]?.closeRequestedMs !== undefined,
    );
    expect(evidence.verdict).toBe("fail");
    expect(evidence.budget.worstCaseUsd).toBeGreaterThan(0);
    const source = evidence.schedulerRenewal!.allocations[0]!;
    expect(source.closeRequestedMs).toBeDefined();
    expect(source.cleanup).toBeUndefined();
    expect(source.leaseCleanup).toBeUndefined();
  }, 180_000);
}

// Every evidence write fails from the cleanup checkpoint on, as on a full disk, once
// A's submission shows the owner exists. The paid path shares execute's exit code.
test("public renewal continuous still closes its source and exits 1 when evidence writes fail during cleanup", async () => {
  const { evidence, status, output } = await stopAtCheckpoint(
    "continuous",
    "failCleanupWrites",
    "SIGINT",
    (renewal) => renewal.items.length > 0,
  );
  expect(status, output).toBe(1);
  expect(output).toContain("hosted-qualification-fail scheduler-renewal");
  expect(output).toContain("the final evidence was not saved");
  // The incomplete file cannot name every session, so the run prints them.
  const allocated = evidence.schedulerRenewal?.allocations.flatMap((slot) =>
    slot.sessionId === undefined ? [] : [slot.sessionId],
  );
  expect(allocated).toHaveLength(1);
  expect(output).toContain(`sessions this run recorded: ${allocated?.join(", ")}`);
  // The owner still closed the allocated source: no dashboard instruction names it.
  expect(output).not.toContain("was not confirmed ended");
  // The durable file keeps the admission's reservation and the last checkpoint before cleanup.
  expect(evidence.budget.worstCaseUsd).toBeGreaterThan(0);
  expect(evidence.schedulerRenewal?.cleanup).toBeUndefined();
}, 180_000);

// Every write fails from the second open's own checkpoint on. That open runs in the
// SDK's renewal fiber, which the scenario's failure race does not interrupt, so only
// the harness can keep it from allocating a session no durable record would name.
test("public renewal continuous opens no second source once an evidence write fails", () => {
  const run = rehearse("scheduler-renewal", ["failSecondOpenWrites"], "continuous");
  expect(run.status, run.output).toBe(1);
  // The stand-in coordinator allocated the first session only, and it was closed.
  expect(run.output).toContain("rehearsal twin: 1 created, 0 not closed");
  expect(run.output).not.toContain("was not confirmed ended");
  expect(run.output).not.toContain("allocation outcome is unknown");
}, 180_000);
