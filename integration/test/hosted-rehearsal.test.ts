/**
 * The hosted qualification, rehearsed end to end against the local twin: the
 * same script and checks a paid run executes, through its command line, with
 * every failure path the stop rules exist for. Nothing leaves loopback.
 */
import { spawnSync } from "node:child_process";
import { mkdtempSync, readFileSync, readdirSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { expect, test } from "bun:test";
import * as Schema from "effect/Schema";
import { Evidence } from "../hosted/evidence.js";

const script = fileURLToPath(new URL("../hosted/qualify.ts", import.meta.url));

const rehearse = (check: string, faults: readonly string[] = []) => {
  const ledger = mkdtempSync(join(tmpdir(), "hosted-rehearsal-"));
  const result = spawnSync(
    process.execPath,
    [
      script,
      "rehearse",
      check,
      `--ledger=${ledger}`,
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
