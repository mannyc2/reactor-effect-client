/** The evidence schema, its completeness rules, the ledger and the writer, offline. */
import { mkdtempSync, readFileSync, readdirSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { expect, test } from "bun:test";
import * as Schema from "effect/Schema";
import {
  Evidence,
  Writer,
  conclude,
  format,
  lockLedger,
  missing,
  readLedger,
  reservedUsd,
} from "../hosted/evidence.js";
import type { Draft } from "../hosted/evidence.js";
import { Refused } from "../hosted/gates.js";
import { summarize } from "../hosted/report.js";

const directory = () => mkdtempSync(join(tmpdir(), "hosted-evidence-"));

const video = {
  frames: 144,
  formats: ["BGRA"],
  sizes: ["1344x768"],
  firstFrameMs: 3_100,
  fps: 23.9,
  interval: { p50: 41.7, p95: 55, max: 120 },
  lit: 144,
  distinct: 144,
  meanLuma: 96.2,
  lost: 0,
  gaps: 0,
  withMetadata: 144,
  withFrameId: 0,
  withTimestamp: 144,
};

/** A complete, passing vertical run, as the check would leave it. */
const vertical = (): Draft => ({
  format,
  runId: "3f2a9c1e-0000-4000-8000-000000000000",
  check: "vertical",
  mode: "paid",
  startedAt: "2026-09-30T14:02:11.000Z",
  finishedAt: "2026-09-30T14:02:40.000Z",
  environment: {
    runtime: "bun 1.4.2",
    os: "darwin arm64 25.0.0",
    commit: "0123456789abcdef0123456789abcdef01234567",
    dirty: false,
    packages: { "reactor-effect-client": "0.3.0-rc.0", "reactor-effect-native": "0.3.0-rc.0" },
    native: { webrtcPrebuilt: "webrtc-146-5a2b8e1f-p1", abiVersion: 4 },
    network: "home fiber, no VPN",
    apiOrigin: "https://api.reactor.inc",
  },
  budget: {
    checkUsd: 0.75,
    totalUsd: 1.5,
    reservedBeforeUsd: 0,
    sessionSeconds: 50,
    rate: { creditsPerSecond: 10, creditsPerDollar: 6000 },
    worstCaseUsd: 0.1,
    estimatedUsd: 0.1,
  },
  grant: { maxSessions: 1, maxSessionSeconds: 50, expiresAt: 1_790_000_000 },
  session: {
    id: "sess_0123",
    allocatedMs: 1_100,
    endedMs: 22_100,
    tracks: [{ name: "main_video", kind: "video", direction: "recvonly" }],
  },
  server: {
    cluster: "c1",
    zone: "z1",
    serverVersion: "1.2.3",
    transport: "webrtc/1.0",
    additional: {},
  },
  milestones: [
    { atMs: 0, step: "started" },
    { atMs: 1_100, step: "allocated" },
  ],
  spans: [
    {
      name: "reactor.session.connect",
      startMs: 1_200,
      durationMs: 1_900,
      status: "ok",
      attributes: { "reactor.connection.generation": "1" },
      events: [
        { name: "reactor.connect.described", atMs: 1_400 },
        { name: "reactor.connect.ready", atMs: 3_100 },
      ],
    },
  ],
  contract: {
    modelName: "reactor/h3-reference-to-video-turbo-realtime",
    documentedVersion: "0.5.5",
    deploymentTitle: "H3",
    deploymentVersion: "0.5.5",
    messages: { clip_queued: 1, queue_update: 3 },
    unknown: {},
    duplicates: 0,
    stale: 0,
    diagnostics: {},
  },
  acceptance: {
    clipId: "5f0c3b1e-7c2d-4a8e-9b6f-0a1b2c3d4e5f",
    evidence: "correlated",
    transportGeneration: "1",
    submitMs: 3_300,
    acceptedMs: 3_500,
    metadataEchoes: { clip_queued: 1, queue_update: 2 },
  },
  lifecycle: {
    generated: { atMs: 4_800, message: "clip_generated", transportGeneration: "1" },
    started: { atMs: 6_900, message: "clip_started", transportGeneration: "1" },
  },
  media: {
    audioOffered: false,
    firstClipFrameMs: 7_000,
    video,
    pressure: {
      deliveredVideo: "144",
      deliveredAudio: "0",
      droppedVideo: "0",
      droppedAudio: "0",
      readerOverflows: "0",
    },
  },
  network: {
    pair: { local: "srflx", remote: "host" },
    samples: [{ atMs: 4_000, local: "srflx", remote: "host", rttMs: 38 }],
  },
  termination: {
    requestedMs: 21_900,
    reportedMs: 22_100,
    confirmed: true,
    trail: [{ atMs: 22_100, state: "CLOSED" }],
    terminalMs: 22_100,
  },
  outcomes: ["replied"],
  criteria: [{ name: "correlated acceptance", passed: true }],
  missing: [],
  reasons: [],
});

test("a complete run encodes to JSON and decodes back unchanged", () => {
  const run = vertical();
  conclude(run, undefined);
  expect(run.verdict).toBe("pass");
  const json = JSON.parse(JSON.stringify(Schema.encodeSync(Evidence)(run))) as unknown;
  expect(Schema.decodeUnknownSync(Evidence)(json)).toEqual(run);
});

test("a run missing any required field fails as incomplete, whatever else passed", () => {
  const { acceptance: _acceptance, ...run }: Draft = vertical();
  const { pressure: _pressure, ...media } = run.media!;
  run.media = media;
  expect(missing(run)).toEqual(["acceptance", "media.pressure"]);
  conclude(run, undefined);
  expect(run.verdict).toBe("fail");
  expect(run.reasons).toEqual(["incomplete evidence: acceptance, media.pressure"]);
});

test("audio is required only when the session offered it, and takeover needs its own fields", () => {
  const offered = vertical();
  offered.media = { ...offered.media!, audioOffered: true };
  expect(missing(offered)).toEqual(["media.audio"]);
  const takeover: Draft = { ...vertical(), check: "takeover" };
  expect(missing(takeover)).toEqual([
    "takeover.attachMs",
    "takeover.clipIdentified",
    "takeover.metadataPreserved",
    "takeover.enqueuesAfterAttach",
    "takeover.firstFreshFrameMs",
    "takeover.video",
  ]);
});

test("the resume check needs the takeover's fields and the library's own close report", () => {
  const resume: Draft = { ...vertical(), check: "resume" };
  expect(missing(resume)).toEqual([
    "takeover.attachMs",
    "takeover.clipIdentified",
    "takeover.metadataPreserved",
    "takeover.enqueuesAfterAttach",
    "takeover.firstFreshFrameMs",
    "takeover.video",
    "termination.close",
  ]);
});

test("the audio check also needs the deployment's declaration and what the clip reports", () => {
  const audio: Draft = { ...vertical(), check: "audio" };
  expect(missing(audio)).toEqual(["contract.referenceAudio", "acceptance.references"]);
  audio.contract = { ...audio.contract!, referenceAudio: true };
  audio.acceptance = {
    ...audio.acceptance!,
    references: {
      images: 1,
      audio: 1,
      reportedImages: 1,
      reportedAudio: null,
      hasReferenceAudio: null,
    },
  };
  expect(missing(audio)).toEqual([]);
  conclude(audio, undefined);
  const json = JSON.parse(JSON.stringify(Schema.encodeSync(Evidence)(audio))) as unknown;
  expect(Schema.decodeUnknownSync(Evidence)(json)).toEqual(audio);
  expect(summarize([audio])).toContain("the clip reports 1 image(s), ? audio");
});

test("a stop rule, a failed criterion, a failure and no criteria each fail the run", () => {
  const unknown = vertical();
  unknown.outcomes = ["unknown"];
  conclude(unknown, undefined);
  expect(unknown.reasons).toEqual(["an outcome is unknown; the check stops and is not repeated"]);
  const failed = vertical();
  failed.criteria = [
    { name: "live video", passed: false, detail: "every frame was black" },
    { name: "correlated acceptance", passed: true },
  ];
  conclude(failed, "Timeout: a step ran past its deadline");
  expect(failed.reasons).toEqual([
    "Timeout: a step ran past its deadline",
    "live video: every frame was black",
  ]);
  const unjudged = vertical();
  unjudged.criteria = [];
  conclude(unjudged, undefined);
  expect(unjudged.verdict).toBe("fail");
  expect(unjudged.reasons).toEqual(["no criterion was evaluated"]);
});

test("the writer claims a new file, replaces it atomically, and refuses a credential", () => {
  const ledger = directory();
  const path = join(ledger, "run.json");
  const secrets: string[] = ["reactor-api-key-0123456789"];
  const writer = new Writer(path, () => secrets);
  const run = vertical();
  writer.save(run);
  run.milestones.push({ atMs: 2_000, step: "connected" });
  writer.save(run);
  expect(readLedger(ledger).map((entry) => entry.evidence.milestones.length)).toEqual([3]);
  expect(readdirSync(ledger)).toEqual(["run.json"]);
  // A second run never overwrites the first one's evidence.
  expect(() => new Writer(path, () => []).save(vertical())).toThrow();
  secrets.push("eyJhbGciOiJIUzI1NiJ9.session-token");
  run.environment = { ...run.environment, network: "eyJhbGciOiJIUzI1NiJ9.session-token" };
  expect(() => writer.save(run)).toThrow("would contain a credential");
  expect(readFileSync(path, "utf8")).not.toContain("session-token");
});

test("the ledger reserves each paid run's worst case and refuses unreadable evidence", () => {
  const paid = vertical();
  const rehearsal: Draft = { ...vertical(), mode: "rehearsal" };
  const { grant: _grant, ...admittedOnly }: Draft = vertical();
  const { worstCaseUsd: _worst, ...budget } = paid.budget;
  const grantWithoutReservation: Draft = { ...vertical(), budget };
  const { grant: _unused, ...beforeAdmission } = grantWithoutReservation;
  expect(reservedUsd(paid)).toBe(0.1);
  expect(reservedUsd(rehearsal)).toBe(0);
  expect(reservedUsd(admittedOnly)).toBe(0.1);
  // A grant without a recorded worst case reserves the most a check may spend.
  expect(reservedUsd(grantWithoutReservation)).toBe(0.75);
  expect(reservedUsd({ ...grantWithoutReservation, check: "scheduler" })).toBe(1.5);
  expect(reservedUsd(beforeAdmission)).toBe(0);
  const ledger = directory();
  new Writer(join(ledger, "a.json"), () => []).save(paid);
  expect(readLedger(ledger)).toHaveLength(1);
  expect(readLedger(join(ledger, "absent"))).toEqual([]);
  writeFileSync(join(ledger, "b.json"), "{ not evidence");
  expect(() => readLedger(ledger)).toThrow(Refused);
});

test("scheduler evidence requires a separately confirmed replacement close", () => {
  const run: Draft = {
    ...vertical(),
    check: "scheduler",
    scheduler: {
      replacement: {
        grant: { maxSessions: 1, maxSessionSeconds: 50, expiresAt: 1_790_000_000 },
        session: { id: "replacement", allocatedMs: 15_000 },
      },
      builds: [
        {
          clipId: "first",
          requestedSeconds: 5,
          submittedMs: 2_000,
          readyMs: 4_000,
          readySeconds: 5,
        },
        {
          clipId: "second",
          requestedSeconds: 15,
          submittedMs: 3_000,
          readyMs: 7_000,
          readySeconds: 15,
        },
      ],
      latencyByRequestedSeconds: [],
      readyMove: { clipId: "second", replyMs: 8_000, elapsedMs: 50, queue: "playout", position: 0 },
      positionZero: {
        buildingClipId: "first",
        requestedClipId: "second",
        generationOrder: ["first", "second"],
      },
      poppedBuild: {
        clipId: "popped",
        wasGeneration: true,
        poppedMs: 9_000,
        observedUntilMs: 10_000,
        generatedAfterPop: false,
        startedAfterPop: false,
      },
      metadata: { observed: { clip_queued: 2 }, mismatched: {} },
      decodedHandoff: { oldLastFrameMs: 14_000, replacementFirstFrameMs: 16_000, gapMs: 2_000 },
    },
  };
  expect(missing(run)).toEqual([
    "scheduler.replacement.session.endedMs",
    "scheduler.replacement.termination",
  ]);
  run.scheduler = {
    ...run.scheduler!,
    replacement: {
      ...run.scheduler!.replacement,
      session: { ...run.scheduler!.replacement.session!, endedMs: 22_000 },
      termination: { requestedMs: 21_000, reportedMs: 22_000, confirmed: false },
    },
  };
  expect(missing(run)).toEqual([]);
  conclude(run, undefined);
  expect(run.verdict).toBe("fail");
  expect(run.reasons).toContain(
    "remote termination is unconfirmed; the session may still be billing",
  );
});

test("one run at a time holds the ledger", () => {
  const ledger = directory();
  const release = lockLedger(ledger);
  expect(() => lockLedger(ledger)).toThrow(Refused);
  release();
  lockLedger(ledger)();
});

test("the summary names every run and what each saw", () => {
  const run = vertical();
  conclude(run, undefined);
  const text = summarize([run]);
  expect(text).toContain("| 3f2a9c1e | vertical | paid | pass |");
  expect(text).toContain("described +0.20 s, ready +1.70 s");
  expect(text).toContain("144 frames, 1344x768 BGRA, 23.9 fps");
  expect(text).toContain("srflx to host, RTT median 38 ms");
  expect(text).toContain("confirmed; coordinator terminal +0.20 s after the request");
  expect(text).toContain("✓ correlated acceptance");
});
