/** Offline evidence seams for the public renewing scheduler qualification. */
import { readFileSync, readdirSync } from "node:fs";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { expect, test } from "bun:test";
import { Clock, Effect, Schema, Stream } from "effect";
import { TestClock } from "effect/testing";
import type { VideoFrame } from "reactor-effect-client/host";
import {
  elapsedClock,
  readInto,
  VideoReader,
  FrameAttribution,
  AudioReader,
} from "../hosted/collect.js";

import * as Reactor from "reactor-effect-client";
import type * as Orchestration from "reactor-effect-client/orchestration";
import {
  Evidence,
  SchedulerRenewal,
  cleanupInstructions,
  conclude,
  format,
  renewalCriteria,
  renewalJudgments,
  confirmedOwnedCleanup,
  reservedUsd,
  type Draft,
} from "../hosted/evidence.js";
import { admit } from "../hosted/gates.js";
import { summarize } from "../hosted/report.js";

const frame = (sequence: bigint): VideoFrame => ({
  _tag: "VideoFrame",
  track: "video",
  format: "BGRA",
  width: 1,
  height: 1,
  data: new Uint8Array([80, 80, 80, 255]),
  metadata: new Uint8Array(),
  sequence,
  frameId: 0n,
  timestampMicros: 0n,
});

test("renewal elapsed clock ignores wall corrections and injects the same origin into media readers", () =>
  Effect.runPromise(
    Effect.gen(function* () {
      const base = yield* Clock.Clock;
      const testClock = yield* TestClock.testClockWith(Effect.succeed);
      let wall = 0;
      const shifted: Clock.Clock = {
        ...base,
        currentTimeMillis: Effect.sync(() => wall),
        currentTimeMillisUnsafe: () => wall,
        currentTimeNanos: Effect.sync(() => BigInt(wall) * 1_000_000n),
        currentTimeNanosUnsafe: () => BigInt(wall) * 1_000_000n,
      };
      const time = yield* elapsedClock.pipe(Effect.provideService(Clock.Clock, shifted));
      const reader = new VideoReader();
      yield* testClock.adjust(125);
      wall = 90_000;
      expect(time.now()).toBe(125);
      yield* readInto(Stream.make({ _tag: "Frame", frame: frame(0n) }), reader, time.elapsed);
      yield* testClock.adjust(25);
      wall = -90_000;
      yield* readInto(Stream.make({ _tag: "Frame", frame: frame(1n) }), reader, time.elapsed);
      expect(reader.summary().arrivalsMs).toEqual([125, 150]);
    }).pipe(Effect.provide(TestClock.layer())),
  ));

test("attribution preserves objects and resets recorder accounting at source and generation boundaries", () =>
  Effect.runPromise(
    Effect.gen(function* () {
      const attribution = new FrameAttribution();
      const a = frame(5n),
        aLost = frame(8n),
        b = frame(100n),
        b2 = frame(103n);
      const tagged = Stream.concat(
        attribution.tag(Stream.make(a, aLost), { sessionId: "a", generation: 1n }),
        Stream.concat(
          attribution.tag(Stream.make(b), { sessionId: "b", generation: 1n }),
          attribution.tag(Stream.make(b2), { sessionId: "b", generation: 2n }),
        ),
      );
      const recorded = yield* attribution.recorded(tagged).pipe(Stream.runCollect);
      expect(recorded.map((event) => event._tag)).toEqual([
        "Frame",
        "Lost",
        "Frame",
        "Frame",
        "Frame",
      ]);
      if (recorded[0]?._tag !== "Frame") throw new Error("Missing frame");
      expect(recorded[0].frame).toBe(a);
      expect(attribution.get(a)).toEqual({ sessionId: "a", generation: 1n });
      expect(attribution.get(b2)).toEqual({ sessionId: "b", generation: 2n });
      expect(attribution.get(frame(200n))).toBeUndefined();
      expect(recorded[1]).toMatchObject({ _tag: "Lost", count: 2n });
      // An object a second source re-emits has no single source, so it counts as untagged.
      yield* attribution
        .tag(Stream.make(a), { sessionId: "b", generation: 1n })
        .pipe(Stream.runDrain);
      expect(attribution.get(a)).toBeUndefined();
    }),
  ));

const canonical = (sessionId: string): Orchestration.SourceCleanup => ({
  lease: {
    localClosed: true,
    allocation: "known",
    ownership: "owned",
    sessionId,
    remote: {
      attempted: true,
      responseReceived: true,
      confirmed: true,
      evidence: "terminal",
      deleteStatus: 202,
      state: "CLOSED",
    },
    localErrors: [],
    unresolvedPublications: [],
    unpublishSubmitted: [],
  },
  policy: [],
});
const summary = (...times: number[]) => {
  const reader = new VideoReader();
  for (const [index, at] of times.entries())
    reader.add({ _tag: "Frame", frame: frame(BigInt(index)) }, at);
  return reader.summary();
};
const renewal = (): SchedulerRenewal => {
  const reportA = canonical("a"),
    reportB = canonical("b"),
    audio = new AudioReader().summary();
  return {
    version: 1,
    clock: "effect-monotonic",
    scenario: "two-source-accepted-drain",
    configuration: {
      constructor: "legacy",
      setupLimitMs: 20000,
      workLimitMs: 40000,
      cleanupLimitMs: 20000,
      leadMs: 40000,
      graceMs: 250,
      clipSeconds: 5,
      maxSessions: 2,
      maxOpenAttempts: 2,
    },
    openAttempts: 2,
    fillerRequests: 0,
    fillerEvents: [],
    allocations: [
      {
        slot: 1,
        sessionId: "a",
        allocatedMs: 0,
        allocatedAt: "2026-09-26T00:00:00Z",
        capEndsAt: "2026-09-26T00:00:50Z",
        grant: { maxSessions: 1, maxSessionSeconds: 50, expiresAt: 110 },
        closeRequestedMs: 10500,
        closedMs: 10501,
        cleanup: reportA,
      },
      {
        slot: 2,
        sessionId: "b",
        allocatedMs: 10000,
        allocatedAt: "2026-09-26T00:00:10Z",
        capEndsAt: "2026-09-26T00:01:00Z",
        grant: { maxSessions: 1, maxSessionSeconds: 50, expiresAt: 110 },
        closeRequestedMs: 16001,
        closedMs: 16002,
        cleanup: reportB,
      },
    ],
    items: [
      {
        key: "qualification-A",
        requestedSeconds: 5,
        clipId: "clip-a",
        sessionId: "a",
        statuses: [
          { _tag: "Started", atMs: 100, sessionId: "a" },
          { _tag: "Ended", atMs: 5100, termination: "finished" },
        ],
      },
      {
        key: "qualification-B",
        requestedSeconds: 5,
        clipId: "clip-b",
        sessionId: "b",
        statuses: [
          { _tag: "Building", atMs: 10200 },
          { _tag: "Ready", atMs: 10300, sessionId: "b" },
          { _tag: "Started", atMs: 11000, sessionId: "b" },
          { _tag: "Ended", atMs: 16000, termination: "finished" },
        ],
      },
    ],
    prepared: { atMs: 10100, preferredSessionId: "b" },
    switches: [
      {
        atMs: 10501,
        retiringSessionId: "a",
        handoff: {
          replacementSessionId: "b",
          decision: "count-complete",
          finalClip: {
            _tag: "Observed",
            clipId: "clip-a",
            expectedVideoFrames: 1,
            receivedVideoFrames: 1,
            videoStatus: "count-complete",
          },
          grace: { _tag: "NotObserved" },
        },
        tail: {
          video: {
            framesPerSecond: 24,
            expectedFrames: 1,
            receivedFrames: 1,
            status: "count-complete",
          },
          audio: { receivedSamples: 0, status: "unverified" },
          sourceDrops: { video: "0", audio: "0" },
          forwarded: { queuedVideoFrames: 0, queuedAudioSamples: 0 },
        },
      },
    ],
    media: {
      video: summary(5000, 11000),
      audio,
      audioCompleteness: "unverified",
      attributionComplete: true,
      sources: [
        { sessionId: "a", generation: "1", video: summary(5000), audio },
        { sessionId: "b", generation: "1", video: summary(11000), audio },
      ],
      decodedBoundary: {
        retiringSessionId: "a",
        replacementSessionId: "b",
        lastRetiringFrameMs: 5000,
        firstReplacementFrameMs: 11000,
        gapMs: 6000,
      },
    },
    drain: {
      requestedMs: 10201,
      completedMs: 16001,
      acceptedKeys: ["qualification-A", "qualification-B"],
      outcome: "completed",
      allocationsWhenRequested: 2,
      allocationsWhenCompleted: 2,
    },
    cleanup: {
      _tag: "Legacy",
      requestedMs: 16001,
      completedMs: 16002,
      incomplete: [],
      report: { sessions: [reportA, reportB] },
    },
  };
};
const draft = (schedulerRenewal = renewal()): Draft => ({
  format,
  runId: "fixture",
  check: "scheduler-renewal",
  mode: "rehearsal",
  startedAt: "2026-09-26T00:00:00Z",
  environment: {
    runtime: "fixture",
    os: "fixture",
    packages: {},
    network: "loopback",
    apiOrigin: "http://127.0.0.1",
  },
  budget: {
    checkUsd: 1.5,
    totalUsd: 3.75,
    reservedBeforeUsd: 0,
    sessionSeconds: 50,
    rate: { creditsPerSecond: 10, creditsPerDollar: 6000 },
    worstCaseUsd: 0.2,
    estimatedUsd: 0.2,
  },
  milestones: [],
  spans: [],
  outcomes: [],
  missing: [],
  reasons: [],
  schedulerRenewal,
  criteria: renewalCriteria.map((name) => ({ name, passed: true })),
});

test("complete legacy renewal evidence encodes, while absent milestone fields remain valid incomplete checkpoints", () => {
  const run = draft();
  conclude(run, undefined);
  expect(run.verdict).toBe("pass");
  expect(Schema.decodeSync(Evidence)(Schema.encodeSync(Evidence)(run))).toEqual(run);
  const partial = draft({
    ...renewal(),
    allocations: [],
    items: [],
    switches: [],
    openAttempts: 0,
  });
  conclude(partial, undefined);
  expect(partial.verdict).toBe("fail");
  expect(() => Schema.encodeSync(Evidence)(partial)).not.toThrow();
});

test("renewal refuses incomplete or contradictory evidence even when supplied criteria all pass", () => {
  const original = renewal();
  const invalid: readonly SchedulerRenewal[] = [
    { ...original, switches: [] },
    { ...original, openAttempts: 3 },
    { ...original, allocations: original.allocations.map((slot) => ({ ...slot, sessionId: "a" })) },
    {
      ...original,
      allocations: original.allocations.map((slot) =>
        slot.slot === 2 ? { ...slot, cleanup: canonical("wrong") } : slot,
      ),
    },
    { ...original, media: { ...original.media, attributionComplete: false } },
    {
      ...original,
      media: { ...original.media, video: { ...original.media.video, arrivalsMs: [0, 1] } },
    },
    {
      ...original,
      allocations: original.allocations.map((slot) => ({ ...slot, slot: slot.slot === 1 ? 2 : 1 })),
    },
    { ...original, media: { ...original.media, sources: original.media.sources.slice(0, 1) } },
    {
      ...original,
      items: original.items.map((item) =>
        item.key === "qualification-B" ? { ...item, statuses: [] } : item,
      ),
    },
    { ...original, drain: { ...original.drain!, allocationsWhenCompleted: 3 } },
    {
      ...original,
      cleanup: {
        _tag: "Legacy",
        requestedMs: 16001,
        report: { sessions: [canonical("a")] },
        incomplete: [],
      },
    },
  ];
  for (const evidence of invalid) {
    const run = draft(evidence);
    conclude(run, undefined);
    expect(run.verdict).toBe("fail");
  }
  const noCriterion = draft();
  noCriterion.criteria.pop();
  conclude(noCriterion, undefined);
  expect(noCriterion.reasons.join(" ")).toContain("missing required criterion");
  // A drain that never completed says so; no deadline elapsed here.
  const { completedMs: _completed, ...requested } = original.drain!;
  const pending = draft({ ...original, drain: { ...requested, outcome: "pending" } });
  conclude(pending, undefined);
  expect(pending.reasons.join(" ")).toContain("accepted drain must finish");
  expect(pending.reasons.join(" ")).not.toContain("deadline");
});

test("a renewal summary claims no pass or confirmed cleanup its evidence does not prove", () => {
  const { schedulerRenewal: _omitted, ...bare } = draft();
  const text = summarize([{ ...bare, verdict: "pass", criteria: [] }]);
  expect(text).toContain("| fixture | scheduler-renewal | rehearsal | fail |");
  expect(text).toContain("### scheduler-renewal: fail");
  // A confirmed flag without canonical terminal evidence is not a confirmed owned termination.
  const original = renewal(),
    lease = canonical("a").lease;
  const weak = draft({
    ...original,
    allocations: original.allocations.map((slot) =>
      slot.slot === 1
        ? {
            ...slot,
            cleanup: {
              lease: { ...lease, remote: { ...lease.remote, evidence: null } },
              policy: [],
            },
          }
        : slot,
    ),
  });
  conclude(weak, undefined);
  const weakText = summarize([weak]);
  expect(weakText).toContain("**Source 1:** a; canonical owned termination UNCONFIRMED");
  expect(weakText).not.toContain("canonical source reports; complete");
});

test("renewal refuses event orders the harness flow cannot produce", () => {
  const original = renewal(),
    switched = original.switches[0]!,
    [a, b] = original.items;
  const impossible: readonly SchedulerRenewal[] = [
    // Switched is announced only after the retiring source's close returned.
    { ...original, switches: [{ ...switched, atMs: 0 }] },
    // Cleanup begins only once the switch was recorded.
    { ...original, switches: [{ ...switched, atMs: 16_003 }] },
    // Prepared follows the replacement's allocation, and B follows Prepared.
    {
      ...original,
      prepared: { ...original.prepared!, atMs: 9_000 },
      items: [
        a!,
        {
          ...b!,
          statuses: [
            { _tag: "Building", atMs: 9_100 },
            { _tag: "Ready", atMs: 9_200, sessionId: "b" },
            ...b!.statuses.slice(2),
          ],
        },
      ],
    },
    // The first source opens while the owner is built, before A is submitted.
    {
      ...original,
      allocations: original.allocations.map((slot) =>
        slot.slot === 1 ? { ...slot, allocatedMs: 200 } : slot,
      ),
    },
    // The SDK reports the grace it was configured with.
    {
      ...original,
      switches: [
        {
          ...switched,
          handoff: {
            replacementSessionId: "b",
            decision: "grace-elapsed",
            finalClip: {
              _tag: "Observed",
              clipId: "clip-a",
              expectedVideoFrames: 2,
              receivedVideoFrames: 1,
              videoStatus: "incomplete",
            },
            grace: { _tag: "Observed", origin: "Ended", elapsedMs: 0, limitMs: 0 },
          },
        },
      ],
    },
  ];
  const nominal = draft(original);
  conclude(nominal, undefined);
  expect(nominal.verdict).toBe("pass");
  const verdicts = impossible.map((evidence) => {
    // Each order is valid to the codec; only the cross-field judgment can refuse it.
    const run = draft(Schema.decodeUnknownSync(SchedulerRenewal)(evidence));
    conclude(run, undefined);
    return run.verdict;
  });
  expect(verdicts).toEqual(impossible.map(() => "fail"));
  // A failure can start cleanup before the drain completes and the switch is recorded.
  const { completedMs: _completed, ...requested } = original.drain!;
  const failed = renewalJudgments(
    draft({
      ...original,
      drain: { ...requested, outcome: "pending" },
      switches: [{ ...switched, atMs: 16_003 }],
    }),
  );
  expect(failed.find((criterion) => criterion.name === "planned switch")?.passed).toBe(true);
});

test("handoff codec rejects impossible count and grace claims", () => {
  const original = renewal(),
    switched = original.switches[0]!,
    handoff = switched.handoff!;
  for (const candidate of [
    { ...handoff, decision: "grace-elapsed" },
    {
      ...handoff,
      finalClip: {
        _tag: "Observed",
        clipId: "x",
        expectedVideoFrames: -1,
        receivedVideoFrames: 0,
        videoStatus: "count-complete",
      },
    },
    {
      ...handoff,
      grace: { _tag: "Observed", origin: "Ended", elapsedMs: Number.NaN, limitMs: 250 },
    },
  ])
    expect(() =>
      Schema.decodeUnknownSync(SchedulerRenewal)({
        ...original,
        switches: [{ ...switched, handoff: candidate }],
      }),
    ).toThrow();
});

test("admission reserves the two-session worst case rounded up to the ledger's precision", () => {
  // Two billed minutes at 1 credit/s and 9,000 credits a dollar cost $0.013333…;
  // four-decimal rounding reserved $0.0133, below what the run's own judgment requires.
  const rate = { creditsPerSecond: 1, creditsPerDollar: 9_000 };
  const reserved = admit(rate, 1.5, 2);
  expect(reserved).toBe(0.0134);
  const run = draft();
  run.budget = { ...run.budget, rate, worstCaseUsd: reserved };
  conclude(run, undefined);
  expect(run.verdict).toBe("pass");
});

test("cleanup instructions stay printable when a recorded grant expiry is out of range", () => {
  // Token expiry is only bounded below; formatting it must not throw after a run has spent.
  const original = renewal();
  const run = draft({
    ...original,
    allocations: [
      original.allocations[0]!,
      {
        slot: 2,
        allocation: "unknown",
        grant: { maxSessions: 1, maxSessionSeconds: 50, expiresAt: 1e20 },
      },
    ],
  });
  expect(cleanupInstructions(run)).toContain("Source 2's allocation outcome is unknown");
});

test("a partial new two-session paid run retains the full ceiling without a reservation", () => {
  const run = draft();
  run.mode = "paid";
  delete run.budget.worstCaseUsd;
  expect(reservedUsd(run)).toBe(1.5);
});

test("every committed historical ledger still decodes and preserves its reservation", () => {
  const root = fileURLToPath(new URL("../hosted/evidence/", import.meta.url));
  const files = readdirSync(root, { recursive: true, encoding: "utf8" }).filter((name) =>
    name.endsWith(".json"),
  );
  expect(files.length).toBeGreaterThan(0);
  for (const file of files) {
    const decoded = Schema.decodeUnknownSync(Evidence)(
      JSON.parse(readFileSync(join(root, file), "utf8")),
    );
    expect(reservedUsd(decoded)).toBe(decoded.budget.worstCaseUsd!);
  }
});

test("canonical owned cleanup rejects contradictory confirmation and failed required evidence", () => {
  const original = canonical("a");
  expect(confirmedOwnedCleanup(original, "a")).toBe(true);
  for (const remote of [
    { ...original.lease.remote, error: Reactor.ReactorError.fromCode("Shutdown", "failed") },
    { ...original.lease.remote, evidence: null },
    { ...original.lease.remote, confirmed: false },
    { ...original.lease.remote, attempted: false },
    { ...original.lease.remote, responseReceived: false },
  ])
    expect(confirmedOwnedCleanup({ ...original, lease: { ...original.lease, remote } }, "a")).toBe(
      false,
    );
});

const continuous = (): SchedulerRenewal => ({
  ...renewal(),
  configuration: {
    ...renewal().configuration,
    constructor: "continuous",
    retainedSuccessfulCleanups: 1,
    maxUnresolvedCleanups: 2,
  },
  cleanup: {
    _tag: "Continuous",
    requestedMs: 16001,
    completedMs: 16002,
    incomplete: [],
    summary: {
      format: "reactor-orchestration-cleanup-summary/v1",
      totalRetirements: 2n,
      omittedComplete: { ownedTerminated: 1n, noAllocation: 0n, attachedDetached: 0n },
      retained: [
        {
          ordinal: 2n,
          source: { sessionId: "b", incarnation: 2n },
          cleanup: canonical("b"),
          retirement: {
            accounting: "settled",
            scope: "closed",
            affinity: "retired",
            unknownSubmissions: 0n,
            errors: [],
          },
          disposition: "complete",
        },
      ],
      exhausted: false,
    },
  },
});

test("continuous summary reconciles compaction with original source reports and gates paid qualification", () => {
  const evidence = draft(continuous());
  evidence.mode = "paid";
  conclude(evidence, undefined);
  expect(evidence.verdict).toBe("pass");
  const encoded = Schema.encodeSync(Evidence)(evidence);
  expect(Schema.decodeSync(Evidence)(encoded)).toEqual(evidence);
  expect(JSON.stringify(encoded)).toContain('"totalRetirements":"2"');
  const legacy = draft();
  legacy.mode = "paid";
  conclude(legacy, undefined);
  expect(legacy.verdict).toBe("fail");
  expect(legacy.reasons.join(" ")).toContain("requires the continuous constructor");
});

test("continuous omissions cannot replace missing canonical cleanup or hide incomplete retained evidence", () => {
  const original = continuous();
  if (original.cleanup?._tag !== "Continuous" || original.cleanup.summary === undefined)
    throw new Error("Missing fixture summary");
  const cleanup = original.cleanup,
    summary = cleanup.summary!,
    row = summary.retained[0]!;
  const summaries: readonly Orchestration.CleanupSummary[] = [
    { ...summary, exhausted: true },
    { ...summary, totalRetirements: 3n },
    {
      ...summary,
      omittedComplete: { ...summary.omittedComplete, ownedTerminated: 0n, noAllocation: 1n },
    },
    { ...summary, retained: [{ ...row, source: { sessionId: "a", incarnation: 1n } }] },
    { ...summary, retained: [{ ...row, cleanup: canonical("wrong") }] },
    { ...summary, retained: [{ ...row, conflictingCleanup: canonical("b") }] },
    { ...summary, retained: [{ ...row, disposition: "incomplete" }] },
    {
      ...summary,
      retained: [{ ...row, retirement: { ...row.retirement, accounting: "timed-out" } }],
    },
    {
      ...summary,
      retained: [{ ...row, retirement: { ...row.retirement, unknownSubmissions: 1n } }],
    },
  ];
  for (const candidate of summaries) {
    const evidence = draft({ ...original, cleanup: { ...cleanup, summary: candidate } });
    conclude(evidence, undefined);
    expect(evidence.verdict).toBe("fail");
  }
  const missing = draft({
    ...original,
    allocations: original.allocations.map(({ cleanup: _cleanup, ...slot }) => slot),
  });
  conclude(missing, undefined);
  expect(missing.verdict).toBe("fail");
  const wrongKind = draft({ ...original, cleanup: renewal().cleanup! });
  conclude(wrongKind, undefined);
  expect(wrongKind.verdict).toBe("fail");
});

test("constructor schema requires its actual retention settings", () => {
  const original = continuous();
  for (const configuration of [
    { ...original.configuration, retainedSuccessfulCleanups: 2 },
    { ...original.configuration, maxUnresolvedCleanups: undefined },
    { ...original.configuration, constructor: "legacy" },
  ])
    expect(() =>
      Schema.decodeUnknownSync(SchedulerRenewal)({ ...original, configuration }),
    ).toThrow();
});
