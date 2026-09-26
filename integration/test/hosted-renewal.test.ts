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
  conclude,
  format,
  renewalCriteria,
  confirmedOwnedCleanup,
  reservedUsd,
  type Draft,
} from "../hosted/evidence.js";

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
    { ...original, media: { ...original.media, sources: original.media.sources.slice(0, 1) } },
    {
      ...original,
      items: original.items.map((item) =>
        item.key === "qualification-B" ? { ...item, statuses: [] } : item,
      ),
    },
    { ...original, drain: { ...original.drain!, allocationsWhenCompleted: 3 } },
    { ...original, cleanup: { ...original.cleanup!, report: { sessions: [canonical("a")] } } },
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
  ])
    expect(confirmedOwnedCleanup({ ...original, lease: { ...original.lease, remote } }, "a")).toBe(
      false,
    );
});
