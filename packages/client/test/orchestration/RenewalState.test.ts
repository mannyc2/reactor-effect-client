import { expect, test } from "vitest";
import {
  canHandoff,
  finalClipSettled,
  handoffEvidence,
  type FinalClip,
  decideRenewal,
  isClosed,
  needsReplacement,
  recoveryBudget,
  transitionSource,
  type RenewalFacts,
  type SourcePhase,
  type SourceTransition,
} from "../../src/orchestration/renewal-state.js";

import { ClipId } from "../../src/orchestration/request.js";

const clip = (fields: Partial<FinalClip> = {}): FinalClip => ({
  video: "count-complete",
  clipId: ClipId.make("last"),
  expectedVideoFrames: 24,
  receivedVideoFrames: 24,
  endedAgoMs: undefined,
  graceOrigin: undefined,
  ...fields,
});

const active: SourcePhase = { _tag: "Active" };
const facts: RenewalFacts = {
  running: true,
  now: 0,
  current: { openedAt: 0, maxSeconds: 1, phase: active },
  replacement: "Absent",
  leadSeconds: 0.5,
  retryAt: 0,
};

test("expiry outranks replacement preparation and every warm-source state", () => {
  for (const replacement of [
    "Absent",
    "Opening",
    active,
    { _tag: "Recovering", mode: "replace" },
    { _tag: "Closing" },
    { _tag: "Closed" },
  ] as const) {
    for (const now of [1000, 1001, 10_000])
      expect(decideRenewal({ ...facts, replacement, now, retryAt: 20_000 })).toBe("Expire");
  }
});

test("preparation observes the lead boundary and backoff without allocating twice", () => {
  expect(decideRenewal({ ...facts, now: 499 })).toBe("Retain");
  expect(decideRenewal({ ...facts, now: 500 })).toBe("Prepare");
  expect(decideRenewal({ ...facts, now: 600, retryAt: 700 })).toBe("Retain");
  expect(decideRenewal({ ...facts, now: 700, retryAt: 700 })).toBe("Prepare");
  expect(decideRenewal({ ...facts, now: 700, replacement: "Opening" })).toBe("Retain");
  expect(decideRenewal({ ...facts, now: 700, replacement: active })).toBe("InspectHandoff");
  expect(
    decideRenewal({
      ...facts,
      now: 1_000_000,
      current: { openedAt: 0, maxSeconds: Infinity, phase: active },
    }),
  ).toBe("Retain");
});

test("a closing handle and an independently recovering source do not receive a second lifecycle operation", () => {
  expect(decideRenewal({ ...facts, now: 1000, running: false })).toBe("Retain");
  expect(decideRenewal({ ...facts, now: 1000, current: undefined })).toBe("Retain");
  for (const phase of [
    { _tag: "Recovering", mode: "reconnect" },
    { _tag: "Closing" },
    { _tag: "Closed" },
  ] as const)
    expect(
      decideRenewal({ ...facts, now: 1000, current: { openedAt: 0, maxSeconds: 1, phase } }),
    ).toBe("Retain");
});

test("a handoff requires complete independent queue, sequence and media evidence", () => {
  const ready = {
    sequenceOpen: false,
    currentIdle: true,
    replacementReady: true,
    finalClip: clip(),
    graceMs: 250,
  };
  expect(canHandoff(ready)).toBe(true);
  expect(
    canHandoff({ ...ready, finalClip: clip({ video: "not-started", clipId: undefined }) }),
  ).toBe(true);
  for (const missing of [
    { sequenceOpen: true },
    { currentIdle: false },
    { replacementReady: false },
    { finalClip: clip({ video: "incomplete", endedAgoMs: undefined }) },
    { finalClip: clip({ video: "incomplete", endedAgoMs: 249, graceOrigin: "Ended" }) },
  ])
    expect(canHandoff({ ...ready, ...missing })).toBe(false);
});

test("a short final clip hands off once its grace past Ended has elapsed", () => {
  const short = (endedAgoMs: number | undefined) => ({
    video: "incomplete" as const,
    endedAgoMs,
  });
  expect(finalClipSettled(short(undefined), 250)).toBe(false);
  expect(finalClipSettled(short(0), 250)).toBe(false);
  expect(finalClipSettled(short(249), 250)).toBe(false);
  expect(finalClipSettled(short(250), 250)).toBe(true);
  expect(finalClipSettled(short(0), 0)).toBe(true);
  // A clip still playing is never settled by the grace, however long it runs.
  expect(finalClipSettled(short(undefined), 0)).toBe(false);
  expect(finalClipSettled({ video: "count-complete", endedAgoMs: undefined }, 250)).toBe(true);
});

test("recovery uses remaining lifetime while cleanup can retain its full independent budget", () => {
  const source = { openedAt: 100, maxSeconds: 1 };
  expect(recoveryBudget(source, 900, 5000)).toBe(200);
  expect(recoveryBudget(source, 900, 50)).toBe(50);
  expect(recoveryBudget(source, 1100, 5000)).toBe(1);
  expect(recoveryBudget({ openedAt: 0, maxSeconds: Infinity }, 10_000, 5000)).toBe(5000);
  expect(needsReplacement(active, true, false)).toBe(true);
  expect(needsReplacement(active, false, true)).toBe(true);
});

test("all lifecycle traces through five events preserve closure and recovery escalation", () => {
  const alphabet: readonly SourceTransition[] = [
    { _tag: "Recover", mode: "reconnect" },
    { _tag: "Recover", mode: "replace" },
    { _tag: "Recovered" },
    { _tag: "Close" },
    { _tag: "Closed" },
  ];
  // Breadth-first enumeration reports a shortest failing trace, without wall-clock scheduling or a random seed.
  let traces: SourceTransition[][] = [[]];
  for (let depth = 0; depth <= 5; depth++) {
    for (const trace of traces) {
      let phase: SourcePhase = active;
      let closeRequested = false,
        finalized = false,
        recoveryRequested = false,
        escalation = false;
      for (const event of trace) {
        if (event._tag === "Close") closeRequested = true;
        if (event._tag === "Closed" && closeRequested) finalized = true;
        if (!closeRequested && event._tag === "Recover") {
          recoveryRequested = true;
          escalation ||= event.mode === "replace";
        }
        if (!closeRequested && event._tag === "Recovered" && !escalation) recoveryRequested = false;
        phase = transitionSource(phase, event);
      }
      const expected = finalized
        ? "Closed"
        : closeRequested
          ? "Closing"
          : recoveryRequested
            ? "Recovering"
            : "Active";
      if (
        phase._tag !== expected ||
        isClosed(phase) !== closeRequested ||
        (!closeRequested && needsReplacement(phase, false, false) !== escalation)
      ) {
        throw new Error(
          `Lifecycle trace ${JSON.stringify(trace)} produced ${JSON.stringify(phase)}`,
        );
      }
    }
    traces = traces.flatMap((trace) => alphabet.map((event) => [...trace, event]));
  }
});

test("handoff evidence preserves every policy decision at and around the grace boundary", () => {
  for (const video of ["not-started", "count-complete", "incomplete"] as const)
    for (const endedAgoMs of [undefined, 0, 249, 250, 251])
      for (const sequenceOpen of [false, true])
        for (const currentIdle of [false, true])
          for (const replacementReady of [false, true]) {
            const facts = {
              sequenceOpen,
              currentIdle,
              replacementReady,
              graceMs: 250,
              finalClip: clip({
                video,
                endedAgoMs,
                clipId: video === "not-started" ? undefined : ClipId.make("last"),
                receivedVideoFrames: video === "incomplete" ? 23 : 24,
                graceOrigin: endedAgoMs === undefined ? undefined : "Ended",
              }),
            };
            const expected =
              !sequenceOpen &&
              currentIdle &&
              replacementReady &&
              (video !== "incomplete" || (endedAgoMs !== undefined && endedAgoMs >= 250));
            expect(canHandoff(facts)).toBe(expected);
            const evidence = handoffEvidence(facts);
            expect(evidence !== undefined).toBe(expected);
            if (evidence === undefined) continue;
            expect(evidence.decision).toBe(
              video === "not-started"
                ? "no-observed-start"
                : video === "incomplete"
                  ? "grace-elapsed"
                  : "count-complete",
            );
            expect(evidence.finalClip).toEqual(
              video === "not-started"
                ? { _tag: "NoObservedStart" }
                : {
                    _tag: "Observed",
                    clipId: "last",
                    expectedVideoFrames: 24,
                    receivedVideoFrames: video === "incomplete" ? 23 : 24,
                    videoStatus: video,
                  },
            );
            expect(evidence.grace).toEqual(
              endedAgoMs === undefined
                ? { _tag: "NotObserved" }
                : { _tag: "Observed", origin: "Ended", elapsedMs: endedAgoMs, limitMs: 250 },
            );
            expect(Object.isFrozen(evidence)).toBe(true);
            expect(Object.isFrozen(evidence.finalClip)).toBe(true);
            expect(Object.isFrozen(evidence.grace)).toBe(true);
          }
});
