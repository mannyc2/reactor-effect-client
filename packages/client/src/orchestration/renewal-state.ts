import type { ClipId } from "./request.js";
import type { Renewal } from "./types.js";

/** Pure renewal policy. Inputs are observations, never resource handles or effects. */
export type RecoveryMode = "reconnect" | "replace";

export type SourcePhase =
  | { readonly _tag: "Active" }
  | { readonly _tag: "Recovering"; readonly mode: RecoveryMode }
  | { readonly _tag: "Closing" }
  | { readonly _tag: "Closed" };

export type SourceTransition =
  | { readonly _tag: "Recover"; readonly mode: RecoveryMode }
  | { readonly _tag: "Recovered" }
  | { readonly _tag: "Close" }
  | { readonly _tag: "Closed" };

/** Recovery can escalate, but neither a late completion nor another error can reopen a source. */
export const transitionSource = (phase: SourcePhase, event: SourceTransition): SourcePhase => {
  switch (phase._tag) {
    case "Closed":
      return phase;
    case "Closing":
      return event._tag === "Closed" ? { _tag: "Closed" } : phase;
    case "Active":
      switch (event._tag) {
        case "Recover":
          return { _tag: "Recovering", mode: event.mode };
        case "Close":
          return { _tag: "Closing" };
        default:
          return phase;
      }
    case "Recovering":
      switch (event._tag) {
        case "Recover":
          return event.mode === "replace" ? { _tag: "Recovering", mode: "replace" } : phase;
        case "Recovered":
          return phase.mode === "reconnect" ? { _tag: "Active" } : phase;
        case "Close":
          return { _tag: "Closing" };
        default:
          return phase;
      }
  }
};

export const isClosed = (phase: SourcePhase): boolean =>
  phase._tag === "Closing" || phase._tag === "Closed";

export interface Lifetime {
  /** When the source opened, in monotonic milliseconds, so wall-clock corrections cannot age it. */
  readonly openedAt: number;
  readonly maxSeconds: number;
}

export const ageMillis = (source: Lifetime, now: number): number => now - source.openedAt;
export const expired = (source: Lifetime, now: number): boolean =>
  ageMillis(source, now) >= source.maxSeconds * 1000;

/** Recovery shares the remote lifetime; post-lease accounting has a separate cleanup budget. */
export const recoveryBudget = (source: Lifetime, now: number, timeoutMs: number): number =>
  Math.min(timeoutMs, Math.max(1, source.maxSeconds * 1000 - ageMillis(source, now)));

export interface RenewalFacts {
  readonly running: boolean;
  /** Monotonic milliseconds, as `Lifetime.openedAt` and `retryAt` are. */
  readonly now: number;
  readonly current: (Lifetime & { readonly phase: SourcePhase }) | undefined;
  readonly replacement: "Absent" | "Opening" | SourcePhase;
  readonly leadSeconds: number;
  readonly retryAt: number;
}

export type RenewalDecision = "Retain" | "Prepare" | "InspectHandoff" | "Expire";

/** Expiry is checked before sequence, queue, or media readiness can delay retirement. */
export const decideRenewal = (facts: RenewalFacts): RenewalDecision => {
  const current = facts.current;
  if (!facts.running || current?.phase._tag !== "Active") return "Retain";
  if (expired(current, facts.now)) return "Expire";
  if (facts.replacement === "Absent") {
    return ageMillis(current, facts.now) >= (current.maxSeconds - facts.leadSeconds) * 1000 &&
      facts.now >= facts.retryAt
      ? "Prepare"
      : "Retain";
  }
  return facts.replacement !== "Opening" && facts.replacement._tag === "Active"
    ? "InspectHandoff"
    : "Retain";
};

/** The retiring source's last started clip, as its owner has observed it. */
export interface FinalClip {
  readonly video: "not-started" | "count-complete" | "incomplete";
  readonly clipId: ClipId | undefined;
  readonly expectedVideoFrames: number;
  readonly receivedVideoFrames: number;
  /** Monotonic milliseconds since Ended or the idle fallback; undefined while it plays. */
  readonly endedAgoMs: number | undefined;
  readonly graceOrigin: "Ended" | "Idle" | undefined;
}

export interface HandoffFacts {
  readonly sequenceOpen: boolean;
  readonly currentIdle: boolean;
  readonly replacementReady: boolean;
  readonly finalClip: FinalClip;
  readonly graceMs: number;
}

/**
 * Media and provider events arrive on separate readers, so a clip's last frames
 * can land after its Ended. A frame the provider never sent never lands, so a
 * short clip is waited on only for the grace past Ended; waiting for the count
 * instead left the retiring source idle until expiry. The shortfall is still
 * reported, with any earlier loss, on the retirement's tail.
 */
export const finalClipSettled = (
  clip: Pick<FinalClip, "video" | "endedAgoMs">,
  graceMs: number,
): boolean =>
  clip.video !== "incomplete" || (clip.endedAgoMs !== undefined && clip.endedAgoMs >= graceMs);

type Handoff = NonNullable<Extract<Renewal, { readonly _tag: "Switched" }>["handoff"]>;

/** One decision owns both admission and evidence, so the two cannot disagree at a grace boundary. */
export const handoffEvidence = (
  facts: HandoffFacts,
): Omit<Handoff, "replacementSessionId"> | undefined => {
  if (
    facts.sequenceOpen ||
    !facts.currentIdle ||
    !facts.replacementReady ||
    !finalClipSettled(facts.finalClip, facts.graceMs)
  )
    return undefined;
  const clip = facts.finalClip;
  return Object.freeze({
    decision:
      clip.clipId === undefined
        ? "no-observed-start"
        : clip.video === "incomplete"
          ? "grace-elapsed"
          : "count-complete",
    finalClip:
      clip.clipId === undefined
        ? Object.freeze({ _tag: "NoObservedStart" })
        : Object.freeze({
            _tag: "Observed",
            clipId: clip.clipId,
            expectedVideoFrames: clip.expectedVideoFrames,
            receivedVideoFrames: clip.receivedVideoFrames,
            videoStatus: clip.video === "incomplete" ? "incomplete" : "count-complete",
          }),
    grace:
      clip.endedAgoMs === undefined || clip.graceOrigin === undefined
        ? Object.freeze({ _tag: "NotObserved" })
        : Object.freeze({
            _tag: "Observed",
            origin: clip.graceOrigin,
            elapsedMs: clip.endedAgoMs,
            limitMs: facts.graceMs,
          }),
  });
};

export const canHandoff = (facts: HandoffFacts): boolean => handoffEvidence(facts) !== undefined;

export const needsReplacement = (
  phase: SourcePhase,
  indeterminate: boolean,
  isExpired: boolean,
): boolean =>
  indeterminate || isExpired || (phase._tag === "Recovering" && phase.mode === "replace");
