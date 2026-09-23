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

export interface HandoffFacts {
  readonly sequenceOpen: boolean;
  readonly currentIdle: boolean;
  readonly replacementReady: boolean;
  readonly video: "not-started" | "count-complete" | "incomplete";
  readonly droppedVideo: bigint | null;
  readonly droppedAudio: bigint | null;
}

/** Unknown pressure never establishes a clean handoff. Queued output remains caller-owned. */
export const canHandoff = (facts: HandoffFacts): boolean =>
  !facts.sequenceOpen &&
  facts.currentIdle &&
  facts.replacementReady &&
  facts.video !== "incomplete" &&
  facts.droppedVideo === 0n &&
  facts.droppedAudio === 0n;

export const needsReplacement = (
  phase: SourcePhase,
  indeterminate: boolean,
  isExpired: boolean,
): boolean =>
  indeterminate || isExpired || (phase._tag === "Recovering" && phase.mode === "replace");
