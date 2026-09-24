/**
 * The spending and evidence gates of the hosted qualification. Pure, so a test
 * proves every refusal without a network or a credential.
 */
import * as Data from "effect/Data";

/**
 * The most one check may spend, and the most every paid run together may
 * spend: one billed minute at the published rate ($0.75 on September 24,
 * 2026), and the three checks of the plan. The operator's own limit, given
 * on the command line, may only be lower.
 */
export const maxCheckUsd = 0.75;
export const maxTotalUsd = 2.25;
/**
 * Reactor bills a session by the minute, from `ready` until it ends. Its
 * documentation does not say how a started minute rounds, so every gate
 * counts it whole.
 */
export const billedSeconds = 60;
/**
 * Every token caps its one session at this many seconds, server-side: short of
 * a billed minute, so a cap enforced a few seconds late still bills one.
 */
export const sessionSeconds = 50;
/** A token outlives its session by this much, so cleanup still holds a valid one. */
export const tokenSeconds = sessionSeconds + 60;
/**
 * Everything a check does with its session happens this long after allocation
 * at the latest, so a slow step fails the check instead of running into the cap.
 */
export const workSeconds = sessionSeconds - 10;

export const checks = ["vertical", "takeover", "turn"] as const;
export type Check = (typeof checks)[number];

export interface Authorization {
  readonly check: Check;
  /** The most this run may spend: its session's worst case must fit. */
  readonly budgetUsd: number;
  /** The most every paid run recorded in the ledger may spend, this one included. */
  readonly totalBudgetUsd: number;
  /** The evidence directory. Every paid run writes a new file there, and so it is the ledger. */
  readonly ledger: string;
  /** The operator's own description of the network the check runs from. */
  readonly network: string;
}

/** A gate refused: nothing past it runs. */
export class Refused extends Data.TaggedError("Refused")<{ readonly message: string }> {}

const refuse = (message: string): never => {
  throw new Refused({ message });
};

/** `--name=value` options, each at most once, and flags; anything else refuses. */
export const options = (
  args: readonly string[],
  known: readonly string[],
  flags: readonly string[] = [],
): ReadonlyMap<string, string> => {
  const found = new Map<string, string>();
  for (const arg of args) {
    if (flags.includes(arg)) {
      found.set(arg, "");
      continue;
    }
    const match = /^--([a-z-]+)=(.*)$/s.exec(arg);
    if (match === null || !known.includes(match[1]!)) return refuse(`unknown argument ${arg}`);
    if (found.has(match[1]!)) return refuse(`--${match[1]} was given more than once`);
    found.set(match[1]!, match[2]!);
  }
  return found;
};

const usd = (value: string | undefined, name: string, most: number): number => {
  const amount = value === undefined ? Number.NaN : Number(value);
  if (!Number.isFinite(amount) || amount <= 0 || amount > most)
    return refuse(`--${name} must be more than 0 and at most ${most}`);
  return amount;
};

/**
 * Read an explicit authorization from the command line: the check, a budget
 * for it of at most `maxCheckUsd`, a total of at most `maxTotalUsd` across
 * every paid run in the ledger, the ledger directory, a description of the
 * network, and the flag that says a person authorized paid use. Anything
 * missing, unknown or out of bounds refuses.
 */
export const authorize = (args: readonly string[]): Authorization => {
  const check = args[0];
  if (!checks.includes(check as Check)) return refuse("name the check: vertical, takeover or turn");
  const given = options(
    args.slice(1),
    ["budget-usd", "total-budget-usd", "ledger", "network"],
    ["--i-authorize-paid-sessions"],
  );
  if (!given.has("--i-authorize-paid-sessions"))
    return refuse("paid use needs --i-authorize-paid-sessions from the authorizing maintainer");
  const budgetUsd = usd(given.get("budget-usd"), "budget-usd", maxCheckUsd);
  const totalBudgetUsd = usd(given.get("total-budget-usd"), "total-budget-usd", maxTotalUsd);
  if (budgetUsd > totalBudgetUsd) return refuse("--budget-usd cannot exceed --total-budget-usd");
  const ledger = given.get("ledger") ?? "";
  if (ledger.length === 0) return refuse("--ledger=<directory> names where the evidence is kept");
  const network = (given.get("network") ?? "").trim();
  if (network.length === 0)
    return refuse('--network="<description>" says where the check runs from, without addresses');
  return { check: check as Check, budgetUsd, totalBudgetUsd, ledger, network };
};

export interface Rate {
  readonly creditsPerSecond: number;
  readonly creditsPerDollar: number;
}

/** What `seconds` of billed session time costs at `rate`, every started minute whole. */
export const billedUsd = (rate: Rate, seconds: number): number =>
  (rate.creditsPerSecond * Math.ceil(Math.max(0, seconds) / billedSeconds) * billedSeconds) /
  rate.creditsPerDollar;

/** The worst case of one session at `rate`: the whole server-enforced cap, billed by the minute. */
export const worstCaseUsd = (rate: Rate): number => billedUsd(rate, sessionSeconds);

/** Refuse unless the whole session fits the budget at the published rate. */
export const admit = (rate: Rate, budgetUsd: number): number => {
  const cost = worstCaseUsd(rate);
  if (!(cost <= budgetUsd + 1e-9))
    return refuse(
      `a ${sessionSeconds} s session bills up to $${cost.toFixed(4)}, over the $${budgetUsd} budget`,
    );
  return cost;
};

/**
 * Refuse unless this run's worst case fits what the ledger has left. Each
 * earlier paid run counts at the worst case it reserved, whatever it spent:
 * the ledger never trusts an estimate to free budget.
 */
export const admitTotal = (
  reserved: readonly number[],
  worstCase: number,
  totalBudgetUsd: number,
): number => {
  const before = reserved.reduce((sum, amount) => sum + amount, 0);
  // A nanodollar of float slack, so three $0.75 runs still fit $2.25.
  if (!(before + worstCase <= totalBudgetUsd + 1e-9))
    return refuse(
      `the ledger holds $${before.toFixed(4)} of paid runs; one more of up to $${worstCase.toFixed(4)} exceeds the $${totalBudgetUsd} total`,
    );
  return before;
};

/** The limits a returned token actually grants. */
export interface Granted {
  readonly maxSessions: number;
  readonly maxSessionSeconds: number;
}

/** Refuse a token that grants more than one session or a longer one than asked for. */
export const acceptGrant = (granted: Granted): void => {
  if (granted.maxSessions !== 1 || granted.maxSessionSeconds > sessionSeconds)
    return refuse("the token grants more than one session of at most the capped length");
};

/** A remote outcome the check observed. */
export type Outcome = "not-submitted" | "unknown" | "replied";

/** What stops a check at once: an unknown outcome, or a paid session whose end is unconfirmed. */
export const stopFor = (evidence: {
  readonly outcomes: readonly Outcome[];
  readonly terminationConfirmed: boolean | undefined;
}): string | undefined => {
  if (evidence.outcomes.includes("unknown"))
    return "an outcome is unknown; the check stops and is not repeated";
  if (evidence.terminationConfirmed === false)
    return "remote termination is unconfirmed; the session may still be billing";
  return undefined;
};

/** What the video reader saw, summarized as it read: frames are never kept. */
export interface VideoSeen {
  readonly frames: number;
  readonly formats: readonly string[];
  /** Frames with any non-black sampled pixel. */
  readonly lit: number;
  /** Distinct sampled-pixel digests. */
  readonly distinct: number;
}

/** Frames the vertical check needs to see: changing, not black, in BGRA. */
export const liveVideo = (video: VideoSeen): string | undefined => {
  if (video.frames < 2) return "fewer than two frames arrived";
  if (video.formats.some((format) => format !== "BGRA")) return "a frame was not BGRA";
  if (video.lit === 0) return "every frame was black";
  return video.distinct > 1 ? undefined : "the frames never changed";
};

/**
 * The relay check exists to prove a relay path, so it runs only while no
 * earlier paid run has already selected one.
 */
export const admitRelayCheck = (earlier: readonly (readonly [string, string])[]): void => {
  if (earlier.some((pair) => pair.includes("relay")))
    return refuse("an earlier paid run already selected a relay pair; the turn check adds nothing");
};
