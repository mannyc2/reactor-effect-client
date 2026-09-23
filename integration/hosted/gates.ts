/**
 * The spending and evidence gates of the hosted qualification. Pure, so a test
 * proves every refusal without a network or a credential.
 */
import * as Data from "effect/Data";

/** The most one check may spend, and the most all three may spend together. */
export const maxCheckUsd = 0.5;
export const maxTotalUsd = 1.5;
/** Every token caps its one session at this many seconds, server-side. */
export const sessionSeconds = 60;
/** A token outlives its session by this much, so cleanup still holds a valid one. */
export const tokenSeconds = sessionSeconds + 60;

export type Check = "vertical" | "takeover" | "turn";

export interface Authorization {
  readonly check: Check;
  readonly budgetUsd: number;
  /** Where the evidence is written. It must not exist yet: a check never repeats by itself. */
  readonly evidence: string;
}

/** A gate refused: nothing past it runs. */
export class Refused extends Data.TaggedError("Refused")<{ readonly message: string }> {}

const refuse = (message: string): never => {
  throw new Refused({ message });
};

const option = (args: readonly string[], name: string): string | undefined => {
  const prefix = `--${name}=`;
  const found = args.filter((arg) => arg.startsWith(prefix));
  if (found.length > 1) return refuse(`--${name} was given more than once`);
  return found[0]?.slice(prefix.length);
};

/**
 * Read an explicit authorization from the command line: the check, a budget
 * of at most $0.50, an evidence path, and the flag that says a person
 * authorized paid use. Anything missing or out of bounds refuses.
 */
export const authorize = (args: readonly string[]): Authorization => {
  const check = args[0];
  if (check !== "vertical" && check !== "takeover" && check !== "turn")
    return refuse("name the check: vertical, takeover or turn");
  if (!args.includes("--i-authorize-paid-sessions"))
    return refuse("paid use needs --i-authorize-paid-sessions from the authorizing maintainer");
  const budget = option(args, "budget-usd");
  const budgetUsd = budget === undefined ? Number.NaN : Number(budget);
  if (!Number.isFinite(budgetUsd) || budgetUsd <= 0 || budgetUsd > maxCheckUsd)
    return refuse(`--budget-usd must be more than 0 and at most ${maxCheckUsd}`);
  const evidence = option(args, "evidence");
  if (evidence === undefined || evidence.length === 0)
    return refuse("--evidence=<path> names where the evidence is written");
  return { check, budgetUsd, evidence };
};

export interface Rate {
  readonly creditsPerSecond: number;
  readonly creditsPerDollar: number;
}

/** The worst case of one session at `rate`: the whole server-enforced cap. */
export const worstCaseUsd = (rate: Rate, seconds = sessionSeconds): number =>
  (rate.creditsPerSecond * seconds) / rate.creditsPerDollar;

/** Refuse unless the whole session fits the budget at the published rate. */
export const admit = (rate: Rate, budgetUsd: number): number => {
  const cost = worstCaseUsd(rate);
  if (!(cost <= budgetUsd))
    return refuse(
      `a ${sessionSeconds} s session costs up to $${cost.toFixed(4)}, over the $${budgetUsd} budget`,
    );
  return cost;
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

/** Frames the vertical check needs to see: changing, not black, in BGRA. */
export const liveVideo = (
  frames: readonly { readonly format: string; readonly data: Uint8Array }[],
): string | undefined => {
  if (frames.length < 2) return "fewer than two frames arrived";
  if (frames.some((frame) => frame.format !== "BGRA")) return "a frame was not BGRA";
  const lit = frames.filter((frame) => {
    for (let index = 0; index < frame.data.length; index += 4)
      if (frame.data[index] !== 0 || frame.data[index + 1] !== 0 || frame.data[index + 2] !== 0)
        return true;
    return false;
  });
  if (lit.length === 0) return "every frame was black";
  const first = frames[0]!.data;
  const changed = frames.some(
    (frame) =>
      frame.data.length !== first.length || frame.data.some((byte, index) => byte !== first[index]),
  );
  return changed ? undefined : "the frames never changed";
};
