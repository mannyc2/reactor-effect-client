/**
 * What a paid check may spend, and the gates that refuse before it does. Pure,
 * so a test proves every refusal without a network or a credential.
 */
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";

/**
 * The checks. `vertical`, `takeover` and `turn` qualified 0.3.0; each later
 * capability added one: `audio` (reference audio), `resume` (adopting a dead
 * owner's session), `queue` (H3's own move and pop near a boundary), and the
 * playout's `renewal`, `edits` and `cut`.
 */
export const checks = [
  "vertical",
  "takeover",
  "turn",
  "audio",
  "resume",
  "queue",
  "renewal",
  "edits",
  "cut",
] as const;
export const Check = Schema.Literals(checks);
export type Check = typeof Check.Type;

/**
 * One billed minute at the published rate ($0.75 on September 24, 2026) for a
 * one-session check, two for `renewal`; every paid run in a ledger shares the
 * total. The operator's limits may only be lower.
 */
export const maxCheckUsd = 0.75;
export const maxTotalUsd = 3.75;
/** Each token caps its one session at this many seconds, server-side. */
export const sessionSeconds = 50;
/** A token outlives its session by this much, so cleanup still holds a valid one. */
export const tokenSeconds = sessionSeconds + 60;
/** A check's work ends this long after allocation, so a slow step fails it before the cap does. */
export const workSeconds = sessionSeconds - 10;

export const sessionsFor = (check: Check): number => (check === "renewal" ? 2 : 1);
export const ceilingFor = (check: Check): number => sessionsFor(check) * maxCheckUsd;

/** A gate refused: nothing past it runs, and nothing was spent. */
export class Refused extends Schema.TaggedError<Refused>(
  "reactor-effect-integration/hosted/Spend/Refused",
)("Refused", { message: Schema.String }) {}

const refuse = (message: string) => Effect.fail(Refused.make({ message }));

export interface Rate {
  readonly creditsPerSecond: number;
  readonly creditsPerDollar: number;
  /** The unit the pricing states the rate in. */
  readonly per: "second" | "minute";
}

/**
 * What `seconds` of session time bills at `rate`, every started unit of the
 * rate whole. The billing page says Reactor bills "per session-minute", while
 * the pricing endpoint states H3's rate per second (September 2026) and
 * measured charges were lower still, so the rate's own unit is the one counted.
 */
export const billedUsd = (input: { readonly rate: Rate; readonly seconds: number }): number => {
  const unit = input.rate.per === "second" ? 1 : 60;
  return (
    (input.rate.creditsPerSecond * Math.ceil(Math.max(0, input.seconds) / unit) * unit) /
    input.rate.creditsPerDollar
  );
};

/**
 * What the ledger reserves for `amount`: rounded up to its four decimals, so a
 * reservation never falls below the worst case it stands for. The slack keeps
 * 0.7500000000000001 at 0.75.
 */
export const reservationUsd = (amount: number): number => Math.ceil(amount * 1e4 - 1e-6) / 1e4;

export interface Authorization {
  readonly check: Check;
  /** The most this run may spend: every session it can open, at its capped length. */
  readonly budgetUsd: number;
  /** The most every paid run in the ledger may spend, this one included. */
  readonly totalUsd: number;
}

/** Refuses a budget outside the check's ceiling or the ledger's, or above the total it counts against. */
export const authorize = (input: Authorization): Effect.Effect<Authorization, Refused> => {
  const within = (value: number, most: number) =>
    Number.isFinite(value) && value > 0 && value <= most;
  if (!within(input.budgetUsd, ceilingFor(input.check)))
    return refuse(`--budget-usd must be more than 0 and at most ${ceilingFor(input.check)}`);
  if (!within(input.totalUsd, maxTotalUsd))
    return refuse(`--total-budget-usd must be more than 0 and at most ${maxTotalUsd}`);
  if (input.budgetUsd > input.totalUsd)
    return refuse("--budget-usd cannot exceed --total-budget-usd");
  return Effect.succeed(input);
};

/**
 * The run's worst case at `rate`: every session it can open, billed for its
 * whole capped length. Refused unless it fits the run's budget and what the
 * ledger's earlier runs, each counted at the worst case it reserved, leave of
 * the total.
 */
export const admit = (input: {
  readonly rate: Rate;
  readonly authorization: Authorization;
  readonly reservedUsd: number;
}): Effect.Effect<number, Refused> => {
  const { rate, authorization, reservedUsd } = input;
  const sessions = sessionsFor(authorization.check);
  const worst = reservationUsd(billedUsd({ rate, seconds: sessionSeconds }) * sessions);
  if (!(worst <= authorization.budgetUsd + 1e-9))
    return refuse(
      `${sessions} capped ${sessionSeconds} s session(s) bill up to $${worst.toFixed(4)}, over the $${authorization.budgetUsd} budget`,
    );
  // A nanodollar of float slack, so five $0.75 runs still fit $3.75.
  if (!(reservedUsd + worst <= authorization.totalUsd + 1e-9))
    return refuse(
      `the ledger holds $${reservedUsd.toFixed(4)} of paid runs; one more of up to $${worst.toFixed(4)} exceeds the $${authorization.totalUsd} total`,
    );
  return Effect.succeed(worst);
};

/** A token's claims, as hosted tokens have carried them; Reactor documents only its reply's echo. */
const Claims = Schema.StringFromBase64Url.pipe(
  Schema.decodeTo(
    Schema.fromJsonString(
      Schema.Struct({
        authorization_details: Schema.Tuple([
          Schema.Struct({
            constraints: Schema.Struct({
              max_sessions: Schema.Int,
              max_session_duration_seconds: Schema.Int,
            }),
          }),
        ]),
      }),
    ),
  ),
);

/**
 * What a token provably grants: Reactor's echo of the grant, else the token's
 * own claims. The SDK reads only the echo; a paid check needs its cap proven
 * either way, so a token that proves neither is refused before anything is allocated.
 */
export const provenGrant = (grant: {
  readonly jwt: string;
  readonly granted?:
    | {
        readonly maxSessions: number | undefined;
        readonly maxSessionSeconds: number | "unlimited" | undefined;
      }
    | undefined;
}): Effect.Effect<
  { readonly maxSessions: number; readonly maxSessionSeconds: number },
  Refused
> => {
  const echoed = grant.granted;
  if (echoed?.maxSessions !== undefined && typeof echoed.maxSessionSeconds === "number")
    return Effect.succeed({
      maxSessions: echoed.maxSessions,
      maxSessionSeconds: echoed.maxSessionSeconds,
    });
  const payload = grant.jwt.split(".")[1] ?? "";
  return Schema.decodeEffect(Claims)(payload).pipe(
    Effect.map(({ authorization_details: [entry] }) => ({
      maxSessions: entry.constraints.max_sessions,
      maxSessionSeconds: entry.constraints.max_session_duration_seconds,
    })),
    Effect.catch(() => refuse("the token proves neither its session count nor its cap")),
  );
};

/** Refuses a token granting more than one session, or a longer one than a check may hold. */
export const acceptGrant = (input: {
  readonly check: Check;
  readonly granted: { readonly maxSessions: number; readonly maxSessionSeconds: number };
}): Effect.Effect<void, Refused> => {
  const { check, granted } = input;
  if (granted.maxSessions !== 1 || granted.maxSessionSeconds > sessionSeconds)
    return refuse("the token grants more than one session of at most the capped length");
  // Renewal's timing rests on two full grants; shorter ones cannot qualify it.
  if (check === "renewal" && granted.maxSessionSeconds !== sessionSeconds)
    return refuse("renewal needs the full 50-second grant");
  return Effect.void;
};

/** `turn` proves a relay path, so it runs only while no earlier paid run selected one. */
export const admitRelay = (earlierPairs: ReadonlyArray<string>): Effect.Effect<void, Refused> =>
  earlierPairs.includes("relay")
    ? refuse("an earlier paid run already selected a relay pair; turn adds nothing")
    : Effect.void;
