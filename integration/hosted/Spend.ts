/**
 * What a paid check may spend, and the gates that refuse before it does. Pure,
 * so a test proves every refusal without a network or a credential.
 */
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import { dual } from "effect/Function";
import * as H3 from "reactor-effect-client/H3";

/**
 * The checks. `vertical`, `takeover` and `turn` qualified 0.3.0; each later
 * capability added one: `audio` (reference audio), `resume` (adopting a dead
 * owner's session), `queue` (H3's own move and pop near a boundary), the
 * playout's `renewal`, `edits` and `cut`, and `tokens` (a session outliving the
 * token that created it). `tour` then walks the raw API through one longer
 * session. `unconnected` asks Reactor a question instead: what becomes of a
 * session nothing connects to. `showreel` records footage rather than
 * qualifying anything: the playout's picture and sound, to an MP4. `avatar`
 * asks another question, of Vidu S2-Avatar: what its docs leave open about a
 * call, for the SDK's module for it. `character` qualifies that module, the
 * `ViduS2Avatar` provider, through one call. `fasth3` asks what FastH3's docs
 * leave open before its provider is built. `rejoin` asks what reconnecting
 * during a live call does, with the runner's liveness and fresh media kept.
 */
export const checks = [
  "vertical",
  "takeover",
  "turn",
  "audio",
  "frames",
  "resume",
  "queue",
  "renewal",
  "edits",
  "cut",
  "tokens",
  "tour",
  "adoption",
  "show",
  "unconnected",
  "dropped",
  "showreel",
  "avatar",
  "character",
  "fasth3",
  "rejoin",
  "candidate",
] as const;
export const Check = Schema.Literals(checks);
export type Check = typeof Check.Type;

/** A model a check's sessions run, and the rate its ceiling was reviewed at. */
export interface Model {
  readonly name: string;
  /**
   * What the pricing endpoint stated when the ceiling was reviewed, counted
   * by the started minute; a run reserves at the rate stated as it starts.
   */
  readonly reviewed: Rate;
}

/** H3: 350 credits a second at 10,000 a dollar, $2.10 a minute, on September 30, 2026. */
export const h3: Model = {
  name: H3.modelName,
  reviewed: { creditsPerSecond: 350, creditsPerDollar: 10_000, per: "minute" },
};

/** FastH3: 350 credits a second at 10,000 a dollar, $2.10 a minute, on October 3, 2026. */
export const fastH3: Model = {
  name: "reactor/fast-h3",
  reviewed: { creditsPerSecond: 350, creditsPerDollar: 10_000, per: "minute" },
};

export const models = { h3, "fast-h3": fastH3 } as const;
export type ModelKey = keyof typeof models;

/** Vidu S2-Avatar: 70 credits a second at 10,000 a dollar, $0.42 a minute, on October 1, 2026. */
const vidu: Model = {
  name: "reactor/vidu-s2-avatar",
  reviewed: { creditsPerSecond: 70, creditsPerDollar: 10_000, per: "minute" },
};

/**
 * What a check may hold: the model its sessions run, how many it opens, and
 * each one's cap in seconds, which its token sets server-side. A check that
 * renews rests its timing on full grants, so a shorter grant cannot qualify it.
 */
interface Capped {
  readonly model: Model;
  readonly sessions: number;
  readonly seconds: number;
  readonly renews?: true;
  /**
   * How long each session may run, in seconds, when the check does not count
   * on its cap to end it: what it may be billed for in place of `seconds`.
   */
  readonly holds?: ReadonlyArray<number>;
}

/** An uncapped session needs an explicit hold to bound what the check reserves. */
interface Uncapped {
  readonly model: Model;
  readonly sessions: number;
  readonly seconds: "unlimited";
  readonly holds: ReadonlyArray<number>;
}

type Allocation = Capped | Uncapped;

export type Plan = (
  | Allocation
  | {
      /** The check's default family; its allocations each declare their actual model. */
      readonly model: Model;
      readonly allocations: ReadonlyArray<Allocation>;
    }
) & {
  /** The H3-family models this check permits; otherwise only its declared model. */
  readonly models?: ReadonlyArray<ModelKey>;
};

/** One 50-second session, which every check held before the longer runs. */
export const sessionSeconds = 50;
const single = { model: h3, sessions: 1, seconds: sessionSeconds } satisfies Plan;
const familySingle = { ...single, models: ["h3", "fast-h3"] } satisfies Plan;

export const plans = {
  vertical: familySingle,
  takeover: familySingle,
  turn: familySingle,
  audio: single,
  frames: { model: fastH3, sessions: 1, seconds: sessionSeconds },
  resume: familySingle,
  queue: familySingle,
  renewal: { ...familySingle, sessions: 2, renews: true },
  edits: familySingle,
  cut: familySingle,
  tokens: familySingle,
  tour: { ...familySingle, seconds: 90 },
  adoption: { ...familySingle, seconds: 75 },
  show: { ...familySingle, sessions: 3, seconds: 75, renews: true },
  // One session, watched past its cap and then ended with the key, 155 s at most from its
  // request. On a token of its own, one more, held past its ready and ended with the key within
  // 59 s of its request, and a third if that token's second create allocates again, ended
  // within 56 s of that create. Each short one stays within a started minute.
  unconnected: { model: h3, sessions: 3, seconds: 60, holds: [155, 59, 56] },
  // One uncapped session, watched at most 67 s after allocation, with 13 s left for its
  // owner's report, the last read and the key's first two termination attempts.
  dropped: { model: h3, sessions: 1, seconds: "unlimited", holds: [80] },
  // Five 8 s scenes air from about 7 s in and end about 47 s in; the cap leaves room for a
  // slower build and the close. $2.45 at most at 350 credits a second.
  showreel: { model: h3, sessions: 1, seconds: 70 },
  // Two calls and their refusals are planned to end about 125 s in, at the timing the first paid
  // run measured; the cap leaves room for a slower avatar and call. $1.26 at most at 70 credits
  // a second, by the started minute.
  avatar: { model: vidu, sessions: 1, seconds: 180 },
  // One call is planned to end about 28 s in at the second paid avatar run's timing, and about
  // 59 s in at the first's, whose avatar took 26.7 s and end_call 13.2 s; a 60 s cap would leave
  // that one no 10 s margin. $0.525 at most at 70 credits a second, and $0.84 by the started
  // minute.
  character: { model: vidu, sessions: 1, seconds: 75 },
  fasth3: { model: fastH3, sessions: 1, seconds: sessionSeconds },
  // One call, a reconnect during it and end_call are planned to end about 70 s in, including
  // the first paid avatar's 26.7 s create and 13.2 s end. $0.84 at most at 70 credits a second,
  // and $0.84 by the started minute; the work ends 110 s in, before the 120 s cap.
  rejoin: { model: vidu, sessions: 1, seconds: 120 },
  candidate: {
    model: h3,
    allocations: [
      { model: h3, sessions: 1, seconds: 90 },
      { model: fastH3, sessions: 1, seconds: 90 },
    ],
  },
} satisfies { readonly [C in Check]: Plan };

const allowedModels = (check: Check): ReadonlyArray<Model> => {
  const plan: Plan = plans[check];
  return plan.models === undefined ? [plan.model] : plan.models.map((key) => models[key]);
};

/** The selected, permitted model, or the check's declared default. */
export const modelFor: {
  (key?: ModelKey): (check: Check) => Model;
  (check: Check, key?: ModelKey): Model;
} = dual(
  (args) => Schema.is(Check)(args[0]),
  (check: Check, key?: ModelKey): Model =>
    key !== undefined && allowedModels(check).includes(models[key])
      ? models[key]
      : plans[check].model,
);

/** Every model and allocation this check may open. */
export const allocationsFor: {
  (key?: ModelKey): (check: Check) => ReadonlyArray<Allocation>;
  (check: Check, key?: ModelKey): ReadonlyArray<Allocation>;
} = dual(
  (args) => Schema.is(Check)(args[0]),
  (check: Check, key?: ModelKey) => {
    const plan: Plan = plans[check];
    return "allocations" in plan ? plan.allocations : [{ ...plan, model: modelFor(check, key) }];
  },
);

const holds = (plan: Allocation): ReadonlyArray<number> => {
  if (plan.seconds === "unlimited") return plan.holds;
  const cap = plan.seconds;
  return plan.holds ?? Array.from({ length: plan.sessions }, () => cap);
};

/** Every physical session's model and reviewed hold, including heterogeneous checks. */
export interface SessionBound {
  readonly model: Model;
  readonly seconds: number;
}
export const sessionBoundsFor: {
  (key?: ModelKey): (check: Check) => ReadonlyArray<SessionBound>;
  (check: Check, key?: ModelKey): ReadonlyArray<SessionBound>;
} = dual(
  (args) => Schema.is(Check)(args[0]),
  (check: Check, key?: ModelKey) =>
    allocationsFor(check, key).flatMap((plan) =>
      holds(plan).map((seconds) => ({ model: plan.model, seconds })),
    ),
);

/** How long each of a check's sessions may run: its cap, unless the check holds it longer. */
export const holdsFor = (check: Check): ReadonlyArray<number> =>
  sessionBoundsFor(check).map((bound) => bound.seconds);

/** A check's tokens outlive its sessions by a minute, so cleanup still holds a valid one. */
export const tokenSecondsFor = (check: Check): number => Math.max(...holdsFor(check)) + 60;
/** A check's work ends this long after allocation, so a slow step fails it before the cap does. */
export const workSecondsFor = (check: Check): number =>
  Math.min(
    ...allocationsFor(check).map((plan) =>
      plan.seconds === "unlimited" ? Math.max(...plan.holds) : plan.seconds,
    ),
  ) - 10;

/**
 * The most a check may spend: every started minute of each of its sessions,
 * for as long as it may run, at its model's reviewed rate: $2.10 for one
 * 50-second H3 session, $1.26 for `avatar`'s 180-second one. Every paid run in
 * a ledger shares the total, which admits the costliest check, `unconnected`,
 * at H3's rate per second ($9.45). The operator's limits may only be lower.
 */
export const ceilingFor: {
  (key?: ModelKey): (check: Check) => number;
  (check: Check, key?: ModelKey): number;
} = dual(
  (args) => Schema.is(Check)(args[0]),
  (check: Check, key?: ModelKey): number =>
    reservationUsd(
      sessionBoundsFor(check, key).reduce(
        (total, bound) => total + billedUsd({ rate: bound.model.reviewed, seconds: bound.seconds }),
        0,
      ),
    ),
);
export const maxTotalUsd = 10;

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

/** A model's independently read live rate. */
export interface ModelRate {
  readonly model: string;
  readonly rate: Rate;
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
  readonly model?: ModelKey | undefined;
  /** The most this run may spend: every session it can open, at its capped length. */
  readonly budgetUsd: number;
  /** The most every paid run in the ledger may spend, this one included. */
  readonly totalUsd: number;
}

/** Refuses a budget outside the check's ceiling or the ledger's, or above the total it counts against. */
export const authorize = (input: Authorization): Effect.Effect<Authorization, Refused> => {
  if (input.check === "candidate" && input.model !== undefined)
    return refuse("candidate runs one H3 and one FastH3 session; --model cannot change them");
  const allowed = allowedModels(input.check);
  if (input.model !== undefined && !allowed.includes(models[input.model])) {
    const names = allowed.map(
      (model) => Object.entries(models).find(([, value]) => value === model)?.[0] ?? model.name,
    );
    return refuse(`${input.check} runs on ${names.join(" or ")} only`);
  }
  const within = (value: number, most: number) =>
    Number.isFinite(value) && value > 0 && value <= most;
  if (!within(input.budgetUsd, ceilingFor(input.check, input.model)))
    return refuse(
      `--budget-usd must be more than 0 and at most ${ceilingFor(input.check, input.model)}`,
    );
  if (!within(input.totalUsd, maxTotalUsd))
    return refuse(`--total-budget-usd must be more than 0 and at most ${maxTotalUsd}`);
  if (input.budgetUsd > input.totalUsd)
    return refuse("--budget-usd cannot exceed --total-budget-usd");
  return Effect.succeed(input);
};

/**
 * The run's worst case at `rate`: every session it can open, billed for as
 * long as it may run. Refused unless it fits the run's budget and what the
 * ledger's earlier runs, each counted at the worst case it reserved, leave of
 * the total.
 */
export const admit = (input: {
  readonly rates: ReadonlyArray<ModelRate>;
  readonly authorization: Authorization;
  readonly reservedUsd: number;
}): Effect.Effect<number, Refused> => {
  const { rates, authorization, reservedUsd } = input;
  const bounds = sessionBoundsFor(authorization.check, authorization.model);
  let amount = 0;
  for (const bound of bounds) {
    const rate = rates.find((entry) => entry.model === bound.model.name)?.rate;
    if (rate === undefined) return refuse(`no live rate was read for ${bound.model.name}`);
    amount += billedUsd({ rate, seconds: bound.seconds });
  }
  const worst = reservationUsd(amount);
  if (!(worst <= authorization.budgetUsd + 1e-9))
    return refuse(
      `${bounds.length} session(s) of up to ${[...new Set(bounds.map((bound) => bound.seconds))].join(" and ")} s bill up to $${worst.toFixed(4)}, over the $${authorization.budgetUsd} budget`,
    );
  // A nanodollar of float slack, so four $2.10 runs still fit $8.40.
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
              max_session_duration_seconds: Schema.optionalKey(Schema.Int),
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
  { readonly maxSessions: number; readonly maxSessionSeconds: number | "unlimited" },
  Refused
> => {
  const echoed = grant.granted;
  if (echoed?.maxSessions !== undefined && echoed.maxSessionSeconds !== undefined)
    return Effect.succeed({
      maxSessions: echoed.maxSessions,
      maxSessionSeconds: echoed.maxSessionSeconds,
    });
  const payload = grant.jwt.split(".")[1] ?? "";
  return Schema.decodeEffect(Claims)(payload).pipe(
    Effect.map(({ authorization_details: [entry] }) => ({
      maxSessions: entry.constraints.max_sessions,
      maxSessionSeconds: entry.constraints.max_session_duration_seconds ?? ("unlimited" as const),
    })),
    Effect.catch(() => refuse("the token proves neither its session count nor its cap")),
  );
};

/**
 * What a token bound to one open session provably grants. Reactor counts a
 * bound session in `max_sessions`, so a token whose echo binds exactly that
 * session and counts no more creates none: it acts only on that session, whose
 * cap its creating token proved. Refused unless the echo says so.
 */
export const provenBind = (input: {
  readonly sessionId: string;
  /** The bound session's cap, as its creating token proved it. */
  readonly sessionSeconds: number;
  readonly granted?:
    | { readonly maxSessions: number | undefined; readonly bound: ReadonlyArray<string> }
    | undefined;
}): Effect.Effect<
  { readonly maxSessions: number; readonly maxSessionSeconds: number },
  Refused
> => {
  const echoed = input.granted;
  if (echoed?.bound.length !== 1 || echoed.bound[0] !== input.sessionId)
    return refuse("the bound token's echo does not bind exactly its session");
  if (echoed.maxSessions === undefined || echoed.maxSessions > echoed.bound.length)
    return refuse("the bound token may create sessions");
  return Effect.succeed({
    maxSessions: echoed.maxSessions,
    maxSessionSeconds: input.sessionSeconds,
  });
};

/** Refuses a token granting more than one session, or a longer one than a check may hold. */
export const acceptGrant = (input: {
  readonly check: Check;
  readonly model?: string;
  readonly granted: {
    readonly maxSessions: number;
    readonly maxSessionSeconds: number | "unlimited";
  };
}): Effect.Effect<void, Refused> => {
  const { check, granted } = input;
  const plan = allocationsFor(check).find(
    (allocation) => input.model === undefined || allocation.model.name === input.model,
  );
  if (plan === undefined) return refuse("the grant names a model this check cannot allocate");
  if (plan.seconds === "unlimited")
    return granted.maxSessions === 1 && granted.maxSessionSeconds === "unlimited"
      ? Effect.void
      : refuse(`${check} needs a token for one uncapped session`);
  if (
    granted.maxSessions !== 1 ||
    granted.maxSessionSeconds === "unlimited" ||
    granted.maxSessionSeconds > plan.seconds
  )
    return refuse("the token grants more than one session of at most the capped length");
  if (plan.renews === true && granted.maxSessionSeconds !== plan.seconds)
    return refuse(`${check} needs the full ${plan.seconds}-second grant`);
  return Effect.void;
};

/** `turn` proves a relay path, so it runs only while no earlier paid run selected one. */
export const admitRelay = (earlierPairs: ReadonlyArray<string>): Effect.Effect<void, Refused> =>
  earlierPairs.includes("relay")
    ? refuse("an earlier paid run already selected a relay pair; turn adds nothing")
    : Effect.void;
