/**
 * The playout's pure policy: one state record and `step`, which applies one
 * input and returns the new state, what to do, and when to be woken if nothing
 * happens first. It holds no Effect, reads no clock and performs no I/O; the
 * runtime supplies monotonic and wall time with every input.
 *
 * The plan is the single source of truth. Each session only executes its window
 * of it: new work goes to the newest live session, Ready clips are ordered
 * within their own session and never ranked across two, and at most one
 * provider command is in flight, so a refusal is always attributable.
 */
import { dual } from "effect/Function";
import type { Request } from "../../H3.js";
import type { CommandFailure } from "../../ReactorError.js";
import type {
  AsRunStatus,
  ClipTag,
  Event,
  FillContext,
  PlayingClip,
  SourceClip,
  SourceEvent,
  SourceState,
  State as PublicState,
  WithdrawOutcome,
} from "../../Playout.js";
import type { ItemKey } from "./errors.js";

export type Late = "nextBoundary" | "drop" | { readonly skipAfterMs: number };

/** A submission as the runtime normalized it: durations in milliseconds, lanes as indexes. */
export interface Spec {
  readonly key: ItemKey;
  readonly lane: number;
  readonly request: Request;
  readonly seconds: number;
  readonly fingerprint: string;
  readonly cues: ReadonlyArray<{
    readonly name: string;
    readonly from: "start" | "end";
    readonly offsetMs: number;
  }>;
  readonly continuity: boolean;
  readonly window?: {
    readonly notBeforeMs?: number;
    readonly startByMs?: number;
    readonly firm: boolean;
  };
  readonly start:
    | { readonly _tag: "Follow" | "Asap" | "Manual" }
    | { readonly _tag: "At"; readonly time: number; readonly late: Late };
}

export type EditInput =
  | { readonly _tag: "Submit"; readonly spec: Spec }
  | {
      readonly _tag: "SubmitGroup";
      readonly key: ItemKey;
      readonly lane: number;
      readonly parts: ReadonlyArray<Spec>;
      readonly fingerprint: string;
    }
  | {
      readonly _tag: "Insert";
      readonly spec: Spec;
      readonly anchor: ItemKey;
      readonly side: "before" | "after";
    }
  | { readonly _tag: "Replace"; readonly key: ItemKey; readonly spec: Spec }
  | { readonly _tag: "Withdraw"; readonly key: ItemKey };

export type Command =
  | {
      readonly _tag: "Enqueue";
      readonly request: Request;
      readonly tag: ClipTag;
      readonly continueFrom?: string | undefined;
    }
  | { readonly _tag: "Remove"; readonly clipId: string }
  | { readonly _tag: "Move"; readonly clipId: string; readonly position: number }
  | { readonly _tag: "Autoplay"; readonly enabled: boolean }
  /** Stop `clipId` if it still plays, once autoplay is off; done when its end is reported. */
  | { readonly _tag: "Stop"; readonly clipId: string }
  | { readonly _tag: "Play"; readonly clipId: string };

/** A command's result: the clip an enqueue created, or how a failure left the provider. */
export type CommandResult =
  | { readonly _tag: "Done"; readonly clipId?: string | undefined }
  /** Its outcome says whether the provider may have applied it, and whether a retry may mend it. */
  | { readonly _tag: "Failed"; readonly cause: CommandFailure }
  /** It died or was interrupted, so whether the provider applied it cannot be told. */
  | { readonly _tag: "Died" };

/**
 * Whether a command was refused unsent because its session was not in a state
 * to take it, as when its connection drops before its source can say so. Only a
 * change in what the session reports can mend that, so it waits for one.
 */
const unready = (cause: CommandFailure): boolean =>
  cause.context.outcome === "not-submitted" && cause.reason._tag === "InvalidState";

/** Whether a command that failed may have taken effect unseen. */
const uncertain = (result: Exclude<CommandResult, { readonly _tag: "Done" }>): boolean =>
  result._tag === "Died" || result.cause.context.outcome === "unknown";

export type Input =
  /** `batch`: the edits take effect together, make-before-break, as `Playout.edit` promises. */
  | {
      readonly _tag: "Edit";
      readonly id: number;
      readonly edits: ReadonlyArray<EditInput>;
      readonly batch: boolean;
    }
  | { readonly _tag: "Release"; readonly id: number; readonly key: ItemKey }
  | { readonly _tag: "Drain"; readonly id: number; readonly finish: "playing" | "accepted" }
  | { readonly _tag: "Opened"; readonly sessionId: string; readonly lifetimeMs: number }
  | { readonly _tag: "OpenFailed"; readonly reason: string; readonly fatal: boolean }
  | { readonly _tag: "Source"; readonly sessionId: string; readonly event: SourceEvent }
  | { readonly _tag: "Lost"; readonly sessionId: string; readonly reason: string }
  | { readonly _tag: "Result"; readonly id: number; readonly result: CommandResult }
  | { readonly _tag: "Tick" }
  | { readonly _tag: "Close" };

export type Refusal =
  | { readonly _tag: "KeyMismatch"; readonly key: ItemKey }
  | { readonly _tag: "WouldMissDeadline"; readonly key: ItemKey }
  | { readonly _tag: "LaneBusy"; readonly key: ItemKey; readonly lane: number }
  | { readonly _tag: "InvalidItem"; readonly key: string; readonly message: string }
  | { readonly _tag: "PlayoutClosed" };

export type EditReply =
  | { readonly _tag: "Added"; readonly key: ItemKey }
  | { readonly _tag: "AddedGroup"; readonly key: ItemKey; readonly parts: ReadonlyArray<ItemKey> }
  | { readonly _tag: "Withdrawal" };

export type Action =
  | {
      readonly _tag: "Command";
      readonly id: number;
      readonly sessionId: string;
      readonly command: Command;
    }
  | { readonly _tag: "Open" }
  | { readonly _tag: "Close"; readonly sessionId: string }
  | { readonly _tag: "OnAir"; readonly sessionId: string }
  | { readonly _tag: "Emit"; readonly event: Event }
  | { readonly _tag: "Accepted"; readonly id: number; readonly results: ReadonlyArray<EditReply> }
  | { readonly _tag: "Refused"; readonly id: number; readonly refusal: Refusal }
  | { readonly _tag: "Committed"; readonly id: number }
  | {
      readonly _tag: "Withdrawn";
      readonly id: number;
      readonly index: number;
      readonly outcome: WithdrawOutcome;
    }
  | { readonly _tag: "Drained"; readonly id: number }
  /**
   * The playout fails for good: `open` when sessions could not be opened, `lost`
   * when they were lost before playing, `moderation` when content moderation
   * ended too many.
   */
  | {
      readonly _tag: "Fail";
      readonly reason: string;
      readonly cause: "open" | "lost" | "moderation";
    }
  /** Settled keys the history bound dropped: they may be submitted afresh. */
  | { readonly _tag: "Forget"; readonly keys: ReadonlyArray<ItemKey> };

export interface Config {
  readonly lanes: ReadonlyArray<{
    readonly name: string;
    readonly conflict: "queue" | "replace" | "skip";
    readonly cut: boolean;
  }>;
  readonly filler:
    | {
        readonly floor: number;
        readonly target: number;
        readonly clip: (context: FillContext) => Request;
        readonly lengths: { readonly min: number; readonly max: number };
      }
    | undefined;
  readonly maxBuildsInFlight: number;
  readonly maxHistory: number;
  readonly unknownTimeoutMs: number;
  readonly leadMs: number;
  readonly graceMs: number;
  readonly maxSetupFailures: number;
  readonly maxModerations: number;
}

export interface Now {
  readonly mono: number;
  readonly wall: number;
}

type Phase = "Accepted" | "Building" | "Ready" | "Started" | "Settled" | "Unknown";
type DropReason = "late" | "withdrawn" | "replaced";

interface Item {
  readonly spec: Spec;
  readonly order: number;
  readonly generation: number;
  readonly group?: { readonly key: ItemKey; readonly index: number } | undefined;
  readonly inserted: boolean;
  /** Where an insert was placed, which a resubmission under its key must repeat. */
  readonly anchor?: { readonly key: ItemKey; readonly side: "before" | "after" } | undefined;
  readonly replaces?: ItemKey | undefined;
  /** The pending batch that added it: it builds, but does not air until the batch commits. */
  readonly batch?: number | undefined;
  readonly mode: "follow" | "asap" | "held";
  readonly phase: Phase;
  readonly status?: AsRunStatus | undefined;
  readonly clipId?: string | undefined;
  readonly sessionId?: string | undefined;
  readonly notBefore?: number | undefined;
  readonly startBy?: number | undefined;
  readonly dispatchedAt?: number | undefined;
  /** Its build in flight continues from another clip, so its build time is measured apart. */
  readonly continued?: boolean | undefined;
  /**
   * The Ready clip it continues from when that clip airs after its place: it
   * was projected Ready only after the clip before its place ended. It waits
   * behind that clip until the clip starts.
   */
  readonly follows?: string | undefined;
  readonly everUnknown: boolean;
  /** Sessions in a row its clip was lost with before it was built. */
  readonly unbuiltLosses: number;
  readonly unknownSince?: number | undefined;
  readonly retryAt?: number | undefined;
  readonly startedAt?: number | undefined;
  readonly fired: ReadonlyArray<number>;
  /** A withdrawal the plan wants, and the batch withdrawals waiting on its outcome. */
  readonly withdraw?: DropReason | undefined;
  readonly waiting: ReadonlyArray<{ readonly id: number; readonly index: number }>;
  /** The source state a refused remove saw; it is retried once that changes. */
  readonly blockedRemove?: string | undefined;
  /**
   * The session that last refused its enqueue unsent, not ready for it, and
   * that session's availability changes then: it is sent there again only after
   * another change.
   */
  readonly unsent?: { readonly sessionId: string; readonly changes: number } | undefined;
  /**
   * A clip of its own taken off because it was Ready too early: never adopted
   * again as queued, though if it plays anyway, it aired.
   */
  readonly discarded?: string | undefined;
  /** The provider's length for its clip, once it started. */
  readonly airSeconds?: number | undefined;
}

interface Session {
  readonly id: string;
  readonly openedAt: number;
  readonly lifetimeMs: number;
  readonly source: SourceState | undefined;
  /** How often its source's `available` has changed: a reconnect changes it twice. */
  readonly changes: number;
  readonly autoplay: boolean | undefined;
  readonly wantAutoplay: boolean;
  readonly retiring: boolean;
  readonly lastEndedAt: number | undefined;
  readonly startedAny: boolean;
  /** An enqueue here stayed unknown past the deadline: new work goes elsewhere, and a replacement takes over. */
  readonly indeterminate: boolean;
  /**
   * Filler enqueues sent here whose outcome is unknown, until a queue read shows
   * their clip or the session goes: each may be building here, and only here.
   */
  readonly unknownFiller: ReadonlyArray<{ readonly index: number; readonly since: number }>;
  /**
   * The clip the provider last reported playing, and when its start was
   * observed: `at` on the monotonic clock, `wall` in epoch milliseconds.
   */
  readonly playing: (PlayingClip & { readonly at: number; readonly wall: number }) | undefined;
  /** What the latest enqueue sent here was for: a moderation verdict names no clip. */
  readonly lastEnqueue: ClipTag | undefined;
}

interface Batch {
  readonly id: number;
  readonly adds: ReadonlyArray<ItemKey>;
  readonly targets: ReadonlyArray<{
    readonly key: ItemKey;
    readonly index: number;
    readonly reason: DropReason;
  }>;
}

export interface State {
  readonly items: ReadonlyMap<ItemKey, Item>;
  readonly groups: ReadonlyMap<
    ItemKey,
    { readonly fingerprint: string; readonly parts: ReadonlyArray<ItemKey> }
  >;
  readonly settled: ReadonlyArray<ItemKey>;
  readonly nextOrder: number;
  readonly sessions: ReadonlyArray<Session>;
  readonly air: string | undefined;
  readonly opening: boolean;
  readonly openRetryAt: number;
  readonly setupFailures: number;
  /** Sessions content moderation ended. */
  readonly moderations: number;
  readonly busy:
    | { readonly id: number; readonly sessionId: string; readonly command: Command }
    | undefined;
  readonly nextCommand: number;
  readonly fillers: ReadonlyMap<string, { readonly index: number; readonly sessionId: string }>;
  readonly filler: {
    readonly index: number;
    readonly request: Request | undefined;
    readonly retryAt: number;
    readonly dispatchedAt: number | undefined;
    readonly seconds: number | undefined;
    readonly refilling: boolean;
  };
  readonly batches: ReadonlyArray<Batch>;
  /**
   * Withdrawals of a group key still waiting on some of its parts, by edit and
   * position: the parts not answered yet, and whether any was withdrawn.
   */
  readonly joins: ReadonlyMap<
    string,
    { readonly left: number; readonly outcomes: ReadonlyArray<WithdrawOutcome> }
  >;
  readonly drains: ReadonlyArray<{ readonly id: number; readonly finish: "playing" | "accepted" }>;
  readonly accepting: boolean;
  readonly closed: boolean;
  /**
   * Recent builds, in build seconds per requested second, of independent
   * clips and of clips continued from another; and actual over requested length.
   */
  readonly samples: {
    readonly build: ReadonlyArray<number>;
    readonly continued: ReadonlyArray<number>;
    readonly length: ReadonlyArray<number>;
  };
  readonly blockedMove: string | undefined;
  /**
   * The clip last cut. It is never cut again, whatever its cut's result: H3's
   * stop names no clip, and a stopped clip goes on looking like it plays until
   * its end is reported, so a second cut would stop the clip after it.
   */
  readonly cut: string | undefined;
  /**
   * The cut under way, one command at a time: autoplay off on its session, a
   * stop of the clip it cuts, then a play of its cutter. Autoplay stays off
   * until the cutter has left the Ready queue, played or withdrawn, or a step
   * failed with the cutter still wanted, which then airs at the next boundary.
   */
  readonly cutting:
    | {
        readonly sessionId: string;
        readonly clipId: string;
        readonly next: string;
        readonly stage: "stopping" | "stopped" | "played" | "failed";
      }
    | undefined;
  readonly starving: boolean;
  readonly starved: number;
}

export interface Step {
  readonly state: State;
  readonly actions: ReadonlyArray<Action>;
  /** Monotonic milliseconds at which a `Tick` is due, if nothing comes sooner. */
  readonly wake: number | undefined;
}

export const initial: State = {
  items: new Map(),
  groups: new Map(),
  settled: [],
  nextOrder: 1,
  sessions: [],
  air: undefined,
  opening: false,
  openRetryAt: 0,
  setupFailures: 0,
  moderations: 0,
  busy: undefined,
  nextCommand: 1,
  fillers: new Map(),
  filler: {
    index: 0,
    request: undefined,
    retryAt: 0,
    dispatchedAt: undefined,
    seconds: undefined,
    refilling: false,
  },
  batches: [],
  joins: new Map(),
  drains: [],
  accepting: true,
  closed: false,
  samples: { build: [], continued: [], length: [] },
  blockedMove: undefined,
  cut: undefined,
  cutting: undefined,
  starving: false,
  starved: 0,
};

const retryDelayMs = 1_000;
const cutMarginMs = 1_000;
const exposureMarginMs = 1_500;
const lookaheadMarginSeconds = 1;
const maxSamples = 32;
const minimumSamples = 3;

const quantile = (values: ReadonlyArray<number>, q: number): number => {
  const sorted = [...values].sort((a, b) => a - b);
  return sorted[Math.min(sorted.length - 1, Math.floor(q * sorted.length))] ?? 0;
};

const spreadOf = (samples: ReadonlyArray<number>) =>
  samples.length < minimumSamples
    ? undefined
    : { median: quantile(samples, 0.5), p95: quantile(samples, 0.95) };

const estimatesOf = (samples: State["samples"]): PublicState["estimates"] => ({
  build: spreadOf(samples.build),
  continuedBuild: spreadOf(samples.continued),
  length: samples.length.length === 0 ? 1 : quantile(samples.length, 0.5),
});

/**
 * Until a continued build is measured, one is projected at this multiple of an
 * independent build: on hosted H3 a continued 5 s clip took 5.45 s to build,
 * against 2.1 to 2.2 s for independent ones (0.7.0 scheduler-edits run).
 */
const continuedBuildFactor = 2.5;

/**
 * Build seconds per requested second a continued build is projected at, erring
 * long: the p95 of measured continued builds, or their longest below three
 * samples; failing those, the same of independent builds times
 * `continuedBuildFactor`. Undefined before any build was measured.
 */
const continuedBuildRate = (samples: State["samples"]): number | undefined => {
  const long = (values: ReadonlyArray<number>) =>
    values.length >= minimumSamples ? quantile(values, 0.95) : Math.max(...values);
  if (samples.continued.length > 0) return long(samples.continued);
  if (samples.build.length > 0) return long(samples.build) * continuedBuildFactor;
  return undefined;
};

/**
 * The newest live session that can take work: new work goes there. One whose
 * enqueue stayed unknown past the deadline takes none, since what it does with
 * a command can no longer be told.
 */
const preferredOf = (sessions: ReadonlyArray<Session>): Session | undefined =>
  [...sessions]
    .reverse()
    .find((value) => !value.retiring && !value.indeterminate && value.source?.available === true);

/**
 * What is left of `value`'s playing clip at `mono`, counted from its observed start. A clip
 * of unknown length may end at any moment, so nothing is counted for it and the plan builds ahead.
 */
const playingRestOf = (value: Session | undefined, mono: number): number => {
  const playing = value?.source?.playing;
  if (playing?.seconds === undefined) return 0;
  const since = value?.playing?.clipId === playing.clipId ? value.playing.at : undefined;
  return since === undefined
    ? playing.seconds * 1000
    : Math.max(0, playing.seconds * 1000 - (mono - since));
};

type Rank = readonly [number, number, number, number];
const compareRank = (a: Rank, b: Rank): number =>
  a[0] - b[0] || a[1] - b[1] || a[2] - b[2] || a[3] - b[3];

/** Applies one input: a pure function of the state, the input, the configuration and the time. */
export const step: {
  (previous: State, input: Input, now: Now): (config: Config) => Step;
  (config: Config, previous: State, input: Input, now: Now): Step;
} = dual(4, (config: Config, previous: State, input: Input, now: Now): Step => {
  const items = new Map(previous.items);
  const groups = new Map(previous.groups);
  let state: State = previous;
  const actions: Array<Action> = [];
  const lanes = config.lanes.length;

  const set = (key: ItemKey, patch: Partial<Item>): Item => {
    const next = { ...items.get(key)!, ...patch };
    items.set(key, next);
    return next;
  };
  const emit = (event: Event): void => {
    actions.push({ _tag: "Emit", event });
  };
  const asRun = (key: ItemKey, status: AsRunStatus): void => {
    set(key, { status });
    emit({ _tag: "AsRun", event: { key, at: now.wall, status } });
  };
  const session = (id: string | undefined): Session | undefined =>
    state.sessions.find((candidate) => candidate.id === id);
  const updateSession = (id: string, patch: Partial<Session>): void => {
    state = {
      ...state,
      sessions: state.sessions.map((value) => (value.id === id ? { ...value, ...patch } : value)),
    };
  };
  /** Records what a session's source reports, counting each change in whether it takes commands. */
  const report = (id: string, source: SourceState): void => {
    const previous = session(id);
    const changed = (previous?.source?.available ?? false) !== source.available;
    updateSession(id, { source, changes: (previous?.changes ?? 0) + (changed ? 1 : 0) });
  };
  /** The session new work goes to: the newest one that is live and not retiring. */
  const preferred = (): Session | undefined => preferredOf(state.sessions);
  const atMono = (item: Item): number | undefined =>
    item.spec.start._tag === "At" ? now.mono + (item.spec.start.time - now.wall) : undefined;

  /**
   * Answers a withdrawal waiting on one item. A group key's withdrawal waits on
   * each part and answers once all have: `withdrawn` if any part was, else
   * `already-started` if any started, as 0.7.0 answered it.
   */
  const answer = (
    waiter: { readonly id: number; readonly index: number },
    outcome: WithdrawOutcome,
  ): void => {
    const id = `${String(waiter.id)}:${String(waiter.index)}`;
    const join = state.joins.get(id);
    if (join === undefined) {
      actions.push({ _tag: "Withdrawn", id: waiter.id, index: waiter.index, outcome });
      return;
    }
    const outcomes = [...join.outcomes, outcome];
    const joins = new Map(state.joins);
    if (join.left > 1) joins.set(id, { left: join.left - 1, outcomes });
    else joins.delete(id);
    state = { ...state, joins };
    if (join.left > 1) return;
    const combined = outcomes.includes("withdrawn")
      ? "withdrawn"
      : outcomes.includes("already-started")
        ? "already-started"
        : "not-found";
    actions.push({ _tag: "Withdrawn", id: waiter.id, index: waiter.index, outcome: combined });
  };
  /** What a withdrawal waiting on an item learns from how it settled. */
  const outcomeOf = (item: Item, status: AsRunStatus): WithdrawOutcome =>
    status._tag === "Dropped"
      ? "withdrawn"
      : item.startedAt !== undefined || status._tag === "Ended"
        ? "already-started"
        : "not-found";
  /** Settles an item for good, resolving withdrawals that wait on it and recording history. */
  const settle = (key: ItemKey, status: AsRunStatus): void => {
    const item = items.get(key);
    if (item === undefined || item.phase === "Settled") return;
    // The as-run goes out first, so a withdrawal's caller finds it already published.
    set(key, { phase: "Settled", waiting: [], withdraw: undefined });
    asRun(key, status);
    const outcome = outcomeOf(item, status);
    for (const wait of item.waiting) answer(wait, outcome);
    state = { ...state, settled: [...state.settled, key] };
    // A part that fails or is dropped takes the parts after it with it; a replaced one does not,
    // since its replacement takes its place.
    if (
      item.group !== undefined &&
      !item.inserted &&
      (status._tag === "Failed" || (status._tag === "Dropped" && status.reason !== "replaced"))
    )
      for (const part of groups.get(item.group.key)?.parts ?? []) {
        const other = items.get(part);
        if (other?.group !== undefined && other.group.index > item.group.index)
          withdraw(part, "withdrawn");
      }
  };

  /**
   * Withdraws an item the plan no longer wants. What has no clip goes at once;
   * a clip is removed from its provider first, and a started one cannot go.
   */
  const withdraw = (
    key: ItemKey,
    reason: DropReason,
    waiter?: { readonly id: number; readonly index: number },
  ): void => {
    const item = items.get(key);
    if (item === undefined) {
      if (waiter !== undefined) answer(waiter, "not-found");
      return;
    }
    if (item.phase === "Started" || (item.phase === "Settled" && item.startedAt !== undefined)) {
      if (waiter !== undefined) answer(waiter, "already-started");
      return;
    }
    if (item.phase === "Settled") {
      if (waiter !== undefined)
        answer(waiter, item.status?._tag === "Dropped" ? "withdrawn" : "not-found");
      return;
    }
    const waiting = waiter === undefined ? item.waiting : [...item.waiting, waiter];
    if (item.phase === "Accepted") {
      set(key, { waiting });
      settle(key, { _tag: "Dropped", reason });
      return;
    }
    set(key, { withdraw: item.withdraw ?? reason, waiting });
  };

  const live = (item: Item | undefined): item is Item =>
    item !== undefined && item.phase !== "Settled";
  const readyOf = (value: Session): ReadonlyArray<SourceClip> => value.source?.ready ?? [];
  const itemOf = (clip: PlayingClip | undefined): Item | undefined =>
    clip?.tag?._tag === "Item" ? items.get(clip.tag.key) : undefined;

  // Ranks: lexicographic places in a session's Ready order and in the build order.
  const superseded = new Set(
    state.batches.flatMap((batch) => batch.targets.map((target) => target.key)),
  );
  const begun = (item: Item): boolean =>
    item.group !== undefined &&
    (groups.get(item.group.key)?.parts ?? []).some((part) => {
      const other = items.get(part);
      return other?.startedAt !== undefined || other?.phase === "Started";
    });
  /** The item still in the place a replacement takes; replaced replacements lead back to it. */
  const replacedOf = (item: Item): Item | undefined => {
    let old = item.replaces === undefined ? undefined : items.get(item.replaces);
    while (old?.phase === "Settled" && old.startedAt === undefined && old.replaces !== undefined)
      old = items.get(old.replaces);
    return old;
  };
  const rankItem = (item: Item): Rank => {
    const at = atMono(item);
    if (item.mode === "held" || (at !== undefined && now.mono < at) || item.batch !== undefined)
      return [lanes + 1, 1, item.order, item.generation];
    // Its build continues the Ready clip it follows, so it airs right behind that clip until
    // that clip starts. An item accepted again for a rebuild follows nothing yet.
    const followed =
      item.follows === undefined || item.phase === "Accepted"
        ? undefined
        : state.sessions
            .flatMap((value) => readyOf(value))
            .find((clip) => clip.clipId === item.follows);
    if (followed !== undefined) {
      const behind = rankClip(followed);
      return [behind[0], behind[1], behind[2], behind[3] + 0.5];
    }
    if (item.mode === "asap") return [-0.5, 0, item.order, item.generation];
    const displaced = [...items.values()].some(
      (other) =>
        (other.phase === "Ready" || other.phase === "Started") &&
        other.batch === undefined &&
        replacedOf(other)?.spec.key === item.spec.key,
    );
    return [
      item.spec.lane,
      superseded.has(item.spec.key) ? 2 : begun(item) ? 0 : 1,
      item.order,
      displaced ? Infinity : item.generation,
    ];
  };
  const rankClip = (clip: SourceClip): Rank => {
    if (clip.tag === undefined) return [-1, 0, 0, 0];
    if (clip.tag._tag === "Filler") return [lanes, 1, clip.tag.index, 0];
    const item = items.get(clip.tag.key);
    return item === undefined ? [-1, 0, 0, 0] : rankItem(item);
  };
  /** Which of two waiting items takes the build slot first. */
  const buildOrder = (a: Item, b: Item): number =>
    Number(b.mode === "asap") - Number(a.mode === "asap") ||
    Number(a.mode === "held") - Number(b.mode === "held") ||
    a.spec.lane - b.spec.lane ||
    Number(b.replaces !== undefined) - Number(a.replaces !== undefined) ||
    Number(b.group !== undefined && b.group.index > 0) -
      Number(a.group !== undefined && a.group.index > 0) ||
    (a.startBy ?? Infinity) - (b.startBy ?? Infinity) ||
    a.order - b.order;
  /** A group's part may build only once the part before it was admitted. */
  const previousAdmitted = (item: Item): boolean => {
    // An insert waits for whatever airs just before it in its lane to be admitted.
    if (item.inserted) {
      const before = [...items.values()]
        .filter(
          (other) => live(other) && other.spec.lane === item.spec.lane && other.order < item.order,
        )
        .sort((a, b) => b.order - a.order)[0];
      return before?.phase !== "Accepted";
    }
    if (item.group === undefined) return true;
    const index = item.group.index;
    return !(groups.get(item.group.key)?.parts ?? []).some((part) => {
      const other = items.get(part);
      return (
        other?.group !== undefined &&
        !other.inserted &&
        other.group.index === index - 1 &&
        other.phase === "Accepted"
      );
    });
  };

  const estimates = (): PublicState["estimates"] => estimatesOf(state.samples);
  const playingRestMs = (value: Session | undefined): number => playingRestOf(value, now.mono);
  const airs = (clip: SourceClip): boolean => {
    const item = itemOf(clip);
    if (item === undefined) return true;
    const at = atMono(item);
    return (
      item.mode !== "held" &&
      item.batch === undefined &&
      (at === undefined || at <= now.mono) &&
      item.withdraw === undefined
    );
  };
  /** Seconds of air secured on the session that takes new work. */
  const runway = (): number => {
    const target = preferred() ?? session(state.air);
    if (target === undefined) return 0;
    const onAir = target.id === state.air ? playingRestMs(target) / 1000 : 0;
    return (
      onAir +
      readyOf(target)
        .filter(airs)
        .reduce((total, clip) => total + clip.seconds, 0)
    );
  };
  /** The earliest a clip in `item`'s place could start, optimistically. */
  const projectedStart = (item: Item): number => {
    const target = preferred() ?? session(state.air);
    const rank = rankItem(item);
    const aheadMs = (target === undefined ? [] : readyOf(target))
      .filter(
        (clip) => compareRank(rankClip(clip), rank) < 0 && itemOf(clip)?.withdraw === undefined,
      )
      .reduce((total, clip) => total + clip.seconds * 1000, 0);
    const playable = now.mono + playingRestMs(session(state.air)) + aheadMs;
    const perSecond = estimates().build?.median;
    if (perSecond === undefined) return playable;
    const buildMs = (seconds: number) => seconds * perSecond * 1000;
    const inFlight = [
      ...[...items.values()].flatMap((other) =>
        other.phase === "Building" &&
        other.dispatchedAt !== undefined &&
        other.spec.key !== item.spec.key
          ? [other.dispatchedAt + buildMs(other.spec.seconds)]
          : [],
      ),
      ...(state.filler.dispatchedAt === undefined || state.filler.seconds === undefined
        ? []
        : [state.filler.dispatchedAt + buildMs(state.filler.seconds)]),
    ];
    const first = [...items.values()]
      .filter(
        (other) =>
          other.phase === "Accepted" &&
          other.spec.key !== item.spec.key &&
          other.withdraw === undefined &&
          other.mode !== "held" &&
          (atMono(other) ?? -Infinity) <= now.mono &&
          (other.notBefore ?? -Infinity) <= now.mono &&
          previousAdmitted(other) &&
          buildOrder(other, item) < 0,
      )
      .reduce((total, other) => total + buildMs(other.spec.seconds), 0);
    return Math.max(playable, Math.max(now.mono, ...inFlight) + first + buildMs(item.spec.seconds));
  };

  const newItem = (spec: Spec, place: Partial<Item> = {}): Item => ({
    spec,
    order: state.nextOrder,
    generation: 0,
    inserted: false,
    mode: spec.start._tag === "Asap" ? "asap" : spec.start._tag === "Manual" ? "held" : "follow",
    phase: "Accepted",
    notBefore:
      spec.window?.notBeforeMs === undefined ? undefined : now.mono + spec.window.notBeforeMs,
    startBy: spec.window?.startByMs === undefined ? undefined : now.mono + spec.window.startByMs,
    everUnknown: false,
    unbuiltLosses: 0,
    fired: [],
    waiting: [],
    ...place,
  });

  const applyEdit = (id: number, edits: ReadonlyArray<EditInput>, batched: boolean): void => {
    // A drain stops admissions, not withdrawals.
    if (state.closed || (!state.accepting && edits.some((edit) => edit._tag !== "Withdraw"))) {
      actions.push({ _tag: "Refused", id, refusal: { _tag: "PlayoutClosed" } });
      return;
    }
    const refuse = (refusal: Refusal): void => {
      actions.push({ _tag: "Refused", id, refusal });
    };
    const seen = new Set<string>();
    // Check every edit before any takes effect.
    for (const edit of edits) {
      const keys =
        edit._tag === "SubmitGroup"
          ? [edit.key, ...edit.parts.map((part) => part.key)]
          : edit._tag === "Withdraw"
            ? []
            : [edit.spec.key];
      for (const key of keys) {
        if (seen.has(key))
          return refuse({ _tag: "InvalidItem", key, message: "a key appears twice in one batch" });
        seen.add(key);
        const existing = items.get(key);
        const fingerprint =
          edit._tag === "SubmitGroup" && key === edit.key
            ? edit.fingerprint
            : edit._tag === "SubmitGroup"
              ? edit.parts.find((part) => part.key === key)?.fingerprint
              : edit._tag === "Withdraw"
                ? undefined
                : edit.spec.fingerprint;
        const group = groups.get(key);
        if (
          (existing !== undefined && existing.spec.fingerprint !== fingerprint) ||
          (group !== undefined && group.fingerprint !== fingerprint) ||
          // An insert's place is part of it; a new group cannot take another item's key as a part.
          (edit._tag === "Insert" &&
            existing !== undefined &&
            (existing.anchor?.key !== edit.anchor || existing.anchor.side !== edit.side)) ||
          (edit._tag === "SubmitGroup" &&
            key !== edit.key &&
            existing !== undefined &&
            !groups.has(edit.key))
        )
          return refuse({ _tag: "KeyMismatch", key: key });
      }
      if (edit._tag === "Insert") {
        const anchor = items.get(edit.anchor) ?? firstOrLastPart(edit.anchor, edit.side);
        // After the playing item is the next boundary; before it is already past.
        if (!live(anchor) || (anchor.phase === "Started" && edit.side === "before"))
          return refuse({
            _tag: "InvalidItem",
            key: edit.spec.key,
            message: "the anchor is not waiting to air",
          });
        if (anchor.mode === "held")
          return refuse({
            _tag: "InvalidItem",
            key: edit.spec.key,
            message: "a held Manual item cannot anchor an insert",
          });
      }
      if (edit._tag === "Replace" && !live(items.get(edit.key)) && !items.has(edit.spec.key))
        return refuse({
          _tag: "InvalidItem",
          key: edit.key,
          message: "no waiting item has this key",
        });
      if (edit._tag === "Submit" && !items.has(edit.spec.key)) {
        const lane = config.lanes[edit.spec.lane];
        if (
          lane?.conflict === "skip" &&
          [...items.values()].some((item) => item.spec.lane === edit.spec.lane && live(item))
        )
          return refuse({ _tag: "LaneBusy", key: edit.spec.key, lane: edit.spec.lane });
      }
    }
    const results: Array<EditReply> = [];
    const adds: Array<ItemKey> = [];
    const targets: Array<Batch["targets"][number]> = [];
    /** Group-key withdrawals by position, and how many parts each waits on. */
    const joined: Array<{ readonly index: number; readonly parts: number }> = [];
    const put = (item: Item): void => {
      items.set(item.spec.key, item);
      adds.push(item.spec.key);
      state = { ...state, nextOrder: state.nextOrder + 1 };
    };
    for (const [index, edit] of edits.entries()) {
      switch (edit._tag) {
        case "Submit": {
          if (!items.has(edit.spec.key)) {
            const lane = config.lanes[edit.spec.lane];
            if (lane?.conflict === "replace") {
              batched = true;
              for (const other of items.values())
                if (
                  other.spec.lane === edit.spec.lane &&
                  (other.phase === "Accepted" ||
                    other.phase === "Building" ||
                    other.phase === "Ready" ||
                    other.phase === "Unknown")
                )
                  targets.push({ key: other.spec.key, index: -1, reason: "replaced" });
            }
            put(newItem(edit.spec));
          }
          results.push({ _tag: "Added", key: edit.spec.key });
          break;
        }
        case "SubmitGroup": {
          if (!groups.has(edit.key)) {
            groups.set(edit.key, {
              fingerprint: edit.fingerprint,
              parts: edit.parts.map((part) => part.key),
            });
            edit.parts.forEach((part, partIndex) => {
              const { window: _window, ...rest } = part;
              put(
                newItem(partIndex === 0 ? part : rest, {
                  group: { key: edit.key, index: partIndex },
                }),
              );
            });
          }
          results.push({
            _tag: "AddedGroup",
            key: edit.key,
            parts: edit.parts.map((part) => part.key),
          });
          break;
        }
        case "Insert": {
          if (!items.has(edit.spec.key)) {
            const anchor = items.get(edit.anchor) ?? firstOrLastPart(edit.anchor, edit.side)!;
            const sameLane = [...items.values()]
              .filter((other) => other.spec.lane === anchor.spec.lane && other !== anchor)
              .map((other) => other.order);
            const neighbour =
              edit.side === "before"
                ? Math.max(anchor.order - 1, ...sameLane.filter((order) => order < anchor.order))
                : Math.min(anchor.order + 1, ...sameLane.filter((order) => order > anchor.order));
            // A playing anchor's start is spent: the insert follows it at the next boundary.
            const playing = anchor.phase === "Started";
            put(
              newItem(
                {
                  ...edit.spec,
                  lane: anchor.spec.lane,
                  start: playing ? { _tag: "Follow" } : anchor.spec.start,
                },
                {
                  order: (anchor.order + neighbour) / 2,
                  group:
                    anchor.group === undefined
                      ? undefined
                      : { key: anchor.group.key, index: anchor.group.index },
                  inserted: true,
                  anchor: { key: edit.anchor, side: edit.side },
                  mode: playing ? "follow" : anchor.mode,
                },
              ),
            );
            state = { ...state, nextOrder: state.nextOrder - 1 };
          }
          results.push({ _tag: "Added", key: edit.spec.key });
          break;
        }
        case "Replace": {
          if (!items.has(edit.spec.key)) {
            const old = items.get(edit.key)!;
            put(
              newItem(
                { ...edit.spec, lane: old.spec.lane, start: old.spec.start },
                {
                  order: old.order,
                  generation: old.generation + 1,
                  group: old.group,
                  inserted: old.inserted,
                  replaces: old.spec.key,
                  mode: old.mode,
                  startBy: old.startBy,
                },
              ),
            );
            state = { ...state, nextOrder: state.nextOrder - 1 };
            // A replacement takes its part's place: withdrawing the group or that place reaches it.
            const group =
              old.group === undefined || old.inserted ? undefined : groups.get(old.group.key);
            if (old.group !== undefined && group !== undefined)
              groups.set(old.group.key, { ...group, parts: [...group.parts, edit.spec.key] });
          }
          results.push({ _tag: "Added", key: edit.spec.key });
          break;
        }
        case "Withdraw": {
          const group = groups.get(edit.key);
          const keys = group === undefined ? [edit.key] : group.parts;
          const known = keys.some((key) => items.has(key));
          if (!known) actions.push({ _tag: "Withdrawn", id, index, outcome: "not-found" });
          else if (group !== undefined) {
            // A group key withdraws every part, and answers once each part has.
            joined.push({ index, parts: group.parts.length });
            for (const key of group.parts) targets.push({ key, index, reason: "withdrawn" });
          } else if (items.get(edit.key)?.group !== undefined && !items.get(edit.key)!.inserted) {
            // A part key withdraws that part and every part after it, and answers for that part.
            const part = items.get(edit.key)!;
            for (const other of groups.get(part.group!.key)?.parts ?? [])
              if ((items.get(other)?.group?.index ?? -1) >= part.group!.index)
                targets.push({
                  key: other,
                  index: other === edit.key ? index : -1,
                  reason: "withdrawn",
                });
          } else targets.push({ key: edit.key, index, reason: "withdrawn" });
          results.push({ _tag: "Withdrawal" });
          break;
        }
      }
    }
    for (const key of adds) {
      const item = items.get(key)!;
      if (
        item.spec.window?.firm === true &&
        item.startBy !== undefined &&
        item.group?.index !== undefined &&
        item.group.index > 0
      )
        continue;
      if (
        item.spec.window?.firm === true &&
        item.startBy !== undefined &&
        projectedStart(item) > item.startBy
      ) {
        for (const added of adds) items.delete(added);
        for (const [group, value] of groups)
          if (value.parts.some((part) => adds.includes(part))) groups.delete(group);
        return refuse({ _tag: "WouldMissDeadline", key });
      }
    }
    if (joined.length > 0) {
      const joins = new Map(state.joins);
      for (const join of joined)
        joins.set(`${String(id)}:${String(join.index)}`, { left: join.parts, outcomes: [] });
      state = { ...state, joins };
    }
    const pending = batched && (adds.length > 0 || targets.length > 0);
    for (const key of adds) {
      set(key, { batch: pending && batched ? id : undefined });
      asRun(key, { _tag: "Accepted" });
    }
    actions.push({ _tag: "Accepted", id, results });
    if (pending || targets.length > 0) {
      // What nothing was built for covers nothing, so it goes at once, and is answered once.
      const later = targets.filter((target) => items.get(target.key)?.phase !== "Accepted");
      for (const target of targets)
        if (!later.includes(target))
          withdraw(
            target.key,
            target.reason,
            target.index >= 0 ? { id, index: target.index } : undefined,
          );
      state = { ...state, batches: [...state.batches, { id, adds, targets: later }] };
    } else actions.push({ _tag: "Committed", id });
  };
  /** A group's first or last waiting part by place; a replacement shares its part's place. */
  function firstOrLastPart(key: ItemKey, side: "before" | "after"): Item | undefined {
    const parts = (
      groups
        .get(key)
        ?.parts.map((part) => items.get(part))
        .filter(live) ?? []
    ).sort((a, b) => (a.group?.index ?? 0) - (b.group?.index ?? 0) || a.generation - b.generation);
    return side === "before" ? parts[0] : parts[parts.length - 1];
  }

  /** Records the clip a session plays, and when a new one's start was seen. */
  const nowPlaying = (sessionId: string, clip: PlayingClip): void => {
    const playing = session(sessionId)?.playing;
    // A later report of the same clip may name what an earlier one could not.
    if (playing?.clipId === clip.clipId) {
      updateSession(sessionId, {
        playing: {
          ...playing,
          tag: clip.tag ?? playing.tag,
          seconds: clip.seconds ?? playing.seconds,
        },
      });
      return;
    }
    updateSession(sessionId, {
      startedAny: true,
      playing: { ...clip, at: now.mono, wall: now.wall },
    });
    if (clip.tag?._tag === "Filler")
      emit({
        _tag: "Filler",
        index: clip.tag.index,
        phase: "Started",
        at: now.wall,
        seconds: clip.seconds,
      });
  };
  const started = (sessionId: string, clip: PlayingClip): void => {
    nowPlaying(sessionId, clip);
    // A clip on air ends a run of sessions that failed to set up or to play anything.
    state = { ...state, setupFailures: 0 };
    if (clip.tag?._tag !== "Item") return;
    const item = items.get(clip.tag.key);
    if (item === undefined || item.startedAt !== undefined || item.phase === "Settled") return;
    const lateBy = item.startBy === undefined ? undefined : now.mono - item.startBy;
    const at = atMono(item);
    const late =
      lateBy !== undefined && lateBy > 0
        ? lateBy
        : at !== undefined && now.mono > at
          ? now.mono - at
          : undefined;
    set(clip.tag.key, {
      phase: "Started",
      startedAt: now.mono,
      clipId: clip.clipId,
      sessionId,
      waiting: [],
      withdraw: undefined,
      airSeconds: clip.seconds,
    });
    asRun(clip.tag.key, {
      _tag: "Started",
      at: now.wall,
      sessionId,
      // A clip the provider named without its length is counted at the length requested.
      seconds: clip.seconds ?? item.spec.seconds,
      ...(late === undefined ? {} : { lateByMillis: Math.round(late) }),
    });
    // A withdrawal that waited on it is too late: it answers now, not when the clip ends.
    for (const wait of item.waiting) answer(wait, "already-started");
  };
  const forgetFiller = (clipId: string): void => {
    state = { ...state, fillers: new Map([...state.fillers].filter(([id]) => id !== clipId)) };
  };
  const ended = (sessionId: string, event: Extract<SourceEvent, { _tag: "Ended" }>): void => {
    const { clip } = event;
    updateSession(sessionId, {
      lastEndedAt: now.mono,
      ...(session(sessionId)?.playing?.clipId === clip.clipId ? { playing: undefined } : {}),
    });
    if (clip.tag?._tag === "Filler") {
      emit({
        _tag: "Filler",
        index: clip.tag.index,
        phase: "Ended",
        at: now.wall,
        seconds: clip.seconds,
      });
      return forgetFiller(clip.clipId);
    }
    const item = itemOf(clip);
    if (item === undefined || item.phase === "Settled") return;
    if (item.startedAt === undefined) {
      settle(item.spec.key, { _tag: "Unobserved" });
      return;
    }
    const aired = event.airedSeconds ?? (now.mono - item.startedAt) / 1000;
    settle(item.spec.key, {
      _tag: "Ended",
      at: now.wall,
      termination: event.termination,
      airedSeconds: aired,
    });
    state = {
      ...state,
      samples: {
        ...state.samples,
        length: [...state.samples.length, clip.seconds / item.spec.seconds].slice(-maxSamples),
      },
    };
  };
  /**
   * A moderation verdict. On `terminate` the latest enqueue sent to the session
   * is held to blame: it fails for good rather than be rebuilt on the next
   * session and flagged again, and enough such endings fail the playout.
   */
  const moderated = (
    sessionId: string,
    event: Extract<SourceEvent, { readonly _tag: "Moderated" }>,
  ): void => {
    const terminate = event.action === "terminate";
    const suspect = terminate ? session(sessionId)?.lastEnqueue : undefined;
    const key = suspect?._tag === "Item" ? suspect.key : undefined;
    emit({
      _tag: "Session",
      event: {
        _tag: "Moderated",
        sessionId,
        action: event.action,
        categories: event.categories,
        ...(key === undefined ? {} : { key }),
      },
    });
    if (!terminate) return;
    if (key !== undefined)
      settle(key, {
        _tag: "Failed",
        reason: { _tag: "Moderated", categories: event.categories },
      });
    // A flagged filler request is not asked for again.
    if (suspect?._tag === "Filler" && suspect.index === state.filler.index)
      state = {
        ...state,
        filler: { ...state.filler, index: state.filler.index + 1, request: undefined },
      };
    const moderations = state.moderations + 1;
    state = { ...state, moderations };
    if (moderations >= config.maxModerations)
      actions.push({
        _tag: "Fail",
        reason: `content moderation ended ${String(moderations)} sessions`,
        cause: "moderation",
      });
  };
  const failed = (event: Extract<SourceEvent, { readonly _tag: "Failed" }>): void => {
    if (event.clip.tag?._tag === "Filler") return forgetFiller(event.clip.clipId);
    const item = itemOf(event.clip);
    if (item !== undefined)
      settle(item.spec.key, {
        _tag: "Failed",
        reason: { _tag: "Clip", message: event.message, provider: event.provider },
      });
  };
  /** Reads a session's queues back into the plan: adoption by key, Ready, and clips that vanished. */
  const observe = (sessionId: string, source: SourceState): void => {
    report(sessionId, source);
    // What plays is what the provider reports; a clip seen playing without its start event
    // started no later than now.
    if (source.playing === undefined) updateSession(sessionId, { playing: undefined });
    else nowPlaying(sessionId, source.playing);
    const listed = new Map<string, "Building" | "Ready" | "Playing">();
    for (const clip of source.building) listed.set(clip.clipId, "Building");
    for (const clip of source.ready) listed.set(clip.clipId, "Ready");
    if (source.playing !== undefined) listed.set(source.playing.clipId, "Playing");
    const fillers = new Map(state.fillers);
    for (const clip of [
      ...source.building,
      ...source.ready,
      ...(source.playing === undefined ? [] : [source.playing]),
    ]) {
      if (clip.tag?._tag === "Filler")
        fillers.set(clip.clipId, { index: clip.tag.index, sessionId });
      if (clip.tag?._tag !== "Item") continue;
      const item = items.get(clip.tag.key);
      if (item === undefined || item.phase === "Settled") continue;
      const where = listed.get(clip.clipId)!;
      if (where !== "Playing" && item.discarded === clip.clipId) continue;
      if (where === "Playing") {
        if (item.startedAt === undefined) {
          if (item.clipId === undefined) set(item.spec.key, { clipId: clip.clipId, sessionId });
          started(sessionId, clip);
        }
        continue;
      }
      // Evidence only moves an item forward: a queue snapshot older than its start cannot undo it.
      const waiting =
        item.phase === "Accepted" || item.phase === "Building" || item.phase === "Unknown";
      if (where === "Ready" && waiting) {
        if (item.dispatchedAt !== undefined && !item.everUnknown) {
          // A continued build takes longer, so it is measured apart from independent ones.
          const kind = item.continued === true ? "continued" : "build";
          const sample = (now.mono - item.dispatchedAt) / 1000 / item.spec.seconds;
          state = {
            ...state,
            samples: {
              ...state.samples,
              [kind]: [...state.samples[kind], sample].slice(-maxSamples),
            },
          };
        }
        set(item.spec.key, {
          phase: "Ready",
          clipId: clip.clipId,
          sessionId,
          dispatchedAt: undefined,
          unknownSince: undefined,
          unbuiltLosses: 0,
        });
        asRun(item.spec.key, { _tag: "Ready", sessionId });
      } else if (
        where === "Building" &&
        waiting &&
        (item.phase !== "Building" || item.clipId === undefined)
      ) {
        set(item.spec.key, {
          phase: "Building",
          clipId: clip.clipId,
          sessionId,
          unknownSince: undefined,
        });
        asRun(item.spec.key, { _tag: "Building", sessionId });
      }
    }
    // A filler clip listed here proves where its uncertain enqueue went.
    const listedFiller = new Set(
      [...fillers.values()].flatMap((owner) =>
        owner.sessionId === sessionId ? [owner.index] : [],
      ),
    );
    const uncertain = session(sessionId)?.unknownFiller ?? [];
    if (uncertain.some((entry) => listedFiller.has(entry.index)))
      updateSession(sessionId, {
        unknownFiller: uncertain.filter((entry) => !listedFiller.has(entry.index)),
      });
    // A filler the provider no longer lists has aired or failed; either way it stops being ours.
    for (const [clipId, owner] of fillers)
      if (owner.sessionId === sessionId && !listed.has(clipId)) fillers.delete(clipId);
    state = { ...state, fillers };
    // A clip that left every queue without a start we saw may have aired unseen.
    for (const item of items.values())
      if (
        item.sessionId === sessionId &&
        item.clipId !== undefined &&
        (item.phase === "Building" || item.phase === "Ready") &&
        !listed.has(item.clipId)
      )
        settle(
          item.spec.key,
          item.withdraw !== undefined
            ? { _tag: "Dropped", reason: item.withdraw }
            : { _tag: "Unobserved" },
        );
  };
  /**
   * A session that is gone: its unaired clips are rebuilt from the plan, never
   * replayed. Reactor ends a session over flagged content, and need not say
   * so, so rebuilding is bounded twice. A clip lost before it was built with
   * two sessions in a row fails: a built clip passed screening, so it is
   * rebuilt however often it is lost, and never fails alongside a flagged one.
   * And a session lost before any clip sent to it started counts as a failed setup.
   * Whatever ended it here, it is closed: an owned session still running server-side
   * would otherwise bill beside its replacement.
   */
  const lose = (sessionId: string, reason: string, planned = false): void => {
    const lost = session(sessionId);
    if (lost === undefined) return;
    actions.push({ _tag: "Close", sessionId });
    let carried = 0;
    for (const item of items.values()) {
      if (item.sessionId !== sessionId || item.phase === "Settled") continue;
      if (item.phase === "Started")
        settle(item.spec.key, { _tag: "Failed", reason: { _tag: "Lost", sessionId } });
      else if (item.phase === "Unknown") settle(item.spec.key, { _tag: "Unknown", terminal: true });
      else if (item.withdraw !== undefined)
        settle(item.spec.key, { _tag: "Dropped", reason: item.withdraw });
      else if (!planned && item.phase === "Building" && item.unbuiltLosses + 1 >= 2)
        settle(item.spec.key, { _tag: "Failed", reason: { _tag: "Lost", sessionId } });
      else {
        carried++;
        set(item.spec.key, {
          phase: "Accepted",
          clipId: undefined,
          sessionId: undefined,
          dispatchedAt: undefined,
          ...(!planned && item.phase === "Building"
            ? { unbuiltLosses: item.unbuiltLosses + 1 }
            : {}),
        });
        asRun(item.spec.key, { _tag: "Accepted", carried: { sessionId } });
      }
    }
    if (!planned && !lost.startedAny && lost.lastEnqueue !== undefined) {
      const consecutive = state.setupFailures + 1;
      state = { ...state, setupFailures: consecutive };
      emit({
        _tag: "Session",
        event: {
          _tag: "SetupFailed",
          reason: `lost before a clip sent to it started: ${reason}`,
          consecutive,
        },
      });
      if (consecutive >= config.maxSetupFailures)
        actions.push({ _tag: "Fail", reason, cause: "lost" });
    }
    state = {
      ...state,
      sessions: state.sessions.filter((value) => value.id !== sessionId),
      fillers: new Map([...state.fillers].filter(([, owner]) => owner.sessionId !== sessionId)),
      busy: state.busy?.sessionId === sessionId ? undefined : state.busy,
    };
    if (lost.retiring && carried === 0 && state.air !== sessionId) return;
    emit({ _tag: "Session", event: { _tag: "Replaced", from: sessionId, reason, carried } });
    if (state.air === sessionId) {
      const next = preferred() ?? state.sessions[state.sessions.length - 1];
      state = { ...state, air: next?.id };
      if (next !== undefined) {
        updateSession(next.id, { wantAutoplay: true, retiring: false });
        actions.push({ _tag: "OnAir", sessionId: next.id });
      }
    }
  };

  const cutFailed = (sessionId: string): void => {
    if (state.cutting?.sessionId === sessionId)
      state = { ...state, cutting: { ...state.cutting, stage: "failed" } };
  };
  const applyResult = (id: number, result: CommandResult): void => {
    const busy = state.busy;
    if (busy === undefined || busy.id !== id) return;
    state = { ...state, busy: undefined };
    const command = busy.command;
    switch (command._tag) {
      case "Enqueue": {
        if (command.tag._tag === "Filler") {
          const filler = state.filler;
          if (result._tag === "Done") {
            state = {
              ...state,
              filler: { ...filler, index: filler.index + 1, request: undefined },
              fillers:
                result.clipId === undefined
                  ? state.fillers
                  : new Map([
                      ...state.fillers,
                      [result.clipId, { index: filler.index, sessionId: busy.sessionId }],
                    ]),
            };
          } else if (uncertain(result)) {
            // Its clip may be building on that session: it holds that session's build slot until
            // a queue read shows it, the deadline passes or the session goes.
            state = {
              ...state,
              filler: { ...filler, index: filler.index + 1, request: undefined },
            };
            updateSession(busy.sessionId, {
              unknownFiller: [
                ...(session(busy.sessionId)?.unknownFiller ?? []),
                { index: filler.index, since: now.mono },
              ],
            });
          } else
            state = {
              ...state,
              filler: { ...filler, retryAt: now.mono + retryDelayMs, dispatchedAt: undefined },
            };
          return;
        }
        const item = items.get(command.tag.key);
        if (item === undefined || item.phase === "Settled") return;
        const owner = session(busy.sessionId);
        if (result._tag === "Done") {
          set(item.spec.key, { unsent: undefined });
          if (item.clipId === undefined)
            set(item.spec.key, { clipId: result.clipId, sessionId: busy.sessionId });
          if (item.phase === "Unknown")
            set(item.spec.key, { phase: "Building", unknownSince: undefined });
          const now_ = items.get(item.spec.key)!;
          if (now_.phase === "Building" && now_.status?._tag !== "Building")
            asRun(item.spec.key, { _tag: "Building", sessionId: busy.sessionId });
        } else if (uncertain(result)) {
          if (item.clipId === undefined) {
            set(item.spec.key, {
              phase: "Unknown",
              sessionId: busy.sessionId,
              everUnknown: true,
              unknownSince: now.mono,
            });
            asRun(item.spec.key, { _tag: "Unknown" });
          }
        } else if (result._tag === "Failed" && result.cause.isRetryable)
          set(item.spec.key, {
            phase: "Accepted",
            sessionId: undefined,
            dispatchedAt: undefined,
            retryAt: now.mono + retryDelayMs,
          });
        else if (
          result._tag === "Failed" &&
          unready(result.cause) &&
          // Refused again while the session says it takes commands, nothing is left to wait for.
          !(item.unsent?.sessionId === busy.sessionId && owner?.source?.available === true)
        )
          set(item.spec.key, {
            phase: "Accepted",
            sessionId: undefined,
            dispatchedAt: undefined,
            unsent: { sessionId: busy.sessionId, changes: owner?.changes ?? 0 },
          });
        else if (result._tag === "Failed")
          settle(item.spec.key, {
            _tag: "Failed",
            reason: { _tag: "Command", cause: result.cause },
          });
        return;
      }
      case "Remove": {
        const owner = [...items.values()].find(
          (item) => item.clipId === command.clipId && item.phase !== "Settled",
        );
        if (owner === undefined) {
          if (result._tag === "Done") forgetFiller(command.clipId);
          return;
        }
        if (result._tag === "Done")
          settle(owner.spec.key, { _tag: "Dropped", reason: owner.withdraw ?? "withdrawn" });
        else set(owner.spec.key, { blockedRemove: signature(session(busy.sessionId)) });
        return;
      }
      case "Move":
        if (result._tag === "Failed")
          state = { ...state, blockedMove: signature(session(busy.sessionId)) + command.clipId };
        return;
      case "Autoplay":
        if (result._tag === "Done") updateSession(busy.sessionId, { autoplay: command.enabled });
        else if (!command.enabled) cutFailed(busy.sessionId);
        return;
      // Its clip was marked cut when the cut began; no result makes it cuttable again.
      case "Stop":
        if (result._tag === "Failed") return cutFailed(busy.sessionId);
        if (state.cutting?.sessionId === busy.sessionId && state.cutting.clipId === command.clipId)
          state = { ...state, cutting: { ...state.cutting, stage: "stopped" } };
        return;
      case "Play":
        if (result._tag === "Failed") return cutFailed(busy.sessionId);
        if (state.cutting?.sessionId === busy.sessionId && state.cutting.next === command.clipId)
          state = { ...state, cutting: { ...state.cutting, stage: "played" } };
        return;
    }
  };

  switch (input._tag) {
    case "Edit":
      applyEdit(input.id, input.edits, input.batch);
      break;
    case "Release": {
      const item = items.get(input.key);
      if (!live(item) || item.mode !== "held")
        actions.push({
          _tag: "Refused",
          id: input.id,
          refusal: { _tag: "InvalidItem", key: input.key, message: "no held item has this key" },
        });
      else {
        set(input.key, { mode: "asap" });
        actions.push({ _tag: "Accepted", id: input.id, results: [] });
      }
      break;
    }
    case "Drain":
      state = {
        ...state,
        accepting: false,
        drains: [...state.drains, { id: input.id, finish: input.finish }],
      };
      for (const item of items.values())
        if (
          live(item) &&
          (item.mode === "held" || (input.finish === "playing" && item.phase !== "Started"))
        )
          withdraw(item.spec.key, "withdrawn");
      break;
    case "Opened": {
      const first = state.air === undefined;
      state = {
        ...state,
        opening: false,
        air: first ? input.sessionId : state.air,
        sessions: [
          ...state.sessions,
          {
            id: input.sessionId,
            openedAt: now.mono,
            lifetimeMs: input.lifetimeMs,
            source: undefined,
            changes: 0,
            autoplay: undefined,
            wantAutoplay: first,
            retiring: false,
            lastEndedAt: undefined,
            startedAny: false,
            indeterminate: false,
            unknownFiller: [],
            playing: undefined,
            lastEnqueue: undefined,
          },
        ],
      };
      // The session on air before stays until the replacement can take over at a boundary.
      if (!first)
        for (const value of state.sessions)
          if (value.id !== input.sessionId) updateSession(value.id, { retiring: true });
      if (first) actions.push({ _tag: "OnAir", sessionId: input.sessionId });
      emit({
        _tag: "Session",
        event: {
          _tag: "Opened",
          sessionId: input.sessionId,
          lifetimeSeconds: input.lifetimeMs / 1000,
        },
      });
      break;
    }
    case "OpenFailed": {
      const consecutive = state.setupFailures + 1;
      state = {
        ...state,
        opening: false,
        setupFailures: consecutive,
        openRetryAt: now.mono + retryDelayMs * consecutive,
      };
      emit({ _tag: "Session", event: { _tag: "SetupFailed", reason: input.reason, consecutive } });
      if (input.fatal || consecutive >= config.maxSetupFailures)
        actions.push({ _tag: "Fail", reason: input.reason, cause: "open" });
      break;
    }
    case "Source": {
      if (session(input.sessionId) === undefined) break;
      const event = input.event;
      switch (event._tag) {
        case "State":
          observe(input.sessionId, event.state);
          break;
        case "Started":
          started(input.sessionId, event.clip);
          break;
        case "Ended":
          ended(input.sessionId, event);
          break;
        case "Failed":
          failed(event);
          break;
        case "Moderated":
          moderated(input.sessionId, event);
          break;
        case "Reconnecting": {
          // It takes no commands until its source reports a state again.
          const source = session(input.sessionId)?.source;
          if (source !== undefined) report(input.sessionId, { ...source, available: false });
          emit({ _tag: "Session", event: { _tag: "Reconnecting", sessionId: input.sessionId } });
          break;
        }
        case "Reconnected":
          emit({
            _tag: "Session",
            event: {
              _tag: "Reconnected",
              sessionId: input.sessionId,
              afterMillis: event.afterMillis,
            },
          });
          break;
        case "ReaderOverflow":
          emit({
            _tag: "ReaderOverflow",
            sessionId: input.sessionId,
            track: event.track,
            at: now.wall,
            pressure: event.pressure,
          });
          break;
      }
      break;
    }
    case "Lost":
      lose(input.sessionId, input.reason);
      break;
    case "Result":
      applyResult(input.id, input.result);
      break;
    case "Tick":
      break;
    case "Close": {
      state = { ...state, accepting: false, closed: true };
      // A pending batch's withdrawals take effect now: what they name never airs, and each
      // is answered with what became of its item (#64).
      for (const batch of state.batches)
        for (const target of batch.targets)
          withdraw(
            target.key,
            target.reason,
            target.index >= 0 ? { id: batch.id, index: target.index } : undefined,
          );
      for (const item of items.values()) {
        if (item.phase === "Settled") continue;
        if (item.phase === "Unknown") settle(item.spec.key, { _tag: "Unknown", terminal: true });
        else if (item.phase === "Started") settle(item.spec.key, { _tag: "Unobserved" });
        else if (item.withdraw !== undefined)
          settle(item.spec.key, { _tag: "Dropped", reason: item.withdraw });
        else settle(item.spec.key, { _tag: "Failed", reason: { _tag: "Closed" } });
      }
      for (const batch of state.batches)
        actions.push({ _tag: "Refused", id: batch.id, refusal: { _tag: "PlayoutClosed" } });
      for (const drain of state.drains) actions.push({ _tag: "Drained", id: drain.id });
      state = { ...state, batches: [], drains: [] };
      break;
    }
  }
  if (state.closed) return { state: { ...state, items }, actions, wake: undefined };

  // Sweep every waiting item, not only the heads: expiry must not strand behind a live one.
  const boundaryLate = (item: Item): boolean => {
    const at = atMono(item);
    if (at === undefined || item.spec.start._tag !== "At") return false;
    const late = item.spec.start.late;
    return late === "nextBoundary"
      ? false
      : late === "drop"
        ? now.mono > at
        : now.mono - at > late.skipAfterMs;
  };
  for (const item of [...items.values()]) {
    if (item.phase !== "Accepted" && item.phase !== "Building" && item.phase !== "Ready") continue;
    if (
      (item.spec.window?.firm === true && item.startBy !== undefined && now.mono >= item.startBy) ||
      boundaryLate(item)
    )
      withdraw(item.spec.key, "late");
    else if (
      item.phase === "Accepted" &&
      item.spec.window?.firm === true &&
      item.startBy !== undefined &&
      item.dispatchedAt === undefined &&
      estimates().build !== undefined &&
      projectedStart(item) > item.startBy
    )
      withdraw(item.spec.key, "late");
  }
  // A replacement takes the place once Ready, or at once if nothing was built for what it
  // replaces; if what it replaces starts first, the replacement goes instead.
  for (const item of [...items.values()]) {
    if (item.replaces === undefined || item.phase === "Settled") continue;
    const old = replacedOf(item);
    if (old === undefined || old.phase === "Settled") continue;
    if (old.phase === "Started") {
      if (item.phase !== "Started") withdraw(item.spec.key, "withdrawn");
    } else if (
      (item.phase === "Ready" && item.batch === undefined) ||
      item.phase === "Started" ||
      (old.phase === "Accepted" && old.dispatchedAt === undefined)
    )
      withdraw(old.spec.key, "replaced");
  }
  // A batch takes effect once everything it adds is Ready or has settled.
  for (const batch of state.batches) {
    const done = batch.adds.every((key) => {
      const phase = items.get(key)?.phase;
      return phase === undefined || phase === "Ready" || phase === "Started" || phase === "Settled";
    });
    if (!done) continue;
    state = { ...state, batches: state.batches.filter((value) => value.id !== batch.id) };
    for (const key of batch.adds) if (items.has(key)) set(key, { batch: undefined });
    for (const target of batch.targets)
      withdraw(
        target.key,
        target.reason,
        target.index >= 0 ? { id: batch.id, index: target.index } : undefined,
      );
    actions.push({ _tag: "Committed", id: batch.id });
  }

  // An enqueue whose outcome stays unknown past the deadline makes its session indeterminate:
  // it takes no new work and a replacement takes over, now rather than at the next input. One
  // not on air has nothing to finish, so it goes at once.
  const expired = (since: number | undefined): boolean =>
    since !== undefined && now.mono - since >= config.unknownTimeoutMs;
  for (const value of [...state.sessions]) {
    if (value.indeterminate) continue;
    const stuck =
      value.unknownFiller.some((entry) => expired(entry.since)) ||
      [...items.values()].some(
        (item) =>
          item.phase === "Unknown" && item.sessionId === value.id && expired(item.unknownSince),
      );
    if (!stuck) continue;
    updateSession(value.id, { indeterminate: true });
    if (value.id !== state.air) lose(value.id, "an enqueue's outcome stayed unknown");
  }

  // Renewal.
  const air = session(state.air);
  const expiresAt = (value: Session) => value.openedAt + value.lifetimeMs;
  const wantsAir =
    state.accepting ||
    state.drains.some((drain) => drain.finish === "accepted") ||
    [...items.values()].some(live);
  if (!state.opening && now.mono >= state.openRetryAt && wantsAir) {
    const replacementLive = state.sessions.some(
      (value) => !value.retiring && value.id !== state.air,
    );
    const next = air === undefined || replacementLive ? undefined : eligible()[0];
    const due =
      air === undefined ||
      (!replacementLive &&
        (air.indeterminate ||
          now.mono >= expiresAt(air) - config.leadMs ||
          (next !== undefined && !fits(air, next.spec.seconds))));
    if (due && state.sessions.length < 2) {
      state = { ...state, opening: true };
      actions.push({ _tag: "Open" });
    }
  }
  for (const value of state.sessions)
    if (now.mono >= expiresAt(value) && value.lifetimeMs !== Infinity)
      lose(value.id, "the session's granted length ended");
  const current = session(state.air);
  const next = state.sessions.find((value) => value.id !== state.air && !value.retiring);
  if (current !== undefined && next !== undefined && next.source?.available === true) {
    // The drain rule: once the replacement has an item Ready, the retiring filler goes.
    if (readyOf(next).some((clip) => clip.tag?._tag === "Item"))
      for (const clip of readyOf(current))
        if (clip.tag?._tag === "Filler" && state.busy === undefined)
          queueCommand(current.id, { _tag: "Remove", clipId: clip.clipId });
    const idle =
      current.source !== undefined &&
      current.source.playing === undefined &&
      current.source.ready.length === 0 &&
      current.source.building.every((clip) => clip.tag === undefined) &&
      ![...items.values()].some(
        (item) =>
          item.sessionId === current.id &&
          (item.phase === "Building" || (item.phase === "Unknown" && !expired(item.unknownSince))),
      ) &&
      current.unknownFiller.every((entry) => expired(entry.since));
    const graceOver =
      current.lastEndedAt !== undefined && now.mono >= current.lastEndedAt + config.graceMs;
    if (idle && (!current.startedAny || graceOver)) {
      state = { ...state, air: next.id };
      updateSession(next.id, { wantAutoplay: true });
      actions.push({ _tag: "OnAir", sessionId: next.id });
      emit({
        _tag: "Session",
        event: {
          _tag: "Switched",
          from: current.id,
          to: next.id,
          decision: current.startedAny ? "grace-elapsed" : "no-observed-start",
        },
      });
      lose(current.id, "retired", true);
    }
  }
  // Starvation: nothing on air while the plan wants air.
  const onAir = session(state.air);
  const taking = state.sessions.some(
    (value) => value.id !== state.air && !value.retiring && readyOf(value).length > 0,
  );
  const dry =
    onAir?.source !== undefined &&
    onAir.source.playing === undefined &&
    onAir.source.ready.length === 0 &&
    onAir.startedAny &&
    !taking;
  const owed =
    [...items.values()].some((item) => item.phase !== "Settled" && item.mode !== "held") ||
    config.filler !== undefined;
  if (dry && owed && !state.starving) {
    state = { ...state, starving: true, starved: state.starved + 1 };
    emit({ _tag: "Starved", at: now.wall });
  } else if (!dry && state.starving) state = { ...state, starving: false };

  const fireCue = (item: Item, index: number): void =>
    emit({
      _tag: "Cue",
      event: { key: item.spec.key, name: item.spec.cues[index]!.name, at: now.wall },
    });
  // Cues of the playing item fall due on its observed start.
  for (const item of [...items.values()]) {
    if (item.phase !== "Started" || item.startedAt === undefined || item.spec.cues.length === 0)
      continue;
    const due = item.spec.cues
      .map((cue, index) => ({ index, at: cueAt(item, cue) }))
      .filter(({ index, at }) => !item.fired.includes(index) && at <= now.mono)
      .sort((a, b) => a.at - b.at);
    for (const { index } of due) fireCue(item, index);
    if (due.length > 0)
      set(item.spec.key, { fired: [...item.fired, ...due.map(({ index }) => index)] });
  }
  // A playing clip's cues after its end fire at the end, if it ended there.
  for (const item of [...items.values()])
    if (
      item.phase === "Settled" &&
      item.status?._tag === "Ended" &&
      item.startedAt !== undefined &&
      item.fired.length < item.spec.cues.length
    ) {
      const fire = item.spec.cues
        .map((_, index) => index)
        .filter(
          (index) =>
            !item.fired.includes(index) && cueAt(item, item.spec.cues[index]!) <= now.mono + 1,
        );
      for (const index of fire) fireCue(item, index);
      set(item.spec.key, { fired: item.spec.cues.map((_, index) => index) });
    }

  // Drains finish once nothing they wait for is left.
  for (const drain of state.drains) {
    const pending = [...items.values()].some(
      (item) => live(item) && (drain.finish === "accepted" || item.phase === "Started"),
    );
    if (!pending) {
      actions.push({ _tag: "Drained", id: drain.id });
      state = { ...state, drains: state.drains.filter((value) => value.id !== drain.id) };
    }
  }

  // One provider command at a time.
  if (state.busy === undefined) decideCommand();

  // History: settled keys past the bound are forgotten, oldest first.
  if (state.settled.length > config.maxHistory) {
    const drop = state.settled.slice(0, state.settled.length - config.maxHistory);
    for (const key of drop) items.delete(key);
    actions.push({ _tag: "Forget", keys: drop });
    for (const [group, value] of groups)
      if (value.parts.every((part) => !items.has(part))) groups.delete(group);
    state = { ...state, settled: state.settled.slice(drop.length) };
  }

  return { state: { ...state, items, groups }, actions, wake: wake() };

  function queueCommand(sessionId: string, command: Command): void {
    if (state.busy !== undefined) return;
    const id = state.nextCommand;
    state = { ...state, busy: { id, sessionId, command }, nextCommand: id + 1 };
    if (command._tag === "Enqueue") updateSession(sessionId, { lastEnqueue: command.tag });
    actions.push({ _tag: "Command", id, sessionId, command });
  }
  function signature(value: Session | undefined): string {
    const source = value?.source;
    return source === undefined
      ? ""
      : [...source.building, ...source.ready].map((clip) => clip.clipId).join(",") +
          ":" +
          (source.playing?.clipId ?? "");
  }
  function cueAt(item: Item, cue: Spec["cues"][number]): number {
    return cue.from === "start"
      ? item.startedAt! + cue.offsetMs
      : item.startedAt! + (item.airSeconds ?? item.spec.seconds) * 1000 - cue.offsetMs;
  }
  function decideCommand(): void {
    // A cut ends once its cutter has left its session's Ready queue; once a step failed with the
    // cutter still wanted, which then airs at the next boundary; or once a cutter no longer
    // wanted is not being removed.
    const cutting = state.cutting;
    const cutOn = session(cutting?.sessionId);
    const cutter = cutOn?.source?.ready.find((clip) => clip.clipId === cutting?.next);
    const cutterItem = itemOf(cutter);
    const wanted = cutter !== undefined && airs(cutter) && cutterItem?.phase === "Ready";
    const removing =
      cutterItem?.withdraw !== undefined && cutterItem.blockedRemove !== signature(cutOn);
    if (
      cutting !== undefined &&
      (cutter === undefined || (cutting.stage === "failed" && wanted) || (!wanted && !removing))
    )
      state = { ...state, cutting: undefined };
    // Autoplay as each session's role wants it: off on a replacement until it takes the air,
    // and off on the air while a cut is under way.
    for (const value of state.sessions) {
      const autoplay = value.wantAutoplay && state.cutting?.sessionId !== value.id;
      if (value.source?.available === true && value.autoplay !== autoplay)
        return queueCommand(value.id, { _tag: "Autoplay", enabled: autoplay });
    }
    // The cut's next step, with autoplay off: stop the clip it cuts, and once that has ended,
    // play the cutter. The plan looks again between them, so a cutter withdrawn meanwhile goes
    // instead, and until its play nothing else is sent that could hold it up.
    if (
      state.cutting !== undefined &&
      (state.cutting.stage === "stopping" || state.cutting.stage === "stopped") &&
      wanted &&
      cutOn?.source?.available === true
    ) {
      const playing = cutOn.source.playing?.clipId;
      if (state.cutting.stage === "stopping")
        return queueCommand(cutOn.id, { _tag: "Stop", clipId: state.cutting.clipId });
      if (playing === undefined)
        return queueCommand(cutOn.id, { _tag: "Play", clipId: state.cutting.next });
      if (playing === state.cutting.clipId) return;
      // Something else plays: the cutter waits for the next boundary.
      state = { ...state, cutting: undefined };
      return decideCommand();
    }
    // Withdrawals the plan wants, retried once a refused one's session changes.
    for (const item of items.values())
      if (
        item.withdraw !== undefined &&
        item.clipId !== undefined &&
        item.sessionId !== undefined &&
        (item.phase === "Building" || item.phase === "Ready")
      ) {
        const owner = session(item.sessionId);
        if (owner?.source?.available === true && item.blockedRemove !== signature(owner))
          return queueCommand(item.sessionId, { _tag: "Remove", clipId: item.clipId });
      }
    // A drain withdraws filler once nothing accepted still needs it to cover the wait.
    const fillerNeeded =
      state.drains.every((drain) => drain.finish === "accepted") &&
      [...items.values()].some(
        (item) =>
          live(item) &&
          item.mode !== "held" &&
          item.phase !== "Started" &&
          (item.phase !== "Ready" || (atMono(item) ?? -Infinity) > now.mono),
      );
    if (state.drains.length > 0 && !fillerNeeded)
      for (const value of state.sessions)
        for (const clip of readyOf(value))
          if (clip.tag?._tag === "Filler" && value.source?.available === true)
            return queueCommand(value.id, { _tag: "Remove", clipId: clip.clipId });
    // Order each session's Ready clips by rank. A move never ranks across sessions.
    for (const value of state.sessions) {
      if (value.source?.available !== true) continue;
      const actual = readyOf(value);
      const desired = [...actual].sort((a, b) => compareRank(rankClip(a), rankClip(b)));
      const moved = actual.findIndex((clip, index) => clip.clipId !== desired[index]!.clipId);
      if (moved < 0) continue;
      const wanted = desired[moved]!;
      const key = signature(value) + wanted.clipId;
      if (wanted.tag !== undefined && state.blockedMove !== key)
        return queueCommand(value.id, { _tag: "Move", clipId: wanted.clipId, position: moved });
    }
    // A cut lane's Ready item at the front cuts a lower lane's clip, or filler, that has a while to run.
    const onAirNow = session(state.air);
    const front = onAirNow === undefined ? undefined : readyOf(onAirNow)[0];
    const cutItem = itemOf(front);
    const playing = onAirNow?.source?.playing;
    if (
      onAirNow !== undefined &&
      front !== undefined &&
      cutItem !== undefined &&
      playing !== undefined &&
      state.cutting === undefined &&
      config.lanes[cutItem.spec.lane]?.cut === true &&
      state.cut !== playing.clipId &&
      playingRestMs(onAirNow) > cutMarginMs
    ) {
      // Only filler or a clip of a strictly lower lane is cut, never one of the cutter's lane or above.
      const playingItem = itemOf(playing);
      const lower =
        playing.tag?._tag === "Filler" ||
        (playingItem !== undefined && playingItem.spec.lane > cutItem.spec.lane);
      if (lower && airs(front)) {
        state = {
          ...state,
          cut: playing.clipId,
          cutting: {
            sessionId: onAirNow.id,
            clipId: playing.clipId,
            next: front.clipId,
            stage: "stopping",
          },
        };
        return decideCommand();
      }
    }
    // A held item about to be next, or an At item Ready too early, is removed and rebuilt later.
    for (const value of state.sessions) {
      const ready = readyOf(value);
      for (const [index, clip] of ready.entries()) {
        const item = itemOf(clip);
        if (item?.phase !== "Ready") continue;
        const aheadMs =
          playingRestMs(value) +
          ready
            .slice(0, index)
            .filter(airs)
            .reduce((total, other) => total + other.seconds * 1000, 0);
        const at = atMono(item);
        const exposed =
          item.mode === "held"
            ? aheadMs < exposureMarginMs
            : at !== undefined && at > now.mono && aheadMs < at - now.mono;
        if (exposed && value.source?.available === true) {
          // It goes back to the plan, to be built again once the air ahead covers its wait.
          set(item.spec.key, {
            phase: "Accepted",
            withdraw: undefined,
            dispatchedAt: undefined,
            clipId: undefined,
            sessionId: undefined,
            discarded: clip.clipId,
          });
          return queueCommand(value.id, { _tag: "Remove", clipId: clip.clipId });
        }
      }
    }
    // Build: the first eligible item by build order, else filler below its floor.
    const target = preferred();
    if (
      target === undefined ||
      !(state.accepting || state.drains.some((drain) => drain.finish === "accepted"))
    )
      return;
    const inFlight =
      [...items.values()].filter(
        (item) =>
          (item.phase === "Building" || item.phase === "Unknown") && item.sessionId === target.id,
      ).length +
      target.source!.building.filter((clip) => clip.tag?._tag === "Filler").length +
      target.unknownFiller.length;
    if (inFlight >= config.maxBuildsInFlight) return;
    const room = runway();
    const floorSeconds = fillerFloor();
    for (const item of eligible()) {
      // What cannot air before this session's cap waits for its replacement, and so does what
      // follows it, which would otherwise air ahead of it.
      if (!fits(target, item.spec.seconds)) break;
      const from = item.spec.continuity
        ? continuation(item, target)
        : { _tag: "from" as const, clipId: undefined, follows: undefined };
      if (from._tag === "wait") continue;
      set(item.spec.key, {
        phase: "Building",
        sessionId: target.id,
        dispatchedAt: now.mono,
        retryAt: undefined,
        continued: from.clipId !== undefined,
        follows: from.follows,
      });
      return queueCommand(target.id, {
        _tag: "Enqueue",
        request: item.spec.request,
        tag: { _tag: "Item", key: item.spec.key },
        continueFrom: from.clipId,
      });
    }
    const filler = config.filler;
    if (filler === undefined || now.mono < state.filler.retryAt) return;
    const anchorGap = Math.max(
      0,
      ...[...items.values()].flatMap((item) => {
        const at = atMono(item);
        return live(item) && at !== undefined && at > now.mono ? [(at - now.mono) / 1000] : [];
      }),
    );
    const targetSeconds = Math.max(filler.target, floorSeconds);
    const refilling =
      anchorGap > room || (state.filler.refilling ? room < targetSeconds : room < floorSeconds);
    state = { ...state, filler: { ...state.filler, refilling } };
    const drainingNeeds = state.drains.length === 0 || fillerNeeded;
    if (!refilling || room >= Math.max(targetSeconds, anchorGap) || !drainingNeeds) return;
    const seconds = fillLength(anchorGap - room, filler.lengths, estimates().length);
    if (!fits(target, seconds)) return;
    const request =
      state.filler.request ??
      filler.clip({ index: state.filler.index, runwaySeconds: room, seconds });
    state = { ...state, filler: { ...state.filler, request, dispatchedAt: now.mono, seconds } };
    queueCommand(target.id, {
      _tag: "Enqueue",
      request,
      tag: { _tag: "Filler", index: state.filler.index },
    });
  }
  /** Items that may be built now, first in build order. */
  function eligible(): ReadonlyArray<Item> {
    const room = runway();
    const floorSeconds = fillerFloor();
    const target = preferred();
    return [...items.values()]
      .filter(
        (item) =>
          item.phase === "Accepted" &&
          item.withdraw === undefined &&
          previousAdmitted(item) &&
          (item.retryAt ?? -Infinity) <= now.mono &&
          // A session that refused it unsent gets it again only once what it reports has changed.
          (item.unsent === undefined ||
            item.unsent.sessionId !== target?.id ||
            item.unsent.changes !== target.changes) &&
          (item.notBefore ?? -Infinity) <= now.mono &&
          // Autoplay cannot hold a Ready clip: build a future anchor once air ahead covers the wait.
          ((atMono(item) ?? -Infinity) <= now.mono ||
            room >= ((atMono(item) ?? 0) - now.mono) / 1000) &&
          // A held item is built ahead only while filler keeps the runway at its floor.
          (item.mode !== "held" || (floorSeconds > 0 && room >= floorSeconds)),
      )
      .sort(buildOrder);
  }
  /**
   * Whether a clip of `seconds` built on `target` now would finish airing before
   * the session's cap, counting everything that airs ahead of it there: the
   * playing clip's rest, its Ready clips, its builds in flight and, for a
   * replacement, what the session on air still has. A clip no fresh session
   * could air whole is not held back.
   */
  function fits(target: Session, seconds: number): boolean {
    if (target.lifetimeMs === Infinity) return true;
    const ratio = estimates().length;
    const lengthMs = seconds * ratio * 1000;
    const marginMs = lookaheadMarginSeconds * 1000;
    if (lengthMs > target.lifetimeMs - marginMs) return true;
    const queuedMs = (value: Session): number =>
      readyOf(value)
        .filter(airs)
        .reduce((total, clip) => total + clip.seconds * 1000, 0);
    const onAir = session(state.air);
    const startsAt =
      target.id === state.air || onAir === undefined
        ? now.mono + playingRestMs(target)
        : now.mono + playingRestMs(onAir) + queuedMs(onAir);
    const buildingMs =
      [...items.values()]
        .filter(
          (item) =>
            item.sessionId === target.id && (item.phase === "Building" || item.phase === "Unknown"),
        )
        .reduce((total, item) => total + item.spec.seconds * ratio * 1000, 0) +
      (target.source?.building ?? [])
        .filter((clip) => clip.tag?._tag === "Filler")
        .reduce((total, clip) => total + clip.seconds * 1000, 0);
    return (
      startsAt + queuedMs(target) + buildingMs + lengthMs <=
      target.openedAt + target.lifetimeMs - marginMs
    );
  }
  function fillerFloor(): number {
    const filler = config.filler;
    if (filler === undefined || filler.floor <= 0) return 0;
    const p95 = estimates().build?.p95;
    // A floor covers one p95 build of the next filler clip, so a refill started there is Ready in time.
    return p95 === undefined
      ? filler.floor
      : Math.max(filler.floor, p95 * filler.lengths.min + lookaheadMarginSeconds);
  }
  /**
   * What a continuing item continues from: the clip that airs just before it on
   * the same session, or the one that will by the time it is built
   * (`airsBefore`), if the provider still offers it. It waits while that
   * predecessor is an item not built yet; there is nothing to continue across sessions.
   */
  function continuation(
    item: Item,
    target: Session,
  ):
    | { readonly _tag: "wait" }
    | {
        readonly _tag: "from";
        readonly clipId: string | undefined;
        readonly follows?: string | undefined;
      } {
    const place = rankItem(item);
    const before = (rank: Rank) => compareRank(rank, place) < 0;
    let best: { readonly rank: Rank; readonly clipId: string | undefined } | undefined;
    for (const clip of readyOf(target)) {
      const rank = rankClip(clip);
      const owner = itemOf(clip);
      // What this item replaces, or a batch is taking off, is no predecessor.
      if (
        before(rank) &&
        owner?.withdraw === undefined &&
        (owner === undefined || (!superseded.has(owner.spec.key) && replacedOf(item) !== owner)) &&
        (best === undefined || compareRank(rank, best.rank) > 0)
      )
        best = { rank, clipId: clip.clipId };
    }
    for (const other of items.values())
      if (
        other !== item &&
        other.withdraw === undefined &&
        !superseded.has(other.spec.key) &&
        replacedOf(item) !== other &&
        (other.phase === "Accepted" || other.phase === "Building" || other.phase === "Unknown")
      ) {
        const rank = rankItem(other);
        if (before(rank) && (best === undefined || compareRank(rank, best.rank) > 0))
          best = { rank, clipId: undefined };
      }
    if (best !== undefined && best.clipId === undefined) return { _tag: "wait" };
    const predecessor =
      best?.clipId ?? (target.id === state.air ? target.source?.playing?.clipId : undefined);
    if (predecessor === undefined) return { _tag: "from", clipId: undefined };
    const from = airsBefore(item, predecessor, target);
    return target.source?.continuable.includes(from.clipId) === true
      ? { _tag: "from", ...from }
      : { _tag: "from", clipId: undefined };
  }
  /**
   * The clip that airs just before `item` if its continued build starts now:
   * `predecessor`, unless that build is projected Ready only after
   * `predecessor` ends. The Ready clips behind it then air first, and the item
   * continues from, and follows, the one that will be playing when it is Ready,
   * or the last of them. A clip still to be built is never chosen: H3 continues
   * only from a clip that finished generating.
   */
  function airsBefore(
    item: Item,
    predecessor: string,
    target: Session,
  ): { readonly clipId: string; readonly follows?: string | undefined } {
    const rate = continuedBuildRate(state.samples);
    if (rate === undefined) return { clipId: predecessor };
    const readyAt = now.mono + (rate * item.spec.seconds + lookaheadMarginSeconds) * 1000;
    const onAir = target.id === state.air ? target.source?.playing : undefined;
    const queued = readyOf(target)
      .filter(airs)
      .sort((a, b) => compareRank(rankClip(a), rankClip(b)));
    const after =
      onAir?.clipId === predecessor
        ? 0
        : queued.findIndex((clip) => clip.clipId === predecessor) + 1;
    if (after === 0 && onAir?.clipId !== predecessor) return { clipId: predecessor };
    let end =
      now.mono +
      (onAir === undefined ? 0 : playingRestMs(target)) +
      queued.slice(0, after).reduce((total, clip) => total + clip.seconds * 1000, 0);
    if (readyAt <= end) return { clipId: predecessor };
    const behind = queued.slice(after);
    for (const [index, clip] of behind.entries()) {
      end += clip.seconds * 1000;
      if (readyAt <= end || index === behind.length - 1)
        return { clipId: clip.clipId, follows: clip.clipId };
    }
    return { clipId: predecessor };
  }
  function wake(): number | undefined {
    const times: Array<number> = [];
    const later = (at: number | undefined) => {
      if (at !== undefined && at > now.mono && Number.isFinite(at)) times.push(at);
    };
    for (const item of items.values()) {
      if (item.phase === "Settled") continue;
      later(item.notBefore);
      later(item.startBy);
      later(item.retryAt);
      const at = atMono(item);
      later(at);
      if (
        at !== undefined &&
        item.spec.start._tag === "At" &&
        typeof item.spec.start.late === "object"
      )
        later(at + item.spec.start.late.skipAfterMs);
      if (item.unknownSince !== undefined) later(item.unknownSince + config.unknownTimeoutMs);
      if (item.phase === "Started")
        for (const [index, cue] of item.spec.cues.entries())
          if (!item.fired.includes(index)) later(cueAt(item, cue));
    }
    for (const value of state.sessions) {
      for (const entry of value.unknownFiller) later(entry.since + config.unknownTimeoutMs);
      later(value.openedAt + value.lifetimeMs - config.leadMs);
      later(value.openedAt + value.lifetimeMs);
      if (value.lastEndedAt !== undefined) later(value.lastEndedAt + config.graceMs);
    }
    later(state.openRetryAt);
    later(state.filler.retryAt);
    // The runway falls while a clip plays: wake when it will cross the filler floor, or a cut's margin.
    const onAirNow = session(state.air);
    if (onAirNow?.source?.playing !== undefined) {
      later(now.mono + Math.max(1, (runway() - fillerFloor()) * 1000));
      later(now.mono + Math.max(1, playingRestMs(onAirNow) - cutMarginMs));
      later(now.mono + playingRestMs(onAirNow) + 1);
    }
    return times.length === 0 ? undefined : Math.min(...times);
  }
});

/**
 * Equal clips that tile the gap to an anchor within the provider's lengths; else the shortest.
 * A tile that falls short costs a whole clip more, so each asks for no less than its share. A
 * provider that makes clips longer than asked covers the share anyway: H3 aligns a length up to
 * its frame grid, which airs 5 s as 5.167 s but 5.806 s as 5.875 s, so a ratio learned on one
 * length does not carry to another. Only one that makes them shorter is asked for more.
 */
const fillLength = (
  gapSeconds: number,
  lengths: { readonly min: number; readonly max: number },
  ratio: number,
): number => {
  if (!(gapSeconds > 0)) return lengths.min;
  const shortfall = Math.min(1, ratio);
  const pieces = Math.max(1, Math.ceil(gapSeconds / (lengths.max * shortfall)));
  return Math.min(lengths.max, Math.max(lengths.min, gapSeconds / pieces / shortfall));
};

/** The public view of the plan. */
export const view: {
  (state: State, now: Now): (config: Config) => PublicState;
  (config: Config, state: State, now: Now): PublicState;
} = dual(3, (config: Config, state: State, now: Now): PublicState => {
  const own = (clip: PlayingClip): ItemKey | "filler" | "other" =>
    clip.tag?._tag === "Item" ? clip.tag.key : clip.tag?._tag === "Filler" ? "filler" : "other";
  const air = state.sessions.find((value) => value.id === state.air);
  const playing = air?.playing;
  const target = preferredOf(state.sessions) ?? air;
  const rest = target?.id === state.air ? playingRestOf(air, now.mono) / 1000 : 0;
  return {
    accepting: state.accepting,
    runwaySeconds:
      rest + (target?.source?.ready ?? []).reduce((total, clip) => total + clip.seconds, 0),
    playing:
      playing === undefined
        ? null
        : { key: own(playing), startedAt: playing.wall, seconds: playing.seconds },
    lanes: config.lanes.map((lane, index) => ({
      name: lane.name,
      keys: [...state.items.values()]
        .filter((item) => item.spec.lane === index && item.phase !== "Settled")
        .sort((a, b) => a.order - b.order)
        .map((item) => item.spec.key),
    })),
    sessions: state.sessions.map((value) => ({
      sessionId: value.id,
      role: value.id === state.air ? "on-air" : value.retiring ? "retiring" : "replacement",
      ready: (value.source?.ready ?? []).map(own),
    })),
    starved: state.starved,
    estimates: estimatesOf(state.samples),
  };
});

/** Whether an as-run status settles an item's start, and its outcome. */
export const decides = (
  status: AsRunStatus,
): { readonly started: boolean; readonly outcome: boolean } => {
  switch (status._tag) {
    case "Started":
      return { started: true, outcome: false };
    case "Ended":
    case "Dropped":
    case "Failed":
    case "Unobserved":
      return { started: true, outcome: true };
    case "Unknown":
      return { started: status.terminal === true, outcome: status.terminal === true };
    default:
      return { started: false, outcome: false };
  }
};
