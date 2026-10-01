/**
 * The playout's pure policy: one state record and `step`, which applies one
 * input and returns the new state, what to do, and when to be woken if nothing
 * happens first. It holds no Effect, reads no clock and performs no I/O; the
 * runtime supplies monotonic and wall time with every input.
 *
 * The plan is the single source of truth. Each session only executes its window
 * of it: new work goes to the newest live session, Ready clips are ordered
 * within their own session and never ranked across two, and each session has
 * at most one provider command in flight, so a refusal is always attributable.
 * Sessions' commands run side by side; what must follow across sessions, a
 * switch or the removal of the retiring session's filler, waits on what the
 * plan has seen, never on another session's command.
 */
import { dual } from "effect/Function";
import type { Request } from "../../H3.js";
import type { CommandFailure } from "../../ReactorError.js";
import type {
  AsRunStatus,
  ClipTag,
  Event,
  FillContext,
  NotStarted,
  Placement,
  PlayingClip,
  Settled,
  SourceClip,
  SourceEvent,
  SourceState,
  State as PublicState,
  WithdrawOutcome,
} from "../../Playout.js";
import { requestSeconds } from "../h3/profile.js";
import { ItemKey } from "./errors.js";

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
  /** The clip it airs right after, or it is dropped as `displaced`. */
  readonly follows?: ClipTag;
}

/** `place`'s question, as the runtime normalized it. */
export interface Probe {
  readonly seconds: number;
  readonly continuity: boolean;
  /** How long until the caller submits the clip. */
  readonly submitInMs: number;
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
  | {
      readonly _tag: "OpenFailed";
      readonly reason: string;
      readonly fatal: boolean;
      /** It allocated a session, or may have: all but a refusal known to have allocated nothing. */
      readonly allocated: boolean;
      /** How long the refusal asked to wait before asking again. */
      readonly retryAfterMs?: number | undefined;
    }
  | { readonly _tag: "Source"; readonly sessionId: string; readonly event: SourceEvent }
  | { readonly _tag: "Lost"; readonly sessionId: string; readonly reason: string }
  | { readonly _tag: "Result"; readonly id: number; readonly result: CommandResult }
  | { readonly _tag: "Tick" }
  | { readonly _tag: "Close" }
  /** Where a clip like `probe` would land. */
  | { readonly _tag: "Place"; readonly id: number; readonly probe: Probe };

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
      /** The item whose work this command executes; absent for autonomous session work. */
      readonly key?: ItemKey | undefined;
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
  /** The answer to a `Place`. */
  | { readonly _tag: "Placed"; readonly id: number; readonly placement: Placement | null }
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
  /** The filler clip at `index` asked for a request outside H3's limits, which would be refused again. */
  | {
      readonly _tag: "Fail";
      readonly reason: string;
      readonly cause: "filler";
      readonly index: number;
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
        /** Where the filler clip at `index` asks for more than H3 takes, or undefined. */
        readonly invalid: (request: Request, index: number) => string | undefined;
        /**
         * What goes first when an item's build would outlast the air secured: a
         * filler clip covering it, or the item.
         */
        readonly protect: "air" | "order";
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
type DropReason = "late" | "withdrawn" | "replaced" | "displaced";

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
   * A filler clip went ahead of its build to cover it: it waits for no other,
   * though its enqueue goes again. Carried to another session, or taken off to
   * be built again later, it may be covered again.
   */
  readonly covered?: boolean | undefined;
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
   * A clip of its own taken off because it was Ready too early, and its
   * session: never adopted again as queued, nor taken off again for that,
   * though if it plays anyway, it aired.
   */
  readonly discarded?: { readonly sessionId: string; readonly clipId: string } | undefined;
  /** The provider's length for its clip, once it started. */
  readonly airSeconds?: number | undefined;
  /** The clip it `follows` has been on air while it waited. */
  readonly followsAired?: boolean | undefined;
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
  /**
   * The last autoplay change that failed, refused or uncertain, and when it is
   * asked again: a second later. Nothing but asking shows whether it applied,
   * and a provider that refused autoplay on would air nothing. Only that value
   * waits; a change the other way goes at once.
   */
  readonly autoplayRetry: { readonly enabled: boolean; readonly at: number } | undefined;
  /** A selected follower fences autoplay before its enqueue is sent. */
  readonly guardItem?: ItemKey | undefined;
  /** A failed explicit start is retried only after its queues change or a second passes. */
  readonly playRetry?: { readonly signature: string; readonly at: number } | undefined;
  readonly retiring: boolean;
  /**
   * The clip that last left the air here, and when on the monotonic clock. Its source may go on
   * naming it playing for a while: H3 answers an end with the facts it held before it until its
   * next state and queue reads agree.
   */
  readonly lastEnded: { readonly clipId: string; readonly at: number } | undefined;
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
  /** Filler clips whose removal was refused, asked again only once its queues have changed. */
  readonly refusedFiller: { readonly signature: string; readonly clipIds: ReadonlyArray<string> };
  /** Its last refused move, by its queues then and the clip: sent again only once they change. */
  readonly blockedMove: string | undefined;
  /** No move goes out before it: one whose command died is asked again a second later. */
  readonly moveRetryAt: number;
  /** The command in flight on its lane, which carries its commands one at a time. */
  readonly busy: { readonly id: number; readonly command: Command } | undefined;
  /** When its latest filler enqueue went out and the seconds it asked for, unless refused. */
  readonly fillerSent: { readonly at: number; readonly seconds: number } | undefined;
  /** Its filler clips by id, with the index each was asked for. */
  readonly fillers: ReadonlyMap<
    string,
    {
      readonly index: number;
      /** When its enqueue went out and the seconds it asked for, until its build is measured. */
      readonly build?: { readonly dispatchedAt: number; readonly seconds: number } | undefined;
    }
  >;
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
  /**
   * No open goes out before it: after a failed setup, a second for each in a
   * row up to `maxSetupFailures`, or a refusal's `Retry-After` if that is
   * longer.
   */
  readonly openRetryAt: number;
  /**
   * Failed setups ran out while a session held the air: nothing opens until
   * none does, and then one more open is tried, at `openRetryAt` at the
   * soonest.
   */
  readonly openingPaused: boolean;
  /** Failed setups in a row: each makes the next open wait a second longer, up to a limit. */
  readonly setupFailures: number;
  /**
   * Of those, the ones `maxSetupFailures` counts: all but refusals that
   * allocated nothing, and so billed nothing, refused while a session held the
   * air.
   */
  readonly countedFailures: number;
  /** Sessions content moderation ended. */
  readonly moderations: number;
  readonly nextCommand: number;
  readonly filler: {
    /** The next filler clip's index: each clip takes its own as its enqueue goes out. */
    readonly index: number;
    /**
     * Clips whose enqueue did not apply, or applied only on a session since lost: each is asked
     * for again as it was, first in index order, on whichever lane is free.
     */
    readonly retries: ReadonlyArray<{ readonly index: number; readonly request: Request }>;
    /** Clips moderation flagged: never asked for again. */
    readonly flagged: ReadonlyArray<number>;
    readonly retryAt: number;
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
   * clips and of clips continued from another; actual over requested length;
   * and the length recent clips aired at, by the length each asked for.
   */
  readonly samples: {
    readonly build: ReadonlyArray<number>;
    readonly continued: ReadonlyArray<number>;
    readonly length: ReadonlyArray<number>;
    readonly aired: ReadonlyArray<{ readonly requested: number; readonly seconds: number }>;
  };
  /**
   * The clip last cut, and its session. It is never cut again, whatever its
   * cut's result: H3's stop names no clip, and a stopped clip goes on looking
   * like it plays until its end is reported, so a second cut would stop the
   * clip after it.
   */
  readonly cut: { readonly sessionId: string; readonly clipId: string } | undefined;
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
  /** The clip that last started on air, on any session: an item admitted now may follow it. */
  readonly lastOnAir:
    | { readonly sessionId: string; readonly clipId: string; readonly tag: ClipTag | undefined }
    | undefined;
}

export interface Step {
  readonly state: State;
  readonly actions: ReadonlyArray<Action>;
  /**
   * Monotonic milliseconds at which a `Tick` is due, if nothing comes sooner. Before it
   * nothing falls due: a `Tick` then decides nothing and names the same wake.
   */
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
  openingPaused: false,
  setupFailures: 0,
  countedFailures: 0,
  moderations: 0,
  nextCommand: 1,
  filler: {
    index: 0,
    retries: [],
    flagged: [],
    retryAt: 0,
    refilling: false,
  },
  batches: [],
  joins: new Map(),
  drains: [],
  accepting: true,
  closed: false,
  samples: { build: [], continued: [], length: [], aired: [] },
  cut: undefined,
  cutting: undefined,
  starving: false,
  starved: 0,
  lastOnAir: undefined,
};

const retryDelayMs = 1_000;
/**
 * The wait for the next open after `consecutive` failed setups in a row: a second for each, but
 * no more than for `maxSetupFailures` of them, and at least one.
 */
const setupDelayMs = (config: Config, consecutive: number): number =>
  retryDelayMs * Math.min(consecutive, Math.max(1, config.maxSetupFailures));
const cutMarginMs = 1_000;
const exposureMarginMs = 1_500;
const lookaheadMarginSeconds = 1;
/**
 * How long before a boundary a clip must be Ready to air there ahead of the clips queued: H3 arms
 * its queue's head as a clip ends, so the plan must have seen the clip Ready and its move must
 * have landed. A state read and a move take 0.1 to 0.2 s on hosted H3, and a command already in
 * flight on the session holds the move up to 0.1 s more.
 */
const readinessMarginMs = 250;
/** A clip's end to the next one's start: hosted median 36 ms, n = 44. */
const seamMs = 40;
/** The key the clip `place` asks about goes by in a forward run; no item's. */
const probeKey = ItemKey.make("\u0000placement probe");
/**
 * Looks one step takes at most. None in the property's scripts decided anything past its
 * second look: the bound only keeps a fault from spinning the loop.
 */
const maxLooks = 16;
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

/** Records that a clip asking for `requested` seconds aired at `seconds`, newest last. */
const airedSample = (
  samples: State["samples"],
  requested: number,
  seconds: number,
): State["samples"] => ({
  ...samples,
  aired: [
    ...samples.aired.filter((sample) => Math.abs(sample.requested - requested) > 1e-6),
    { requested, seconds },
  ].slice(-maxSamples),
});

/**
 * The length a clip asking for `requested` seconds is projected to air at: that of the latest one
 * that asked for as much, since a provider's grid maps a length to one; else at the median ratio,
 * which a length on another grid step does not carry to (`fillLength`).
 */
const airedLengthOf = (samples: State["samples"], requested: number): number =>
  samples.aired.find((sample) => Math.abs(sample.requested - requested) <= 1e-6)?.seconds ??
  requested * estimatesOf(samples).length;

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
 * When `value`'s playing clip ends, counted from its observed start. Undefined for a clip of
 * unknown length or whose start was not seen: what is left of it does not fall with the time.
 */
const playingEndOf = (value: Session | undefined): number | undefined => {
  const playing = value?.source?.playing;
  const observed = value?.playing;
  if (playing?.seconds === undefined || observed?.clipId !== playing.clipId) return undefined;
  return observed.at + playing.seconds * 1000;
};

/**
 * What is left of `value`'s playing clip at `mono`, counted from its observed start. A clip
 * of unknown length may end at any moment, so nothing is counted for it and the plan builds ahead.
 * Nothing is left of a clip seen to end, though the source may still name it: counted whole
 * again, it would put what airs next a clip too late.
 */
const playingRestOf = (value: Session | undefined, mono: number): number => {
  const playing = value?.source?.playing;
  if (playing?.seconds === undefined || playing.clipId === value?.lastEnded?.clipId) return 0;
  const end = playingEndOf(value);
  return end === undefined ? playing.seconds * 1000 : Math.max(0, end - mono);
};

/** Whether `tag` is the clip `named`; a clip this playout did not enqueue is none. */
const sameClip = (named: ClipTag, tag: ClipTag | undefined): boolean => {
  if (tag === undefined) return false;
  switch (named._tag) {
    case "Item":
      return tag._tag === "Item" && tag.key === named.key;
    case "Filler":
      return tag._tag === "Filler" && tag.index === named.index;
  }
};
/**
 * Whether an item that `follows` a clip must still wait, not airing, for that clip to air. One
 * inserted right after the very item it follows is kept behind it by rank already, while that
 * item is Ready and airs when its turn comes (`ahead`); held, waiting on its batch or its enqueue,
 * it would not.
 */
const followHeld = (
  item: Item,
  aired: (item: Item) => boolean,
  ahead: (anchor: Item) => boolean,
  itemOf: (key: ItemKey) => Item | undefined,
): boolean => {
  const follows = item.spec.follows;
  if (follows === undefined || aired(item)) return false;
  const anchor =
    item.inserted &&
    item.anchor?.side === "after" &&
    follows._tag === "Item" &&
    follows.key === item.anchor.key
      ? itemOf(item.anchor.key)
      : undefined;
  return anchor === undefined || !ahead(anchor);
};
const followsAiredOf = (item: Item): boolean => item.followsAired === true;
/** When an `At` item due at `at` goes late: never at the next boundary, else at or after its time. */
const lateAt = (at: number, late: Late): number => {
  if (late === "nextBoundary") return Infinity;
  if (late === "drop") return at;
  return at + late.skipAfterMs;
};
/**
 * The start an insert takes from its anchor. A playing anchor's is spent, so the insert follows it
 * at the next boundary. An `At` anchor's lateness belongs to the anchor's content: past its time
 * an insert after it airs at the next boundary. One before it keeps it, since it airs no later.
 */
const insertStart = (anchor: Item, side: "before" | "after"): Spec["start"] => {
  if (anchor.phase === "Started") return { _tag: "Follow" };
  const start = anchor.spec.start;
  return start._tag === "At" && side === "after" ? { ...start, late: "nextBoundary" } : start;
};

/**
 * Whether an item's clip is queued to air in its turn: it isn't held, waiting on its batch or the
 * clip it follows, or withdrawn. An `At` item's time may still be to come.
 */
const queuedToAir = (items: ReadonlyMap<ItemKey, Item>, item: Item): boolean =>
  item.mode !== "held" &&
  item.batch === undefined &&
  item.withdraw === undefined &&
  !followHeldOf(items, item);
/** Whether an item's clip airs when its turn comes: it is queued to air, and its time has come. */
const airsItem = (items: ReadonlyMap<ItemKey, Item>, item: Item, now: Now): boolean => {
  const at =
    item.spec.start._tag === "At" ? now.mono + (item.spec.start.time - now.wall) : undefined;
  return (at === undefined || at <= now.mono) && queuedToAir(items, item);
};
/**
 * `followHeld` for the plan as it stands: an anchor is ahead while it is Ready and queued to air.
 * An insert takes its anchor's time, so the anchor's does not count.
 */
const followHeldOf = (items: ReadonlyMap<ItemKey, Item>, item: Item): boolean =>
  followHeld(
    item,
    followsAiredOf,
    (anchor) => anchor.phase === "Ready" && queuedToAir(items, anchor),
    (key) => items.get(key),
  );
/** Whether a Ready clip airs when its turn comes, as `airsItem`. A clip that is no item's airs. */
const airsOf = (items: ReadonlyMap<ItemKey, Item>, clip: SourceClip, now: Now): boolean => {
  const item = clip.tag?._tag === "Item" ? items.get(clip.tag.key) : undefined;
  return item === undefined || airsItem(items, item, now);
};

/**
 * Seconds of air secured: the playing clip's rest, then the Ready clips that
 * air from the session on air and from its replacement once that takes over.
 */
const securedOf = (state: Pick<State, "items" | "sessions" | "air">, now: Now): number => {
  const air = state.sessions.find((value) => value.id === state.air);
  const replacement = state.sessions.find((value) => value.id !== state.air && !value.retiring);
  const ready = (value: Session | undefined): number =>
    (value?.source?.ready ?? [])
      .filter((clip) => airsOf(state.items, clip, now))
      .reduce((total, clip) => total + clip.seconds, 0);
  return playingRestOf(air, now.mono) / 1000 + ready(air) + ready(replacement);
};

type Rank = readonly [number, number, number, number];
const compareRank = (a: Rank, b: Rank): number =>
  a[0] - b[0] || a[1] - b[1] || a[2] - b[2] || a[3] - b[3];

/** A group's answer from its parts': `withdrawn` if any was, else `already-started` if any started. */
const combined = (outcomes: ReadonlyArray<WithdrawOutcome>): WithdrawOutcome => {
  if (outcomes.includes("withdrawn")) return "withdrawn";
  return outcomes.includes("already-started") ? "already-started" : "not-found";
};

/** What a withdrawal finds of an item from its record: dropped, started, or neither. */
const fateOf = (item: Item): WithdrawOutcome => {
  if (item.status?._tag === "Dropped") return "withdrawn";
  const started =
    item.startedAt !== undefined || item.phase === "Started" || item.status?._tag === "Ended";
  return started ? "already-started" : "not-found";
};

/** A clip in a forward run of the plan. */
interface Projected {
  /** Undefined for a clip this playout did not enqueue. */
  readonly tag: ClipTag | undefined;
  /** The item it is for; the probed clip's is never put in the plan. */
  readonly item: Item | undefined;
  readonly sessionId: string;
  readonly clipId: string | undefined;
  /** How long it airs. */
  readonly seconds: number;
  /** Monotonic milliseconds when it is Ready; now for a clip Ready when the run starts. */
  readonly readyAt: number;
  /** A build still to finish when the run starts. */
  readonly projected: boolean;
  readonly continued: boolean;
}

/** The clip `place` asks about, submitted at `submitAt`. */
interface Hypothesis {
  readonly item: Item;
  readonly submitAt: number;
  readonly anchor: ItemKey | "next";
}

/** A build in flight when a forward run starts. */
interface Flight {
  readonly at: number;
  readonly seconds: number;
  readonly continued: boolean;
  readonly tag: ClipTag | undefined;
  readonly item: Item | undefined;
  readonly clipId: string | undefined;
}

/** What a forward run aired, from the clip on air, and what became of the clip `place` asks about. */
interface Run {
  readonly aired: ReadonlyArray<{ readonly clip: Projected; readonly start: number }>;
  readonly probe:
    | {
        readonly readyAt: number | undefined;
        readonly continued: boolean;
        /** Its place in `aired`, if it aired. */
        readonly index: number | undefined;
      }
    | undefined;
  /** Items the run dropped or displaced. */
  readonly gone: ReadonlySet<Item>;
}

/** `unmeasured` before three builds are measured; else whether what it rests on is projected. */
const basisOf = (unmeasured: boolean, projected: boolean): Placement["basis"] => {
  if (unmeasured) return "unmeasured";
  return projected ? "projected" : "ready";
};

/** One look at the plan: applies the input, then decides what is due at `now`. */
const decide = (config: Config, previous: State, input: Input, now: Now): Step => {
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
    actions.push({
      _tag: "Withdrawn",
      id: waiter.id,
      index: waiter.index,
      outcome: combined(outcomes),
    });
  };
  /** Settles an item for good, resolving withdrawals that wait on it and recording history. */
  const settle = (key: ItemKey, status: AsRunStatus): void => {
    const item = items.get(key);
    if (item === undefined || item.phase === "Settled") return;
    // The as-run goes out first, so a withdrawal's caller finds it already published.
    set(key, { phase: "Settled", waiting: [], withdraw: undefined });
    asRun(key, status);
    const outcome = fateOf({ ...item, status });
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
  /** Whether a session still holds the air: it takes new work, or has clips of its own to air. */
  const holding = (value: Session | undefined): boolean =>
    value !== undefined &&
    (!value.indeterminate || value.source?.playing !== undefined || readyOf(value).length > 0);
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
    if (
      item.mode === "held" ||
      (at !== undefined && now.mono < at) ||
      item.batch !== undefined ||
      heldForFollows(item)
    )
      return [lanes + 1, 1, item.order, item.generation];
    return airRank(item);
  };
  const heldForFollows = (item: Item): boolean => followHeldOf(items, item);
  const cuts = (item: Item): boolean => config.lanes[item.spec.lane]?.cut === true;
  /** Why a clip `key` would follow could never air before it, or undefined. */
  const followsIssue = (key: ItemKey, follows: ClipTag | undefined): string | undefined => {
    if (follows === undefined) return undefined;
    if (follows._tag === "Item") {
      if (follows.key === key) return "an item cannot follow itself";
      if (groups.has(follows.key)) return "a group key names no clip: follow one of its parts";
      return undefined;
    }
    if (!Number.isSafeInteger(follows.index) || follows.index < 0)
      return "a filler clip's index is a whole number from 0";
    if (config.filler === undefined) return "there is no filler to follow";
    return undefined;
  };
  /** `rankItem` once nothing holds the item back. */
  const airRank = (item: Item): Rank => {
    // Its build continues the Ready clip it follows on its session, so it airs right behind that
    // clip until that clip starts. An item accepted again for a rebuild follows nothing yet.
    const followed =
      item.follows === undefined || item.phase === "Accepted"
        ? undefined
        : session(item.sessionId)?.source?.ready.find((clip) => clip.clipId === item.follows);
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
  const airs = (clip: SourceClip): boolean => airsOf(items, clip, now);
  /**
   * The runway as the time passes: at `time` it is `(Math.max(end, time) - time) / 1000 +
   * seconds`. It falls while the clip on air plays, to that clip's end, and holds still with
   * none, or with one whose length or start is unknown, counted as `playingRestOf` counts it.
   */
  const runwayTerms = (): { readonly end: number; readonly seconds: number } => {
    const target = preferred() ?? session(state.air);
    if (target === undefined) return { end: -Infinity, seconds: 0 };
    const onAir = target.id === state.air;
    const end = onAir ? playingEndOf(target) : undefined;
    const ready = readyOf(target)
      .filter(airs)
      .reduce((total, clip) => total + clip.seconds, 0);
    return end === undefined
      ? { end: -Infinity, seconds: (onAir ? playingRestMs(target) / 1000 : 0) + ready }
      : { end, seconds: ready };
  };
  /** Seconds of air secured on the session that takes new work. */
  const runway = (): number => {
    const { end, seconds } = runwayTerms();
    return (Math.max(end, now.mono) - now.mono) / 1000 + seconds;
  };
  /** When the runway falls to `seconds` as the clip on air plays; undefined if it doesn't. */
  const fallsTo = (seconds: number): number | undefined => {
    const { end, seconds: after } = runwayTerms();
    return Number.isFinite(end) && seconds > after ? end - (seconds - after) * 1000 : undefined;
  };
  /** Whether the runway is below `seconds`: it is from the instant it falls there. */
  const below = (seconds: number): boolean =>
    runway() < seconds || now.mono >= (fallsTo(seconds) ?? Infinity);
  /**
   * The earliest a clip in `item`'s place could start, optimistically, as the time passes: at
   * `time` it is `Math.max(from, time + after)`. The end of the clip on air and the builds in
   * flight fix `from`; the air ahead and the builds it waits for add `after` to the time, as
   * they do once that clip is over.
   */
  const projection = (item: Item): { readonly from: number; readonly after: number } => {
    const target = preferred() ?? session(state.air);
    const rank = rankItem(item);
    const aheadMs = (target === undefined ? [] : readyOf(target))
      .filter(
        (clip) => compareRank(rankClip(clip), rank) < 0 && itemOf(clip)?.withdraw === undefined,
      )
      .reduce((total, clip) => total + clip.seconds * 1000, 0);
    const air = session(state.air);
    const end = playingEndOf(air);
    const playable = {
      from: (end ?? -Infinity) + aheadMs,
      after: (end === undefined ? playingRestMs(air) : 0) + aheadMs,
    };
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
      ...(target?.fillerSent === undefined
        ? []
        : [target.fillerSent.at + buildMs(target.fillerSent.seconds)]),
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
    const waits = first + buildMs(item.spec.seconds);
    return {
      from: Math.max(playable.from, Math.max(...inFlight) + waits),
      after: Math.max(playable.after, waits),
    };
  };
  /**
   * Whether a clip in `item`'s place could not start before `startBy`, as projected. Once
   * nothing holds it, the projection grows with the time: it misses from `startBy - after` on.
   */
  const misses = (item: Item, startBy: number): boolean => {
    const { from, after } = projection(item);
    return Math.max(from, now.mono + after) >= startBy || now.mono >= startBy - after;
  };
  /** Whether `item` is firm, not sent, and dropped once projected to miss its `startBy`. */
  const lateWhenProjected = (item: Item): item is Item & { readonly startBy: number } =>
    item.phase === "Accepted" &&
    item.spec.window?.firm === true &&
    item.startBy !== undefined &&
    item.dispatchedAt === undefined &&
    estimates().build !== undefined;

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
      // An insert already admitted under this key, spec and anchor is answered as it is.
      if (edit._tag === "Insert" && !items.has(edit.spec.key)) {
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
      if (edit._tag === "Submit" || edit._tag === "Insert") {
        const issue = followsIssue(edit.spec.key, edit.spec.follows);
        if (issue !== undefined)
          return refuse({ _tag: "InvalidItem", key: edit.spec.key, message: issue });
        // A cut airs when its clip is Ready, not at a boundary, so it can't follow a clip.
        const lane =
          edit._tag === "Submit"
            ? edit.spec.lane
            : (items.get(edit.anchor) ?? firstOrLastPart(edit.anchor, edit.side))?.spec.lane;
        if (
          edit.spec.follows !== undefined &&
          lane !== undefined &&
          config.lanes[lane]?.cut === true
        )
          return refuse({
            _tag: "InvalidItem",
            key: edit.spec.key,
            message: "an item in a cutting lane cannot follow a clip",
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
    // An edit refused below, as an item it adds would miss its deadline, leaves the plan as it was.
    const saved = { items: new Map(items), groups: new Map(groups), nextOrder: state.nextOrder };
    const results: Array<EditReply> = [];
    const adds: Array<ItemKey> = [];
    const targets: Array<Batch["targets"][number]> = [];
    /** Group-key withdrawals by position, and how many parts each waits on. */
    const joined: Array<{ readonly index: number; readonly parts: number }> = [];
    /** Withdrawals of keys the plan does not know, answered once the edit is accepted. */
    const notFound: Array<number> = [];
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
            const playing = anchor.phase === "Started";
            put(
              newItem(
                { ...edit.spec, lane: anchor.spec.lane, start: insertStart(anchor, edit.side) },
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
                {
                  ...edit.spec,
                  lane: old.spec.lane,
                  start: old.spec.start,
                  ...(old.spec.follows === undefined ? {} : { follows: old.spec.follows }),
                },
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
          if (!known) notFound.push(index);
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
        misses(item, item.startBy)
      ) {
        items.clear();
        for (const [other, value] of saved.items) items.set(other, value);
        groups.clear();
        for (const [group, value] of saved.groups) groups.set(group, value);
        state = { ...state, nextOrder: saved.nextOrder };
        return refuse({ _tag: "WouldMissDeadline", key });
      }
    }
    for (const index of notFound)
      actions.push({ _tag: "Withdrawn", id, index, outcome: "not-found" });
    if (joined.length > 0) {
      const joins = new Map(state.joins);
      for (const join of joined)
        joins.set(`${String(id)}:${String(join.index)}`, { left: join.parts, outcomes: [] });
      state = { ...state, joins };
    }
    const pending = batched && (adds.length > 0 || targets.length > 0);
    for (const key of adds) {
      set(key, { batch: pending && batched ? id : undefined });
      // A clip it follows that was the last on air has aired; one that aired before that is gone,
      // and the sweep drops the item at once.
      const follows = items.get(key)?.spec.follows;
      if (follows !== undefined)
        set(key, { followsAired: sameClip(follows, state.lastOnAir?.tag) });
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
    // A session's first clip on air ends a run of sessions that failed to set up or to play
    // anything. A later clip on a session that already aired says nothing of those.
    if (session(sessionId)?.startedAny === false)
      state = { ...state, setupFailures: 0, countedFailures: 0 };
    updateSession(sessionId, {
      startedAny: true,
      playing: { ...clip, at: now.mono, wall: now.wall },
    });
    onAirChanged(sessionId, clip);
    if (clip.tag?._tag === "Filler")
      emit({
        _tag: "Filler",
        index: clip.tag.index,
        phase: "Started",
        at: now.wall,
        seconds: clip.seconds,
      });
  };
  /**
   * Records the clip now on air. The clip an item follows starting has aired; once it has, any
   * other clip starting before the item displaces it.
   */
  const onAirChanged = (sessionId: string, clip: PlayingClip): void => {
    state = { ...state, lastOnAir: { sessionId, clipId: clip.clipId, tag: clip.tag } };
    for (const item of [...items.values()]) {
      const follows = item.spec.follows;
      if (follows === undefined || !live(item) || item.phase === "Started") continue;
      if (item.withdraw !== undefined) continue;
      if (clip.tag?._tag === "Item" && clip.tag.key === item.spec.key) continue;
      if (sameClip(follows, clip.tag)) set(item.spec.key, { followsAired: true });
      else if (item.followsAired === true) withdraw(item.spec.key, "displaced");
    }
  };
  const started = (sessionId: string, clip: PlayingClip): void => {
    nowPlaying(sessionId, clip);
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
  const forgetFiller = (sessionId: string, clipId: string): void =>
    updateSession(sessionId, {
      fillers: new Map([...(session(sessionId)?.fillers ?? [])].filter(([id]) => id !== clipId)),
    });
  /**
   * A follower not Ready as the clip it follows leaves the air has missed the place it was written
   * for. Keeping autoplay fenced lets its removal finish even when its build completes during the
   * command.
   */
  const missed = (tag: ClipTag | undefined): void => {
    for (const item of [...items.values()])
      if (
        item.spec.follows !== undefined &&
        sameClip(item.spec.follows, tag) &&
        live(item) &&
        item.phase !== "Ready" &&
        item.phase !== "Started" &&
        item.withdraw === undefined
      )
        withdraw(item.spec.key, "displaced");
  };
  const ended = (sessionId: string, event: Extract<SourceEvent, { _tag: "Ended" }>): void => {
    const { clip } = event;
    missed(clip.tag);
    updateSession(sessionId, {
      lastEnded: { clipId: clip.clipId, at: now.mono },
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
      return forgetFiller(sessionId, clip.clipId);
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
        ...airedSample(state.samples, item.spec.seconds, clip.seconds),
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
    if (suspect?._tag === "Filler")
      state = {
        ...state,
        filler: {
          ...state.filler,
          retries: state.filler.retries.filter((retry) => retry.index !== suspect.index),
          flagged: [...state.filler.flagged, suspect.index],
        },
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
  const failed = (
    sessionId: string,
    event: Extract<SourceEvent, { readonly _tag: "Failed" }>,
  ): void => {
    const { clip } = event;
    // A clip that fails on air leaves it as an ended one does, and the switch's grace counts from now.
    const onAir = session(sessionId)?.playing?.clipId === clip.clipId;
    if (onAir) {
      missed(clip.tag);
      updateSession(sessionId, {
        lastEnded: { clipId: clip.clipId, at: now.mono },
        playing: undefined,
      });
    }
    if (clip.tag?._tag === "Filler") {
      if (onAir)
        emit({
          _tag: "Filler",
          index: clip.tag.index,
          phase: "Ended",
          at: now.wall,
          seconds: clip.seconds,
        });
      return forgetFiller(sessionId, clip.clipId);
    }
    const item = itemOf(clip);
    if (item !== undefined)
      settle(item.spec.key, {
        _tag: "Failed",
        reason: { _tag: "Clip", message: event.message, provider: event.provider },
      });
  };
  /** Reads a session's queues back into the plan: adoption by key, Ready, and clips that vanished. */
  const observe = (sessionId: string, source: SourceState): void => {
    report(sessionId, source);
    // What plays is what the provider reports while its report is current; a clip seen playing
    // without its start event started no later than now. H3 answers a start or an end with the
    // facts it held before it, and says so: they would name the clip that played before.
    if (source.available) {
      if (source.playing === undefined) updateSession(sessionId, { playing: undefined });
      else nowPlaying(sessionId, source.playing);
    }
    const listed = new Map<string, "Building" | "Ready" | "Playing">();
    for (const clip of source.building) listed.set(clip.clipId, "Building");
    for (const clip of source.ready) listed.set(clip.clipId, "Ready");
    if (source.playing !== undefined) listed.set(source.playing.clipId, "Playing");
    const fillers = new Map(session(sessionId)?.fillers);
    const sample = (kind: "build" | "continued", dispatchedAt: number, seconds: number): void => {
      const value = (now.mono - dispatchedAt) / 1000 / seconds;
      state = {
        ...state,
        samples: { ...state.samples, [kind]: [...state.samples[kind], value].slice(-maxSamples) },
      };
    };
    for (const clip of [
      ...source.building,
      ...source.ready,
      ...(source.playing === undefined ? [] : [source.playing]),
    ]) {
      if (clip.tag?._tag === "Filler") {
        // A filler build is measured as an item's is, so a channel airing only filler learns its
        // build rate, and the floor covers a build.
        const build = fillers.get(clip.clipId)?.build;
        const where = listed.get(clip.clipId);
        if (build !== undefined && where === "Ready") {
          sample("build", build.dispatchedAt, build.seconds);
          if (clip.seconds !== undefined)
            state = { ...state, samples: airedSample(state.samples, build.seconds, clip.seconds) };
        }
        fillers.set(clip.clipId, {
          index: clip.tag.index,
          build: where === "Building" ? build : undefined,
        });
      }
      if (clip.tag?._tag !== "Item") continue;
      const item = items.get(clip.tag.key);
      if (item === undefined || item.phase === "Settled") continue;
      const where = listed.get(clip.clipId)!;
      if (
        where !== "Playing" &&
        item.discarded?.sessionId === sessionId &&
        item.discarded.clipId === clip.clipId
      )
        continue;
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
        // A continued build takes longer, so it is measured apart from independent ones.
        if (item.dispatchedAt !== undefined && !item.everUnknown)
          sample(
            item.continued === true ? "continued" : "build",
            item.dispatchedAt,
            item.spec.seconds,
          );
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
    const listedFiller = new Set([...fillers.values()].map((owner) => owner.index));
    const uncertain = session(sessionId)?.unknownFiller ?? [];
    if (uncertain.some((entry) => listedFiller.has(entry.index)))
      updateSession(sessionId, {
        unknownFiller: uncertain.filter((entry) => !listedFiller.has(entry.index)),
      });
    // A filler the provider no longer lists has aired or failed; either way it stops being ours.
    for (const clipId of fillers.keys()) if (!listed.has(clipId)) fillers.delete(clipId);
    updateSession(sessionId, { fillers });
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
   * After a failed setup that counts, once `maxSetupFailures` do: with no
   * session holding the air, the playout fails. While `air` holds it, opening
   * pauses until it no longer does, when one more open is tried once the last
   * failure's wait is over.
   */
  const pauseOrFail = (reason: string, cause: "open" | "lost", air: Session | undefined): void => {
    if (state.countedFailures < config.maxSetupFailures) return;
    if (holding(air)) state = { ...state, openingPaused: true };
    else actions.push({ _tag: "Fail", reason, cause });
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
    // Lost unplanned, the clip on air leaves the air with its session. What follows that clip starts
    // right after it only if Ready on another session: carried, it would air seconds later.
    const left =
      !planned && state.lastOnAir?.sessionId === sessionId ? state.lastOnAir.tag : undefined;
    let carried = 0;
    for (const item of items.values()) {
      if (item.sessionId !== sessionId || item.phase === "Settled") continue;
      if (item.phase === "Started")
        settle(item.spec.key, { _tag: "Failed", reason: { _tag: "Lost", sessionId } });
      else if (item.phase === "Unknown") settle(item.spec.key, { _tag: "Unknown", terminal: true });
      else if (item.withdraw !== undefined)
        settle(item.spec.key, { _tag: "Dropped", reason: item.withdraw });
      else if (item.spec.follows !== undefined && sameClip(item.spec.follows, left))
        settle(item.spec.key, { _tag: "Dropped", reason: "displaced" });
      else if (!planned && item.phase === "Building" && item.unbuiltLosses + 1 >= 2)
        settle(item.spec.key, { _tag: "Failed", reason: { _tag: "Lost", sessionId } });
      else {
        carried++;
        set(item.spec.key, {
          phase: "Accepted",
          clipId: undefined,
          sessionId: undefined,
          dispatchedAt: undefined,
          covered: undefined,
          ...(!planned && item.phase === "Building"
            ? { unbuiltLosses: item.unbuiltLosses + 1 }
            : {}),
        });
        asRun(item.spec.key, { _tag: "Accepted", carried: { sessionId } });
      }
    }
    missed(left);
    // A filler enqueue in flight there made no clip that can still air: it is asked for again.
    const lostCommand = lost.busy?.command;
    if (lostCommand?._tag === "Enqueue" && lostCommand.tag._tag === "Filler")
      retryFiller(lostCommand.tag.index, lostCommand.request);
    if (!planned && !lost.startedAny && lost.lastEnqueue !== undefined) {
      const consecutive = state.setupFailures + 1;
      state = {
        ...state,
        setupFailures: consecutive,
        countedFailures: state.countedFailures + 1,
        // As after a failed open, the next waits a second longer, and still for a Retry-After.
        openRetryAt: Math.max(state.openRetryAt, now.mono + setupDelayMs(config, consecutive)),
      };
      emit({
        _tag: "Session",
        event: {
          _tag: "SetupFailed",
          reason: `lost before a clip sent to it started: ${reason}`,
          consecutive,
        },
      });
      pauseOrFail(reason, "lost", state.air === sessionId ? undefined : session(state.air));
    }
    state = {
      ...state,
      sessions: state.sessions.filter((value) => value.id !== sessionId),
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
    // The lane it came back on. A result from a session already lost comes too late: the loss
    // settled or carried what its command was for.
    const lane = state.sessions.find((value) => value.busy?.id === id);
    const command = lane?.busy?.command;
    if (lane === undefined || command === undefined) return;
    const sessionId = lane.id;
    updateSession(sessionId, { busy: undefined });
    switch (command._tag) {
      case "Enqueue": {
        if (command.tag._tag === "Filler") {
          const index = command.tag.index;
          if (result._tag === "Done") {
            const clipId = result.clipId;
            // H3 may list the clip before it replies: one already listed built is not measured.
            const reported = session(sessionId)?.source;
            const built =
              reported !== undefined &&
              (reported.playing?.clipId === clipId ||
                reported.ready.some((clip) => clip.clipId === clipId));
            const sent = lane.fillerSent;
            const build =
              sent === undefined || built
                ? undefined
                : { dispatchedAt: sent.at, seconds: sent.seconds };
            if (clipId !== undefined)
              updateSession(sessionId, {
                fillers: new Map([...lane.fillers, [clipId, { index, build }]]),
              });
          } else if (uncertain(result))
            // Its clip may be building on that session: it holds that session's build slot until
            // a queue read shows it, the deadline passes or the session goes. It is never asked
            // for again, since it may be there.
            updateSession(sessionId, {
              unknownFiller: [
                ...(session(sessionId)?.unknownFiller ?? []),
                { index, since: now.mono },
              ],
            });
          else {
            retryFiller(index, command.request);
            state = { ...state, filler: { ...state.filler, retryAt: now.mono + retryDelayMs } };
            updateSession(sessionId, { fillerSent: undefined });
          }
          return;
        }
        const item = items.get(command.tag.key);
        if (item === undefined || item.phase === "Settled") return;
        const owner = session(sessionId);
        if (result._tag === "Done") {
          set(item.spec.key, { unsent: undefined });
          if (item.clipId === undefined) set(item.spec.key, { clipId: result.clipId, sessionId });
          if (item.phase === "Unknown")
            set(item.spec.key, { phase: "Building", unknownSince: undefined });
          const now_ = items.get(item.spec.key)!;
          if (now_.phase === "Building" && now_.status?._tag !== "Building")
            asRun(item.spec.key, { _tag: "Building", sessionId });
        } else if (uncertain(result)) {
          if (item.clipId === undefined) {
            set(item.spec.key, {
              phase: "Unknown",
              sessionId,
              everUnknown: true,
              unknownSince: now.mono,
            });
            asRun(item.spec.key, { _tag: "Unknown" });
          }
        } else if (item.withdraw !== undefined)
          // Refused with no clip made: the withdrawal waiting on it has nothing left to remove.
          settle(item.spec.key, { _tag: "Dropped", reason: item.withdraw });
        else if (result._tag === "Failed" && result.cause.isRetryable)
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
          !(item.unsent?.sessionId === sessionId && owner?.source?.available === true)
        )
          set(item.spec.key, {
            phase: "Accepted",
            sessionId: undefined,
            dispatchedAt: undefined,
            unsent: { sessionId, changes: owner?.changes ?? 0 },
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
          (item) =>
            item.sessionId === sessionId &&
            item.clipId === command.clipId &&
            item.phase !== "Settled",
        );
        // A removal whose outcome is unknown goes again at once: if it applied, the next is
        // refused, and the clip is gone. One refused goes again once the queues change.
        const unknown = result._tag === "Failed" && result.cause.context.outcome === "unknown";
        if (owner === undefined) {
          if (result._tag === "Done") return forgetFiller(sessionId, command.clipId);
          if (unknown) return;
          const refused = session(sessionId);
          if (refused === undefined) return;
          const now_ = signature(refused);
          const earlier =
            refused.refusedFiller.signature === now_ ? refused.refusedFiller.clipIds : [];
          updateSession(sessionId, {
            refusedFiller: { signature: now_, clipIds: [...earlier, command.clipId] },
          });
          return;
        }
        if (result._tag === "Done")
          settle(owner.spec.key, { _tag: "Dropped", reason: owner.withdraw ?? "withdrawn" });
        else if (!unknown) set(owner.spec.key, { blockedRemove: signature(session(sessionId)) });
        return;
      }
      case "Move":
        if (result._tag === "Failed")
          updateSession(sessionId, { blockedMove: signature(session(sessionId)) + command.clipId });
        // One that died may have applied: a read of the queues will show if it did.
        else if (result._tag === "Died")
          updateSession(sessionId, { moveRetryAt: now.mono + retryDelayMs });
        return;
      case "Autoplay":
        if (result._tag === "Done") return updateSession(sessionId, { autoplay: command.enabled });
        updateSession(sessionId, {
          // One that may have applied leaves autoplay unknown, so the value wanted goes again.
          ...(uncertain(result) ? { autoplay: undefined } : {}),
          autoplayRetry: { enabled: command.enabled, at: now.mono + retryDelayMs },
        });
        if (!command.enabled) cutFailed(sessionId);
        return;
      // Its clip was marked cut when the cut began; no result makes it cuttable again.
      // One whose fiber died may not have been sent, so it fails the cut like a refusal.
      case "Stop":
        if (result._tag !== "Done") return cutFailed(sessionId);
        if (state.cutting?.sessionId === sessionId && state.cutting.clipId === command.clipId)
          state = { ...state, cutting: { ...state.cutting, stage: "stopped" } };
        return;
      case "Play":
        if (result._tag !== "Done") {
          if (state.cutting?.sessionId !== sessionId)
            updateSession(sessionId, {
              playRetry: { signature: signature(session(sessionId)), at: now.mono + retryDelayMs },
            });
          return cutFailed(sessionId);
        }
        updateSession(sessionId, { playRetry: undefined });
        if (state.cutting?.sessionId === sessionId && state.cutting.next === command.clipId)
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
            autoplayRetry: undefined,
            retiring: false,
            lastEnded: undefined,
            startedAny: false,
            indeterminate: false,
            unknownFiller: [],
            playing: undefined,
            lastEnqueue: undefined,
            refusedFiller: { signature: "", clipIds: [] },
            blockedMove: undefined,
            moveRetryAt: 0,
            busy: undefined,
            fillerSent: undefined,
            fillers: new Map(),
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
          ...(Number.isFinite(input.lifetimeMs)
            ? { lifetimeSeconds: input.lifetimeMs / 1000 }
            : {}),
        },
      });
      break;
    }
    case "OpenFailed": {
      const consecutive = state.setupFailures + 1;
      // A refusal that allocated nothing billed nothing: refused while a session holds the air, it
      // counts toward no limit, then or later, though the next open waits longer.
      const counted = input.allocated || !holding(session(state.air));
      state = {
        ...state,
        opening: false,
        setupFailures: consecutive,
        countedFailures: state.countedFailures + (counted ? 1 : 0),
        // A refusal that says when to ask again is not asked sooner.
        openRetryAt:
          now.mono + Math.max(setupDelayMs(config, consecutive), input.retryAfterMs ?? 0),
      };
      emit({ _tag: "Session", event: { _tag: "SetupFailed", reason: input.reason, consecutive } });
      if (input.fatal) actions.push({ _tag: "Fail", reason: input.reason, cause: "open" });
      // One that doesn't count is asked again after its delay, even once those that do ran out.
      else if (counted) pauseOrFail(input.reason, "open", session(state.air));
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
          failed(input.sessionId, event);
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
    // Answered at the end of the step, from the plan the step leaves.
    case "Place":
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
  if (state.closed) {
    if (input._tag === "Place") actions.push({ _tag: "Placed", id: input.id, placement: null });
    return { state: { ...state, items }, actions, wake: undefined };
  }

  // Sweep every waiting item, not only the heads: expiry must not strand behind a live one. Each
  // is late from the instant its deadline falls due, where the wake is.
  const boundaryLate = (item: Item): boolean => {
    const at = atMono(item);
    if (at === undefined || item.spec.start._tag !== "At") return false;
    return now.mono >= lateAt(at, item.spec.start.late);
  };
  for (const item of [...items.values()]) {
    if (item.phase !== "Accepted" && item.phase !== "Building" && item.phase !== "Ready") continue;
    if (
      (item.spec.window?.firm === true && item.startBy !== undefined && now.mono >= item.startBy) ||
      boundaryLate(item)
    )
      withdraw(item.spec.key, "late");
    else if (lateWhenProjected(item) && misses(item, item.startBy)) withdraw(item.spec.key, "late");
  }
  // An item that follows a clip goes once that clip can no longer air right before it, or, not
  // sent yet, once it is projected unable to be Ready by that clip's end.
  for (const item of [...items.values()]) {
    if (item.spec.follows === undefined || !live(item) || item.phase === "Started") continue;
    if (item.withdraw !== undefined) continue;
    if (followsGone(item) || (projectedLate(item)?.due ?? Infinity) <= now.mono)
      withdraw(item.spec.key, "displaced");
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
    since !== undefined && now.mono >= since + config.unknownTimeoutMs;
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
  const expiresAt = (value: Session) => value.openedAt + value.lifetimeMs;
  for (const value of state.sessions)
    if (now.mono >= expiresAt(value) && value.lifetimeMs !== Infinity)
      lose(value.id, "the session's granted length ended");
  // Setups ran out while a session held the air; once none does, one more open is tried, once
  // the last failure's wait is over.
  if (state.openingPaused && !holding(session(state.air)))
    state = { ...state, openingPaused: false };
  const air = session(state.air);
  const wantsAir =
    state.accepting ||
    state.drains.some((drain) => drain.finish === "accepted") ||
    [...items.values()].some(live);
  if (!state.opening && !state.openingPaused && now.mono >= state.openRetryAt && wantsAir) {
    const replacementLive = state.sessions.some(
      (value) => !value.retiring && value.id !== state.air,
    );
    // Only the lead opens a replacement. What doesn't fit before the cap waits for it: the
    // session on air then holds air nearly to its cap, and a replacement opened earlier would
    // bill while it waits to air.
    const due =
      air === undefined ||
      (!replacementLive && (air.indeterminate || now.mono >= expiresAt(air) - config.leadMs));
    if (due && state.sessions.length < 2) {
      state = { ...state, opening: true };
      actions.push({ _tag: "Open" });
    }
  }
  const current = session(state.air);
  const next = state.sessions.find((value) => value.id !== state.air && !value.retiring);
  if (current !== undefined && next !== undefined && next.source?.available === true) {
    // The drain rule: once the replacement has an item Ready, the retiring filler goes.
    if (readyOf(next).some((clip) => clip.tag?._tag === "Item"))
      for (const clip of readyOf(current))
        if (fillerRemovable(current, clip))
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
      current.lastEnded !== undefined && now.mono >= current.lastEnded.at + config.graceMs;
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

  // Each session's commands go one at a time on a lane of its own.
  endCut();
  for (const value of state.sessions) decideCommand(value.id);
  if (input._tag === "Place")
    actions.push({ _tag: "Placed", id: input.id, placement: place(input.probe) });

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

  function queueCommand(sessionId: string, command: Command, key?: ItemKey): void {
    const lane = session(sessionId);
    if (lane === undefined || lane.busy !== undefined) return;
    const id = state.nextCommand;
    state = { ...state, nextCommand: id + 1 };
    updateSession(sessionId, {
      busy: { id, command },
      ...(command._tag === "Enqueue" ? { lastEnqueue: command.tag } : {}),
    });
    actions.push({
      _tag: "Command",
      id,
      sessionId,
      command,
      ...(key === undefined ? {} : { key }),
    });
  }
  /** A filler clip a session takes commands for, unless its removal was refused as things stand. */
  function fillerRemovable(value: Session, clip: SourceClip): boolean {
    return (
      clip.tag?._tag === "Filler" &&
      value.source?.available === true &&
      !(
        value.refusedFiller.signature === signature(value) &&
        value.refusedFiller.clipIds.includes(clip.clipId)
      )
    );
  }
  /** A session's queues as they stand: a clip finishing its build changes them too. */
  function signature(value: Session | undefined): string {
    const source = value?.source;
    return source === undefined
      ? ""
      : [source.building, source.ready, source.playing === undefined ? [] : [source.playing]]
          .map((clips) => clips.map((clip) => clip.clipId).join(","))
          .join(":");
  }
  function cueAt(item: Item, cue: Spec["cues"][number]): number {
    return cue.from === "start"
      ? item.startedAt! + cue.offsetMs
      : item.startedAt! + (item.airSeconds ?? item.spec.seconds) * 1000 - cue.offsetMs;
  }
  /** The cut's cutter while it waits Ready on its session, and whether the plan still wants it. */
  function cutterOf(): { readonly cutter: SourceClip | undefined; readonly wanted: boolean } {
    const cutting = state.cutting;
    const cutter = session(cutting?.sessionId)?.source?.ready.find(
      (clip) => clip.clipId === cutting?.next,
    );
    return {
      cutter,
      wanted: cutter !== undefined && airs(cutter) && itemOf(cutter)?.phase === "Ready",
    };
  }
  /**
   * A cut ends once its cutter has left its session's Ready queue; once a step
   * failed with the cutter still wanted, which then airs at the next boundary;
   * or once a cutter no longer wanted is not being removed.
   */
  function endCut(): void {
    const cutting = state.cutting;
    if (cutting === undefined) return;
    const { cutter, wanted } = cutterOf();
    const cutterItem = itemOf(cutter);
    const removing =
      cutterItem?.withdraw !== undefined &&
      cutterItem.blockedRemove !== signature(session(cutting.sessionId));
    if (cutter === undefined || (cutting.stage === "failed" && wanted) || (!wanted && !removing))
      state = { ...state, cutting: undefined };
  }
  /** The next command for a session whose lane is free, if the plan wants one there. */
  function decideCommand(sessionId: string): void {
    const value = session(sessionId);
    if (value === undefined || value.busy !== undefined) return;
    const actual = readyOf(value);
    const desired = [...actual].sort((a, b) => compareRank(rankClip(a), rankClip(b)));
    // Autoplay as its role wants it: off on a replacement until it takes the air, and off on the
    // air while a cut is under way.
    const selected = value.guardItem === undefined ? undefined : items.get(value.guardItem);
    if (
      value.guardItem !== undefined &&
      (selected?.phase !== "Accepted" || selected.spec.follows === undefined)
    )
      updateSession(value.id, { guardItem: undefined });
    // The clip on air here, or with none, the one that left it last: a follower Ready right behind
    // it is next by every rule, and autoplay starts it.
    const lastHere =
      value.source?.playing?.tag ??
      (state.lastOnAir?.sessionId === value.id ? state.lastOnAir.tag : undefined);
    const guarding =
      live(selected) && selected.phase === "Accepted" && selected.spec.follows !== undefined
        ? selected
        : [...items.values()].find(
            (item) =>
              item.spec.follows !== undefined &&
              item.sessionId === value.id &&
              live(item) &&
              item.phase !== "Started" &&
              !(
                item.phase === "Ready" &&
                item.withdraw === undefined &&
                actual[0]?.clipId === item.clipId &&
                // Fence before moving another clip ahead: it may start and end while the Move
                // reply is pending, leaving this clip after a different predecessor.
                desired[0]?.clipId === item.clipId &&
                sameClip(item.spec.follows, lastHere)
              ),
          );
    const guarded = guarding !== undefined;
    if (!guarded && value.playRetry !== undefined)
      updateSession(value.id, { playRetry: undefined });
    // Inside a boundary H3 holds the next clip armed, and turning autoplay off then leaves it
    // unstarted: on air the fence goes up only while a clip plays with time to spare, as a cut's
    // does, or stays as it is.
    const fenceable =
      value.id !== state.air ||
      value.autoplay === false ||
      (value.source?.playing !== undefined && playingRestMs(value) > cutMarginMs);
    const autoplay =
      value.wantAutoplay && state.cutting?.sessionId !== value.id && !(guarded && fenceable);
    if (value.source?.available === true && value.autoplay !== autoplay) {
      // Nothing else goes before it, even while a failed one waits to be asked again.
      const retry = value.autoplayRetry;
      if (retry === undefined || retry.enabled !== autoplay || now.mono >= retry.at)
        queueCommand(
          value.id,
          { _tag: "Autoplay", enabled: autoplay },
          state.cutting?.sessionId === value.id
            ? itemOf(cutterOf().cutter)?.spec.key
            : guarding?.spec.key,
        );
      return;
    }
    // The cut's next step, with autoplay off: stop the clip it cuts, and once that has ended,
    // play the cutter. The plan looks again between them, so a cutter withdrawn meanwhile goes
    // instead, and until its play nothing else is sent that could hold it up.
    const cutting = state.cutting;
    if (
      cutting?.sessionId === value.id &&
      (cutting.stage === "stopping" || cutting.stage === "stopped") &&
      cutterOf().wanted &&
      value.source?.available === true
    ) {
      const playing = value.source.playing?.clipId;
      if (cutting.stage === "stopping")
        return queueCommand(
          value.id,
          { _tag: "Stop", clipId: cutting.clipId },
          itemOf(cutterOf().cutter)?.spec.key,
        );
      if (playing === undefined)
        return queueCommand(
          value.id,
          { _tag: "Play", clipId: cutting.next },
          itemOf(cutterOf().cutter)?.spec.key,
        );
      if (playing === cutting.clipId) return;
      // Something else plays: the cutter waits for the next boundary.
      state = { ...state, cutting: undefined };
      return decideCommand(sessionId);
    }
    // Withdrawals the plan wants, retried once a refused one's session changes.
    for (const item of items.values())
      if (
        item.withdraw !== undefined &&
        item.clipId !== undefined &&
        item.sessionId === value.id &&
        (item.phase === "Building" || item.phase === "Ready") &&
        value.source?.available === true &&
        item.blockedRemove !== signature(value)
      )
        return queueCommand(value.id, { _tag: "Remove", clipId: item.clipId }, item.spec.key);
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
      for (const clip of readyOf(value))
        if (fillerRemovable(value, clip))
          return queueCommand(value.id, { _tag: "Remove", clipId: clip.clipId });
    // Order its Ready clips by rank. A move never ranks across sessions.
    const moved = actual.findIndex((clip, index) => clip.clipId !== desired[index]!.clipId);
    const misplaced = moved < 0 ? undefined : desired[moved];
    if (
      value.source?.available === true &&
      misplaced?.tag !== undefined &&
      value.blockedMove !== signature(value) + misplaced.clipId &&
      now.mono >= value.moveRetryAt
    )
      return queueCommand(
        value.id,
        { _tag: "Move", clipId: misplaced.clipId, position: moved },
        misplaced.tag._tag === "Item" ? misplaced.tag.key : undefined,
      );
    // A cut lane's Ready item at the front cuts a lower lane's clip, or filler, that has a while to run.
    const front = value.id === state.air ? actual[0] : undefined;
    const cutItem = itemOf(front);
    const playing = value.source?.playing;
    if (
      front !== undefined &&
      cutItem?.phase === "Ready" &&
      playing !== undefined &&
      state.cutting === undefined &&
      config.lanes[cutItem.spec.lane]?.cut === true &&
      !(state.cut?.sessionId === value.id && state.cut.clipId === playing.clipId) &&
      playingRestMs(value) > cutMarginMs
    ) {
      // Only filler or a clip of a strictly lower lane is cut, never one of the cutter's lane or above.
      const playingItem = itemOf(playing);
      const lower =
        playing.tag?._tag === "Filler" ||
        (playingItem !== undefined && playingItem.spec.lane > cutItem.spec.lane);
      if (lower && airs(front)) {
        state = {
          ...state,
          cut: { sessionId: value.id, clipId: playing.clipId },
          cutting: {
            sessionId: value.id,
            clipId: playing.clipId,
            next: front.clipId,
            stage: "stopping",
          },
        };
        return decideCommand(sessionId);
      }
    }
    // A held item about to be next, or an At item Ready too early, is removed: rebuilt later, or
    // dropped if withdrawn.
    for (const [index, clip] of actual.entries()) {
      const item = itemOf(clip);
      if (item?.phase !== "Ready") continue;
      // An item waiting for the clip it follows would air before that clip.
      const pair = heldForFollows(item);
      const exposed = exposure(value, item, readyAheadMs(value, index));
      if (exposed && pair && item.withdraw === undefined) {
        withdraw(item.spec.key, "displaced");
        return decideCommand(sessionId);
      }
      if (exposed && value.source?.available === true) {
        // A withdrawal waiting on its session's queues to change is due now: its removal is asked
        // again, once, and drops the item. Refused again, it waits for a change after all.
        if (item.withdraw !== undefined) {
          if (item.discarded?.sessionId === value.id && item.discarded.clipId === clip.clipId)
            continue;
          set(item.spec.key, {
            blockedRemove: undefined,
            discarded: { sessionId: value.id, clipId: clip.clipId },
          });
          return queueCommand(value.id, { _tag: "Remove", clipId: clip.clipId }, item.spec.key);
        }
        // It goes back to the plan, to be built again once the air ahead covers its wait.
        set(item.spec.key, {
          phase: "Accepted",
          dispatchedAt: undefined,
          clipId: undefined,
          sessionId: undefined,
          covered: undefined,
          discarded: { sessionId: value.id, clipId: clip.clipId },
        });
        return queueCommand(value.id, { _tag: "Remove", clipId: clip.clipId }, item.spec.key);
      }
    }
    // A constrained clip cannot be left to autoplay: an early end or unexpectedly slow build
    // can make it next before its removal lands. Start only the Ready head the plan can honour.
    const head = actual[0];
    const headItem = itemOf(head);
    const retry = value.playRetry;
    // A clip reported playing while it waited out its seam, then held there as autoplay went off,
    // still heads the queue though it never started: it is played as a Ready one is.
    const stranded =
      headItem?.phase === "Started" &&
      headItem.sessionId === value.id &&
      headItem.clipId === head?.clipId;
    if (
      guarded &&
      value.id === state.air &&
      value.source?.available === true &&
      value.source.playing === undefined &&
      state.cutting === undefined &&
      head !== undefined &&
      airs(head) &&
      (headItem === undefined ||
        stranded ||
        (headItem.phase === "Ready" && headItem.withdraw === undefined)) &&
      (retry === undefined || retry.signature !== signature(value) || now.mono >= retry.at)
    )
      return queueCommand(value.id, { _tag: "Play", clipId: head.clipId }, headItem?.spec.key);
    // Build, on the session that takes new work: the first eligible item by build order, else
    // filler below its floor.
    const target = preferred();
    if (
      target?.id !== value.id ||
      !(state.accepting || state.drains.some((drain) => drain.finish === "accepted"))
    )
      return;
    // An enqueue whose reply was lost holds no build slot: H3 builds in order, so if it landed it
    // builds ahead of what follows, and if it did not, nothing would free the slot but the
    // unknown timeout. The cap check still counts it, since it may yet take air here.
    const inFlight =
      [...items.values()].filter(
        (item) => item.phase === "Building" && item.sessionId === target.id,
      ).length + target.source!.building.filter((clip) => clip.tag?._tag === "Filler").length;
    if (inFlight >= config.maxBuildsInFlight) return;
    const room = runway();
    const floorSeconds = fillerFloor();
    const drainingNeeds = state.drains.length === 0 || fillerNeeded;
    for (const item of eligible(floorSeconds)) {
      // What cannot air before this session's cap waits for its replacement, and so does what
      // follows it, which would otherwise air ahead of it.
      if (!fits(target, item.spec.seconds)) break;
      const from = item.spec.continuity
        ? continuation(item, target)
        : { _tag: "from" as const, clipId: undefined, follows: undefined };
      if (from._tag === "wait") continue;
      if (drainingNeeds && coverFirst(target, item, from.clipId !== undefined, room)) return;
      if (item.spec.follows !== undefined && value.autoplay !== false) {
        // It goes once a clip plays here with time to spare.
        if (!fenceable) continue;
        updateSession(value.id, { guardItem: item.spec.key });
        return queueCommand(value.id, { _tag: "Autoplay", enabled: false }, item.spec.key);
      }
      updateSession(value.id, { guardItem: undefined });
      set(item.spec.key, {
        phase: "Building",
        sessionId: target.id,
        dispatchedAt: now.mono,
        retryAt: undefined,
        continued: from.clipId !== undefined,
        follows: from.follows,
      });
      return queueCommand(
        target.id,
        {
          _tag: "Enqueue",
          request: item.spec.request,
          tag: { _tag: "Item", key: item.spec.key },
          continueFrom: from.clipId,
        },
        item.spec.key,
      );
    }
    const filler = config.filler;
    if (filler === undefined || !fillerFree()) return;
    const anchorGap = Math.max(
      0,
      ...[...items.values()].flatMap((item) => {
        const at = atMono(item);
        return live(item) && at !== undefined && at > now.mono ? [(at - now.mono) / 1000] : [];
      }),
    );
    const targetSeconds = Math.max(filler.target, floorSeconds);
    const refilling =
      anchorGap > room || (state.filler.refilling ? below(targetSeconds) : below(floorSeconds));
    state = { ...state, filler: { ...state.filler, refilling } };
    if (!refilling || (!below(targetSeconds) && room >= anchorGap) || !drainingNeeds) return;
    const seconds = fillLength(anchorGap - room, filler.lengths, estimates().length);
    if (!fits(target, fillerLength(seconds))) return;
    sendFiller(filler, target, seconds, room);
  }
  /**
   * `item`'s p95 build in seconds, at the continued rate if it continues a
   * clip. Unknown until three builds were measured.
   */
  function buildOf(item: Item, continued: boolean): number | undefined {
    const p95 = estimates().build?.p95;
    if (p95 === undefined) return undefined;
    return (continued ? (continuedBuildRate(state.samples) ?? p95) : p95) * item.spec.seconds;
  }
  /**
   * Protecting the air, sends a filler clip ahead of `item` when its p95 build
   * would outlast the air secured. The clip is long enough, within the filler's
   * lengths, that the air it adds less what its own build drains covers the
   * rest and a margin, but airs no longer than the item builds, which a longer
   * clip would only delay, unless the shortest the filler takes is longer. An
   * item waits for one such clip at most, and for none that cannot be sent
   * now, would add no air or build no sooner than the item, or would not air
   * before the session's cap. A shortfall within the margin alone is left to
   * the floor, which refills without holding the item.
   */
  function coverFirst(target: Session, item: Item, continued: boolean, room: number): boolean {
    const filler = config.filler;
    if (filler === undefined || !protects(item) || item.covered === true || !fillerFree())
      return false;
    const build = buildOf(item, continued);
    const rate = estimates().build?.p95;
    if (build === undefined || rate === undefined) return false;
    const dark = build - securedOf({ ...state, items }, now);
    const short = dark + lookaheadMarginSeconds;
    // A requested second airs as long as asked, or as a provider that cuts clips short leaves it.
    const airs = Math.min(1, estimates().length);
    if (dark <= 0 || airs <= rate) return false;
    // Sized as if the clip's build starts now and the item's once it ends, as with one build in
    // flight, the default. With more, a build already in flight here goes ahead of both, and
    // neither the air it drains meanwhile nor the air it adds is counted.
    const seconds = Math.min(
      filler.lengths.max,
      Math.max(filler.lengths.min, Math.min(short / (airs - rate), build / airs)),
    );
    // A refused clip goes again as it was asked for: it must build sooner and fit at its length.
    const length = fillerLength(seconds);
    if (rate * length >= build || !fits(target, length)) return false;
    set(item.spec.key, { covered: true });
    sendFiller(filler, target, seconds, room);
    return true;
  }
  /**
   * Whether filler protects the air ahead of `item`'s build. What has a time to
   * meet goes as soon as it may: filler ahead would only make it later, and a
   * firm one late enough to be dropped. An `At` start or a `startBy` names that
   * time; an `Asap` start, a released `Manual` item, which airs as one, or a
   * lane that cuts makes it now.
   */
  function protects(item: Item): boolean {
    return (
      config.filler?.protect === "air" &&
      item.startBy === undefined &&
      item.spec.start._tag !== "At" &&
      item.mode !== "asap" &&
      config.lanes[item.spec.lane]?.cut !== true &&
      // A cover would air between it and the clip it follows.
      item.spec.follows === undefined
    );
  }
  /**
   * Whether a filler clip may be sent: none waits out a refused one. Each clip takes its index
   * as its enqueue goes out, so each lane may carry one, and none waits for another's.
   */
  function fillerFree(): boolean {
    return now.mono >= state.filler.retryAt;
  }
  /** Asks for the filler clip `index` again as it was, unless moderation flagged it. */
  function retryFiller(index: number, request: Request): void {
    if (state.filler.flagged.includes(index)) return;
    const retries = [...state.filler.retries, { index, request }].sort((a, b) => a.index - b.index);
    state = { ...state, filler: { ...state.filler, retries } };
  }
  /**
   * Seconds the next filler clip asks for: a refused one is asked for again as it was, at its own
   * length, and a new one for `seconds`.
   */
  function fillerLength(seconds: number): number {
    const request = state.filler.retries[0]?.request;
    return request === undefined ? seconds : (request.seconds ?? requestSeconds.min);
  }
  /** Sends the next filler clip to `target`, a new one asked for `seconds`. */
  function sendFiller(
    filler: NonNullable<Config["filler"]>,
    target: Session,
    seconds: number,
    room: number,
  ): void {
    const [retry, ...retries] = state.filler.retries;
    const index = retry?.index ?? state.filler.index;
    const request = retry?.request ?? filler.clip({ index, runwaySeconds: room, seconds });
    const invalid = filler.invalid(request, index);
    if (invalid !== undefined) {
      // Skipping it would leave the air uncovered without a word, and asking again gets the same.
      state = { ...state, filler: { ...state.filler, retryAt: Infinity } };
      actions.push({ _tag: "Fail", reason: invalid, cause: "filler", index });
      return;
    }
    state = {
      ...state,
      filler: {
        ...state.filler,
        ...(retry === undefined ? { index: index + 1 } : { retries }),
      },
    };
    updateSession(target.id, {
      fillerSent: { at: now.mono, seconds: request.seconds ?? requestSeconds.min },
    });
    queueCommand(target.id, { _tag: "Enqueue", request, tag: { _tag: "Filler", index } });
  }
  /** Items that may be built now, first in build order. */
  function eligible(floorSeconds: number): ReadonlyArray<Item> {
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
          covered(item) &&
          // Nor one waiting for the clip it follows: with nothing ahead of it, it would air before
          // its removal could land.
          (!heldForFollows(item) || followCovered(item)) &&
          // A held item is built ahead only while filler keeps the runway at its floor.
          (item.mode !== "held" || (floorSeconds > 0 && !below(floorSeconds))),
      )
      .sort(buildOrder);
  }
  /**
   * Whether the air secured lasts to an `At` item's time, so that its build may go. Once the
   * runway stops falling, the item's time comes within it, at `coveredAt`.
   */
  function covered(item: Item): boolean {
    const at = atMono(item);
    if (at === undefined || at <= now.mono) return true;
    const { end, seconds } = runwayTerms();
    return Math.max(end, now.mono) + seconds * 1000 >= at || now.mono >= at - seconds * 1000;
  }
  /**
   * Whether an item waiting for the clip it follows may be built: once Ready it airs whenever
   * nothing airs ahead of it, so it goes only while the air ahead of a clip sent now, as
   * `pairAheadMs` counts it but with `At` clips before their time, outlasts its independent
   * build at the median by the exposure margin. A continued build waits for that clip to be Ready
   * ahead of it, and ends before it does (`followContinuation`).
   */
  function followCovered(item: Item): boolean {
    // Before three builds are measured its build may outlast any air ahead: it goes once the clip
    // it follows is Ready ahead of it, which airs first.
    if (estimates().build === undefined) return followedReady(item);
    const slack = followSlackMs(item);
    return slack !== undefined && slack > 0;
  }
  /** Whether the clip `item` follows is Ready and queued to air where it would air before it. */
  function followedReady(item: Item): boolean {
    const follows = item.spec.follows;
    if (follows === undefined) return false;
    return [session(state.air), preferred()].some((value) =>
      (value === undefined ? [] : readyOf(value)).some((clip) => {
        const other = itemOf(clip);
        return sameClip(follows, clip.tag) && (other === undefined || queuedToAir(items, other));
      }),
    );
  }
  /**
   * By how much the air ahead of `item`, as `followCovered` counts it, outlasts its build and the
   * margin; it falls as the clip on air plays. Undefined with no session taking new work.
   */
  function followSlackMs(item: Item): number | undefined {
    const target = preferred();
    if (target === undefined) return undefined;
    const air = session(state.air);
    // An `At` clip kept Ready airs at its time, which the air ahead of it reaches.
    const queuedMs = (value: Session | undefined): number =>
      (value === undefined ? [] : readyOf(value))
        .filter((clip) => {
          const other = itemOf(clip);
          return other === undefined || queuedToAir(items, other);
        })
        .reduce((total, clip) => total + clip.seconds * 1000, 0);
    const aheadMs =
      playingRestMs(air) + queuedMs(air) + (target.id === air?.id ? 0 : queuedMs(target));
    const buildMs = item.spec.seconds * (estimates().build?.median ?? 0) * 1000;
    return aheadMs - buildMs - exposureMarginMs;
  }
  /** When `followCovered` stops holding for `item` as the clip on air plays; undefined if not. */
  function followUncoveredAt(item: Item): number | undefined {
    if (estimates().build === undefined) return undefined;
    const slack = followSlackMs(item);
    const end = playingEndOf(session(state.air));
    if (slack === undefined || slack <= 0 || end === undefined) return undefined;
    return now.mono + slack < end ? now.mono + slack : undefined;
  }
  function coveredAt(item: Item): number | undefined {
    const at = atMono(item);
    return at === undefined || covered(item) ? undefined : at - runwayTerms().seconds * 1000;
  }
  /**
   * Whether `item`, Ready on `value` behind `readyMs` of Ready air, must leave its queue now: held
   * and about to be next, waiting for the clip it follows and about to air before it, or an `At`
   * item Ready too early.
   */
  function exposure(value: Session, item: Item, readyMs: number): boolean {
    const aheadMs = playingRestMs(value) + readyMs;
    if (item.mode === "held")
      return aheadMs < exposureMarginMs || now.mono >= (exposedAt(value, readyMs) ?? Infinity);
    if (heldForFollows(item))
      return (
        pairAheadMs(value, readyMs) < exposureMarginMs ||
        now.mono >= (pairExposedAt(value, readyMs) ?? Infinity)
      );
    const at = atMono(item);
    return at !== undefined && at > now.mono && aheadMs < at - now.mono;
  }
  /** Milliseconds of Ready air ahead of `value`'s Ready clip at `index`, less what plays. */
  function readyAheadMs(value: Session, index: number): number {
    return readyOf(value)
      .slice(0, index)
      .filter(airs)
      .reduce((total, other) => total + other.seconds * 1000, 0);
  }
  /**
   * When a held item Ready on `value` behind `aheadMs` of Ready air comes within the margin of
   * airing, as the clip playing there airs; undefined if it doesn't before that clip ends.
   */
  function exposedAt(value: Session, aheadMs: number): number | undefined {
    const end = playingEndOf(value);
    return end === undefined || aheadMs >= exposureMarginMs
      ? undefined
      : end + aheadMs - exposureMarginMs;
  }
  /**
   * How soon an item waiting for the clip it follows, Ready on `value` behind `readyMs` of Ready
   * air there, would air: on the session on air, after what plays; on the replacement, after all
   * the session on air has left, since the switch starts the replacement's queue at its head.
   */
  function pairAheadMs(value: Session, readyMs: number): number {
    if (value.id === state.air) return playingRestMs(value) + readyMs;
    const air = session(state.air);
    return playingRestMs(air) + (air === undefined ? 0 : readyAheadMs(air, Infinity)) + readyMs;
  }
  /** When `pairAheadMs` falls within the margin, as the clip on air plays. */
  function pairExposedAt(value: Session, readyMs: number): number | undefined {
    if (value.id === state.air) return exposedAt(value, readyMs);
    const air = session(state.air);
    return air === undefined ? undefined : exposedAt(air, readyAheadMs(air, Infinity) + readyMs);
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
  /** The runway filler refills below; protecting the air, it covers the next item's build too. */
  function fillerFloor(): number {
    const floor = clipFloor();
    if (floor <= 0 || config.filler?.protect !== "air") return floor;
    const next = eligible(floor)[0];
    const build =
      next === undefined || !protects(next) ? undefined : buildOf(next, next.spec.continuity);
    return build === undefined ? floor : Math.max(floor, build + lookaheadMarginSeconds);
  }
  function clipFloor(): number {
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
    if (item.spec.follows !== undefined)
      return followContinuation(item.spec.follows, target, item.spec.seconds);
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
   * What an item continues from when it follows a clip: that clip, playing or Ready on the same
   * session, never a clip after it, since that would air between. It waits while that clip is an
   * item not built yet. If the continued build is projected Ready only after that clip ends, as
   * `airsBefore` projects it, it builds independent: keeping its place matters more than the join.
   */
  function followContinuation(
    follows: ClipTag,
    target: Session,
    seconds: number,
  ): { readonly _tag: "wait" } | { readonly _tag: "from"; readonly clipId: string | undefined } {
    const onAir = target.id === state.air ? target.source?.playing : undefined;
    const playing = onAir !== undefined && sameClip(follows, onAir.tag);
    const queued = readyOf(target)
      .filter(airs)
      .sort((a, b) => compareRank(rankClip(a), rankClip(b)));
    const index = playing ? -1 : queued.findIndex((clip) => sameClip(follows, clip.tag));
    const predecessor = playing ? onAir.clipId : queued[index]?.clipId;
    if (predecessor === undefined) {
      const other = follows._tag === "Item" ? items.get(follows.key) : undefined;
      const unbuilt =
        other?.phase === "Accepted" || other?.phase === "Building" || other?.phase === "Unknown";
      return unbuilt ? { _tag: "wait" } : { _tag: "from", clipId: undefined };
    }
    if (target.source?.continuable.includes(predecessor) !== true)
      return { _tag: "from", clipId: undefined };
    const rate = continuedBuildRate(state.samples);
    if (rate === undefined) return { _tag: "from", clipId: predecessor };
    const readyAt = now.mono + (rate * seconds + lookaheadMarginSeconds) * 1000;
    const end =
      now.mono +
      (onAir === undefined ? 0 : playingRestMs(target)) +
      queued.slice(0, index + 1).reduce((total, clip) => total + clip.seconds * 1000, 0);
    return { _tag: "from", clipId: readyAt <= end ? predecessor : undefined };
  }
  /**
   * Whether the clip an item follows, not on air while the item waited, can no longer air before
   * it: an item settled, started or withdrawn, or a filler clip no session lists and none will
   * send again.
   */
  function followsGone(item: Item): boolean {
    const follows = item.spec.follows;
    if (follows === undefined || item.followsAired === true) return false;
    if (follows._tag === "Item") {
      const other = items.get(follows.key);
      return (
        other === undefined ||
        other.phase === "Settled" ||
        other.startedAt !== undefined ||
        other.withdraw !== undefined
      );
    }
    const index = follows.index;
    const listed = state.sessions.some(
      (value) =>
        [...value.fillers.values()].some((owner) => owner.index === index) ||
        value.unknownFiller.some((entry) => entry.index === index) ||
        (value.busy?.command._tag === "Enqueue" &&
          value.busy.command.tag._tag === "Filler" &&
          value.busy.command.tag.index === index) ||
        (value.playing?.tag?._tag === "Filler" && value.playing.tag.index === index),
    );
    const again = state.filler.retries.some((retry) => retry.index === index);
    return !listed && !again && index < state.filler.index;
  }
  /**
   * When the clip an item follows ends, once that is fixed: it plays on air, or is Ready there
   * behind the clip on air, whose end is known, with no item still to be built ranked ahead of
   * it. Undefined otherwise.
   */
  function followsEnd(item: Item): number | undefined {
    const follows = item.spec.follows;
    const air = session(state.air);
    const playing = air?.playing;
    if (follows === undefined || air === undefined || playing?.seconds === undefined)
      return undefined;
    const playingEnd = playing.at + playing.seconds * 1000;
    if (sameClip(follows, playing.tag)) return playingEnd;
    if (item.followsAired === true) return undefined;
    const queued = readyOf(air)
      .filter((clip) => clip.clipId !== playing.clipId && airs(clip))
      .sort((a, b) => compareRank(rankClip(a), rankClip(b)));
    const index = queued.findIndex((clip) => sameClip(follows, clip.tag));
    if (index < 0) return undefined;
    // An item not built yet that ranks ahead of that clip may still air before it.
    const rank = rankClip(queued[index]!);
    const unbuilt = [...items.values()].some(
      (other) =>
        other !== item &&
        other.withdraw === undefined &&
        (other.phase === "Accepted" || other.phase === "Building" || other.phase === "Unknown") &&
        compareRank(rankItem(other), rank) < 0,
    );
    if (unbuilt) return undefined;
    return (
      playingEnd +
      queued.slice(0, index + 1).reduce((total, clip) => total + clip.seconds * 1000, 0)
    );
  }
  /**
   * When an item that follows a clip, not sent yet, is projected, at the median, unable to be
   * Ready a readiness margin before that clip ends: as `projection` does, from the builds in
   * flight and those ahead of it in build order, at the continued rate for continued ones. Its own
   * build counts as independent, since a continued one that would miss falls back to that. The
   * projection grows with the time once nothing holds it, as `misses` has it.
   */
  function projectedLate(item: Item): { readonly due: number } | undefined {
    const perSecond = estimates().build?.median;
    if (
      item.spec.follows === undefined ||
      item.phase !== "Accepted" ||
      item.dispatchedAt !== undefined ||
      perSecond === undefined
    )
      return undefined;
    const end = followsEnd(item);
    if (end === undefined) return undefined;
    const continued = estimates().continuedBuild?.median ?? perSecond * continuedBuildFactor;
    const buildMs = (seconds: number, again: boolean) =>
      seconds * (again ? continued : perSecond) * 1000;
    const target = preferred() ?? session(state.air);
    const inFlight = [
      ...[...items.values()].flatMap((other) =>
        other.phase === "Building" &&
        other.dispatchedAt !== undefined &&
        other.spec.key !== item.spec.key
          ? [other.dispatchedAt + buildMs(other.spec.seconds, other.continued === true)]
          : [],
      ),
      ...(target?.fillerSent === undefined
        ? []
        : [target.fillerSent.at + buildMs(target.fillerSent.seconds, false)]),
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
      .reduce((total, other) => total + buildMs(other.spec.seconds, other.spec.continuity), 0);
    const after = first + buildMs(item.spec.seconds, false);
    const from = Math.max(-Infinity, ...inFlight) + after;
    return {
      due: from + readinessMarginMs >= end ? now.mono : end - readinessMarginMs - after,
    };
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
  /**
   * Where a clip like `probe` would land: the first clip of the projected air order it can follow.
   * After an item, it goes as an insert after that item; after a filler clip, or an item in a
   * cutting lane, as an `Asap` item in the lowest lane that waits for that clip. A boundary counts
   * when a forward run with the clip submitted there airs it right after that clip, Ready a
   * readiness margin before the boundary, and drops nothing the run without it airs. A clip this
   * playout did not enqueue can't be followed, nor a cut not on air yet, which the run doesn't
   * project.
   */
  function place(probe: Probe): Placement | null {
    const air = session(state.air);
    if (air === undefined) return null;
    const unmeasured = estimates().build === undefined;
    // An enqueue whose outcome is unknown may hold the build slot: nothing is Ready for sure.
    const target = preferred() ?? air;
    const unknown =
      air.unknownFiller.length > 0 ||
      target.unknownFiller.length > 0 ||
      [...items.values()].some(
        (item) =>
          item.phase === "Unknown" && (item.sessionId === air.id || item.sessionId === target.id),
      );
    const last = state.lastOnAir;
    const base = forward(undefined, 40);
    // With nothing on air, the clip that aired last may still be followed, across the gap.
    const lastClip: Projected | undefined =
      air.playing !== undefined || last === undefined
        ? undefined
        : {
            tag: last.tag,
            item: undefined,
            sessionId: last.sessionId,
            clipId: last.clipId,
            seconds: 0,
            readyAt: -Infinity,
            projected: false,
            continued: false,
          };
    const candidates = [
      ...(lastClip === undefined ? [] : [{ clip: lastClip, aired: true }]),
      ...base.aired.map((entry, index) => ({
        clip: entry.clip,
        aired: index === 0 && air.playing !== undefined,
      })),
    ];
    const airedBase = new Set(base.aired.flatMap((entry) => entry.clip.item ?? []));
    for (const candidate of candidates) {
      const tag = candidate.clip.tag;
      if (tag === undefined) continue;
      const item = candidate.clip.item;
      if (item !== undefined && cuts(item) && item.phase !== "Started") continue;
      const hypothesis = hypothesize(candidate.clip, tag, candidate.aired, probe);
      if (hypothesis === undefined) continue;
      const run = forward(hypothesis, 0);
      const index = run.probe?.index;
      if (run.probe === undefined || index === undefined) continue;
      if ([...run.gone].some((other) => other !== hypothesis.item && airedBase.has(other)))
        continue;
      const before = index === 0 ? undefined : run.aired[index - 1];
      const previous = before === undefined ? lastClip?.tag : before.clip.tag;
      if (!sameClip(tag, previous)) continue;
      // Ready a margin before the boundary, not after a gap, which the projected drop would not
      // wait for. With nothing on air, the first clip Ready airs at once.
      const boundary = before === undefined ? Infinity : before.start + before.clip.seconds * 1000;
      const readyAt = run.probe.readyAt ?? now.mono;
      if (readyAt + readinessMarginMs > boundary) continue;
      const start = run.aired[index]?.start ?? now.mono;
      const next = run.aired[index + 1]?.clip;
      const built =
        run.aired.slice(0, index).every((entry) => !entry.clip.projected) &&
        next?.projected !== true;
      return {
        after: tag,
        before: next?.tag ?? null,
        anchor: hypothesis.anchor,
        startsAt: now.wall + (start - now.mono),
        basis: basisOf(unmeasured, !built || unknown),
        continues: run.probe.continued,
      };
    }
    return null;
  }
  /**
   * The clip `place` asks about, as an edit would admit it right after `clip`: an insert after a
   * waiting or playing item, which takes its lane, place, group and start, or else an `Asap` item
   * in the lowest lane; either way following `clip`. Undefined when that lane would refuse it or
   * have it replace what waits there.
   */
  function hypothesize(
    clip: Projected,
    tag: ClipTag,
    aired: boolean,
    probe: Probe,
  ): Hypothesis | undefined {
    const anchor =
      clip.item !== undefined && live(clip.item) && clip.item.mode !== "held" && !cuts(clip.item)
        ? clip.item
        : undefined;
    const lowest = config.lanes[lanes - 1];
    if (
      anchor === undefined &&
      (lowest === undefined ||
        lowest.cut ||
        lowest.conflict === "replace" ||
        (lowest.conflict === "skip" &&
          [...items.values()].some((item) => item.spec.lane === lanes - 1 && live(item))))
    )
      return undefined;
    const spec: Spec = {
      key: probeKey,
      lane: anchor?.spec.lane ?? lanes - 1,
      request: { prompt: "placement probe", seconds: probe.seconds },
      seconds: probe.seconds,
      fingerprint: "",
      cues: [],
      continuity: probe.continuity,
      start: anchor === undefined ? { _tag: "Asap" } : insertStart(anchor, "after"),
      follows: tag,
    };
    const submitAt = now.mono + probe.submitInMs;
    if (anchor === undefined)
      return { item: { ...newItem(spec), followsAired: aired }, submitAt, anchor: "next" };
    const neighbour = Math.min(
      anchor.order + 1,
      ...[...items.values()]
        .filter((other) => other.spec.lane === anchor.spec.lane && other !== anchor)
        .map((other) => other.order)
        .filter((order) => order > anchor.order),
    );
    const playing = anchor.phase === "Started";
    return {
      item: newItem(spec, {
        order: (anchor.order + neighbour) / 2,
        group: anchor.group,
        inserted: true,
        anchor: { key: anchor.spec.key, side: "after" },
        mode: playing ? "follow" : anchor.mode,
        followsAired: aired,
      }),
      submitAt,
      anchor: anchor.spec.key,
    };
  }
  /**
   * The plan run forward from now at the median build rates. Builds go one at a time in H3's
   * order: those in flight, then what the plan would send as each slot frees, the best eligible
   * item by build order, else a filler refill below the floor. At each boundary the best clip by
   * rank airs among those Ready a readiness margin before it, or else the first Ready; with nothing
   * Ready the air goes dark until something is. Items that follow a clip keep to it. New work goes
   * to the replacement once one is open, or from the lead before the cap, when the plan opens one;
   * until then what cannot air before the cap waits for it, and the air moves to it once the
   * session on air has nothing left. With `hypothesis`, the run ends once it aired and a clip after
   * it did, or once it is gone; without, after `limit` clips.
   */
  function forward(hypothesis: Hypothesis | undefined, limit: number): Run {
    const air = session(state.air);
    const target = preferred() ?? air;
    if (air === undefined || target === undefined)
      return { aired: [], probe: undefined, gone: new Set() };
    // New work goes to the replacement from `opensAt` on: one opening now, or the one the plan
    // opens its lead before the cap of the session on air, taken to open at once.
    const replacement = state.sessions.find((value) => value.id !== air.id && !value.retiring);
    const opensAtOf = (): number | undefined => {
      if (target.id !== air.id) return undefined;
      if (replacement !== undefined) return now.mono;
      if (air.lifetimeMs === Infinity) return undefined;
      return Math.max(now.mono, air.openedAt + air.lifetimeMs - config.leadMs);
    };
    const opensAt = opensAtOf();
    const replacementId = replacement?.id ?? "\u0000replacement";
    let targetId = target.id;
    const measured = estimates();
    const perSecond = measured.build?.median;
    const continuedPerSecond = measured.continuedBuild?.median;
    // Under three builds nothing is known of the build: none is counted.
    const buildMs = (seconds: number, continued: boolean): number => {
      if (perSecond === undefined) return 0;
      const rate = continued ? (continuedPerSecond ?? perSecond * continuedBuildFactor) : perSecond;
      return seconds * rate * 1000;
    };
    const ratio = measured.length;
    const lengthOf = (seconds: number): number => airedLengthOf(state.samples, seconds);
    const filler = config.filler;
    const floorSeconds = fillerFloor();
    const targetSeconds = Math.max(filler?.target ?? 0, floorSeconds);
    const horizon = now.mono + 600_000;

    const pool: Array<Projected> = [];
    const gone = new Set<Item>();
    const done = new Set<Item>();
    const dispatched = new Set<Item>();
    const followsAired = new Map<Item, boolean>();
    const free = new Map<string, number>();
    const aired: Array<{ readonly clip: Projected; readonly start: number }> = [];
    let retries = [...state.filler.retries];
    let nextIndex = state.filler.index;
    let refilling = state.filler.refilling;
    let probeReady: number | undefined;
    let probeContinued = false;

    const pending = [...items.values()].filter(
      (item) => item.phase === "Accepted" && item.withdraw === undefined && item.mode !== "held",
    );
    if (hypothesis !== undefined) pending.push(hypothesis.item);
    const ordered = [
      ...[...items.values()].filter(live),
      ...(hypothesis === undefined ? [] : [hypothesis.item]),
    ];
    const followed = ordered.filter(
      (item) => item.phase !== "Started" && item.withdraw === undefined,
    );
    for (const item of followed)
      if (item.spec.follows !== undefined) followsAired.set(item, item.followsAired === true);

    // Ready now on the session on air and on the one taking new work; the clip on air is not.
    const sessions = target.id === air.id ? [air] : [air, target];
    for (const value of sessions)
      for (const clip of readyOf(value)) {
        const item = itemOf(clip);
        if (clip.clipId === value.playing?.clipId) continue;
        if (
          item !== undefined &&
          (!live(item) || item.startedAt !== undefined || item.withdraw !== undefined)
        )
          continue;
        pool.push({
          tag: clip.tag,
          item,
          sessionId: value.id,
          clipId: clip.clipId,
          seconds: clip.seconds,
          readyAt: now.mono,
          projected: false,
          continued: false,
        });
      }
    // Builds in flight, in the order H3 takes them, each once the one before it is done.
    for (const value of sessions) {
      const busy = value.busy?.command;
      const flights: Array<Flight> = [
        ...[...items.values()].flatMap((item): ReadonlyArray<Flight> =>
          item.phase === "Building" && item.sessionId === value.id && item.withdraw === undefined
            ? [
                {
                  at: item.dispatchedAt ?? now.mono,
                  seconds: item.spec.seconds,
                  continued: item.continued === true,
                  tag: { _tag: "Item", key: item.spec.key },
                  item,
                  clipId: item.clipId,
                },
              ]
            : [],
        ),
        ...[...value.fillers].flatMap(([clipId, owner]): ReadonlyArray<Flight> =>
          owner.build === undefined
            ? []
            : [
                {
                  at: owner.build.dispatchedAt,
                  seconds: owner.build.seconds,
                  continued: false,
                  tag: { _tag: "Filler", index: owner.index },
                  item: undefined,
                  clipId,
                },
              ],
        ),
        ...(busy?._tag === "Enqueue" && busy.tag._tag === "Filler"
          ? [
              {
                at: value.fillerSent?.at ?? now.mono,
                seconds: value.fillerSent?.seconds ?? requestSeconds.min,
                continued: false,
                tag: busy.tag,
                item: undefined,
                clipId: undefined,
              },
            ]
          : []),
      ].sort((a, b) => a.at - b.at);
      let readyAt = -Infinity;
      for (const flight of flights) {
        readyAt = Math.max(
          now.mono,
          Math.max(readyAt, flight.at) + buildMs(flight.seconds, flight.continued),
        );
        pool.push({
          tag: flight.tag,
          item: flight.item,
          sessionId: value.id,
          clipId: flight.clipId,
          seconds: lengthOf(flight.seconds),
          readyAt,
          projected: true,
          continued: flight.continued,
        });
        if (flight.item !== undefined) dispatched.add(flight.item);
      }
      free.set(value.id, Math.max(now.mono, readyAt));
    }

    let onAir = air.id;
    const playing = air.playing;
    // A clip of unknown length may end at any moment.
    let t =
      playing?.seconds === undefined
        ? now.mono
        : Math.max(now.mono, playing.at + playing.seconds * 1000);
    let current = playing !== undefined;
    if (playing !== undefined)
      aired.push({
        clip: {
          tag: playing.tag,
          item: itemOf(playing),
          sessionId: air.id,
          clipId: playing.clipId,
          seconds: playing.seconds ?? 0,
          readyAt: -Infinity,
          projected: false,
          continued: false,
        },
        start: playing.at,
      });

    const deadline = (item: Item): number => {
      const at = atMono(item);
      const late = item.spec.start._tag === "At" ? item.spec.start.late : "nextBoundary";
      const own = at === undefined ? Infinity : lateAt(at, late);
      const firm =
        item.spec.window?.firm === true && item.startBy !== undefined ? item.startBy : Infinity;
      return Math.min(own, firm);
    };
    const readyBy = (item: Item, time: number): boolean =>
      item.phase === "Ready" ||
      item.phase === "Started" ||
      item.phase === "Settled" ||
      done.has(item) ||
      gone.has(item) ||
      pool.some((clip) => clip.item === item && clip.readyAt <= time);
    // A pending batch holds what it adds until all of it is Ready.
    const open = (id: number | undefined, time: number): boolean => {
      const batch = id === undefined ? undefined : state.batches.find((value) => value.id === id);
      return (
        batch?.adds.some((key) => {
          const add = items.get(key);
          return add !== undefined && !readyBy(add, time);
        }) === true
      );
    };
    // As `followHeldOf`: an anchor is ahead while it is Ready by `time`, not aired, and queued.
    const held = (item: Item, time: number): boolean =>
      followHeld(
        item,
        (other) => followsAired.get(other) ?? other.followsAired === true,
        (anchor) =>
          !done.has(anchor) &&
          pool.some((clip) => clip.item === anchor && clip.readyAt <= time) &&
          queuedAt(anchor, time),
        (key) => items.get(key),
      );
    // As `queuedToAir`.
    const queuedAt = (item: Item, time: number): boolean =>
      !gone.has(item) && item.mode !== "held" && !open(item.batch, time) && !held(item, time);
    const airsAt = (clip: Projected, time: number): boolean => {
      const item = clip.item;
      if (item === undefined) return true;
      const at = atMono(item);
      return (at === undefined || at <= time) && queuedAt(item, time);
    };
    const rankIn = (clip: Projected): Rank => {
      const item = clip.item;
      if (item === undefined)
        return clip.tag?._tag === "Filler" ? [lanes, 1, clip.tag.index, 0] : [-1, 0, 0, 0];
      const behind =
        item.follows === undefined || item.phase === "Accepted"
          ? undefined
          : pool.find(
              (other) => other.clipId === item.follows && other.sessionId === clip.sessionId,
            );
      if (behind !== undefined) {
        const rank = rankIn(behind);
        return [rank[0], rank[1], rank[2], rank[3] + 0.5];
      }
      if (item.mode === "asap") return [-0.5, 0, item.order, item.generation];
      return [
        item.spec.lane,
        superseded.has(item.spec.key) ? 2 : begun(item) ? 0 : 1,
        item.order,
        item.generation,
      ];
    };
    const byRank = (a: Projected, b: Projected): number => compareRank(rankIn(a), rankIn(b));
    /** Seconds of air secured on the session taking new work, as `runway` counts them at `time`. */
    const runwayAt = (time: number): number => {
      const rest = targetId === onAir && current ? Math.max(0, t - time) : 0;
      const ready = pool
        .filter((clip) => clip.sessionId === targetId && clip.readyAt <= time && airsAt(clip, time))
        .reduce((total, clip) => total + clip.seconds * 1000, 0);
      return (rest + ready) / 1000;
    };
    /** As `followCovered`, at `time`. */
    const coveredFor = (item: Item, time: number): boolean => {
      const follows = item.spec.follows;
      if (perSecond === undefined)
        return (
          follows !== undefined &&
          pool.some(
            (clip) =>
              (clip.sessionId === onAir || clip.sessionId === targetId) &&
              clip.readyAt <= time &&
              sameClip(follows, clip.tag) &&
              (clip.item === undefined || queuedAt(clip.item, time)),
          )
        );
      return aheadAt(time) > buildMs(item.spec.seconds, false) + exposureMarginMs;
    };
    /** As `followCovered` counts the air ahead of a clip sent at `time`. */
    const aheadAt = (time: number): number => {
      const readyOn = (id: string): number =>
        pool
          .filter(
            (clip) =>
              clip.sessionId === id &&
              clip.readyAt <= time &&
              (clip.item === undefined || queuedAt(clip.item, time)),
          )
          .reduce((total, clip) => total + clip.seconds * 1000, 0);
      const rest = current ? Math.max(0, t - time) : 0;
      return rest + readyOn(onAir) + (targetId === onAir ? 0 : readyOn(targetId));
    };
    const gapAt = (time: number): number =>
      Math.max(
        0,
        ...followed.flatMap((item) => {
          const at = atMono(item);
          return !gone.has(item) && !done.has(item) && at !== undefined && at > time
            ? [(at - time) / 1000]
            : [];
        }),
      );
    const pendingIn = (item: Item): boolean =>
      item.phase === "Accepted" && !dispatched.has(item) && !gone.has(item);
    // As `previousAdmitted`, against what the run has sent.
    const admitted = (item: Item): boolean => {
      if (item.inserted) {
        const before = ordered
          .filter(
            (other) =>
              other !== item &&
              !gone.has(other) &&
              other.spec.lane === item.spec.lane &&
              other.order < item.order,
          )
          .sort((a, b) => b.order - a.order)[0];
        return before === undefined || !pendingIn(before);
      }
      if (item.group === undefined) return true;
      const index = item.group.index;
      return !(groups.get(item.group.key)?.parts ?? []).some((part) => {
        const other = items.get(part);
        return (
          other?.group !== undefined &&
          !other.inserted &&
          other.group.index === index - 1 &&
          pendingIn(other)
        );
      });
    };
    // A continuing item waits for the item it follows to be built.
    const followsUnbuilt = (item: Item, time: number): boolean => {
      const follows = item.spec.follows;
      if (!item.spec.continuity || follows?._tag !== "Item") return false;
      const other = items.get(follows.key);
      return (
        other !== undefined &&
        (other.phase === "Accepted" || other.phase === "Building") &&
        !readyBy(other, time)
      );
    };
    const eligibleAt = (item: Item, time: number): boolean => {
      if (!pendingIn(item)) return false;
      if (item === hypothesis?.item && time < hypothesis.submitAt) return false;
      if ((item.notBefore ?? -Infinity) > time) return false;
      if (time >= deadline(item)) {
        gone.add(item);
        return false;
      }
      const at = atMono(item);
      if (at !== undefined && at > time && time + runwayAt(time) * 1000 < at) return false;
      if (held(item, time) && !coveredFor(item, time)) return false;
      return admitted(item) && !followsUnbuilt(item, time);
    };
    /** When `tag` ends, as `followContinuation` projects it at `time`: on air, or Ready behind what airs first. */
    const endOf = (tag: ClipTag, time: number): number | undefined => {
      const onTarget = targetId === onAir && current;
      const last = aired.at(-1)?.clip;
      if (onTarget && last !== undefined && sameClip(tag, last.tag)) return t;
      const queued = pool
        .filter((clip) => clip.sessionId === targetId && clip.readyAt <= time && airsAt(clip, time))
        .sort(byRank);
      const index = queued.findIndex((clip) => sameClip(tag, clip.tag));
      if (index < 0) return undefined;
      return (
        (onTarget ? Math.max(t, time) : time) +
        queued.slice(0, index + 1).reduce((total, clip) => total + clip.seconds * 1000, 0)
      );
    };
    // As `followContinuation` decides at dispatch; any other continuing item is taken to continue.
    const continues = (item: Item, time: number): boolean => {
      if (!item.spec.continuity) return false;
      const follows = item.spec.follows;
      if (follows === undefined) return true;
      const end = endOf(follows, time);
      if (end === undefined) return false;
      const rate = continuedBuildRate(state.samples);
      return (
        rate === undefined ||
        time + (rate * item.spec.seconds + lookaheadMarginSeconds) * 1000 <= end
      );
    };
    const build = (clip: Omit<Projected, "readyAt">, seconds: number, time: number): number => {
      const start = Math.max(free.get(targetId) ?? -Infinity, time);
      const readyAt = start + buildMs(seconds, clip.continued);
      free.set(targetId, readyAt);
      pool.push({ ...clip, readyAt });
      return readyAt;
    };
    const launch = (item: Item, time: number): void => {
      const decision = continues(item, time);
      dispatched.add(item);
      const readyAt = build(
        {
          tag: { _tag: "Item", key: item.spec.key },
          item,
          sessionId: targetId,
          clipId: undefined,
          seconds: lengthOf(item.spec.seconds),
          projected: true,
          continued: decision,
        },
        item.spec.seconds,
        time,
      );
      if (item === hypothesis?.item) {
        probeReady = readyAt;
        probeContinued = decision;
      }
    };
    /** As `fits`: what the session on air builds must finish airing before its cap. */
    const fitsAt = (seconds: number, time: number): boolean => {
      if (targetId !== air.id || air.lifetimeMs === Infinity) return true;
      const lengthMs = seconds * ratio * 1000;
      const marginMs = lookaheadMarginSeconds * 1000;
      if (lengthMs > air.lifetimeMs - marginMs) return true;
      const startsAt = onAir === air.id && current ? Math.max(t, time) : time;
      const queuedMs = pool
        .filter(
          (clip) => clip.sessionId === air.id && (clip.item === undefined || !gone.has(clip.item)),
        )
        .reduce((total, clip) => total + clip.seconds * 1000, 0);
      return startsAt + queuedMs + lengthMs <= air.openedAt + air.lifetimeMs - marginMs;
    };
    // As the plan refills below the floor, up to its target, or to tile the gap before an `At` anchor.
    const refill = (time: number): boolean => {
      if (filler === undefined || time < state.filler.retryAt) return false;
      const room = runwayAt(time);
      const gap = gapAt(time);
      // As `below`: under `seconds`, or at the instant the clip on air brings it there.
      const below = (seconds: number): boolean => {
        if (room < seconds) return true;
        if (targetId !== onAir || !current) return false;
        const ready = room - Math.max(0, t - time) / 1000;
        return seconds > ready && time >= t - (seconds - ready) * 1000;
      };
      refilling = gap > room || (refilling ? below(targetSeconds) : below(floorSeconds));
      if (!refilling || (!below(targetSeconds) && room >= gap)) return false;
      const [retry, ...rest] = retries;
      const seconds =
        retry === undefined
          ? fillLength(gap - room, filler.lengths, ratio)
          : (retry.request.seconds ?? requestSeconds.min);
      if (!fitsAt(seconds, time)) return false;
      const index = retry?.index ?? nextIndex;
      if (retry === undefined) nextIndex++;
      else retries = rest;
      build(
        {
          tag: { _tag: "Filler", index },
          item: undefined,
          sessionId: targetId,
          clipId: undefined,
          seconds: lengthOf(seconds),
          projected: true,
          continued: false,
        },
        seconds,
        time,
      );
      return true;
    };
    const dispatch = (time: number): void => {
      if (opensAt !== undefined && targetId !== replacementId && time >= opensAt) {
        targetId = replacementId;
        free.set(replacementId, Math.max(free.get(replacementId) ?? -Infinity, time));
      }
      // As the plan sweeps each step: what went late meanwhile no longer counts as air.
      purge(time);
      for (let sent = 0; sent < 64; sent++) {
        const inFlight = pool.filter(
          (clip) =>
            clip.sessionId === targetId &&
            clip.projected &&
            clip.readyAt > time &&
            (clip.item === undefined || !gone.has(clip.item)),
        ).length;
        if (inFlight >= config.maxBuildsInFlight) return;
        const next = pending.filter((item) => eligibleAt(item, time)).sort(buildOrder)[0];
        // What cannot air before the cap of the session on air waits for the replacement.
        if (next !== undefined && fitsAt(next.spec.seconds, time)) launch(next, time);
        else if (!refill(time)) return;
      }
    };
    /** When the runway on the session taking new work falls to the refill threshold, as a clip there plays. */
    const refillAt = (time: number): number | undefined => {
      if (filler === undefined || targetId !== onAir || !current) return undefined;
      const threshold = refilling ? targetSeconds : floorSeconds;
      const ready = runwayAt(time) - Math.max(0, t - time) / 1000;
      const at = t - (threshold - ready) * 1000;
      return at > time && at < t ? at : undefined;
    };
    const nextDispatch = (time: number): number | undefined => {
      const times = [
        ...pool.flatMap((clip) =>
          clip.item === undefined || gone.has(clip.item)
            ? [clip.readyAt]
            : [clip.readyAt, deadline(clip.item)],
        ),
        ...pending.flatMap((item) =>
          pendingIn(item)
            ? [
                item.notBefore,
                atMono(item),
                item === hypothesis?.item ? hypothesis.submitAt : undefined,
              ]
            : [],
        ),
        refillAt(time),
        state.filler.retryAt,
        targetId === replacementId ? undefined : opensAt,
      ].filter((at): at is number => at !== undefined && at > time && Number.isFinite(at));
      return times.length === 0 ? undefined : Math.min(...times);
    };
    const remove = (clip: Projected): void => {
      const index = pool.indexOf(clip);
      if (index >= 0) pool.splice(index, 1);
    };
    // What a deadline drops, a batch commit withdraws, or a Ready replacement displaces.
    const purge = (time: number): void => {
      for (const clip of pool) {
        const item = clip.item;
        if (item === undefined || gone.has(item)) continue;
        const batch = state.batches.find((value) =>
          value.targets.some((entry) => entry.key === item.spec.key),
        );
        const replaced = pool.some(
          (other) =>
            other.item !== undefined &&
            !gone.has(other.item) &&
            other.item.replaces === item.spec.key &&
            other.readyAt <= time &&
            !open(other.item.batch, time),
        );
        if (time >= deadline(item) || (batch !== undefined && !open(batch.id, time)) || replaced)
          gone.add(item);
      }
    };
    /** The next clip at the boundary `t`, or false once nothing more can air. */
    const boundary = (clock: number): boolean => {
      purge(t);
      // The replacement takes the air once the session on air has nothing left, after the grace.
      if (onAir !== targetId) {
        if (
          pool.some(
            (clip) => clip.sessionId === targetId && clip.item !== undefined && clip.readyAt <= t,
          )
        )
          for (const clip of [...pool])
            if (clip.sessionId === onAir && clip.tag?._tag === "Filler") remove(clip);
        const left = pool.some(
          (clip) => clip.sessionId === onAir && (clip.item === undefined || !gone.has(clip.item)),
        );
        if (!left) {
          onAir = targetId;
          // The grace, then the replacement's first clip after a seam.
          t += (current ? config.graceMs : 0) + seamMs;
          current = false;
        }
      }
      const queued = pool.filter(
        (clip) => clip.sessionId === onAir && clip.readyAt <= t && airsAt(clip, t),
      );
      const moved = queued.filter(
        (clip) => !clip.projected || clip.readyAt + readinessMarginMs <= t,
      );
      const head =
        moved.length > 0 ? moved.sort(byRank)[0] : queued.sort((a, b) => a.readyAt - b.readyAt)[0];
      if (head === undefined) {
        // The plan takes back a clip waiting for the clip it follows rather than let it air first.
        for (const clip of pool)
          if (
            clip.sessionId === onAir &&
            clip.item !== undefined &&
            clip.readyAt <= t &&
            held(clip.item, t)
          )
            gone.add(clip.item);
        const upcoming = pool
          .filter(
            (clip) => clip.sessionId === onAir && (clip.item === undefined || !gone.has(clip.item)),
          )
          .map((clip) =>
            Math.max(
              clip.readyAt,
              clip.item === undefined ? -Infinity : (atMono(clip.item) ?? -Infinity),
            ),
          )
          .filter((at) => at > t);
        const soonest = Math.min(...upcoming, nextDispatch(clock) ?? Infinity);
        if (!Number.isFinite(soonest)) return false;
        // The air is dark until then; nothing airs between the last clip and the next.
        t = soonest;
        current = false;
        return true;
      }
      const start = current ? t + seamMs : t;
      aired.push({ clip: head, start });
      remove(head);
      const item = head.item;
      if (item !== undefined) done.add(item);
      // As `onAirChanged`: the clip an item follows has aired, and another clip after it displaces
      // the item.
      for (const other of followed) {
        const follows = other.spec.follows;
        if (follows === undefined || other === item || gone.has(other) || done.has(other)) continue;
        if (sameClip(follows, head.tag)) followsAired.set(other, true);
        else if (followsAired.get(other) === true) gone.add(other);
      }
      // What it replaces, or what replaces it, goes.
      if (item !== undefined)
        for (const other of followed)
          if (other.replaces === item.spec.key || item.replaces === other.spec.key) gone.add(other);
      t = start + head.seconds * 1000;
      current = true;
      return true;
    };
    const finished = (): boolean => {
      if (hypothesis === undefined) return aired.length >= limit;
      if (gone.has(hypothesis.item)) return true;
      const index = aired.findIndex((entry) => entry.clip.item === hypothesis.item);
      return index >= 0 && aired.length > index + 1;
    };

    let clock = now.mono;
    dispatch(clock);
    for (let look = 0; look < 4000 && !finished() && t <= horizon; look++) {
      const soon = nextDispatch(clock);
      if (soon !== undefined && soon <= t) {
        clock = soon;
        dispatch(clock);
        continue;
      }
      clock = Math.max(clock, t);
      if (!boundary(clock)) break;
    }
    const index =
      hypothesis === undefined
        ? -1
        : aired.findIndex((entry) => entry.clip.item === hypothesis.item);
    return {
      aired,
      probe:
        hypothesis === undefined
          ? undefined
          : {
              readyAt: probeReady,
              continued: probeContinued,
              index: index < 0 ? undefined : index,
            },
      gone,
    };
  }
  function wake(): number | undefined {
    const times: Array<number> = [];
    const later = (at: number | undefined) => {
      if (at !== undefined && at > now.mono && Number.isFinite(at)) times.push(at);
    };
    for (const item of items.values()) {
      const at = atMono(item);
      // A settled item's clip may still be listed Ready until a read shows it gone: its time
      // still ranks it there.
      later(at);
      if (item.phase === "Settled") continue;
      later(item.notBefore);
      later(item.startBy);
      later(item.retryAt);
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
      if (item.phase === "Accepted") later(coveredAt(item));
      if (lateWhenProjected(item)) later(item.startBy - projection(item).after);
      later(projectedLate(item)?.due);
      if (item.phase === "Accepted" && heldForFollows(item)) later(followUncoveredAt(item));
    }
    for (const value of state.sessions) {
      for (const entry of value.unknownFiller) later(entry.since + config.unknownTimeoutMs);
      later(value.openedAt + value.lifetimeMs - config.leadMs);
      later(value.openedAt + value.lifetimeMs);
      if (value.lastEnded !== undefined) later(value.lastEnded.at + config.graceMs);
      later(value.autoplayRetry?.at);
      later(value.playRetry?.at);
      later(value.moveRetryAt);
      for (const [index, clip] of readyOf(value).entries()) {
        const item = itemOf(clip);
        if (item?.phase === "Ready" && item.mode === "held")
          later(exposedAt(value, readyAheadMs(value, index)));
        else if (item?.phase === "Ready" && heldForFollows(item))
          later(pairExposedAt(value, readyAheadMs(value, index)));
      }
    }
    if (!state.openingPaused) later(state.openRetryAt);
    later(state.filler.retryAt);
    // The runway falls while a clip plays: wake as it falls to the filler's floor, which,
    // protecting the air, also covers the next item's build. Below it, filler goes at once.
    later(fallsTo(clipFloor()));
    later(fallsTo(fillerFloor()));
    return times.length === 0 ? undefined : Math.min(...times);
  }
};

/**
 * Applies one input: a pure function of the state, the input, the configuration and the time.
 * A decision can make another due at the same instant, as a build sent can leave a firm item
 * projected late, or a session lost at its cap settle what a batch waits on: the plan looks
 * again until nothing more is due, so none of it waits for a later input or wake.
 */
export const step: {
  (previous: State, input: Input, now: Now): (config: Config) => Step;
  (config: Config, previous: State, input: Input, now: Now): Step;
} = dual(4, (config: Config, previous: State, input: Input, now: Now): Step => {
  let result = decide(config, previous, input, now);
  for (let look = 1, decided = result.actions.length; decided > 0 && look < maxLooks; look++) {
    const again = decide(config, result.state, { _tag: "Tick" }, now);
    result = { ...again, actions: [...result.actions, ...again.actions] };
    decided = again.actions.length;
  }
  return result;
});

/**
 * What a withdrawal of `key` finds once nothing can change: the recorded fate of
 * the item, or of a group's parts, answered as a withdrawal waiting on them would be.
 */
export const fate: {
  (key: ItemKey): (state: State) => WithdrawOutcome;
  (state: State, key: ItemKey): WithdrawOutcome;
} = dual(2, (state: State, key: ItemKey): WithdrawOutcome =>
  combined(
    (state.groups.get(key)?.parts ?? [key]).flatMap((part) => {
      const item = state.items.get(part);
      return item === undefined ? [] : [fateOf(item)];
    }),
  ),
);

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
  const playing = state.sessions.find((value) => value.id === state.air)?.playing;
  return {
    accepting: state.accepting,
    runwaySeconds: securedOf(state, now),
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

/** The status of an item nothing can settle any more. */
export const indeterminate = { _tag: "Unknown", terminal: true } as const satisfies Settled;

/** What an as-run status settles: the item's start, its outcome, or both. */
export const decides = (
  status: AsRunStatus,
): {
  readonly started?: Extract<AsRunStatus, { readonly _tag: "Started" }> | NotStarted | undefined;
  readonly outcome?: Settled | undefined;
} => {
  switch (status._tag) {
    case "Started":
      return { started: status };
    // An item's start is always published before its end.
    case "Ended":
      return { outcome: status };
    case "Dropped":
    case "Failed":
    case "Unobserved":
      return { started: status, outcome: status };
    case "Unknown":
      return status.terminal === true ? { started: indeterminate, outcome: indeterminate } : {};
    default:
      return {};
  }
};
