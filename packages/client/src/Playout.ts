/**
 * Plays a schedule of clips to air across Reactor sessions. Applications submit
 * keyed items into priority lanes; one plan decides what to build, in what
 * order, what to withdraw, and when a renewing session takes over; each
 * session only executes its window of that plan. What actually aired is
 * reported as as-run evidence, kept apart from what was asked for.
 *
 * A pure policy makes every decision. The service applies them one command at
 * a time on each session, wakes on a submission, a session's evidence or the
 * policy's next deadline, and never polls. Sessions are supplied by an `open`
 * effect, such as `H3Source.open`, so the same plan runs on paid H3, a local
 * renderer (`LocalSource`) or the simulated Reactor in `ReactorTest`.
 */
import * as Context from "effect/Context";
import type * as Duration from "effect/Duration";
import type * as Effect from "effect/Effect";
import { dual } from "effect/Function";
import * as Layer from "effect/Layer";
import type * as Redacted from "effect/Redacted";
import type * as Scope from "effect/Scope";
import type * as Stream from "effect/Stream";
import type { Request } from "./H3.js";
import type {
  InvalidFiller,
  InvalidItem,
  ItemKey,
  PlayoutClosed,
  SubmitError,
} from "./internal/playout/errors.js";
import { clipModel } from "./internal/h3/clipModel.js";
import * as Runtime from "./internal/playout/runtime.js";
import type { AudioFrame, MediaPressure, VideoFrame } from "./Media.js";
import type { CommandFailure, ReactorError, ReactorFailure } from "./ReactorError.js";
import type { CloseReport } from "./Session.js";

export {
  InvalidFiller,
  InvalidItem,
  ItemKey,
  KeyMismatch,
  LaneBusy,
  PlayoutClosed,
  WouldMissDeadline,
} from "./internal/playout/errors.js";
export type { SubmitError } from "./internal/playout/errors.js";

export interface LaneSpec {
  /** Lanes are listed from the highest priority to the lowest. */
  readonly name: string;
  /**
   * What a new item does to its lane's waiting items: wait behind them (the
   * default); replace them, make-before-break, so they stay as cover until it
   * is Ready and then settle `Dropped` as `replaced`; or be refused with
   * `LaneBusy` while the lane has an item waiting or playing.
   */
  readonly conflict?: "queue" | "replace" | "skip" | undefined;
  /**
   * A Ready item of this lane stops a playing clip of a strictly lower lane, or
   * filler, instead of waiting for its end, unless that clip ends within a
   * second. A clip is stopped at most once, and a stopped clip cannot resume.
   */
  readonly cut?: boolean | undefined;
}

/** When an item may air. Only `Follow` keeps its lane's order. */
export type Start =
  /** The next boundary its lane reaches, in order: the default. */
  | { readonly _tag: "Follow" }
  /** The next boundary, ahead of everything waiting in every lane. */
  | { readonly _tag: "Asap" }
  /**
   * Held until `release(key)`, then as `Asap`. It is built ahead only while
   * filler keeps the runway at its floor, and taken back to be built again
   * if it would air next before then; otherwise it is built once released,
   * as always on a playout without filler.
   */
  | { readonly _tag: "Manual" }
  | {
      readonly _tag: "At";
      /** Epoch milliseconds: a wall-clock instant, so a clock correction moves it. */
      readonly time: number;
      /**
       * What to do when the boundary after `time` comes late. A start already under way when it
       * goes late, as a firm `startBy`'s, may still land.
       */
      readonly late:
        | { readonly _tag: "nextBoundary" }
        | { readonly _tag: "skipIfLaterThan"; readonly by: Duration.Input }
        | { readonly _tag: "drop" };
    };

/** A secondary event on a clip, fired while it plays: from its observed start, or back from its end. */
export interface Cue {
  readonly name: string;
  readonly at: { readonly from: "start" | "end"; readonly offset: Duration.Input };
}

/** Measured from submission on the monotonic clock. */
export interface Window {
  readonly notBefore?: Duration.Input | undefined;
  readonly startBy?: Duration.Input | undefined;
  /**
   * A firm item is refused with `WouldMissDeadline` when the plan, at the
   * median build rates, projects it to start no earlier than `startBy`:
   * every clip queued to air ahead of it airs first, at its own length,
   * built or not, and a boundary the next of them is not Ready for goes to
   * whichever clip is, filler included. Admitted, it is dropped as `late`
   * at `startBy`, or before it is sent once even its earliest start is past
   * it. A start already under way at `startBy`, a clip the provider holds
   * armed for its seam or a `play` of it in flight, may still land, up to a
   * command's round trip past it. A soft one may still air, recorded as late.
   */
  readonly firm: boolean;
}

interface ClipSpec<Req extends ClipRequest = Request> {
  readonly key: ItemKey;
  /** `request.seconds` is the requested length; the playout asks for its model's default length without it, H3's by default. */
  readonly request: Req;
  readonly cues?: ReadonlyArray<Cue> | undefined;
  /**
   * Build it continuing from the clip that airs just before it, when that clip
   * is on the same session and the provider still offers it. A continued build
   * takes longer: if it would be Ready only after that clip ends, it continues
   * instead from the clip that will be playing by then, and airs right behind
   * that one. The provider falls back to an independent clip without saying
   * so, so as-run never claims continuity.
   */
  readonly continuity?: "previous" | undefined;
}

/** The clip `place` asks about. */
export interface PlaceProbe {
  /** The key it will be submitted under; a refusal names it. */
  readonly key: ItemKey;
  /** The requested length; the model's default length when absent, H3's by default. */
  readonly seconds?: number | undefined;
  /** As it will be submitted: `"previous"` projects a continued build. */
  readonly continuity?: "previous" | undefined;
  /**
   * How long until the caller submits it, from this call on the monotonic
   * clock, from zero: the caller's own work, such as writing it; none when
   * absent. The playout adds the wait for a build slot and the build.
   */
  readonly submitIn?: Duration.Input | undefined;
}

/**
 * Where a clip submitted to follow `after` would land. It projects the plan as
 * it is at the call, at the median build rates, each lane's and filler's own
 * once three of their builds are measured, per second of the length their
 * model builds, and reserves nothing: a clip submitted later may still come
 * between the call and the clip, or after it.
 * A clip not built yet counts at the length the last clip that asked for as
 * much aired at, else at the length its model builds (`ClipModel.builtSeconds`),
 * scaled by the median ratio of the lengths clips aired at to those their model
 * built. While a `follows` item waits on a session, that session starts its
 * clips with a provider command, whose latency is not projected.
 */
export interface Placement {
  /** The clip it would follow: pass it as `follows`. */
  readonly after: ClipTag;
  /**
   * The clip projected to follow it, from what is queued now; null when
   * nothing this playout enqueued is. It is not enforced.
   */
  readonly before: ClipTag | null;
  /**
   * How to submit it: `insert` after this item, or, for `"next"`, `submit`
   * to the lowest lane with an `Asap` start, which waits for `after` to air.
   * `"next"` comes only when that lane would take it: one that doesn't cut,
   * replace what waits there, or skip while busy.
   */
  readonly anchor: ItemKey | "next";
  /** When it would start, in epoch milliseconds. */
  readonly startsAt: number;
  /**
   * `ready`: every clip through `after`, and `before`, is on air or Ready, and
   * no enqueue ahead has an unknown outcome. `projected`: a clip among them is
   * still to be built, a filler clip not yet sent included, or an enqueue with
   * an unknown outcome may hold the build slot; after a filler clip not yet
   * sent, it airs only at the next gap in what ranks above filler.
   * `unmeasured`: fewer than three builds are measured, so no build time is
   * counted.
   */
  readonly basis: "ready" | "projected" | "unmeasured";
  /**
   * Whether, as projected, the playout would ask for it to be built continuing
   * from `after`, which it never does across a switch of sessions. It decides
   * again as it sends the build, and builds independent if a continued build
   * would then end after `after` does. The provider falls back to an
   * independent clip without saying so.
   */
  readonly continues: boolean;
}

/** A clip a forecast projects to air. */
export interface ForecastedClip {
  /** An item, a filler clip, or null for a clip this playout did not enqueue. */
  readonly clip: ClipTag | null;
  /**
   * The group it is a place of, and which; undefined for an item in no group, or one inserted
   * beside a part.
   */
  readonly group: { readonly key: ItemKey; readonly part: number } | undefined;
  /** When it is projected to start and to end, in epoch milliseconds. */
  readonly startsAt: number;
  readonly endsAt: number;
  /** `playing` now; `ready` built; `building` sent and not built yet; `queued` not sent yet. */
  readonly state: "playing" | "ready" | "building" | "queued";
  /** The session it airs on: the one on air, or its replacement, open or still to open. */
  readonly session: "on-air" | "replacement";
}

/** An item a forecast projects to go without airing. */
export interface ForecastedDrop {
  readonly key: ItemKey;
  /** The group it is a place of, and which, as a clip's. */
  readonly group?: { readonly key: ItemKey; readonly part: number } | undefined;
  /**
   * `late` past its deadline; `displaced` from the clip it follows; `withdrawn` by an edit, a
   * group's break, or its old item airing first; `replaced` by its Ready replacement.
   */
  readonly reason: "late" | "displaced" | "withdrawn" | "replaced";
  /** When the projection drops it, in epoch milliseconds; now for a withdrawal already asked. */
  readonly at: number;
}

/** An item a forecast cannot place, and why. */
export interface Unplaced {
  readonly key: ItemKey;
  /**
   * `held` until released; `unknown`, sent with its outcome never seen; `cut`, waiting to cut a
   * lower lane, which the projection does not model; `blocked` behind one of these, a member of
   * its group or the item whose clip it follows; `beyond` what it projects: past the ten minutes
   * it looks ahead, or with no session on air yet.
   */
  readonly why: "held" | "unknown" | "cut" | "blocked" | "beyond";
}

/**
 * What the plan projects to air from now, at the build rates it has measured: a projection, not a
 * promise. It changes as builds are measured and items arrive, and ages between changes: a build
 * running longer than projected shows once it ends. It looks ten minutes ahead and projects one
 * renewal at most.
 */
export interface Forecast {
  /** When it was taken, in epoch milliseconds. */
  readonly at: number;
  /** In the order they air, from the clip on air, up to the last item it can place. */
  readonly clips: ReadonlyArray<ForecastedClip>;
  /** In the order they go. */
  readonly drops: ReadonlyArray<ForecastedDrop>;
  /** Every item still to air that it neither airs nor drops. */
  readonly unplaced: ReadonlyArray<Unplaced>;
  /** When the last item it projects to air ends, in epoch milliseconds; null when none does. */
  readonly drainsAt: number | null;
  /** `unmeasured` before three builds were measured, when every build counts as instant. */
  readonly basis: "measured" | "unmeasured";
}

/** Where a forecast puts an item. */
export type ForecastedItem =
  | { readonly _tag: "Airs"; readonly clip: ForecastedClip }
  | { readonly _tag: "Drops"; readonly drop: ForecastedDrop }
  | { readonly _tag: "Unplaced"; readonly why: Unplaced["why"] }
  /** Not in it: settled already, or never submitted. */
  | { readonly _tag: "Absent" };

/** Where `forecast` puts the item `key`. */
export const forecastFor: {
  (key: ItemKey): (forecast: Forecast) => ForecastedItem;
  (forecast: Forecast, key: ItemKey): ForecastedItem;
} = dual(2, (forecast: Forecast, key: ItemKey): ForecastedItem => {
  const clip = forecast.clips.find(
    (entry) => entry.clip?._tag === "Item" && entry.clip.key === key,
  );
  if (clip !== undefined) return { _tag: "Airs", clip };
  const drop = forecast.drops.find((entry) => entry.key === key);
  if (drop !== undefined) return { _tag: "Drops", drop };
  const unplaced = forecast.unplaced.find((entry) => entry.key === key);
  return unplaced === undefined ? { _tag: "Absent" } : { _tag: "Unplaced", why: unplaced.why };
});

/**
 * A group's places as a forecast projects them: from the start of the first it airs to the end of
 * the last, and the places it drops. Items inserted beside its parts are not among them.
 */
export interface ForecastedGroup {
  readonly startsAt: number;
  readonly endsAt: number;
  readonly parts: ReadonlyArray<ForecastedClip>;
  readonly drops: ReadonlyArray<ForecastedDrop>;
}

/** Where `forecast` puts the places of the group `key`; undefined when it airs none of them. */
export const forecastGroup: {
  (key: ItemKey): (forecast: Forecast) => ForecastedGroup | undefined;
  (forecast: Forecast, key: ItemKey): ForecastedGroup | undefined;
} = dual(2, (forecast: Forecast, key: ItemKey): ForecastedGroup | undefined => {
  const parts = forecast.clips.filter((clip) => clip.group?.key === key);
  const [first] = parts;
  const last = parts.at(-1);
  if (first === undefined || last === undefined) return undefined;
  const drops = forecast.drops.filter((drop) => drop.group?.key === key);
  return { startsAt: first.startsAt, endsAt: last.endsAt, parts, drops };
});

export interface ItemSpec<Req extends ClipRequest = Request> extends ClipSpec<Req> {
  readonly lane: string;
  readonly window?: Window | undefined;
  readonly start?: Start | undefined;
  /**
   * The clip it must start right after on air, across a renewal too, or it is
   * dropped as `displaced`: once that clip goes without airing, once another
   * clip, filler included, starts after it first, once the item is projected
   * unable to be Ready by that clip's end, or once it is Ready and would air
   * before that clip. It waits, not airing, until that clip has aired, and is
   * built only while the air ahead of it outlasts its build, or before builds
   * are measured once that clip is Ready ahead of it. Autoplay is fenced
   * before its build, once a clip the provider reported starting plays on
   * that session, so an early end cannot air it before that clip. If it is
   * not Ready, on a session still open, when that clip ends, fails on air or
   * is lost on air with its session, it is dropped as `displaced`; a planned
   * switch keeps it. While it waits on a session, that session starts its
   * clips with a provider command, a round trip after each boundary, so a
   * command whose outcome is unknown can hold the air there dark until the
   * provider's reply deadline. Following an item in a pending batch, it may be
   * built and then dropped before the batch commits. It gets no filler cover;
   * with `continuity`, it builds independent rather than miss; a replacement
   * keeps it. A firm window on it is checked against a projection that puts
   * it behind everything queued, so it is likely refused. It is refused with
   * `InvalidItem` in a cutting lane, naming the item's own key or a group key,
   * or naming filler on a playout without filler or by an index that is not a
   * whole number.
   */
  readonly follows?: ClipTag | undefined;
}

export type GroupPart<Req extends ClipRequest = Request> = ClipSpec<Req>;

export interface GroupSpec<Req extends ClipRequest = Request> {
  readonly key: ItemKey;
  readonly lane: string;
  /**
   * Built in order and aired in order, each place once the one before it has started; a higher
   * lane may still go between.
   */
  readonly parts: readonly [GroupPart<Req>, ...ReadonlyArray<GroupPart<Req>>];
  /**
   * When the first part may air, as an item's `start`; the rest follow it, with no time of their
   * own. `Follow` by default.
   */
  readonly start?: Start | undefined;
  /** It applies to the first part. */
  readonly window?: Window | undefined;
}

/**
 * A clip that airs immediately before or after an anchor. Give exactly one of the two.
 * `after` an item already playing airs at the next boundary; `before` one refuses.
 * It takes the anchor's lane, place, group and start. After an `At` anchor it
 * does not take the anchor's `late`: past the anchor's time it airs at the next
 * boundary.
 */
export interface InsertSpec<Req extends ClipRequest = Request> extends ClipSpec<Req> {
  readonly before?: ItemKey | undefined;
  readonly after?: ItemKey | undefined;
  readonly window?: Window | undefined;
  /**
   * The clip it must start right after on air, across a renewal too, or it is
   * dropped as `displaced`: once that clip goes without airing, once another
   * clip, filler included, starts after it first, once the item is projected
   * unable to be Ready by that clip's end, or once it is Ready and would air
   * before that clip. It waits, not airing, until that clip has aired, and is
   * built only while the air ahead of it outlasts its build, or before builds
   * are measured once that clip is Ready ahead of it. Autoplay is fenced
   * before its build, once a clip the provider reported starting plays on
   * that session, so an early end cannot air it before that clip. If it is
   * not Ready, on a session still open, when that clip ends, fails on air or
   * is lost on air with its session, it is dropped as `displaced`; a planned
   * switch keeps it. While it waits on a session, that session starts its
   * clips with a provider command, a round trip after each boundary, so a
   * command whose outcome is unknown can hold the air there dark until the
   * provider's reply deadline. Following an item in a pending batch, it may be
   * built and then dropped before the batch commits. It gets no filler cover;
   * with `continuity`, it builds independent rather than miss; a replacement
   * keeps it. A firm window on it is checked against a projection that puts
   * it behind everything queued, so it is likely refused. It is refused with
   * `InvalidItem` in a cutting lane, naming the item's own key or a group key,
   * or naming filler on a playout without filler or by an index that is not a
   * whole number. Placed in a group, it is refused with `InvalidItem` when it
   * would wait for a member of that group after it, which waits behind it:
   * naming that member, or an item that waits for one in turn, by the clip it
   * follows or behind the members of its own group before it.
   */
  readonly follows?: ClipTag | undefined;
}

/** The clip that takes a queued item's place, under a key of its own. */
export type ReplacementSpec<Req extends ClipRequest = Request> = ClipSpec<Req>;

/** One edit of a batch applied together. */
export type Edit<Req extends ClipRequest = Request> =
  | { readonly _tag: "Submit"; readonly item: ItemSpec<Req> }
  | { readonly _tag: "SubmitGroup"; readonly group: GroupSpec<Req> }
  | { readonly _tag: "Insert"; readonly insert: InsertSpec<Req> }
  | { readonly _tag: "Replace"; readonly key: ItemKey; readonly next: ReplacementSpec<Req> }
  | { readonly _tag: "Withdraw"; readonly key: ItemKey };

export type WithdrawOutcome = "withdrawn" | "already-started" | "not-found";

/** Why an item failed for good. */
export type FailureReason =
  /**
   * The provider failed its clip. `message` is the library's; the provider's
   * own words stay in `provider`, out of messages, logs and spans.
   */
  | {
      readonly _tag: "Clip";
      readonly message: string;
      readonly provider: Redacted.Redacted<string>;
    }
  /**
   * A command for it failed in a way a retry would not mend. An enqueue its
   * session refused unsent, not ready for it, is sent there again once what the
   * session reports has changed, as after a reconnect; refused again while the
   * session reports itself ready, it fails here.
   */
  | { readonly _tag: "Command"; readonly cause: CommandFailure }
  /** Its session was lost while it played, or before it was built on two sessions in a row. */
  | { readonly _tag: "Lost"; readonly sessionId: string }
  /** Content moderation ended its session over it; it is never built again. */
  | { readonly _tag: "Moderated"; readonly categories: ReadonlyArray<string> }
  /** The playout closed before it aired. */
  | { readonly _tag: "Closed" };

/**
 * What became of an item. `Unknown` is "sent, acknowledgement never seen";
 * `Unobserved` is "acknowledged, start never seen". Neither is ever replayed
 * or given an invented time.
 */
export type AsRunStatus =
  | {
      readonly _tag: "Accepted";
      /** Accepted again: its clip was lost with this session before it aired, and it is rebuilt. */
      readonly carried?: { readonly sessionId: string } | undefined;
    }
  | { readonly _tag: "Building"; readonly sessionId: string }
  | { readonly _tag: "Ready"; readonly sessionId: string }
  | {
      readonly _tag: "Started";
      /** Epoch milliseconds of the local observation. */
      readonly at: number;
      readonly sessionId: string;
      readonly seconds: number;
      /** How late it started, as observed here: past its window's `startBy`, else its `At` time. */
      readonly lateByMillis?: number | undefined;
    }
  | {
      readonly _tag: "Ended";
      readonly at: number;
      readonly termination: "finished" | "stopped";
      readonly airedSeconds: number;
    }
  | {
      readonly _tag: "Dropped";
      /** `displaced`: the clip it `follows` could not air right before it. */
      readonly reason: "late" | "withdrawn" | "replaced" | "displaced";
    }
  | { readonly _tag: "Failed"; readonly reason: FailureReason }
  | { readonly _tag: "Unobserved" }
  | {
      readonly _tag: "Unknown";
      /** Nothing can settle it any more: its session retired or the playout closed. */
      readonly terminal?: true | undefined;
    };

export interface AsRunEvent {
  readonly key: ItemKey;
  /** Epoch milliseconds when this evidence was observed. */
  readonly at: number;
  readonly status: AsRunStatus;
}

/**
 * What became of an item for good: nothing changes it afterwards. `Unknown`
 * is settled only once it is terminal.
 */
export type Settled =
  | Extract<AsRunStatus, { readonly _tag: "Ended" | "Dropped" | "Failed" | "Unobserved" }>
  | { readonly _tag: "Unknown"; readonly terminal: true };

/** How an item settled without a start the playout saw. */
export type NotStarted = Exclude<Settled, { readonly _tag: "Ended" }>;

export interface ItemHandle {
  readonly key: ItemKey;
  /** The item's start, or how it settled without one. */
  readonly started: Effect.Effect<Extract<AsRunStatus, { readonly _tag: "Started" }> | NotStarted>;
  /** How the item settled. */
  readonly outcome: Effect.Effect<Settled>;
}

export interface GroupHandle {
  readonly key: ItemKey;
  readonly parts: readonly [ItemHandle, ...ReadonlyArray<ItemHandle>];
  /** The first place's start, or how the group settled without one. */
  readonly started: Effect.Effect<Extract<AsRunStatus, { readonly _tag: "Started" }> | NotStarted>;
  /** How the group settled, once every place has. */
  readonly outcome: Effect.Effect<GroupOutcome>;
}

/**
 * How a group settled. A place is a part with the replacements that took its place: its outcome
 * is that of its item that started, if one did, else that of its newest. Items inserted beside
 * parts are no places.
 */
export type GroupOutcome =
  /** Its first place never started: dropped, failed, or sent with its outcome never known. */
  | {
      readonly _tag: "NotAired";
      readonly first: Exclude<NotStarted, { readonly _tag: "Unobserved" }>;
    }
  /** Its first place started, or may have (`Unobserved`). */
  | {
      readonly _tag: "Aired";
      /**
       * How many places played out, in order, before the first that did not: `Ended` as
       * `finished`, or `Unobserved`.
       */
      readonly played: number;
      /**
       * The first place that did not play out, and how it settled; undefined when every place
       * played out.
       */
      readonly stopped: { readonly part: number; readonly outcome: Settled } | undefined;
      /** Each place's outcome, in order. */
      readonly parts: readonly [Settled, ...ReadonlyArray<Settled>];
    }
  /**
   * The playout died and could not settle the group as it closed: what aired is not known. A
   * playout that closes settles each place it had not, so its groups are `Aired` or `NotAired`.
   */
  | { readonly _tag: "Indeterminate" };

export type EditResult =
  | { readonly _tag: "Added"; readonly handle: ItemHandle }
  | { readonly _tag: "AddedGroup"; readonly handle: GroupHandle }
  | { readonly _tag: "Withdrawal"; readonly outcome: Effect.Effect<WithdrawOutcome> };

export interface EditHandle {
  readonly results: ReadonlyArray<EditResult>;
  /**
   * The batch took effect together: every clip it adds is Ready or settled,
   * and what it withdraws or replaces went at once.
   */
  readonly committed: Effect.Effect<void, PlayoutClosed>;
}

export interface CueEvent {
  readonly key: ItemKey;
  readonly name: string;
  /** Epoch milliseconds when it fired. */
  readonly at: number;
}

/** A session's part in the playout. */
export type SessionEvent =
  | {
      readonly _tag: "Opened";
      readonly sessionId: string;
      /** What remains of its granted length; absent for a session no cap ends. */
      readonly lifetimeSeconds?: number | undefined;
    }
  | { readonly _tag: "SetupFailed"; readonly reason: string; readonly consecutive: number }
  /** The replacement took the air at a boundary. */
  | {
      readonly _tag: "Switched";
      readonly from: string;
      readonly to: string;
      /** The retiring session never started a clip, or its last one left the air and the grace elapsed. */
      readonly decision: "no-observed-start" | "grace-elapsed";
    }
  /** A session was lost or expired before a planned switch; its unaired clips are rebuilt. */
  | {
      readonly _tag: "Replaced";
      readonly from: string;
      readonly reason: string;
      readonly carried: number;
    }
  /** The session's connection dropped, and is being reconnected. */
  | { readonly _tag: "Reconnecting"; readonly sessionId: string }
  /** The session is connected again, this long after the drop was seen. */
  | { readonly _tag: "Reconnected"; readonly sessionId: string; readonly afterMillis: number }
  /**
   * Content moderation flagged an input. On `terminate` Reactor ends the
   * session; the item whose enqueue was sent there last is held to blame, as
   * the verdict names no clip, and fails instead of being rebuilt.
   */
  | {
      readonly _tag: "Moderated";
      readonly sessionId: string;
      readonly action: string;
      readonly categories: ReadonlyArray<string>;
      readonly key?: ItemKey | undefined;
    };

export type Event =
  | { readonly _tag: "AsRun"; readonly event: AsRunEvent }
  | { readonly _tag: "Cue"; readonly event: CueEvent }
  | { readonly _tag: "Session"; readonly event: SessionEvent }
  /**
   * A filler clip started or ended, at epoch milliseconds; one that fails on
   * air ends then. `seconds` is its length as the provider built it, when known.
   */
  | {
      readonly _tag: "Filler";
      readonly index: number;
      readonly phase: "Started" | "Ended";
      readonly at: number;
      readonly seconds: number | undefined;
    }
  /**
   * A reader of the session's `track` fell behind its bound and missed frames;
   * it reads on from the next one. `pressure` is the session's media pressure
   * just after, its `readerOverflows` counting this one.
   */
  | {
      readonly _tag: "ReaderOverflow";
      readonly sessionId: string;
      readonly track: "video" | "audio";
      readonly at: number;
      readonly pressure: MediaPressure;
    }
  /** Nothing was left to play while the plan still wanted air. */
  | { readonly _tag: "Starved"; readonly at: number };

export interface State {
  readonly accepting: boolean;
  /**
   * Seconds of air secured: the playing clip's rest and the Ready clips that
   * will air after it, on the session on air and then on its replacement. A
   * held clip, or one anchored later, counts once it may air.
   */
  readonly runwaySeconds: number;
  /**
   * The clip on air: whose it is, when its start was seen in epoch
   * milliseconds, and its length as the provider built it. The length is
   * unknown for a clip that started before the playout attached, until it ends.
   */
  readonly playing: {
    readonly key: ItemKey | "filler" | "other";
    readonly startedAt: number;
    readonly seconds: number | undefined;
  } | null;
  readonly lanes: ReadonlyArray<{ readonly name: string; readonly keys: ReadonlyArray<ItemKey> }>;
  readonly sessions: ReadonlyArray<{
    readonly sessionId: string;
    readonly role: "on-air" | "replacement" | "retiring";
    readonly ready: ReadonlyArray<ItemKey | "filler" | "other">;
  }>;
  readonly starved: number;
  /**
   * Learned from this playout's own clips: build seconds per requested second
   * over every build, filler's included (median and p95, once three were
   * measured), apart for clips built continuing from another, which take
   * longer; and an item's actual over requested length.
   */
  readonly estimates: {
    readonly build: BuildSpread | undefined;
    readonly continuedBuild: BuildSpread | undefined;
    readonly length: number;
    /**
     * Each lane's own builds, per requested second, once three were measured;
     * the playout projects a lane's items with them.
     */
    readonly lanes: ReadonlyArray<{
      readonly name: string;
      readonly build: BuildSpread | undefined;
      readonly continuedBuild: BuildSpread | undefined;
    }>;
    /** Filler's own builds, per requested second; the floor covers one of filler's slow builds. */
    readonly filler: { readonly build: BuildSpread | undefined };
  };
}

/** Build seconds per second of clip over measured builds: their median and their p95. */
export interface BuildSpread {
  readonly median: number;
  readonly p95: number;
}

/** Identifies what a clip was enqueued for, read back from the provider's clip metadata. */
export type ClipTag =
  | { readonly _tag: "Item"; readonly key: ItemKey }
  | { readonly _tag: "Filler"; readonly index: number };

export interface SourceClip {
  readonly clipId: string;
  /** Undefined for a clip this playout did not enqueue. */
  readonly tag: ClipTag | undefined;
  /** The provider's length for the clip, which may differ from the request. */
  readonly seconds: number;
}

/**
 * The clip playing, or armed to play. A provider may name one it never
 * reported starting, such as H3 for a clip playing before the source attached:
 * its tag and length are then unknown until it ends.
 */
export interface PlayingClip extends Omit<SourceClip, "seconds"> {
  readonly seconds: number | undefined;
}

/** One session's queues, as its provider last reported them. */
export interface SourceState {
  /** The provider accepts commands. */
  readonly available: boolean;
  /** Waiting to build, the build in flight first, in the provider's order. */
  readonly building: ReadonlyArray<SourceClip>;
  /**
   * Built and waiting to play, in playout order; a `move` position counts from its head. It may
   * still list the clip named `playing`, as H3 does for a clip armed through its seam, and the
   * playout counts that clip's length once.
   */
  readonly ready: ReadonlyArray<SourceClip>;
  readonly playing: PlayingClip | undefined;
  /** Clips a new build can continue from. */
  readonly continuable: ReadonlyArray<string>;
}

export type SourceEvent =
  | { readonly _tag: "State"; readonly state: SourceState }
  | { readonly _tag: "Started"; readonly clip: SourceClip }
  | {
      readonly _tag: "Ended";
      readonly clip: SourceClip;
      readonly termination: "finished" | "stopped";
      /** From the provider's own count when it gives one. */
      readonly airedSeconds?: number | undefined;
    }
  /** The provider failed a clip: `message` in the source's words, `provider` in its own. */
  | {
      readonly _tag: "Failed";
      readonly clip: SourceClip;
      readonly message: string;
      readonly provider: Redacted.Redacted<string>;
    }
  /** The provider's content moderation flagged an input; `terminate` ends the session. */
  | {
      readonly _tag: "Moderated";
      readonly action: string;
      readonly categories: ReadonlyArray<string>;
    }
  /**
   * The connection dropped and is being reconnected; a reconnect that fails, or outlasts the
   * source's wait, fails `events`.
   */
  | { readonly _tag: "Reconnecting" }
  /** The connection is back, this long after the drop was seen. */
  | { readonly _tag: "Reconnected"; readonly afterMillis: number }
  /** A reader of `video` or `audio` fell behind its bound, missed frames and reads on. */
  | {
      readonly _tag: "ReaderOverflow";
      readonly track: "video" | "audio";
      readonly pressure: MediaPressure;
    };

/** What any model's request has in common for the playout: its requested length, if it names one. */
export interface ClipRequest {
  readonly seconds?: number | undefined;
}

/**
 * A model as the playout plans for it: what a clip may ask for and how long it
 * is counted at. A source states the model it runs, and the playout refuses a
 * source whose model has another name than its own.
 */
export interface ClipModel<Req extends ClipRequest = Request> {
  /** Names the model in refusals; two different models must not share a name. */
  readonly name: string;
  /** The lengths a request may ask for, in seconds: filler's default `lengths` and `place`'s bounds. */
  readonly lengths: { readonly min: number; readonly max: number };
  /** The length a request without `seconds` is sent and planned at. */
  readonly defaultSeconds: number;
  /**
   * The length the model builds for a request of `seconds` within `lengths`: H3 and FastH3 align
   * it up to their frame grid. The playout counts a clip not built yet at this length and measures
   * its build per second of it; a renderer that builds what it is asked answers `seconds`.
   */
  readonly builtSeconds: (seconds: number) => number;
  /**
   * Where a request for the clip `tag` falls outside the model's limits: one
   * entry per field, naming the field and the limit, never the value, which may
   * be a prompt. Empty when the request is within them.
   */
  readonly check: (request: Req, tag: ClipTag) => ReadonlyArray<string>;
}

/**
 * One session as the playout drives it: its evidence, its commands and its
 * media. What the playout relies on:
 *
 * - `sessionId` is unique among the sources open at once; the playout refuses
 *   a second under the same id.
 * - A clip id names one clip of its session. Another session's clip may have
 *   the same id.
 * - `events` starts with a `State`. `Started`, `Ended` and `Failed` come before
 *   the `State` that reflects them, and once `enqueue` has returned no `State`
 *   leaves the clip out until its `Ended` or `Failed`.
 * - `events` fails only when the session is lost for good; the source
 *   recovers a dropped connection itself, and reports it.
 * - Each command ends, done or failed, in bounded time. The playout sends a
 *   session its commands one at a time, so one that never ends holds up the
 *   rest of that session's, though no other session's.
 * - A method that throws when called is reported as a defect, as one whose
 *   effect dies is, and whether its command applied is taken as unknown. An
 *   `enqueue` is never sent again, and unless a read of the session's queues
 *   shows its clip within `unknownTimeout`, a replacement takes over. A
 *   `remove` is asked again once the session's queues change, or at once
 *   while autoplay would start its clip next, and a `move` a second later.
 *   After such a `setAutoplay` the session's autoplay is unknown, and the
 *   value the playout wants goes again: a second later if it is the one that
 *   died, and at once if not. Until its autoplay is as wanted, a session is
 *   sent nothing else but, as it retires, the removal of its filler once its
 *   replacement has an item Ready, and, before autoplay comes on, the removal
 *   of a clip the playout withdrew or took off to build again. Removals that
 *   do not apply one after another, refused or with their outcome unknown,
 *   hold autoplay off for a second at most: then it comes on as wanted, and
 *   the removal is still asked. A `stop` or `play` ends its cut, and the
 *   cutter airs at the next boundary.
 * - `play` also starts clips outside a cut: while a `follows` item fences a
 *   session's autoplay, the playout starts that session's Ready head itself.
 *   One that fails or dies is asked again once the session's queues change or
 *   a second later, and one that succeeded is not sent again before its start
 *   shows in them, or a second passes.
 */
export interface Source<Req extends ClipRequest = Request> {
  /**
   * The model this source runs; the playout refuses one whose name differs from its own.
   * A source without one is not checked.
   */
  readonly model?: ClipModel<Req> | undefined;
  readonly sessionId: string;
  /**
   * What remains of the session's granted length when the source is returned,
   * counted by the playout from then; `Infinity` for none.
   */
  readonly lifetime: Duration.Duration;
  readonly events: Stream.Stream<SourceEvent, ReactorError>;
  readonly enqueue: (
    request: Req,
    tag: ClipTag,
    continueFrom?: string,
  ) => Effect.Effect<string, CommandFailure>;
  readonly remove: (clipId: string) => Effect.Effect<void, CommandFailure>;
  readonly move: (clipId: string, position: number) => Effect.Effect<void, CommandFailure>;
  readonly setAutoplay: (enabled: boolean) => Effect.Effect<void, CommandFailure>;
  /**
   * Stops `clipId` if it still plays, and completes once the provider reports
   * it ended. A provider's stop may name no clip, so one that ended first, or
   * another clip playing by then, is left alone. The playout turns autoplay
   * off first, so nothing starts in its place.
   */
  readonly stop: (clipId: string) => Effect.Effect<void, CommandFailure>;
  /** Plays `clipId`, a Ready clip, while nothing plays. */
  readonly play: (clipId: string) => Effect.Effect<void, CommandFailure>;
  readonly video: Stream.Stream<VideoFrame, ReactorError>;
  readonly audio: Stream.Stream<AudioFrame, ReactorError>;
  /** Idempotent; closing an owned session terminates it. */
  readonly close: Effect.Effect<CloseReport>;
}

export interface FillContext {
  /**
   * Numbers filler clips from zero as each is first asked for. One the provider refused is asked
   * for again as it was, under its own index, on whichever session's lane is free.
   */
  readonly index: number;
  readonly runwaySeconds: number;
  /**
   * A requested length within `filler.lengths`: before an `At` anchor, one of
   * equal clips that tile the uncovered gap, none asking for less than its
   * share, or, where such a clip would air past a capped session's cap, the
   * longest that airs before it; ahead of an item whose build it covers
   * (`filler.protect`), as long as that takes; otherwise the shortest, which
   * keeps boundaries, and so reactions, frequent. The playout plans with this
   * length: the tiling before an `At` anchor and `place`'s `startsAt` count a
   * filler clip not yet sent at the length its model builds for it, so a request
   * for another length moves them by the difference.
   */
  readonly seconds: number;
}

export interface Options<R = never, Req extends ClipRequest = Request> {
  /** Opens one session. Called for the first and for each replacement. */
  readonly open: Effect.Effect<Source<Req>, ReactorFailure, R | Scope.Scope>;
  readonly lanes: ReadonlyArray<LaneSpec>;
  /** The bottom lane, never owed: generated clips that keep the air covered. */
  readonly filler?:
    | {
        /** Air secured ahead: refill below `floor`, up to `target`. */
        readonly runway: { readonly floor: Duration.Input; readonly target: Duration.Input };
        /**
         * Called once per clip, as it is first asked for; keep it pure. A request
         * without `seconds` asks for the context's `seconds`. A request outside
         * its model's documented limits fails the playout with `InvalidFiller`.
         */
        readonly clip: (context: FillContext) => Req;
        /** Lengths a filler clip may take; the model's request range by default, H3's by default. */
        readonly lengths?: { readonly min: number; readonly max: number } | undefined;
        /**
         * What goes first when an item's build would outlast the air secured.
         * With `"air"`, the default, once three builds were measured, an item
         * whose p95 build, at the continued rate if it continues a clip,
         * exceeds `State.runwaySeconds` waits for one filler clip that builds
         * sooner than it does: long enough, within `lengths`, to cover the rest
         * and a second more, and airing no longer than the item builds unless
         * `lengths.min` is longer. The floor, which filler refills without
         * holding any item, covers the next such item's build and a second
         * more. An item with a time to meet goes as soon as it may: one with an
         * `At` start or a `startBy`, and, since its time is now, one with an
         * `Asap` start, a released `Manual` one, or one on a lane that cuts.
         * `"order"` builds each item as soon as it may: it airs sooner, but the
         * air may go dark while it builds.
         */
        readonly protect?: "air" | "order" | undefined;
      }
    | undefined;
  /**
   * Builds in flight at once on the session that takes new work; one by
   * default. A moderation verdict that names no item fails the latest enqueue
   * on its session: with more than one in flight that may be an innocent
   * item, while the flagged one is carried to the next session.
   */
  readonly maxBuildsInFlight?: number | undefined;
  /** Settled keys kept for idempotency, oldest dropped first; 4,096 by default. */
  readonly maxHistory?: number | undefined;
  /**
   * An enqueue whose outcome stays unknown this long marks its session
   * indeterminate, so a replacement takes over; 60 seconds by default. Until
   * then it holds no build slot: what follows builds behind it, and it is
   * never sent again.
   */
  readonly unknownTimeout?: Duration.Input | undefined;
  /**
   * Sessions content moderation may end before the playout fails, rather than
   * open more paid sessions for content that keeps being flagged; 2 by default.
   */
  readonly maxModerations?: number | undefined;
  readonly renewal?:
    | {
        /**
         * Opens the replacement this long before a session's lifetime ends, and
         * no earlier: what cannot air before the cap waits for it. 30 seconds by default.
         */
        readonly lead?: Duration.Input | undefined;
        /**
         * How long opening a session may take, the wait for a GPU included;
         * 3 minutes by default. Waiting for a GPU is not billed.
         */
        readonly openTimeout?: Duration.Input | undefined;
        /**
         * How long after the retiring session's last clip ends, or fails on air,
         * the switch waits; 250 ms by default.
         */
        readonly grace?: Duration.Input | undefined;
        /**
         * Consecutive failed setups that end the playout: opens that failed,
         * and sessions lost before any clip sent to them started. A run ends
         * when a session airs its first clip; more clips on the session already
         * on air don't end it. 3 by default. Each open is retried a second
         * longer after each failure, but at most this many seconds later, and
         * never sooner than a refusal's `Retry-After`. An open that failed with
         * an `AcquisitionFailure` whose `cleanup.allocation` is `"none"`, such
         * as a refusal with a 4xx status, billed nothing: refused while a
         * session holds the air, it counts neither then nor later, so it is
         * asked again however often it fails. Running out while a session holds
         * the air doesn't end the playout: opening pauses until that session's
         * cap ends it or it is lost, and then one more open is tried, once the
         * last failure's wait is over.
         */
        readonly maxSetupFailures?: number | undefined;
      }
    | undefined;
}

/**
 * Close reports of the sessions this playout retired, failed opens that
 * allocated included: every one that may still bill (`Session.mayStillBill`)
 * and the latest others.
 */
export interface Cleanup {
  readonly sessions: number;
  readonly retained: ReadonlyArray<CloseReport>;
}

export interface Service<Req extends ClipRequest = Request> {
  readonly submit: (item: ItemSpec<Req>) => Effect.Effect<ItemHandle, SubmitError>;
  /**
   * Parts that air in order, each place, a part and any replacement of it, once the one before it
   * has started. A place that fails, on air too, is dropped, or is sent with its outcome never
   * known ends the group: every part and insert after it goes as `withdrawn`. One that may have
   * aired unseen (`Unobserved`) does not. The handle's `outcome` says how many places played and
   * where the group stopped.
   */
  readonly submitGroup: (group: GroupSpec<Req>) => Effect.Effect<GroupHandle, SubmitError>;
  readonly insert: (spec: InsertSpec<Req>) => Effect.Effect<ItemHandle, SubmitError>;
  /**
   * Builds `next` for the item's place, lane, group position, start and the
   * clip it `follows`, and for a group's part, its window too: a place has one
   * time. Once `next` is Ready the item goes as `replaced`; if the item starts
   * first, `next` is dropped as `withdrawn`.
   */
  readonly replace: (
    key: ItemKey,
    next: ReplacementSpec<Req>,
  ) => Effect.Effect<ItemHandle, SubmitError>;
  /**
   * Several edits as one make-before-break change: all are checked before any
   * takes effect, and what the batch withdraws or replaces stays as cover
   * until everything it adds is Ready or settled.
   */
  readonly edit: (edits: ReadonlyArray<Edit<Req>>) => Effect.Effect<EditHandle, SubmitError>;
  /**
   * Releases a held `Manual` item to air at the next boundary. A group key releases the group's
   * held first part, and any replacement of it; the rest follow it.
   */
  readonly release: (key: ItemKey) => Effect.Effect<void, InvalidItem>;
  /**
   * Where a clip like `probe` would land: at the first boundary of the
   * projected air order it could make, submitted `submitIn` from now to
   * follow the clip before that boundary, as a submission its lane would
   * take, without dropping or displacing anything submitted already, and
   * with its clip projected Ready a readiness margin and one provider
   * command's round trip before the boundary. A clip that cannot air before
   * the cap of the session on air is placed on its replacement, if it can
   * air there before that one's cap. It never answers after a clip this
   * playout did not enqueue, nor after a cut not yet on air, which it does
   * not project. Null once the playout has stopped, with no session on air,
   * or when no boundary is makeable, as before anything has aired.
   */
  readonly place: (probe: PlaceProbe) => Effect.Effect<Placement | null, InvalidItem>;
  /**
   * The plan's projection of what airs from now; see `Forecast`. Once the playout has stopped it
   * projects nothing: a forecast with no clips, taken at the call.
   */
  readonly forecast: Effect.Effect<Forecast>;
  /**
   * A forecast now, then a new one each time the plan changes; a slow reader skips to the newest.
   * It ends when the playout stops.
   */
  readonly forecasts: Stream.Stream<Forecast>;
  /**
   * A group key withdraws its unstarted parts, and answers `withdrawn` if any
   * part was, else `already-started` if any started; a part key withdraws that
   * part and those after it, and answers for that part. Once the playout has
   * stopped, it answers from what became of the item or the parts. It answers
   * `not-found` for a key it doesn't hold, and for an item that settled
   * without starting, such as one that failed: its handle says how.
   */
  readonly withdraw: (key: ItemKey) => Effect.Effect<WithdrawOutcome>;
  /** Admits nothing more and completes once the chosen work has aired or settled. */
  readonly drain: (options?: {
    readonly finish?: "playing" | "accepted";
  }) => Effect.Effect<void, PlayoutClosed>;
  readonly state: Effect.Effect<State>;
  /** Every event from subscription on, in order. */
  readonly events: Stream.Stream<Event>;
  readonly asRun: Stream.Stream<AsRunEvent>;
  /**
   * The on-air session's picture, continuing across renewals, and ending
   * when the playout stops. It fails with the on-air source's media failure;
   * reading it again starts from the session then on air.
   */
  readonly video: Stream.Stream<VideoFrame, ReactorError>;
  /** The on-air session's sound, as `video` is its picture. */
  readonly audio: Stream.Stream<AudioFrame, ReactorError>;
  /**
   * Why the playout stopped: a session could not be opened or kept, a filler
   * request was outside its model's limits (`InvalidFiller`), or its scope closed
   * (`Closed`). A defect that stopped it, such as a throwing `filler.clip`,
   * stays a defect: this dies with it.
   */
  readonly failure: Effect.Effect<ReactorFailure | InvalidFiller>;
  readonly cleanup: Effect.Effect<Cleanup>;
}

export class Playout extends Context.Service<Playout, Service>()("reactor-effect-client/Playout") {}

/** Options for a playout whose request limits and lengths come from its model. */
export interface ModelOptions<R, Req extends ClipRequest> extends Options<R, Req> {
  /** The model this playout plans for; each opened source must name the same model. */
  readonly model: ClipModel<Req>;
}

/**
 * A playout in the caller's scope; closing the scope retires every session.
 * Pass `model` for another model's requests and lengths. The `Playout` service tag is H3's.
 */
export function make<R, Req extends ClipRequest>(
  options: ModelOptions<R, Req>,
): Effect.Effect<Service<Req>, never, R | Scope.Scope>;
export function make<R>(options: Options<R>): Effect.Effect<Service, never, R | Scope.Scope>;
export function make<R, Req extends ClipRequest>(options: ModelOptions<R, Req> | Options<R>) {
  return "model" in options
    ? Runtime.make(options, options.model)
    : Runtime.make(options, clipModel);
}

/**
 * H3's playout service in the caller's scope. For another model, declare your own service,
 * `class Channel extends Context.Service<Channel, Playout.Service<MyRequest>>()("app/Channel") {}`,
 * and build it with `Layer.effect(Channel, Playout.make({ model, … }))`:
 * a service key names one service type.
 */
export const layer = <R>(options: Options<R>): Layer.Layer<Playout, never, R> =>
  Layer.effect(Playout, make(options));

/** The lineup: one lane of the application's items ahead of filler. */
export const lineup = <Req extends ClipRequest = Request>(
  filler: NonNullable<Options<never, Req>["filler"]>,
): Pick<Options<never, Req>, "lanes" | "filler"> => ({
  lanes: [{ name: "line" }],
  filler,
});
