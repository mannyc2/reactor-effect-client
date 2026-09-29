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
  /** Built ahead and held until `release(key)`, then as `Asap`. */
  | { readonly _tag: "Manual" }
  | {
      readonly _tag: "At";
      /** Epoch milliseconds: a wall-clock instant, so a clock correction moves it. */
      readonly time: number;
      /** What to do when the boundary after `time` comes late. */
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
  /** A firm item is dropped at `startBy`; a soft one may still air, recorded as late. */
  readonly firm: boolean;
}

interface ClipSpec {
  readonly key: ItemKey;
  /** `request.seconds` is the requested length; 5 seconds when absent. */
  readonly request: Request;
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

export interface ItemSpec extends ClipSpec {
  readonly lane: string;
  readonly window?: Window | undefined;
  readonly start?: Start | undefined;
}

export type GroupPart = ClipSpec;

export interface GroupSpec {
  readonly key: ItemKey;
  readonly lane: string;
  /** Built in order and aired back to back; a higher lane may still go between parts. */
  readonly parts: readonly [GroupPart, ...ReadonlyArray<GroupPart>];
  /** It applies to the first part. */
  readonly window?: Window | undefined;
}

/**
 * A clip that airs immediately before or after an anchor. Give exactly one of the two.
 * `after` an item already playing airs at the next boundary; `before` one refuses.
 */
export interface InsertSpec extends ClipSpec {
  readonly before?: ItemKey | undefined;
  readonly after?: ItemKey | undefined;
  readonly window?: Window | undefined;
}

/** The clip that takes a queued item's place, under a key of its own. */
export type ReplacementSpec = ClipSpec;

/** One edit of a batch applied together. */
export type Edit =
  | { readonly _tag: "Submit"; readonly item: ItemSpec }
  | { readonly _tag: "SubmitGroup"; readonly group: GroupSpec }
  | { readonly _tag: "Insert"; readonly insert: InsertSpec }
  | { readonly _tag: "Replace"; readonly key: ItemKey; readonly next: ReplacementSpec }
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
      /** How late a soft window let it start. */
      readonly lateByMillis?: number | undefined;
    }
  | {
      readonly _tag: "Ended";
      readonly at: number;
      readonly termination: "finished" | "stopped";
      readonly airedSeconds: number;
    }
  | { readonly _tag: "Dropped"; readonly reason: "late" | "withdrawn" | "replaced" }
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
}

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
    readonly build: { readonly median: number; readonly p95: number } | undefined;
    readonly continuedBuild: { readonly median: number; readonly p95: number } | undefined;
    readonly length: number;
  };
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
  /** Built and waiting to play, in playout order. */
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
 *   `remove` is asked again once the session's queues change, and a `move` or
 *   `setAutoplay` a second later; nothing else goes to a session before its
 *   autoplay. A `stop` or `play` ends its cut, and the cutter airs at the next
 *   boundary.
 */
export interface Source {
  readonly sessionId: string;
  /**
   * What remains of the session's granted length when the source is returned,
   * counted by the playout from then; `Infinity` for none.
   */
  readonly lifetime: Duration.Duration;
  readonly events: Stream.Stream<SourceEvent, ReactorError>;
  readonly enqueue: (
    request: Request,
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
   * share; ahead of an item whose build it covers (`filler.protect`), as long
   * as that takes; otherwise the shortest, which keeps boundaries, and so
   * reactions, frequent.
   */
  readonly seconds: number;
}

export interface Options<R = never> {
  /** Opens one session. Called for the first and for each replacement. */
  readonly open: Effect.Effect<Source, ReactorFailure, R | Scope.Scope>;
  readonly lanes: ReadonlyArray<LaneSpec>;
  /** The bottom lane, never owed: generated clips that keep the air covered. */
  readonly filler?:
    | {
        /** Air secured ahead: refill below `floor`, up to `target`. */
        readonly runway: { readonly floor: Duration.Input; readonly target: Duration.Input };
        /**
         * Called once per clip, as it is first asked for; keep it pure. A request
         * outside H3's documented limits fails the playout with `InvalidFiller`.
         */
        readonly clip: (context: FillContext) => Request;
        /** Lengths a filler clip may take; H3's request range by default. */
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
         * Consecutive failed setups that end the playout: opens that failed, and
         * sessions lost before any clip sent to them started. A run ends when a
         * session airs its first clip; more clips on the session already on air
         * don't end it. 3 by default. Each open is retried a second longer after
         * each failure, and never sooner than a refusal's `Retry-After`. While a
         * session holds the air, only setups that allocated a session, or may
         * have, count, then or once that session is gone: an open that failed
         * with an `AcquisitionFailure` whose `cleanup.allocation` is `"none"`,
         * such as a refusal with a 4xx status, billed nothing, so it is asked
         * again however often it fails. Running out then doesn't end the
         * playout: opening pauses until that session's cap ends it or it is
         * lost, and then one more open is tried, once the last failure's wait is
         * over.
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

export class Playout extends Context.Service<
  Playout,
  {
    readonly submit: (item: ItemSpec) => Effect.Effect<ItemHandle, SubmitError>;
    readonly submitGroup: (group: GroupSpec) => Effect.Effect<GroupHandle, SubmitError>;
    readonly insert: (spec: InsertSpec) => Effect.Effect<ItemHandle, SubmitError>;
    /**
     * Builds `next` for the item's place, lane and group position. Once `next`
     * is Ready the item goes as `replaced`; if the item starts first, `next`
     * is dropped as `withdrawn`.
     */
    readonly replace: (
      key: ItemKey,
      next: ReplacementSpec,
    ) => Effect.Effect<ItemHandle, SubmitError>;
    /**
     * Several edits as one make-before-break change: all are checked before any
     * takes effect, and what the batch withdraws or replaces stays as cover
     * until everything it adds is Ready or settled.
     */
    readonly edit: (edits: ReadonlyArray<Edit>) => Effect.Effect<EditHandle, SubmitError>;
    /** Releases a held `Manual` item to air at the next boundary. */
    readonly release: (key: ItemKey) => Effect.Effect<void, InvalidItem>;
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
     * request was outside H3's limits (`InvalidFiller`), or its scope closed
     * (`Closed`). A defect that stopped it, such as a throwing `filler.clip`,
     * stays a defect: this dies with it.
     */
    readonly failure: Effect.Effect<ReactorFailure | InvalidFiller>;
    readonly cleanup: Effect.Effect<Cleanup>;
  }
>()("reactor-effect-client/Playout") {}

/** A playout in the caller's scope; closing the scope retires every session. */
export const make: <R>(
  options: Options<R>,
) => Effect.Effect<Playout["Service"], never, R | Scope.Scope> = Runtime.make;

export const layer = <R>(options: Options<R>): Layer.Layer<Playout, never, R> =>
  Layer.effect(Playout, make(options));

/** The lineup: one lane of the application's items ahead of filler. */
export const lineup = (
  filler: NonNullable<Options["filler"]>,
): Pick<Options, "lanes" | "filler"> => ({
  lanes: [{ name: "line" }],
  filler,
});
