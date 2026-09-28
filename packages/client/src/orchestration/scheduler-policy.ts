import * as Option from "effect/Option";
import type { ClipId } from "./request.js";
import type { EngineState } from "./types.js";
import type { AsRunStatus, ItemKey } from "./scheduler.js";

export type DropReason = Extract<AsRunStatus, { readonly _tag: "Dropped" }>["reason"];

/** The actor's immutable projection of one caller item. */
export interface PlannedItem {
  readonly key: ItemKey;
  readonly lane: string;
  readonly admission: number;
  readonly phase: "Accepted" | "Building" | "Ready" | "Started" | "Terminal" | "Unknown";
  readonly clipId?: ClipId;
  readonly sessionId: string | undefined;
  readonly unknownSessionId: string | undefined;
  readonly notBeforeMs?: number;
  readonly startByMs?: number;
  /** A retryable admission refusal is not replayed in the same scheduler turn. */
  readonly retryAtMs?: number;
  readonly firm: boolean;
  readonly atMs?: number;
  readonly late: "nextBoundary" | "drop" | { readonly skipIfLaterThanMs: number };
  /** The group this item is a part of, and its place in that group. */
  readonly group?: { readonly key: ItemKey; readonly index: number };
  /** Set once the item's start was observed. */
  readonly startedAtMonoMs?: number;
  /** The item this one replaces; it keeps that item's place and builds first in its lane. */
  readonly replaces?: ItemKey;
  /** How many replacements this item is in its place: the tiebreak behind the one it replaces. */
  readonly generation?: number;
  /** Inserted beside an anchor: it keeps its place in a group, but its fate never breaks one. */
  readonly inserted?: boolean;
  /** The requested length, in seconds; the provider's actual length follows once built. */
  readonly seconds?: number;
  /** When its build command was sent, until the clip is Ready. */
  readonly dispatchedAtMs?: number;
  /** A `Manual` item not yet released: it waits behind everything and is not runway. */
  readonly held?: boolean;
  /** An `Asap` or released item: it airs ahead of everything waiting in every lane. */
  readonly asap?: boolean;
  /** Built continuing from the clip that airs just before it. */
  readonly continuity?: "previous";
  /**
   * The clip it was built to continue from, when that clip airs after its place: it would
   * not be Ready before the clip ahead of it ended. It waits behind that clip until it starts.
   */
  readonly follows?: ClipId;
}

/**
 * The indexes of a playing clip's cues that fall due by `untilMs` and have not fired, in
 * the order they fall: an offset from its start, or back from the end of its length.
 */
export const dueCues = (
  cues: ReadonlyArray<{ readonly from: "start" | "end"; readonly offsetMs: number }>,
  fired: ReadonlySet<number>,
  startedAtMs: number,
  durationSeconds: number,
  untilMs: number,
): ReadonlyArray<number> =>
  cues
    .map((cue, index) => ({
      index,
      atMs:
        cue.from === "start"
          ? startedAtMs + cue.offsetMs
          : startedAtMs + durationSeconds * 1000 - cue.offsetMs,
    }))
    .filter(({ index, atMs }) => !fired.has(index) && atMs <= untilMs)
    .sort((a, b) => a.atMs - b.atMs)
    .map(({ index }) => index);

/**
 * The requested length for the next filler clip. Before an anchor, equal clips that tile the
 * uncovered gap within the provider's lengths, so filler ends on the anchor; otherwise the
 * shortest clip, which keeps boundaries, and so reactions, frequent. `ratio` is a built
 * clip's actual over requested length.
 */
export const fillLength = (
  uncoveredSeconds: number,
  lengths: { readonly min: number; readonly max: number },
  ratio: number,
): number => {
  const clamp = (requested: number) => Math.min(lengths.max, Math.max(lengths.min, requested));
  if (!(uncoveredSeconds > 0)) return lengths.min;
  const pieces = Math.max(1, Math.ceil(uncoveredSeconds / (lengths.max * ratio)));
  return clamp(uncoveredSeconds / pieces / ratio);
};

/** A held clip that would be next within this many seconds is removed and rebuilt. */
const exposureMarginSeconds = 1.5;

/** What the scheduler has measured of its provider, from its own builds. */
export interface Estimates {
  /** Seconds of build per requested second of clip, once a few builds were measured. */
  readonly build: { readonly median: number; readonly p95: number } | undefined;
  /** The same for clips built continuing from another, which take longer. */
  readonly continuedBuild?: { readonly median: number; readonly p95: number } | undefined;
  /** A built clip's actual length over its requested length. */
  readonly length: number;
}

/** Enough measured builds to act on. */
export const minimumSamples = 3;

const quantile = (values: ReadonlyArray<number>, q: number): number => {
  const sorted = [...values].sort((a, b) => a - b);
  return sorted[Math.min(sorted.length - 1, Math.floor(q * sorted.length))] ?? 0;
};

const spreadOf = (samples: ReadonlyArray<number>) =>
  samples.length < minimumSamples
    ? undefined
    : { median: quantile(samples, 0.5), p95: quantile(samples, 0.95) };

/**
 * Estimates from recent samples: build seconds per requested second, of independent and of
 * continued clips, and actual over requested length.
 */
export const estimatesFrom = (
  build: ReadonlyArray<number>,
  length: ReadonlyArray<number>,
  continued: ReadonlyArray<number> = [],
): Estimates => {
  const continuedBuild = spreadOf(continued);
  return {
    build: spreadOf(build),
    ...(continuedBuild === undefined ? {} : { continuedBuild }),
    length: length.length === 0 ? 1 : quantile(length, 0.5),
  };
};

/**
 * Until a continued build is measured, one is projected at this multiple of an independent
 * build. On hosted H3 a continued 5 s clip took 5.45 s to build, against 2.1 to 2.2 s for
 * independent ones (the 0.7.0 `scheduler-edits` run).
 */
const continuedBuildFactor = 2.5;

/**
 * Build seconds per requested second that a continued build is projected at, erring long:
 * the p95 of measured continued builds, or their longest while there are fewer than three;
 * failing those, the same of independent builds times `continuedBuildFactor`. Undefined
 * before any build was measured.
 */
export const continuedBuildRate = (
  build: ReadonlyArray<number>,
  continued: ReadonlyArray<number>,
): number | undefined => {
  const long = (samples: ReadonlyArray<number>) =>
    samples.length >= minimumSamples ? quantile(samples, 0.95) : Math.max(...samples);
  if (continued.length > 0) return long(continued);
  if (build.length > 0) return long(build) * continuedBuildFactor;
  return undefined;
};

/**
 * Seconds added to the measured p95 build time before a refill counts as early enough: the
 * scheduler's 100 ms turn, a command round trip, and the provider's delay in starting the
 * next clip (30 to 110 ms on hosted H3).
 */
const lookaheadMarginSeconds = 1;

/**
 * An edit batch that has not taken effect yet. Until it does, what it withdraws is only
 * cover: it airs after everything else in its lane, so any clip the batch adds goes first.
 */
export interface PendingBatch {
  readonly id: number;
  /** The batch takes effect once each of these is Ready, started or settled. */
  readonly waitFor: ReadonlyArray<ItemKey>;
  /** The items it withdraws once it takes effect, and why. */
  readonly targets: ReadonlyArray<PolicyWithdrawal>;
}

/** Items that pending batches withdraw. */
export const supersededBy = (batches: ReadonlyArray<PendingBatch>): ReadonlySet<ItemKey> =>
  new Set(batches.flatMap((batch) => batch.targets.map((target) => target.key)));

export type OwnedClip =
  | { readonly _tag: "Item"; readonly key: ItemKey }
  | { readonly _tag: "Filler"; readonly index: number; readonly sessionId: string | undefined };

export interface PolicySnapshot {
  readonly engine: EngineState;
  readonly items: ReadonlyArray<PlannedItem>;
  readonly owned: ReadonlyMap<ClipId, OwnedClip>;
  readonly lanes: ReadonlyArray<string>;
  readonly nowMs: number;
  readonly playingStartedMs: ReadonlyMap<ClipId, number>;
  readonly floorSeconds: number;
  readonly targetSeconds: number;
  readonly refillActive: boolean;
  readonly maxBuildsInFlight: number;
  /** New admissions are open: the scheduler is neither draining nor closed. */
  readonly accepting: boolean;
  /** How a requested drain finishes, once one is requested. */
  readonly drain: "playing" | "accepted" | undefined;
  readonly fillerRetryAtMs: number;
  /** Uncertain filler reservations on the preferred source, including unfenced sends. */
  readonly unknownFillerCount: number;
  /** A refused move is retried only after the Ready snapshot changes. */
  readonly blockedMove: string | undefined;
  /** Items with a withdrawal already requested. */
  readonly withdrawing: ReadonlySet<ItemKey>;
  /** Items whose build command has been sent and has not returned. */
  readonly dispatched: ReadonlySet<ItemKey>;
  /** The first failed or dropped part of each broken group. */
  readonly brokenGroups: ReadonlyMap<ItemKey, number>;
  readonly batches: ReadonlyArray<PendingBatch>;
  readonly estimates: Estimates;
  /** What a continued build is projected at, per requested second; see `continuedBuildRate`. */
  readonly continuedBuildRate: number | undefined;
  /** When the filler build in flight on the preferred source was sent, if one is. */
  readonly fillerDispatchedAtMs: number | undefined;
  /** The requested length of the next filler clip, if known. */
  readonly fillerSeconds: number | undefined;
  /** Lanes whose Ready items cut a playing clip that ranks below them. */
  readonly cutLanes: ReadonlySet<string>;
  /** The requested clip lengths filler may take, in seconds. */
  readonly fillLengths: { readonly min: number; readonly max: number };
  /** A playing clip already cut, or whose cut was refused, is not cut again. */
  readonly blockedCut: ClipId | undefined;
}

/** A clip ending within this many seconds is left to end rather than cut. */
const cutMarginSeconds = 1;

export type PolicyAction =
  | { readonly _tag: "DeferAt"; readonly key: ItemKey; readonly clipId: ClipId }
  | {
      readonly _tag: "Build";
      readonly key: ItemKey;
      /** The generated clip it continues from, for an item that asks for continuity. */
      readonly continueFrom?: ClipId;
      /** Set when that clip airs after the item's place, so the item must wait behind it. */
      readonly follows?: ClipId;
    }
  | { readonly _tag: "Cut"; readonly clipId: ClipId }
  | {
      readonly _tag: "BuildFiller";
      readonly targetSeconds: number;
      /** The requested length suggested for this filler clip. */
      readonly durationSeconds: number;
    }
  | { readonly _tag: "Order"; readonly clipId: ClipId; readonly position: number };

export interface PolicyWithdrawal {
  readonly key: ItemKey;
  readonly reason: DropReason;
}

export interface PolicyDecision {
  readonly runwaySeconds: number;
  readonly refillActive: boolean;
  /** Every item the plan no longer wants, with the reason as-run records; the first reason wins. */
  readonly withdraw: ReadonlyArray<PolicyWithdrawal>;
  /** Filler clips the plan no longer wants. */
  readonly withdrawFiller: ReadonlyArray<ClipId>;
  /** Edit batches to take effect now; the plan is read again once they have. */
  readonly commit: ReadonlyArray<number>;
  /** The one provider command to send when none is in flight. */
  readonly action?: PolicyAction;
}

const preferredSession = (state: EngineState): string | undefined =>
  Option.getOrUndefined(state.preferredSessionId);

/**
 * When the playing clip started on the monotonic clock. Engine state can show a new clip
 * playing before its Started event is handled, so the state's own observation fills in.
 */
const playingSince = (
  playing: Option.Option.Value<EngineState["playing"]> | undefined,
  playingStartedMs: ReadonlyMap<ClipId, number>,
): number | undefined =>
  playing === undefined
    ? undefined
    : (playingStartedMs.get(playing.clipId) ?? playing.startedAtMonotonicMillis);

/** Ready material on a retiring source is not runway for the replacement. */
export const runwaySeconds = (
  state: EngineState,
  nowMs: number,
  playingStartedMs: ReadonlyMap<ClipId, number>,
  owned: ReadonlyMap<ClipId, OwnedClip>,
  items: ReadonlyArray<PlannedItem>,
): number => {
  const preferred = preferredSession(state);
  const playing = Option.getOrUndefined(state.playing);
  const record = playing === undefined ? undefined : Option.getOrUndefined(playing.record);
  const started = playingSince(playing, playingStartedMs);
  const rest =
    record === undefined || started === undefined
      ? 0
      : Math.max(0, record.durationSeconds - (nowMs - started) / 1000);
  return (
    rest +
    state.ready
      .filter((clip) => {
        if (preferred !== undefined && clip.sessionId !== preferred) return false;
        const owner = owned.get(clip.clipId);
        if (owner?._tag !== "Item") return true;
        const item = items.find((candidate) => candidate.key === owner.key);
        return item?.held !== true && (item?.atMs === undefined || item.atMs <= nowMs);
      })
      .reduce((seconds, clip) => seconds + clip.durationSeconds, 0)
  );
};

type BuildKey = Pick<
  PlannedItem,
  "lane" | "admission" | "asap" | "held" | "replaces" | "group" | "startByMs"
>;

/**
 * Which of two waiting items the build slot takes first: Asap, then everything but held
 * items, then lane, a replacement racing its item, a group already under way, the earlier
 * deadline, and admission.
 */
const buildOrder =
  (lanes: ReadonlyArray<string>) =>
  (a: BuildKey, b: BuildKey): number => {
    const asap = Number(b.asap === true) - Number(a.asap === true);
    const held = Number(a.held === true) - Number(b.held === true);
    const lane = lanes.indexOf(a.lane) - lanes.indexOf(b.lane);
    // A replacement races the clip it replaces, so it builds first in its lane.
    const replacing = Number(b.replaces !== undefined) - Number(a.replaces !== undefined);
    // A deadline may move a whole group ahead in its lane, never into one being built.
    const open =
      Number(b.group !== undefined && b.group.index > 0) -
      Number(a.group !== undefined && a.group.index > 0);
    return (
      asap ||
      held ||
      lane ||
      replacing ||
      open ||
      (a.startByMs ?? Infinity) - (b.startByMs ?? Infinity) ||
      a.admission - b.admission
    );
  };

/** Lexicographic place in a session's Ready order; lower airs first. */
export type Rank = readonly [number, number, number, number];

const compareRank = (left: Rank, right: Rank): number =>
  left[0] - right[0] || left[1] - right[1] || left[2] - right[2] || left[3] - right[3];

/** Where a place sits among a group's parts, and whether a part must wait for the one before it. */
const ordering = (
  items: ReadonlyArray<PlannedItem>,
  lanes: ReadonlyArray<string>,
  nowMs: number,
  superseded: ReadonlySet<ItemKey>,
  owned: ReadonlyMap<ClipId, OwnedClip>,
  /** Ready clips that have not started: an item following one of them waits behind it. */
  waiting: ReadonlySet<ClipId>,
) => {
  // A group's parts in order. A part already pruned has settled, so it counts as admitted.
  const groups = new Map<ItemKey, PlannedItem[]>();
  for (const item of items)
    if (item.group !== undefined)
      groups.set(item.group.key, [...(groups.get(item.group.key) ?? []), item]);
  const previousAdmitted = (item: PlannedItem): boolean => {
    if (item.group === undefined) return true;
    const index = item.group.index;
    const parts = groups.get(item.group.key) ?? [];
    const previous = Math.max(
      ...parts.map((part) => part.group?.index ?? 0).filter((value) => value < index),
    );
    // The previous place may hold a part and its replacement; either one unadmitted holds this part.
    return !parts.some((part) => part.group?.index === previous && part.phase === "Accepted");
  };
  // Once a group airs, its remaining parts stay ahead of the rest of their lane. A group
  // whose first part was pruned has settled it, and a settled first part that did not
  // air withdrew the others.
  const begun = (item: Pick<PlannedItem, "group"> | undefined): boolean => {
    if (item?.group === undefined) return false;
    const parts = groups.get(item.group.key) ?? [];
    return (
      parts.some((part) => part.startedAtMonoMs !== undefined) ||
      Math.min(...parts.map((part) => part.group?.index ?? 0)) > 0
    );
  };
  // Within its place, a Ready replacement airs ahead of the item it replaces.
  const byKey = new Map(items.map((item) => [item.key, item]));
  /**
   * The live item a replacement takes the place of. Replacing a replacement that was never
   * built drops that one, so the chain leads back to the item still in the place.
   */
  const replacedOf = (item: PlannedItem): PlannedItem | undefined => {
    let old = item.replaces === undefined ? undefined : byKey.get(item.replaces);
    while (old?.phase === "Terminal" && old.replaces !== undefined) old = byKey.get(old.replaces);
    return old;
  };
  const replacedReady = new Set(
    items.flatMap((item) => {
      const old = item.phase === "Ready" || item.phase === "Started" ? replacedOf(item) : undefined;
      return old === undefined ? [] : [old.key];
    }),
  );
  const rankItem = (
    item:
      | (Pick<PlannedItem, "lane" | "admission" | "group" | "atMs" | "generation"> & {
          readonly key?: ItemKey;
          readonly held?: boolean;
          readonly asap?: boolean;
          readonly follows?: ClipId;
        })
      | undefined,
  ): Rank => {
    const generation =
      item?.key !== undefined && replacedReady.has(item.key) ? Infinity : (item?.generation ?? 0);
    if (item?.held === true || (item?.atMs !== undefined && nowMs < item.atMs))
      return [lanes.length + 1, 1, item.admission, generation];
    // It continues the clip it follows, so it airs right behind that clip until that starts.
    if (item?.follows !== undefined && waiting.has(item.follows)) {
      const behind = rankClip(item.follows);
      return [behind[0], behind[1], behind[2], behind[3] + 0.5];
    }
    if (item?.asap === true) return [-0.5, 0, item.admission, generation];
    return [
      Math.max(0, lanes.indexOf(item?.lane ?? "")),
      item?.key !== undefined && superseded.has(item.key) ? 2 : begun(item) ? 0 : 1,
      item?.admission ?? 0,
      generation,
    ];
  };
  const rankClip = (clipId: ClipId): Rank => {
    const clip = owned.get(clipId);
    if (clip === undefined) return [-1, 0, 0, 0];
    if (clip._tag === "Filler") return [lanes.length, 1, clip.index, 0];
    return rankItem(items.find((value) => value.key === clip.key));
  };
  return { previousAdmitted, begun, rankItem, rankClip, replacedOf };
};

/**
 * An accepted drain still owes air to every line not yet Ready, and to a Ready line held
 * for a future At anchor, which runway must reach. Filler is the only material that can
 * cover either, so it keeps playing and refilling until nothing accepted can still leave
 * the host frozen. An Unknown item does not count: without a fenced source it can stay
 * open until the scheduler closes, and filler for it would never stop.
 */
const fillerHeldFor = (snapshot: PolicySnapshot): boolean =>
  snapshot.drain === "accepted" &&
  snapshot.items.some(
    (item) =>
      item.held !== true &&
      (item.phase === "Accepted" ||
        item.phase === "Building" ||
        (item.phase === "Ready" && item.atMs !== undefined && item.atMs > snapshot.nowMs)),
  );

export interface ProjectionView {
  readonly engine: EngineState;
  readonly items: ReadonlyArray<PlannedItem>;
  readonly owned: ReadonlyMap<ClipId, OwnedClip>;
  readonly lanes: ReadonlyArray<string>;
  readonly playingStartedMs: ReadonlyMap<ClipId, number>;
  readonly nowMs: number;
  readonly batches: ReadonlyArray<PendingBatch>;
  readonly estimates: Estimates;
  readonly fillerDispatchedAtMs: number | undefined;
  readonly fillerSeconds: number | undefined;
  /** Items being withdrawn, which count for nothing ahead. */
  readonly withdrawing?: ReadonlySet<ItemKey>;
}

/** The place a projection is for: a new submission's is the end of its lane. */
export type ProjectedPlace = Pick<PlannedItem, "lane" | "admission" | "group"> & {
  readonly key?: ItemKey;
  readonly seconds?: number;
  readonly startByMs?: number;
  readonly asap?: boolean;
  readonly held?: boolean;
  readonly replaces?: ItemKey;
};

/**
 * When a clip could start at the earliest if it took `place`, optimistically, so a refusal
 * means it cannot make it: after the rest of the playing clip and every Ready clip on the
 * preferred source ranked ahead, and after its own build, once builds have been measured.
 * The build waits for the one in flight and for every build that must go first: higher
 * lanes, a replacement or a group already under way in its lane, and earlier deadlines.
 */
export const projectedStartMs = (view: ProjectionView, place: ProjectedPlace): number => {
  const { engine, owned, nowMs } = view;
  const playing = Option.getOrUndefined(engine.playing);
  const playingRecord = playing === undefined ? undefined : Option.getOrUndefined(playing.record);
  const started = playingSince(playing, view.playingStartedMs);
  const restMs =
    playingRecord === undefined || started === undefined
      ? 0
      : Math.max(0, playingRecord.durationSeconds * 1000 - (nowMs - started));
  const { rankItem, rankClip, previousAdmitted } = ordering(
    view.items,
    view.lanes,
    nowMs,
    supersededBy(view.batches),
    owned,
    new Set(engine.ready.map((clip) => clip.clipId)),
  );
  const rank = rankItem(place);
  const preferred = preferredSession(engine);
  const withdrawing = view.withdrawing ?? new Set<ItemKey>();
  const aheadMs = engine.ready.reduce((total, record) => {
    if (record.sessionId !== preferred || compareRank(rankClip(record.clipId), rank) >= 0)
      return total;
    const owner = owned.get(record.clipId);
    if (owner?._tag === "Item" && withdrawing.has(owner.key)) return total;
    return total + record.durationSeconds * 1000;
  }, 0);
  const playable = nowMs + restMs + aheadMs;
  const perSecond = view.estimates.build?.median;
  if (perSecond === undefined || place.seconds === undefined) return playable;
  const buildMs = (seconds: number) => seconds * perSecond * 1000;
  // The build slot is busy until the build in flight is done.
  const inFlight = [
    ...view.items.flatMap((item) =>
      item.phase === "Building" && item.dispatchedAtMs !== undefined && item.key !== place.key
        ? [item.dispatchedAtMs + buildMs(item.seconds ?? 0)]
        : [],
    ),
    ...(view.fillerDispatchedAtMs === undefined || view.fillerSeconds === undefined
      ? []
      : [view.fillerDispatchedAtMs + buildMs(view.fillerSeconds)]),
  ];
  // Only builds that can go now and that the build order puts first count, so the
  // projection stays optimistic: not held, future or not-yet items, nor a group's parts
  // after one not yet built.
  const order = buildOrder(view.lanes);
  const first = view.items
    .filter(
      (item) =>
        item.phase === "Accepted" &&
        item.key !== place.key &&
        !withdrawing.has(item.key) &&
        item.held !== true &&
        (item.atMs === undefined || item.atMs <= nowMs) &&
        (item.notBeforeMs === undefined || item.notBeforeMs <= nowMs) &&
        previousAdmitted(item) &&
        order(item, place) < 0,
    )
    .reduce((total, item) => total + buildMs(item.seconds ?? 0), 0);
  const built = Math.max(nowMs, ...inFlight) + first + buildMs(place.seconds);
  return Math.max(playable, built);
};

/**
 * The pure policy: every withdrawal the plan wants now, and at most one serialized provider
 * command. The actor re-reads Engine after each applied command.
 */
export const plan = (snapshot: PolicySnapshot): PolicyDecision => {
  const { engine, items, nowMs, owned } = snapshot;
  const { previousAdmitted, rankClip, rankItem, replacedOf } = ordering(
    items,
    snapshot.lanes,
    nowMs,
    supersededBy(snapshot.batches),
    owned,
    new Set(engine.ready.map((clip) => clip.clipId)),
  );
  /**
   * The clip that airs just before `item` if its continued build starts now: `predecessor`,
   * unless that build is projected to be Ready only after `predecessor` ends. The clips Ready
   * behind it then air first, and the item continues from, and follows, the one that will be
   * playing when it is Ready, or the last of them. A clip it continues from must already be
   * generated, so none that is still to be built is chosen.
   */
  const airsBefore = (
    item: PlannedItem,
    predecessor: ClipId,
    gone: ReadonlySet<ItemKey>,
  ): { readonly clipId: ClipId; readonly follows?: ClipId } => {
    const rate = snapshot.continuedBuildRate;
    if (rate === undefined || item.seconds === undefined) return { clipId: predecessor };
    const readyAtMs = nowMs + (rate * item.seconds + lookaheadMarginSeconds) * 1000;
    const playing = Option.getOrUndefined(engine.playing);
    const record = playing === undefined ? undefined : Option.getOrUndefined(playing.record);
    const since = playingSince(playing, snapshot.playingStartedMs);
    if (playing !== undefined && (record === undefined || since === undefined))
      return { clipId: predecessor };
    let endMs =
      record === undefined || since === undefined ? nowMs : since + record.durationSeconds * 1000;
    // The preferred source's Ready clips in the order they air; held and future ones wait.
    const queued = engine.ready
      .filter((clip) => {
        const owner = owned.get(clip.clipId);
        return (
          clip.sessionId === preferredSession(engine) &&
          !(owner?._tag === "Item" && gone.has(owner.key)) &&
          rankClip(clip.clipId)[0] <= snapshot.lanes.length
        );
      })
      .sort((a, b) => compareRank(rankClip(a.clipId), rankClip(b.clipId)));
    const after =
      playing?.clipId === predecessor
        ? 0
        : queued.findIndex((clip) => clip.clipId === predecessor) + 1;
    if (after === 0 && playing?.clipId !== predecessor) return { clipId: predecessor };
    for (const clip of queued.slice(0, after)) endMs += clip.durationSeconds * 1000;
    if (readyAtMs <= endMs) return { clipId: predecessor };
    const behind = queued.slice(after);
    for (const [index, clip] of behind.entries()) {
      endMs += clip.durationSeconds * 1000;
      if (readyAtMs <= endMs || index === behind.length - 1)
        return { clipId: clip.clipId, follows: clip.clipId };
    }
    return { clipId: predecessor };
  };
  /**
   * What an item continuing from its predecessor continues from: the Ready clip or waiting
   * item that airs just before it on the preferred source, else the clip playing there, or
   * the one that will be by the time it is built (`airsBefore`). `wait` while that predecessor
   * is not built yet; no clip when there is none, or when the provider no longer offers it
   * for continuation.
   */
  const continuation = (
    item: PlannedItem,
  ):
    | { readonly _tag: "wait" }
    | { readonly _tag: "from"; readonly clipId?: ClipId; readonly follows?: ClipId } => {
    const place = rankItem(item);
    // What will not air before it is no predecessor: the item it replaces, and what a batch
    // or a withdrawal is taking off.
    const gone = new Set<ItemKey>([...supersededBy(snapshot.batches), ...snapshot.withdrawing]);
    const replaced = replacedOf(item);
    if (replaced !== undefined) gone.add(replaced.key);
    let best: { readonly rank: Rank; readonly clipId?: ClipId } | undefined;
    const consider = (rank: Rank, clipId: ClipId | undefined) => {
      if (compareRank(rank, place) < 0 && (best === undefined || compareRank(rank, best.rank) > 0))
        best = clipId === undefined ? { rank } : { rank, clipId };
    };
    for (const clip of engine.ready) {
      const owner = owned.get(clip.clipId);
      if (
        clip.sessionId === preferredSession(engine) &&
        !(owner?._tag === "Item" && gone.has(owner.key))
      )
        consider(rankClip(clip.clipId), clip.clipId);
    }
    for (const other of items)
      if (
        other.key !== item.key &&
        !gone.has(other.key) &&
        (other.phase === "Accepted" || other.phase === "Building" || other.phase === "Unknown")
      )
        consider(rankItem(other), undefined);
    if (best !== undefined && best.clipId === undefined) return { _tag: "wait" };
    const predecessor = best?.clipId ?? Option.getOrUndefined(engine.playing)?.clipId;
    if (predecessor === undefined) return { _tag: "from" };
    const from = airsBefore(item, predecessor, gone);
    return engine.continuable.includes(from.clipId) ? { _tag: "from", ...from } : { _tag: "from" };
  };
  const runway = runwaySeconds(engine, nowMs, snapshot.playingStartedMs, owned, items);
  const nextAnchorMs = items
    .filter((item) => item.phase !== "Terminal" && item.atMs !== undefined && item.atMs > nowMs)
    .reduce((earliest, item) => Math.min(earliest, item.atMs ?? Infinity), Infinity);
  const anchorGapSeconds = nextAnchorMs === Infinity ? 0 : (nextAnchorMs - nowMs) / 1000;
  // A nonzero floor covers at least one measured p95 build of the next filler clip, so a
  // refill started at the floor is Ready before the picture runs out. A zero floor keeps
  // filler off.
  const p95 = snapshot.estimates.build?.p95;
  const floorSeconds =
    snapshot.floorSeconds > 0 && p95 !== undefined && snapshot.fillerSeconds !== undefined
      ? Math.max(snapshot.floorSeconds, p95 * snapshot.fillerSeconds + lookaheadMarginSeconds)
      : snapshot.floorSeconds;
  const targetSeconds = Math.max(snapshot.targetSeconds, floorSeconds);
  const fillTarget = Math.max(targetSeconds, anchorGapSeconds);
  const filling =
    anchorGapSeconds > runway ||
    (snapshot.refillActive ? runway < targetSeconds : runway < floorSeconds);
  const playingId = Option.getOrUndefined(engine.playing)?.clipId;
  const fillerHeld = fillerHeldFor(snapshot);

  const withdraw: PolicyWithdrawal[] = [];
  const listed = new Set<ItemKey>();
  const drop = (item: PlannedItem, reason: DropReason): void => {
    if (snapshot.withdrawing.has(item.key) || listed.has(item.key)) return;
    listed.add(item.key);
    withdraw.push({ key: item.key, reason });
  };
  const waiting = (item: PlannedItem): boolean =>
    item.phase === "Accepted" ||
    item.phase === "Building" ||
    item.phase === "Ready" ||
    item.phase === "Unknown";
  // Sweep every status; expiry behind a nonexpired head must not be stranded.
  for (const item of items) {
    if (item.phase !== "Accepted" && item.phase !== "Building" && item.phase !== "Ready") continue;
    const lateBy = item.atMs === undefined ? undefined : nowMs - item.atMs;
    const atExpired =
      lateBy === undefined || item.late === "nextBoundary"
        ? false
        : item.late === "drop"
          ? lateBy > 0
          : lateBy > item.late.skipIfLaterThanMs;
    if ((item.firm && item.startByMs !== undefined && nowMs > item.startByMs) || atExpired)
      drop(item, "late");
  }
  // A firm item that cannot start before its deadline is dropped before it takes the build
  // slot, once builds have been measured.
  if (snapshot.estimates.build !== undefined)
    for (const item of items)
      if (
        item.firm &&
        item.phase === "Accepted" &&
        item.startByMs !== undefined &&
        !snapshot.dispatched.has(item.key) &&
        projectedStartMs(snapshot, item) > item.startByMs
      )
        drop(item, "late");
  // A part that failed or was dropped withdraws the parts after it.
  for (const item of items) {
    const brokenAt =
      item.group === undefined ? undefined : snapshot.brokenGroups.get(item.group.key);
    if (brokenAt !== undefined && item.group!.index > brokenAt && waiting(item))
      drop(item, "withdrawn");
  }
  // A replacement takes the place once Ready, or at once if nothing was built for the item it
  // replaces; a batch's replacement waits for its batch. If the replaced item starts first,
  // the replacement goes.
  const pendingReplacements = new Set(snapshot.batches.flatMap((batch) => batch.waitFor));
  for (const item of items) {
    if (item.replaces === undefined || item.phase === "Terminal") continue;
    const old = replacedOf(item);
    if (old === undefined || old.phase === "Terminal") continue;
    if (old.phase === "Started") {
      if (item.phase !== "Started") drop(item, "withdrawn");
    } else if (
      // A batch's Ready replacement waits for its batch, but one that has started takes
      // the place at once, so the item it replaces never follows it.
      (item.phase === "Ready" && !pendingReplacements.has(item.key)) ||
      item.phase === "Started" ||
      (old.phase === "Accepted" && !snapshot.dispatched.has(old.key))
    )
      drop(old, "replaced");
  }
  // A batch takes effect once everything it adds is Ready or has settled. Until then what it
  // withdraws keeps its place, except an item nothing was built for, which covers nothing.
  const commit: number[] = [];
  for (const batch of snapshot.batches) {
    const find = (key: ItemKey) => items.find((item) => item.key === key);
    if (
      batch.waitFor.every((key) => {
        const phase = find(key)?.phase;
        return (
          phase === undefined || phase === "Ready" || phase === "Started" || phase === "Terminal"
        );
      })
    )
      commit.push(batch.id);
    else
      for (const { key, reason } of batch.targets) {
        const target = find(key);
        if (target?.phase === "Accepted" && !snapshot.dispatched.has(key)) drop(target, reason);
      }
  }
  // A held item cannot air without a release a drain will not wait for.
  if (snapshot.drain !== undefined)
    for (const item of items) if (item.held === true && waiting(item)) drop(item, "withdrawn");
  // A drain that finishes only the playing clip withdraws everything waiting.
  if (snapshot.drain === "playing")
    for (const item of items)
      if (waiting(item) && (item.clipId === undefined || item.clipId !== playingId))
        drop(item, "withdrawn");

  const withdrawFiller: ClipId[] = [];
  const preferred = preferredSession(engine);
  const retiring = Option.getOrUndefined(engine.retiringSessionId);
  if (
    preferred !== undefined &&
    retiring !== undefined &&
    engine.handoffReady === true &&
    !engine.queued.some((clip) => clip.sessionId === retiring) &&
    Option.getOrUndefined(engine.building)?.record.sessionId !== retiring &&
    !items.some(
      (item) =>
        (item.phase === "Building" || item.phase === "Unknown") &&
        (item.sessionId ?? item.unknownSessionId) === retiring,
    ) &&
    engine.ready.some(
      (clip) => clip.sessionId === preferred && owned.get(clip.clipId)?._tag === "Item",
    )
  )
    for (const clip of engine.ready)
      if (clip.sessionId === retiring && owned.get(clip.clipId)?._tag === "Filler")
        withdrawFiller.push(clip.clipId);
  // A drain withdraws filler once nothing accepted still needs it to cover the wait.
  if (snapshot.drain !== undefined && !fillerHeld)
    for (const [clipId, owner] of owned)
      if (owner._tag === "Filler" && clipId !== playingId && !withdrawFiller.includes(clipId))
        withdrawFiller.push(clipId);
  const base = { runwaySeconds: runway, refillActive: filling, withdraw, withdrawFiller, commit };

  // Physical sources are independent queues. A move never ranks across them.
  let offset = 0;
  for (const session of engine.ready.length === 0 ? [] : engine.sessions) {
    const actual = engine.ready.filter((clip) => clip.sessionId === session.sessionId);
    if (session.availability !== "Ready") {
      offset += actual.length;
      continue;
    }
    if (actual.length === 0) continue;
    const desired = [...actual].sort((a, b) => compareRank(rankClip(a.clipId), rankClip(b.clipId)));
    for (let index = 0; index < actual.length; index++) {
      if (actual[index]?.clipId === desired[index]?.clipId) continue;
      const wanted = desired[index]!;
      if (owned.has(wanted.clipId)) {
        const action = { _tag: "Order", clipId: wanted.clipId, position: offset + index } as const;
        const signature =
          engine.ready.map((record) => record.clipId).join(",") +
          ":" +
          action.clipId +
          ":" +
          action.position;
        if (snapshot.blockedMove !== signature) return { ...base, action };
      }
      const displaced = actual[index]!;
      if (owned.has(displaced.clipId)) {
        const action = {
          _tag: "Order",
          clipId: displaced.clipId,
          position: offset + desired.findIndex((clip) => clip.clipId === displaced.clipId),
        } as const;
        const signature =
          engine.ready.map((record) => record.clipId).join(",") +
          ":" +
          action.clipId +
          ":" +
          action.position;
        if (snapshot.blockedMove !== signature) return { ...base, action };
      }
    }
    offset += actual.length;
  }

  // A cut lane's Ready item at the front of its session cuts the playing clip there, once
  // the provider has it at the front.
  if (snapshot.cutLanes.size > 0) {
    const current = Option.getOrUndefined(engine.playing);
    const record = current === undefined ? undefined : Option.getOrUndefined(current.record);
    const since = playingSince(current, snapshot.playingStartedMs);
    const front =
      record === undefined
        ? undefined
        : engine.ready.find((clip) => clip.sessionId === record.sessionId);
    const owner = front === undefined ? undefined : owned.get(front.clipId);
    const cutter =
      owner?._tag === "Item" ? items.find((item) => item.key === owner.key) : undefined;
    // Only filler or a clip of a strictly lower lane is cut, never a clip of the cutter's
    // own lane, whatever its rank.
    const playingOwner = current === undefined ? undefined : owned.get(current.clipId);
    const playingItem =
      playingOwner?._tag === "Item"
        ? items.find((item) => item.key === playingOwner.key)
        : undefined;
    const lower =
      cutter !== undefined &&
      (playingOwner?._tag === "Filler" ||
        (playingItem !== undefined &&
          snapshot.lanes.indexOf(playingItem.lane) > snapshot.lanes.indexOf(cutter.lane)));
    if (
      current !== undefined &&
      record !== undefined &&
      since !== undefined &&
      front !== undefined &&
      cutter !== undefined &&
      snapshot.cutLanes.has(cutter.lane) &&
      snapshot.blockedCut !== current.clipId &&
      lower &&
      record.durationSeconds - (nowMs - since) / 1000 > cutMarginSeconds
    )
      return { ...base, action: { _tag: "Cut", clipId: current.clipId } };
  }

  // Reorder movable Ready runway before treating a future At clip as exposed.
  // A wall-clock correction can otherwise require removing and rebuilding it.
  const playing = Option.getOrUndefined(engine.playing);
  const playingRecord = playing === undefined ? undefined : Option.getOrUndefined(playing.record);
  const playingStarted = playingSince(playing, snapshot.playingStartedMs);
  const playingRest =
    playingRecord === undefined || playingStarted === undefined
      ? 0
      : Math.max(0, playingRecord.durationSeconds - (nowMs - playingStarted) / 1000);
  const exposedAnchor = items.find((item) => {
    const held = item.held === true;
    if (
      item.phase !== "Ready" ||
      item.clipId === undefined ||
      (!held && (item.atMs === undefined || item.atMs <= nowMs))
    )
      return false;
    const index = engine.ready.findIndex((clip) => clip.clipId === item.clipId);
    if (index < 0) return false;
    const sessionId = engine.ready[index]!.sessionId;
    const ahead = engine.ready
      .slice(0, index)
      .filter((clip) => {
        if (clip.sessionId !== sessionId) return false;
        const owner = owned.get(clip.clipId);
        if (owner?._tag !== "Item") return true;
        const prior = items.find((candidate) => candidate.key === owner.key);
        return prior?.held !== true && (prior?.atMs === undefined || prior.atMs <= nowMs);
      })
      .reduce((seconds, clip) => seconds + clip.durationSeconds, 0);
    return held
      ? ahead === 0 && playingRest < exposureMarginSeconds
      : playingRest + ahead < (item.atMs! - nowMs) / 1000;
  });
  if (exposedAnchor?.clipId !== undefined)
    return {
      ...base,
      action: { _tag: "DeferAt", key: exposedAnchor.key, clipId: exposedAnchor.clipId },
    };

  const preferredAvailability = engine.sessions.find(
    (session) => session.sessionId === preferred,
  )?.availability;
  if (!(snapshot.accepting || snapshot.drain === "accepted") || preferredAvailability !== "Ready")
    return base;
  const activeFiller = [...owned.entries()].filter(
    ([clipId, owner]) =>
      owner._tag === "Filler" &&
      (owner.sessionId === undefined || owner.sessionId === preferred) &&
      !engine.ready.some((clip) => clip.clipId === clipId) &&
      Option.getOrUndefined(engine.playing)?.clipId !== clipId &&
      !engine.failed.includes(clipId),
  ).length;
  const inflight =
    items.filter(
      (item) =>
        (item.phase === "Building" || item.phase === "Unknown") &&
        (item.sessionId ?? item.unknownSessionId ?? preferred) === preferred,
    ).length +
    activeFiller +
    snapshot.unknownFillerCount;
  if (inflight >= snapshot.maxBuildsInFlight) return base;
  const eligible = items
    .filter(
      (item) =>
        item.phase === "Accepted" &&
        !listed.has(item.key) &&
        !snapshot.withdrawing.has(item.key) &&
        previousAdmitted(item) &&
        (item.retryAtMs === undefined || nowMs >= item.retryAtMs) &&
        (item.notBeforeMs === undefined || nowMs >= item.notBeforeMs) &&
        // Autoplay cannot hold a Ready clip. Admit a future anchor only once
        // known material ahead of it covers the time until that anchor.
        (item.atMs === undefined || item.atMs <= nowMs || runway >= (item.atMs - nowMs) / 1000) &&
        // A held item is built ahead only while filler keeps the runway at its floor.
        (item.held !== true || (floorSeconds > 0 && runway >= floorSeconds)) &&
        // A continuing clip waits for the clip it continues from to be built.
        (item.continuity !== "previous" || continuation(item)._tag !== "wait"),
    )
    .sort(buildOrder(snapshot.lanes));
  const next = eligible[0];
  if (next !== undefined) {
    const from = next.continuity === "previous" ? continuation(next) : undefined;
    return {
      ...base,
      action:
        from?._tag === "from" && from.clipId !== undefined
          ? {
              _tag: "Build",
              key: next.key,
              continueFrom: from.clipId,
              ...(from.follows === undefined ? {} : { follows: from.follows }),
            }
          : { _tag: "Build", key: next.key },
    };
  }
  if (
    (snapshot.drain === undefined || fillerHeld) &&
    filling &&
    runway < fillTarget &&
    snapshot.unknownFillerCount === 0 &&
    nowMs >= snapshot.fillerRetryAtMs
  )
    return {
      ...base,
      action: {
        _tag: "BuildFiller",
        targetSeconds: fillTarget - runway,
        durationSeconds: fillLength(
          anchorGapSeconds - runway,
          snapshot.fillLengths,
          snapshot.estimates.length,
        ),
      },
    };
  return base;
};
