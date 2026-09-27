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
}

/**
 * An edit batch that has not taken effect yet. Until it does, what it withdraws is only
 * cover: it airs after everything else in its lane, so any clip the batch adds goes first.
 */
export interface PendingBatch {
  readonly id: number;
  /** The batch takes effect once each of these is Ready, started or settled. */
  readonly waitFor: ReadonlyArray<ItemKey>;
  /** The items it withdraws once it takes effect. */
  readonly targets: ReadonlyArray<ItemKey>;
}

/** Items that pending batches withdraw. */
export const supersededBy = (batches: ReadonlyArray<PendingBatch>): ReadonlySet<ItemKey> =>
  new Set(batches.flatMap((batch) => batch.targets));

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
}

export type PolicyAction =
  | { readonly _tag: "DeferAt"; readonly key: ItemKey; readonly clipId: ClipId }
  | { readonly _tag: "Build"; readonly key: ItemKey }
  | { readonly _tag: "BuildFiller"; readonly targetSeconds: number }
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
  const started = playing === undefined ? undefined : playingStartedMs.get(playing.clipId);
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
        return item?.atMs === undefined || item.atMs <= nowMs;
      })
      .reduce((seconds, clip) => seconds + clip.durationSeconds, 0)
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
  const replacedReady = new Set(
    items.flatMap((item) =>
      item.replaces !== undefined && item.phase === "Ready" ? [item.replaces] : [],
    ),
  );
  const rankItem = (
    item:
      | (Pick<PlannedItem, "lane" | "admission" | "group" | "atMs" | "generation"> & {
          readonly key?: ItemKey;
        })
      | undefined,
  ): Rank => {
    if (item?.atMs !== undefined && nowMs < item.atMs)
      return [lanes.length + 1, 1, item.admission, item.generation ?? 0];
    return [
      Math.max(0, lanes.indexOf(item?.lane ?? "")),
      item?.key !== undefined && superseded.has(item.key) ? 2 : begun(item) ? 0 : 1,
      item?.admission ?? 0,
      item?.key !== undefined && replacedReady.has(item.key) ? Infinity : (item?.generation ?? 0),
    ];
  };
  const rankClip = (clipId: ClipId, owned: ReadonlyMap<ClipId, OwnedClip>): Rank => {
    const clip = owned.get(clipId);
    if (clip === undefined) return [-1, 0, 0, 0];
    if (clip._tag === "Filler") return [lanes.length, 1, clip.index, 0];
    return rankItem(items.find((value) => value.key === clip.key));
  };
  return { previousAdmitted, begun, rankItem, rankClip };
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
      item.phase === "Accepted" ||
      item.phase === "Building" ||
      (item.phase === "Ready" && item.atMs !== undefined && item.atMs > snapshot.nowMs),
  );

/**
 * When an item could start at the earliest if it took `place`: after the rest of the playing
 * clip and every Ready clip on the preferred source that ranks ahead of that place. A new
 * submission's place is the end of its lane.
 */
export interface ProjectionView {
  readonly engine: EngineState;
  readonly items: ReadonlyArray<PlannedItem>;
  readonly owned: ReadonlyMap<ClipId, OwnedClip>;
  readonly lanes: ReadonlyArray<string>;
  readonly playingStartedMs: ReadonlyMap<ClipId, number>;
  readonly nowMs: number;
  readonly batches: ReadonlyArray<PendingBatch>;
}

export const projectedStartMs = (
  view: ProjectionView,
  place: Pick<PlannedItem, "lane" | "admission" | "group">,
): number => {
  const { engine, owned, nowMs } = view;
  const playing = Option.getOrUndefined(engine.playing);
  const playingRecord = playing === undefined ? undefined : Option.getOrUndefined(playing.record);
  const started = playing === undefined ? undefined : view.playingStartedMs.get(playing.clipId);
  const restMs =
    playingRecord === undefined || started === undefined
      ? 0
      : Math.max(0, playingRecord.durationSeconds * 1000 - (nowMs - started));
  const { rankItem, rankClip } = ordering(
    view.items,
    view.lanes,
    nowMs,
    supersededBy(view.batches),
  );
  const rank = rankItem(place);
  const preferred = preferredSession(engine);
  const aheadMs = engine.ready.reduce(
    (total, record) =>
      record.sessionId === preferred && compareRank(rankClip(record.clipId, owned), rank) < 0
        ? total + record.durationSeconds * 1000
        : total,
    0,
  );
  return nowMs + restMs + aheadMs;
};

/**
 * The pure policy: every withdrawal the plan wants now, and at most one serialized provider
 * command. The actor re-reads Engine after each applied command.
 */
export const plan = (snapshot: PolicySnapshot): PolicyDecision => {
  const { engine, items, nowMs, owned } = snapshot;
  const { previousAdmitted, rankClip } = ordering(
    items,
    snapshot.lanes,
    nowMs,
    supersededBy(snapshot.batches),
  );
  const runway = runwaySeconds(engine, nowMs, snapshot.playingStartedMs, owned, items);
  const nextAnchorMs = items
    .filter((item) => item.phase !== "Terminal" && item.atMs !== undefined && item.atMs > nowMs)
    .reduce((earliest, item) => Math.min(earliest, item.atMs ?? Infinity), Infinity);
  const anchorGapSeconds = nextAnchorMs === Infinity ? 0 : (nextAnchorMs - nowMs) / 1000;
  const fillTarget = Math.max(snapshot.targetSeconds, anchorGapSeconds);
  const filling =
    anchorGapSeconds > runway ||
    (snapshot.refillActive ? runway < snapshot.targetSeconds : runway < snapshot.floorSeconds);
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
    if (item.replaces === undefined) continue;
    const old = items.find((candidate) => candidate.key === item.replaces);
    if (old === undefined || old.phase === "Terminal") continue;
    if (old.phase === "Started") {
      if (item.phase !== "Started" && item.phase !== "Terminal") drop(item, "withdrawn");
    } else if (
      ((item.phase === "Ready" || item.phase === "Started") &&
        !pendingReplacements.has(item.key)) ||
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
      for (const key of batch.targets) {
        const target = find(key);
        if (target?.phase === "Accepted" && !snapshot.dispatched.has(key))
          drop(target, "withdrawn");
      }
  }
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
    const desired = [...actual].sort((a, b) =>
      compareRank(rankClip(a.clipId, owned), rankClip(b.clipId, owned)),
    );
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

  // Reorder movable Ready runway before treating a future At clip as exposed.
  // A wall-clock correction can otherwise require removing and rebuilding it.
  const playing = Option.getOrUndefined(engine.playing);
  const playingRecord = playing === undefined ? undefined : Option.getOrUndefined(playing.record);
  const playingStarted =
    playing === undefined ? undefined : snapshot.playingStartedMs.get(playing.clipId);
  const playingRest =
    playingRecord === undefined || playingStarted === undefined
      ? 0
      : Math.max(0, playingRecord.durationSeconds - (nowMs - playingStarted) / 1000);
  const exposedAnchor = items.find((item) => {
    if (
      item.phase !== "Ready" ||
      item.clipId === undefined ||
      item.atMs === undefined ||
      item.atMs <= nowMs
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
        return prior?.atMs === undefined || prior.atMs <= nowMs;
      })
      .reduce((seconds, clip) => seconds + clip.durationSeconds, 0);
    return playingRest + ahead < (item.atMs - nowMs) / 1000;
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
        (item.atMs === undefined || item.atMs <= nowMs || runway >= (item.atMs - nowMs) / 1000),
    )
    .sort((a, b) => {
      const lane = snapshot.lanes.indexOf(a.lane) - snapshot.lanes.indexOf(b.lane);
      // A replacement races the clip it replaces, so it builds first in its lane.
      const replacing = Number(b.replaces !== undefined) - Number(a.replaces !== undefined);
      // A deadline may move a whole group ahead in its lane, never into one being built.
      const open =
        Number(b.group !== undefined && b.group.index > 0) -
        Number(a.group !== undefined && a.group.index > 0);
      return (
        lane ||
        replacing ||
        open ||
        (a.startByMs ?? Infinity) - (b.startByMs ?? Infinity) ||
        a.admission - b.admission
      );
    });
  if (eligible[0] !== undefined)
    return { ...base, action: { _tag: "Build", key: eligible[0].key } };
  if (
    (snapshot.drain === undefined || fillerHeld) &&
    filling &&
    runway < fillTarget &&
    snapshot.unknownFillerCount === 0 &&
    nowMs >= snapshot.fillerRetryAtMs
  )
    return {
      ...base,
      action: { _tag: "BuildFiller", targetSeconds: fillTarget - runway },
    };
  return base;
};
