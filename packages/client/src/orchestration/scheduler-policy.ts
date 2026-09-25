import * as Option from "effect/Option";
import type { ClipId } from "./request.js";
import type { EngineState } from "./types.js";
import type { ItemKey } from "./scheduler.js";

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
}

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
  readonly accepting: boolean;
  readonly fillerRetryAtMs: number;
  readonly fillerUnknown: boolean;
  readonly fillerUnknownSessionId: string | undefined;
  /** A refused move is retried only after the Ready snapshot changes. */
  readonly blockedMove: string | undefined;
}

export type PolicyAction =
  | { readonly _tag: "Withdraw"; readonly key: ItemKey; readonly reason: "late" }
  | { readonly _tag: "DeferAt"; readonly key: ItemKey; readonly clipId: ClipId }
  | { readonly _tag: "WithdrawFiller"; readonly clipId: ClipId }
  | { readonly _tag: "Build"; readonly key: ItemKey }
  | { readonly _tag: "BuildFiller"; readonly targetSeconds: number }
  | { readonly _tag: "Order"; readonly clipId: ClipId; readonly position: number };

export interface PolicyDecision {
  readonly runwaySeconds: number;
  readonly refillActive: boolean;
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

/** A pure, single-action policy. The actor re-reads Engine after each applied action. */
export const plan = (snapshot: PolicySnapshot): PolicyDecision => {
  const { engine, items, nowMs, owned } = snapshot;
  const runway = runwaySeconds(engine, nowMs, snapshot.playingStartedMs, owned, items);
  const nextAnchorMs = items
    .filter((item) => item.phase !== "Terminal" && item.atMs !== undefined && item.atMs > nowMs)
    .reduce((earliest, item) => Math.min(earliest, item.atMs ?? Infinity), Infinity);
  const anchorGapSeconds = nextAnchorMs === Infinity ? 0 : (nextAnchorMs - nowMs) / 1000;
  const fillTarget = Math.max(snapshot.targetSeconds, anchorGapSeconds);
  const filling =
    anchorGapSeconds > runway ||
    (snapshot.refillActive ? runway < snapshot.targetSeconds : runway < snapshot.floorSeconds);
  const base = { runwaySeconds: runway, refillActive: filling };

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
      return { ...base, action: { _tag: "Withdraw", key: item.key, reason: "late" } };
  }

  const preferred = preferredSession(engine);
  const retiring = Option.getOrUndefined(engine.retiringSessionId);
  if (
    preferred !== undefined &&
    retiring !== undefined &&
    engine.ready.some(
      (clip) => clip.sessionId === preferred && owned.get(clip.clipId)?._tag === "Item",
    )
  ) {
    const staleFiller = engine.ready.find(
      (clip) => clip.sessionId === retiring && owned.get(clip.clipId)?._tag === "Filler",
    );
    if (staleFiller !== undefined)
      return { ...base, action: { _tag: "WithdrawFiller", clipId: staleFiller.clipId } };
  }

  // Physical sources are independent queues. A move never ranks across them.
  let offset = 0;
  for (const session of engine.sessions) {
    const actual = engine.ready.filter((clip) => clip.sessionId === session.sessionId);
    const rank = (clipId: ClipId): readonly [number, number] => {
      const clip = owned.get(clipId);
      if (clip === undefined) return [-1, 0];
      if (clip._tag === "Filler") return [snapshot.lanes.length, clip.index];
      const item = items.find((value) => value.key === clip.key);
      if (item?.atMs !== undefined && nowMs < item.atMs)
        return [snapshot.lanes.length + 1, item.admission];
      return [Math.max(0, snapshot.lanes.indexOf(item?.lane ?? "")), item?.admission ?? 0];
    };
    const desired = [...actual].sort((a, b) => {
      const left = rank(a.clipId);
      const right = rank(b.clipId);
      return left[0] - right[0] || left[1] - right[1];
    });
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
  if (!snapshot.accepting || preferredAvailability !== "Ready") return base;
  const activeFiller = [...owned.entries()].filter(
    ([clipId, owner]) =>
      owner._tag === "Filler" &&
      owner.sessionId === preferred &&
      !engine.ready.some((clip) => clip.clipId === clipId) &&
      Option.getOrUndefined(engine.playing)?.clipId !== clipId &&
      !engine.failed.includes(clipId),
  ).length;
  const inflight =
    items.filter(
      (item) =>
        (item.phase === "Building" || item.phase === "Unknown") &&
        (item.sessionId ?? item.unknownSessionId) === preferred,
    ).length +
    activeFiller +
    (snapshot.fillerUnknown &&
    (snapshot.fillerUnknownSessionId === undefined || snapshot.fillerUnknownSessionId === preferred)
      ? 1
      : 0);
  if (inflight >= snapshot.maxBuildsInFlight) return base;
  const eligible = items
    .filter(
      (item) =>
        item.phase === "Accepted" &&
        (item.retryAtMs === undefined || nowMs >= item.retryAtMs) &&
        (item.notBeforeMs === undefined || nowMs >= item.notBeforeMs) &&
        // Autoplay cannot hold a Ready clip. Admit a future anchor only once
        // known material ahead of it covers the time until that anchor.
        (item.atMs === undefined || item.atMs <= nowMs || runway >= (item.atMs - nowMs) / 1000),
    )
    .sort((a, b) => {
      const lane = snapshot.lanes.indexOf(a.lane) - snapshot.lanes.indexOf(b.lane);
      return (
        lane || (a.startByMs ?? Infinity) - (b.startByMs ?? Infinity) || a.admission - b.admission
      );
    });
  if (eligible[0] !== undefined)
    return { ...base, action: { _tag: "Build", key: eligible[0].key } };
  if (
    filling &&
    runway < fillTarget &&
    !(
      snapshot.fillerUnknown &&
      (snapshot.fillerUnknownSessionId === undefined ||
        snapshot.fillerUnknownSessionId === preferred)
    ) &&
    nowMs >= snapshot.fillerRetryAtMs
  )
    return {
      ...base,
      action: { _tag: "BuildFiller", targetSeconds: fillTarget - runway },
    };
  return base;
};
