import * as Clock from "effect/Clock";
import * as Context from "effect/Context";
import * as Deferred from "effect/Deferred";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as PubSub from "effect/PubSub";
import * as Queue from "effect/Queue";
import * as Result from "effect/Result";
import * as Schema from "effect/Schema";
import type * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import * as SubscriptionRef from "effect/SubscriptionRef";
import { duration } from "../duration.js";
import { parsedInput, ReactorError } from "../errors.js";
import { monotonicMillis } from "./elapsed.js";
import { captureRequest, PolicyFailure } from "./request.js";
import type { ClipId, ClipRequest } from "./request.js";
import { activeIds } from "./routing.js";
import {
  fillerIndexFromKey,
  fillerKey,
  isReservedSchedulerKey,
  keyedRequest,
  keyFromProviderMetadata,
} from "./scheduler-key.js";
import { plan, runwaySeconds } from "./scheduler-policy.js";
import type { OwnedClip, PlannedItem, PolicyAction } from "./scheduler-policy.js";
import type { EngineError, EngineEvent, EngineState, RemoveOutcome } from "./types.js";
import { Engine } from "./types.js";

/** Stable caller identity for a scheduled item. */
export const ItemKey = Schema.NonEmptyString.pipe(Schema.brand("ItemKey"));
export type ItemKey = typeof ItemKey.Type;

export interface LaneSpec {
  /** Lanes are listed from highest to lowest priority. */
  readonly name: string;
}

export type StartMode =
  | { readonly _tag: "Follow" }
  | {
      readonly _tag: "At";
      /** Epoch milliseconds; its monotonic deadline follows wall-clock corrections. */
      readonly time: number;
      readonly late:
        | { readonly _tag: "nextBoundary" }
        | { readonly _tag: "skipIfLaterThan"; readonly by: Duration.Input }
        | { readonly _tag: "drop" };
    };

export interface ItemSpec {
  readonly key: ItemKey;
  readonly lane: string;
  readonly request: ClipRequest;
  /** Relative to admission, measured on the monotonic clock. */
  readonly window?: {
    readonly notBefore?: Duration.Input;
    readonly startBy?: Duration.Input;
    readonly firm: boolean;
  };
  readonly start?: StartMode;
}

export interface FillContext {
  readonly index: number;
  readonly runwaySeconds: number;
  /** For an `At` anchor, the uncovered gap that filler should approach. */
  readonly targetSeconds?: number;
}

export interface SchedulerOptions {
  readonly lanes: ReadonlyArray<LaneSpec>;
  readonly filler: {
    /** Ready airtime, including the estimated rest of the playing clip. */
    readonly runway: { readonly floor: Duration.Input; readonly target: Duration.Input };
    /** Pure callback; each admitted index is requested once. */
    readonly clip: (context: FillContext) => ClipRequest;
  };
  /** Provider build admissions in flight on the preferred source, independent of runway. Defaults to one. */
  readonly maxBuildsInFlight?: number;
}

export type AsRunStatus =
  | { readonly _tag: "Accepted" }
  | { readonly _tag: "Building" }
  | { readonly _tag: "Ready"; readonly sessionId: string }
  | {
      readonly _tag: "Started";
      readonly at: number;
      readonly sessionId: string;
      readonly lateByMillis?: number;
    }
  | {
      readonly _tag: "Ended";
      readonly at: number;
      readonly termination: "finished" | "stopped";
      readonly airedSeconds: number;
    }
  | { readonly _tag: "Dropped"; readonly reason: "late" | "withdrawn" }
  | { readonly _tag: "Failed"; readonly reason: string }
  | { readonly _tag: "Unobserved" }
  | {
      readonly _tag: "Unknown";
      /** The original source retired without keyed proof; no further reconciliation is possible. */
      readonly terminal?: true;
    };

/** The first observed start or a disposition that rules out a known start. */
export type FirstDecisiveStatus =
  | Extract<
      AsRunStatus,
      { readonly _tag: "Started" | "Ended" | "Dropped" | "Failed" | "Unobserved" }
    >
  | { readonly _tag: "Unknown"; readonly terminal: true };

const isFirstDecisive = (status: AsRunStatus): status is FirstDecisiveStatus =>
  status._tag === "Started" ||
  status._tag === "Ended" ||
  status._tag === "Dropped" ||
  status._tag === "Failed" ||
  status._tag === "Unobserved" ||
  (status._tag === "Unknown" && status.terminal === true);

export interface AsRunEvent {
  readonly key: ItemKey;
  /** Epoch milliseconds when this evidence was observed. */
  readonly at: number;
  readonly status: AsRunStatus;
}

export interface ItemHandle {
  readonly key: ItemKey;
  /** Resolves when the item starts or cannot be known to start. */
  readonly started: Effect.Effect<AsRunStatus>;
  /** Resolves when the item ends or cannot be known to end. */
  readonly outcome: Effect.Effect<AsRunStatus>;
  /** Retains the first decisive evidence, even if later as-run events or a lost reply follow. */
  readonly firstDecisive: Effect.Effect<FirstDecisiveStatus>;
}

export type WithdrawOutcome = "withdrawn" | "already-started" | "not-found";

export interface SchedulerState {
  readonly accepting: boolean;
  readonly runwaySeconds: number;
  readonly playing: ItemKey | "filler" | "other" | null;
  readonly lanes: ReadonlyArray<{
    readonly name: string;
    readonly keys: ReadonlyArray<ItemKey>;
  }>;
  readonly sessions: ReadonlyArray<{
    readonly sessionId: string;
    readonly ready: ReadonlyArray<ItemKey | "filler" | "other">;
  }>;
  readonly starved: number;
}

export class KeyMismatch extends Schema.TaggedError<KeyMismatch>()("KeyMismatch", {
  key: ItemKey,
}) {
  static of(key: ItemKey): KeyMismatch {
    return new KeyMismatch({ key });
  }
}

export class WouldMissDeadline extends Schema.TaggedError<WouldMissDeadline>()(
  "WouldMissDeadline",
  { key: ItemKey },
) {
  static of(key: ItemKey): WouldMissDeadline {
    return new WouldMissDeadline({ key });
  }
}

interface CapturedItem {
  readonly key: ItemKey;
  readonly lane: string;
  readonly request: ClipRequest;
  readonly fingerprint: string;
  readonly notBeforeOffsetMs: number | undefined;
  readonly startByOffsetMs: number | undefined;
  readonly firm: boolean;
  readonly atWallMs: number | undefined;
  readonly late: PlannedItem["late"];
}

interface Entry extends PlannedItem {
  readonly request: ClipRequest;
  readonly fingerprint: string;
  readonly handle: ItemHandle;
  readonly startedWaiter: Deferred.Deferred<AsRunStatus>;
  readonly outcomeWaiter: Deferred.Deferred<AsRunStatus>;
  readonly firstDecisiveWaiter: Deferred.Deferred<FirstDecisiveStatus>;
  readonly atWallMs?: number;
  phase: PlannedItem["phase"];
  retryAtMs?: number;
  atMs?: number;
  clipId?: ClipId;
  sessionId: string | undefined;
  unknownSessionId: string | undefined;
  unknownCause: EngineError | undefined;
  acknowledgedAtSnapshotSerial?: number;
  startedAtMonoMs?: number;
  startedAtEpochMs?: number;
  status: AsRunStatus;
}

interface FillerEntry {
  readonly index: number;
  clipId?: ClipId;
  sessionId: string | undefined;
  /** A snapshot acquired before this result cannot disprove this filler. */
  acknowledgedAtSnapshotSerial?: number;
}

type Command =
  | {
      readonly _tag: "Build";
      readonly key: ItemKey;
      readonly request: ClipRequest;
      readonly sessionId: string | undefined;
    }
  | {
      readonly _tag: "BuildFiller";
      readonly index: number;
      readonly request: ClipRequest;
      readonly sessionId: string | undefined;
    }
  | {
      readonly _tag: "RemoveItem";
      readonly key: ItemKey;
      readonly clipId: ClipId;
      readonly reason: "late" | "withdrawn";
    }
  | { readonly _tag: "DeferAt"; readonly key: ItemKey; readonly clipId: ClipId }
  | { readonly _tag: "RemoveFiller"; readonly clipId: ClipId }
  | { readonly _tag: "RemoveDuplicate"; readonly clipId: ClipId }
  | {
      readonly _tag: "Order";
      readonly clipId: ClipId;
      readonly position: number;
      readonly signature: string;
    }
  | { readonly _tag: "PauseAutoplay" };

type CommandValue = ClipId | RemoveOutcome | void;

type EarlyClipEvent = Extract<EngineEvent, { readonly _tag: "Started" | "Ended" | "Failed" }>;

const maxEarlyClipIds = 4096;

interface PendingWithdrawal {
  readonly reason: "late" | "withdrawn";
  readonly replies: Deferred.Deferred<WithdrawOutcome, EngineError>[];
}

type Message =
  | {
      readonly _tag: "Submit";
      readonly item: CapturedItem;
      readonly reply: Deferred.Deferred<ItemHandle, KeyMismatch | WouldMissDeadline | EngineError>;
    }
  | {
      readonly _tag: "Withdraw";
      readonly key: ItemKey;
      readonly reply: Deferred.Deferred<WithdrawOutcome, EngineError>;
    }
  | { readonly _tag: "Drain"; readonly reply: Deferred.Deferred<void, EngineError> }
  | { readonly _tag: "Snapshot"; readonly state: EngineState; readonly acquiredSerial: number }
  | { readonly _tag: "Event"; readonly event: EngineEvent }
  | { readonly _tag: "Tick" }
  | {
      readonly _tag: "CommandDone";
      readonly command: Command;
      readonly result: Result.Result<CommandValue, EngineError>;
    }
  | { readonly _tag: "Closed"; readonly reason: string };

const invalid = (message: string): PolicyFailure =>
  PolicyFailure.refuse("InvalidRequest", message, "submit");

const ownedData = (
  input: unknown,
  fields: ReadonlyArray<string>,
  name: string,
): Result.Result<Record<string, unknown>, PolicyFailure> => {
  if (input === null || typeof input !== "object" || Array.isArray(input))
    return Result.fail(invalid(`${name} must be an object`));
  const descriptors = Object.getOwnPropertyDescriptors(input);
  if (
    Object.getOwnPropertySymbols(input).length > 0 ||
    Object.keys(descriptors).some(
      (field) => !fields.includes(field) || !("value" in descriptors[field]!),
    )
  )
    return Result.fail(invalid(`${name} has unsupported fields or accessors`));
  return Result.succeed(
    Object.fromEntries(
      Object.entries(descriptors).map(([field, descriptor]) => [field, descriptor.value]),
    ),
  );
};

const captureDuration = (input: unknown): Effect.Effect<Duration.Input, PolicyFailure> =>
  Effect.gen(function* () {
    if (input === null || typeof input !== "object" || Duration.isDuration(input))
      return input as Duration.Input;
    if (Array.isArray(input)) {
      const descriptors: Record<string, PropertyDescriptor> =
        Object.getOwnPropertyDescriptors(input);
      if (
        Object.getOwnPropertySymbols(input).length > 0 ||
        Object.keys(descriptors).some(
          (field) => !["0", "1", "length"].includes(field) || !("value" in descriptors[field]!),
        ) ||
        descriptors.length?.value !== 2 ||
        descriptors["0"] === undefined ||
        descriptors["1"] === undefined
      )
        return yield* invalid("Duration tuple has unsupported fields or accessors");
      return [descriptors["0"].value, descriptors["1"].value];
    }
    return (yield* Effect.fromResult(
      ownedData(
        input,
        [
          "weeks",
          "days",
          "hours",
          "minutes",
          "seconds",
          "milliseconds",
          "microseconds",
          "nanoseconds",
        ],
        "Duration",
      ),
    )) as Duration.Input;
  });

const requestDuration = (input: unknown, name: string, allowZero = false) =>
  Effect.gen(function* () {
    const captured = yield* captureDuration(input);
    return yield* parsedInput(
      () => Duration.toMillis(duration(captured, name, { allowZero })),
      "submit",
    ).pipe(Effect.mapError((error) => invalid(error.message)));
  });

/** Capture scheduling fields before reading the request's placement fields. */
const captureItem = (input: ItemSpec, lanes: ReadonlySet<string>) =>
  Effect.gen(function* (): Effect.fn.Return<CapturedItem, PolicyFailure> {
    if (input === null || typeof input !== "object" || Array.isArray(input))
      return yield* invalid("Scheduled item must be an object");
    const descriptors = Object.getOwnPropertyDescriptors(input);
    if (
      Object.getOwnPropertySymbols(input).length > 0 ||
      Object.keys(descriptors).some(
        (field) =>
          !["key", "lane", "request", "window", "start"].includes(field) ||
          !("value" in descriptors[field]!),
      )
    )
      return yield* invalid("Scheduled item has unsupported fields or accessors");
    const key = yield* Schema.decodeUnknownEffect(ItemKey)(descriptors.key?.value).pipe(
      Effect.mapError(() => invalid("Scheduled item key must be nonempty")),
    );
    if (isReservedSchedulerKey(key)) return yield* invalid("Scheduled item key is reserved");
    const lane: unknown = descriptors.lane?.value;
    if (typeof lane !== "string" || !lanes.has(lane))
      return yield* invalid("Scheduled item lane is not configured");
    const rawRequest: unknown = descriptors.request?.value;
    const request = yield* captureRequest(rawRequest as ClipRequest);
    if (
      request.position !== undefined ||
      request.before !== undefined ||
      request.sameSessionAs !== undefined ||
      request.continueFrom !== undefined ||
      request.sequence !== undefined
    )
      return yield* invalid("Scheduler owns placement and source selection");
    const rawWindow: unknown = descriptors.window?.value;
    const window =
      rawWindow === undefined
        ? undefined
        : yield* Effect.fromResult(
            ownedData(rawWindow, ["notBefore", "startBy", "firm"], "Scheduled window"),
          );
    const notBeforeOffsetMs =
      window?.notBefore === undefined
        ? undefined
        : yield* requestDuration(window.notBefore, "notBefore", true);
    const startByOffsetMs =
      window?.startBy === undefined
        ? undefined
        : yield* requestDuration(window.startBy, "startBy", true);
    if (
      window !== undefined &&
      (typeof window.firm !== "boolean" ||
        (notBeforeOffsetMs !== undefined &&
          startByOffsetMs !== undefined &&
          notBeforeOffsetMs > startByOffsetMs))
    )
      return yield* invalid("Scheduled window is inconsistent");
    const rawStart: unknown = descriptors.start?.value;
    const start =
      rawStart === undefined
        ? undefined
        : yield* Effect.fromResult(
            ownedData(rawStart, ["_tag", "time", "late"], "Scheduled start mode"),
          );
    let atWallMs: number | undefined;
    let late: PlannedItem["late"] = "nextBoundary";
    if (start?._tag === "Follow" && (start.time !== undefined || start.late !== undefined))
      return yield* invalid("Follow cannot contain an anchor or lateness policy");
    if (start !== undefined && start._tag !== "Follow") {
      if (start._tag !== "At" || !Number.isFinite(start.time))
        return yield* invalid("Scheduled start mode is invalid");
      atWallMs = start.time as number;
      const policy = yield* Effect.fromResult(
        ownedData(start.late, ["_tag", "by"], "Scheduled lateness policy"),
      );
      if (policy._tag === "drop" && policy.by === undefined) late = "drop";
      else if (policy._tag === "skipIfLaterThan")
        late = {
          skipIfLaterThanMs: yield* requestDuration(policy.by, "skipIfLaterThan", true),
        };
      else if (policy._tag !== "nextBoundary" || policy.by !== undefined)
        return yield* invalid("Scheduled lateness policy is invalid");
    }
    return {
      key,
      lane,
      request,
      notBeforeOffsetMs,
      startByOffsetMs,
      firm: window?.firm === true,
      atWallMs,
      late,
      fingerprint: JSON.stringify({
        lane,
        request,
        notBeforeOffsetMs,
        startByOffsetMs,
        firm: window?.firm === true,
        atWallMs,
        late,
      }),
    };
  });

export interface SchedulerShape {
  readonly submit: (
    item: ItemSpec,
  ) => Effect.Effect<ItemHandle, KeyMismatch | WouldMissDeadline | EngineError>;
  readonly withdraw: (key: ItemKey) => Effect.Effect<WithdrawOutcome, EngineError>;
  readonly drain: Effect.Effect<void, EngineError>;
  readonly state: Effect.Effect<SchedulerState>;
  /** Lifecycle evidence is ordered per item; subscribers receive later events. */
  readonly asRun: Stream.Stream<AsRunEvent>;
}

export const lineup = (filler: SchedulerOptions["filler"]): SchedulerOptions => ({
  lanes: [{ name: "line" }],
  filler,
});

export class Scheduler extends Context.Service<Scheduler, SchedulerShape>()(
  "reactor-effect-client/Orchestration/Scheduler",
) {
  static readonly lineup = lineup;
}

export const makeScheduler = (
  options: SchedulerOptions,
): Effect.Effect<SchedulerShape, ReactorError | PolicyFailure, Engine | Scope.Scope> =>
  Effect.gen(function* () {
    const engine = yield* Engine;
    const clock = yield* Clock.Clock;
    const lanes = yield* parsedInput(() => {
      if (
        options.lanes.length === 0 ||
        options.lanes.some((lane) => typeof lane.name !== "string" || lane.name.length === 0) ||
        new Set(options.lanes.map((lane) => lane.name)).size !== options.lanes.length
      )
        throw ReactorError.fromCode("InvalidInput", "Scheduler lanes must be unique and nonempty");
      return options.lanes.map((lane) => lane.name);
    }, "makeScheduler");
    const { floorSeconds, targetSeconds, maxBuildsInFlight } = yield* parsedInput(() => {
      const floorSeconds =
        Duration.toMillis(
          duration(options.filler.runway.floor, "runway floor", { allowZero: true }),
        ) / 1000;
      const targetSeconds =
        Duration.toMillis(duration(options.filler.runway.target, "runway target")) / 1000;
      const maxBuildsInFlight = options.maxBuildsInFlight ?? 1;
      if (
        floorSeconds > targetSeconds ||
        !Number.isSafeInteger(maxBuildsInFlight) ||
        maxBuildsInFlight < 1 ||
        maxBuildsInFlight > 1024
      )
        throw ReactorError.fromCode("InvalidInput", "Scheduler runway or build cap is invalid");
      return { floorSeconds, targetSeconds, maxBuildsInFlight };
    }, "makeScheduler");
    const captureFiller = (
      index: number,
      runway: number,
      targetSeconds?: number,
    ): Effect.Effect<ClipRequest, PolicyFailure> =>
      Effect.gen(function* () {
        const request = yield* captureRequest(
          options.filler.clip({
            index,
            runwaySeconds: runway,
            ...(targetSeconds === undefined ? {} : { targetSeconds }),
          }),
        );
        if (
          request.position !== undefined ||
          request.before !== undefined ||
          request.sameSessionAs !== undefined ||
          request.continueFrom !== undefined ||
          request.sequence !== undefined
        )
          return yield* PolicyFailure.refuse(
            "InvalidRequest",
            "Scheduler filler cannot choose placement or source",
          );
        return request;
      });
    let fillerIndex = 0;
    let fillerRetryAtMs = 0;
    let unknownFiller:
      | {
          readonly index: number;
          readonly sessionId: string | undefined;
        }
      | undefined;
    let upcomingFiller: ClipRequest | undefined = yield* captureFiller(0, 0);
    const inbox = yield* Queue.unbounded<Message>();
    const commandQueue = yield* Queue.unbounded<Command>();
    const events = yield* PubSub.unbounded<AsRunEvent>();
    yield* Effect.addFinalizer(() =>
      Effect.gen(function* () {
        yield* Queue.shutdown(commandQueue);
        yield* Queue.shutdown(inbox);
        yield* PubSub.shutdown(events);
      }),
    );
    const stateRef = yield* SubscriptionRef.make<SchedulerState>({
      accepting: true,
      runwaySeconds: 0,
      playing: null,
      lanes: lanes.map((name) => ({ name, keys: [] })),
      sessions: [],
      starved: 0,
    });
    const initialized = yield* Deferred.make<void, ReactorError>();
    const items = new Map<ItemKey, Entry>();
    const owned = new Map<ClipId, OwnedClip>();
    // An event can overtake its enqueue reply. Keep only the evidence needed to
    // decide that clip's fate until the one in-flight command returns its ID.
    const earlyClipEvents = new Map<ClipId, EarlyClipEvent[]>();
    const removedIds = new Set<ClipId>();
    const filler = new Map<number, FillerEntry>();
    const playingStartedMs = new Map<ClipId, number>();
    let admission = 0;
    let accepting = true;
    let starved = 0;
    let refillActive = false;
    let ended: string | undefined;
    let drainFailure: EngineError | undefined;
    const drainReplies: Deferred.Deferred<void, EngineError>[] = [];
    const pendingWithdrawals = new Map<ItemKey, PendingWithdrawal>();
    const pendingBuilds = new Set<ItemKey>();
    const pendingAtDeferrals = new Set<ItemKey>();
    const pendingItemRemovals = new Set<ItemKey>();
    const pendingFillerRemovals = new Set<ClipId>();
    const removalRetryAtMs = new Map<ClipId, number>();
    const removalUnknownAtSerial = new Map<ClipId, number>();
    let commandCount = 0;
    let snapshotAcquiredSerial = 0;
    let lastProcessedSnapshotSerial = 0;
    let draining = false;
    let drained = false;
    let observationReady = false;
    let blockedMove: string | undefined;

    const emit = (entry: Entry, status: AsRunStatus): Effect.Effect<void> =>
      Effect.gen(function* () {
        if (JSON.stringify(entry.status) === JSON.stringify(status)) return;
        entry.status = status;
        entry.phase =
          status._tag === "Accepted" ||
          status._tag === "Building" ||
          status._tag === "Ready" ||
          status._tag === "Started"
            ? status._tag
            : status._tag === "Unknown"
              ? "Unknown"
              : "Terminal";
        yield* PubSub.publish(events, {
          key: entry.key,
          at: clock.currentTimeMillisUnsafe(),
          status,
        });
        if (isFirstDecisive(status)) yield* Deferred.succeed(entry.firstDecisiveWaiter, status);
        if (status._tag === "Started") yield* Deferred.succeed(entry.startedWaiter, status);
        if (
          status._tag === "Ended" ||
          status._tag === "Dropped" ||
          status._tag === "Failed" ||
          status._tag === "Unobserved" ||
          status._tag === "Unknown"
        ) {
          yield* Deferred.succeed(entry.startedWaiter, status);
          yield* Deferred.succeed(entry.outcomeWaiter, status);
        }
      });

    const closeActor = (reason: string): Effect.Effect<void> =>
      Effect.gen(function* () {
        if (ended !== undefined) return;
        ended = reason;
        accepting = false;
        for (const item of items.values()) {
          if (item.phase === "Terminal") continue;
          if (item.phase === "Unknown") {
            yield* emit(item, { _tag: "Unknown", terminal: true });
            item.phase = "Terminal";
          } else yield* emit(item, { _tag: "Failed", reason });
        }
        const closed = PolicyFailure.refuse("SessionClosed", reason);
        for (const [key, pending] of pendingWithdrawals) {
          for (const reply of pending.replies) yield* Deferred.fail(reply, closed);
          pendingWithdrawals.delete(key);
        }
        for (const reply of drainReplies.splice(0)) yield* Deferred.fail(reply, closed);
      });

    const publishState = (state: EngineState): Effect.Effect<void> => {
      const playing = Option.getOrUndefined(state.playing);
      const playingOwner = playing === undefined ? undefined : owned.get(playing.clipId);
      const playingKey: SchedulerState["playing"] =
        playing === undefined
          ? null
          : playingOwner === undefined
            ? "other"
            : playingOwner._tag === "Filler"
              ? "filler"
              : playingOwner.key;
      return SubscriptionRef.set(stateRef, {
        accepting,
        runwaySeconds: runwaySeconds(state, monotonicMillis(clock), playingStartedMs, owned, [
          ...items.values(),
        ]),
        playing: playingKey,
        lanes: lanes.map((name) => ({
          name,
          keys: [...items.values()]
            .filter((item) => item.lane === name && item.phase !== "Terminal")
            .map((item) => item.key),
        })),
        sessions: state.sessions.map(({ sessionId }) => ({
          sessionId,
          ready: state.ready
            .filter((clip) => clip.sessionId === sessionId)
            .map((clip) => {
              const owner = owned.get(clip.clipId);
              return owner === undefined ? "other" : owner._tag === "Filler" ? "filler" : owner.key;
            }),
        })),
        starved,
      });
    };

    const activeRecords = (state: EngineState) => [
      ...state.queued,
      ...state.ready,
      ...Option.match(state.building, {
        onNone: () => [],
        onSome: (build) => [build.record],
      }),
      ...Option.match(state.playing, {
        onNone: () => [],
        onSome: (playing) =>
          Option.match(playing.record, {
            onNone: () => [],
            onSome: (record) => [record],
          }),
      }),
    ];

    const knownRecord = (state: EngineState, clipId: ClipId) =>
      activeRecords(state).find((record) => record.clipId === clipId);

    const refreshAnchors = (): void => {
      const nowMs = monotonicMillis(clock);
      const wallMs = clock.currentTimeMillisUnsafe();
      for (const item of items.values())
        if (item.atWallMs !== undefined) item.atMs = nowMs + item.atWallMs - wallMs;
    };

    const observed = (state: EngineState, recoverMissing = false): Effect.Effect<void> =>
      Effect.gen(function* () {
        const active = new Set(activeIds(state));
        if (recoverMissing) {
          for (const [clipId, owner] of owned) {
            if (owner._tag !== "Filler" || active.has(clipId)) continue;
            const tracked = filler.get(owner.index);
            if (
              tracked?.clipId === clipId &&
              tracked?.acknowledgedAtSnapshotSerial !== undefined &&
              lastProcessedSnapshotSerial <= tracked.acknowledgedAtSnapshotSerial
            )
              continue;
            const sessionId = owner.sessionId;
            const source = state.sessions.find((entry) => entry.sessionId === sessionId);
            if (
              sessionId === undefined
                ? state.availability !== "Ready"
                : source !== undefined && source.availability !== "Ready"
            )
              continue;
            owned.delete(clipId);
            if (tracked?.clipId === clipId) filler.delete(owner.index);
            playingStartedMs.delete(clipId);
            pendingFillerRemovals.delete(clipId);
          }
        }
        for (const record of activeRecords(state)) {
          const index = fillerIndexFromKey(keyFromProviderMetadata(record.provider.metadata));
          if (index === undefined || removedIds.has(record.clipId)) continue;
          const existing = owned.get(record.clipId);
          if (existing?._tag === "Filler") {
            if (existing.sessionId !== record.sessionId)
              owned.set(record.clipId, { ...existing, sessionId: record.sessionId });
            const tracked = filler.get(index);
            if (tracked?.clipId === record.clipId) tracked.sessionId = record.sessionId;
            continue;
          }
          if (existing !== undefined) continue;
          owned.set(record.clipId, { _tag: "Filler", index, sessionId: record.sessionId });
          filler.set(index, { index, clipId: record.clipId, sessionId: record.sessionId });
          const priorIndex = fillerIndex;
          fillerIndex = Math.max(fillerIndex, index + 1);
          if (unknownFiller?.index === index) unknownFiller = undefined;
          if (fillerIndex !== priorIndex) upcomingFiller = undefined;
        }
        for (const item of items.values()) {
          if (item.clipId === undefined) {
            const resumed = activeRecords(state).find(
              (record) =>
                !removedIds.has(record.clipId) &&
                keyFromProviderMetadata(record.provider.metadata) === item.key,
            );
            if (resumed !== undefined) {
              item.clipId = resumed.clipId;
              item.sessionId = resumed.sessionId;
              item.unknownSessionId = undefined;
              owned.set(resumed.clipId, { _tag: "Item", key: item.key });
            }
          }
          const clipId = item.clipId;
          if (clipId === undefined || item.phase === "Terminal") continue;
          const playing = Option.getOrUndefined(state.playing);
          if (playing?.clipId === clipId) {
            const record = Option.getOrUndefined(playing.record);
            const at = Option.getOrUndefined(playing.startedAt);
            const startedAtMonotonicMillis = playing.startedAtMonotonicMillis;
            if (
              record !== undefined &&
              at !== undefined &&
              startedAtMonotonicMillis !== undefined &&
              item.phase !== "Started"
            ) {
              item.sessionId = record.sessionId;
              item.startedAtEpochMs = at;
              item.startedAtMonoMs = startedAtMonotonicMillis;
              playingStartedMs.set(clipId, startedAtMonotonicMillis);
              yield* emit(item, {
                _tag: "Started",
                at,
                sessionId: record.sessionId,
                ...((item.atMs ?? item.startByMs) === undefined ||
                startedAtMonotonicMillis <= (item.atMs ?? item.startByMs)!
                  ? {}
                  : { lateByMillis: startedAtMonotonicMillis - (item.atMs ?? item.startByMs)! }),
              });
            } else if (
              recoverMissing &&
              (record === undefined || at === undefined) &&
              item.phase !== "Started"
            )
              yield* emit(item, { _tag: "Unobserved" });
          } else if (state.ready.some((record) => record.clipId === clipId)) {
            const record = knownRecord(state, clipId)!;
            item.sessionId = record.sessionId;
            if (item.phase !== "Started")
              yield* emit(item, { _tag: "Ready", sessionId: record.sessionId });
          } else if (
            state.queued.some((record) => record.clipId === clipId) ||
            state.generationOrder.includes(clipId) ||
            Option.getOrUndefined(state.building)?.record.clipId === clipId
          ) {
            item.sessionId = knownRecord(state, clipId)?.sessionId ?? item.sessionId;
            if (item.phase === "Accepted" || item.phase === "Unknown")
              yield* emit(item, { _tag: "Building" });
          } else if (state.failed.includes(clipId))
            yield* emit(item, { _tag: "Failed", reason: "The clip failed unobserved" });
          else if (
            recoverMissing &&
            item.sessionId !== undefined &&
            (state.sessions.find((source) => source.sessionId === item.sessionId)?.availability ??
              "Ready") === "Ready" &&
            !active.has(clipId) &&
            (item.acknowledgedAtSnapshotSerial === undefined ||
              lastProcessedSnapshotSerial > item.acknowledgedAtSnapshotSerial)
          )
            yield* emit(item, { _tag: "Unobserved" });
        }
        yield* publishState(state);
      });

    const onEvent = (event: EngineEvent): Effect.Effect<void> =>
      Effect.gen(function* () {
        if (event._tag === "Starved") starved++;
        if (event._tag === "SessionFailed") {
          yield* closeActor(event.failure.message);
          return;
        }
        if (!("clipId" in event)) return;
        if (
          commandCount > 0 &&
          (event._tag === "Started" || event._tag === "Ended" || event._tag === "Failed")
        ) {
          const prior = earlyClipEvents.get(event.clipId);
          if (prior === undefined) {
            if (earlyClipEvents.size >= maxEarlyClipIds) {
              // A lost pre-ack start cannot be recast as a definite failure.
              for (const key of pendingBuilds) {
                const item = items.get(key);
                if (item !== undefined && item.phase !== "Terminal" && item.phase !== "Started") {
                  yield* emit(item, { _tag: "Unknown", terminal: true });
                  item.phase = "Terminal";
                }
              }
              yield* closeActor("Unattributed clip evidence exceeded its bound");
              return;
            }
            earlyClipEvents.set(event.clipId, [event]);
          } else if (
            !prior.some((seen) =>
              event._tag === "Started"
                ? seen._tag === "Started"
                : seen._tag === "Ended" || seen._tag === "Failed",
            )
          )
            prior.push(event);
        }
        const owner = owned.get(event.clipId);
        if (owner === undefined) return;
        if (owner._tag === "Filler") {
          if (event._tag === "Started")
            playingStartedMs.set(event.clipId, event.atMonotonicMillis ?? monotonicMillis(clock));
          if (event._tag === "Ended" || event._tag === "Failed") {
            owned.delete(event.clipId);
            if (filler.get(owner.index)?.clipId === event.clipId) filler.delete(owner.index);
            playingStartedMs.delete(event.clipId);
          }
          return;
        }
        const item = items.get(owner.key);
        if (item === undefined || item.phase === "Terminal") return;
        switch (event._tag) {
          case "Queued":
          case "Building":
            if (item.phase === "Accepted" || item.phase === "Unknown")
              yield* emit(item, { _tag: "Building" });
            break;
          case "Ready": {
            const record = knownRecord(yield* engine.state, event.clipId);
            if (record !== undefined) {
              item.sessionId = record.sessionId;
              if (item.phase !== "Started")
                yield* emit(item, { _tag: "Ready", sessionId: record.sessionId });
            }
            break;
          }
          case "Started": {
            if (item.phase === "Started") break;
            const state = yield* engine.state;
            const record = knownRecord(state, event.clipId);
            const sessionId = record?.sessionId ?? item.sessionId;
            if (sessionId === undefined) yield* emit(item, { _tag: "Unobserved" });
            else {
              item.sessionId = sessionId;
              const playing = Option.getOrUndefined(state.playing);
              item.startedAtMonoMs =
                event.atMonotonicMillis ??
                (playing?.clipId === event.clipId
                  ? (playing.startedAtMonotonicMillis ?? monotonicMillis(clock))
                  : monotonicMillis(clock));
              item.startedAtEpochMs = event.at;
              playingStartedMs.set(event.clipId, item.startedAtMonoMs);
              yield* emit(item, {
                _tag: "Started",
                at: event.at,
                sessionId,
                ...((item.atMs ?? item.startByMs) === undefined ||
                item.startedAtMonoMs <= (item.atMs ?? item.startByMs)!
                  ? {}
                  : { lateByMillis: item.startedAtMonoMs - (item.atMs ?? item.startByMs)! }),
              });
            }
            break;
          }
          case "Ended":
            yield* emit(
              item,
              item.startedAtMonoMs === undefined
                ? { _tag: "Unobserved" }
                : {
                    _tag: "Ended",
                    at: event.at ?? clock.currentTimeMillisUnsafe(),
                    termination: event.termination,
                    airedSeconds: Math.max(
                      0,
                      ((event.atMonotonicMillis ?? monotonicMillis(clock)) - item.startedAtMonoMs) /
                        1000,
                    ),
                  },
            );
            playingStartedMs.delete(event.clipId);
            break;
          case "Failed":
            yield* emit(item, { _tag: "Failed", reason: event.reason });
            break;
          default:
            break;
        }
      });

    const replayEarlyClipEvents = (clipId: ClipId): Effect.Effect<void> =>
      Effect.gen(function* () {
        const saved = earlyClipEvents.get(clipId);
        earlyClipEvents.delete(clipId);
        if (saved === undefined) return;
        for (const event of saved) yield* onEvent(event);
      });

    const executeCommand = (command: Command): Effect.Effect<CommandValue, EngineError> => {
      switch (command._tag) {
        case "Build":
        case "BuildFiller":
          return Effect.gen(function* () {
            if (
              command.sessionId === undefined ||
              Option.getOrUndefined((yield* engine.state).preferredSessionId) !== command.sessionId
            )
              return yield* PolicyFailure.refuse(
                "RouteChanged",
                "The preferred source changed before dispatch",
              );
            return yield* engine.enqueueOnSource === undefined
              ? engine.enqueue(command.request)
              : engine.enqueueOnSource(command.request, command.sessionId);
          });
        case "RemoveItem":
        case "RemoveFiller":
        case "RemoveDuplicate":
        case "DeferAt":
          return engine.remove(command.clipId);
        case "Order":
          return engine.move(command.clipId, command.position, "playout");
        case "PauseAutoplay":
          return engine.setAutoplay(false);
      }
    };

    const commandWorker = Effect.gen(function* () {
      for (;;) {
        const command = yield* Queue.take(commandQueue);
        const result = yield* Effect.result(executeCommand(command));
        yield* Queue.offer(inbox, { _tag: "CommandDone", command, result });
      }
    });

    const sendCommand = (command: Command): Effect.Effect<void> =>
      Effect.gen(function* () {
        if (command._tag === "Build") pendingBuilds.add(command.key);
        if (command._tag === "DeferAt") pendingAtDeferrals.add(command.key);
        commandCount++;
        yield* Queue.offer(commandQueue, command);
      });

    const canRetryRemoval = (clipId: ClipId): boolean =>
      monotonicMillis(clock) >= (removalRetryAtMs.get(clipId) ?? 0) &&
      (removalUnknownAtSerial.get(clipId) === undefined ||
        lastProcessedSnapshotSerial > removalUnknownAtSerial.get(clipId)!);

    const finishWithdrawal = (
      key: ItemKey,
      result: Result.Result<WithdrawOutcome, EngineError>,
    ): Effect.Effect<void> =>
      Effect.gen(function* () {
        const pending = pendingWithdrawals.get(key);
        pendingWithdrawals.delete(key);
        if (pending === undefined) return;
        for (const reply of pending.replies)
          if (Result.isSuccess(result)) yield* Deferred.succeed(reply, result.success);
          else yield* Deferred.fail(reply, result.failure);
      });

    const sendItemRemoval = (item: Entry, reason: "late" | "withdrawn"): Effect.Effect<void> =>
      Effect.gen(function* () {
        if (
          item.clipId === undefined ||
          pendingItemRemovals.has(item.key) ||
          !canRetryRemoval(item.clipId)
        )
          return;
        pendingItemRemovals.add(item.key);
        yield* sendCommand({ _tag: "RemoveItem", key: item.key, clipId: item.clipId, reason });
      });

    const requestWithdrawal = (
      item: Entry,
      reason: "late" | "withdrawn",
      reply?: Deferred.Deferred<WithdrawOutcome, EngineError>,
    ): Effect.Effect<void> =>
      Effect.gen(function* () {
        if (item.phase === "Started" || item.phase === "Terminal") {
          if (reply !== undefined)
            yield* Deferred.succeed(
              reply,
              item.phase === "Started" ? "already-started" : "not-found",
            );
          return;
        }
        const pending = pendingWithdrawals.get(item.key);
        if (pending !== undefined) {
          if (reply !== undefined) pending.replies.push(reply);
          return;
        }
        pendingWithdrawals.set(item.key, { reason, replies: reply === undefined ? [] : [reply] });
        if (pendingAtDeferrals.has(item.key)) return;
        if (item.clipId === undefined) {
          if (item.phase === "Unknown" || pendingBuilds.has(item.key)) return;
          yield* emit(item, { _tag: "Dropped", reason });
          yield* finishWithdrawal(item.key, Result.succeed("withdrawn"));
          return;
        }
        yield* sendItemRemoval(item, reason);
      });

    const applyPolicy = (action: PolicyAction, state: EngineState): Effect.Effect<void> =>
      Effect.gen(function* () {
        switch (action._tag) {
          case "Withdraw": {
            const item = items.get(action.key);
            if (item !== undefined) yield* requestWithdrawal(item, action.reason);
            break;
          }
          case "DeferAt":
            yield* sendCommand({ _tag: "DeferAt", key: action.key, clipId: action.clipId });
            break;
          case "WithdrawFiller":
            if (!pendingFillerRemovals.has(action.clipId) && canRetryRemoval(action.clipId)) {
              pendingFillerRemovals.add(action.clipId);
              yield* sendCommand({ _tag: "RemoveFiller", clipId: action.clipId });
            }
            break;
          case "Order": {
            const signature =
              state.ready.map((record) => record.clipId).join(",") +
              ":" +
              action.clipId +
              ":" +
              action.position;
            if (blockedMove !== signature)
              yield* sendCommand({
                _tag: "Order",
                clipId: action.clipId,
                position: action.position,
                signature,
              });
            break;
          }
          case "Build": {
            const item = items.get(action.key);
            if (item?.phase !== "Accepted") break;
            const prepared = yield* Effect.result(keyedRequest(item.request, item.key));
            if (Result.isFailure(prepared)) {
              yield* emit(item, { _tag: "Failed", reason: prepared.failure.message });
              break;
            }
            yield* sendCommand({
              _tag: "Build",
              key: item.key,
              request: prepared.success,
              sessionId: Option.getOrUndefined(state.preferredSessionId),
            });
            break;
          }
          case "BuildFiller": {
            const captured =
              upcomingFiller === undefined
                ? yield* Effect.result(
                    captureFiller(
                      fillerIndex,
                      runwaySeconds(state, monotonicMillis(clock), playingStartedMs, owned, [
                        ...items.values(),
                      ]),
                      action.targetSeconds,
                    ),
                  )
                : Result.succeed(upcomingFiller);
            if (Result.isFailure(captured)) {
              yield* closeActor(captured.failure.message);
              break;
            }
            upcomingFiller = captured.success;
            const prepared = yield* Effect.result(
              keyedRequest(captured.success, fillerKey(fillerIndex)),
            );
            if (Result.isFailure(prepared)) {
              yield* closeActor(prepared.failure.message);
              break;
            }
            yield* sendCommand({
              _tag: "BuildFiller",
              index: fillerIndex,
              request: prepared.success,
              sessionId: Option.getOrUndefined(state.preferredSessionId),
            });
            break;
          }
        }
      });

    const resolveRetiredUnknown = (item: Entry): Effect.Effect<void> =>
      Effect.gen(function* () {
        if (item.phase !== "Unknown" || item.clipId !== undefined) return;
        item.unknownSessionId = undefined;
        // A retired source cannot play this key again, but it may have played
        // during a missed observation. Preserve Unknown instead of replaying it.
        yield* emit(item, { _tag: "Unknown", terminal: true });
        item.phase = "Terminal";
        const pending = pendingWithdrawals.get(item.key);
        if (pending !== undefined)
          yield* finishWithdrawal(
            item.key,
            Result.fail(
              item.unknownCause ??
                PolicyFailure.refuse(
                  "SessionRecovering",
                  "The original source retired without an enqueue result",
                ),
            ),
          );
      });

    const handleCommandDone = (
      command: Command,
      result: Result.Result<CommandValue, EngineError>,
    ): Effect.Effect<void> =>
      Effect.gen(function* () {
        commandCount--;
        if (ended !== undefined) {
          if (command._tag === "Build") pendingBuilds.delete(command.key);
          if (command._tag === "DeferAt") pendingAtDeferrals.delete(command.key);
          if (command._tag === "RemoveItem") pendingItemRemovals.delete(command.key);
          if (command._tag === "RemoveFiller") pendingFillerRemovals.delete(command.clipId);
          return;
        }
        switch (command._tag) {
          case "Build": {
            const item = items.get(command.key);
            pendingBuilds.delete(command.key);
            if (item === undefined) break;
            if (Result.isSuccess(result)) {
              const clipId = result.success as ClipId;
              if (item.clipId !== undefined && item.clipId !== clipId) {
                yield* sendCommand({ _tag: "RemoveDuplicate", clipId });
                break;
              }
              item.clipId = clipId;
              item.acknowledgedAtSnapshotSerial = snapshotAcquiredSerial;
              owned.set(clipId, { _tag: "Item", key: item.key });
              item.sessionId =
                knownRecord(yield* engine.state, clipId)?.sessionId ??
                (engine.enqueueOnSource === undefined ? undefined : command.sessionId);
              item.unknownSessionId = undefined;
              item.unknownCause = undefined;
              yield* replayEarlyClipEvents(clipId);
              const pending = pendingWithdrawals.get(item.key);
              if (pending !== undefined) {
                if (
                  item.phase === "Started" ||
                  item.status._tag === "Ended" ||
                  item.status._tag === "Unobserved"
                )
                  yield* finishWithdrawal(item.key, Result.succeed("already-started"));
                else if (item.phase === "Terminal")
                  yield* finishWithdrawal(item.key, Result.succeed("not-found"));
                else yield* sendItemRemoval(item, pending.reason);
              } else if (item.phase === "Accepted" || item.phase === "Unknown")
                yield* emit(item, { _tag: "Building" });
              yield* observed(yield* engine.state);
            } else if (result.failure.context.outcome === "unknown") {
              if (item.clipId === undefined) {
                // A generic engine may re-route between the state read and dispatch.
                // Only an owner-fenced enqueue can attribute its unknown result.
                item.unknownSessionId =
                  engine.enqueueOnSource === undefined ? undefined : command.sessionId;
                item.unknownCause = result.failure;
                yield* emit(item, { _tag: "Unknown" });
              }
            } else {
              const pending = pendingWithdrawals.get(item.key);
              if (pending !== undefined) {
                yield* emit(item, { _tag: "Dropped", reason: pending.reason });
                yield* finishWithdrawal(item.key, Result.succeed("withdrawn"));
              } else if (
                result.failure.context.outcome !== "not-submitted" ||
                !["QueueFull", "SessionRecovering", "RouteChanged"].includes(
                  result.failure.reason._tag,
                )
              )
                yield* emit(item, { _tag: "Failed", reason: result.failure.message });
              else item.retryAtMs = monotonicMillis(clock) + 1_000;
            }
            break;
          }
          case "BuildFiller": {
            if (Result.isSuccess(result)) {
              const clipId = result.success as ClipId;
              const actualSessionId =
                knownRecord(yield* engine.state, clipId)?.sessionId ??
                (engine.enqueueOnSource === undefined ? undefined : command.sessionId);
              owned.set(clipId, {
                _tag: "Filler",
                index: command.index,
                sessionId: actualSessionId,
              });
              filler.set(command.index, {
                index: command.index,
                clipId,
                sessionId: actualSessionId,
                acknowledgedAtSnapshotSerial: snapshotAcquiredSerial,
              });
              fillerIndex = Math.max(fillerIndex, command.index + 1);
              upcomingFiller = undefined;
              yield* replayEarlyClipEvents(clipId);
            } else if (result.failure.context.outcome === "unknown") {
              if (!filler.has(command.index))
                unknownFiller = {
                  index: command.index,
                  sessionId: engine.enqueueOnSource === undefined ? undefined : command.sessionId,
                };
            } else {
              fillerRetryAtMs = monotonicMillis(clock) + 1_000;
              if (result.failure.context.outcome === "replied") {
                fillerIndex = Math.max(fillerIndex, command.index + 1);
                upcomingFiller = undefined;
              }
            }
            yield* observed(yield* engine.state);
            break;
          }
          case "RemoveItem": {
            pendingItemRemovals.delete(command.key);
            const item = items.get(command.key);
            if (Result.isSuccess(result)) {
              removalRetryAtMs.delete(command.clipId);
              removalUnknownAtSerial.delete(command.clipId);
              removedIds.add(command.clipId);
              owned.delete(command.clipId);
              playingStartedMs.delete(command.clipId);
              if (item?.phase === "Started")
                yield* finishWithdrawal(command.key, Result.succeed("already-started"));
              else {
                if (item !== undefined && item.phase !== "Terminal")
                  yield* emit(item, { _tag: "Dropped", reason: command.reason });
                yield* finishWithdrawal(command.key, Result.succeed("withdrawn"));
              }
            } else {
              removalRetryAtMs.set(command.clipId, monotonicMillis(clock) + 1_000);
              if (result.failure.context.outcome === "unknown")
                removalUnknownAtSerial.set(command.clipId, snapshotAcquiredSerial);
              if (item?.phase === "Started")
                yield* finishWithdrawal(command.key, Result.succeed("already-started"));
              else yield* finishWithdrawal(command.key, Result.fail(result.failure));
            }
            break;
          }
          case "RemoveFiller": {
            pendingFillerRemovals.delete(command.clipId);
            if (Result.isSuccess(result)) {
              removalRetryAtMs.delete(command.clipId);
              removalUnknownAtSerial.delete(command.clipId);
              removedIds.add(command.clipId);
              const owner = owned.get(command.clipId);
              if (owner?._tag === "Filler" && filler.get(owner.index)?.clipId === command.clipId)
                filler.delete(owner.index);
              owned.delete(command.clipId);
              playingStartedMs.delete(command.clipId);
            } else {
              removalRetryAtMs.set(command.clipId, monotonicMillis(clock) + 1_000);
              if (result.failure.context.outcome === "unknown")
                removalUnknownAtSerial.set(command.clipId, snapshotAcquiredSerial);
            }
            break;
          }
          case "RemoveDuplicate":
            if (Result.isFailure(result))
              yield* closeActor("A duplicate keyed clip could not be withdrawn");
            break;
          case "DeferAt": {
            pendingAtDeferrals.delete(command.key);
            const item = items.get(command.key);
            if (Result.isFailure(result)) {
              if (item?.phase !== "Started" && item?.phase !== "Terminal")
                yield* closeActor("A timed clip could not be held for its anchor");
              break;
            }
            owned.delete(command.clipId);
            removedIds.add(command.clipId);
            playingStartedMs.delete(command.clipId);
            if (item === undefined || item.phase === "Started" || item.phase === "Terminal") break;
            delete item.clipId;
            item.sessionId = undefined;
            const pending = pendingWithdrawals.get(command.key);
            if (pending !== undefined) {
              yield* emit(item, { _tag: "Dropped", reason: pending.reason });
              yield* finishWithdrawal(command.key, Result.succeed("withdrawn"));
            } else yield* emit(item, { _tag: "Accepted" });
            break;
          }
          case "Order":
            blockedMove = Result.isFailure(result) ? command.signature : undefined;
            break;
          case "PauseAutoplay":
            if (Result.isFailure(result)) {
              drainFailure = result.failure;
              const replies = drainReplies.splice(0);
              for (const reply of replies) yield* Deferred.fail(reply, result.failure);
            }
            break;
        }
      });

    const drainPending = (state: EngineState): Effect.Effect<void> =>
      Effect.gen(function* () {
        if (!draining) return;
        const playingId = Option.getOrUndefined(state.playing)?.clipId;
        for (const item of items.values()) {
          if (item.phase === "Terminal" || item.phase === "Started") continue;
          if (item.clipId === playingId) continue;
          yield* requestWithdrawal(item, "withdrawn");
        }
        for (const entry of filler.values()) {
          if (
            entry.clipId !== undefined &&
            entry.clipId !== playingId &&
            !pendingFillerRemovals.has(entry.clipId) &&
            canRetryRemoval(entry.clipId)
          ) {
            pendingFillerRemovals.add(entry.clipId);
            yield* sendCommand({ _tag: "RemoveFiller", clipId: entry.clipId });
          }
        }
      });

    const projectedStartMs = (state: EngineState, lane: string, nowMs: number): number => {
      const playing = Option.getOrUndefined(state.playing);
      const playingRecord =
        playing === undefined ? undefined : Option.getOrUndefined(playing.record);
      const started = playing === undefined ? undefined : playingStartedMs.get(playing.clipId);
      const restMs =
        playingRecord === undefined || started === undefined
          ? 0
          : Math.max(0, playingRecord.durationSeconds * 1000 - (nowMs - started));
      const laneRank = lanes.indexOf(lane);
      const preferred = Option.getOrUndefined(state.preferredSessionId);
      const aheadMs = state.ready.reduce((total, record) => {
        if (record.sessionId !== preferred) return total;
        const owner = owned.get(record.clipId);
        if (owner?._tag === "Filler") return total;
        if (owner?._tag === "Item") {
          const prior = items.get(owner.key);
          if (prior !== undefined && lanes.indexOf(prior.lane) > laneRank) return total;
        }
        return total + record.durationSeconds * 1000;
      }, 0);
      return nowMs + restMs + aheadMs;
    };

    const reconcile = Effect.gen(function* () {
      const state = yield* engine.state;
      refreshAnchors();
      if (!observationReady) {
        yield* publishState(state);
        return;
      }
      if (unknownFiller !== undefined) {
        const uncertain = unknownFiller;
        const sourceRetired =
          uncertain.sessionId !== undefined &&
          !state.sessions.some((session) => session.sessionId === uncertain.sessionId);
        if (sourceRetired) {
          unknownFiller = undefined;
          fillerIndex = Math.max(fillerIndex, uncertain.index + 1);
          upcomingFiller = undefined;
        } else if (
          uncertain.sessionId !== undefined &&
          Option.getOrUndefined(state.preferredSessionId) !== uncertain.sessionId &&
          fillerIndex <= uncertain.index
        ) {
          // A replacement may build independently, with a distinct key. The
          // uncertain old request can still be adopted if it later appears.
          fillerIndex = uncertain.index + 1;
          upcomingFiller = undefined;
        }
      }
      for (const [key, pending] of pendingWithdrawals) {
        const item = items.get(key);
        if (item === undefined) continue;
        if (item.phase === "Started")
          yield* finishWithdrawal(key, Result.succeed("already-started"));
        else if (item.clipId !== undefined) yield* sendItemRemoval(item, pending.reason);
      }
      for (const item of items.values())
        if (
          item.phase === "Unknown" &&
          item.clipId === undefined &&
          item.unknownSessionId !== undefined &&
          !state.sessions.some((session) => session.sessionId === item.unknownSessionId)
        )
          yield* resolveRetiredUnknown(item);
      if (draining) yield* drainPending(state);
      const decision = plan({
        engine: state,
        items: [...items.values()],
        owned,
        lanes,
        nowMs: monotonicMillis(clock),
        playingStartedMs,
        floorSeconds,
        targetSeconds,
        refillActive,
        maxBuildsInFlight,
        accepting,
        fillerRetryAtMs,
        fillerUnknown: unknownFiller !== undefined,
        fillerUnknownSessionId: unknownFiller?.sessionId,
        blockedMove,
      });
      refillActive = decision.refillActive;
      yield* publishState(state);
      if (!draining && commandCount === 0 && decision.action !== undefined)
        yield* applyPolicy(decision.action, state);
      if (
        drainFailure === undefined &&
        drainReplies.length > 0 &&
        commandCount === 0 &&
        pendingWithdrawals.size === 0 &&
        unknownFiller === undefined
      ) {
        const after = yield* engine.state;
        if (Option.isNone(after.playing) && !activeIds(after).some((clipId) => owned.has(clipId))) {
          drained = true;
          const replies = drainReplies.splice(0);
          for (const reply of replies) yield* Deferred.succeed(reply, undefined);
        }
      }
    });

    const actor = Effect.gen(function* () {
      for (;;) {
        const message = yield* Queue.take(inbox);
        if (ended !== undefined && message._tag !== "CommandDone") {
          if (message._tag === "Submit")
            yield* Deferred.fail(message.reply, invalid("Scheduler is closed"));
          else if (message._tag === "Withdraw") yield* Deferred.succeed(message.reply, "not-found");
          else if (message._tag === "Drain")
            yield* Deferred.fail(message.reply, PolicyFailure.refuse("SessionClosed", ended));
          continue;
        }
        switch (message._tag) {
          case "Submit": {
            if (!accepting) {
              yield* Deferred.fail(message.reply, invalid("Scheduler is draining or closed"));
              break;
            }
            const existing = items.get(message.item.key);
            if (existing !== undefined) {
              if (existing.fingerprint === message.item.fingerprint)
                yield* Deferred.succeed(message.reply, existing.handle);
              else yield* Deferred.fail(message.reply, KeyMismatch.of(message.item.key));
              break;
            }
            const nowMs = monotonicMillis(clock);
            const state = yield* engine.state;
            const startByMs =
              message.item.startByOffsetMs === undefined
                ? undefined
                : nowMs + message.item.startByOffsetMs;
            const projectedMs = projectedStartMs(state, message.item.lane, nowMs);
            if (startByMs !== undefined && projectedMs > startByMs) {
              yield* Deferred.fail(message.reply, WouldMissDeadline.of(message.item.key));
              break;
            }
            const startedWaiter = yield* Deferred.make<AsRunStatus>();
            const outcomeWaiter = yield* Deferred.make<AsRunStatus>();
            const firstDecisiveWaiter = yield* Deferred.make<FirstDecisiveStatus>();
            const handle: ItemHandle = {
              key: message.item.key,
              started: Deferred.await(startedWaiter),
              outcome: Deferred.await(outcomeWaiter),
              firstDecisive: Deferred.await(firstDecisiveWaiter),
            };
            const item: Entry = {
              key: message.item.key,
              lane: message.item.lane,
              request: message.item.request,
              fingerprint: message.item.fingerprint,
              admission: ++admission,
              phase: "Accepted",
              status: { _tag: "Accepted" },
              ...(message.item.notBeforeOffsetMs === undefined
                ? {}
                : { notBeforeMs: nowMs + message.item.notBeforeOffsetMs }),
              ...(startByMs === undefined ? {} : { startByMs }),
              firm: message.item.firm,
              ...(message.item.atWallMs === undefined
                ? {}
                : {
                    atWallMs: message.item.atWallMs,
                    atMs: nowMs + message.item.atWallMs - clock.currentTimeMillisUnsafe(),
                  }),
              late: message.item.late,
              sessionId: undefined,
              unknownSessionId: undefined,
              unknownCause: undefined,
              handle,
              startedWaiter,
              outcomeWaiter,
              firstDecisiveWaiter,
            };
            items.set(item.key, item);
            yield* PubSub.publish(events, {
              key: item.key,
              at: clock.currentTimeMillisUnsafe(),
              status: item.status,
            });
            yield* observed(state);
            yield* Deferred.succeed(message.reply, handle);
            break;
          }
          case "Withdraw": {
            const item = items.get(message.key);
            if (item === undefined) yield* Deferred.succeed(message.reply, "not-found");
            else yield* requestWithdrawal(item, "withdrawn", message.reply);
            break;
          }
          case "Drain": {
            if (drainFailure !== undefined) {
              yield* Deferred.fail(message.reply, drainFailure);
              break;
            }
            accepting = false;
            if (drained) {
              yield* Deferred.succeed(message.reply, undefined);
              break;
            }
            drainReplies.push(message.reply);
            if (!draining) {
              draining = true;
              yield* sendCommand({ _tag: "PauseAutoplay" });
            }
            yield* drainPending(yield* engine.state);
            break;
          }
          case "Snapshot": {
            lastProcessedSnapshotSerial = message.acquiredSerial;
            observationReady = true;
            yield* observed(message.state, true);
            blockedMove = undefined;
            break;
          }
          case "Event":
            yield* onEvent(message.event);
            yield* observed(yield* engine.state);
            blockedMove = undefined;
            break;
          case "Tick":
            break;
          case "CommandDone":
            yield* handleCommandDone(message.command, message.result);
            if (commandCount === 0) earlyClipEvents.clear();
            break;
          case "Closed":
            yield* closeActor(message.reason);
            yield* Deferred.fail(initialized, ReactorError.fromCode("Closed", message.reason));
            break;
        }
        if (ended === undefined) yield* reconcile;
        if (message._tag === "Snapshot") yield* Deferred.succeed(initialized, undefined);
      }
    });

    const observe = Effect.gen(function* () {
      for (;;) {
        const result = yield* Effect.result(
          Effect.scoped(
            Effect.gen(function* () {
              // Claim the serial before acquiring the initial state. A snapshot
              // begun before an enqueue result must not disprove its clip.
              const acquiredSerial = ++snapshotAcquiredSerial;
              const observation = yield* engine.observe({ capacity: 4096 });
              yield* Queue.offer(inbox, {
                _tag: "Snapshot",
                state: observation.initial,
                acquiredSerial,
              });
              yield* Stream.runForEach(observation.events, (event) =>
                Queue.offer(inbox, { _tag: "Event", event }),
              );
            }),
          ),
        );
        if (Result.isSuccess(result)) {
          yield* Queue.offer(inbox, { _tag: "Closed", reason: "The orchestration closed" });
          return;
        }
        if (result.failure.reason._tag !== "Overflow") {
          yield* Queue.offer(inbox, { _tag: "Closed", reason: result.failure.message });
          return;
        }
      }
    });
    yield* Effect.forkScoped(commandWorker);
    yield* Effect.forkScoped(actor);
    yield* Effect.forkScoped(observe);
    yield* Effect.forkScoped(
      Effect.gen(function* () {
        for (;;) {
          yield* Effect.sleep("100 millis");
          yield* Queue.offer(inbox, { _tag: "Tick" });
        }
      }),
    );
    yield* Queue.offer(inbox, { _tag: "Tick" });
    yield* Effect.addFinalizer(() =>
      Effect.forEach(
        [...items.values()].filter((item) => item.phase !== "Terminal"),
        (item) =>
          Effect.gen(function* () {
            if (item.phase === "Unknown") {
              yield* emit(item, { _tag: "Unknown", terminal: true });
              item.phase = "Terminal";
            } else yield* emit(item, { _tag: "Failed", reason: ended ?? "The scheduler closed" });
          }),
        { discard: true },
      ),
    );
    yield* Deferred.await(initialized);

    const submit: SchedulerShape["submit"] = (
      input,
    ): Effect.Effect<ItemHandle, KeyMismatch | WouldMissDeadline | EngineError> =>
      Effect.gen(function* () {
        const item = yield* captureItem(input, new Set(lanes));
        yield* Effect.annotateCurrentSpan("reactor.scheduler.item.key", item.key);
        const reply = yield* Deferred.make<
          ItemHandle,
          KeyMismatch | WouldMissDeadline | EngineError
        >();
        yield* Queue.offer(inbox, { _tag: "Submit", item, reply });
        return yield* Deferred.await(reply);
      }).pipe(Effect.withSpan("Scheduler.submit", {}, { captureStackTrace: false }));
    const withdraw: SchedulerShape["withdraw"] = (key) =>
      Effect.gen(function* () {
        const reply = yield* Deferred.make<WithdrawOutcome, EngineError>();
        yield* Queue.offer(inbox, { _tag: "Withdraw", key, reply });
        return yield* Deferred.await(reply);
      }).pipe(
        Effect.withSpan(
          "Scheduler.withdraw",
          { attributes: { key } },
          {
            captureStackTrace: false,
          },
        ),
      );
    const drain: SchedulerShape["drain"] = Effect.gen(function* () {
      const reply = yield* Deferred.make<void, EngineError>();
      yield* Queue.offer(inbox, { _tag: "Drain", reply });
      yield* Deferred.await(reply);
    }).pipe(Effect.withSpan("Scheduler.drain", {}, { captureStackTrace: false }));
    return {
      submit,
      withdraw,
      drain,
      state: SubscriptionRef.get(stateRef),
      asRun: Stream.fromPubSub(events),
    };
  });

export const layerScheduler = (
  options: SchedulerOptions,
): Layer.Layer<Scheduler, ReactorError | PolicyFailure, Engine> =>
  Layer.effect(Scheduler, makeScheduler(options));
