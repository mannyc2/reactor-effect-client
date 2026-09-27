import * as Clock from "effect/Clock";
import * as Context from "effect/Context";
import * as Deferred from "effect/Deferred";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as PubSub from "effect/PubSub";
import * as Queue from "effect/Queue";
import * as Result from "effect/Result";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import * as SubscriptionRef from "effect/SubscriptionRef";
import { take as takeQueue } from "../_internal/queue.js";
import { duration } from "../duration.js";
import { parsedInput, ReactorError } from "../errors.js";
import type { ReactorFailure } from "../errors.js";
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
import { estimatesFrom, plan, projectedStartMs, runwaySeconds } from "./scheduler-policy.js";
import type { DropReason, OwnedClip, PlannedItem, PolicyAction } from "./scheduler-policy.js";
import type { EngineError, EngineEvent, EngineState, RemoveOutcome } from "./types.js";
import { Engine } from "./types.js";

/** Stable caller identity for a scheduled item. */
export const ItemKey = Schema.NonEmptyString.pipe(Schema.brand("ItemKey"));
export type ItemKey = typeof ItemKey.Type;

export interface LaneSpec {
  /** Lanes are listed from highest to lowest priority. */
  readonly name: string;
  /**
   * What a new submission does to the lane's items still waiting: queue behind them (the
   * default); replace them, make-before-break, so they stay only as cover until it is Ready
   * and then settle `Dropped` as `replaced`; or skip, refusing it with `LaneBusy` while the
   * lane has an item waiting or playing. Inserts and replacements place themselves and are
   * not affected.
   */
  readonly conflict?: "queue" | "replace" | "skip";
  /**
   * A Ready item of this lane cuts a playing clip of a lower lane, or filler, instead of
   * waiting for its end, unless that clip ends within a second anyway. The provider stops
   * the clip, which cannot resume, and starts the next Ready one.
   */
  readonly cut?: boolean;
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
  /** Completed keys retained for idempotency, oldest first; defaults to 4096. Active keys are never evicted. */
  readonly maxHistory?: number;
  /** Unknown admission service deadline; defaults to 60 seconds, positive and finite up to 10 minutes. */
  readonly unknownRecoveryTimeout?: Duration.Input;
}

export type ItemFailureReason =
  | { readonly _tag: "Scheduler"; readonly cause: ReactorFailure }
  | { readonly _tag: "Command"; readonly cause: EngineError }
  | { readonly _tag: "Clip"; readonly message: string };

export type AsRunStatus =
  | { readonly _tag: "Accepted" }
  | { readonly _tag: "Building" }
  | { readonly _tag: "Ready"; readonly sessionId: string }
  | {
      readonly _tag: "Started";
      readonly at: number;
      readonly sessionId: string;
      readonly durationSeconds: number;
      readonly lateByMillis?: number;
    }
  | {
      readonly _tag: "Ended";
      readonly at: number;
      readonly termination: "finished" | "stopped";
      readonly airedSeconds: number;
    }
  | { readonly _tag: "Dropped"; readonly reason: "late" | "withdrawn" | "replaced" }
  | { readonly _tag: "Failed"; readonly reason: ItemFailureReason }
  | { readonly _tag: "Unobserved" }
  | {
      readonly _tag: "Unknown";
      /** Source retirement or scheduler failure ended reconciliation; remote fate remains unknown. */
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

/** One part of a group: its own key, and the clip it asks for. */
export interface GroupPart {
  readonly key: ItemKey;
  readonly request: ClipRequest;
}

export interface GroupSpec {
  readonly key: ItemKey;
  readonly lane: string;
  /** Built in order and aired back to back; a higher lane may still go in between parts. */
  readonly parts: readonly [GroupPart, ...ReadonlyArray<GroupPart>];
  /** Relative to admission, measured on the monotonic clock; it applies to the first part. */
  readonly window?: ItemSpec["window"];
}

export interface GroupHandle {
  readonly key: ItemKey;
  /** One handle per part, in order. */
  readonly parts: readonly [ItemHandle, ...ReadonlyArray<ItemHandle>];
}

/** The clip that takes a queued item's place, under a key of its own. */
export interface ReplacementSpec {
  readonly key: ItemKey;
  readonly request: ClipRequest;
}

/**
 * A clip that airs immediately before or after an anchor: an item, a group part or a
 * group key. Give exactly one of `before` and `after`.
 */
export interface InsertSpec {
  readonly key: ItemKey;
  readonly request: ClipRequest;
  readonly before?: ItemKey;
  readonly after?: ItemKey;
  /** Relative to admission, measured on the monotonic clock. */
  readonly window?: ItemSpec["window"];
}

export type WithdrawOutcome = "withdrawn" | "already-started" | "not-found";

/** One edit in a batch applied together. */
export type Edit =
  | { readonly _tag: "Submit"; readonly item: ItemSpec }
  | { readonly _tag: "SubmitGroup"; readonly group: GroupSpec }
  | { readonly _tag: "Insert"; readonly insert: InsertSpec }
  | { readonly _tag: "Replace"; readonly key: ItemKey; readonly next: ReplacementSpec }
  | { readonly _tag: "Withdraw"; readonly key: ItemKey };

/** What one edit of a batch did: the handle of what it added, or its withdrawal's outcome. */
export type EditResult =
  | { readonly _tag: "Added"; readonly handle: ItemHandle }
  | { readonly _tag: "AddedGroup"; readonly handle: GroupHandle }
  | { readonly _tag: "Withdrawal"; readonly outcome: Effect.Effect<WithdrawOutcome, EngineError> };

export interface EditHandle {
  /** One result per edit, in the order given. */
  readonly results: ReadonlyArray<EditResult>;
  /**
   * Resolves when the batch takes effect together: every clip it adds is Ready or has
   * settled, and its withdrawals and replacements are requested at once. Fails with the
   * scheduler's closing refusal if it closes first.
   */
  readonly committed: Effect.Effect<void, EngineError>;
}

export interface DrainOptions {
  /** Finish only current playback, or every item already accepted. Defaults to playing. */
  readonly finish?: "playing" | "accepted";
}

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
  /**
   * What the scheduler has measured from its own recent builds: seconds from sending a
   * build to its clip being Ready, per requested second of clip (absent until three builds
   * were measured), and a built clip's actual length over its requested length.
   */
  readonly estimates: {
    readonly build: { readonly median: number; readonly p95: number } | undefined;
    readonly length: number;
  };
}

export class KeyMismatch extends Schema.TaggedError<KeyMismatch>()("KeyMismatch", {
  key: ItemKey,
}) {
  static of(key: ItemKey): KeyMismatch {
    return new KeyMismatch({ key });
  }
}

export class LaneBusy extends Schema.TaggedError<LaneBusy>()("LaneBusy", {
  key: ItemKey,
  lane: Schema.String,
}) {
  static of(key: ItemKey, lane: string): LaneBusy {
    return new LaneBusy({ key, lane });
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

interface UnknownAdmission {
  readonly sessionId: string | undefined;
  readonly atMs: number;
  readonly cause: EngineError;
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
  /** Set once another item was submitted to take this one's place. */
  replacedBy?: ItemKey;
  dispatchedAtMs?: number;
  retryAtMs?: number;
  atMs?: number;
  clipId?: ClipId;
  sessionId: string | undefined;
  unknownSessionId: string | undefined;
  unknownCause: EngineError | undefined;
  unknownAtMs: number | undefined;
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
      readonly reason: DropReason;
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
  | { readonly _tag: "PauseAutoplay" }
  | { readonly _tag: "Cut"; readonly clipId: ClipId };

type CommandValue = ClipId | RemoveOutcome | void;

type EarlyClipEvent = Extract<EngineEvent, { readonly _tag: "Started" | "Ended" | "Failed" }>;

const maxEarlyClipIds = 4096;

interface Batch {
  readonly id: number;
  readonly waitFor: ReadonlyArray<ItemKey>;
  /** Items the batch withdraws, each with its reason and any reply its withdrawal settles. */
  readonly targets: Map<
    ItemKey,
    {
      readonly reason: DropReason;
      readonly reply: Deferred.Deferred<WithdrawOutcome, EngineError> | undefined;
    }
  >;
  readonly committed: Deferred.Deferred<void, EngineError>;
}

interface PendingWithdrawal {
  readonly reason: DropReason;
  readonly replies: Deferred.Deferred<WithdrawOutcome, EngineError>[];
}

type Message =
  | {
      readonly _tag: "Edit";
      readonly edits: ReadonlyArray<CapturedEdit>;
      /** A batch holds what it adds until it commits; a single call does not. */
      readonly batched: boolean;
      readonly reply: Deferred.Deferred<
        EditHandle,
        KeyMismatch | WouldMissDeadline | LaneBusy | EngineError
      >;
    }
  | {
      readonly _tag: "Withdraw";
      readonly key: ItemKey;
      readonly reply: Deferred.Deferred<WithdrawOutcome, EngineError>;
    }
  | {
      readonly _tag: "Drain";
      readonly finish: "playing" | "accepted";
      readonly reply: Deferred.Deferred<void, EngineError>;
    }
  | { readonly _tag: "Snapshot"; readonly state: EngineState; readonly acquiredSerial: number }
  | { readonly _tag: "Event"; readonly event: EngineEvent }
  | { readonly _tag: "Tick" }
  | {
      readonly _tag: "CommandDone";
      readonly command: Command;
      readonly result: Result.Result<CommandValue, EngineError>;
    }
  | { readonly _tag: "Closed"; readonly cause: ReactorFailure };

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

/** A Duration tuple or object read from own data properties; no accessor runs. */
const durationData = (input: object): Result.Result<Duration.Input, PolicyFailure> => {
  if (Array.isArray(input)) {
    const descriptors: Record<string, PropertyDescriptor> = Object.getOwnPropertyDescriptors(input);
    if (
      Object.getOwnPropertySymbols(input).length > 0 ||
      Object.keys(descriptors).some(
        (field) => !["0", "1", "length"].includes(field) || !("value" in descriptors[field]!),
      ) ||
      descriptors.length?.value !== 2 ||
      descriptors["0"] === undefined ||
      descriptors["1"] === undefined
    )
      return Result.fail(invalid("Duration tuple has unsupported fields or accessors"));
    const seconds: unknown = descriptors["0"].value;
    const nanos: unknown = descriptors["1"].value;
    return Result.succeed([seconds, nanos] as readonly [number, number]);
  }
  const fields = ownedData(
    input,
    ["weeks", "days", "hours", "minutes", "seconds", "milliseconds", "microseconds", "nanoseconds"],
    "Duration",
  );
  return Result.isFailure(fields) ? Result.fail(fields.failure) : Result.succeed(fields.success);
};

const captureDuration = (input: unknown): Effect.Effect<Duration.Input, PolicyFailure> =>
  Effect.gen(function* () {
    if (input === null || typeof input !== "object" || Duration.isDuration(input))
      return input as Duration.Input;
    return yield* Effect.fromResult(durationData(input));
  });

/**
 * A Duration option read as caller data: a data property, own or inherited,
 * captured without running any accessor. Rejections throw for `parsedInput`.
 */
const optionDuration = (
  options: object,
  field: string,
  name: string,
): Duration.Input | undefined => {
  let descriptor: PropertyDescriptor | undefined;
  for (
    let owner: object | null = options;
    descriptor === undefined && owner !== null;
    owner = Object.getPrototypeOf(owner) as object | null
  )
    descriptor = Object.getOwnPropertyDescriptor(owner, field);
  if (descriptor === undefined) return undefined;
  const reject = () =>
    ReactorError.fromCode("InvalidInput", `${name} must be a Duration of data properties`);
  if (!("value" in descriptor)) throw reject();
  const value: unknown = descriptor.value;
  if (value === undefined) return undefined;
  if (value === null) throw reject();
  if (typeof value !== "object" || Duration.isDuration(value)) return value;
  const captured = durationData(value);
  if (Result.isFailure(captured)) throw reject();
  return captured.success;
};

const requestDuration = (input: unknown, name: string, allowZero = false) =>
  Effect.gen(function* () {
    const captured = yield* captureDuration(input);
    return yield* parsedInput(
      () =>
        Duration.toMillis(
          duration(captured, name, { allowZero, allowNegative: name === "startBy" }),
        ),
      "submit",
    ).pipe(Effect.mapError((error) => invalid(error.message)));
  });

/** Capture scheduling fields before reading the request's placement fields. */
/** A caller's clip request, without the placement and source fields the scheduler owns. */
const captureClip = (input: unknown) =>
  Effect.gen(function* (): Effect.fn.Return<ClipRequest, PolicyFailure> {
    const request = yield* captureRequest(input as ClipRequest);
    if (
      request.position !== undefined ||
      request.before !== undefined ||
      request.sameSessionAs !== undefined ||
      request.continueFrom !== undefined ||
      request.sequence !== undefined
    )
      return yield* invalid("Scheduler owns placement and source selection");
    return request;
  });

/** A window's offsets from admission, read as caller data. */
const captureWindow = (input: unknown) =>
  Effect.gen(function* (): Effect.fn.Return<
    {
      readonly notBeforeOffsetMs: number | undefined;
      readonly startByOffsetMs: number | undefined;
      readonly firm: boolean;
    },
    PolicyFailure
  > {
    const window =
      input === undefined
        ? undefined
        : yield* Effect.fromResult(
            ownedData(input, ["notBefore", "startBy", "firm"], "Scheduled window"),
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
          startByOffsetMs >= 0 &&
          notBeforeOffsetMs > startByOffsetMs))
    )
      return yield* invalid("Scheduled window is inconsistent");
    return { notBeforeOffsetMs, startByOffsetMs, firm: window?.firm === true };
  });

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
    const request = yield* captureClip(descriptors.request?.value);
    const { notBeforeOffsetMs, startByOffsetMs, firm } = yield* captureWindow(
      descriptors.window?.value,
    );
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
      firm,
      atWallMs,
      late,
      fingerprint: JSON.stringify({
        lane,
        request,
        notBeforeOffsetMs,
        startByOffsetMs,
        firm,
        atWallMs,
        late,
      }),
    };
  });

interface CapturedGroup {
  readonly key: ItemKey;
  /** Only the first part carries the group's window. */
  readonly parts: readonly [CapturedItem, ...ReadonlyArray<CapturedItem>];
  readonly fingerprint: string;
}

/** Enough parts for a spoken line of beats, and a bound on one submission's work. */
const maxGroupParts = 64;

const captureGroup = (input: GroupSpec, lanes: ReadonlySet<string>) =>
  Effect.gen(function* (): Effect.fn.Return<CapturedGroup, PolicyFailure> {
    const group = yield* Effect.fromResult(
      ownedData(input, ["key", "lane", "parts", "window"], "Scheduled group"),
    );
    const key = yield* Schema.decodeUnknownEffect(ItemKey)(group.key).pipe(
      Effect.mapError(() => invalid("Scheduled group key must be nonempty")),
    );
    if (isReservedSchedulerKey(key)) return yield* invalid("Scheduled group key is reserved");
    if (!Array.isArray(group.parts))
      return yield* invalid("Scheduled group parts must be an array");
    // Read as a plain object: a mapped array type would hide the descriptors.
    const descriptors = Object.getOwnPropertyDescriptors(group.parts as object);
    const length: unknown = descriptors.length?.value;
    if (typeof length !== "number" || length < 1 || length > maxGroupParts)
      return yield* invalid(`Scheduled group needs 1 to ${maxGroupParts} parts`);
    const parts: CapturedItem[] = [];
    for (let index = 0; index < length; index++) {
      const descriptor = descriptors[String(index)];
      if (descriptor === undefined || !("value" in descriptor))
        return yield* invalid("Scheduled group parts must be data");
      const part = yield* Effect.fromResult(
        ownedData(descriptor.value, ["key", "request"], "Scheduled group part"),
      );
      parts.push(
        yield* captureItem(
          {
            key: part.key,
            lane: group.lane,
            request: part.request,
            ...(index === 0 && group.window !== undefined ? { window: group.window } : {}),
          } as ItemSpec,
          lanes,
        ),
      );
    }
    const [first, ...rest] = parts;
    if (first === undefined || new Set([key, ...parts.map((part) => part.key)]).size !== length + 1)
      return yield* invalid("Scheduled group and part keys must be distinct");
    return {
      key,
      parts: [first, ...rest],
      fingerprint: JSON.stringify(parts.map((part) => [part.key, part.fingerprint])),
    };
  });

interface CapturedReplacement {
  readonly key: ItemKey;
  readonly request: ClipRequest;
}

const captureReplacement = (input: ReplacementSpec) =>
  Effect.gen(function* (): Effect.fn.Return<CapturedReplacement, PolicyFailure> {
    const next = yield* Effect.fromResult(ownedData(input, ["key", "request"], "Replacement"));
    const key = yield* Schema.decodeUnknownEffect(ItemKey)(next.key).pipe(
      Effect.mapError(() => invalid("Replacement key must be nonempty")),
    );
    if (isReservedSchedulerKey(key)) return yield* invalid("Replacement key is reserved");
    return { key, request: yield* captureClip(next.request) };
  });

interface CapturedInsert {
  readonly key: ItemKey;
  readonly request: ClipRequest;
  readonly side: "before" | "after";
  readonly anchor: ItemKey;
  readonly notBeforeOffsetMs: number | undefined;
  readonly startByOffsetMs: number | undefined;
  readonly firm: boolean;
  readonly fingerprint: string;
}

const captureInsert = (input: InsertSpec) =>
  Effect.gen(function* (): Effect.fn.Return<CapturedInsert, PolicyFailure> {
    const spec = yield* Effect.fromResult(
      ownedData(input, ["key", "request", "before", "after", "window"], "Insert"),
    );
    const key = yield* Schema.decodeUnknownEffect(ItemKey)(spec.key).pipe(
      Effect.mapError(() => invalid("Insert key must be nonempty")),
    );
    if (isReservedSchedulerKey(key)) return yield* invalid("Insert key is reserved");
    if ((spec.before === undefined) === (spec.after === undefined))
      return yield* invalid("Insert needs exactly one of before or after");
    const side = spec.before === undefined ? "after" : "before";
    const anchor = yield* Schema.decodeUnknownEffect(ItemKey)(spec.before ?? spec.after).pipe(
      Effect.mapError(() => invalid("Insert anchor must be a nonempty key")),
    );
    const request = yield* captureClip(spec.request);
    const window = yield* captureWindow(spec.window);
    return {
      key,
      request,
      side,
      anchor,
      ...window,
      fingerprint: JSON.stringify({ [side]: anchor, request, ...window }),
    };
  });

type CapturedEdit =
  | { readonly _tag: "Submit"; readonly item: CapturedItem }
  | { readonly _tag: "SubmitGroup"; readonly group: CapturedGroup }
  | { readonly _tag: "Insert"; readonly insert: CapturedInsert }
  | { readonly _tag: "Replace"; readonly key: ItemKey; readonly next: CapturedReplacement }
  | { readonly _tag: "Withdraw"; readonly key: ItemKey };

/** A bound on one batch's work, generous for a spoken plan of several lines. */
const maxEdits = 256;

const editKey = (input: unknown, name: string) =>
  Schema.decodeUnknownEffect(ItemKey)(input).pipe(
    Effect.mapError(() => invalid(`${name} must be a nonempty key`)),
  );

const captureEdits = (input: ReadonlyArray<Edit>, lanes: ReadonlySet<string>) =>
  Effect.gen(function* (): Effect.fn.Return<ReadonlyArray<CapturedEdit>, PolicyFailure> {
    if (!Array.isArray(input)) return yield* invalid("Edits must be an array");
    // Read as a plain object: a mapped array type would hide the descriptors.
    const descriptors = Object.getOwnPropertyDescriptors(input as object);
    const length: unknown = descriptors.length?.value;
    if (typeof length !== "number" || length < 1 || length > maxEdits)
      return yield* invalid(`An edit batch needs 1 to ${maxEdits} edits`);
    const edits: CapturedEdit[] = [];
    for (let index = 0; index < length; index++) {
      const descriptor = descriptors[String(index)];
      if (descriptor === undefined || !("value" in descriptor))
        return yield* invalid("Edits must be data");
      const raw: unknown = descriptor.value;
      const tag: unknown =
        raw !== null && typeof raw === "object"
          ? Object.getOwnPropertyDescriptor(raw, "_tag")?.value
          : undefined;
      switch (tag) {
        case "Submit": {
          const edit = yield* Effect.fromResult(ownedData(raw, ["_tag", "item"], "Edit"));
          edits.push({ _tag: "Submit", item: yield* captureItem(edit.item as ItemSpec, lanes) });
          break;
        }
        case "SubmitGroup": {
          const edit = yield* Effect.fromResult(ownedData(raw, ["_tag", "group"], "Edit"));
          edits.push({
            _tag: "SubmitGroup",
            group: yield* captureGroup(edit.group as GroupSpec, lanes),
          });
          break;
        }
        case "Insert": {
          const edit = yield* Effect.fromResult(ownedData(raw, ["_tag", "insert"], "Edit"));
          edits.push({ _tag: "Insert", insert: yield* captureInsert(edit.insert as InsertSpec) });
          break;
        }
        case "Replace": {
          const edit = yield* Effect.fromResult(ownedData(raw, ["_tag", "key", "next"], "Edit"));
          edits.push({
            _tag: "Replace",
            key: yield* editKey(edit.key, "A replaced key"),
            next: yield* captureReplacement(edit.next as ReplacementSpec),
          });
          break;
        }
        case "Withdraw": {
          const edit = yield* Effect.fromResult(ownedData(raw, ["_tag", "key"], "Edit"));
          edits.push({ _tag: "Withdraw", key: yield* editKey(edit.key, "A withdrawn key") });
          break;
        }
        default:
          return yield* invalid("An edit's _tag is not one the scheduler knows");
      }
    }
    return edits;
  });

export interface SchedulerShape {
  readonly submit: (
    item: ItemSpec,
  ) => Effect.Effect<ItemHandle, KeyMismatch | WouldMissDeadline | LaneBusy | EngineError>;
  readonly submitGroup: (
    group: GroupSpec,
  ) => Effect.Effect<GroupHandle, KeyMismatch | WouldMissDeadline | LaneBusy | EngineError>;
  /**
   * Builds `next` to take the queued item's place, lane and group position. Once `next` is
   * Ready the item is withdrawn as `replaced`; if the item starts first, `next` is withdrawn.
   */
  readonly replace: (
    key: ItemKey,
    next: ReplacementSpec,
  ) => Effect.Effect<ItemHandle, KeyMismatch | EngineError>;
  /**
   * Places a clip immediately before or after an anchor, in the anchor's lane. Between two
   * parts of a group it joins the group, but its own failure or withdrawal never breaks the
   * group. An insert that is not Ready in time for its place airs at the next boundary.
   */
  readonly insert: (
    spec: InsertSpec,
  ) => Effect.Effect<ItemHandle, KeyMismatch | WouldMissDeadline | EngineError>;
  /**
   * Applies several edits as one change, make-before-break: every edit is checked before any
   * takes effect, and a refused edit fails the whole batch. What the batch withdraws or
   * replaces keeps its place, and may still air, until every clip the batch adds is Ready or
   * has settled; the withdrawals and replacements then happen together, and the added clips
   * take their places. A withdrawn item that nothing was built for yet goes at once. Clips the
   * batch adds wait behind everything else on air until then. Each key may appear once.
   */
  readonly edit: (
    edits: ReadonlyArray<Edit>,
  ) => Effect.Effect<EditHandle, KeyMismatch | WouldMissDeadline | LaneBusy | EngineError>;
  /**
   * A group key withdraws every unstarted part; a part key, that part and every part after it.
   * An inserted part withdraws only itself.
   */
  readonly withdraw: (key: ItemKey) => Effect.Effect<WithdrawOutcome, EngineError>;
  readonly drain: (options?: DrainOptions) => Effect.Effect<void, EngineError>;
  /** Published after local handle/control settlement; includes Closed when the owning scope ends. */
  readonly failure: Effect.Effect<ReactorFailure>;
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
      if (
        options.lanes.some(
          (lane) =>
            (lane.conflict !== undefined &&
              !["queue", "replace", "skip"].includes(lane.conflict)) ||
            (lane.cut !== undefined && typeof lane.cut !== "boolean"),
        )
      )
        throw ReactorError.fromCode("InvalidInput", "A lane's conflict or cut is invalid");
      return options.lanes.map((lane) => lane.name);
    }, "makeScheduler");
    const conflicts = new Map(options.lanes.map((lane) => [lane.name, lane.conflict ?? "queue"]));
    const cutLanes: ReadonlySet<string> = new Set(
      options.lanes.filter((lane) => lane.cut === true).map((lane) => lane.name),
    );
    const { floorSeconds, targetSeconds, maxBuildsInFlight, maxHistory, unknownRecoveryMs } =
      yield* parsedInput(() => {
        const floorSeconds =
          Duration.toMillis(
            duration(options.filler.runway.floor, "runway floor", { allowZero: true }),
          ) / 1000;
        const targetSeconds =
          Duration.toMillis(duration(options.filler.runway.target, "runway target")) / 1000;
        const maxBuildsInFlight = options.maxBuildsInFlight ?? 1;
        const maxHistory = options.maxHistory ?? 4096;
        const unknownRecoveryMs = Duration.toMillis(
          duration(
            optionDuration(options, "unknownRecoveryTimeout", "Unknown recovery timeout") ??
              "60 seconds",
            "unknown recovery timeout",
            { maximum: "10 minutes" },
          ),
        );
        if (
          floorSeconds > targetSeconds ||
          !Number.isSafeInteger(maxBuildsInFlight) ||
          maxBuildsInFlight < 1 ||
          maxBuildsInFlight > 1024 ||
          !Number.isSafeInteger(maxHistory) ||
          maxHistory < 0 ||
          maxHistory > 65_536
        )
          throw ReactorError.fromCode("InvalidInput", "Scheduler runway or build cap is invalid");
        return { floorSeconds, targetSeconds, maxBuildsInFlight, maxHistory, unknownRecoveryMs };
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
    const unknownFillers = new Map<number, UnknownAdmission & { readonly index: number }>();
    const maxUnknownFillers = 4096;
    let upcomingFiller: ClipRequest | undefined = yield* captureFiller(0, 0);
    const actorScope = yield* Effect.scope;
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
      estimates: { build: undefined, length: 1 },
    });
    const initialized = yield* Deferred.make<void, ReactorError>();
    const stopped = yield* Deferred.make<ReactorFailure>();
    const settled = yield* Deferred.make<void>();
    const closedCall = Deferred.await(stopped).pipe(
      Effect.flatMap((cause) => PolicyFailure.refuse("SessionClosed", cause.message)),
    );
    const items = new Map<ItemKey, Entry>();
    const history = new Map<ItemKey, Pick<Entry, "fingerprint" | "handle">>();
    /** Kept while any part is active or in history, for idempotency and withdrawal. */
    const groups = new Map<
      ItemKey,
      {
        readonly fingerprint: string;
        readonly handle: GroupHandle;
        /** Every part's key, replacements included; a part's place is its group index. */
        readonly parts: ItemKey[];
        /** The first part that failed or was dropped: the parts after it are withdrawn. */
        brokenAt: number | undefined;
      }
    >();
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
    let ended: Exit.Exit<ReactorFailure> | undefined;
    let drainFailure: EngineError | undefined;
    const drainReplies: Deferred.Deferred<void, EngineError>[] = [];
    const pendingWithdrawals = new Map<ItemKey, PendingWithdrawal>();
    /** Edit batches that have not taken effect yet. */
    const batches = new Map<number, Batch>();
    let batchCount = 0;
    const pendingBuilds = new Set<ItemKey>();
    // A Build whose enqueue has started and whose result the worker has not
    // yet seen. The provider may still accept it after the scheduler ends.
    let dispatching: ItemKey | undefined;
    const pendingAtDeferrals = new Set<ItemKey>();
    const pendingItemRemovals = new Set<ItemKey>();
    const pendingFillerRemovals = new Set<ClipId>();
    const removalRetryAtMs = new Map<ClipId, number>();
    const removalUnknownAtSerial = new Map<ClipId, number>();
    let commandCount = 0;
    let snapshotAcquiredSerial = 0;
    let lastProcessedSnapshotSerial = 0;
    let draining = false;
    let finishAccepted = false;
    let autoplayPaused = false;
    let drained = false;
    let observationReady = false;
    let blockedMove: string | undefined;
    let blockedCut: ClipId | undefined;
    // Recent measured builds: build seconds per requested second, and actual over requested length.
    const buildSamples: number[] = [];
    const lengthSamples: number[] = [];
    const maxSamples = 32;
    let lastFillerSeconds: number | undefined;
    /** Filler builds sent and not yet Ready, by filler index. */
    const fillerDispatch = new Map<number, { readonly atMs: number; readonly seconds: number }>();
    const measure = (atMs: number, requested: number, actual: number): void => {
      if (!(requested > 0)) return;
      buildSamples.push((monotonicMillis(clock) - atMs) / 1000 / requested);
      lengthSamples.push(actual / requested);
      if (buildSamples.length > maxSamples) buildSamples.shift();
      if (lengthSamples.length > maxSamples) lengthSamples.shift();
    };
    /** An item's build is measured once, when its clip is first seen Ready. */
    const measureItem = (item: Entry, actual: number): void => {
      if (item.dispatchedAtMs === undefined) return;
      measure(item.dispatchedAtMs, item.request.durationSeconds, actual);
      delete item.dispatchedAtMs;
    };

    const emit = (entry: Entry, status: AsRunStatus, terminal = false): Effect.Effect<void> =>
      Effect.gen(function* () {
        if (ended !== undefined && !terminal) return;
        if (JSON.stringify(entry.status) === JSON.stringify(status)) return;
        entry.status = status;
        entry.phase =
          status._tag === "Accepted" ||
          status._tag === "Building" ||
          status._tag === "Ready" ||
          status._tag === "Started"
            ? status._tag
            : status._tag === "Unknown" && status.terminal !== true
              ? "Unknown"
              : "Terminal";
        const replaced = entry.replaces === undefined ? undefined : items.get(entry.replaces);
        if (
          entry.group !== undefined &&
          entry.inserted !== true &&
          (status._tag === "Failed" || status._tag === "Dropped") &&
          !(status._tag === "Dropped" && status.reason === "replaced") &&
          (replaced === undefined || replaced.phase === "Terminal")
        ) {
          const group = groups.get(entry.group.key);
          if (group !== undefined)
            group.brokenAt = Math.min(group.brokenAt ?? entry.group.index, entry.group.index);
        }
        // Publish in the step that records it (the PubSub is unbounded): a claim
        // from another fiber cannot land between the two, so asRun never carries
        // a settlement ahead of a recorded status, nor omits one.
        PubSub.publishUnsafe(events, {
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
          (status._tag === "Unknown" && status.terminal === true)
        ) {
          yield* Deferred.succeed(entry.startedWaiter, status);
          yield* Deferred.succeed(entry.outcomeWaiter, status);
        }
      });

    // The claim is synchronous; interruption masking alone would still allow
    // two closers to publish conflicting results. Join only local bookkeeping,
    // never the worker scope (a worker can itself own terminal settlement).
    const terminate = (terminal: Exit.Exit<ReactorFailure>): Effect.Effect<void> =>
      Effect.suspend(() => {
        if (ended !== undefined) return Deferred.await(settled);
        ended = terminal;
        accepting = false;
        // Wake the watchdog so an episode left armed by another owner cannot
        // keep re-arming its timer.
        Deferred.doneUnsafe(recoveryChanged, Exit.void);
        return Effect.gen(function* () {
          for (const item of items.values()) {
            if (Exit.isFailure(terminal)) {
              yield* Deferred.failCause(item.startedWaiter, terminal.cause);
              yield* Deferred.failCause(item.outcomeWaiter, terminal.cause);
              yield* Deferred.failCause(item.firstDecisiveWaiter, terminal.cause);
              continue;
            }
            // emit records and publishes a status before completing its waits,
            // and a claim can land in between: a completion can resume an
            // observer that closes the owner, and another fiber can claim while
            // the actor is preempted. Finish those waits from the record.
            const recorded = item.status;
            if (item.phase === "Started" || item.phase === "Terminal") {
              if (isFirstDecisive(recorded))
                yield* Deferred.succeed(item.firstDecisiveWaiter, recorded);
              yield* Deferred.succeed(item.startedWaiter, recorded);
            }
            if (item.phase === "Terminal") yield* Deferred.succeed(item.outcomeWaiter, recorded);
            else
              yield* emit(
                item,
                item.phase === "Unknown" ||
                  (item.clipId === undefined &&
                    (item.unknownAtMs !== undefined || item.key === dispatching))
                  ? { _tag: "Unknown", terminal: true }
                  : { _tag: "Failed", reason: { _tag: "Scheduler", cause: terminal.value } },
                true,
              );
          }
          const closed = Exit.isSuccess(terminal)
            ? Exit.fail(PolicyFailure.refuse("SessionClosed", terminal.value.message))
            : Exit.failCause(terminal.cause);
          for (const [key, pending] of pendingWithdrawals) {
            // A drop recorded before the claim is the withdrawal's actual result.
            const reply =
              Exit.isSuccess(terminal) && items.get(key)?.status._tag === "Dropped"
                ? Exit.succeed("withdrawn" as const)
                : closed;
            for (const waiter of pending.replies) yield* Deferred.done(waiter, reply);
          }
          pendingWithdrawals.clear();
          for (const batch of batches.values()) {
            for (const target of batch.targets.values())
              if (target.reply !== undefined) yield* Deferred.done(target.reply, closed);
            yield* Deferred.done(batch.committed, closed);
          }
          batches.clear();
          for (const reply of drainReplies.splice(0)) yield* Deferred.done(reply, closed);
          yield* SubscriptionRef.update(stateRef, (state) => ({ ...state, accepting: false }));
          yield* Deferred.done(
            initialized,
            Exit.isFailure(terminal)
              ? Exit.failCause(terminal.cause)
              : Exit.fail(ReactorError.fromCode("Closed", terminal.value.message)),
          );
          // Supervisors may close the owner immediately when this wakes them.
          yield* Deferred.done(stopped, terminal);
        }).pipe(
          Effect.onExit((exit) =>
            Effect.gen(function* () {
              // A bookkeeping defect must release joiners with that Cause, never
              // leave an unfinished latch or report a partial settlement as success.
              yield* Deferred.done(settled, exit);
              if (Exit.isFailure(exit)) {
                yield* Deferred.failCause(initialized, exit.cause);
                yield* Deferred.failCause(stopped, exit.cause);
              }
            }),
          ),
        );
      }).pipe(Effect.uninterruptible);

    const closeActor = (cause: ReactorFailure): Effect.Effect<void> =>
      terminate(Exit.succeed(cause));

    interface RecoveryDeadline {
      readonly deadlineMs: number;
      readonly sessionId: string | undefined;
      readonly cause: EngineError;
    }
    let recoveryState: EngineState | undefined;
    let capacityRecovery: RecoveryDeadline | undefined;
    let recoveryDeadline: RecoveryDeadline | undefined;
    let recoveryChanged = Deferred.makeUnsafe<void>();

    // Retain the original age while Closing hides the old source and acquisition
    // is still pending. Only accepted evidence or a Ready replacement clears it.
    const updateRecovery = (state = recoveryState, fresh?: UnknownAdmission): void => {
      if (ended !== undefined) return;
      recoveryState = state;
      const preferred =
        state === undefined ? undefined : Option.getOrUndefined(state.preferredSessionId);
      const ready =
        preferred !== undefined &&
        state?.sessions.some(
          (source) => source.sessionId === preferred && source.availability === "Ready",
        ) === true;
      let earliest: RecoveryDeadline | undefined;
      let drainDeadline: RecoveryDeadline | undefined;
      const consider = (
        atMs: number,
        sessionId: string | undefined,
        cause: EngineError,
        newlyObserved = false,
      ): void => {
        const deadlineMs = atMs + unknownRecoveryMs;
        if (
          ((!ready && newlyObserved) || sessionId === undefined || sessionId === preferred) &&
          (earliest === undefined || deadlineMs < earliest.deadlineMs)
        )
          earliest = { deadlineMs, sessionId, cause };
        if (draining && (drainDeadline === undefined || deadlineMs < drainDeadline.deadlineMs))
          drainDeadline = { deadlineMs, sessionId, cause };
      };
      for (const entry of unknownFillers.values())
        consider(entry.atMs, entry.sessionId, entry.cause);
      for (const item of items.values())
        if (
          item.phase !== "Terminal" &&
          item.clipId === undefined &&
          item.unknownAtMs !== undefined &&
          item.unknownCause !== undefined
        )
          consider(item.unknownAtMs, item.unknownSessionId, item.unknownCause);
      if (fresh !== undefined) consider(fresh.atMs, fresh.sessionId, fresh.cause, true);
      if (
        ready &&
        (earliest === undefined ||
          (capacityRecovery?.sessionId !== undefined && capacityRecovery.sessionId !== preferred))
      )
        capacityRecovery = undefined;
      if (
        earliest !== undefined &&
        (capacityRecovery === undefined || earliest.deadlineMs < capacityRecovery.deadlineMs)
      )
        capacityRecovery = earliest;
      const next =
        drainDeadline !== undefined &&
        (capacityRecovery === undefined || drainDeadline.deadlineMs < capacityRecovery.deadlineMs)
          ? drainDeadline
          : capacityRecovery;
      if (
        next?.deadlineMs !== recoveryDeadline?.deadlineMs ||
        next?.cause !== recoveryDeadline?.cause
      ) {
        recoveryDeadline = next;
        Deferred.doneUnsafe(recoveryChanged, Exit.void);
      }
    };

    const provedUnknown = (atMs: number | undefined): void => {
      if (atMs !== undefined && capacityRecovery?.deadlineMs === atMs + unknownRecoveryMs)
        capacityRecovery = undefined;
    };

    const watchdog = Effect.gen(function* () {
      for (;;) {
        // A committed result and its queued actor evidence can arrive in one
        // turn. Coalesce those revisions before allocating a replacement timer;
        // the original monotonic deadline remains unchanged.
        yield* Effect.yieldNow;
        if (ended !== undefined) return;
        recoveryChanged = Deferred.makeUnsafe<void>();
        const episode = recoveryDeadline;
        if (episode === undefined) {
          yield* Deferred.await(recoveryChanged);
          continue;
        }
        yield* Deferred.await(recoveryChanged).pipe(
          Effect.timeoutOrElse({
            duration: Math.max(0, episode.deadlineMs - monotonicMillis(clock)),
            orElse: () => Effect.void,
          }),
        );
        // A canceled timer may already have resumed. Recheck the retained
        // identity without awaiting the actor, Engine state, or its permits.
        if (
          ended === undefined &&
          recoveryDeadline === episode &&
          monotonicMillis(clock) >= episode.deadlineMs
        ) {
          const causes = new Set<EngineError>([episode.cause]);
          for (const item of items.values())
            if (item.unknownCause !== undefined) causes.add(item.unknownCause);
          for (const entry of unknownFillers.values()) causes.add(entry.cause);
          yield* closeActor(
            ReactorError.fromCode("Timeout", "Scheduler unknown recovery deadline elapsed", {
              operation: "scheduler.unknownRecovery",
              detail: [...causes],
            }),
          );
          return;
        }
      }
    });

    const prune = (state: EngineState): void => {
      const active = new Set(activeIds(state));
      if (state.availability === "Ready") {
        for (const id of removedIds) if (!active.has(id)) removedIds.delete(id);
        for (const id of removalRetryAtMs.keys()) if (!active.has(id)) removalRetryAtMs.delete(id);
        for (const id of removalUnknownAtSerial.keys())
          if (!active.has(id)) removalUnknownAtSerial.delete(id);
      }
      for (const [key, item] of items) {
        if (
          item.phase !== "Terminal" ||
          pendingBuilds.has(key) ||
          pendingWithdrawals.has(key) ||
          pendingAtDeferrals.has(key) ||
          pendingItemRemovals.has(key) ||
          (item.clipId !== undefined && active.has(item.clipId))
        )
          continue;
        if (item.clipId !== undefined) {
          owned.delete(item.clipId);
          playingStartedMs.delete(item.clipId);
        }
        history.set(key, { fingerprint: item.fingerprint, handle: item.handle });
        items.delete(key);
      }
      while (history.size > maxHistory) history.delete(history.keys().next().value!);
      for (const [key, group] of groups)
        if (!group.parts.some((part) => items.has(part) || history.has(part))) groups.delete(key);
    };

    // Capture membership and public rows in one pass. A fresh token reads every
    // source on every observation, including in-place changes to Engine arrays;
    // frozen empty rows can be shared by every published state without treating
    // array identity as proof.
    const noReady: SchedulerState["sessions"][number]["ready"] = Object.freeze([]);
    const sourceRows = new Map<
      string,
      { readonly empty: SchedulerState["sessions"][number]; seen: symbol }
    >();
    const projectSources = (state: EngineState) => {
      const seen = Symbol();
      const sessions = state.sessions.map<SchedulerState["sessions"][number]>(({ sessionId }) => {
        let cached = sourceRows.get(sessionId);
        if (cached === undefined) {
          cached = { empty: Object.freeze({ sessionId, ready: noReady }), seen };
          sourceRows.set(sessionId, cached);
        } else cached.seen = seen;
        if (state.ready.length === 0) return cached.empty;
        return {
          sessionId,
          ready: state.ready
            .filter((clip) => clip.sessionId === sessionId)
            .map((clip) => {
              const owner = owned.get(clip.clipId);
              return owner === undefined ? "other" : owner._tag === "Filler" ? "filler" : owner.key;
            }),
        };
      });
      // Bound cached rows by snapshot size without scanning unchanged overlap.
      // Membership still requires this observation's token, even before pruning.
      if (sourceRows.size > state.sessions.length)
        for (const [id, cached] of sourceRows) if (cached.seen !== seen) sourceRows.delete(id);
      // Only the serialized actor uses this membership view, before its next projection.
      return { sessions, has: (id: string) => sourceRows.get(id)?.seen === seen };
    };
    const publishState = (
      state: EngineState,
      sessions = projectSources(state).sessions,
    ): Effect.Effect<void> => {
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
        sessions,
        starved,
        estimates: estimatesFrom(buildSamples, lengthSamples),
      });
    };

    /** When the filler build in flight was sent, and the next filler clip's requested length. */
    const fillerView = () => ({
      fillerDispatchedAtMs:
        fillerDispatch.size === 0
          ? undefined
          : Math.min(...[...fillerDispatch.values()].map((sent) => sent.atMs)),
      fillerSeconds: upcomingFiller?.durationSeconds ?? lastFillerSeconds,
    });

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
        const keyed = new Map<string, ReturnType<typeof activeRecords>[number]>();
        for (const record of activeRecords(state)) {
          const key = keyFromProviderMetadata(record.provider.metadata);
          if (key !== undefined && !removedIds.has(record.clipId)) keyed.set(key, record);
          const index = fillerIndexFromKey(key);
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
          provedUnknown(unknownFillers.get(index)?.atMs);
          unknownFillers.delete(index);
          if (fillerIndex !== priorIndex) upcomingFiller = undefined;
        }
        for (const item of items.values()) {
          if (item.phase === "Terminal") continue;
          if (item.clipId === undefined) {
            const resumed = keyed.get(item.key);
            if (resumed !== undefined) {
              item.clipId = resumed.clipId;
              item.sessionId = resumed.sessionId;
              provedUnknown(item.unknownAtMs);
              item.unknownAtMs = undefined;
              item.unknownCause = undefined;
              item.unknownSessionId = undefined;
              owned.set(resumed.clipId, { _tag: "Item", key: item.key });
            }
          }
          const clipId = item.clipId;
          if (clipId === undefined) continue;
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
                durationSeconds: record.durationSeconds,
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
            measureItem(item, record.durationSeconds);
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
            yield* emit(item, {
              _tag: "Failed",
              reason: { _tag: "Clip", message: "The clip failed unobserved" },
            });
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
        updateRecovery(state);
      });

    const onEvent = (event: EngineEvent): Effect.Effect<void> =>
      Effect.gen(function* () {
        if (event._tag === "Starved") starved++;
        if (event._tag === "SessionFailed") {
          yield* closeActor(event.failure);
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
              yield* closeActor(
                ReactorError.fromCode("Overflow", "Unattributed clip evidence exceeded its bound"),
              );
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
          const sent = fillerDispatch.get(owner.index);
          if (event._tag === "Ready" && sent !== undefined)
            measure(sent.atMs, sent.seconds, event.durationSeconds);
          if (event._tag === "Ready" || event._tag === "Ended" || event._tag === "Failed")
            fillerDispatch.delete(owner.index);
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
            measureItem(item, event.durationSeconds);
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
                durationSeconds: event.durationSeconds,
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
            yield* emit(item, { _tag: "Failed", reason: { _tag: "Clip", message: event.reason } });
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
            // Termination can be claimed while the route is read; its settlement
            // must not be followed by a new dispatch.
            if (ended !== undefined)
              return yield* PolicyFailure.refuse("SessionClosed", "The scheduler closed");
            if (command._tag === "Build") dispatching = command.key;
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
        case "Cut":
          return engine.cut(command.clipId);
      }
    };

    const commandWorker = Effect.gen(function* () {
      for (;;) {
        const command = yield* takeQueue(commandQueue);
        if (ended !== undefined) continue;
        const result = yield* Effect.result(executeCommand(command));
        dispatching = undefined;
        // Capture uncertainty before actor delivery: drain may be awaiting a
        // permit in stopRenewal while this committed command finishes.
        if (
          ended === undefined &&
          Result.isFailure(result) &&
          result.failure.context.outcome === "unknown"
        ) {
          let fresh: UnknownAdmission | undefined;
          const sessionId =
            engine.enqueueOnSource === undefined ||
            (command._tag !== "Build" && command._tag !== "BuildFiller")
              ? undefined
              : command.sessionId;
          if (command._tag === "Build") {
            const item = items.get(command.key);
            if (item !== undefined && item.phase !== "Terminal" && item.clipId === undefined) {
              item.unknownAtMs ??= monotonicMillis(clock);
              item.unknownCause = result.failure;
              item.unknownSessionId = sessionId;
              fresh = { sessionId, atMs: item.unknownAtMs, cause: result.failure };
            }
          } else if (
            command._tag === "BuildFiller" &&
            !filler.has(command.index) &&
            !unknownFillers.has(command.index)
          ) {
            fresh = { sessionId, atMs: monotonicMillis(clock), cause: result.failure };
            unknownFillers.set(command.index, { ...fresh, index: command.index });
          }
          updateRecovery(recoveryState, fresh);
        }
        yield* Queue.offer(inbox, { _tag: "CommandDone", command, result });
      }
    });

    const sendCommand = (command: Command): Effect.Effect<void> =>
      Effect.gen(function* () {
        if (ended !== undefined) return;
        if (command._tag === "Build") {
          pendingBuilds.add(command.key);
          const item = items.get(command.key);
          if (item !== undefined) item.dispatchedAtMs = monotonicMillis(clock);
        }
        if (command._tag === "BuildFiller") {
          lastFillerSeconds = command.request.durationSeconds;
          fillerDispatch.set(command.index, {
            atMs: monotonicMillis(clock),
            seconds: command.request.durationSeconds,
          });
          // In-flight filler is bounded like the samples it feeds.
          for (const index of fillerDispatch.keys())
            if (fillerDispatch.size > maxSamples) fillerDispatch.delete(index);
        }
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

    const sendItemRemoval = (item: Entry, reason: DropReason): Effect.Effect<void> =>
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
      reason: DropReason,
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
          case "DeferAt":
            if (canRetryRemoval(action.clipId))
              yield* sendCommand({ _tag: "DeferAt", key: action.key, clipId: action.clipId });
            break;
          case "Cut":
            yield* sendCommand({ _tag: "Cut", clipId: action.clipId });
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
              yield* emit(item, {
                _tag: "Failed",
                reason: { _tag: "Command", cause: prepared.failure },
              });
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
            if (unknownFillers.size >= maxUnknownFillers) {
              yield* closeActor(
                ReactorError.fromCode("Overflow", "Scheduler uncertain filler ledger is full", {
                  operation: "scheduler.unknownRecovery",
                }),
              );
              break;
            }
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
              yield* closeActor(captured.failure);
              break;
            }
            upcomingFiller = captured.success;
            const prepared = yield* Effect.result(
              keyedRequest(captured.success, fillerKey(fillerIndex)),
            );
            if (Result.isFailure(prepared)) {
              yield* closeActor(prepared.failure);
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
              provedUnknown(item.unknownAtMs);
              item.unknownAtMs = undefined;
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
                yield* emit(item, {
                  _tag: "Failed",
                  reason: { _tag: "Command", cause: result.failure },
                });
              else item.retryAtMs = monotonicMillis(clock) + 1_000;
              delete item.dispatchedAtMs;
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
              // The worker already retained this identity and its original age.
            } else {
              fillerDispatch.delete(command.index);
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
              // A refusal is not an outcome: the withdrawal stays pending and is retried.
              // An unknown removal may already have taken effect, so it ends the wait.
              else if (result.failure.context.outcome === "unknown")
                yield* finishWithdrawal(command.key, Result.fail(result.failure));
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
            if (Result.isFailure(result)) yield* closeActor(result.failure);
            break;
          case "DeferAt": {
            pendingAtDeferrals.delete(command.key);
            const item = items.get(command.key);
            if (Result.isFailure(result)) {
              removalRetryAtMs.set(command.clipId, monotonicMillis(clock) + 1_000);
              if (result.failure.context.outcome === "unknown")
                removalUnknownAtSerial.set(command.clipId, snapshotAcquiredSerial);
              break;
            }
            removalRetryAtMs.delete(command.clipId);
            removalUnknownAtSerial.delete(command.clipId);
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
          case "Cut":
            if (Result.isFailure(result)) blockedCut = command.clipId;
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

    const pendingBatches = () =>
      [...batches.values()].map((batch) => ({
        id: batch.id,
        waitFor: batch.waitFor,
        targets: [...batch.targets].map(([key, target]) => ({ key, reason: target.reason })),
      }));

    /** The reply a batch keeps for its withdrawal of `key`, taken once. */
    const takeBatchReply = (
      key: ItemKey,
    ): Deferred.Deferred<WithdrawOutcome, EngineError> | undefined => {
      for (const batch of batches.values()) {
        const target = batch.targets.get(key);
        if (target === undefined) continue;
        batch.targets.delete(key);
        return target.reply;
      }
      return undefined;
    };

    /** A batch takes effect: what it withdraws goes at once, and its replacements take over. */
    const commitBatch = (id: number): Effect.Effect<void> =>
      Effect.gen(function* () {
        const batch = batches.get(id);
        if (batch === undefined) return;
        batches.delete(id);
        for (const [key, target] of batch.targets) {
          const item = items.get(key);
          if (item !== undefined) yield* requestWithdrawal(item, target.reason, target.reply);
          else if (target.reply !== undefined) yield* Deferred.succeed(target.reply, "not-found");
        }
        yield* Deferred.succeed(batch.committed, undefined);
      });

    const reconcile = Effect.gen(function* () {
      const state = yield* engine.state;
      if (ended !== undefined) return;
      prune(state);
      refreshAnchors();
      const sources = projectSources(state);
      if (!observationReady) {
        yield* publishState(state, sources.sessions);
        return;
      }
      for (const uncertain of unknownFillers.values()) {
        const sourceRetired =
          uncertain.sessionId !== undefined && !sources.has(uncertain.sessionId);
        if (sourceRetired) unknownFillers.delete(uncertain.index);
        if (
          sourceRetired ||
          (uncertain.sessionId !== undefined &&
            Option.getOrUndefined(state.preferredSessionId) !== uncertain.sessionId)
        ) {
          if (fillerIndex <= uncertain.index) {
            fillerIndex = uncertain.index + 1;
            upcomingFiller = undefined;
          }
        }
      }
      for (const [key, pending] of pendingWithdrawals) {
        const item = items.get(key);
        if (item === undefined) continue;
        if (item.phase === "Started")
          yield* finishWithdrawal(key, Result.succeed("already-started"));
        // A withdrawal still being retried when its item settles another way. Terminal
        // Unknown keeps its uncertain result from the retirement or terminal settlement.
        else if (item.phase === "Terminal" && item.status._tag !== "Unknown")
          yield* finishWithdrawal(
            key,
            Result.succeed(
              item.status._tag === "Dropped"
                ? "withdrawn"
                : item.status._tag === "Ended" || item.status._tag === "Unobserved"
                  ? "already-started"
                  : "not-found",
            ),
          );
        else if (item.clipId !== undefined) yield* sendItemRemoval(item, pending.reason);
      }
      for (const item of items.values())
        if (
          item.phase === "Unknown" &&
          item.clipId === undefined &&
          item.unknownSessionId !== undefined &&
          !sources.has(item.unknownSessionId)
        )
          yield* resolveRetiredUnknown(item);
      // After retirement, so a deadline no longer outlives the uncertainty that
      // set it until the next message.
      updateRecovery(state);
      const preferred = Option.getOrUndefined(state.preferredSessionId);
      let unknownFillerCount = 0;
      for (const entry of unknownFillers.values())
        if (entry.sessionId === undefined || entry.sessionId === preferred) unknownFillerCount++;
      const brokenGroups = new Map<ItemKey, number>();
      for (const [key, group] of groups)
        if (group.brokenAt !== undefined) brokenGroups.set(key, group.brokenAt);
      const decide = () =>
        plan({
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
          drain: draining ? (finishAccepted ? "accepted" : "playing") : undefined,
          fillerRetryAtMs,
          unknownFillerCount,
          blockedMove,
          withdrawing: new Set(pendingWithdrawals.keys()),
          dispatched: pendingBuilds,
          brokenGroups,
          batches: pendingBatches(),
          estimates: estimatesFrom(buildSamples, lengthSamples),
          ...fillerView(),
          cutLanes,
          blockedCut,
        });
      let decision = decide();
      // A commit changes what is withdrawn and what takes over, so the plan is read again.
      if (decision.commit.length > 0) {
        for (const id of decision.commit) yield* commitBatch(id);
        decision = decide();
      }
      refillActive = decision.refillActive;
      for (const { key, reason } of decision.withdraw) {
        const item = items.get(key);
        if (item !== undefined) yield* requestWithdrawal(item, reason, takeBatchReply(key));
      }
      for (const clipId of decision.withdrawFiller)
        if (!pendingFillerRemovals.has(clipId) && canRetryRemoval(clipId)) {
          pendingFillerRemovals.add(clipId);
          yield* sendCommand({ _tag: "RemoveFiller", clipId });
        }
      yield* publishState(state, sources.sessions);
      if ((!draining || finishAccepted) && commandCount === 0 && decision.action !== undefined)
        yield* applyPolicy(decision.action, state);
      if (
        draining &&
        finishAccepted &&
        !autoplayPaused &&
        commandCount === 0 &&
        [...items.values()].every((item) => item.phase === "Terminal")
      ) {
        autoplayPaused = true;
        yield* sendCommand({ _tag: "PauseAutoplay" });
      }
      if (
        drainFailure === undefined &&
        drainReplies.length > 0 &&
        commandCount === 0 &&
        [...items.values()].every((item) => item.phase === "Terminal") &&
        pendingWithdrawals.size === 0 &&
        unknownFillers.size === 0
      ) {
        const after = yield* engine.state;
        if (Option.isNone(after.playing) && !activeIds(after).some((clipId) => owned.has(clipId))) {
          drained = true;
          // A truthful completed drain no longer needs a replacement to resume
          // service, even when retirement left a capacity episode behind.
          capacityRecovery = undefined;
          updateRecovery(after);
          const replies = drainReplies.splice(0);
          for (const reply of replies) yield* Deferred.succeed(reply, undefined);
        }
      }
    });

    /** Registers a captured item, without yielding; its caller publishes the Accepted evidence. */
    const accept = (
      captured: CapturedItem,
      nowMs: number,
      startByMs: number | undefined,
      group: Entry["group"],
      place?: {
        readonly admission: number;
        readonly replaces?: ItemKey;
        readonly generation?: number;
        readonly inserted?: boolean;
        readonly notBeforeMs: number | undefined;
      },
    ): Entry => {
      const startedWaiter = Deferred.makeUnsafe<AsRunStatus>();
      const outcomeWaiter = Deferred.makeUnsafe<AsRunStatus>();
      const firstDecisiveWaiter = Deferred.makeUnsafe<FirstDecisiveStatus>();
      const item: Entry = {
        key: captured.key,
        lane: captured.lane,
        request: captured.request,
        fingerprint: captured.fingerprint,
        admission: place === undefined ? ++admission : place.admission,
        seconds: captured.request.durationSeconds,
        phase: "Accepted",
        status: { _tag: "Accepted" },
        ...(place === undefined
          ? captured.notBeforeOffsetMs === undefined
            ? {}
            : { notBeforeMs: nowMs + captured.notBeforeOffsetMs }
          : place.notBeforeMs === undefined
            ? {}
            : { notBeforeMs: place.notBeforeMs }),
        ...(place?.replaces === undefined
          ? {}
          : { replaces: place.replaces, generation: place.generation ?? 1 }),
        ...(place?.inserted === true ? { inserted: true } : {}),
        ...(startByMs === undefined ? {} : { startByMs }),
        firm: captured.firm,
        ...(captured.atWallMs === undefined
          ? {}
          : {
              atWallMs: captured.atWallMs,
              atMs: nowMs + captured.atWallMs - clock.currentTimeMillisUnsafe(),
            }),
        late: captured.late,
        ...(group === undefined ? {} : { group }),
        sessionId: undefined,
        unknownSessionId: undefined,
        unknownCause: undefined,
        unknownAtMs: undefined,
        handle: {
          key: captured.key,
          started: Deferred.await(startedWaiter),
          outcome: Deferred.await(outcomeWaiter),
          firstDecisive: Deferred.await(firstDecisiveWaiter),
        },
        startedWaiter,
        outcomeWaiter,
        firstDecisiveWaiter,
      };
      items.set(item.key, item);
      return item;
    };

    /**
     * One reply for withdrawals across a group's parts. A group key reports
     * `withdrawn` if any part was; a part key reports that part's own outcome,
     * while the parts after it are withdrawn too.
     */
    const settleWithdrawals = (
      replies: ReadonlyArray<Deferred.Deferred<WithdrawOutcome, EngineError>>,
      reply: Deferred.Deferred<WithdrawOutcome, EngineError>,
      wholeGroup: boolean,
    ): Effect.Effect<void> =>
      Effect.gen(function* () {
        const outcomes: WithdrawOutcome[] = [];
        for (const pending of replies) {
          const result = yield* Effect.result(Deferred.await(pending));
          if (Result.isFailure(result)) {
            yield* Deferred.fail(reply, result.failure);
            return;
          }
          outcomes.push(result.success);
        }
        yield* Deferred.succeed(
          reply,
          wholeGroup && outcomes.includes("withdrawn") ? "withdrawn" : (outcomes[0] ?? "not-found"),
        );
      });

    /**
     * The place an insert takes beside its anchor, or why it cannot: the anchor's lane, an
     * admission between the anchor and its neighbour in that lane, and a group place when
     * the insert falls between two parts of a group.
     */
    const placeInsert = (
      insert: CapturedInsert,
    ):
      | string
      | {
          readonly lane: string;
          readonly admission: number;
          readonly group?: NonNullable<Entry["group"]>;
        } => {
      const live = (entry: Entry | undefined): entry is Entry =>
        entry !== undefined && entry.phase !== "Terminal" && entry.replacedBy === undefined;
      const named = groups.get(insert.anchor);
      let anchor: Entry | undefined;
      if (named === undefined) anchor = items.get(insert.anchor);
      else {
        const parts = named.parts
          .map((key) => items.get(key))
          .filter(live)
          .sort((a, b) => a.group!.index - b.group!.index);
        if (
          insert.side === "before" &&
          (parts.some((part) => part.phase === "Started") || parts[0]?.group?.index !== 0)
        )
          return "The anchor group already started";
        anchor = insert.side === "before" ? parts[0] : parts.at(-1);
      }
      if (!live(anchor)) return "Nothing queued under the anchor key";
      if (anchor.phase === "Unknown") return "The anchor's admission is uncertain";
      if (insert.side === "before" && anchor.phase === "Started")
        return "The anchor already started";
      const neighbours = [...items.values()]
        .filter((entry) => entry.lane === anchor.lane && entry.phase !== "Terminal")
        .map((entry) => entry.admission);
      const bound =
        insert.side === "before"
          ? Math.max(...neighbours.filter((value) => value < anchor.admission))
          : Math.min(...neighbours.filter((value) => value > anchor.admission));
      const admission = Number.isFinite(bound)
        ? (anchor.admission + bound) / 2
        : anchor.admission + (insert.side === "before" ? -0.5 : 0.5);
      if (admission === anchor.admission || admission === bound)
        return "No room is left to insert at that place";
      let group: Entry["group"];
      if (anchor.group !== undefined) {
        const indexes = (groups.get(anchor.group.key)?.parts ?? [])
          .map((key) => items.get(key)?.group?.index)
          .filter((value): value is number => value !== undefined);
        const index = anchor.group.index;
        const neighbour =
          insert.side === "before"
            ? Math.max(...indexes.filter((value) => value < index))
            : Math.min(...indexes.filter((value) => value > index));
        // Before the first part or after the last one, the insert stays outside the group.
        // Once parts have aired and settled, the place before the first remaining part is
        // still inside the group.
        const between =
          insert.side === "before"
            ? index > 0
              ? (Number.isFinite(neighbour) ? neighbour : index - 1) + index
              : undefined
            : Number.isFinite(neighbour)
              ? index + neighbour
              : undefined;
        if (between !== undefined) {
          if (between / 2 === index || between / 2 === neighbour)
            return "No room is left to insert at that place";
          group = { key: anchor.group.key, index: between / 2 };
        }
      }
      return group === undefined
        ? { lane: anchor.lane, admission }
        : { lane: anchor.lane, admission, group };
    };

    /** The items a withdrawal of `key` names: a group's parts, a part and those after it, or one item. */
    const withdrawalTargets = (key: ItemKey): ReadonlyArray<Entry> => {
      const item = items.get(key);
      const named = groups.get(key);
      const group = named ?? (item?.group === undefined ? undefined : groups.get(item.group.key));
      if (group === undefined || (named === undefined && item?.inserted === true))
        return item === undefined ? [] : [item];
      const from = named === undefined ? item!.group!.index : 0;
      const targets = group.parts
        .map((part) => items.get(part))
        .filter((part): part is Entry => part?.group !== undefined && part.group.index >= from);
      // A part key's reply is that part's own outcome, so it goes first.
      if (named === undefined) targets.sort((a, b) => Number(b === item) - Number(a === item));
      return targets;
    };

    type StagedResult =
      | { readonly _tag: "Added"; readonly handle: ItemHandle }
      | { readonly _tag: "AddedGroup"; readonly handle: GroupHandle }
      | { readonly _tag: "Withdrawal"; readonly key: ItemKey };

    interface Staged {
      readonly results: ReadonlyArray<StagedResult>;
      /** Items this call admitted, in order; their Accepted evidence is still unpublished. */
      readonly added: ReadonlyArray<Entry>;
      /** Items the batch waits for: those it added and any it repeats that another batch holds. */
      readonly waitFor: ReadonlyArray<ItemKey>;
      /** Waiting items of replace lanes that what it added supersedes. */
      readonly replaced: ReadonlyArray<ItemKey>;
    }

    /**
     * Checks and admits a call's edits in order, without yielding. A refused edit undoes the
     * edits before it, so nothing of a refused batch is admitted or published.
     */
    const stageEdits = (
      edits: ReadonlyArray<CapturedEdit>,
      batched: boolean,
      state: EngineState,
      nowMs: number,
    ): Result.Result<Staged, KeyMismatch | WouldMissDeadline | LaneBusy | PolicyFailure> => {
      const undo: Array<() => void> = [];
      const added: Entry[] = [];
      const waitFor: ItemKey[] = [];
      const replaced: ItemKey[] = [];
      const results: StagedResult[] = [];
      const refuse = (error: KeyMismatch | WouldMissDeadline | LaneBusy | PolicyFailure) => {
        for (const step of undo.reverse()) step();
        return Result.fail(error);
      };
      // Clips a batch withdraws or replaces do not count ahead of the clips it adds.
      const excluding = new Set<ItemKey>();
      const named: ItemKey[] = [];
      for (const edit of edits)
        switch (edit._tag) {
          case "Submit":
            named.push(edit.item.key);
            break;
          case "SubmitGroup":
            named.push(edit.group.key, ...edit.group.parts.map((part) => part.key));
            break;
          case "Insert":
            named.push(edit.insert.key);
            break;
          case "Replace":
            named.push(edit.key, edit.next.key);
            excluding.add(edit.key);
            break;
          case "Withdraw": {
            const targets = withdrawalTargets(edit.key);
            named.push(
              edit.key,
              ...targets.map((target) => target.key).filter((key) => key !== edit.key),
            );
            for (const target of targets) excluding.add(target.key);
            break;
          }
        }
      if (new Set(named).size !== named.length)
        return refuse(invalid("An edit batch names each key once"));
      for (const edit of edits)
        if (edit._tag === "Insert" && excluding.has(edit.insert.anchor))
          return refuse(invalid("An insert's anchor is withdrawn or replaced in the same batch"));
      const view = (place: Parameters<typeof projectedStartMs>[1]) =>
        projectedStartMs(
          {
            engine: state,
            items: [...items.values()].filter((item) => !excluding.has(item.key)),
            owned,
            lanes,
            playingStartedMs,
            nowMs,
            batches: pendingBatches(),
            estimates: estimatesFrom(buildSamples, lengthSamples),
            ...fillerView(),
            withdrawing: new Set(pendingWithdrawals.keys()),
          },
          place,
        );
      /**
       * A new submission's lane policy: a skip lane refuses it while the lane has an item
       * waiting or playing; a replace lane supersedes the items still waiting there.
       */
      const conflict = (key: ItemKey, lane: string): LaneBusy | undefined => {
        const others = [...items.values()].filter(
          (entry) =>
            entry.lane === lane &&
            entry.phase !== "Terminal" &&
            !excluding.has(entry.key) &&
            !added.includes(entry),
        );
        switch (conflicts.get(lane)) {
          case "skip":
            return others.length > 0 ? LaneBusy.of(key, lane) : undefined;
          case "replace":
            for (const entry of others)
              if (entry.phase !== "Started" && !pendingWithdrawals.has(entry.key)) {
                replaced.push(entry.key);
                excluding.add(entry.key);
              }
            return undefined;
          default:
            return undefined;
        }
      };
      const admitted = (entry: Entry) => {
        added.push(entry);
        waitFor.push(entry.key);
        undo.push(() => items.delete(entry.key));
      };
      /** An existing key: the same spec returns its handle, and the batch waits for it too. */
      const repeated = (
        key: ItemKey,
        fingerprint: string,
      ): Result.Result<ItemHandle | undefined, KeyMismatch> => {
        const existing = items.get(key) ?? history.get(key);
        if (existing === undefined && !groups.has(key)) return Result.succeed(undefined);
        if (existing?.fingerprint !== fingerprint) return Result.fail(KeyMismatch.of(key));
        waitFor.push(key);
        return Result.succeed(existing.handle);
      };
      for (const edit of edits) {
        switch (edit._tag) {
          case "Submit": {
            const { item } = edit;
            const known = repeated(item.key, item.fingerprint);
            if (Result.isFailure(known)) return refuse(known.failure);
            if (known.success !== undefined) {
              results.push({ _tag: "Added", handle: known.success });
              break;
            }
            const busy = conflict(item.key, item.lane);
            if (busy !== undefined) return refuse(busy);
            const startByMs =
              item.startByOffsetMs === undefined ? undefined : nowMs + item.startByOffsetMs;
            if (
              startByMs !== undefined &&
              view({
                lane: item.lane,
                admission: Infinity,
                seconds: item.request.durationSeconds,
                startByMs,
              }) > startByMs
            )
              return refuse(WouldMissDeadline.of(item.key));
            const entry = accept(item, nowMs, startByMs, undefined);
            admitted(entry);
            results.push({ _tag: "Added", handle: entry.handle });
            break;
          }
          case "SubmitGroup": {
            const { group } = edit;
            const known = groups.get(group.key);
            if (known !== undefined) {
              if (known.fingerprint !== group.fingerprint) return refuse(KeyMismatch.of(group.key));
              waitFor.push(...known.parts);
              results.push({ _tag: "AddedGroup", handle: known.handle });
              break;
            }
            const taken = [group.key, ...group.parts.map((part) => part.key)].find(
              (key) => items.has(key) || history.has(key) || groups.has(key),
            );
            if (taken !== undefined) return refuse(KeyMismatch.of(taken));
            const [first, ...rest] = group.parts;
            const busy = conflict(group.key, first.lane);
            if (busy !== undefined) return refuse(busy);
            const startByMs =
              first.startByOffsetMs === undefined ? undefined : nowMs + first.startByOffsetMs;
            if (
              startByMs !== undefined &&
              view({
                lane: first.lane,
                admission: Infinity,
                seconds: first.request.durationSeconds,
                startByMs,
              }) > startByMs
            )
              return refuse(WouldMissDeadline.of(group.key));
            const parts = [
              accept(first, nowMs, startByMs, { key: group.key, index: 0 }),
              ...rest.map((part, index) =>
                accept(part, nowMs, undefined, { key: group.key, index: index + 1 }),
              ),
            ] as const;
            for (const part of parts) admitted(part);
            const handle: GroupHandle = {
              key: group.key,
              parts: [parts[0].handle, ...parts.slice(1).map((part) => part.handle)],
            };
            groups.set(group.key, {
              fingerprint: group.fingerprint,
              handle,
              parts: group.parts.map((part) => part.key),
              brokenAt: undefined,
            });
            undo.push(() => groups.delete(group.key));
            results.push({ _tag: "AddedGroup", handle });
            break;
          }
          case "Insert": {
            const { insert } = edit;
            const known = repeated(insert.key, insert.fingerprint);
            if (Result.isFailure(known)) return refuse(known.failure);
            if (known.success !== undefined) {
              results.push({ _tag: "Added", handle: known.success });
              break;
            }
            const placed = placeInsert(insert);
            if (typeof placed === "string") return refuse(invalid(placed));
            const startByMs =
              insert.startByOffsetMs === undefined ? undefined : nowMs + insert.startByOffsetMs;
            if (
              startByMs !== undefined &&
              view({ ...placed, seconds: insert.request.durationSeconds, startByMs }) > startByMs
            )
              return refuse(WouldMissDeadline.of(insert.key));
            const entry = accept(
              {
                key: insert.key,
                lane: placed.lane,
                request: insert.request,
                fingerprint: insert.fingerprint,
                notBeforeOffsetMs: insert.notBeforeOffsetMs,
                startByOffsetMs: insert.startByOffsetMs,
                firm: insert.firm,
                atWallMs: undefined,
                late: "nextBoundary",
              },
              nowMs,
              startByMs,
              placed.group,
              {
                admission: placed.admission,
                inserted: true,
                notBeforeMs:
                  insert.notBeforeOffsetMs === undefined
                    ? undefined
                    : nowMs + insert.notBeforeOffsetMs,
              },
            );
            admitted(entry);
            if (placed.group !== undefined) {
              const parts = groups.get(placed.group.key)?.parts;
              parts?.push(entry.key);
              undo.push(() => parts?.pop());
            }
            results.push({ _tag: "Added", handle: entry.handle });
            break;
          }
          case "Replace": {
            const { next } = edit;
            const fingerprint = JSON.stringify({ replaces: edit.key, request: next.request });
            const known = repeated(next.key, fingerprint);
            if (Result.isFailure(known)) return refuse(known.failure);
            if (known.success !== undefined) {
              results.push({ _tag: "Added", handle: known.success });
              break;
            }
            const old = items.get(edit.key);
            if (
              old === undefined ||
              old.replacedBy !== undefined ||
              (old.phase !== "Accepted" && old.phase !== "Building" && old.phase !== "Ready")
            )
              return refuse(invalid("Nothing queued to replace under that key"));
            const entry = accept(
              {
                key: next.key,
                lane: old.lane,
                request: next.request,
                fingerprint,
                notBeforeOffsetMs: undefined,
                startByOffsetMs: undefined,
                firm: old.firm,
                atWallMs: old.atWallMs,
                late: old.late,
              },
              nowMs,
              old.startByMs,
              old.group,
              {
                admission: old.admission,
                replaces: old.key,
                generation: (old.generation ?? 0) + 1,
                inserted: old.inserted === true,
                notBeforeMs: old.notBeforeMs,
              },
            );
            admitted(entry);
            old.replacedBy = entry.key;
            undo.push(() => delete old.replacedBy);
            if (old.group !== undefined) {
              const parts = groups.get(old.group.key)?.parts;
              parts?.push(entry.key);
              undo.push(() => parts?.pop());
            }
            results.push({ _tag: "Added", handle: entry.handle });
            break;
          }
          case "Withdraw":
            results.push({ _tag: "Withdrawal", key: edit.key });
            break;
        }
      }
      return Result.succeed({ results, added, waitFor, replaced });
    };

    /**
     * Makes a staged call's handle. A batch keeps what it withdraws as cover until it commits;
     * a single call adds one item or group and takes effect at once.
     */
    const openBatch = (
      staged: Staged,
      batched: boolean,
    ): { readonly handle: EditHandle; readonly settle: ReadonlyArray<Effect.Effect<void>> } => {
      // A single call adds one item or group and never withdraws; only a replace lane gives
      // it something to supersede.
      if (!batched && staged.replaced.length === 0)
        return {
          handle: {
            results: staged.results.filter((result) => result._tag !== "Withdrawal"),
            committed: Effect.void,
          },
          settle: [],
        };
      const settle: Effect.Effect<void>[] = [];
      const batch: Batch = {
        id: ++batchCount,
        waitFor: staged.waitFor,
        targets: new Map(
          staged.replaced.map((key) => [key, { reason: "replaced", reply: undefined }] as const),
        ),
        committed: Deferred.makeUnsafe<void, EngineError>(),
      };
      const results = staged.results.map((result): EditResult => {
        if (result._tag !== "Withdrawal") return result;
        const replies: Deferred.Deferred<WithdrawOutcome, EngineError>[] = [];
        for (const target of withdrawalTargets(result.key)) {
          const reply = Deferred.makeUnsafe<WithdrawOutcome, EngineError>();
          replies.push(reply);
          batch.targets.set(target.key, { reason: "withdrawn", reply });
        }
        const outcome = Deferred.makeUnsafe<WithdrawOutcome, EngineError>();
        settle.push(settleWithdrawals(replies, outcome, groups.has(result.key)));
        return { _tag: "Withdrawal", outcome: Deferred.await(outcome) };
      });
      batches.set(batch.id, batch);
      return {
        handle: {
          results: batched ? results : results.filter((result) => result._tag !== "Withdrawal"),
          committed: batched ? Deferred.await(batch.committed) : Effect.void,
        },
        settle,
      };
    };

    const actor = Effect.gen(function* () {
      for (;;) {
        const message = yield* takeQueue(inbox);
        // Queued public calls observe the same terminal result through closedCall.
        if (ended !== undefined && message._tag !== "CommandDone") continue;
        switch (message._tag) {
          case "Edit": {
            if (!accepting) {
              yield* Deferred.fail(message.reply, invalid("Scheduler is draining or closed"));
              break;
            }
            const nowMs = monotonicMillis(clock);
            const state = yield* engine.state;
            if (ended !== undefined) continue;
            // No yield from the terminal check above through admission and its
            // publication below: a claim either precedes the items or settles them
            // after their Accepted evidence.
            const staged = stageEdits(message.edits, message.batched, state, nowMs);
            if (Result.isFailure(staged)) {
              yield* Deferred.fail(message.reply, staged.failure);
              break;
            }
            for (const entry of staged.success.added)
              PubSub.publishUnsafe(events, {
                key: entry.key,
                at: clock.currentTimeMillisUnsafe(),
                status: entry.status,
              });
            const opened = openBatch(staged.success, message.batched);
            for (const effect of opened.settle) yield* Effect.forkIn(effect, actorScope);
            yield* observed(state);
            // An edit returns its committed view; other messages publish once
            // through reconciliation after all their evidence is adopted.
            yield* publishState(state);
            yield* Deferred.succeed(message.reply, opened.handle);
            break;
          }
          case "Withdraw": {
            const targets = withdrawalTargets(message.key);
            if (targets.length <= 1 && !groups.has(message.key)) {
              if (targets[0] === undefined) yield* Deferred.succeed(message.reply, "not-found");
              else yield* requestWithdrawal(targets[0], "withdrawn", message.reply);
              break;
            }
            const replies: Deferred.Deferred<WithdrawOutcome, EngineError>[] = [];
            for (const part of targets) {
              const reply = Deferred.makeUnsafe<WithdrawOutcome, EngineError>();
              replies.push(reply);
              yield* requestWithdrawal(part, "withdrawn", reply);
            }
            yield* Effect.forkIn(
              settleWithdrawals(replies, message.reply, groups.has(message.key)),
              actorScope,
            );
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
              finishAccepted = message.finish === "accepted";
              updateRecovery();
              // Renewal admission must stop even while an earlier enqueue is
              // awaiting its reply in the command worker.
              const stoppedRenewal = yield* Effect.result(engine.stopRenewal);
              if (ended !== undefined) break;
              if (Result.isFailure(stoppedRenewal)) {
                drainFailure = stoppedRenewal.failure;
                for (const reply of drainReplies.splice(0))
                  yield* Deferred.fail(reply, stoppedRenewal.failure);
                break;
              }
              if (!finishAccepted) {
                autoplayPaused = true;
                yield* sendCommand({ _tag: "PauseAutoplay" });
              }
            }
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
            yield* closeActor(message.cause);
            yield* Deferred.fail(
              initialized,
              ReactorError.fromCode("Closed", message.cause.message),
            );
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
          yield* Queue.offer(inbox, {
            _tag: "Closed",
            cause: ReactorError.fromCode("Closed", "The orchestration closed"),
          });
          return;
        }
        if (result.failure.reason._tag !== "Overflow") {
          yield* Queue.offer(inbox, { _tag: "Closed", cause: result.failure });
          return;
        }
      }
    });
    // Orderly scope teardown has already claimed and settled Closed before
    // worker interruption; unexpected interruption retains its original Cause.
    const supervise = (effect: Effect.Effect<void>) =>
      effect.pipe(
        Effect.onExit((exit) =>
          Exit.isFailure(exit) && ended === undefined
            ? terminate(Exit.failCause(exit.cause))
            : Effect.void,
        ),
      );
    yield* Effect.forkScoped(supervise(watchdog));
    yield* Effect.forkScoped(supervise(commandWorker));
    yield* Effect.forkScoped(supervise(actor));
    yield* Effect.forkScoped(supervise(observe));
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
      closeActor(ReactorError.fromCode("Closed", "The scheduler closed")),
    );
    yield* Deferred.await(initialized);

    const laneSet = new Set(lanes);
    const send = (
      edits: ReadonlyArray<CapturedEdit>,
      batched: boolean,
    ): Effect.Effect<EditHandle, KeyMismatch | WouldMissDeadline | LaneBusy | EngineError> =>
      Effect.gen(function* () {
        const reply = yield* Deferred.make<
          EditHandle,
          KeyMismatch | WouldMissDeadline | LaneBusy | EngineError
        >();
        if (!(yield* Queue.offer(inbox, { _tag: "Edit", edits, batched, reply })))
          return yield* closedCall;
        return yield* Deferred.await(reply).pipe(Effect.raceFirst(closedCall));
      });
    /** The one result of a single call. */
    const only = (handle: EditHandle): EditResult | undefined => handle.results[0];
    const submit: SchedulerShape["submit"] = (
      input,
    ): Effect.Effect<ItemHandle, KeyMismatch | WouldMissDeadline | LaneBusy | EngineError> =>
      Effect.gen(function* () {
        if (yield* Deferred.isDone(stopped)) return yield* closedCall;
        const item = yield* captureItem(input, laneSet);
        yield* Effect.annotateCurrentSpan("reactor.scheduler.item.key", item.key);
        const result = only(yield* send([{ _tag: "Submit", item }], false));
        if (result?._tag !== "Added") return yield* Effect.die("A submission returned no item");
        return result.handle;
      }).pipe(Effect.withSpan("Scheduler.submit", {}, { captureStackTrace: false }));
    const submitGroup: SchedulerShape["submitGroup"] = (
      input,
    ): Effect.Effect<GroupHandle, KeyMismatch | WouldMissDeadline | LaneBusy | EngineError> =>
      Effect.gen(function* () {
        if (yield* Deferred.isDone(stopped)) return yield* closedCall;
        const group = yield* captureGroup(input, laneSet);
        yield* Effect.annotateCurrentSpan("reactor.scheduler.group.key", group.key);
        const result = only(yield* send([{ _tag: "SubmitGroup", group }], false));
        if (result?._tag !== "AddedGroup") return yield* Effect.die("A group returned no handle");
        return result.handle;
      }).pipe(Effect.withSpan("Scheduler.submitGroup", {}, { captureStackTrace: false }));
    const replace: SchedulerShape["replace"] = (
      key,
      input,
    ): Effect.Effect<ItemHandle, KeyMismatch | EngineError> =>
      Effect.gen(function* () {
        if (yield* Deferred.isDone(stopped)) return yield* closedCall;
        const next = yield* captureReplacement(input);
        const result = only(
          yield* send([{ _tag: "Replace", key, next }], false).pipe(
            // A replacement keeps the replaced item's place and window, so neither a deadline
            // nor its lane's policy is checked for it.
            Effect.catchTag(["WouldMissDeadline", "LaneBusy"], (error) => Effect.die(error)),
          ),
        );
        if (result?._tag !== "Added") return yield* Effect.die("A replacement returned no item");
        return result.handle;
      }).pipe(
        Effect.withSpan("Scheduler.replace", { attributes: { key } }, { captureStackTrace: false }),
      );
    const insert: SchedulerShape["insert"] = (
      input,
    ): Effect.Effect<ItemHandle, KeyMismatch | WouldMissDeadline | EngineError> =>
      Effect.gen(function* () {
        if (yield* Deferred.isDone(stopped)) return yield* closedCall;
        const captured = yield* captureInsert(input);
        yield* Effect.annotateCurrentSpan("reactor.scheduler.item.key", captured.key);
        const result = only(
          yield* send([{ _tag: "Insert", insert: captured }], false).pipe(
            // An insert places itself, so its lane's policy does not apply.
            Effect.catchTag("LaneBusy", (error) => Effect.die(error)),
          ),
        );
        if (result?._tag !== "Added") return yield* Effect.die("An insert returned no item");
        return result.handle;
      }).pipe(Effect.withSpan("Scheduler.insert", {}, { captureStackTrace: false }));
    const edit: SchedulerShape["edit"] = (
      input,
    ): Effect.Effect<EditHandle, KeyMismatch | WouldMissDeadline | LaneBusy | EngineError> =>
      Effect.gen(function* () {
        if (yield* Deferred.isDone(stopped)) return yield* closedCall;
        const edits = yield* captureEdits(input, laneSet);
        yield* Effect.annotateCurrentSpan("reactor.scheduler.edits", edits.length);
        return yield* send(edits, true);
      }).pipe(Effect.withSpan("Scheduler.edit", {}, { captureStackTrace: false }));
    const withdraw: SchedulerShape["withdraw"] = (key) =>
      Effect.gen(function* () {
        const reply = yield* Deferred.make<WithdrawOutcome, EngineError>();
        if (!(yield* Queue.offer(inbox, { _tag: "Withdraw", key, reply })))
          return yield* closedCall;
        return yield* Deferred.await(reply).pipe(Effect.raceFirst(closedCall));
      }).pipe(
        Effect.withSpan(
          "Scheduler.withdraw",
          { attributes: { key } },
          {
            captureStackTrace: false,
          },
        ),
      );
    const drain: SchedulerShape["drain"] = (options = {}): Effect.Effect<void, EngineError> =>
      Effect.gen(function* () {
        const reply = yield* Deferred.make<void, EngineError>();
        const finish = options.finish ?? "playing";
        if (finish !== "playing" && finish !== "accepted")
          return yield* invalid("Drain finish must be playing or accepted");
        if (!(yield* Queue.offer(inbox, { _tag: "Drain", finish, reply })))
          return yield* closedCall;
        yield* Deferred.await(reply).pipe(Effect.raceFirst(closedCall));
      }).pipe(Effect.withSpan("Scheduler.drain", {}, { captureStackTrace: false }));
    return {
      submit,
      submitGroup,
      replace,
      insert,
      edit,
      withdraw,
      drain,
      failure: Deferred.await(stopped),
      state: SubscriptionRef.get(stateRef),
      asRun: Stream.fromPubSub(events),
    };
  }).pipe(
    Effect.provideServiceEffect(
      Scope.Scope,
      Effect.flatMap(Scope.Scope, (parent) => Scope.fork(parent, "sequential")),
    ),
  );

export const layerScheduler = (
  options: SchedulerOptions,
): Layer.Layer<Scheduler, ReactorError | PolicyFailure, Engine> =>
  Layer.effect(Scheduler, makeScheduler(options));
