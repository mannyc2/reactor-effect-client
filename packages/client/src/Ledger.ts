/**
 * The paid sessions an application owns, each recorded before it connects and ended until Reactor
 * confirms it ended. The SDK owns that protocol and the application the storage: a `Store` keeps
 * the entries, in a file (`layerFile`), in memory (`layerMemory`) or in a service of the
 * application's own (`layer`). `ledger.source(H3Source.opener({ tokens }))` is a playout's
 * `open`: before it allocates anything, it resumes or ends what a process that died left recorded.
 */
import * as Cause from "effect/Cause";
import * as Clock from "effect/Clock";
import * as Context from "effect/Context";
import * as Deferred from "effect/Deferred";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as FiberMap from "effect/FiberMap";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as Ref from "effect/Ref";
import * as Schedule from "effect/Schedule";
import * as Schema from "effect/Schema";
import type * as Scope from "effect/Scope";
import * as Semaphore from "effect/Semaphore";
import { CoordinatorClient, terminateUntilConfirmed } from "./CoordinatorClient.js";
import type { Termination } from "./CoordinatorClient.js";
import type { ClipRequest, Source } from "./Playout.js";
import { noAcquisition } from "./Reactor.js";
import { AcquisitionFailure, ReactorError, summarize } from "./ReactorError.js";
import type { CloseReport } from "./Session.js";

/** A paid session this application owns, as recorded before it connected. */
export const Entry = Schema.Struct({
  sessionId: Schema.NonEmptyString,
  model: Schema.String,
  /**
   * The coordinator that allocated it (`CoordinatorClient.apiUrl`): only a ledger on the same one
   * settles it.
   */
  apiUrl: Schema.String,
  /**
   * When its cap ends, in seconds since the epoch, counted from the allocation request as
   * `H3Source.Allocation.endsAt` is; absent when uncapped.
   */
  endsAt: Schema.optionalKey(Schema.Finite),
  /**
   * `live` while its owner may still air it; `ending` once its end was asked and not yet
   * confirmed: it is ended again until it is, and never resumed.
   */
  state: Schema.Literals(["live", "ending"]),
});
export type Entry = typeof Entry.Type;

/** A store's failure; `cause` is the store's own error. */
export class LedgerError extends Schema.TaggedError<LedgerError>()("LedgerError", {
  operation: Schema.Literals(["entries", "put", "remove"]),
  message: Schema.String,
  cause: Schema.Defect(),
}) {}

/**
 * Where the application keeps its entries: a file, a database, a service of its own. One store
 * belongs to one process and one Reactor account: two processes on one store would end each
 * other's sessions as leftovers.
 */
export class Store extends Context.Service<
  Store,
  {
    /** Every entry recorded and not removed, in the order each was first recorded. */
    readonly entries: Effect.Effect<ReadonlyArray<Entry>, LedgerError>;
    /** Records `entry`, replacing the entry for its session id in its place. */
    readonly put: (entry: Entry) => Effect.Effect<void, LedgerError>;
    /** Forgets the entry for `sessionId`; nothing when there is none. */
    readonly remove: (sessionId: string) => Effect.Effect<void, LedgerError>;
  }
>()("reactor-effect-client/Ledger/Store") {}

/** What a ledger needs from a source module to open and resume its sessions. */
export interface Opener<Req extends ClipRequest, R> {
  /** The model its sessions run; only entries of this model are resumed with it. */
  readonly model: string;
  /**
   * Opens a fresh session, running `record` once it is allocated and before it connects, with the
   * end of its cap in seconds since the epoch, absent when uncapped.
   */
  readonly open: (
    record: (allocated: {
      readonly sessionId: string;
      readonly endsAt?: number | undefined;
    }) => Effect.Effect<void, LedgerError>,
  ) => Effect.Effect<Source<Req>, AcquisitionFailure, R | Scope.Scope>;
  /** Adopts a recorded session, so that closing it ends it. */
  readonly resume: (
    entry: Entry,
  ) => Effect.Effect<Source<Req>, AcquisitionFailure, R | Scope.Scope>;
}

export interface SourceOptions {
  /**
   * Resume a recorded session rather than allocate one: the live entry of the opener's model that
   * ends last, among those with at least `minimumLeft` of their cap left (1 minute by default) or no
   * cap; `false` never resumes.
   */
  readonly resume?: { readonly minimumLeft?: Duration.Input | undefined } | false | undefined;
  /**
   * Allocate nothing while a recorded session's end is unconfirmed, since it may still bill: wait
   * for Reactor to confirm it (a playout's `renewal.openTimeout` bounds the wait). An end that
   * stops unconfirmed, refused or out of its `ending` schedule, fails the source. True by default.
   */
  readonly endFirst?: boolean | undefined;
}

/** What `release` did: the ends Reactor confirmed, and those it is still asked for. */
export interface Released {
  readonly ended: ReadonlyArray<string>;
  /** Ends Reactor did not confirm: each is asked again until it does, unless it was refused. */
  readonly ending: ReadonlyArray<string>;
}

export class Ledger extends Context.Service<
  Ledger,
  {
    /**
     * A playout source through `opener`. It first settles every recorded session of this ledger's
     * coordinator that it does not hold: resumes one (`SourceOptions.resume`) and ends the rest,
     * each until Reactor confirms it, waiting for those ends unless `endFirst` is false. Then it
     * opens a session recorded before it connects. Closing the source, or its scope, ends the
     * session and forgets it once Reactor confirms; an unconfirmed end is marked `ending` and
     * asked again.
     */
    readonly source: <Req extends ClipRequest, R>(
      opener: Opener<Req, R>,
      options?: SourceOptions,
    ) => Effect.Effect<Source<Req>, AcquisitionFailure, R | Scope.Scope>;
    /**
     * Ends every recorded session of this ledger's coordinator that it does not hold, each until
     * Reactor confirms it.
     */
    readonly release: Effect.Effect<Released, LedgerError>;
    /** What the store holds now. */
    readonly entries: Effect.Effect<ReadonlyArray<Entry>, LedgerError>;
  }
>()("reactor-effect-client/Ledger") {}

export interface Options {
  /**
   * How an unconfirmed end is asked again; `terminateUntilConfirmed`'s default, for as long as the
   * session runs, when absent. An end whose schedule runs out first stays `ending`, for a later
   * source or the next process to end again.
   */
  readonly ending?: Schedule.Schedule<unknown, Termination> | undefined;
}

/** `entries` with `entry` in the place of the entry for its session, or after them all. */
const upsert = (entries: ReadonlyArray<Entry>, entry: Entry): ReadonlyArray<Entry> =>
  entries.some((each) => each.sessionId === entry.sessionId)
    ? entries.map((each) => (each.sessionId === entry.sessionId ? entry : each))
    : [...entries, entry];

const without = (entries: ReadonlyArray<Entry>, sessionId: string): ReadonlyArray<Entry> =>
  entries.filter((each) => each.sessionId !== sessionId);

const memoryStore = (): Layer.Layer<Store> =>
  Layer.effect(
    Store,
    Effect.map(Ref.make<ReadonlyArray<Entry>>([]), (kept) =>
      Store.of({
        entries: Ref.get(kept),
        put: (entry) => Ref.update(kept, (entries) => upsert(entries, entry)),
        remove: (sessionId) => Ref.update(kept, (entries) => without(entries, sessionId)),
      }),
    ),
  );

const EntriesJson = Entry.pipe(Schema.Array, Schema.fromJsonString);
const decodeEntries = Schema.decodeEffect(EntriesJson);
const encodeEntries = Schema.encodeEffect(EntriesJson);

/**
 * Entries in a JSON file, rewritten whole beside it and renamed over it on every change, so a
 * crash leaves the old list or the new one. A missing file holds no entries.
 */
const fileStore = (file: string): Layer.Layer<Store, never, FileSystem.FileSystem | Path.Path> =>
  Layer.effect(
    Store,
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      // A ledger that cannot make its directory cannot record a session: no open may go ahead.
      yield* fs.makeDirectory(path.dirname(file), { recursive: true }).pipe(Effect.orDie);
      const changes = yield* Semaphore.make(1);
      const staged = `${file}.tmp`;
      const read = fs.readFileString(file).pipe(
        Effect.flatMap(decodeEntries),
        Effect.catchReason("PlatformError", "NotFound", () =>
          Effect.succeed<ReadonlyArray<Entry>>([]),
        ),
      );
      const change = (
        operation: "put" | "remove",
        update: (entries: ReadonlyArray<Entry>) => ReadonlyArray<Entry>,
      ) =>
        read.pipe(
          Effect.map(update),
          Effect.flatMap(encodeEntries),
          Effect.flatMap((text) => fs.writeFileString(staged, `${text}\n`)),
          Effect.andThen(fs.rename(staged, file)),
          Effect.mapError((cause) =>
            LedgerError.make({ operation, message: "the ledger file could not be written", cause }),
          ),
          changes.withPermits(1),
        );
      return Store.of({
        entries: read.pipe(
          Effect.mapError((cause) =>
            LedgerError.make({
              operation: "entries",
              message: "the ledger file could not be read",
              cause,
            }),
          ),
        ),
        put: (entry) => change("put", (entries) => upsert(entries, entry)),
        remove: (sessionId) => change("remove", (entries) => without(entries, sessionId)),
      });
    }),
  );

/** When an entry's session ends; an uncapped one never does. */
const endOf = (entry: Entry): number => entry.endsAt ?? Number.POSITIVE_INFINITY;

/**
 * Which recorded sessions a source settles, and how. It settles only the entries of its ledger's
 * coordinator that the ledger neither holds nor is ending. The one to resume is a `live` entry of
 * `model` with at least `minimumLeftSeconds` of its cap left: the one that ends last, an uncapped
 * one last of all, and of those that end together the one recorded last. Every other one is
 * ended, whatever its model.
 */
const choose = (
  entries: ReadonlyArray<Entry>,
  context: {
    readonly model: string;
    readonly apiUrl: string;
    readonly nowSeconds: number;
    /** Undefined resumes none. */
    readonly minimumLeftSeconds: number | undefined;
    readonly held: ReadonlySet<string>;
  },
): { readonly resume: Entry | undefined; readonly end: ReadonlyArray<Entry> } => {
  const settled = entries.filter(
    (entry) => entry.apiUrl === context.apiUrl && !context.held.has(entry.sessionId),
  );
  const minimum = context.minimumLeftSeconds;
  const resume =
    minimum === undefined
      ? undefined
      : settled
          .filter(
            (entry) =>
              entry.state === "live" &&
              entry.model === context.model &&
              endOf(entry) - context.nowSeconds >= minimum,
          )
          .reduce<Entry | undefined>(
            (best, entry) => (best === undefined || endOf(entry) >= endOf(best) ? entry : best),
            undefined,
          );
  return { resume, end: settled.filter((entry) => entry !== resume) };
};

/** `report` with the store's failure to settle its session, if it failed, among its local errors. */
const kept = (
  report: CloseReport,
  sessionId: string,
  failure: LedgerError | undefined,
): CloseReport =>
  failure === undefined
    ? report
    : {
        ...report,
        localErrors: [
          ...report.localErrors,
          summarize(
            ReactorError.fromCode("Aborted", "the ledger could not settle the session", {
              operation: "ledger",
              sessionId,
              detail: failure,
            }),
          ),
        ],
      };

const make = Effect.fnUntraced(function* (options: Options) {
  const store = yield* Store;
  const coordinator = yield* CoordinatorClient;
  const { apiUrl } = coordinator;
  // Reading the store, choosing what to resume and ending the rest go one at a time, so that no
  // two sources take one session.
  const permit = yield* Semaphore.make(1);
  /** The sessions this ledger's sources hold. */
  const held = yield* Ref.make<ReadonlySet<string>>(new Set());
  /** A fiber for each session whose end is asked until Reactor confirms it, in the ledger's scope. */
  const loops = yield* FiberMap.make<string, void, never>();
  /** Why each end that stopped unconfirmed stopped, until an end of that session is confirmed. */
  const stopped = yield* Ref.make<ReadonlyMap<string, ReactorError>>(new Map());
  const hold = (sessionId: string) => Ref.update(held, (ids) => new Set(ids).add(sessionId));
  const unhold = (sessionId: string) =>
    Ref.update(held, (ids) => new Set([...ids].filter((id) => id !== sessionId)));

  /** A store failure the ending of a session outlives, logged by session and operation. */
  const outlived = (sessionId: string) => (error: LedgerError) =>
    Effect.logWarning("the ledger's store failed").pipe(
      Effect.annotateLogs({
        "reactor.session.id": sessionId,
        "reactor.ledger.operation": error.operation,
      }),
    );

  /**
   * Ends `entry`'s session, marked `ending` first, asking again on `schedule` until Reactor
   * confirms it, and then forgets it. It fails as `terminateUntilConfirmed` does, which leaves the
   * entry `ending`; a store failure is logged, and the end goes on.
   */
  const end = Effect.fnUntraced(function* (
    entry: Entry,
    schedule: Schedule.Schedule<unknown, Termination> | undefined,
  ) {
    if (entry.state === "live")
      yield* store.put({ ...entry, state: "ending" }).pipe(Effect.catch(outlived(entry.sessionId)));
    yield* terminateUntilConfirmed(entry.sessionId, { schedule }).pipe(
      Effect.provideService(CoordinatorClient, coordinator),
    );
    yield* Ref.update(
      stopped,
      (ends) => new Map([...ends].filter(([sessionId]) => sessionId !== entry.sessionId)),
    );
    yield* store.remove(entry.sessionId).pipe(Effect.catch(outlived(entry.sessionId)));
  });

  /** Ends `entry`'s session once; whether Reactor confirmed it. A refusal fails it. */
  const endOnce = (entry: Entry) =>
    end(entry, Schedule.recurs(0)).pipe(
      Effect.as(true),
      Effect.catchReason("ReactorError", "Indeterminate", () => Effect.succeed(false)),
    );

  /**
   * Ends `entry`'s session until Reactor confirms it, in the ledger's scope, unless that runs
   * already. A refusal, or an `ending` schedule that runs out, stops it with an error logged and
   * kept, and leaves the entry `ending` for a later source or the next process.
   */
  const ending = (entry: Entry) =>
    FiberMap.run(
      loops,
      entry.sessionId,
      end(entry, options.ending).pipe(
        Effect.catch((error) =>
          Effect.logError("a recorded session's end was not confirmed").pipe(
            Effect.annotateLogs({
              "reactor.session.id": entry.sessionId,
              "error.type": error.reason._tag,
            }),
            Effect.andThen(
              Ref.update(stopped, (ends) => new Map(ends).set(entry.sessionId, error)),
            ),
          ),
        ),
      ),
      { onlyIfMissing: true },
    );

  /**
   * Waits for every end the ledger asks, those asked meanwhile included, and then fails as an end
   * that stopped unconfirmed did, if one has and no end of its session was confirmed since.
   */
  const awaitEnds = FiberMap.awaitEmpty(loops).pipe(
    Effect.andThen(Ref.get(stopped)),
    Effect.flatMap((ends) => {
      const [unconfirmed] = [...ends.values()];
      return unconfirmed === undefined ? Effect.void : Effect.fail(unconfirmed);
    }),
  );

  /**
   * Settles a held session once its source has closed, or its acquisition stopped short of one:
   * forgets it if Reactor confirmed its end, and otherwise marks it `ending` and ends it until
   * Reactor does. Answers the store's failure, if it failed, for the caller to keep.
   */
  const settle = Effect.fnUntraced(function* (entry: Entry, confirmed: boolean) {
    const stored = yield* Effect.result(
      confirmed ? store.remove(entry.sessionId) : store.put({ ...entry, state: "ending" }),
    );
    if (!confirmed)
      yield* ending(stored._tag === "Success" ? { ...entry, state: "ending" } : entry);
    yield* unhold(entry.sessionId);
    return stored._tag === "Failure" ? stored.failure : undefined;
  });

  /**
   * An acquisition stopped without a report, by an interruption or a defect: its session may have
   * been allocated or adopted, so it is ended, and the cause goes on.
   */
  const abandoned = Effect.fnUntraced(function* (entry: Entry, cause: Cause.Cause<never>) {
    const failure = yield* settle(entry, false);
    if (failure !== undefined) yield* outlived(entry.sessionId)(failure);
    return yield* Effect.failCause(cause);
  });

  /**
   * `source`, held for `entry`, with a close that settles it. It settles once and uninterruptibly,
   * whether the caller closes the source or the scope it was opened in closes first, and both
   * answer the report it settled.
   */
  const watched = Effect.fnUntraced(function* <Req extends ClipRequest>(
    entry: Entry,
    source: Source<Req>,
  ) {
    const settled = yield* Deferred.make<CloseReport>();
    const begun = yield* Ref.make(false);
    const settlement = Effect.gen(function* () {
      const report = yield* source.close;
      return kept(report, entry.sessionId, yield* settle(entry, report.remote.confirmed));
    });
    const once = Effect.gen(function* () {
      if (yield* Ref.getAndSet(begun, true)) return;
      yield* Deferred.into(settlement, settled);
    });
    const close = Effect.andThen(Effect.uninterruptible(once), Deferred.await(settled));
    yield* Effect.addFinalizer(() => Effect.asVoid(close));
    return { ...source, close } satisfies Source<Req>;
  });

  /**
   * Settles the recorded sessions of this coordinator that the ledger neither holds nor is ending:
   * holds the one `choose` resumes, noted in `chosen` first, and ends every other one, once, then
   * again until Reactor confirms those it did not. An unreadable store, or a refused end, fails it.
   */
  const settleLeftovers = Effect.fnUntraced(function* (
    model: string,
    minimumLeftSeconds: number | undefined,
    chosen: Ref.Ref<Entry | undefined>,
  ) {
    const entries = yield* store.entries.pipe(
      Effect.mapError((error) =>
        AcquisitionFailure.from(
          ReactorError.fromCode("Aborted", "the ledger could not be read", {
            operation: "ledger",
            detail: error,
          }),
          noAcquisition,
        ),
      ),
    );
    // An end that stopped unconfirmed matters while its entry is recorded, and no longer.
    const recorded = new Set(entries.map((entry) => entry.sessionId));
    yield* Ref.update(
      stopped,
      (ends) => new Map([...ends].filter(([sessionId]) => recorded.has(sessionId))),
    );
    const endingIds = yield* Effect.sync(() => Array.from(loops, ([sessionId]) => sessionId));
    const { resume, end: ends } = choose(entries, {
      model,
      apiUrl,
      nowSeconds: (yield* Clock.currentTimeMillis) / 1000,
      minimumLeftSeconds,
      held: new Set([...(yield* Ref.get(held)), ...endingIds]),
    });
    if (resume !== undefined) {
      yield* Ref.set(chosen, resume);
      yield* hold(resume.sessionId);
    }
    yield* Effect.forEach(
      ends,
      (entry) =>
        endOnce(entry).pipe(
          Effect.flatMap((confirmed) =>
            confirmed ? Effect.void : Effect.asVoid(ending({ ...entry, state: "ending" })),
          ),
        ),
      { concurrency: "unbounded", discard: true },
    ).pipe(Effect.mapError((refusal) => AcquisitionFailure.from(refusal, noAcquisition)));
  }, permit.withPermits(1));

  /**
   * Resumes `entry`, taking it over from `chosen`: its source, watched to settle it on close, or
   * none when the resume failed and was settled, so that a fresh session opens in its place.
   */
  const resumeOne = <Req extends ClipRequest, R>(
    opener: Opener<Req, R>,
    entry: Entry,
    chosen: Ref.Ref<Entry | undefined>,
  ) =>
    Effect.uninterruptibleMask((restore) =>
      Effect.gen(function* () {
        yield* Ref.set(chosen, undefined);
        const exit = yield* Effect.exit(restore(opener.resume(entry)));
        if (Exit.isSuccess(exit)) return Option.some(yield* watched(entry, exit.value));
        const failure = Cause.findError(exit.cause);
        if (failure._tag === "Failure") return yield* abandoned(entry, failure.failure);
        const stored = yield* settle(entry, failure.success.cleanup.remote.confirmed);
        yield* Effect.logWarning("a recorded session could not be resumed").pipe(
          Effect.annotateLogs({
            "reactor.session.id": entry.sessionId,
            "error.type": failure.success.reason._tag,
          }),
        );
        if (stored !== undefined) yield* outlived(entry.sessionId)(stored);
        return Option.none<Source<Req>>();
      }),
    );

  /**
   * Opens a fresh session through `opener`, recorded and held before it connects: its source,
   * watched to settle it on close. Whatever stops the open once the session is recorded settles
   * it: from the failure's report, or by ending it after an interruption.
   */
  const openFresh = Effect.fnUntraced(function* <Req extends ClipRequest, R>(
    opener: Opener<Req, R>,
  ) {
    const recorded = yield* Ref.make<Entry | undefined>(undefined);
    const record = Effect.fnUntraced(function* (allocated: {
      readonly sessionId: string;
      readonly endsAt?: number | undefined;
    }) {
      const entry: Entry = {
        sessionId: allocated.sessionId,
        model: opener.model,
        apiUrl,
        ...(allocated.endsAt === undefined ? {} : { endsAt: allocated.endsAt }),
        state: "live",
      };
      // Held before it is stored, so that no source takes it for a leftover.
      yield* Ref.set(recorded, entry);
      yield* hold(entry.sessionId);
      yield* store.put(entry);
    });
    return yield* Effect.uninterruptibleMask((restore) =>
      Effect.gen(function* () {
        const exit = yield* Effect.exit(restore(opener.open(record)));
        const entry = yield* Ref.get(recorded);
        if (Exit.isSuccess(exit)) {
          if (entry !== undefined) return yield* watched(entry, exit.value);
          yield* exit.value.close;
          return yield* Effect.die("the ledger's opener opened a session it did not record");
        }
        if (entry === undefined) return yield* Effect.failCause(exit.cause);
        const failure = Cause.findError(exit.cause);
        if (failure._tag === "Failure") return yield* abandoned(entry, failure.failure);
        const { cleanup } = failure.success;
        const stored = yield* settle(entry, cleanup.remote.confirmed);
        return yield* AcquisitionFailure.from(
          failure.success,
          kept(cleanup, entry.sessionId, stored),
        );
      }),
    );
  });

  /**
   * Settles the leftovers, waits for their ends unless told not to, then resumes the session
   * chosen, or opens a fresh one if there is none or its resume failed.
   */
  const acquire = Effect.fnUntraced(function* <Req extends ClipRequest, R>(
    opener: Opener<Req, R>,
    sourceOptions: SourceOptions,
    chosen: Ref.Ref<Entry | undefined>,
  ) {
    const minimumLeftSeconds =
      sourceOptions.resume === false
        ? undefined
        : Duration.toSeconds(
            Duration.fromInputUnsafe(sourceOptions.resume?.minimumLeft ?? "1 minute"),
          );
    const endsConfirmed = ((sourceOptions.endFirst ?? true) ? awaitEnds : Effect.void).pipe(
      Effect.mapError((error) => AcquisitionFailure.from(error, noAcquisition)),
    );
    yield* settleLeftovers(opener.model, minimumLeftSeconds, chosen);
    yield* endsConfirmed;
    const entry = yield* Ref.get(chosen);
    if (entry !== undefined) {
      const resumed = yield* resumeOne(opener, entry, chosen);
      if (Option.isSome(resumed)) return resumed.value;
      yield* endsConfirmed;
    }
    return yield* openFresh(opener);
  });

  const source = Effect.fn("Ledger.source")(function* <Req extends ClipRequest, R>(
    opener: Opener<Req, R>,
    sourceOptions: SourceOptions = {},
  ): Effect.fn.Return<Source<Req>, AcquisitionFailure, R | Scope.Scope> {
    const chosen = yield* Ref.make<Entry | undefined>(undefined);
    const opened = yield* acquire(opener, sourceOptions, chosen).pipe(
      // A session chosen to resume, and stopped before its resume took it over, is left as it was.
      Effect.onExit(() =>
        Effect.flatMap(Ref.get(chosen), (entry) =>
          entry === undefined ? Effect.void : unhold(entry.sessionId),
        ),
      ),
    );
    yield* Effect.annotateCurrentSpan("reactor.session.id", opened.sessionId);
    return opened;
  });

  const release = Effect.gen(function* () {
    const entries = yield* store.entries;
    const holding = yield* Ref.get(held);
    const leftovers = entries.filter(
      (entry) => entry.apiUrl === apiUrl && !holding.has(entry.sessionId),
    );
    const ends = yield* Effect.forEach(
      leftovers,
      (entry) =>
        endOnce(entry).pipe(
          // A refused end is one Reactor did not confirm; its ending logs the refusal.
          Effect.orElseSucceed(() => false),
          Effect.tap((confirmed) =>
            confirmed ? Effect.void : ending({ ...entry, state: "ending" }),
          ),
          Effect.map((confirmed) => ({ sessionId: entry.sessionId, confirmed })),
        ),
      { concurrency: "unbounded" },
    );
    return {
      ended: ends.filter((each) => each.confirmed).map((each) => each.sessionId),
      ending: ends.filter((each) => !each.confirmed).map((each) => each.sessionId),
    } satisfies Released;
  }).pipe(
    permit.withPermits(1),
    Effect.withSpan("Ledger.release", {}, { captureStackTrace: false }),
  );

  return Ledger.of({ source, release, entries: store.entries });
});

/** The ledger over the `Store` the application provides. */
export const layer = (
  options: Options = {},
): Layer.Layer<Ledger, never, Store | CoordinatorClient> => Layer.effect(Ledger, make(options));

/**
 * The ledger over a JSON file it rewrites whole and renames into place on every change, creating
 * its directory.
 */
// A layer is built once at the composition root, never piped into, so it has no pipeable form.
// @effect-diagnostics-next-line missingPipeableSignature:off
export const layerFile = (
  path: string,
  options: Options = {},
): Layer.Layer<Ledger | Store, never, CoordinatorClient | FileSystem.FileSystem | Path.Path> =>
  layer(options).pipe(Layer.provideMerge(fileStore(path)));

/** The ledger over entries kept in memory: for tests, and for one process that never restarts. */
export const layerMemory = (
  options: Options = {},
): Layer.Layer<Ledger | Store, never, CoordinatorClient> =>
  layer(options).pipe(Layer.provideMerge(memoryStore()));
