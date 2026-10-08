/**
 * The playout's shell around its pure policy. One fiber owns the policy state:
 * it takes an input (a caller's edit, a session's evidence, a command's result)
 * or wakes at the policy's next deadline, runs `step`, and carries out what the
 * step asks. Each session's provider commands go through a worker of its own,
 * one at a time and in order, so a slow command holds up only its own
 * session's; session opens and closes run in their own fibers. Each reports
 * back through the inbox.
 */
import * as Cause from "effect/Cause";
import * as Clock from "effect/Clock";
import * as Deferred from "effect/Deferred";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as ErrorReporter from "effect/ErrorReporter";
import * as Exit from "effect/Exit";
import * as Option from "effect/Option";
import * as Predicate from "effect/Predicate";
import * as PubSub from "effect/PubSub";
import * as Queue from "effect/Queue";
import * as Ref from "effect/Ref";
import * as Result from "effect/Result";
import * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import * as SubscriptionRef from "effect/SubscriptionRef";
import * as Tracer from "effect/Tracer";
import type * as Playout from "../../Playout.js";
import { currentParent, spanOptions } from "../trace.js";
import { AcquisitionFailure, CommandFailure, ReactorError } from "../../ReactorError.js";
import type { ReactorFailure } from "../../ReactorError.js";
import { mayStillBill } from "../../Session.js";
import type { CloseReport } from "../../Session.js";
import {
  InvalidFiller,
  InvalidItem,
  ItemKey,
  KeyMismatch,
  LaneBusy,
  PlayoutClosed,
  WouldMissDeadline,
} from "./errors.js";
import type { SubmitError } from "./errors.js";
import * as Policy from "./policy.js";

type Handle = {
  readonly started: Deferred.Deferred<Effect.Success<Playout.ItemHandle["started"]>>;
  readonly outcome: Deferred.Deferred<Playout.Settled>;
};
/** What a group's handle waits on. */
type GroupDeferreds = {
  readonly started: Deferred.Deferred<Effect.Success<Playout.GroupHandle["started"]>>;
  readonly outcome: Deferred.Deferred<Playout.GroupOutcome>;
};
type Reply =
  | { readonly _tag: "Accepted"; readonly results: ReadonlyArray<Policy.EditReply> }
  | { readonly _tag: "Refused"; readonly refusal: Policy.Refusal };
type Command<Req extends Playout.ClipRequest> = Extract<Policy.Action<Req>, { _tag: "Command" }>;
type Input<Req extends Playout.ClipRequest> = {
  readonly input: Policy.Input<Req>;
  readonly parent?: Tracer.ExternalSpan | undefined;
};
type QueuedCommand<Req extends Playout.ClipRequest> = {
  readonly action: Command<Req>;
  readonly parent: Tracer.ExternalSpan | undefined;
};

/** Close reports kept beyond the unconfirmed ones. */
const retainedReports = 8;

const millis = (input: Duration.Input | undefined, fallback: number): number =>
  input === undefined ? fallback : Duration.toMillis(Duration.fromInputUnsafe(input));

const monotonic = Effect.map(Clock.monotonicTimeNanos, (nanos) => Number(nanos) / 1_000_000);

type FillerClip<Req extends Playout.ClipRequest> = NonNullable<
  Policy.Config<Req>["filler"]
>["clip"];

/**
 * The application's filler clips, each asking for the length it was asked for unless it names
 * one: the plan counts it at that, and a provider may build one without a length at its session's own.
 */
const sized =
  <Req extends Playout.ClipRequest>(clip: FillerClip<Req>): FillerClip<Req> =>
  (context) => {
    const request = clip(context);
    return { ...request, seconds: request.seconds ?? context.seconds };
  };

/**
 * The wait until a wake `ms` away, in whole nanoseconds rounded up: the clock counts whole
 * nanoseconds, and a wake between two of them comes at the later one.
 */
const untilWake = (ms: number): Duration.Duration =>
  Duration.nanos(BigInt(Math.max(0, Math.ceil(ms * 1_000_000))));

/**
 * Reports the defects of `cause`, whose failure decides what it was for. A defect beside a
 * failure, such as a finalizer's that died as the effect failed, is still one; the failure, which
 * the plan handles, is not reported.
 */
const reportDefects = (cause: Cause.Cause<unknown>): Effect.Effect<void> => {
  const dies = cause.reasons.filter(Cause.isDieReason);
  return dies.length > 0 ? ErrorReporter.report(Cause.fromReasons(dies)) : Effect.void;
};

/**
 * Closes `scope` with `exit`. A finalizer that dies is reported, and what follows the close goes
 * on: a failed open still says it failed.
 */
const closeScope = (
  scope: Scope.Closeable,
  exit: Exit.Exit<unknown, unknown>,
): Effect.Effect<void> => Scope.close(scope, exit).pipe(Effect.catchCause(reportDefects));

/** A stable fingerprint of a spec: byte payloads by length and a checksum, never by content. */
const fingerprint = (value: unknown): string =>
  JSON.stringify(value, (_, field: unknown) => {
    if (field instanceof Uint8Array) {
      let sum = 0;
      for (const byte of field) sum = (sum * 31 + byte) >>> 0;
      return `bytes:${field.length}:${sum}`;
    }
    return typeof field === "bigint" ? field.toString() : field;
  });

export const make = Effect.fnUntraced(function* <R, Req extends Playout.ClipRequest>(
  options: Playout.Options<R, Req>,
  model: Playout.ClipModel<Req>,
) {
  const config: Policy.Config<Req> = {
    defaultSeconds: model.defaultSeconds,
    builtSeconds: model.builtSeconds,
    lanes: options.lanes.map((lane) => ({
      name: lane.name,
      conflict: lane.conflict ?? "queue",
      cut: lane.cut ?? false,
      strict: lane.strict ?? false,
    })),
    filler:
      options.filler === undefined
        ? undefined
        : {
            floor: millis(options.filler.runway.floor, 0) / 1000,
            target: millis(options.filler.runway.target, 0) / 1000,
            clip: sized(options.filler.clip),
            lengths: options.filler.lengths ?? model.lengths,
            invalid: (request, index) => {
              const issues = model.check(request, { _tag: "Filler", index }).join("; ");
              return issues === ""
                ? undefined
                : `filler clip ${String(index)} asked for a request outside ${model.name}'s documented limits: ${issues}`;
            },
            protect: options.filler.protect ?? "air",
          },
    maxBuildsInFlight: options.maxBuildsInFlight ?? 1,
    maxHistory: options.maxHistory ?? 4096,
    unknownTimeoutMs: millis(options.unknownTimeout, 60_000),
    leadMs: millis(options.renewal?.lead, 30_000),
    graceMs: millis(options.renewal?.grace, 250),
    maxSetupFailures: options.renewal?.maxSetupFailures ?? 3,
    maxModerations: options.maxModerations ?? 2,
  };
  /** An open must finish within this, the wait for a GPU and the connection included. */
  const openTimeout = options.renewal?.openTimeout ?? "3 minutes";
  const scope = yield* Effect.scope;
  const context = yield* Effect.context<R>();
  const acquisition = yield* currentParent;
  const inbox = yield* Queue.unbounded<Input<Req>>();
  const events = yield* PubSub.unbounded<Playout.Event>();
  const state = yield* Ref.make<Policy.State<Req>>(Policy.initial);
  const ids = yield* Ref.make(0);
  // What callers wait for, each resolved once and then forgotten: an edit's reply, a batch's
  // commit, a batch withdrawal's outcome, a drain.
  const replies = yield* Ref.make(new Map<number, Deferred.Deferred<Reply>>());
  const commits = yield* Ref.make(new Map<number, Deferred.Deferred<void, PlayoutClosed>>());
  const withdrawals = yield* Ref.make(
    new Map<string, Deferred.Deferred<Playout.WithdrawOutcome>>(),
  );
  const drains = yield* Ref.make(new Map<number, Deferred.Deferred<void>>());
  const placements = yield* Ref.make(
    new Map<number, Deferred.Deferred<Playout.Placement | null>>(),
  );
  const handles = yield* Ref.make(new Map<ItemKey, Handle>());
  const groupHandles = yield* Ref.make(new Map<ItemKey, GroupDeferreds>());
  const parents = yield* Ref.make(new Map<ItemKey, Tracer.ExternalSpan | undefined>());
  // Each live session's source, the scope it lives in, and its lane: the commands waiting there.
  const sources = yield* Ref.make(
    new Map<
      string,
      {
        readonly source: Playout.Source<Req>;
        readonly scope: Scope.Closeable;
        readonly lane: Queue.Queue<QueuedCommand<Req>>;
      }
    >(),
  );
  const onAir = yield* SubscriptionRef.make<Playout.Source<Req> | undefined>(undefined);
  // Counts the steps that changed the plan, so `forecasts` reads it again only after one.
  const revision = yield* SubscriptionRef.make(0);
  // Why the playout stopped; it dies with a defect that stopped it.
  const failure = yield* Deferred.make<ReactorFailure | InvalidFiller>();
  /** Completes once the playout has stopped, however it stopped. */
  const stopped = Deferred.await(failure).pipe(Effect.exit);
  // Why the latest open failed while no open has succeeded since, and why the latest session was
  // lost: the error, or the defect when there was no error.
  const lastOpenError = yield* Ref.make<
    Result.Result<ReactorFailure, Cause.Cause<never>> | undefined
  >(undefined);
  const lastLostError = yield* Ref.make<
    Result.Result<ReactorError, Cause.Cause<never>> | undefined
  >(undefined);
  const cleanup = yield* Ref.make<Playout.Cleanup>({ sessions: 0, retained: [] });

  const now = Effect.all({ mono: monotonic, wall: Clock.currentTimeMillis });
  const nextId = Ref.modify(ids, (id) => [id + 1, id + 1] as const);
  const offer = (input: Policy.Input<Req>, parent?: Tracer.ExternalSpan) =>
    Queue.offer(inbox, { input, parent });

  const handle = (key: ItemKey): Effect.Effect<Handle> =>
    Effect.gen(function* () {
      const existing = (yield* Ref.get(handles)).get(key);
      if (existing !== undefined) return existing;
      const created: Handle = {
        started: yield* Deferred.make<Effect.Success<Playout.ItemHandle["started"]>>(),
        outcome: yield* Deferred.make<Playout.Settled>(),
      };
      yield* Ref.update(handles, (all) => new Map(all).set(key, created));
      return created;
    });
  const itemHandle = (key: ItemKey): Effect.Effect<Playout.ItemHandle> =>
    Effect.map(handle(key), (value) => ({
      key,
      started: Deferred.await(value.started),
      outcome: Deferred.await(value.outcome),
    }));
  /** A group's start and outcome, which the plan resolves as it decides them. */
  const groupHandle = (key: ItemKey): Effect.Effect<GroupDeferreds> =>
    Effect.gen(function* () {
      const existing = (yield* Ref.get(groupHandles)).get(key);
      if (existing !== undefined) return existing;
      const created: GroupDeferreds = {
        started: yield* Deferred.make<Effect.Success<Playout.GroupHandle["started"]>>(),
        outcome: yield* Deferred.make<Playout.GroupOutcome>(),
      };
      yield* Ref.update(groupHandles, (all) => new Map(all).set(key, created));
      return created;
    });
  /** Registers `deferred` under `id` until it is taken, once, to be resolved. */
  const register = <K, D>(registry: Ref.Ref<Map<K, D>>, id: K, deferred: D) =>
    Ref.update(registry, (all) => new Map(all).set(id, deferred));
  const claim = <K, D>(registry: Ref.Ref<Map<K, D>>, id: K): Effect.Effect<D | undefined> =>
    Ref.modify(registry, (all) => {
      const found = all.get(id);
      if (found === undefined) return [undefined, all] as const;
      const next = new Map(all);
      next.delete(id);
      return [found, next] as const;
    });
  /**
   * Waits for `deferred`, or ends with `closed` once the playout has stopped
   * without resolving it; what it resolved first still wins.
   */
  const unlessStopped = <A, E, E2>(
    deferred: Deferred.Deferred<A, E>,
    closed: Effect.Effect<A, E2>,
  ): Effect.Effect<A, E | E2> => {
    const settled: Effect.Effect<A, E> = Deferred.await(deferred);
    const done: Effect.Effect<A, E | E2> = Effect.andThen(
      stopped,
      Effect.flatMap(Deferred.isDone(deferred), (done): Effect.Effect<A, E | E2> =>
        done ? settled : closed,
      ),
    );
    return Effect.raceFirst(settled, done);
  };

  const record = (report: CloseReport): Effect.Effect<void> =>
    Ref.update(cleanup, (value) => {
      const all = [...value.retained, report];
      const settled = all.filter((entry) => !mayStillBill(entry)).slice(-retainedReports);
      return {
        sessions: value.sessions + 1,
        retained: all.filter((entry) => mayStillBill(entry) || settled.includes(entry)),
      };
    });

  /** Closes `source` and records its report. A close that dies leaves none: it is reported. */
  const closeRecorded = (source: Playout.Source<Req>): Effect.Effect<void> =>
    Effect.flatMap(Effect.exit(source.close), (closed) =>
      Exit.isSuccess(closed) ? record(closed.value) : reportDefects(closed.cause),
    );

  const closeSource = (sessionId: string): Effect.Effect<void> =>
    Effect.gen(function* () {
      const entry = (yield* Ref.get(sources)).get(sessionId);
      if (entry === undefined) return;
      yield* Ref.update(sources, (all) => {
        const next = new Map(all);
        next.delete(sessionId);
        return next;
      });
      yield* closeRecorded(entry.source);
      yield* closeScope(entry.scope, Exit.void);
    });

  /** What a command for a session already gone gets: it was never sent. */
  const gone = (command: Policy.Command<Req>): Policy.CommandResult => ({
    _tag: "Failed",
    cause: CommandFailure.from(ReactorError.fromCode("InvalidState", "the session is gone"), {
      operation: command._tag,
      outcome: "not-submitted",
    }),
  });

  const run = ({ action, parent }: QueuedCommand<Req>): Effect.Effect<Policy.CommandResult> =>
    Effect.gen(function* () {
      const entry = (yield* Ref.get(sources)).get(action.sessionId);
      if (entry === undefined) return gone(action.command);
      const source = entry.source;
      const command = action.command;
      // A method that throws when called, rather than returning an effect, dies here as its
      // effect would have: its lane carries on.
      const result = Effect.suspend((): Effect.Effect<string | void, CommandFailure> => {
        switch (command._tag) {
          case "Enqueue":
            return source.enqueue(command.request, command.tag, command.continueFrom);
          case "Remove":
            return source.remove(command.clipId);
          case "Move":
            return source.move(command.clipId, command.position);
          case "Autoplay":
            return source.setAutoplay(command.enabled);
          case "Stop":
            return source.stop(command.clipId);
          case "Play":
            return source.play(command.clipId);
        }
      });
      const exit = yield* result.pipe(
        Effect.withSpan(
          "Playout.command",
          {
            ...spanOptions({ acquisition, parent }),
            attributes: {
              "reactor.playout.command": command._tag,
              "reactor.playout.command.id": action.id,
              "reactor.session.id": action.sessionId,
              ...(action.key === undefined ? {} : { "reactor.playout.item.key": action.key }),
            },
          },
          { captureStackTrace: false },
        ),
        Effect.exit,
      );
      if (Exit.isSuccess(exit))
        return {
          _tag: "Done",
          clipId: Predicate.isString(exit.value) ? exit.value : undefined,
        } as const;
      const error = Exit.findErrorOption(exit);
      if (Option.isSome(error)) {
        yield* reportDefects(exit.cause);
        return { _tag: "Failed", cause: error.value } as const;
      }
      // Whether the command went out can't be told: the plan treats it as unknown.
      yield* ErrorReporter.report(exit.cause);
      return { _tag: "Died" } as const;
    });

  const open = Effect.gen(function* () {
    const child = yield* Scope.fork(scope);
    const opened = yield* options.open.pipe(
      Effect.timeoutOrElse({
        duration: openTimeout,
        orElse: () =>
          Effect.fail(ReactorError.fromCode("Timeout", "opening a session took too long")),
      }),
      Effect.withSpan("Playout.open", spanOptions({ acquisition }), { captureStackTrace: false }),
      Scope.provide(child),
      Effect.provide(context),
      Effect.exit,
    );
    if (Exit.isFailure(opened)) {
      yield* closeScope(child, opened);
      const found = Cause.findError(opened.cause);
      yield* Ref.set(lastOpenError, found);
      if (Result.isFailure(found)) yield* ErrorReporter.report(opened.cause);
      else yield* reportDefects(opened.cause);
      const error = Result.getOrUndefined(found);
      // A failed acquisition still reports what it allocated, and that may still bill. Any other
      // failure, a timeout or a defect among them, may have allocated unseen.
      const allocated = !AcquisitionFailure.is(error) || error.cleanup.allocation !== "none";
      if (AcquisitionFailure.is(error) && allocated) yield* record(error.cleanup);
      const retryAfter = error?.retryAfter;
      return yield* offer({
        _tag: "OpenFailed",
        reason: error?.message ?? "opening a session died",
        fatal: false,
        allocated,
        ...(retryAfter === undefined ? {} : { retryAfterMs: Duration.toMillis(retryAfter) }),
      });
    }
    const source = opened.value;
    if ((yield* Ref.get(sources)).has(source.sessionId)) {
      // The plan knows a session by its id: a second live source under one would be taken for
      // the first. It is closed unused, and the playout fails, since opening again gives the same.
      yield* closeRecorded(source);
      yield* closeScope(child, Exit.void);
      const duplicate = ReactorError.fromCode(
        "InvalidState",
        "an opened source's session id is already in use",
      );
      yield* Ref.set(lastOpenError, Result.succeed(duplicate));
      return yield* offer({
        _tag: "OpenFailed",
        reason: duplicate.message,
        fatal: true,
        allocated: true,
      });
    }
    if (source.model !== undefined && source.model.name !== model.name) {
      // A source planned with another model's lengths and limits would air what this plan
      // never counted; opening again gives the same source.
      yield* closeRecorded(source);
      yield* closeScope(child, Exit.void);
      const mismatch = ReactorError.fromCode(
        "InvalidState",
        `an opened source runs ${source.model.name}, and this playout plans for ${model.name}`,
      );
      yield* Ref.set(lastOpenError, Result.succeed(mismatch));
      return yield* offer({
        _tag: "OpenFailed",
        reason: mismatch.message,
        fatal: true,
        allocated: true,
      });
    }
    yield* Ref.set(lastOpenError, undefined);
    // The session's commands, one at a time and in order. Its worker lives in the session's
    // scope, so the lane ends with the session.
    const lane = yield* Queue.unbounded<QueuedCommand<Req>>();
    yield* Effect.forever(
      Effect.flatMap(Queue.take(lane), (queued) =>
        Effect.flatMap(run(queued), (result) =>
          offer({ _tag: "Result", id: queued.action.id, result }),
        ),
      ),
    ).pipe(Effect.forkIn(child));
    yield* Ref.update(sources, (all) =>
      new Map(all).set(source.sessionId, { source, scope: child, lane }),
    );
    const lifetimeMs = Duration.toMillis(source.lifetime);
    yield* offer({
      _tag: "Opened",
      sessionId: source.sessionId,
      lifetimeMs: Number.isFinite(lifetimeMs) ? lifetimeMs : Infinity,
    });
    yield* source.events.pipe(
      Stream.runForEach((event) => offer({ _tag: "Source", sessionId: source.sessionId, event })),
      Effect.exit,
      Effect.flatMap((exit) =>
        Effect.gen(function* () {
          const found = Exit.isSuccess(exit) ? undefined : Cause.findError(exit.cause);
          yield* Ref.set(lastLostError, found);
          if (Exit.isFailure(exit) && found !== undefined) {
            if (Result.isFailure(found)) yield* ErrorReporter.report(exit.cause);
            else yield* reportDefects(exit.cause);
          }
          const error = found === undefined ? undefined : Result.getOrUndefined(found);
          yield* offer({
            _tag: "Lost",
            sessionId: source.sessionId,
            reason:
              found === undefined
                ? "the session ended"
                : (error?.message ?? "the session's events died"),
          });
        }),
      ),
      Effect.forkIn(child),
    );
  });

  /**
   * Why the plan failed the playout: the error behind a failed open or a loss,
   * or the defect when there was no error.
   */
  const failureOf = Effect.fnUntraced(function* (
    action: Extract<Policy.Action<Req>, { _tag: "Fail" }>,
  ): Effect.fn.Return<Result.Result<ReactorFailure | InvalidFiller, Cause.Cause<never>>> {
    switch (action.cause) {
      case "filler":
        return Result.succeed(InvalidFiller.make({ index: action.index, message: action.reason }));
      case "moderation":
        return Result.succeed(ReactorError.fromCode("Moderated", action.reason));
      case "open":
        return (
          (yield* Ref.get(lastOpenError)) ??
          Result.succeed(ReactorError.fromCode("InvalidState", action.reason))
        );
      case "lost":
        return (
          (yield* Ref.get(lastLostError)) ??
          Result.succeed(ReactorError.fromCode("InvalidState", action.reason))
        );
    }
  });

  const act = (action: Policy.Action<Req>): Effect.Effect<void> =>
    Effect.gen(function* () {
      switch (action._tag) {
        case "Command": {
          const entry = (yield* Ref.get(sources)).get(action.sessionId);
          if (entry !== undefined) {
            const parent =
              action.key === undefined ? undefined : (yield* Ref.get(parents)).get(action.key);
            return yield* Queue.offer(entry.lane, { action, parent });
          }
          return yield* offer({ _tag: "Result", id: action.id, result: gone(action.command) });
        }
        case "Open":
          return yield* Effect.forkIn(open, scope);
        case "Close":
          return yield* Effect.forkIn(closeSource(action.sessionId), scope);
        case "OnAir": {
          const entry = (yield* Ref.get(sources)).get(action.sessionId);
          return yield* SubscriptionRef.set(onAir, entry?.source);
        }
        case "Emit": {
          // Published before any handle resolves, so a subscriber has the event queued by the
          // time a caller waiting on the handle carries on.
          yield* PubSub.publish(events, action.event);
          if (action.event._tag === "AsRun") {
            const { key, status } = action.event.event;
            const value = yield* handle(key);
            const decided = Policy.decides(status);
            if (decided.started !== undefined)
              yield* Deferred.succeed(value.started, decided.started);
            if (decided.outcome !== undefined)
              yield* Deferred.succeed(value.outcome, decided.outcome);
          }
          return;
        }
        case "Accepted":
        case "Refused": {
          const reply = yield* claim(replies, action.id);
          const answer: Reply =
            action._tag === "Accepted"
              ? { _tag: "Accepted", results: action.results }
              : { _tag: "Refused", refusal: action.refusal };
          if (reply !== undefined) yield* Deferred.succeed(reply, answer);
          // A batch refused, at admission or when the playout closes, never commits.
          const commit = action._tag === "Refused" ? yield* claim(commits, action.id) : undefined;
          if (commit !== undefined) yield* Deferred.fail(commit, PlayoutClosed.make({}));
          return;
        }
        case "Committed": {
          const commit = yield* claim(commits, action.id);
          if (commit !== undefined) yield* Deferred.succeed(commit, undefined);
          return;
        }
        case "Withdrawn": {
          const waiting = yield* claim(withdrawals, `${action.id}:${action.index}`);
          if (waiting !== undefined) yield* Deferred.succeed(waiting, action.outcome);
          return;
        }
        case "Drained": {
          const drain = yield* claim(drains, action.id);
          if (drain !== undefined) yield* Deferred.succeed(drain, undefined);
          return;
        }
        case "Placed": {
          const placed = yield* claim(placements, action.id);
          if (placed !== undefined) yield* Deferred.succeed(placed, action.placement);
          return;
        }
        // A forecast is read in a look of its own, never asked of the loop.
        case "Forecasted":
          return;
        case "GroupStarted":
          return yield* Deferred.succeed((yield* groupHandle(action.key)).started, action.started);
        case "GroupSettled":
          return yield* Deferred.succeed((yield* groupHandle(action.key)).outcome, action.outcome);
        case "Forget": {
          const forget = <V>(all: Map<ItemKey, V>): Map<ItemKey, V> => {
            const next = new Map(all);
            for (const key of action.keys) next.delete(key);
            return next;
          };
          yield* Ref.update(parents, forget);
          yield* Ref.update(groupHandles, forget);
          return yield* Ref.update(handles, forget);
        }
        case "Fail": {
          const why = yield* failureOf(action);
          yield* Result.isSuccess(why)
            ? Deferred.succeed(failure, why.success)
            : Deferred.failCause(failure, why.failure);
          // A playout that failed for good closes its sessions at once: an owned one would bill off air.
          yield* Effect.forEach([...(yield* Ref.get(sources)).keys()], closeSource, {
            discard: true,
          }).pipe(Effect.forkIn(scope));
          return yield* offer({ _tag: "Close" });
        }
      }
    });

  const apply = (
    input: Policy.Input<Req>,
    parent?: Tracer.ExternalSpan,
  ): Effect.Effect<number | undefined> =>
    Effect.gen(function* () {
      const result = Policy.step(config, yield* Ref.get(state), input, yield* now);
      yield* Ref.set(state, result.state);
      // A step can both accept and dispatch, or even forget, an item. Register ownership
      // before carrying out its actions; a duplicate admission keeps the original owner.
      if (input._tag === "Edit")
        yield* Ref.update(parents, (all) => {
          const next = new Map(all);
          for (const action of result.actions)
            if (
              action._tag === "Emit" &&
              action.event._tag === "AsRun" &&
              action.event.event.status._tag === "Accepted" &&
              !next.has(action.event.event.key)
            )
              next.set(action.event.event.key, parent);
          return next;
        });
      yield* Effect.forEach(result.actions, act, { discard: true });
      // A wake that decided nothing, or a placement answered with nothing decided, leaves the plan
      // as it was.
      const changed =
        (input._tag !== "Tick" && input._tag !== "Place") ||
        result.actions.some((action) => action._tag !== "Placed");
      if (changed) yield* SubscriptionRef.update(revision, (count) => count + 1);
      return result.wake;
    });

  // When the scope closes: the loop stops first, then every waiting item settles and each session closes.
  yield* Effect.addFinalizer(() =>
    Effect.gen(function* () {
      yield* apply({ _tag: "Close" });
      yield* Deferred.succeed(failure, ReactorError.fromCode("Closed", "the playout closed"));
      for (const sessionId of (yield* Ref.get(sources)).keys()) yield* closeSource(sessionId);
    }),
  );
  /**
   * The loop died of a defect, such as a throwing `filler.clip`: the playout
   * dies with it, settles what it can, and closes every session, so nothing
   * waits on a loop that is gone and no owned session bills on. The defect is
   * reported, and its text never becomes a message of the library's.
   */
  const crashed = (cause: Cause.Cause<never>) =>
    Effect.gen(function* () {
      yield* ErrorReporter.report(cause);
      yield* Deferred.failCause(failure, cause);
      yield* apply({ _tag: "Close" }).pipe(Effect.catchCause(() => Effect.void));
      for (const value of (yield* Ref.get(handles)).values()) {
        yield* Deferred.succeed(value.started, Policy.indeterminate);
        yield* Deferred.succeed(value.outcome, Policy.indeterminate);
      }
      for (const value of (yield* Ref.get(groupHandles)).values()) {
        yield* Deferred.succeed(value.started, Policy.indeterminate);
        yield* Deferred.succeed(value.outcome, { _tag: "Indeterminate" });
      }
      yield* Effect.forEach([...(yield* Ref.get(sources)).keys()], closeSource, { discard: true });
    });
  yield* Effect.gen(function* () {
    let wake = yield* apply({ _tag: "Tick" });
    while (true) {
      const mono = yield* monotonic;
      const input =
        wake === undefined
          ? Option.some(yield* Queue.take(inbox))
          : yield* Queue.take(inbox).pipe(Effect.timeoutOption(untilWake(wake - mono)));
      // Nothing falls due before the wake: a wait that ends short of it, on a clock coarser than
      // the wake, waits on.
      if (Option.isNone(input) && wake !== undefined && (yield* monotonic) < wake) continue;
      const received = Option.getOrElse(input, (): Input<Req> => ({ input: { _tag: "Tick" } }));
      wake = yield* apply(received.input, received.parent);
    }
  }).pipe(
    Effect.catchCauseIf((cause) => !Cause.hasInterruptsOnly(cause), crashed),
    Effect.forkIn(scope),
  );

  // ---------------------------------------------------------------------------
  // Caller operations
  // ---------------------------------------------------------------------------
  const refusal = (refused: Policy.Refusal): SubmitError => {
    switch (refused._tag) {
      case "KeyMismatch":
        return KeyMismatch.make({ key: refused.key });
      case "WouldMissDeadline":
        return WouldMissDeadline.make({ key: refused.key });
      case "LaneBusy":
        return LaneBusy.make({ key: refused.key, lane: config.lanes[refused.lane]?.name ?? "" });
      case "InvalidItem":
        return InvalidItem.make({ key: refused.key, message: refused.message });
      case "PlayoutClosed":
        return PlayoutClosed.make({});
    }
  };
  const lane = (key: string, name: string) => {
    const index = config.lanes.findIndex((value) => value.name === name);
    return index < 0
      ? Effect.fail(InvalidItem.make({ key, message: `no lane is named ${name}` }))
      : Effect.succeed(index);
  };
  const itemKey = (key: string) =>
    Effect.fromOption(ItemKey.makeOption(key)).pipe(
      Effect.mapError(() => InvalidItem.make({ key, message: "a key must be a nonempty string" })),
    );
  /** An `At` start's lateness, its durations in milliseconds. */
  const lateOf = (
    late: Extract<Playout.Start, { readonly _tag: "At" }>["late"],
    duration: (value: Duration.Input) => Effect.Effect<number | undefined, InvalidItem>,
  ): Effect.Effect<Policy.Late, InvalidItem> => {
    switch (late._tag) {
      case "nextBoundary":
      case "drop":
        return Effect.succeed(late._tag);
      case "skipIfLaterThan":
        return Effect.map(duration(late.by), (skipAfterMs) => ({ skipAfterMs: skipAfterMs ?? 0 }));
      case "readyBy":
        return Effect.map(duration(late.by), (readyByMs) => ({ readyByMs: readyByMs ?? 0 }));
    }
  };
  const spec = (
    input: {
      readonly key: string;
      readonly request: Playout.ItemSpec<Req>["request"];
      readonly cues?: ReadonlyArray<Playout.Cue> | undefined;
      readonly continuity?: "previous" | undefined;
      readonly window?: Playout.Window | undefined;
      readonly start?: Playout.Start | undefined;
      readonly follows?: Playout.ClipTag | undefined;
    },
    laneIndex: number,
  ): Effect.Effect<Policy.Spec<Req>, InvalidItem> =>
    Effect.gen(function* () {
      const key = yield* itemKey(input.key);
      const bad = (message: string) => InvalidItem.make({ key, message });
      const issues = model.check(input.request, { _tag: "Item", key }).join("; ");
      if (issues !== "")
        return yield* bad(`the request is outside ${model.name}'s documented limits: ${issues}`);
      const duration = (value: Duration.Input | undefined) =>
        value === undefined
          ? Effect.undefined
          : Effect.fromOption(Duration.fromInput(value)).pipe(
              Effect.mapBoth({
                onFailure: () => bad("a duration is malformed"),
                onSuccess: Duration.toMillis,
              }),
            );
      const start = input.start ?? { _tag: "Follow" };
      const late = start._tag === "At" ? yield* lateOf(start.late, duration) : undefined;
      const cues = yield* Effect.forEach(input.cues ?? [], (cue) =>
        Effect.map(duration(cue.at.offset), (offsetMs) => ({
          name: cue.name,
          from: cue.at.from,
          offsetMs: offsetMs ?? 0,
        })),
      );
      const notBeforeMs = yield* duration(input.window?.notBefore);
      const startByMs = yield* duration(input.window?.startBy);
      const seconds = input.request.seconds ?? model.defaultSeconds;
      // Sent at the length the plan counts it at: a provider may build a request without
      // a length at its own default.
      const request = { ...input.request, seconds };
      const window: Policy.Spec<Req>["window"] =
        input.window === undefined
          ? undefined
          : {
              firm: input.window.firm,
              ...(notBeforeMs === undefined ? {} : { notBeforeMs }),
              ...(startByMs === undefined ? {} : { startByMs }),
            };
      const normalized: Policy.Spec<Req>["start"] =
        start._tag === "At"
          ? { _tag: "At", time: start.time, late: late ?? "nextBoundary" }
          : { _tag: start._tag };
      const result: Policy.Spec<Req> = {
        key,
        lane: laneIndex,
        request,
        seconds,
        fingerprint: fingerprint([
          laneIndex,
          request,
          cues,
          input.continuity,
          window,
          normalized,
          input.follows,
        ]),
        cues,
        continuity: input.continuity === "previous",
        ...(window === undefined ? {} : { window }),
        start: normalized,
        ...(input.follows === undefined ? {} : { follows: input.follows }),
      };
      return result;
    });

  const closed = Effect.fail(PlayoutClosed.make({}));
  /**
   * Sends one edit batch. Its commit and each withdrawal's outcome are
   * registered first, since the policy may settle them in the step that accepts it.
   */
  const submitEdits = (edits: ReadonlyArray<Policy.EditInput<Req>>, batch: boolean) =>
    Effect.gen(function* () {
      const id = yield* nextId;
      const reply = yield* Deferred.make<Reply>();
      yield* register(replies, id, reply);
      const commit = yield* Deferred.make<void, PlayoutClosed>();
      if (batch) yield* register(commits, id, commit);
      const outcomes = new Map<number, Deferred.Deferred<Playout.WithdrawOutcome>>();
      for (const [index, edit] of edits.entries())
        if (edit._tag === "Withdraw") {
          const outcome = yield* Deferred.make<Playout.WithdrawOutcome>();
          outcomes.set(index, outcome);
          yield* register(withdrawals, `${id}:${index}`, outcome);
        }
      // A refused or unanswered batch will settle none of what was registered for it.
      const forget = Effect.forEach(
        [...outcomes.keys()],
        (index) => claim(withdrawals, `${id}:${index}`),
        { discard: true },
      ).pipe(Effect.andThen(claim(commits, id)));
      yield* offer({ _tag: "Edit", id, edits, batch }, yield* currentParent);
      const answer = yield* unlessStopped(reply, closed).pipe(
        Effect.ensuring(claim(replies, id)),
        Effect.onError(() => forget),
      );
      if (answer._tag === "Refused") {
        yield* forget;
        return yield* refusal(answer.refusal);
      }
      return { commit, outcomes, results: answer.results };
    });
  /**
   * What a withdrawal of `key` would have found once the playout stopped: the
   * recorded fate of its item, or of a group's parts.
   */
  const stoppedOutcome = (key: ItemKey): Effect.Effect<Playout.WithdrawOutcome> =>
    Effect.map(Ref.get(state), (value) => Policy.fate(value, key));
  const toEdit = (edit: Playout.Edit<Req>): Effect.Effect<Policy.EditInput<Req>, InvalidItem> =>
    Effect.gen(function* () {
      switch (edit._tag) {
        case "Submit": {
          const index = yield* lane(edit.item.key, edit.item.lane);
          return { _tag: "Submit", spec: yield* spec(edit.item, index) };
        }
        case "SubmitGroup": {
          const index = yield* lane(edit.group.key, edit.group.lane);
          // A part is its clip alone: the group's start and window are its first part's, and the
          // rest follow it.
          const parts = yield* Effect.forEach(edit.group.parts, (part, partIndex) =>
            spec(
              {
                key: part.key,
                request: part.request,
                cues: part.cues,
                continuity: part.continuity,
                ...(partIndex === 0 ? { start: edit.group.start, window: edit.group.window } : {}),
              },
              index,
            ),
          );
          return {
            _tag: "SubmitGroup",
            key: yield* itemKey(edit.group.key),
            lane: index,
            parts,
            fingerprint: fingerprint(parts.map((part) => part.fingerprint)),
          };
        }
        case "Insert": {
          const anchor = edit.insert.before ?? edit.insert.after;
          if (
            anchor === undefined ||
            (edit.insert.before !== undefined && edit.insert.after !== undefined)
          )
            return yield* InvalidItem.make({
              key: edit.insert.key,
              message: "give exactly one of before and after",
            });
          return {
            _tag: "Insert",
            spec: yield* spec(edit.insert, 0),
            anchor: yield* itemKey(anchor),
            side: edit.insert.before === undefined ? "after" : "before",
          };
        }
        case "Replace":
          return {
            _tag: "Replace",
            key: yield* itemKey(edit.key),
            spec: yield* spec(edit.next, 0),
          };
        case "Withdraw":
          return { _tag: "Withdraw", key: yield* itemKey(edit.key) };
      }
    });
  const edit = (input: ReadonlyArray<Playout.Edit<Req>>, batch: boolean) =>
    Effect.gen(function* () {
      const edits = yield* Effect.forEach(input, toEdit);
      const { commit, outcomes, results } = yield* submitEdits(edits, batch);
      const mapped = yield* Effect.forEach(
        results,
        (result, index): Effect.Effect<Playout.EditResult> => {
          switch (result._tag) {
            case "Added":
              return Effect.map(itemHandle(result.key), (value) => ({
                _tag: "Added",
                handle: value,
              }));
            case "AddedGroup":
              return Effect.map(
                Effect.all([Effect.forEach(result.parts, itemHandle), groupHandle(result.key)]),
                ([parts, group]) => ({
                  _tag: "AddedGroup",
                  handle: {
                    key: result.key,
                    parts: parts as unknown as readonly [
                      Playout.ItemHandle,
                      ...Array<Playout.ItemHandle>,
                    ],
                    started: Deferred.await(group.started),
                    outcome: Deferred.await(group.outcome),
                  },
                }),
              );
            case "Withdrawal": {
              const outcome = outcomes.get(index);
              const key = edits[index]?._tag === "Withdraw" ? edits[index].key : undefined;
              const stopped =
                key === undefined ? Effect.succeed("not-found" as const) : stoppedOutcome(key);
              return Effect.succeed({
                _tag: "Withdrawal",
                outcome: outcome === undefined ? stopped : unlessStopped(outcome, stopped),
              });
            }
          }
        },
      );
      return { commit, results: mapped };
    });

  /**
   * The plan's forecast now, read as `state` is and never through the inbox. A playout that has
   * stopped projects nothing, though its plan closes only at the step after.
   */
  const forecast: Effect.Effect<Playout.Forecast> = Effect.gen(function* () {
    const value = yield* Ref.get(state);
    const at = yield* now;
    const stoppedAlready = yield* Deferred.isDone(failure);
    return Policy.forecast(config, stoppedAlready ? { ...value, closed: true } : value, at);
  });

  const service: Playout.Service<Req> = {
    submit: Effect.fn("Playout.submit")(function* (item: Playout.ItemSpec<Req>) {
      const { results } = yield* edit([{ _tag: "Submit", item }], false);
      const [first] = results;
      return first?._tag === "Added"
        ? first.handle
        : yield* Effect.die("a submit returned no handle");
    }),
    submitGroup: Effect.fn("Playout.submitGroup")(function* (group: Playout.GroupSpec<Req>) {
      const { results } = yield* edit([{ _tag: "SubmitGroup", group }], false);
      const [first] = results;
      return first?._tag === "AddedGroup"
        ? first.handle
        : yield* Effect.die("a group submit returned no handle");
    }),
    insert: Effect.fn("Playout.insert")(function* (insert: Playout.InsertSpec<Req>) {
      const { results } = yield* edit([{ _tag: "Insert", insert }], false);
      const [first] = results;
      return first?._tag === "Added"
        ? first.handle
        : yield* Effect.die("an insert returned no handle");
    }),
    replace: Effect.fn("Playout.replace")(function* (
      key: ItemKey,
      next: Playout.ReplacementSpec<Req>,
    ) {
      const { results } = yield* edit([{ _tag: "Replace", key, next }], false);
      const [first] = results;
      return first?._tag === "Added"
        ? first.handle
        : yield* Effect.die("a replace returned no handle");
    }),
    edit: Effect.fn("Playout.edit")(function* (edits: ReadonlyArray<Playout.Edit<Req>>) {
      const { commit, results } = yield* edit(edits, true);
      return { results, committed: unlessStopped(commit, closed) };
    }),
    release: Effect.fn("Playout.release")(function* (key: ItemKey) {
      const id = yield* nextId;
      const reply = yield* Deferred.make<Reply>();
      yield* register(replies, id, reply);
      yield* offer({ _tag: "Release", id, key });
      const answer = yield* unlessStopped(
        reply,
        Effect.fail(InvalidItem.make({ key, message: "the playout has stopped" })),
      ).pipe(Effect.ensuring(claim(replies, id)));
      if (answer._tag === "Refused")
        return yield* InvalidItem.make({
          key,
          message: answer.refusal._tag === "InvalidItem" ? answer.refusal.message : "not released",
        });
    }),
    place: Effect.fn("Playout.place")(function* (probe: Playout.PlaceProbe) {
      const key = yield* itemKey(probe.key);
      const seconds = probe.seconds ?? model.defaultSeconds;
      if (!Number.isFinite(seconds) || seconds < model.lengths.min || seconds > model.lengths.max)
        return yield* InvalidItem.make({
          key,
          message: `seconds must be from ${model.lengths.min} to ${model.lengths.max}`,
        });
      const submitIn =
        probe.submitIn === undefined
          ? Option.some(0)
          : Option.map(Duration.fromInput(probe.submitIn), Duration.toMillis);
      if (
        Option.isNone(submitIn) ||
        !Number.isFinite(submitIn.value) ||
        submitIn.value < 0 ||
        (typeof probe.submitIn === "number" && Number.isNaN(probe.submitIn))
      )
        return yield* InvalidItem.make({
          key,
          message: "submitIn must be a finite duration, from zero",
        });
      const id = yield* nextId;
      const reply = yield* Deferred.make<Playout.Placement | null>();
      yield* register(placements, id, reply);
      yield* offer({
        _tag: "Place",
        id,
        probe: {
          seconds,
          continuity: probe.continuity === "previous",
          submitInMs: submitIn.value,
        },
      });
      return yield* unlessStopped(reply, Effect.succeed(null)).pipe(
        Effect.ensuring(claim(placements, id)),
      );
    }),
    forecast,
    // A slow reader skips to the newest revision, and reads the forecast once for it.
    forecasts: SubscriptionRef.changes(revision).pipe(
      Stream.buffer({ capacity: 1, strategy: "sliding" }),
      Stream.mapEffect(() => forecast),
      Stream.interruptWhen(stopped),
    ),
    withdraw: Effect.fn("Playout.withdraw")(function* (key: ItemKey) {
      const none = Effect.succeed({ results: [] as ReadonlyArray<Playout.EditResult> });
      const { results } = yield* edit([{ _tag: "Withdraw", key }], false).pipe(
        // A malformed key names nothing; a stopped playout answers from the item's recorded
        // fate. A withdrawal admits nothing, so no other refusal can come back.
        Effect.catchTags({
          InvalidItem: () => none,
          PlayoutClosed: () => none,
          KeyMismatch: Effect.die,
          LaneBusy: Effect.die,
          WouldMissDeadline: Effect.die,
        }),
      );
      const [first] = results;
      if (first?._tag === "Withdrawal") return yield* first.outcome;
      return yield* stoppedOutcome(key);
    }),
    drain: Effect.fn("Playout.drain")(function* (drainOptions?: {
      readonly finish?: "playing" | "accepted";
    }) {
      if (yield* Deferred.isDone(failure)) return yield* PlayoutClosed.make({});
      const id = yield* nextId;
      const drained = yield* Deferred.make<void>();
      yield* register(drains, id, drained);
      yield* offer({ _tag: "Drain", id, finish: drainOptions?.finish ?? "playing" });
      yield* unlessStopped(drained, closed).pipe(Effect.ensuring(claim(drains, id)));
    }),
    state: Effect.flatMap(Ref.get(state), (value) =>
      Effect.map(now, (at) => Policy.view(config, value, at)),
    ),
    events: Stream.fromPubSub(events),
    asRun: Stream.fromPubSub(events).pipe(
      Stream.filter(
        (event): event is Extract<Playout.Event, { _tag: "AsRun" }> => event._tag === "AsRun",
      ),
      Stream.map((event) => event.event),
    ),
    video: SubscriptionRef.changes(onAir).pipe(
      Stream.switchMap((source) => source?.video ?? Stream.never),
      Stream.interruptWhen(stopped),
    ),
    audio: SubscriptionRef.changes(onAir).pipe(
      Stream.switchMap((source) => source?.audio ?? Stream.never),
      Stream.interruptWhen(stopped),
    ),
    failure: Deferred.await(failure),
    cleanup: Ref.get(cleanup),
  };
  return service;
});
