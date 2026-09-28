/**
 * The playout's shell around its pure policy. One fiber owns the policy state:
 * it takes an input (a caller's edit, a session's evidence, a command's result)
 * or wakes at the policy's next deadline, runs `step`, and carries out what the
 * step asks. Provider commands go through one worker, one at a time; session
 * opens and closes run in their own fibers and report back through the inbox.
 */
import * as Cause from "effect/Cause";
import * as Clock from "effect/Clock";
import * as Deferred from "effect/Deferred";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Option from "effect/Option";
import * as PubSub from "effect/PubSub";
import * as Queue from "effect/Queue";
import * as Ref from "effect/Ref";
import * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import * as SubscriptionRef from "effect/SubscriptionRef";
import type * as Playout from "../../Playout.js";
import { requestSeconds } from "../h3/profile.js";
import { take } from "../queue.js";
import { ReactorError } from "../../ReactorError.js";
import type { ReactorFailure } from "../../ReactorError.js";
import type { CloseReport } from "../../Session.js";
import {
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
  readonly started: Deferred.Deferred<Playout.AsRunStatus>;
  readonly outcome: Deferred.Deferred<Playout.AsRunStatus>;
};
type Reply =
  | { readonly _tag: "Accepted"; readonly results: ReadonlyArray<Policy.EditReply> }
  | { readonly _tag: "Refused"; readonly refusal: Policy.Refusal };

/** Close reports kept beyond the unconfirmed ones. */
const retainedReports = 8;

const millis = (input: Duration.Input | undefined, fallback: number): number =>
  input === undefined ? fallback : Duration.toMillis(Duration.fromInputUnsafe(input));

const monotonic = Effect.map(Clock.monotonicTimeNanos, (nanos) => Number(nanos) / 1_000_000);

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

export const make = Effect.fnUntraced(function* <R>(options: Playout.Options<R>) {
  const config: Policy.Config = {
    lanes: options.lanes.map((lane) => ({
      name: lane.name,
      conflict: lane.conflict ?? "queue",
      cut: lane.cut ?? false,
    })),
    filler:
      options.filler === undefined
        ? undefined
        : {
            floor: millis(options.filler.runway.floor, 0) / 1000,
            target: millis(options.filler.runway.target, 0) / 1000,
            clip: options.filler.clip,
            lengths: options.filler.lengths ?? requestSeconds,
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
  const inbox = yield* Queue.unbounded<Policy.Input>();
  const work = yield* Queue.unbounded<Extract<Policy.Action, { _tag: "Command" }>>();
  const events = yield* PubSub.unbounded<Playout.Event>();
  const state = yield* Ref.make(Policy.initial);
  const ids = yield* Ref.make(0);
  // What callers wait for, each resolved once and then forgotten: an edit's reply, a batch's
  // commit, a batch withdrawal's outcome, a drain.
  const replies = yield* Ref.make(new Map<number, Deferred.Deferred<Reply>>());
  const commits = yield* Ref.make(new Map<number, Deferred.Deferred<void, PlayoutClosed>>());
  const withdrawals = yield* Ref.make(
    new Map<string, Deferred.Deferred<Playout.WithdrawOutcome>>(),
  );
  const drains = yield* Ref.make(new Map<number, Deferred.Deferred<void>>());
  const handles = yield* Ref.make(new Map<ItemKey, Handle>());
  const sources = yield* Ref.make(
    new Map<string, { readonly source: Playout.Source; readonly scope: Scope.Closeable }>(),
  );
  const onAir = yield* SubscriptionRef.make<Playout.Source | undefined>(undefined);
  const failure = yield* Deferred.make<ReactorFailure>();
  // Why the latest open failed while no open has succeeded since, and why the latest session was lost.
  const lastOpenError = yield* Ref.make<ReactorFailure | undefined>(undefined);
  const lastLostError = yield* Ref.make<ReactorError | undefined>(undefined);
  const cleanup = yield* Ref.make<Playout.Cleanup>({ sessions: 0, retained: [] });

  const now = Effect.all({ mono: monotonic, wall: Clock.currentTimeMillis });
  const nextId = Ref.modify(ids, (id) => [id + 1, id + 1] as const);
  const offer = (input: Policy.Input) => Queue.offer(inbox, input);

  const handle = (key: ItemKey): Effect.Effect<Handle> =>
    Effect.gen(function* () {
      const existing = (yield* Ref.get(handles)).get(key);
      if (existing !== undefined) return existing;
      const created: Handle = {
        started: yield* Deferred.make<Playout.AsRunStatus>(),
        outcome: yield* Deferred.make<Playout.AsRunStatus>(),
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
  /** Waits for `deferred`, or ends with `closed` once the playout has stopped. */
  const unlessStopped = <A, E, E2>(
    deferred: Deferred.Deferred<A, E>,
    closed: Effect.Effect<A, E2>,
  ): Effect.Effect<A, E | E2> =>
    Effect.raceFirst(Deferred.await(deferred), Effect.andThen(Deferred.await(failure), closed));

  const record = (report: CloseReport): Effect.Effect<void> =>
    Ref.update(cleanup, (value) => {
      const unconfirmed = (entry: CloseReport) =>
        entry.allocation !== "none" && entry.ownership === "owned" && !entry.remote.confirmed;
      const all = [...value.retained, report];
      const confirmed = all.filter((entry) => !unconfirmed(entry)).slice(-retainedReports);
      return {
        sessions: value.sessions + 1,
        retained: all.filter((entry) => unconfirmed(entry) || confirmed.includes(entry)),
      };
    });

  const closeSource = (sessionId: string): Effect.Effect<void> =>
    Effect.gen(function* () {
      const entry = (yield* Ref.get(sources)).get(sessionId);
      if (entry === undefined) return;
      yield* Ref.update(sources, (all) => {
        const next = new Map(all);
        next.delete(sessionId);
        return next;
      });
      yield* record(yield* entry.source.close);
      yield* Scope.close(entry.scope, Exit.void);
    });

  const open = Effect.gen(function* () {
    const child = yield* Scope.fork(scope);
    const opened = yield* options.open.pipe(
      Effect.timeoutOrElse({
        duration: openTimeout,
        orElse: () =>
          Effect.fail(ReactorError.fromCode("Timeout", "opening a session took too long")),
      }),
      Scope.provide(child),
      Effect.provide(context),
      Effect.exit,
    );
    if (Exit.isFailure(opened)) {
      yield* Scope.close(child, opened);
      const error = Exit.findErrorOption(opened).pipe(Option.getOrUndefined);
      yield* Ref.set(lastOpenError, error);
      return yield* offer({
        _tag: "OpenFailed",
        reason: error?.message ?? "opening a session failed",
        fatal: false,
      });
    }
    const source = opened.value;
    yield* Ref.set(lastOpenError, undefined);
    yield* Ref.update(sources, (all) =>
      new Map(all).set(source.sessionId, { source, scope: child }),
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
      Effect.flatMap((exit) => {
        const error = Exit.isSuccess(exit)
          ? undefined
          : Exit.findErrorOption(exit).pipe(Option.getOrUndefined);
        return Ref.set(lastLostError, error).pipe(
          Effect.andThen(
            offer({
              _tag: "Lost",
              sessionId: source.sessionId,
              reason: Exit.isSuccess(exit)
                ? "the session ended"
                : (error?.message ?? "the session failed"),
            }),
          ),
        );
      }),
      Effect.forkIn(child),
    );
  });

  const run = (
    action: Extract<Policy.Action, { _tag: "Command" }>,
  ): Effect.Effect<Policy.CommandResult> =>
    Effect.gen(function* () {
      const entry = (yield* Ref.get(sources)).get(action.sessionId);
      if (entry === undefined)
        return {
          _tag: "Failed",
          outcome: "not-submitted",
          retryable: false,
          reason: "the session is gone",
        } as const;
      const source = entry.source;
      const command = action.command;
      const result =
        command._tag === "Enqueue"
          ? Effect.map(
              source.enqueue(command.request, command.tag, command.continueFrom),
              (clipId) => ({ clipId }),
            )
          : command._tag === "Remove"
            ? source.remove(command.clipId)
            : command._tag === "Move"
              ? source.move(command.clipId, command.position)
              : command._tag === "Autoplay"
                ? source.setAutoplay(command.enabled)
                : source.cut(command.clipId, command.next);
      const exit = yield* Effect.exit(result);
      if (Exit.isSuccess(exit)) {
        const value: unknown = exit.value;
        return {
          _tag: "Done",
          clipId:
            typeof value === "object" && value !== null && "clipId" in value
              ? String(value.clipId)
              : undefined,
        } as const;
      }
      const error = Exit.findErrorOption(exit).pipe(Option.getOrUndefined);
      return {
        _tag: "Failed",
        outcome: error?.context.outcome ?? "unknown",
        retryable: error?.isRetryable ?? false,
        reason: error?.message ?? "the command failed",
      } as const;
    });

  const act = (action: Policy.Action): Effect.Effect<void> =>
    Effect.gen(function* () {
      switch (action._tag) {
        case "Command":
          return yield* Queue.offer(work, action);
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
            if (decided.started) yield* Deferred.succeed(value.started, status);
            if (decided.outcome) yield* Deferred.succeed(value.outcome, status);
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
        case "Forget":
          return yield* Ref.update(handles, (all) => {
            const next = new Map(all);
            for (const key of action.keys) next.delete(key);
            return next;
          });
        case "Fail": {
          const error: ReactorFailure =
            action.cause === "moderation"
              ? ReactorError.fromCode("Moderated", action.reason)
              : ((action.cause === "open"
                  ? yield* Ref.get(lastOpenError)
                  : yield* Ref.get(lastLostError)) ??
                ReactorError.fromCode("InvalidState", action.reason));
          yield* Deferred.succeed(failure, error);
          // A playout that failed for good closes its sessions at once: an owned one would bill off air.
          yield* Effect.forEach([...(yield* Ref.get(sources)).keys()], closeSource, {
            discard: true,
          }).pipe(Effect.forkIn(scope));
          return yield* offer({ _tag: "Close" });
        }
      }
    });

  const apply = (input: Policy.Input): Effect.Effect<number | undefined> =>
    Effect.gen(function* () {
      const result = Policy.step(config, yield* Ref.get(state), input, yield* now);
      yield* Ref.set(state, result.state);
      yield* Effect.forEach(result.actions, act, { discard: true });
      return result.wake;
    });

  // The worker: provider commands one at a time, each result back through the inbox.
  yield* Effect.forever(
    Effect.flatMap(take(work), (action) =>
      Effect.flatMap(run(action), (result) => offer({ _tag: "Result", id: action.id, result })),
    ),
  ).pipe(Effect.forkIn(scope));

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
   * fails, settles what it can, and closes every session, so nothing waits on a
   * loop that is gone and no owned session bills on.
   */
  const crashed = (cause: Cause.Cause<never>) =>
    Effect.gen(function* () {
      yield* Deferred.succeed(
        failure,
        ReactorError.fromCode("InvalidState", `the playout's plan failed: ${Cause.pretty(cause)}`),
      );
      yield* apply({ _tag: "Close" }).pipe(Effect.catchCause(() => Effect.void));
      const unsettled: Playout.AsRunStatus = { _tag: "Unknown", terminal: true };
      for (const value of (yield* Ref.get(handles)).values()) {
        yield* Deferred.succeed(value.started, unsettled);
        yield* Deferred.succeed(value.outcome, unsettled);
      }
      yield* Effect.forEach([...(yield* Ref.get(sources)).keys()], closeSource, { discard: true });
    });
  yield* Effect.gen(function* () {
    let wake = yield* apply({ _tag: "Tick" });
    while (true) {
      const mono = yield* monotonic;
      const input =
        wake === undefined
          ? Option.some(yield* take(inbox))
          : yield* take(inbox).pipe(Effect.timeoutOption(Math.max(0, wake - mono)));
      wake = yield* apply(Option.getOrElse(input, (): Policy.Input => ({ _tag: "Tick" })));
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
  const spec = (
    input: {
      readonly key: string;
      readonly request: Playout.ItemSpec["request"];
      readonly cues?: ReadonlyArray<Playout.Cue> | undefined;
      readonly continuity?: "previous" | undefined;
      readonly window?: Playout.Window | undefined;
      readonly start?: Playout.Start | undefined;
    },
    laneIndex: number,
  ): Effect.Effect<Policy.Spec, InvalidItem> =>
    Effect.gen(function* () {
      const key = yield* itemKey(input.key);
      const bad = (message: string) => InvalidItem.make({ key, message });
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
      const late: Policy.Late | undefined =
        start._tag !== "At"
          ? undefined
          : start.late._tag === "skipIfLaterThan"
            ? { skipAfterMs: (yield* duration(start.late.by)) ?? 0 }
            : start.late._tag;
      const cues = yield* Effect.forEach(input.cues ?? [], (cue) =>
        Effect.map(duration(cue.at.offset), (offsetMs) => ({
          name: cue.name,
          from: cue.at.from,
          offsetMs: offsetMs ?? 0,
        })),
      );
      const notBeforeMs = yield* duration(input.window?.notBefore);
      const startByMs = yield* duration(input.window?.startBy);
      const seconds = input.request.seconds ?? requestSeconds.min;
      const window: Policy.Spec["window"] =
        input.window === undefined
          ? undefined
          : {
              firm: input.window.firm,
              ...(notBeforeMs === undefined ? {} : { notBeforeMs }),
              ...(startByMs === undefined ? {} : { startByMs }),
            };
      const normalized: Policy.Spec["start"] =
        start._tag === "At"
          ? { _tag: "At", time: start.time, late: late ?? "nextBoundary" }
          : { _tag: start._tag };
      const result: Policy.Spec = {
        key,
        lane: laneIndex,
        request: input.request,
        seconds,
        fingerprint: fingerprint([
          laneIndex,
          input.request,
          cues,
          input.continuity,
          window,
          normalized,
        ]),
        cues,
        continuity: input.continuity === "previous",
        ...(window === undefined ? {} : { window }),
        start: normalized,
      };
      return result;
    });

  const closed = Effect.fail(PlayoutClosed.make({}));
  /**
   * Sends one edit batch. Its commit and each withdrawal's outcome are
   * registered first, since the policy may settle them in the step that accepts it.
   */
  const submitEdits = (edits: ReadonlyArray<Policy.EditInput>, batch: boolean) =>
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
      yield* offer({ _tag: "Edit", id, edits, batch });
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
  /** What a withdrawal of `key` would have found once the playout stopped: its recorded fate. */
  const stoppedOutcome = (key: ItemKey): Effect.Effect<Playout.WithdrawOutcome> =>
    Effect.gen(function* () {
      const value = (yield* Ref.get(handles)).get(key);
      if (value === undefined || !(yield* Deferred.isDone(value.started))) return "not-found";
      const started = yield* Deferred.await(value.started);
      if (started._tag === "Dropped") return "withdrawn";
      return started._tag === "Started" || started._tag === "Ended"
        ? "already-started"
        : "not-found";
    });
  const toEdit = (edit: Playout.Edit): Effect.Effect<Policy.EditInput, InvalidItem> =>
    Effect.gen(function* () {
      switch (edit._tag) {
        case "Submit": {
          const index = yield* lane(edit.item.key, edit.item.lane);
          return { _tag: "Submit", spec: yield* spec(edit.item, index) };
        }
        case "SubmitGroup": {
          const index = yield* lane(edit.group.key, edit.group.lane);
          const parts = yield* Effect.forEach(edit.group.parts, (part, partIndex) =>
            spec(
              {
                ...part,
                ...(partIndex === 0 && edit.group.window !== undefined
                  ? { window: edit.group.window }
                  : {}),
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
  const edit = (input: ReadonlyArray<Playout.Edit>, batch: boolean) =>
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
              return Effect.map(Effect.forEach(result.parts, itemHandle), (parts) => ({
                _tag: "AddedGroup",
                handle: {
                  key: result.key,
                  parts: parts as unknown as readonly [
                    Playout.ItemHandle,
                    ...Array<Playout.ItemHandle>,
                  ],
                },
              }));
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

  const service: Playout.Playout["Service"] = {
    submit: Effect.fn("Playout.submit")(function* (item: Playout.ItemSpec) {
      const { results } = yield* edit([{ _tag: "Submit", item }], false);
      const [first] = results;
      return first?._tag === "Added"
        ? first.handle
        : yield* Effect.die("a submit returned no handle");
    }),
    submitGroup: Effect.fn("Playout.submitGroup")(function* (group: Playout.GroupSpec) {
      const { results } = yield* edit([{ _tag: "SubmitGroup", group }], false);
      const [first] = results;
      return first?._tag === "AddedGroup"
        ? first.handle
        : yield* Effect.die("a group submit returned no handle");
    }),
    insert: Effect.fn("Playout.insert")(function* (insert: Playout.InsertSpec) {
      const { results } = yield* edit([{ _tag: "Insert", insert }], false);
      const [first] = results;
      return first?._tag === "Added"
        ? first.handle
        : yield* Effect.die("an insert returned no handle");
    }),
    replace: Effect.fn("Playout.replace")(function* (key: ItemKey, next: Playout.ReplacementSpec) {
      const { results } = yield* edit([{ _tag: "Replace", key, next }], false);
      const [first] = results;
      return first?._tag === "Added"
        ? first.handle
        : yield* Effect.die("a replace returned no handle");
    }),
    edit: Effect.fn("Playout.edit")(function* (edits: ReadonlyArray<Playout.Edit>) {
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
    ),
    audio: SubscriptionRef.changes(onAir).pipe(
      Stream.switchMap((source) => source?.audio ?? Stream.never),
    ),
    failure: Deferred.await(failure),
    cleanup: Ref.get(cleanup),
  };
  return service;
});
