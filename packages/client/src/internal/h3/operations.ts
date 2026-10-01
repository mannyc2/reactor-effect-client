/**
 * Clip operations by submission: what the provider's evidence established
 * about each committed clip, from acceptance to its end. Every transition is
 * pure and returns the waiters it completes, run once the new table is in place.
 */
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Result from "effect/Result";
import { ClipEnded, CommandFailure, ReactorError } from "../../ReactorError.js";
import type { CommandReply } from "../../Session.js";
import type { Clip, DecodedMessage, MessageType } from "./messages.js";
import type { Acceptance, Identity } from "./state.js";

/** A phase a clip can be awaited to reach. */
export type ClipPhase = "generated" | "started";

/** One piece of evidence about a clip, and the transport generation that carried it. */
export interface ClipFact {
  readonly clipId: string;
  /** The lifecycle message that established it, or the snapshot that listed the clip. */
  readonly message: MessageType;
  readonly transportGeneration: bigint;
  readonly source: CommandReply;
}

/** What one clip operation has established so far; monotone. */
export interface OperationFacts {
  readonly submissionId: string;
  readonly acceptance?: Acceptance;
  readonly generated?: ClipFact;
  readonly started?: ClipFact;
  readonly ended?: ClipFact;
  /** The provider retired, or failed for good, before evidence decided the rest. */
  readonly indeterminate: boolean;
}

/**
 * A committed clip's facts as they arrive, each resolved at most once from the
 * provider's own evidence, and a phase only once the acceptance is decided. A
 * phase completes when it or a later one is observed, fails with `ClipEnded`
 * when the clip failed or was popped first, and fails `Indeterminate` if the
 * provider retires before evidence decides it. If the provider fails for good
 * first, as it does once its session ends (`TerminalSession`, `Moderated`) or
 * closes, what evidence did not decide fails with the provider's `failure`.
 * Every fact fails with the enqueue's own failure when that failure was
 * definite. Evidence from a later transport generation of the same session
 * still resolves the operation.
 */
export interface ClipOperation {
  readonly submissionId: string;
  readonly accepted: Effect.Effect<Acceptance, ReactorError | CommandFailure>;
  readonly reached: (phase: ClipPhase) => Effect.Effect<ClipFact, ReactorError | CommandFailure>;
  readonly ended: Effect.Effect<ClipFact, ReactorError | CommandFailure>;
  readonly facts: Effect.Effect<OperationFacts>;
}

type Failure = ReactorError | CommandFailure;

export interface Waiters {
  readonly accepted: Deferred.Deferred<Acceptance, Failure>;
  readonly generated: Deferred.Deferred<ClipFact, Failure>;
  readonly started: Deferred.Deferred<ClipFact, Failure>;
  readonly finished: Deferred.Deferred<ClipFact, Failure>;
}

export interface Operation {
  readonly identity: Identity;
  readonly waiters: Waiters;
  readonly acceptance: Acceptance | undefined;
  /** The clip the evidence names, known before the acceptance is decided. */
  readonly clipId: string | undefined;
  readonly generated: ClipFact | undefined;
  readonly started: ClipFact | undefined;
  readonly ended: ClipFact | undefined;
  readonly indeterminate: boolean;
  /** Decided without a clip: the enqueue's failure was definite. */
  readonly rejected: boolean;
  readonly holders: number;
}

export interface Table {
  readonly capacity: number;
  readonly operations: ReadonlyMap<string, Operation>;
  readonly byClip: ReadonlyMap<string, string>;
}

export type Step = readonly [Table, ReadonlyArray<Effect.Effect<void>>];

export const empty = (capacity: number): Table => ({
  capacity,
  operations: new Map(),
  byClip: new Map(),
});

export const makeWaiters: Effect.Effect<Waiters> = Effect.all({
  accepted: Deferred.make<Acceptance, Failure>(),
  generated: Deferred.make<ClipFact, Failure>(),
  started: Deferred.make<ClipFact, Failure>(),
  finished: Deferred.make<ClipFact, Failure>(),
});

/** Nothing more can change it: its clip ended once accepted, or it was decided without one. */
const settled = (operation: Operation): boolean =>
  (operation.ended !== undefined && operation.acceptance !== undefined) ||
  operation.indeterminate ||
  operation.rejected;

const put = (table: Table, operation: Operation): Table => {
  const operations = new Map(table.operations).set(operation.identity.id, operation);
  const byClip =
    operation.clipId === undefined || operation.rejected
      ? table.byClip
      : new Map(table.byClip).set(operation.clipId, operation.identity.id);
  return { ...table, operations, byClip };
};

const remove = (table: Table, id: string): Table => {
  const operation = table.operations.get(id);
  if (operation === undefined) return table;
  const operations = new Map(table.operations);
  operations.delete(id);
  const byClip = new Map(table.byClip);
  if (operation.clipId !== undefined && byClip.get(operation.clipId) === id)
    byClip.delete(operation.clipId);
  return { ...table, operations, byClip };
};

const succeed = <A>(deferred: Deferred.Deferred<A, Failure>, value: A) =>
  Effect.asVoid(Deferred.succeed(deferred, value));
const fail = <A>(deferred: Deferred.Deferred<A, Failure>, error: Failure) =>
  Effect.asVoid(Deferred.fail(deferred, error));

/**
 * Takes a slot for a submission about to dispatch. Operations that ended are
 * evicted first, oldest first; a table of unresolved operations refuses.
 */
export const reserve = ({
  table,
  identity,
  waiters,
}: {
  readonly table: Table;
  readonly identity: Identity;
  readonly waiters: Waiters;
}): Result.Result<Table, ReactorError> => {
  if (table.operations.has(identity.id)) return Result.succeed(table);
  let next = table;
  if (next.operations.size >= next.capacity) {
    const evictable = [...next.operations.values()].find(settled);
    if (evictable !== undefined) next = remove(next, evictable.identity.id);
  }
  if (next.operations.size >= next.capacity)
    return Result.fail(
      ReactorError.fromCode("Overflow", "H3 clip operation bound reached", {
        operation: "enqueue",
        outcome: "not-submitted",
      }),
    );
  return Result.succeed(
    put(next, {
      identity,
      waiters,
      acceptance: undefined,
      clipId: undefined,
      generated: undefined,
      started: undefined,
      ended: undefined,
      indeterminate: false,
      rejected: false,
      holders: 0,
    }),
  );
};

/** A commit that failed after reserving sent nothing. */
export const abandon = ({ table, id }: { readonly table: Table; readonly id: string }): Table =>
  remove(table, id);

/** What an operation established resolves its waiters, once its acceptance is decided. */
const publish = (operation: Operation): ReadonlyArray<Effect.Effect<void>> => {
  if (operation.acceptance === undefined) return [];
  const { generated, started, ended, waiters } = operation;
  const effects: Array<Effect.Effect<void>> = [];
  if (generated !== undefined) effects.push(succeed(waiters.generated, generated));
  if (started !== undefined) effects.push(succeed(waiters.started, started));
  if (ended === undefined) return effects;
  if (ended.message === "clip_failed" || ended.message === "clip_popped") {
    const error = ReactorError.make({
      reason: ClipEnded.make({
        message: `clip ended by ${ended.message}`,
        clipId: ended.clipId,
        lifecycle: ended.message,
        transportGeneration: ended.transportGeneration,
      }),
      context: { operation: "clip operation" },
    });
    // A phase that was not reached never will be.
    if (generated === undefined) effects.push(fail(waiters.generated, error));
    if (started === undefined) effects.push(fail(waiters.started, error));
  }
  effects.push(succeed(waiters.finished, ended));
  return effects;
};

/** The enqueue's own result: a definite failure decides the operation without a clip. */
export const settle = ({
  table,
  id,
  exit,
}: {
  readonly table: Table;
  readonly id: string;
  readonly exit: Exit.Exit<Acceptance, unknown>;
}): Step => {
  const operation = table.operations.get(id);
  if (operation === undefined || operation.acceptance !== undefined || Exit.isSuccess(exit))
    return [table, []];
  const error = Exit.findError(exit);
  if (error._tag !== "Success" || !CommandFailure.is(error.success)) return [table, []];
  const failure = error.success;
  if (failure.context.outcome === "unknown") return [table, []];
  // No clip will ever carry this submission, and what a clip showed before the
  // refusal is not its fact.
  const rejected: Operation = {
    ...operation,
    rejected: true,
    generated: undefined,
    started: undefined,
    ended: undefined,
  };
  const next = put(remove(table, id), { ...rejected, clipId: undefined });
  const { waiters } = operation;
  return [
    next,
    [
      fail(waiters.accepted, failure),
      fail(waiters.generated, failure),
      fail(waiters.started, failure),
      fail(waiters.finished, failure),
    ],
  ];
};

/**
 * Evidence the provider holds until the enqueue's reply decides the
 * acceptance: the clip's facts accrue meanwhile.
 */
export const identify = ({
  table,
  acceptance,
}: {
  readonly table: Table;
  readonly acceptance: Acceptance;
}): Table => {
  const operation = table.operations.get(acceptance.submissionId);
  if (operation === undefined || operation.clipId !== undefined || operation.rejected) return table;
  return put(table, { ...operation, clipId: acceptance.clip.clip_id });
};

/** The acceptance the provider recorded, on the same path that records it. */
export const accept = ({
  table,
  acceptance,
}: {
  readonly table: Table;
  readonly acceptance: Acceptance;
}): Step => {
  const operation = table.operations.get(acceptance.submissionId);
  if (operation === undefined || operation.acceptance !== undefined) return [table, []];
  const accepted: Operation = {
    ...operation,
    acceptance,
    clipId: operation.clipId ?? acceptance.clip.clip_id,
  };
  return [
    put(table, accepted),
    [succeed(operation.waiters.accepted, acceptance), ...publish(accepted)],
  ];
};

/**
 * A clip carrying a submission's exact prompt and metadata after its pending
 * acceptance expired, or in a later generation. It resolves the operation only.
 */
export const lateEvidence = ({
  table,
  id,
  clip,
  source,
}: {
  readonly table: Table;
  readonly id: string;
  readonly clip: Clip;
  readonly source: CommandReply;
}): Step => {
  const operation = table.operations.get(id);
  if (
    operation === undefined ||
    operation.acceptance !== undefined ||
    operation.rejected ||
    operation.identity.metadata !== clip.metadata ||
    operation.identity.prompt !== clip.prompt
  )
    return [table, []];
  return accept({
    table,
    acceptance: { submissionId: id, clip, evidence: { kind: "metadata", source } },
  });
};

const fact = (clipId: string, message: MessageType, source: CommandReply): ClipFact => ({
  clipId,
  message,
  transportGeneration: source.generation,
  source,
});

const advance = (
  table: Table,
  clipId: string,
  message: MessageType,
  source: CommandReply,
  phase: "generated" | "started" | "ended",
): Step => {
  const id = table.byClip.get(clipId);
  const operation = id === undefined ? undefined : table.operations.get(id);
  if (operation === undefined || operation.ended !== undefined || operation.indeterminate)
    return [table, []];
  const evidence = fact(clipId, message, source);
  // Started implies generated, and an end that finished or stopped implies both.
  const reachedStart =
    phase === "started" ||
    (phase === "ended" && (message === "clip_finished" || message === "clip_stopped"));
  const next: Operation = {
    ...operation,
    generated:
      operation.generated ?? (phase === "generated" || reachedStart ? evidence : undefined),
    started: operation.started ?? (reachedStart ? evidence : undefined),
    ended: phase === "ended" ? evidence : undefined,
  };
  return [put(table, next), publish(next)];
};

/** Advances operations from a message the reducer applied. */
export const observe = ({
  table,
  message,
  source,
}: {
  readonly table: Table;
  readonly message: DecodedMessage;
  readonly source: CommandReply;
}): Step => {
  if (table.byClip.size === 0) return [table, []];
  switch (message.type) {
    case "queue_update": {
      let next = table;
      const effects: Array<Effect.Effect<void>> = [];
      for (const clip of message.data.playout)
        if (clip.ready) {
          const [after, done] = advance(next, clip.clip_id, "queue_update", source, "generated");
          next = after;
          effects.push(...done);
        }
      return [next, effects];
    }
    case "state_update":
      return message.data.playing_clip_id === null
        ? [table, []]
        : advance(table, message.data.playing_clip_id, "state_update", source, "started");
    case "clip_generated":
      return advance(table, message.data.clip.clip_id, message.type, source, "generated");
    case "clip_started":
      return advance(table, message.data.clip.clip_id, message.type, source, "started");
    case "clip_finished":
    case "clip_stopped":
    case "clip_failed":
    case "clip_popped":
      return advance(table, message.data.clip.clip_id, message.type, source, "ended");
    default:
      return [table, []];
  }
};

/**
 * No more evidence can come: what it did not decide fails with `error`. A retiring provider also
 * lets go of the operations nobody holds.
 */
const strand = (table: Table, error: ReactorError, retiring: boolean): Step => {
  let next = table;
  const effects: Array<Effect.Effect<void>> = [];
  for (const operation of table.operations.values()) {
    const { waiters } = operation;
    effects.push(
      fail(waiters.accepted, error),
      fail(waiters.generated, error),
      fail(waiters.started, error),
      fail(waiters.finished, error),
    );
    next =
      retiring && operation.holders === 0
        ? remove(next, operation.identity.id)
        : put(next, {
            ...operation,
            indeterminate: operation.indeterminate || !settled(operation),
          });
  }
  return [next, effects];
};

/**
 * The provider failed for good, so it reads no more evidence: what evidence did not decide fails
 * with the provider's failure.
 */
export const failUndecided = ({
  table,
  error,
}: {
  readonly table: Table;
  readonly error: ReactorError;
}): Step => strand(table, error, false);

/** The provider retired: what evidence did not decide is `Indeterminate`. */
export const retire = (table: Table): Step =>
  strand(
    table,
    ReactorError.fromCode("Indeterminate", "H3 provider retired before the clip's evidence", {
      operation: "clip operation",
    }),
    true,
  );

/** A holder of an operation; releasing the last holder acknowledges it and frees its slot. */
export const hold = ({
  table,
  id,
}: {
  readonly table: Table;
  readonly id: string;
}): Result.Result<Table, ReactorError> => {
  const operation = table.operations.get(id);
  return operation === undefined
    ? Result.fail(
        ReactorError.fromCode(
          "InvalidState",
          "H3 has no clip operation for this submission; it has not committed or was released",
          { operation: "clip operation" },
        ),
      )
    : Result.succeed(put(table, { ...operation, holders: operation.holders + 1 }));
};

export const release = ({ table, id }: { readonly table: Table; readonly id: string }): Table => {
  const operation = table.operations.get(id);
  if (operation === undefined) return table;
  return operation.holders <= 1
    ? remove(table, id)
    : put(table, { ...operation, holders: operation.holders - 1 });
};

/** A clip's facts are the operation's once its acceptance is decided. */
export const factsOf = ({
  id,
  operation,
}: {
  readonly id: string;
  readonly operation: Operation | undefined;
}): OperationFacts =>
  operation?.acceptance === undefined
    ? { submissionId: id, indeterminate: operation?.indeterminate ?? false }
    : {
        submissionId: id,
        acceptance: operation.acceptance,
        ...(operation.generated === undefined ? {} : { generated: operation.generated }),
        ...(operation.started === undefined ? {} : { started: operation.started }),
        ...(operation.ended === undefined ? {} : { ended: operation.ended }),
        indeterminate: operation.indeterminate,
      };
