import { Effect, Ref, Schema, Semaphore } from "effect";

export type SequenceStatus = "open" | "sealed" | "indeterminate" | "retired";

export type MemberOutcome =
  | {
      readonly _tag: "Accepted";
      readonly memberId: string;
      readonly clipId: string;
      readonly index: number;
      readonly final: boolean;
    }
  | { readonly _tag: "Rejected"; readonly memberId: string; readonly reason: string | undefined }
  | {
      readonly _tag: "Indeterminate";
      readonly memberId: string;
      readonly reason: string | undefined;
    };

export interface SequenceSnapshot<Owner> {
  readonly id: string;
  readonly owner: Owner;
  readonly status: SequenceStatus;
  readonly acceptedCount: number;
  readonly rejectedCount: number;
  readonly indeterminateCount: number;
  readonly pendingCount: number;
  readonly sealRequested: boolean;
  readonly members: ReadonlyArray<MemberOutcome>;
}

/** An affinity bound that is not a positive safe integer. */
export class InvalidSequenceOptions extends Schema.TaggedError<InvalidSequenceOptions>(
  "reactor-effect-client/InvalidSequenceOptions",
)("InvalidSequenceOptions", { message: Schema.String }) {}

/** Why a sequence refused an operation. */
export const SequenceCode = Schema.Literals([
  "capacity",
  "member-capacity",
  "owner-mismatch",
  "sealed",
  "sealing",
  "indeterminate",
  "retired",
  "missing",
  "member-missing",
  "member-duplicate",
  "not-releasable",
]);
export type SequenceCode = typeof SequenceCode.Type;

/** A sequence operation the affinity state refused; `code` says why. */
export class SequenceError extends Schema.TaggedError<SequenceError>(
  "reactor-effect-client/SequenceError",
)("SequenceError", { sequenceId: Schema.String, code: SequenceCode }) {
  override get message(): string {
    return `Sequence ${this.sequenceId}: ${this.code}`;
  }
}

export interface Affinity<Owner> {
  readonly get: (id: string) => Effect.Effect<SequenceSnapshot<Owner> | undefined>;
  readonly snapshots: Effect.Effect<ReadonlyArray<SequenceSnapshot<Owner>>>;
  /** Bind no later than the first member dispatch. Existing ownership never changes. */
  readonly bind: (id: string, owner: Owner) => Effect.Effect<Owner, SequenceError>;
  /** Register one member whose remote outcome is not yet known. */
  readonly begin: (id: string, memberId: string) => Effect.Effect<void, SequenceError>;
  readonly accepted: (
    id: string,
    memberId: string,
    clipId: string,
    final?: boolean,
  ) => Effect.Effect<void, SequenceError>;
  readonly rejected: (
    id: string,
    memberId: string,
    reason?: string,
    final?: boolean,
  ) => Effect.Effect<void, SequenceError>;
  readonly uncertain: (
    id: string,
    memberId: string,
    reason?: string,
  ) => Effect.Effect<void, SequenceError>;
  /**
   * Stop admitting members. With no pending member this seals immediately;
   * otherwise the sequence seals after every already-started member resolves.
   */
  readonly seal: (id: string) => Effect.Effect<void, SequenceError>;
  /** Session death retires known sequences and makes still-pending members indeterminate. */
  readonly retire: (owner: Owner) => Effect.Effect<void>;
  /**
   * Explicitly acknowledge that an indeterminate sequence has been accounted
   * for externally. Its history stays queryable as Retired until release.
   */
  readonly acknowledgeIndeterminate: (id: string) => Effect.Effect<void, SequenceError>;
  /** Explicit cleanup after history has been consumed. Open/unknown/pending work is retained. */
  readonly release: (id: string) => Effect.Effect<void, SequenceError>;
  readonly size: Effect.Effect<number>;
}

export interface Options<Owner> {
  readonly maxEntries?: number;
  readonly maxMembers?: number;
  readonly sameOwner?: (left: Owner, right: Owner) => boolean;
}

interface Entry<Owner> {
  readonly id: string;
  readonly owner: Owner;
  readonly status: SequenceStatus;
  readonly acceptedCount: number;
  readonly rejectedCount: number;
  readonly indeterminateCount: number;
  readonly pending: ReadonlySet<string>;
  readonly sealRequested: boolean;
  readonly members: ReadonlyArray<MemberOutcome>;
}

const immutableMembers = (members: ReadonlyArray<MemberOutcome>): ReadonlyArray<MemberOutcome> =>
  Object.freeze(members.map((member) => Object.freeze({ ...member })));

const snapshot = <Owner>(entry: Entry<Owner>): SequenceSnapshot<Owner> =>
  Object.freeze({
    id: entry.id,
    owner: entry.owner,
    status: entry.status,
    acceptedCount: entry.acceptedCount,
    rejectedCount: entry.rejectedCount,
    indeterminateCount: entry.indeterminateCount,
    pendingCount: entry.pending.size,
    sealRequested: entry.sealRequested,
    members: immutableMembers(entry.members),
  });

const positiveInteger = (
  value: number,
  name: string,
): Effect.Effect<number, InvalidSequenceOptions> =>
  Number.isSafeInteger(value) && value > 0
    ? Effect.succeed(value)
    : Effect.fail(
        new InvalidSequenceOptions({ message: `${name} must be a positive safe integer` }),
      );

/**
 * Bounded sequence-affinity state. Unresolved work is never evicted to admit
 * another sequence or member; callers explicitly account and release history.
 */
export const makeAffinity = <Owner>(
  options: Options<Owner> = {},
): Effect.Effect<Affinity<Owner>, InvalidSequenceOptions> =>
  Effect.gen(function* () {
    const maxEntries = yield* positiveInteger(options.maxEntries ?? 256, "maxEntries");
    const maxMembers = yield* positiveInteger(options.maxMembers ?? 256, "maxMembers");
    const sameOwner = options.sameOwner ?? Object.is;
    const entries = yield* Ref.make(new Map<string, Entry<Owner>>());
    const lock = yield* Semaphore.make(1);

    const get = (id: string) =>
      Ref.get(entries).pipe(
        Effect.map((all) => {
          const entry = all.get(id);
          return entry === undefined ? undefined : snapshot(entry);
        }),
      );

    const unavailable = (id: string, entry: Entry<Owner>): SequenceError | undefined =>
      entry.status === "sealed"
        ? new SequenceError({ sequenceId: id, code: "sealed" })
        : entry.status === "indeterminate"
          ? new SequenceError({ sequenceId: id, code: "indeterminate" })
          : entry.status === "retired"
            ? new SequenceError({ sequenceId: id, code: "retired" })
            : entry.sealRequested
              ? new SequenceError({ sequenceId: id, code: "sealing" })
              : undefined;

    const set = (all: ReadonlyMap<string, Entry<Owner>>, entry: Entry<Owner>) =>
      Ref.set(entries, new Map(all).set(entry.id, entry));

    const bind = (id: string, owner: Owner) =>
      lock.withPermit(
        Effect.gen(function* () {
          const all = yield* Ref.get(entries);
          const existing = all.get(id);
          if (existing !== undefined) {
            if (!sameOwner(existing.owner, owner))
              return yield* new SequenceError({ sequenceId: id, code: "owner-mismatch" });
            const failure = unavailable(id, existing);
            if (failure !== undefined) return yield* failure;
            return existing.owner;
          }
          if (all.size >= maxEntries)
            return yield* new SequenceError({ sequenceId: id, code: "capacity" });
          const entry: Entry<Owner> = {
            id,
            owner,
            status: "open",
            acceptedCount: 0,
            rejectedCount: 0,
            indeterminateCount: 0,
            pending: new Set(),
            sealRequested: false,
            members: [],
          };
          yield* set(all, entry);
          return owner;
        }),
      );

    const begin = (id: string, memberId: string) =>
      lock.withPermit(
        Effect.gen(function* () {
          const all = yield* Ref.get(entries);
          const entry = all.get(id);
          if (entry === undefined)
            return yield* new SequenceError({ sequenceId: id, code: "missing" });
          const failure = unavailable(id, entry);
          if (failure !== undefined) return yield* failure;
          if (
            entry.pending.has(memberId) ||
            entry.members.some((member) => member.memberId === memberId)
          ) {
            return yield* new SequenceError({ sequenceId: id, code: "member-duplicate" });
          }
          if (entry.pending.size + entry.members.length >= maxMembers) {
            return yield* new SequenceError({ sequenceId: id, code: "member-capacity" });
          }
          yield* set(all, { ...entry, pending: new Set(entry.pending).add(memberId) });
        }),
      );

    const settle = (
      id: string,
      memberId: string,
      outcome: (entry: Entry<Owner>) => MemberOutcome,
      update: (
        entry: Entry<Owner>,
      ) => Pick<Entry<Owner>, "acceptedCount" | "rejectedCount" | "indeterminateCount">,
      terminal: "known" | "unknown",
      requestSeal = false,
    ) =>
      lock.withPermit(
        Effect.gen(function* () {
          const all = yield* Ref.get(entries);
          const entry = all.get(id);
          if (entry === undefined)
            return yield* new SequenceError({ sequenceId: id, code: "missing" });
          if (!entry.pending.has(memberId)) {
            if (entry.members.some((member) => member.memberId === memberId)) {
              return yield* new SequenceError({ sequenceId: id, code: "member-duplicate" });
            }
            return yield* new SequenceError({ sequenceId: id, code: "member-missing" });
          }
          const pending = new Set(entry.pending);
          pending.delete(memberId);
          const counts = update(entry);
          const sealRequested = entry.sealRequested || requestSeal;
          const status: SequenceStatus =
            entry.status === "indeterminate" || entry.status === "retired"
              ? entry.status
              : terminal === "unknown"
                ? "indeterminate"
                : sealRequested && pending.size === 0
                  ? "sealed"
                  : entry.status;
          yield* set(all, {
            ...entry,
            ...counts,
            status,
            sealRequested,
            pending,
            members: [...entry.members, outcome(entry)],
          });
        }),
      );

    const accepted = (id: string, memberId: string, clipId: string, final = false) =>
      settle(
        id,
        memberId,
        (entry) => ({ _tag: "Accepted", memberId, clipId, index: entry.acceptedCount, final }),
        (entry) => ({
          acceptedCount: entry.acceptedCount + 1,
          rejectedCount: entry.rejectedCount,
          indeterminateCount: entry.indeterminateCount,
        }),
        "known",
        final,
      );

    const rejected = (id: string, memberId: string, reason?: string, final = false) =>
      settle(
        id,
        memberId,
        () => ({ _tag: "Rejected", memberId, reason }),
        (entry) => ({
          acceptedCount: entry.acceptedCount,
          rejectedCount: entry.rejectedCount + 1,
          indeterminateCount: entry.indeterminateCount,
        }),
        "known",
        final,
      );

    const uncertain = (id: string, memberId: string, reason?: string) =>
      settle(
        id,
        memberId,
        () => ({ _tag: "Indeterminate", memberId, reason }),
        (entry) => ({
          acceptedCount: entry.acceptedCount,
          rejectedCount: entry.rejectedCount,
          indeterminateCount: entry.indeterminateCount + 1,
        }),
        "unknown",
      );

    const seal = (id: string) =>
      lock.withPermit(
        Effect.gen(function* () {
          const all = yield* Ref.get(entries);
          const entry = all.get(id);
          if (entry === undefined)
            return yield* new SequenceError({ sequenceId: id, code: "missing" });
          if (entry.status === "indeterminate")
            return yield* new SequenceError({ sequenceId: id, code: "indeterminate" });
          if (entry.status === "retired")
            return yield* new SequenceError({ sequenceId: id, code: "retired" });
          if (entry.status === "sealed") return;
          yield* set(all, {
            ...entry,
            sealRequested: true,
            status: entry.pending.size === 0 ? "sealed" : "open",
          });
        }),
      );

    const retire = (owner: Owner) =>
      lock.withPermit(
        Ref.update(entries, (all) => {
          const next = new Map(all);
          for (const [id, entry] of all) {
            if (!sameOwner(entry.owner, owner)) continue;
            if (entry.pending.size === 0) {
              if (entry.status !== "indeterminate") next.set(id, { ...entry, status: "retired" });
              continue;
            }
            const pendingOutcomes = [...entry.pending].map((memberId): MemberOutcome =>
              Object.freeze({
                _tag: "Indeterminate",
                memberId,
                reason: "owning session retired before outcome was known",
              }),
            );
            next.set(id, {
              ...entry,
              status: "indeterminate",
              indeterminateCount: entry.indeterminateCount + entry.pending.size,
              pending: new Set(),
              members: [...entry.members, ...pendingOutcomes],
            });
          }
          return next;
        }),
      );

    const acknowledgeIndeterminate = (id: string) =>
      lock.withPermit(
        Effect.gen(function* () {
          const all = yield* Ref.get(entries);
          const entry = all.get(id);
          if (entry === undefined)
            return yield* new SequenceError({ sequenceId: id, code: "missing" });
          if (entry.status !== "indeterminate" || entry.pending.size !== 0) {
            return yield* new SequenceError({ sequenceId: id, code: "not-releasable" });
          }
          yield* set(all, { ...entry, status: "retired" });
        }),
      );

    const release = (id: string) =>
      lock.withPermit(
        Effect.gen(function* () {
          const all = yield* Ref.get(entries);
          const entry = all.get(id);
          if (entry === undefined)
            return yield* new SequenceError({ sequenceId: id, code: "missing" });
          if (
            entry.pending.size !== 0 ||
            (entry.status !== "sealed" && entry.status !== "retired")
          ) {
            return yield* new SequenceError({ sequenceId: id, code: "not-releasable" });
          }
          const next = new Map(all);
          next.delete(id);
          yield* Ref.set(entries, next);
        }),
      );

    return {
      get,
      snapshots: Ref.get(entries).pipe(
        Effect.map((all) => Object.freeze([...all.values()].map(snapshot))),
      ),
      bind,
      begin,
      accepted,
      rejected,
      uncertain,
      seal,
      retire,
      acknowledgeIndeterminate,
      release,
      size: Ref.get(entries).pipe(Effect.map((all) => all.size)),
    };
  });
