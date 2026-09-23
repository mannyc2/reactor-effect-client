import { Effect, Ref, Schema } from "effect";

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
  readonly pending: ReadonlySet<string>;
  readonly sealRequested: boolean;
  readonly members: ReadonlyArray<MemberOutcome>;
}

type Entries<Owner> = ReadonlyMap<string, Entry<Owner>>;

/** A transition's result and the next state, or why it was refused (state unchanged). */
type Step<Owner, A> = readonly [A, Entries<Owner>] | SequenceError;

const count = (members: ReadonlyArray<MemberOutcome>, tag: MemberOutcome["_tag"]): number =>
  members.reduce((total, member) => (member._tag === tag ? total + 1 : total), 0);

const immutableMembers = (members: ReadonlyArray<MemberOutcome>): ReadonlyArray<MemberOutcome> =>
  Object.freeze(members.map((member) => Object.freeze({ ...member })));

const snapshot = <Owner>(entry: Entry<Owner>): SequenceSnapshot<Owner> =>
  Object.freeze({
    id: entry.id,
    owner: entry.owner,
    status: entry.status,
    acceptedCount: count(entry.members, "Accepted"),
    rejectedCount: count(entry.members, "Rejected"),
    indeterminateCount: count(entry.members, "Indeterminate"),
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

const refuse = (sequenceId: string, code: SequenceCode) => new SequenceError({ sequenceId, code });

const unavailable = <Owner>(id: string, entry: Entry<Owner>): SequenceError | undefined =>
  entry.status === "sealed"
    ? refuse(id, "sealed")
    : entry.status === "indeterminate"
      ? refuse(id, "indeterminate")
      : entry.status === "retired"
        ? refuse(id, "retired")
        : entry.sealRequested
          ? refuse(id, "sealing")
          : undefined;

const put = <Owner>(all: Entries<Owner>, entry: Entry<Owner>): Entries<Owner> =>
  new Map(all).set(entry.id, entry);

/**
 * Bounded sequence-affinity state. Unresolved work is never evicted to admit
 * another sequence or member; callers explicitly account and release history.
 * Every operation is one pure transition of the whole state, applied with
 * `Ref.modify`, so none observes another half done.
 */
export const makeAffinity = <Owner>(
  options: Options<Owner> = {},
): Effect.Effect<Affinity<Owner>, InvalidSequenceOptions> =>
  Effect.gen(function* () {
    const maxEntries = yield* positiveInteger(options.maxEntries ?? 256, "maxEntries");
    const maxMembers = yield* positiveInteger(options.maxMembers ?? 256, "maxMembers");
    const sameOwner = options.sameOwner ?? Object.is;
    const entries = yield* Ref.make<Entries<Owner>>(new Map());

    const apply = <A>(transition: (all: Entries<Owner>) => Step<Owner, A>) =>
      Ref.modify(entries, (all): readonly [Effect.Effect<A, SequenceError>, Entries<Owner>] => {
        const step = transition(all);
        return step instanceof SequenceError
          ? [Effect.fail(step), all]
          : [Effect.succeed(step[0]), step[1]];
      }).pipe(Effect.flatten);

    /** A transition of one existing sequence. */
    const onEntry =
      <A>(id: string, transition: (entry: Entry<Owner>, all: Entries<Owner>) => Step<Owner, A>) =>
      (all: Entries<Owner>): Step<Owner, A> => {
        const entry = all.get(id);
        return entry === undefined ? refuse(id, "missing") : transition(entry, all);
      };

    const get = (id: string) =>
      Ref.get(entries).pipe(
        Effect.map((all) => {
          const entry = all.get(id);
          return entry === undefined ? undefined : snapshot(entry);
        }),
      );

    const bind = (id: string, owner: Owner) =>
      apply((all): Step<Owner, Owner> => {
        const existing = all.get(id);
        if (existing !== undefined) {
          if (!sameOwner(existing.owner, owner)) return refuse(id, "owner-mismatch");
          return unavailable(id, existing) ?? [existing.owner, all];
        }
        if (all.size >= maxEntries) return refuse(id, "capacity");
        return [
          owner,
          put(all, {
            id,
            owner,
            status: "open",
            pending: new Set(),
            sealRequested: false,
            members: [],
          }),
        ];
      });

    const begin = (id: string, memberId: string) =>
      apply(
        onEntry(id, (entry, all): Step<Owner, void> => {
          const failure = unavailable(id, entry);
          if (failure !== undefined) return failure;
          if (
            entry.pending.has(memberId) ||
            entry.members.some((member) => member.memberId === memberId)
          )
            return refuse(id, "member-duplicate");
          if (entry.pending.size + entry.members.length >= maxMembers)
            return refuse(id, "member-capacity");
          return [undefined, put(all, { ...entry, pending: new Set(entry.pending).add(memberId) })];
        }),
      );

    const settle = (
      id: string,
      memberId: string,
      outcome: (entry: Entry<Owner>) => MemberOutcome,
      terminal: "known" | "unknown",
      requestSeal = false,
    ) =>
      apply(
        onEntry(id, (entry, all): Step<Owner, void> => {
          if (!entry.pending.has(memberId))
            return entry.members.some((member) => member.memberId === memberId)
              ? refuse(id, "member-duplicate")
              : refuse(id, "member-missing");
          const pending = new Set(entry.pending);
          pending.delete(memberId);
          const sealRequested = entry.sealRequested || requestSeal;
          const status: SequenceStatus =
            entry.status === "indeterminate" || entry.status === "retired"
              ? entry.status
              : terminal === "unknown"
                ? "indeterminate"
                : sealRequested && pending.size === 0
                  ? "sealed"
                  : entry.status;
          return [
            undefined,
            put(all, {
              ...entry,
              status,
              sealRequested,
              pending,
              members: [...entry.members, outcome(entry)],
            }),
          ];
        }),
      );

    const accepted = (id: string, memberId: string, clipId: string, final = false) =>
      settle(
        id,
        memberId,
        (entry) => ({
          _tag: "Accepted",
          memberId,
          clipId,
          index: count(entry.members, "Accepted"),
          final,
        }),
        "known",
        final,
      );

    const rejected = (id: string, memberId: string, reason?: string, final = false) =>
      settle(id, memberId, () => ({ _tag: "Rejected", memberId, reason }), "known", final);

    const uncertain = (id: string, memberId: string, reason?: string) =>
      settle(id, memberId, () => ({ _tag: "Indeterminate", memberId, reason }), "unknown");

    const seal = (id: string) =>
      apply(
        onEntry(id, (entry, all): Step<Owner, void> => {
          if (entry.status === "indeterminate") return refuse(id, "indeterminate");
          if (entry.status === "retired") return refuse(id, "retired");
          if (entry.status === "sealed") return [undefined, all];
          return [
            undefined,
            put(all, {
              ...entry,
              sealRequested: true,
              status: entry.pending.size === 0 ? "sealed" : "open",
            }),
          ];
        }),
      );

    const retire = (owner: Owner) =>
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
            pending: new Set(),
            members: [...entry.members, ...pendingOutcomes],
          });
        }
        return next;
      });

    const acknowledgeIndeterminate = (id: string) =>
      apply(
        onEntry(id, (entry, all): Step<Owner, void> =>
          entry.status !== "indeterminate" || entry.pending.size !== 0
            ? refuse(id, "not-releasable")
            : [undefined, put(all, { ...entry, status: "retired" })],
        ),
      );

    const release = (id: string) =>
      apply(
        onEntry(id, (entry, all): Step<Owner, void> => {
          if (entry.pending.size !== 0 || (entry.status !== "sealed" && entry.status !== "retired"))
            return refuse(id, "not-releasable");
          const next = new Map(all);
          next.delete(id);
          return [undefined, next];
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
