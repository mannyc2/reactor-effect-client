/** Sequence affinity under random operation sequences: every step keeps its invariants. */
import { expect, it } from "@effect/vitest";
import { Effect, Result, Schema } from "effect";
import { makeAffinity } from "../src/Sequence.js";
import type { Affinity, SequenceSnapshot } from "../src/Sequence.js";

const maxMembers = 2;
/** Operation kinds, weighted toward member settlement so interleavings are common. */
const kinds = [
  ...["begin", "begin", "begin", "begin"],
  ...["accepted", "accepted", "accepted", "rejected", "rejected", "uncertain"],
  ...["seal", "retire", "acknowledge", "release", "bind", "bind"],
] as const;
const Operation = Schema.Struct({
  kind: Schema.Int.check(Schema.isBetween({ minimum: 0, maximum: kinds.length - 1 })),
  sequence: Schema.Literals(["s1", "s2"]),
  member: Schema.Literals(["m1", "m2", "m3"]),
  owner: Schema.Literals(["o1", "o2"]),
  final: Schema.Boolean,
  /** Begin the member first, so a settlement usually finds it pending. */
  prepare: Schema.Boolean,
});
type Operation = typeof Operation.Type;

const apply = (affinity: Affinity<string>, op: Operation) => {
  switch (kinds[op.kind]!) {
    case "bind":
      return Effect.asVoid(affinity.bind(op.sequence, op.owner));
    case "begin":
      return affinity.begin(op.sequence, op.member);
    case "accepted":
      return affinity.accepted(op.sequence, op.member, `clip-${op.member}`, op.final);
    case "rejected":
      return affinity.rejected(op.sequence, op.member, undefined, op.final);
    case "uncertain":
      return affinity.uncertain(op.sequence, op.member);
    case "seal":
      return affinity.seal(op.sequence);
    case "retire":
      return affinity.retire(op.owner);
    case "acknowledge":
      return affinity.acknowledgeIndeterminate(op.sequence);
    case "release":
      return affinity.release(op.sequence);
  }
};

const invariants = (snapshot: SequenceSnapshot<string>): void => {
  const tagged = (tag: string) => snapshot.members.filter((member) => member._tag === tag);
  expect(snapshot.acceptedCount).toBe(tagged("Accepted").length);
  expect(snapshot.rejectedCount).toBe(tagged("Rejected").length);
  expect(snapshot.indeterminateCount).toBe(tagged("Indeterminate").length);
  // Accepted members are numbered in acceptance order, from zero.
  expect(
    snapshot.members.flatMap((member) => (member._tag === "Accepted" ? [member.index] : [])),
  ).toEqual(tagged("Accepted").map((_, index) => index));
  expect(new Set(snapshot.members.map((member) => member.memberId)).size).toBe(
    snapshot.members.length,
  );
  expect(snapshot.pendingCount + snapshot.members.length).toBeLessThanOrEqual(maxMembers);
  if (snapshot.status === "sealed") expect(snapshot.pendingCount).toBe(0);
  if (snapshot.status === "indeterminate") expect(snapshot.indeterminateCount).toBeGreaterThan(0);
};

it.effect.prop(
  "every operation applies whole or is refused with the state unchanged",
  { operations: Schema.Array(Operation).check(Schema.isMaxLength(40)) },
  ({ operations }) =>
    Effect.gen(function* () {
      const affinity = yield* makeAffinity<string>({ maxEntries: 2, maxMembers });
      yield* affinity.bind("s1", "o1");
      for (const op of operations) {
        const kind = kinds[op.kind];
        if (op.prepare && (kind === "accepted" || kind === "rejected" || kind === "uncertain"))
          yield* Effect.ignore(affinity.begin(op.sequence, op.member));
        const before = yield* affinity.snapshots;
        const result = yield* Effect.result(apply(affinity, op));
        const after = yield* affinity.snapshots;
        if (Result.isFailure(result)) expect(after).toEqual(before);
        if (kind === "retire")
          for (const snapshot of after.filter((entry) => entry.owner === op.owner)) {
            expect(snapshot.pendingCount).toBe(0);
            expect(["retired", "indeterminate"]).toContain(snapshot.status);
          }
        after.forEach(invariants);
      }
    }),
  { arbitrary: { runs: 300, size: 60 } },
);
