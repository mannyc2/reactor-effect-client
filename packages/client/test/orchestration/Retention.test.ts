import { expect, test } from "vitest";
import * as Effect from "effect/Effect";
import * as Result from "effect/Result";
import { ReactorError, PolicyFailure } from "../../src/errors.js";
import type { CloseReport } from "../../src/SessionTypes.js";
import * as Retention from "../../src/orchestration/retention.js";
import type { SourceCleanup } from "../../src/orchestration/types.js";
import { run } from "../harness.js";

const complete: Retention.Retirement = {
  accounting: "settled",
  scope: "closed",
  affinity: "retired",
  errors: [],
  unknownSubmissions: 0n,
};
const unopened: Retention.Retirement = {
  ...complete,
  accounting: "not-applicable",
  affinity: "not-applicable",
};
const noRemote: CloseReport["remote"] = {
  attempted: false,
  responseReceived: false,
  confirmed: false,
  evidence: null,
  deleteStatus: null,
  state: null,
};
const cleanup = (fields: Partial<CloseReport> = {}): SourceCleanup => ({
  lease: {
    localClosed: true,
    allocation: "none",
    remote: noRemote,
    unpublishSubmitted: [],
    unresolvedPublications: [],
    localErrors: [],
    ...fields,
  },
  policy: [],
});
const owned = cleanup({
  allocation: "known",
  ownership: "owned",
  sessionId: "owned",
  remote: { ...noRemote, attempted: true, confirmed: true, evidence: "absent" },
});
const attached = cleanup({ allocation: "known", ownership: "attached", sessionId: "attached" });

test("reserve before opening, including a closing source whose report already arrived", async ({
  signal,
}) => {
  await run(
    Effect.gen(function* () {
      const retention = yield* Retention.make();
      const first = yield* retention.reserve;
      const second = yield* retention.reserve;
      yield* retention.record(first, owned);
      const refused = yield* Effect.result(retention.reserve);
      expect(refused).toMatchObject({ _tag: "Failure", failure: { reason: { _tag: "Overflow" } } });
      expect((yield* retention.summary).totalRetirements).toBe(0n);
      yield* retention.finish(first, complete);
      const third = yield* retention.reserve;
      expect(third.incarnation).toBeGreaterThan(second.incarnation);
      expect((yield* retention.summary).exhausted).toBe(false);
    }),
    { signal },
  );
});

test("failure budget reserves room for both current and replacement cleanup", async ({
  signal,
}) => {
  await run(
    Effect.gen(function* () {
      const retention = yield* Retention.make({ maxUnresolvedCleanups: 3 });
      const failedOpen = yield* retention.reserve;
      yield* retention.record(failedOpen, cleanup({ allocation: "unknown" }));
      yield* retention.finish(failedOpen, unopened);
      const current = yield* retention.reserve;
      const replacement = yield* retention.reserve;
      yield* retention.record(replacement, cleanup({ allocation: "unknown" }));
      yield* retention.finish(replacement, unopened);
      expect(yield* Effect.result(retention.reserve)).toMatchObject({ _tag: "Failure" });
      yield* retention.record(current, cleanup({ localClosed: false }));
      yield* retention.finish(current, complete);
      const summary = yield* retention.summary;
      expect(summary.exhausted).toBe(true);
      expect(summary.totalRetirements).toBe(3n);
      expect(summary.retained).toHaveLength(3);
      expect(summary.retained.every((row) => row.disposition === "incomplete")).toBe(true);
    }),
    { signal },
  );
});

test("newest complete rows survive eviction with exact separate omitted counters", async ({
  signal,
}) => {
  await run(
    Effect.gen(function* () {
      const retention = yield* Retention.make({ retainedSuccessfulCleanups: 1 });
      const reports = [cleanup({ sessionId: "simulation" }), owned, attached, owned];
      for (const report of reports) {
        const owner = yield* retention.reserve;
        yield* retention.record(owner, report);
        yield* retention.finish(owner, complete);
      }
      const summary = yield* retention.summary;
      expect(summary.totalRetirements).toBe(4n);
      expect(summary.omittedComplete).toEqual({
        noAllocation: 1n,
        ownedTerminated: 1n,
        attachedDetached: 1n,
      });
      expect(summary.retained).toHaveLength(1);
      expect(summary.retained[0]?.ordinal).toBe(4n);
      expect(summary.retained[0]?.cleanup).toBe(owned);
      expect(Object.isFrozen(summary)).toBe(true);
      expect(Object.isFrozen(summary.retained)).toBe(true);
    }),
    { signal },
  );
});

test("failed acquisitions sharing one no-allocation lease each count once", async ({ signal }) => {
  await run(
    Effect.gen(function* () {
      const retention = yield* Retention.make({ retainedSuccessfulCleanups: 0 });
      const report = cleanup();
      for (let attempt = 0; attempt < 80; attempt++) {
        const owner = yield* retention.reserve;
        yield* retention.record(owner, report);
        yield* retention.record(owner, { ...report });
        const row = yield* retention.finish(owner, unopened);
        expect(yield* retention.finish(owner, unopened)).toBe(row);
      }
      const summary = yield* retention.summary;
      expect(summary.totalRetirements).toBe(80n);
      expect(summary.omittedComplete.noAllocation).toBe(80n);
      expect(summary.retained).toEqual([]);
    }),
    { signal },
  );
});

test("unknown, erroneous and contradictory cleanup is never compacted", async ({ signal }) => {
  const error = ReactorError.fromCode("InvalidState", "cleanup fixture");
  const reports: SourceCleanup[] = [
    cleanup({ allocation: "unknown" }),
    cleanup({ localClosed: false }),
    cleanup({ localErrors: [error] }),
    cleanup({ unresolvedPublications: ["pending"] }),
    cleanup({ remote: { ...noRemote, error } }),
    cleanup({ remote: { ...noRemote, attempted: true } }),
    cleanup({ allocation: "known", sessionId: "missing-ownership" }),
    cleanup({ allocation: "known", ownership: "owned", sessionId: "unconfirmed" }),
    cleanup({ ...owned.lease, sessionId: "" }),
    cleanup({ ...attached.lease, remote: { ...noRemote, attempted: true } }),
    {
      ...owned,
      policy: [
        {
          operation: "stop",
          result: Result.fail(PolicyFailure.refuse("SessionRetired", "fixture")),
        },
      ],
    },
  ];
  await run(
    Effect.gen(function* () {
      const retention = yield* Retention.make({ retainedSuccessfulCleanups: 0 });
      for (const report of reports) {
        const owner = yield* retention.reserve;
        yield* retention.record(owner, report);
        yield* retention.finish(owner, complete);
      }
      const summary = yield* retention.summary;
      expect(summary.retained.map((row) => row.cleanup)).toEqual(reports);
      expect(summary.retained.every((row) => row.disposition === "incomplete")).toBe(true);
      expect(summary.omittedComplete).toEqual({
        noAllocation: 0n,
        ownedTerminated: 0n,
        attachedDetached: 0n,
      });
    }),
    { signal },
  );
});

test("local retirement failures retain clean canonical evidence by reference", async ({
  signal,
}) => {
  await run(
    Effect.gen(function* () {
      const retention = yield* Retention.make({ retainedSuccessfulCleanups: 0 });
      for (const retirement of [
        { ...complete, accounting: "timed-out" as const },
        { ...complete, scope: "failed" as const },
        { ...complete, affinity: "failed" as const },
        { ...complete, unknownSubmissions: 1n },
        { ...complete, errors: [ReactorError.fromCode("InvalidState", "retirement failed")] },
      ]) {
        const owner = yield* retention.reserve;
        yield* retention.record(owner, owned);
        const row = yield* retention.finish(owner, retirement);
        expect(row.disposition).toBe("incomplete");
        expect(row.cleanup).toBe(owned);
      }
      expect((yield* retention.summary).retained).toHaveLength(5);
    }),
    { signal },
  );
});

test("a close defect with no report remains explicit; conflicting reports stop further admission", async ({
  signal,
}) => {
  await run(
    Effect.gen(function* () {
      const retention = yield* Retention.make();
      const failedClose = yield* retention.reserve;
      const row = yield* retention.finish(failedClose, { ...complete, scope: "failed" });
      expect(row.cleanup).toBeUndefined();
      expect(row.disposition).toBe("incomplete");
      const conflict = yield* retention.reserve;
      yield* retention.record(conflict, owned);
      yield* retention.record(conflict, attached);
      yield* retention.record(conflict, owned);
      const conflicting = yield* retention.finish(conflict, complete);
      expect(conflicting.cleanup).toBe(owned);
      expect(conflicting.conflictingCleanup).toBe(attached);
      expect(conflicting.disposition).toBe("incomplete");
      expect(yield* Effect.result(retention.reserve)).toMatchObject({ _tag: "Failure" });
      expect((yield* retention.summary).exhausted).toBe(true);
    }),
    { signal },
  );
});

test("retirement order is independent of acquisition order and retained incompletes", async ({
  signal,
}) => {
  await run(
    Effect.gen(function* () {
      const retention = yield* Retention.make({ retainedSuccessfulCleanups: 1 });
      const first = yield* retention.reserve;
      const second = yield* retention.reserve;
      yield* retention.identify(first, "owned");
      yield* retention.identify(second, "second");
      yield* retention.record(second, cleanup({ allocation: "unknown" }));
      yield* retention.finish(second, complete);
      yield* retention.record(first, owned);
      yield* retention.finish(first, complete);
      const third = yield* retention.reserve;
      yield* retention.record(third, attached);
      yield* retention.finish(third, complete);
      const summary = yield* retention.summary;
      expect(summary.retained.map((row) => row.ordinal)).toEqual([1n, 3n]);
      expect(summary.retained[0]?.source).toEqual({
        sessionId: "second",
        incarnation: second.incarnation,
      });
      expect(
        BigInt(summary.retained.length) +
          Object.values(summary.omittedComplete).reduce((a, b) => a + b),
      ).toBe(summary.totalRetirements);
    }),
    { signal },
  );
});

test("retention limits reject invalid inputs before reservations exist", async ({ signal }) => {
  await run(
    Effect.gen(function* () {
      for (const options of [
        { retainedSuccessfulCleanups: -1 },
        { retainedSuccessfulCleanups: 4097 },
        { retainedSuccessfulCleanups: 0.5 },
        { maxUnresolvedCleanups: 1 },
        { maxUnresolvedCleanups: Infinity },
        { maxUnresolvedCleanups: 4097 },
      ])
        expect(yield* Effect.result(Retention.make(options))).toMatchObject({
          _tag: "Failure",
          failure: { reason: { _tag: "InvalidInput" } },
        });
    }),
    { signal },
  );
});

test("identified acquisition failures have no invented accounting or affinity obligation", async ({
  signal,
}) => {
  await run(
    Effect.gen(function* () {
      const retention = yield* Retention.make({ retainedSuccessfulCleanups: 0 });
      const owner = yield* retention.reserve;
      yield* retention.identify(owner, "owned");
      yield* retention.record(owner, owned);
      expect((yield* retention.finish(owner, unopened)).disposition).toBe("complete");
      expect((yield* retention.summary).omittedComplete.ownedTerminated).toBe(1n);
    }),
    { signal },
  );
});

test("physical identity is bounded and cannot change within an ownership attempt", async ({
  signal,
}) => {
  await run(
    Effect.gen(function* () {
      const retention = yield* Retention.make();
      const owner = yield* retention.reserve;
      for (const id of ["", "s".repeat(1025), 42 as unknown as string]) {
        expect(yield* Effect.result(retention.identify(owner, id))).toMatchObject({
          _tag: "Failure",
          failure: { reason: { _tag: "InvalidInput" } },
        });
      }
      yield* retention.identify(owner, "s".repeat(1024));
      const changed = yield* Effect.exit(retention.identify(owner, "other"));
      expect(changed._tag).toBe("Failure");
    }),
    { signal },
  );
});

test("contradictory DELETE facts and a different physical lease identity remain incomplete", async ({
  signal,
}) => {
  await run(
    Effect.gen(function* () {
      const retention = yield* Retention.make({ retainedSuccessfulCleanups: 0 });
      for (const remote of [
        { ...owned.lease.remote, deleteStatus: 204 },
        { ...owned.lease.remote, responseReceived: true },
        { ...owned.lease.remote, attempted: false, responseReceived: true, deleteStatus: 204 },
      ]) {
        const owner = yield* retention.reserve;
        yield* retention.record(owner, cleanup({ ...owned.lease, remote }));
        expect((yield* retention.finish(owner, complete)).disposition).toBe("incomplete");
      }
      const mismatched = yield* retention.reserve;
      yield* retention.identify(mismatched, "different-physical-session");
      yield* retention.record(mismatched, owned);
      expect((yield* retention.finish(mismatched, complete)).disposition).toBe("incomplete");
      expect((yield* retention.summary).omittedComplete.ownedTerminated).toBe(0n);
    }),
    { signal },
  );
});
