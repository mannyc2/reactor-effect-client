import { expect, test } from "vitest";
import { Effect, Result } from "effect";
import * as Sequence from "../src/Sequence.js";

test("sequence affinity exposes accepted identities, partial outcomes and explicit sealing", async () => {
  await Effect.runPromise(
    Effect.gen(function* () {
      const affinity = yield* Sequence.makeAffinity<string>({ maxEntries: 2, maxMembers: 4 });
      expect(yield* affinity.bind("run", "session-a")).toBe("session-a");
      yield* affinity.begin("run", "a");
      yield* affinity.accepted("run", "a", "clip-a");
      yield* affinity.begin("run", "b");
      yield* affinity.rejected("run", "b", "queue full");
      yield* affinity.begin("run", "c");
      yield* affinity.accepted("run", "c", "clip-c", true);
      expect(yield* affinity.get("run")).toEqual({
        id: "run",
        owner: "session-a",
        status: "sealed",
        acceptedCount: 2,
        rejectedCount: 1,
        indeterminateCount: 0,
        pendingCount: 0,
        sealRequested: true,
        members: [
          { _tag: "Accepted", memberId: "a", clipId: "clip-a", index: 0, final: false },
          { _tag: "Rejected", memberId: "b", reason: "queue full" },
          { _tag: "Accepted", memberId: "c", clipId: "clip-c", index: 1, final: true },
        ],
      });
      const late = yield* Effect.result(affinity.bind("run", "session-a"));
      expect(Result.isFailure(late) && late.failure.reason).toBe("sealed");
      yield* affinity.release("run");
      expect(yield* affinity.get("run")).toBeUndefined();
    }),
  );
});

test("explicit seal closes zero-member sequences and waits for already-started members", async () => {
  await Effect.runPromise(
    Effect.gen(function* () {
      const affinity = yield* Sequence.makeAffinity<string>();
      yield* affinity.bind("empty", "session");
      yield* affinity.seal("empty");
      expect((yield* affinity.get("empty"))?.status).toBe("sealed");

      yield* affinity.bind("pending", "session");
      yield* affinity.begin("pending", "one");
      yield* affinity.seal("pending");
      const during = yield* affinity.get("pending");
      expect(during?.status).toBe("open");
      expect(during?.sealRequested).toBe(true);
      expect((yield* Effect.result(affinity.begin("pending", "two")))._tag).toBe("Failure");
      yield* affinity.accepted("pending", "one", "clip-one");
      expect((yield* affinity.get("pending"))?.status).toBe("sealed");
    }),
  );
});

test("indeterminate sequences never migrate or evict and require explicit accounting before release", async () => {
  await Effect.runPromise(
    Effect.gen(function* () {
      const affinity = yield* Sequence.makeAffinity<object>({ maxEntries: 1, maxMembers: 2 });
      const owner = {};
      yield* affinity.bind("unknown", owner);
      yield* affinity.begin("unknown", "first");
      yield* affinity.accepted("unknown", "first", "clip-first");
      yield* affinity.begin("unknown", "second");
      yield* affinity.uncertain("unknown", "second", "reply and broadcast lost");
      yield* affinity.retire(owner);
      expect((yield* affinity.get("unknown"))?.status).toBe("indeterminate");
      expect((yield* Effect.result(affinity.bind("unknown", {})))._tag).toBe("Failure");
      const full = yield* Effect.result(affinity.bind("other", {}));
      expect(Result.isFailure(full) && full.failure.reason).toBe("capacity");
      const release = yield* Effect.result(affinity.release("unknown"));
      expect(Result.isFailure(release) && release.failure.reason).toBe("not-releasable");
      yield* affinity.acknowledgeIndeterminate("unknown");
      expect((yield* affinity.get("unknown"))?.status).toBe("retired");
      yield* affinity.release("unknown");
      expect(yield* affinity.size).toBe(0);
    }),
  );
});

test("member and sequence bounds are explicit and invalid configuration is typed", async () => {
  const badEntries = await Effect.runPromise(
    Effect.result(Sequence.makeAffinity({ maxEntries: Number.NaN })),
  );
  expect(Result.isFailure(badEntries) && badEntries.failure._tag).toBe("InvalidSequenceOptions");
  const badMembers = await Effect.runPromise(
    Effect.result(Sequence.makeAffinity({ maxMembers: 0 })),
  );
  expect(Result.isFailure(badMembers) && badMembers.failure._tag).toBe("InvalidSequenceOptions");

  await Effect.runPromise(
    Effect.gen(function* () {
      const affinity = yield* Sequence.makeAffinity<string>({ maxEntries: 1, maxMembers: 1 });
      yield* affinity.bind("bounded", "session");
      yield* affinity.begin("bounded", "one");
      const second = yield* Effect.result(affinity.begin("bounded", "two"));
      expect(Result.isFailure(second) && second.failure.reason).toBe("member-capacity");
      yield* affinity.rejected("bounded", "one", "no");
      const afterResolved = yield* Effect.result(affinity.begin("bounded", "two"));
      expect(Result.isFailure(afterResolved) && afterResolved.failure.reason).toBe(
        "member-capacity",
      );
    }),
  );
});

test("retiring a session turns pending members into retained unknown evidence", async () => {
  await Effect.runPromise(
    Effect.gen(function* () {
      const affinity = yield* Sequence.makeAffinity<string>();
      yield* affinity.bind("partial", "session-a");
      yield* affinity.begin("partial", "first");
      yield* affinity.accepted("partial", "first", "clip-first");
      yield* affinity.begin("partial", "second");
      yield* affinity.retire("session-a");
      const snapshot = yield* affinity.get("partial");
      expect(snapshot?.status).toBe("indeterminate");
      expect(snapshot?.acceptedCount).toBe(1);
      expect(snapshot?.indeterminateCount).toBe(1);
      expect(snapshot?.members.at(-1)).toEqual({
        _tag: "Indeterminate",
        memberId: "second",
        reason: "owning session retired before outcome was known",
      });
    }),
  );
});

test("indeterminate status is absorbing when a later final member is accepted", async () => {
  await Effect.runPromise(
    Effect.gen(function* () {
      const affinity = yield* Sequence.makeAffinity<string>();
      yield* affinity.bind("mixed", "session-a");
      yield* affinity.begin("mixed", "first");
      yield* affinity.begin("mixed", "final");
      yield* affinity.uncertain("mixed", "first", "reply lost");
      yield* affinity.accepted("mixed", "final", "clip-final", true);
      const snapshot = yield* affinity.get("mixed");
      expect(snapshot?.status).toBe("indeterminate");
      expect(snapshot?.sealRequested).toBe(true);
      expect(snapshot?.acceptedCount).toBe(1);
      expect(snapshot?.indeterminateCount).toBe(1);
      expect(snapshot?.pendingCount).toBe(0);
      const release = yield* Effect.result(affinity.release("mixed"));
      expect(Result.isFailure(release) && release.failure.reason).toBe("not-releasable");
      yield* affinity.acknowledgeIndeterminate("mixed");
      yield* affinity.release("mixed");
      expect(yield* affinity.get("mixed")).toBeUndefined();
    }),
  );
});

test("retiring an already-indeterminate sequence resolves its remaining pending members as unknown", async () => {
  await Effect.runPromise(
    Effect.gen(function* () {
      const affinity = yield* Sequence.makeAffinity<string>();
      yield* affinity.bind("retire-mixed", "session-a");
      yield* affinity.begin("retire-mixed", "unknown");
      yield* affinity.begin("retire-mixed", "pending");
      yield* affinity.uncertain("retire-mixed", "unknown", "ambiguous dispatch");
      yield* affinity.retire("session-a");
      const snapshot = yield* affinity.get("retire-mixed");
      expect(snapshot?.status).toBe("indeterminate");
      expect(snapshot?.pendingCount).toBe(0);
      expect(snapshot?.indeterminateCount).toBe(2);
      expect(snapshot?.members).toContainEqual({
        _tag: "Indeterminate",
        memberId: "pending",
        reason: "owning session retired before outcome was known",
      });
    }),
  );
});

test("a rejected final member seals only after earlier pending members settle", async () => {
  await Effect.runPromise(
    Effect.gen(function* () {
      const affinity = yield* Sequence.makeAffinity<string>();
      yield* affinity.bind("final-rejected", "session-a");
      yield* affinity.begin("final-rejected", "earlier");
      yield* affinity.begin("final-rejected", "final");
      yield* affinity.rejected("final-rejected", "final", "queue full", true);

      const pending = yield* affinity.get("final-rejected");
      expect(pending?.status).toBe("open");
      expect(pending?.sealRequested).toBe(true);
      expect(pending?.pendingCount).toBe(1);
      const late = yield* Effect.result(affinity.begin("final-rejected", "late"));
      expect(Result.isFailure(late) && late.failure.reason).toBe("sealing");

      yield* affinity.accepted("final-rejected", "earlier", "clip-earlier");
      const sealed = yield* affinity.get("final-rejected");
      expect(sealed?.status).toBe("sealed");
      expect(sealed?.pendingCount).toBe(0);
      expect(sealed?.rejectedCount).toBe(1);
    }),
  );
});
