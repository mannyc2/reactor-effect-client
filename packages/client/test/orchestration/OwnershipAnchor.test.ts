import { expect, test } from "bun:test";
import { Effect, Fiber, Result } from "effect";
import { ClipId, captureRequest } from "../../src/orchestration/request.js";
import { resolve } from "../../src/orchestration/routing.js";
import type { Candidate } from "../../src/orchestration/routing.js";
import { renewalFixture } from "./RenewalFixture.js";
import { gate, member, readyState, record, request, runClock } from "./SourceFixture.js";

const oldClip = record("foreign-old-ready"),
  warmClip = record("foreign-warm-ready");
const old: Candidate<string> = {
  owner: "old",
  state: readyState({ ready: [oldClip] }),
  accepted: new Set(),
  closed: false,
  recovering: false,
};
const warm: Candidate<string> = {
  owner: "warm",
  state: readyState({
    ready: [warmClip],
    continuable: [warmClip.clipId],
    queued: [record("warm-building")],
    generationOrder: [ClipId.make("warm-building")],
  }),
  accepted: new Set(),
  closed: false,
  recovering: false,
};

test("sameSessionAs retains a ready clip's physical owner without inventing a generation position", async () => {
  const input = request({ sameSessionAs: oldClip.clipId });
  const captured = await Effect.runPromise(captureRequest(input));
  expect(captured.sameSessionAs).toBe(oldClip.clipId);
  expect(await Effect.runPromise(resolve(captured, [old, warm], "warm", undefined))).toEqual({
    owner: "old",
    position: undefined,
  });
  expect(
    await Effect.runPromise(
      resolve(
        request({ sameSessionAs: oldClip.clipId, position: 999 }),
        [old, warm],
        "warm",
        undefined,
      ),
    ),
  ).toEqual({ owner: "old", position: 999 });
  const invalid = await Effect.runPromise(
    Effect.result(captureRequest(request({ sameSessionAs: ClipId.make("") }))),
  );
  expect(Result.isFailure(invalid) && invalid.failure.context.outcome).toBe("not-submitted");
});

for (const [name, fields, candidates, reason] of [
  [
    "missing identity",
    { sameSessionAs: ClipId.make("missing") },
    [old, warm],
    "session_anchor_missing",
  ],
  [
    "ambiguous identity",
    { sameSessionAs: oldClip.clipId },
    [old, { ...warm, accepted: new Set([oldClip.clipId]) }],
    "owner_conflict",
  ],
  [
    "retired source",
    { sameSessionAs: oldClip.clipId },
    [{ ...old, closed: true }, warm],
    "session_retired",
  ],
  [
    "recovering source",
    { sameSessionAs: oldClip.clipId },
    [{ ...old, recovering: true }, warm],
    "session_recovering",
  ],
  [
    "conflicting continuation",
    { sameSessionAs: oldClip.clipId, continueFrom: warmClip.clipId },
    [old, warm],
    "owner_conflict",
  ],
  [
    "conflicting insertion",
    { sameSessionAs: oldClip.clipId, before: ClipId.make("warm-building") },
    [old, warm],
    "owner_conflict",
  ],
] as const)
  test(`sameSessionAs refuses ${name} before dispatch`, async () => {
    const outcome = await Effect.runPromise(
      Effect.result(resolve(request(fields), candidates, "warm", undefined)),
    );
    expect(Result.isFailure(outcome)).toBe(true);
    if (Result.isFailure(outcome)) {
      expect(outcome.failure.reason).toBe(reason);
      expect(outcome.failure.context.outcome).toBe("not-submitted");
    }
  });

test("a ready-owner constraint binds a fresh sequence to the old source while renewal is warm", () =>
  runClock(
    Effect.gen(function* () {
      const {
        handle,
        sources,
        warm: warmSource,
      } = yield* renewalFixture((index) =>
        index === 0 ? { initial: readyState({ ready: [oldClip] }) } : {},
      );
      yield* warmSource;
      yield* handle.engine.enqueue(
        member("ready-owner", true, "first", { sameSessionAs: oldClip.clipId }),
      );
      expect((yield* handle.sequences.get("ready-owner"))?.owner).toBe("source-1");
      expect(sources[0]!.sends).toHaveLength(1);
      expect(sources[0]!.sends[0]!.position).toBeUndefined();
      expect(sources[1]!.sends).toEqual([]);
    }),
  ));

test("sameSessionAs is revalidated after prework and cannot move an already bound sequence", () =>
  runClock(
    Effect.gen(function* () {
      const entered = yield* gate,
        held = yield* gate;
      let pause = false;
      const {
        handle,
        sources,
        warm: warmSource,
      } = yield* renewalFixture((index) =>
        index === 0
          ? {
              initial: readyState({ ready: [oldClip] }),
              prework: () =>
                pause ? entered.release.pipe(Effect.andThen(held.wait)) : Effect.void,
            }
          : { initial: readyState({ ready: [warmClip] }) },
      );
      yield* handle.engine.enqueue(
        member("bound-owner", false, "first", { sameSessionAs: oldClip.clipId }),
      );
      yield* warmSource;
      const conflict = yield* Effect.result(
        handle.engine.enqueue(
          member("bound-owner", true, "second", { sameSessionAs: warmClip.clipId }),
        ),
      );
      expect(Result.isFailure(conflict) && conflict.failure.context.outcome).toBe("not-submitted");
      pause = true;
      const pending = yield* handle.engine
        .enqueue(member("revalidate-owner", true, "first", { sameSessionAs: oldClip.clipId }))
        .pipe(Effect.result, Effect.forkScoped);
      yield* entered.wait;
      yield* sources[0]!.setState(readyState());
      yield* held.release;
      const outcome = yield* Fiber.join(pending);
      expect(Result.isFailure(outcome) && outcome.failure.context.outcome).toBe("not-submitted");
      expect(sources[0]!.sends).toHaveLength(1);
      expect(sources[1]!.sends).toEqual([]);
      expect(yield* handle.sequences.get("revalidate-owner")).toBeUndefined();
    }),
  ));
