import { expect, test } from "vitest";
import { Effect, Option, Result } from "effect";
import { ClipId, PolicyFailure } from "../../src/orchestration/request.js";
import { activeIds, generation, resolve } from "../../src/orchestration/routing.js";
import type { Candidate } from "../../src/orchestration/routing.js";
import type { SequenceSnapshot } from "../../src/Sequence.js";
import { record, readyState, request, refusal } from "./SourceFixture.js";

const first = record("first"),
  second = record("second"),
  third = record("third");
const candidate = (owner: string, fields: Partial<Candidate<string>> = {}): Candidate<string> => ({
  owner,
  state: readyState(),
  accepted: new Set(),
  closed: false,
  recovering: false,
  ...fields,
});
const a = candidate("old", {
  state: readyState({
    queued: [first, second],
    generationOrder: [first.clipId, second.clipId],
    continuable: [first.clipId],
  }),
});
const b = candidate("warm", {
  state: readyState({
    queued: [third],
    generationOrder: [third.clipId],
    continuable: [third.clipId],
  }),
});
const sequence = (fields: Partial<SequenceSnapshot<string>> = {}): SequenceSnapshot<string> => ({
  id: "sequence",
  owner: "old",
  status: "open",
  acceptedCount: 0,
  rejectedCount: 0,
  indeterminateCount: 0,
  pendingCount: 0,
  sealRequested: false,
  members: [],
  ...fields,
});
const assertFailure = (result: Result.Result<unknown, PolicyFailure>, reason: string) => {
  expect(Result.isFailure(result)).toBe(true);
  if (Result.isFailure(result)) {
    expect(result.failure).toBeInstanceOf(PolicyFailure);
    expect(refusal(result.failure)).toBe(reason);
    expect(result.failure.context.outcome).toBe("not-submitted");
  }
};

test("joint sequence, before and continuation ownership stays old while replacement is warm", async () => {
  const decision = await Effect.runPromise(
    resolve(
      request({ before: second.clipId, continueFrom: first.clipId, position: 1 }),
      [a, b],
      "warm",
      sequence(),
    ),
  );
  expect(decision).toEqual({ owner: "old", position: 1 });
  expect(Object.isFrozen(decision)).toBe(true);
});

for (const [name, input, binding] of [
  ["sequence versus before", request({ before: third.clipId }), sequence()],
  ["sequence versus continuation", request({ continueFrom: third.clipId }), sequence()],
  [
    "before versus continuation",
    request({ before: second.clipId, continueFrom: third.clipId }),
    undefined,
  ],
  ["all three owners", request({ before: third.clipId, continueFrom: first.clipId }), sequence()],
] as const)
  test(`rejects ${name} conflict before choosing a dispatch`, async () => {
    assertFailure(
      await Effect.runPromise(Effect.result(resolve(input, [a, b], "warm", binding))),
      "OwnerConflict",
    );
  });

for (const [name, input, candidates, binding, reason] of [
  [
    "missing anchor",
    request({ before: ClipId.make("missing") }),
    [a, b],
    undefined,
    "Missing:anchor",
  ],
  [
    "missing continuation",
    request({ continueFrom: ClipId.make("missing") }),
    [a, b],
    undefined,
    "Missing:continuation",
  ],
  [
    "duplicate foreign identity",
    request({ before: first.clipId }),
    [a, candidate("warm", { accepted: new Set([first.clipId]) })],
    undefined,
    "OwnerConflict",
  ],
  [
    "retired owner",
    request({ continueFrom: first.clipId }),
    [{ ...a, closed: true }, b],
    undefined,
    "SessionRetired",
  ],
  ["missing bound owner", request(), [b], sequence(), "SessionRetired"],
  ["recovering owner", request(), [{ ...a, recovering: true }], sequence(), "SessionRecovering"],
  [
    "incomplete snapshot",
    request(),
    [{ ...a, state: { ...a.state, availability: "Synchronizing" as const } }],
    sequence(),
    "SessionRecovering",
  ],
  [
    "anchor already left generation",
    request({ before: first.clipId }),
    [candidate("old", { accepted: new Set([first.clipId]) })],
    undefined,
    "AnchorUnavailable",
  ],
  [
    "expired continuation",
    request({ continueFrom: second.clipId }),
    [a],
    undefined,
    "ContinuationUnavailable",
  ],
  [
    "position disagrees with anchor",
    request({ before: second.clipId, position: 0 }),
    [a],
    undefined,
    "PositionConflict",
  ],
] as const)
  test(`routing rejects ${name} as a local policy failure`, async () => {
    assertFailure(
      await Effect.runPromise(Effect.result(resolve(input, candidates, "old", binding))),
      reason,
    );
  });

for (const status of ["sealed", "retired", "indeterminate"] as const)
  test(`a ${status} sequence cannot migrate to the warm source`, async () => {
    assertFailure(
      await Effect.runPromise(
        Effect.result(resolve(request(), [a, b], "warm", sequence({ status }))),
      ),
      `Sequence:${status}`,
    );
  });
test("an explicitly sealing sequence rejects new members", async () => {
  assertFailure(
    await Effect.runPromise(
      Effect.result(resolve(request(), [a], "old", sequence({ sealRequested: true }))),
    ),
    "Sequence:sealing",
  );
});
test("explicit positions including past end are unchanged and omitted append remains absent", async () => {
  for (const position of [undefined, 0, 1, 9999]) {
    const input = request(position === undefined ? {} : { position });
    expect(await Effect.runPromise(resolve(input, [a, b], "warm", undefined))).toEqual({
      owner: "warm",
      position,
    });
  }
});
test("generation order preserves the provider's non-head build position and enforces its capacity", async () => {
  const state = readyState({
    queued: [first, third],
    generationOrder: [first.clipId, second.clipId, third.clipId],
    building: Option.some({ record: second, startedAt: Option.some(100) }),
    capacities: { generation: 3, playout: 1 },
  });
  expect(generation(state)).toEqual([first, second, third]);
  expect(activeIds(state)).toEqual([first.clipId, second.clipId, third.clipId]);
  assertFailure(
    await Effect.runPromise(
      Effect.result(resolve(request(), [candidate("old", { state })], "old", undefined)),
    ),
    "QueueFull",
  );
});
test("unknown foreign playback contributes ownership without inventing a record", async () => {
  const state = readyState({
    playing: Option.some({ clipId: first.clipId, record: Option.none(), startedAt: Option.none() }),
  });
  expect(activeIds(state)).toEqual([first.clipId]);
  assertFailure(
    await Effect.runPromise(
      Effect.result(
        resolve(
          request({ continueFrom: first.clipId }),
          [candidate("old", { state })],
          "old",
          undefined,
        ),
      ),
    ),
    "ContinuationUnavailable",
  );
});
