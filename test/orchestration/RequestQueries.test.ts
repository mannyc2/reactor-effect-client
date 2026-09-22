import { expect, test } from "bun:test";
import { Effect, Option, Result } from "effect";
import { captureRequest, PolicyFailure, preworkFailure } from "../../src/orchestration/request.js";
import {
  committedMs,
  emptyState,
  isIdle,
  isLive,
  pendingCount,
  securedMs,
} from "../../src/orchestration/queries.js";
import { failure, record, readyState, request } from "./SourceFixture.js";

test("capture detaches and freezes nested input without changing the caller's objects", async () => {
  const metadata = { nested: { title: "original" }, order: [1, 2] };
  const references = [{ uri: "data:image/png;base64,AAAA" }];
  const input = request({ metadata, references });
  const captured = await Effect.runPromise(captureRequest(input));
  metadata.nested.title = "changed";
  metadata.order.push(3);
  references[0]!.uri = "changed";
  expect(captured.metadata).toEqual({ nested: { title: "original" }, order: [1, 2] });
  expect(captured.references[0]!.uri).toBe("data:image/png;base64,AAAA");
  expect(Object.isFrozen(metadata)).toBe(false);
  expect(Object.isFrozen(captured)).toBe(true);
  expect(Object.isFrozen(captured.metadata.nested)).toBe(true);
  expect(await Effect.runPromise(captureRequest(captured))).toBe(captured);
});

test("malformed request objects, accessors and unsupported provider fields fail without invoking getters", async () => {
  let getters = 0;
  const cyclic = { self: {} };
  cyclic.self = cyclic;
  const invalid: unknown[] = [
    null,
    [],
    { ...request(), metadata: cyclic },
    {
      ...request(),
      get prompt() {
        getters++;
        throw new Error("private getter");
      },
    },
    {
      ...request(),
      metadata: {
        get secret() {
          getters++;
          return "private";
        },
      },
    },
    { ...request(), [Symbol("extra")]: 1 },
    { ...request(), startingFrame: {} },
    { ...request(), endingFrame: {} },
    { ...request(), reference_audio: [] },
    request({ durationSeconds: NaN }),
    request({ durationSeconds: Infinity }),
    request({ durationSeconds: 0 }),
    request({ prompt: "  " }),
    request({ position: -1 }),
    request({ position: 1.5 }),
    request({ seed: -1 }),
    request({ seed: 1.5 }),
    request({ references: [{ uri: "" }] }),
    request({ references: Array.from({ length: 10 }, () => ({ uri: "fixture" })) }),
  ];
  for (const value of invalid) {
    const result = await Effect.runPromise(Effect.result(captureRequest(value as never)));
    expect(Result.isFailure(result)).toBe(true);
    if (Result.isFailure(result)) {
      expect(result.failure).toBeInstanceOf(PolicyFailure);
      expect(result.failure.reason).toBe("invalid_request");
      expect(result.failure.context.outcome).toBe("not-submitted");
    }
  }
  expect(getters).toBe(0);
});
test("prompt-only input is valid and request prework cannot borrow a command's unknown dispatch outcome", async () => {
  expect((await Effect.runPromise(captureRequest(request()))).references).toEqual([]);
  const unknown = failure("unknown"),
    local = failure("not-submitted");
  expect(preworkFailure("load", unknown).context.outcome).toBe("not-submitted");
  expect(preworkFailure("load", local)).toBe(local);
});
test("startup idle and playback horizon require observed availability, record and start time", () => {
  const clip = record("foreign", 7);
  expect(isIdle(emptyState())).toBe(false);
  expect(isIdle(readyState())).toBe(true);
  const foreign = readyState({
    playing: Option.some({ clipId: clip.clipId, record: Option.none(), startedAt: Option.none() }),
  });
  expect(isIdle(foreign)).toBe(false);
  expect(securedMs(foreign, 1000)).toBe(0);
  const untimed = {
    ...foreign,
    playing: Option.some({
      clipId: clip.clipId,
      record: Option.some(clip),
      startedAt: Option.none(),
    }),
  };
  expect(securedMs(untimed, 1000)).toBe(0);
  const timed = {
    ...foreign,
    started: true,
    playing: Option.some({
      clipId: clip.clipId,
      record: Option.some(clip),
      startedAt: Option.some(1000),
    }),
    ready: [record("ready", 2)],
    queued: [record("queued", 3)],
    building: Option.some({ record: record("building", 4), startedAt: Option.none() }),
  };
  expect(securedMs(timed, 2000)).toBe(8000);
  expect(committedMs(timed, 2000)).toBe(15000);
  expect(securedMs(timed, 99000)).toBe(2000);
  expect(pendingCount(timed)).toBe(3);
  expect(isLive(timed)).toBe(true);
});
