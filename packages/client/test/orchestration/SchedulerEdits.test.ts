import { expect, test } from "vitest";
import { Effect, Exit, Fiber, Scope } from "effect";
import { TestClock } from "effect/testing";
import { makeScheduler, ItemKey } from "../../src/orchestration/scheduler.js";
import type {
  EditResult,
  ItemHandle,
  SchedulerOptions,
} from "../../src/orchestration/scheduler.js";
import { ClipRequest } from "../../src/orchestration/request.js";
import * as Simulation from "../../src/simulation/index.js";
import { refusal, runClock } from "./SourceFixture.js";

const clip = (prompt: string) =>
  new ClipRequest({ prompt, references: [], durationSeconds: 5, metadata: {} });
const part = (key: string) => ({ key: ItemKey.make(key), request: clip(key) });
const item = (key: string) => ({ key: ItemKey.make(key), lane: "line", request: clip(key) });
const key = (value: string) => ItemKey.make(value);

const advance = (milliseconds: number) =>
  Effect.gen(function* () {
    for (let elapsed = 0; elapsed < milliseconds; elapsed += 20)
      yield* TestClock.adjust(Math.min(20, milliseconds - elapsed));
  });

// A zero runway floor keeps filler off, so builds run in admission order.
const options = {
  lanes: [{ name: "line" }],
  filler: {
    runway: { floor: "0 seconds", target: "1 second" },
    clip: ({ index }: { index: number }) => clip(`filler ${index}`),
  },
} satisfies SchedulerOptions;

// About hosted H3's pace: a 5 s clip builds in 2 s, one at a time.
const hosted = Simulation.layerSim({ fixedBuildTime: 2_000, buildRatio: 0 });

/** The keys of the handles that started, in the order they started. */
const startOrder = (handles: ReadonlyArray<ItemHandle>) =>
  Effect.gen(function* () {
    const starts: { readonly key: string; readonly at: number }[] = [];
    for (const handle of handles) {
      const status = yield* handle.started;
      if (status._tag === "Started") starts.push({ key: handle.key, at: status.at });
    }
    return starts.sort((a, b) => a.at - b.at).map(({ key }) => key);
  });

const added = (result: EditResult | undefined): ItemHandle => {
  if (result?._tag !== "Added") throw new Error("expected an added item");
  return result.handle;
};

// the-show#92: replacing a whole plan drops everything queued and adds the new plan; the old
// beats keep playing until the new ones are made, then the swap happens at one boundary.
test("a batch keeps old beats on air until its new clips are Ready, then swaps them together", () =>
  runClock(
    Effect.gen(function* () {
      const scheduler = yield* makeScheduler(options);
      yield* advance(1_000);
      const old = [];
      for (const name of ["a", "b", "c", "d"]) old.push(yield* scheduler.submit(item(name)));
      // a plays from 3 s; b is Ready, c is building and d has not started building.
      yield* advance(5_000);
      const batch = yield* scheduler.edit([
        { _tag: "Withdraw", key: key("b") },
        { _tag: "Withdraw", key: key("c") },
        { _tag: "Withdraw", key: key("d") },
        {
          _tag: "SubmitGroup",
          group: { key: key("new"), lane: "line", parts: [part("n1"), part("n2")] },
        },
      ]);
      const committing = yield* Effect.forkChild(batch.committed);
      // d was never built, so n1 and n2 build right after c and are Ready by 11 s.
      yield* advance(5_500);
      expect(committing.pollUnsafe()).toBeDefined();
      yield* advance(40_000);
      yield* Fiber.join(committing);
      const line = batch.results[3];
      if (line?._tag !== "AddedGroup") throw new Error("expected the new line");
      const [a, b, c, d] = old;
      expect(yield* startOrder([...old, ...line.handle.parts])).toEqual(["a", "b", "n1", "n2"]);
      const outcomes = [];
      for (const result of batch.results.slice(0, 3))
        if (result._tag === "Withdrawal") outcomes.push(yield* result.outcome);
      expect(outcomes).toEqual(["already-started", "withdrawn", "withdrawn"]);
      expect((yield* a!.outcome)._tag).toBe("Ended");
      expect((yield* b!.outcome)._tag).toBe("Ended");
      expect(yield* c!.outcome).toEqual({ _tag: "Dropped", reason: "withdrawn" });
      expect(yield* d!.outcome).toEqual({ _tag: "Dropped", reason: "withdrawn" });
    }).pipe(Effect.provide(hosted)),
  ));

test("a clip a batch adds airs ahead of what the batch withdraws, once it is Ready", () =>
  runClock(
    Effect.gen(function* () {
      const scheduler = yield* makeScheduler(options);
      yield* advance(1_000);
      const a = yield* scheduler.submit(item("a"));
      const b = yield* scheduler.submit(item("b"));
      yield* advance(3_000);
      // y is Ready when a ends, z only after: the batch has not taken effect then.
      const batch = yield* scheduler.edit([
        { _tag: "Withdraw", key: key("b") },
        { _tag: "Submit", item: item("y") },
        { _tag: "Submit", item: item("z") },
      ]);
      yield* advance(40_000);
      const [, y, z] = batch.results;
      expect(yield* startOrder([a, b, added(y), added(z)])).toEqual(["a", "y", "z"]);
      expect(yield* b.outcome).toEqual({ _tag: "Dropped", reason: "withdrawn" });
    }).pipe(Effect.provide(hosted)),
  ));

test("a batch's Ready replacement airs in its place before the batch takes effect", () =>
  runClock(
    Effect.gen(function* () {
      const scheduler = yield* makeScheduler(options);
      yield* advance(1_000);
      const a = yield* scheduler.submit(item("a"));
      const b = yield* scheduler.submit(item("b"));
      const c = yield* scheduler.submit(item("c"));
      yield* advance(3_000);
      // b2 is Ready before a ends; y, which the batch also waits for, is not.
      const batch = yield* scheduler.edit([
        { _tag: "Replace", key: key("b"), next: part("b2") },
        { _tag: "Submit", item: item("y") },
      ]);
      yield* advance(40_000);
      const [b2, y] = batch.results;
      expect(yield* startOrder([a, b, c, added(b2), added(y)])).toEqual(["a", "b2", "c", "y"]);
      expect(yield* b.outcome).toEqual({ _tag: "Dropped", reason: "replaced" });
    }).pipe(Effect.provide(hosted)),
  ));

test("a refused edit leaves the whole batch unapplied", () =>
  runClock(
    Effect.gen(function* () {
      const scheduler = yield* makeScheduler(options);
      yield* advance(1_000);
      yield* scheduler.submit(item("a"));
      const missing = yield* Effect.flip(
        scheduler.edit([
          { _tag: "Submit", item: item("n") },
          { _tag: "Replace", key: key("missing"), next: part("m") },
        ]),
      );
      expect(refusal(missing)).toBe("InvalidRequest");
      const twice = yield* Effect.flip(
        scheduler.edit([
          { _tag: "Submit", item: item("n") },
          { _tag: "Withdraw", key: key("n") },
        ]),
      );
      expect(refusal(twice)).toBe("InvalidRequest");
      expect((yield* scheduler.state).lanes[0]?.keys).toEqual([key("a")]);
      // The refused batches admitted nothing, so the key is still free for another spec.
      yield* scheduler.submit({ ...item("n"), request: clip("another") });
    }).pipe(Effect.provide(hosted)),
  ));

// 7b13431: once a batch's replacement started, the item it replaced went back to its place
// and aired right after it.
test("the item a batch replaces never airs once its replacement has started", () =>
  runClock(
    Effect.gen(function* () {
      const scheduler = yield* makeScheduler(options);
      yield* advance(1_000);
      const a = yield* scheduler.submit(item("a"));
      const b = yield* scheduler.submit(item("b"));
      const c = yield* scheduler.submit(item("c"));
      yield* advance(3_000);
      // b2 is Ready before a ends; y1 to y3 keep the batch pending well after b2 starts.
      const batch = yield* scheduler.edit([
        { _tag: "Replace", key: key("b"), next: part("b2") },
        { _tag: "Submit", item: item("y1") },
        { _tag: "Submit", item: item("y2") },
        { _tag: "Submit", item: item("y3") },
      ]);
      yield* advance(60_000);
      const [b2, y1, y2, y3] = batch.results.map(added);
      expect(yield* startOrder([a, b, c, b2!, y1!, y2!, y3!])).toEqual([
        "a",
        "b2",
        "c",
        "y1",
        "y2",
        "y3",
      ]);
      expect(yield* b.outcome).toEqual({ _tag: "Dropped", reason: "replaced" });
    }).pipe(Effect.provide(hosted)),
  ));

// 7b13431: a batch's deadline check counted the Ready clips the same batch withdraws first.
test("a batch's deadline check does not count what the batch withdraws", () =>
  runClock(
    Effect.gen(function* () {
      const scheduler = yield* makeScheduler({
        ...options,
        lanes: [{ name: "ack" }, { name: "line" }],
      });
      yield* advance(1_000);
      for (const name of ["a", "b", "c"]) yield* scheduler.submit(item(name));
      // a plays from 1.5 s to 6.7 s; b and c are Ready behind it.
      yield* advance(4_000);
      yield* scheduler.edit([
        { _tag: "Withdraw", key: key("b") },
        { _tag: "Withdraw", key: key("c") },
        {
          _tag: "Submit",
          item: { ...item("x"), lane: "ack", window: { startBy: "3 seconds", firm: true } },
        },
      ]);
    }).pipe(Effect.provide(Simulation.layerSim({ fixedBuildTime: 500, buildRatio: 0 }))),
  ));

// 7b13431: a batch's withdrawal outcome was settled only by a fiber the closing scheduler
// interrupted, so it never settled.
test("a pending batch's withdrawal outcome settles when the scheduler closes", () =>
  runClock(
    Effect.gen(function* () {
      const scope = yield* Scope.make();
      const scheduler = yield* makeScheduler(options).pipe(Scope.provide(scope));
      yield* advance(1_000);
      yield* scheduler.submit(item("a"));
      yield* scheduler.submit(item("b"));
      yield* advance(3_000);
      const batch = yield* scheduler.edit([
        { _tag: "Withdraw", key: key("b") },
        { _tag: "Submit", item: item("y") },
      ]);
      const withdrawal = batch.results[0];
      if (withdrawal?._tag !== "Withdrawal") throw new Error("expected a withdrawal");
      const outcome = yield* Effect.forkChild(Effect.exit(withdrawal.outcome));
      yield* Scope.close(scope, Exit.void);
      yield* advance(1_000);
      expect(outcome.pollUnsafe()).toBeDefined();
    }).pipe(Effect.provide(hosted)),
  ));

// 7b13431: a withdrawn clip that aired as cover and ended before its batch took effect
// reported not-found.
test("a batch's withdrawal of a clip that aired as cover reports already-started", () =>
  runClock(
    Effect.gen(function* () {
      const scheduler = yield* makeScheduler(options);
      yield* advance(1_000);
      yield* scheduler.submit(item("a"));
      yield* scheduler.submit(item("b"));
      // a plays from 3 s to 8.2 s; nothing the batch adds is Ready then, so b airs as cover,
      // and it ends at 13.3 s, before the batch takes effect at 14 s.
      yield* advance(7_000);
      const batch = yield* scheduler.edit([
        { _tag: "Withdraw", key: key("b") },
        { _tag: "Submit", item: item("y1") },
        { _tag: "Submit", item: item("y2") },
        { _tag: "Submit", item: item("y3") },
      ]);
      yield* advance(30_000);
      const withdrawal = batch.results[0];
      if (withdrawal?._tag !== "Withdrawal") throw new Error("expected a withdrawal");
      expect(yield* withdrawal.outcome).toBe("already-started");
    }).pipe(Effect.provide(hosted)),
  ));
