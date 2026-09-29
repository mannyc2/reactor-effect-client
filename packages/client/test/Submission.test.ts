/** A submission prepares at most once, commits once, and keeps what it committed past its callers. */
import { assert, it } from "@effect/vitest";
import { Deferred, Effect, Exit, Fiber, Ref, Scope } from "effect";
import * as Submission from "../src/internal/h3/submission.js";

/** A counter the test reads after the fact. */
const counter = Effect.map(Ref.make(0), (count) => ({
  bump: Ref.update(count, (n) => n + 1),
  value: Ref.get(count),
}));

it.effect("preparation is inert and repeated/concurrent submit commits once", () =>
  Effect.gen(function* () {
    const prepares = yield* counter;
    const executions = yield* counter;
    const entered = yield* Deferred.make<void>();
    const finish = yield* Deferred.make<number>();
    const operation = yield* Submission.make({
      id: "one",
      prepare: Effect.as(prepares.bump, 7),
      execute: (input) =>
        Effect.gen(function* () {
          yield* executions.bump;
          assert.strictEqual(input, 7);
          yield* Deferred.succeed(entered, undefined);
          return yield* Deferred.await(finish);
        }),
    });
    assert.deepStrictEqual([yield* prepares.value, yield* executions.value], [0, 0]);
    assert.strictEqual((yield* operation.state)._tag, "Prepared");
    const callers = yield* Effect.forEach(Array.from({ length: 20 }), () =>
      Effect.forkChild(operation.submit),
    );
    yield* Deferred.await(entered);
    yield* Deferred.succeed(finish, 42);
    const results = yield* Effect.forEach(callers, Fiber.join);
    assert.deepStrictEqual(
      results,
      Array.from({ length: 20 }, () => 42),
    );
    assert.strictEqual(yield* operation.submit, 42);
    assert.deepStrictEqual([yield* prepares.value, yield* executions.value], [1, 1]);
  }),
);

it.effect("interruption before commit releases preparation resources and sends nothing", () =>
  Effect.gen(function* () {
    const acquired = yield* counter;
    const released = yield* counter;
    const dispatched = yield* counter;
    const entered = yield* Deferred.make<void>();
    const continuePreparation = yield* Deferred.make<void>();
    const operation = yield* Submission.make({
      id: "preparation",
      prepare: Effect.gen(function* () {
        yield* Effect.acquireRelease(acquired.bump, () => released.bump);
        yield* Deferred.succeed(entered, undefined);
        yield* Deferred.await(continuePreparation);
        return "input";
      }),
      execute: () => Effect.as(dispatched.bump, "accepted"),
    });
    const caller = yield* Effect.forkChild(operation.submit);
    yield* Deferred.await(entered);
    yield* Fiber.interrupt(caller);
    assert.deepStrictEqual(
      [yield* acquired.value, yield* released.value, yield* dispatched.value],
      [1, 1, 0],
    );
    assert.strictEqual((yield* operation.state)._tag, "Prepared");
    yield* Deferred.succeed(continuePreparation, undefined);
    assert.strictEqual(yield* operation.submit, "accepted");
    assert.deepStrictEqual([yield* dispatched.value, yield* released.value], [1, 2]);
  }),
);

it.effect(
  "commit registration is atomic with dispatch and a failed hook stays Prepared and retryable",
  () =>
    Effect.gen(function* () {
      const prepares = yield* counter;
      const releases = yield* counter;
      const order = yield* Ref.make<ReadonlyArray<string>>([]);
      const note = (step: string) => Ref.update(order, (all) => [...all, step]);
      const operation = yield* Submission.make({
        id: "commit-hook",
        prepare: Effect.acquireRelease(
          Effect.gen(function* () {
            yield* prepares.bump;
            yield* note("prepare");
            return yield* prepares.value;
          }),
          () => releases.bump,
        ),
        commit: (attempt) =>
          Effect.gen(function* () {
            yield* note(`commit-${attempt}`);
            if (attempt === 1) return yield* Effect.fail("binding changed" as const);
          }),
        execute: (attempt) => Effect.as(note(`execute-${attempt}`), attempt),
      });

      const first = yield* Effect.flip(operation.submit);
      assert.strictEqual(first, "binding changed");
      assert.strictEqual((yield* operation.state)._tag, "Prepared");
      assert.strictEqual(yield* releases.value, 1);

      assert.strictEqual(yield* operation.submit, 2);
      assert.deepStrictEqual(yield* Ref.get(order), [
        "prepare",
        "commit-1",
        "prepare",
        "commit-2",
        "execute-2",
      ]);
      assert.strictEqual(yield* releases.value, 2);
      assert.strictEqual((yield* operation.state)._tag, "Completed");
    }),
);

it.effect(
  "interruption after commit abandons only the caller and retains eventual completion",
  () =>
    Effect.gen(function* () {
      const released = yield* counter;
      const executions = yield* counter;
      const entered = yield* Deferred.make<void>();
      const response = yield* Deferred.make<string>();
      const operation = yield* Submission.make({
        id: "committed",
        prepare: Effect.acquireRelease(Effect.succeed("input"), () => released.bump),
        execute: () =>
          Effect.gen(function* () {
            yield* executions.bump;
            yield* Deferred.succeed(entered, undefined);
            return yield* Deferred.await(response);
          }),
      });
      const caller = yield* Effect.forkChild(operation.submit);
      yield* Deferred.await(entered);
      yield* Fiber.interrupt(caller);
      assert.strictEqual((yield* operation.state)._tag, "Committed");
      assert.strictEqual(yield* released.value, 0);
      yield* Deferred.succeed(response, "accepted-later");
      assert.strictEqual(yield* operation.submit, "accepted-later");
      assert.deepStrictEqual([yield* executions.value, yield* released.value], [1, 1]);
      assert.strictEqual((yield* operation.state)._tag, "Completed");
    }),
);

it.effect("session scope termination joins committed work and releases its resources", () =>
  Effect.gen(function* () {
    const released = yield* counter;
    const scope = yield* Scope.make();
    const entered = yield* Deferred.make<void>();
    const operation = yield* Submission.make({
      id: "scope-close",
      prepare: Effect.acquireRelease(Effect.void, () => released.bump),
      execute: () => Deferred.succeed(entered, undefined).pipe(Effect.andThen(Effect.never)),
    }).pipe(Scope.provide(scope));
    const caller = yield* Effect.forkChild(operation.submit);
    yield* Deferred.await(entered);
    yield* Scope.close(scope, Exit.void);
    assert.isTrue(Exit.isFailure(yield* Fiber.await(caller)));
    assert.strictEqual(yield* released.value, 1);
    const state = yield* operation.state;
    assert.isTrue(state._tag === "Completed" && Exit.isFailure(state.exit));
  }),
);

it.effect("a defective execution is recorded and is never retried", () =>
  Effect.gen(function* () {
    const calls = yield* counter;
    const released = yield* counter;
    const operation = yield* Submission.make({
      id: "defect",
      prepare: Effect.acquireRelease(Effect.void, () => released.bump),
      execute: () =>
        calls.bump.pipe(Effect.andThen(Effect.die("deliberate implementation defect"))),
    });
    assert.isTrue(Exit.isFailure(yield* Effect.exit(operation.submit)));
    assert.isTrue(Exit.isFailure(yield* Effect.exit(operation.submit)));
    assert.deepStrictEqual([yield* calls.value, yield* released.value], [1, 1]);
    assert.strictEqual((yield* operation.state)._tag, "Completed");
  }),
);
