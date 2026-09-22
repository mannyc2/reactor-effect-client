import { expect, test } from "bun:test";
import { Deferred, Effect, Exit, Fiber, Scope } from "effect";
import * as Submission from "../src/Submission.js";

const run = <A, E>(program: Effect.Effect<A, E, Scope.Scope>) =>
  Effect.runPromise(Effect.scoped(program));

test("preparation is inert and repeated/concurrent submit commits once", () =>
  run(
    Effect.gen(function* () {
      let prepares = 0,
        executions = 0;
      const entered = yield* Deferred.make<void>();
      const finish = yield* Deferred.make<number>();
      const operation = yield* Submission.make({
        id: "one",
        prepare: Effect.sync(() => {
          prepares++;
          return 7;
        }),
        execute: (input) =>
          Effect.gen(function* () {
            executions++;
            expect(input).toBe(7);
            yield* Deferred.succeed(entered, undefined);
            return yield* Deferred.await(finish);
          }),
      });
      expect(prepares).toBe(0);
      expect(executions).toBe(0);
      expect((yield* operation.state)._tag).toBe("Prepared");
      const callers = yield* Effect.forEach(Array.from({ length: 20 }), () =>
        Effect.forkChild(operation.submit),
      );
      yield* Deferred.await(entered);
      yield* Deferred.succeed(finish, 42);
      const results = yield* Effect.forEach(callers, Fiber.join);
      expect(results).toEqual(Array.from({ length: 20 }, () => 42));
      expect(yield* operation.submit).toBe(42);
      expect(prepares).toBe(1);
      expect(executions).toBe(1);
    }),
  ));

test("interruption before commit releases preparation resources and sends nothing", () =>
  run(
    Effect.gen(function* () {
      let acquired = 0,
        released = 0,
        dispatched = 0;
      const entered = yield* Deferred.make<void>();
      const continuePreparation = yield* Deferred.make<void>();
      const operation = yield* Submission.make({
        id: "preparation",
        prepare: Effect.gen(function* () {
          yield* Effect.acquireRelease(
            Effect.sync(() => {
              acquired++;
            }),
            () =>
              Effect.sync(() => {
                released++;
              }),
          );
          yield* Deferred.succeed(entered, undefined);
          yield* Deferred.await(continuePreparation);
          return "input";
        }),
        execute: () =>
          Effect.sync(() => {
            dispatched++;
            return "accepted";
          }),
      });
      const caller = yield* Effect.forkChild(operation.submit);
      yield* Deferred.await(entered);
      yield* Fiber.interrupt(caller);
      expect(acquired).toBe(1);
      expect(released).toBe(1);
      expect(dispatched).toBe(0);
      expect((yield* operation.state)._tag).toBe("Prepared");
      yield* Deferred.succeed(continuePreparation, undefined);
      expect(yield* operation.submit).toBe("accepted");
      expect(dispatched).toBe(1);
      expect(released).toBe(2);
    }),
  ));

test("commit registration is atomic with dispatch and a failed hook stays Prepared and retryable", () =>
  run(
    Effect.gen(function* () {
      let prepares = 0,
        commits = 0,
        executions = 0,
        releases = 0;
      const order: string[] = [];
      const operation = yield* Submission.make({
        id: "commit-hook",
        prepare: Effect.acquireRelease(
          Effect.sync(() => {
            prepares++;
            order.push("prepare");
            return prepares;
          }),
          () =>
            Effect.sync(() => {
              releases++;
            }),
        ),
        commit: (attempt) =>
          Effect.gen(function* () {
            commits++;
            order.push(`commit-${attempt}`);
            if (attempt === 1) return yield* Effect.fail("binding changed" as const);
          }),
        execute: (attempt) =>
          Effect.sync(() => {
            executions++;
            order.push(`execute-${attempt}`);
            return attempt;
          }),
      });

      const first = yield* Effect.result(operation.submit);
      expect(first._tag === "Failure" && first.failure).toBe("binding changed");
      expect((yield* operation.state)._tag).toBe("Prepared");
      expect(executions).toBe(0);
      expect(releases).toBe(1);

      expect(yield* operation.submit).toBe(2);
      expect(order).toEqual(["prepare", "commit-1", "prepare", "commit-2", "execute-2"]);
      expect(commits).toBe(2);
      expect(executions).toBe(1);
      expect(releases).toBe(2);
      expect((yield* operation.state)._tag).toBe("Completed");
    }),
  ));

test("interruption after commit abandons only the caller and retains eventual completion", () =>
  run(
    Effect.gen(function* () {
      let released = 0,
        executions = 0;
      const entered = yield* Deferred.make<void>();
      const response = yield* Deferred.make<string>();
      const operation = yield* Submission.make({
        id: "committed",
        prepare: Effect.acquireRelease(Effect.succeed("input"), () =>
          Effect.sync(() => {
            released++;
          }),
        ),
        execute: () =>
          Effect.gen(function* () {
            executions++;
            yield* Deferred.succeed(entered, undefined);
            return yield* Deferred.await(response);
          }),
      });
      const caller = yield* Effect.forkChild(operation.submit);
      yield* Deferred.await(entered);
      yield* Fiber.interrupt(caller);
      expect((yield* operation.state)._tag).toBe("Committed");
      expect(released).toBe(0);
      yield* Deferred.succeed(response, "accepted-later");
      expect(yield* operation.submit).toBe("accepted-later");
      expect(executions).toBe(1);
      expect(released).toBe(1);
      expect((yield* operation.state)._tag).toBe("Completed");
    }),
  ));

test("session scope termination joins committed work and releases its resources", async () => {
  let released = 0;
  const scope = await Effect.runPromise(Scope.make());
  const entered = Deferred.makeUnsafe<void>();
  const operation = await Effect.runPromise(
    Submission.make({
      id: "scope-close",
      prepare: Effect.acquireRelease(Effect.void, () =>
        Effect.sync(() => {
          released++;
        }),
      ),
      execute: () => Deferred.succeed(entered, undefined).pipe(Effect.andThen(Effect.never)),
    }).pipe(Scope.provide(scope)),
  );
  const caller = Effect.runFork(operation.submit);
  await Effect.runPromise(Deferred.await(entered));
  await Effect.runPromise(Scope.close(scope, Exit.void));
  const exit = await Effect.runPromise(Fiber.await(caller));
  expect(Exit.isFailure(exit)).toBe(true);
  expect(released).toBe(1);
  const state = await Effect.runPromise(operation.state);
  expect(state._tag).toBe("Completed");
  if (state._tag === "Completed") expect(Exit.isFailure(state.exit)).toBe(true);
});

test("a defective execution is recorded and is never retried", () =>
  run(
    Effect.gen(function* () {
      let calls = 0,
        released = 0;
      const operation = yield* Submission.make({
        id: "defect",
        prepare: Effect.acquireRelease(Effect.void, () =>
          Effect.sync(() => {
            released++;
          }),
        ),
        execute: () =>
          Effect.sync(() => {
            calls++;
            throw new Error("deliberate implementation defect");
          }),
      });
      expect(Exit.isFailure(yield* Effect.exit(operation.submit))).toBe(true);
      expect(Exit.isFailure(yield* Effect.exit(operation.submit))).toBe(true);
      expect(calls).toBe(1);
      expect(released).toBe(1);
      expect((yield* operation.state)._tag).toBe("Completed");
    }),
  ));
