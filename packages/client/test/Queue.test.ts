/** The queue take workaround: no offer is lost to a yield between an empty read and its waiter. */
import { assert, it } from "@effect/vitest";
import { Cause, Context, Effect, Exit, Queue, Scheduler } from "effect";
import type { Fiber } from "effect";
import { take, takeAll } from "../src/internal/queue.js";

/** A synchronous scheduler that yields a fiber at the operation a test arms, then runs tasks on demand. */
class PreemptionScheduler implements Scheduler.Scheduler {
  readonly executionMode = "sync";
  private readonly tasks: Array<() => void> = [];
  private remaining: number | undefined;
  yielded = false;

  arm(operation: number): void {
    this.remaining = operation;
    this.yielded = false;
  }

  shouldYield(): boolean {
    if (this.remaining === undefined) return false;
    this.remaining--;
    if (this.remaining > 0) return false;
    this.remaining = undefined;
    this.yielded = true;
    return true;
  }

  makeDispatcher(): Scheduler.SchedulerDispatcher {
    return {
      scheduleTask: (task) => {
        this.tasks.push(task);
      },
      flush: () => this.flush(),
    };
  }

  flush(): void {
    for (let task = this.tasks.shift(); task !== undefined; task = this.tasks.shift()) task();
  }
}

/**
 * Fibers on a scheduler of their own, so the test decides when each continuation
 * runs; they are interrupted as the test's scope closes.
 */
const controlled = Effect.acquireRelease(
  Effect.sync(() => {
    const scheduler = new PreemptionScheduler();
    const context = Context.make(Scheduler.Scheduler, scheduler);
    const fibers: Array<Fiber.Fiber<unknown, unknown>> = [];
    const fork = <A, E>(effect: Effect.Effect<A, E>): Fiber.Fiber<A, E> => {
      const fiber = Effect.runForkWith(context)(effect);
      fibers.push(fiber);
      return fiber;
    };
    // runSync installs its own scheduler, which would let queue dispatches escape the task list.
    const run = <A, E>(effect: Effect.Effect<A, E>): Effect.Effect<A, E> =>
      Effect.suspend(() => {
        const fiber = fork(effect);
        scheduler.flush();
        return fiber.pollUnsafe() ?? Effect.die("the effect did not finish synchronously");
      });
    return { scheduler, fork, run, fibers };
  }),
  ({ scheduler, fibers }) =>
    Effect.sync(() => {
      for (const fiber of fibers) fiber.interruptUnsafe();
      scheduler.flush();
    }),
);

/** How many takers wait on `queue`. */
const takers = <A, E>(queue: Queue.Queue<A, E>) =>
  queue.state._tag === "Done" ? 0 : queue.state.takers.size;

it.effect(
  "negative control: upstream take loses an offer between the empty check and waiter registration",
  () =>
    Effect.gen(function* () {
      const { scheduler, fork, run } = yield* controlled;
      const queue = yield* run(Queue.unbounded<number>());
      // In rc.117, operation one checks the queue in suspend; operation two
      // enters andThen before awaitTake has registered its callback.
      scheduler.arm(2);
      const consumer = fork(Queue.take(queue));
      assert.isTrue(scheduler.yielded);
      assert.strictEqual(takers(queue), 0);

      assert.isTrue(Queue.offerUnsafe(queue, 42));
      scheduler.flush();

      assert.isUndefined(consumer.pollUnsafe());
      assert.strictEqual(Queue.sizeUnsafe(queue), 1);
      assert.strictEqual(takers(queue), 1);
      consumer.interruptUnsafe();
      scheduler.flush();
      assert.strictEqual(consumer.pollUnsafe()?._tag, "Failure");
      assert.strictEqual(takers(queue), 0);
    }),
);

it.effect.each([1, 2, 3, 4, 5, 6])(
  "take cannot lose a lone offer with preemption at operation %i",
  (operation) =>
    Effect.gen(function* () {
      const { scheduler, fork, run } = yield* controlled;
      const queue = yield* run(Queue.unbounded<number>());
      scheduler.arm(operation);
      const consumer = fork(take(queue));

      // A later operation may be unreachable until the callback has parked.
      // Either way, the offer happens before queued continuations can run.
      assert.isTrue(Queue.offerUnsafe(queue, 42));
      scheduler.flush();

      assert.deepStrictEqual(consumer.pollUnsafe(), Exit.succeed(42));
      assert.strictEqual(Queue.sizeUnsafe(queue), 0);
      assert.strictEqual(takers(queue), 0);
    }),
);

it.effect.each([1, 2, 3, 4, 5, 6])(
  "takeAll cannot lose an offered batch with preemption at operation %i",
  (operation) =>
    Effect.gen(function* () {
      const { scheduler, fork, run } = yield* controlled;
      const queue = yield* run(Queue.unbounded<number>());
      scheduler.arm(operation);
      const consumer = fork(takeAll(queue));

      assert.deepStrictEqual(Queue.offerAllUnsafe(queue, [1, 2]), []);
      scheduler.flush();

      assert.deepStrictEqual(consumer.pollUnsafe(), Exit.succeed([1, 2]));
      assert.strictEqual(Queue.sizeUnsafe(queue), 0);
      assert.strictEqual(takers(queue), 0);
    }),
);

it.effect("interrupting an idle take removes its waiter and leaves the next offer available", () =>
  Effect.gen(function* () {
    const { scheduler, fork, run } = yield* controlled;
    const queue = yield* run(Queue.unbounded<number>());
    const consumer = fork(take(queue));
    assert.strictEqual(takers(queue), 1);

    consumer.interruptUnsafe();
    scheduler.flush();
    const exit = consumer.pollUnsafe();
    assert.strictEqual(exit !== undefined && Exit.hasInterrupts(exit), true);
    assert.strictEqual(takers(queue), 0);

    assert.isTrue(Queue.offerUnsafe(queue, 7));
    scheduler.flush();
    assert.strictEqual(yield* run(take(queue)), 7);
  }),
);

it.effect("multiple idle takers consume successive offers exactly once", () =>
  Effect.gen(function* () {
    const { scheduler, fork, run } = yield* controlled;
    const queue = yield* run(Queue.unbounded<number>());
    const first = fork(take(queue));
    const second = fork(take(queue));
    assert.strictEqual(takers(queue), 2);

    assert.isTrue(Queue.offerUnsafe(queue, 1));
    scheduler.flush();
    assert.deepStrictEqual(first.pollUnsafe(), Exit.succeed(1));
    assert.isUndefined(second.pollUnsafe());

    assert.isTrue(Queue.offerUnsafe(queue, 2));
    scheduler.flush();
    assert.deepStrictEqual(second.pollUnsafe(), Exit.succeed(2));
    assert.strictEqual(Queue.sizeUnsafe(queue), 0);
    assert.strictEqual(takers(queue), 0);
  }),
);

it.effect("taking from a full bounded queue admits its suspended producer", () =>
  Effect.gen(function* () {
    const { scheduler, fork, run } = yield* controlled;
    const queue = yield* run(Queue.bounded<number>(1));
    assert.isTrue(Queue.offerUnsafe(queue, 1));
    const producer = fork(Queue.offer(queue, 2));
    assert.isUndefined(producer.pollUnsafe());

    assert.strictEqual(yield* run(take(queue)), 1);
    scheduler.flush();
    assert.deepStrictEqual(producer.pollUnsafe(), Exit.succeed(true));
    assert.strictEqual(yield* run(take(queue)), 2);
    assert.strictEqual(Queue.sizeUnsafe(queue), 0);
  }),
);

it.effect("take receives a pending offer from a zero-capacity queue", () =>
  Effect.gen(function* () {
    const { scheduler, fork, run } = yield* controlled;
    const queue = yield* run(Queue.bounded<number>(0));
    const producer = fork(Queue.offer(queue, 42));
    assert.isUndefined(producer.pollUnsafe());

    assert.strictEqual(yield* run(take(queue)), 42);
    scheduler.flush();
    assert.deepStrictEqual(producer.pollUnsafe(), Exit.succeed(true));
  }),
);

it.effect("take rechecks a zero-capacity offer registered during preemption", () =>
  Effect.gen(function* () {
    const { scheduler, fork, run } = yield* controlled;
    const queue = yield* run(Queue.bounded<number>(0));
    scheduler.arm(2);
    const consumer = fork(take(queue));
    assert.isTrue(scheduler.yielded);
    const producer = fork(Queue.offer(queue, 42));
    assert.strictEqual(queue.state._tag === "Done" ? 0 : queue.state.offers.size, 1);

    scheduler.flush();
    assert.deepStrictEqual(consumer.pollUnsafe(), Exit.succeed(42));
    assert.deepStrictEqual(producer.pollUnsafe(), Exit.succeed(true));
  }),
);

it.effect("ending a queue wakes its idle taker with Done", () =>
  Effect.gen(function* () {
    const { scheduler, fork, run } = yield* controlled;
    const queue = yield* run(Queue.unbounded<number, Cause.Done>());
    const consumer = fork(take(queue));
    const batchConsumer = fork(takeAll(queue));

    assert.strictEqual(Queue.endUnsafe(queue), true);
    scheduler.flush();
    assert.deepStrictEqual(consumer.pollUnsafe(), Exit.fail(Cause.Done()));
    assert.deepStrictEqual(batchConsumer.pollUnsafe(), Exit.fail(Cause.Done()));
    assert.deepStrictEqual(yield* run(Effect.exit(take(queue))), Exit.fail(Cause.Done()));
    assert.deepStrictEqual(yield* run(Effect.exit(takeAll(queue))), Exit.fail(Cause.Done()));
  }),
);

it.effect("a closing queue drains its buffered value before Done", () =>
  Effect.gen(function* () {
    const { run } = yield* controlled;
    const queue = yield* run(Queue.unbounded<number, Cause.Done>());
    assert.isTrue(Queue.offerUnsafe(queue, 42));
    assert.strictEqual(Queue.endUnsafe(queue), true);

    assert.strictEqual(yield* run(take(queue)), 42);
    assert.deepStrictEqual(yield* run(Effect.exit(take(queue))), Exit.fail(Cause.Done()));
  }),
);

it.effect("takeAll drains a closing queue's buffered batch before Done", () =>
  Effect.gen(function* () {
    const { run } = yield* controlled;
    const queue = yield* run(Queue.unbounded<number, Cause.Done>());
    assert.deepStrictEqual(Queue.offerAllUnsafe(queue, [1, 2]), []);
    assert.strictEqual(Queue.endUnsafe(queue), true);

    assert.deepStrictEqual(yield* run(takeAll(queue)), [1, 2]);
    assert.deepStrictEqual(yield* run(Effect.exit(takeAll(queue))), Exit.fail(Cause.Done()));
  }),
);

it.effect("queue failure reaches both a parked take and future takes unchanged", () =>
  Effect.gen(function* () {
    const { scheduler, fork, run } = yield* controlled;
    const queue = yield* run(Queue.unbounded<number, string>());
    const consumer = fork(take(queue));
    const batchConsumer = fork(takeAll(queue));

    assert.strictEqual(Queue.failCauseUnsafe(queue, Cause.fail("disconnected")), true);
    scheduler.flush();
    assert.deepStrictEqual(consumer.pollUnsafe(), Exit.fail("disconnected"));
    assert.deepStrictEqual(batchConsumer.pollUnsafe(), Exit.fail("disconnected"));
    assert.deepStrictEqual(yield* run(Effect.exit(take(queue))), Exit.fail("disconnected"));
    assert.deepStrictEqual(yield* run(Effect.exit(takeAll(queue))), Exit.fail("disconnected"));
  }),
);
