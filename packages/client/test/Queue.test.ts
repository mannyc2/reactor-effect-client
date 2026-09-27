import { Cause, Context, Effect, Exit, Fiber, Queue, Scheduler } from "effect";
import { expect, onTestFinished, test } from "vitest";
import { take, takeAll } from "../src/_internal/queue.js";

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

const runtime = () => {
  const scheduler = new PreemptionScheduler();
  const context = Context.make(Scheduler.Scheduler, scheduler);
  const fibers: Array<Fiber.Fiber<unknown, unknown>> = [];
  const fork = <A, E>(effect: Effect.Effect<A, E>): Fiber.Fiber<A, E> => {
    const fiber = Effect.runForkWith(context)(effect);
    fibers.push(fiber);
    return fiber;
  };
  onTestFinished(() => {
    for (const fiber of fibers) fiber.interruptUnsafe();
    scheduler.flush();
  });
  const run = <A, E>(effect: Effect.Effect<A, E>): A => {
    // runSync installs its own scheduler, which would let queue dispatches
    // escape this test's manually controlled task list.
    const fiber = fork(effect);
    scheduler.flush();
    const exit = fiber.pollUnsafe();
    if (exit === undefined) throw new Error("Expected the effect to finish synchronously");
    if (Exit.isFailure(exit)) throw new Error(Cause.pretty(exit.cause));
    return exit.value;
  };
  return { scheduler, fork, run };
};

test("negative control: upstream take loses an offer between the empty check and waiter registration", () => {
  const { scheduler, fork, run } = runtime();
  const queue = run(Queue.unbounded<number>());
  // In rc.117, operation one checks the queue in suspend; operation two
  // enters andThen before awaitTake has registered its callback.
  scheduler.arm(2);
  const consumer = fork(Queue.take(queue));
  expect(scheduler.yielded).toBe(true);
  expect(queue.state._tag !== "Done" && queue.state.takers.size).toBe(0);

  expect(Queue.offerUnsafe(queue, 42)).toBe(true);
  scheduler.flush();

  expect(consumer.pollUnsafe()).toBeUndefined();
  expect(Queue.sizeUnsafe(queue)).toBe(1);
  expect(queue.state._tag !== "Done" && queue.state.takers.size).toBe(1);
  consumer.interruptUnsafe();
  scheduler.flush();
  expect(consumer.pollUnsafe()?._tag).toBe("Failure");
  expect(queue.state._tag !== "Done" && queue.state.takers.size).toBe(0);
});

test.each([1, 2, 3, 4, 5, 6])(
  "take cannot lose a lone offer with preemption at operation %i",
  (operation) => {
    const { scheduler, fork, run } = runtime();
    const queue = run(Queue.unbounded<number>());
    scheduler.arm(operation);
    const consumer = fork(take(queue));

    // A later operation may be unreachable until the callback has parked.
    // Either way, the offer happens before queued continuations can run.
    expect(Queue.offerUnsafe(queue, 42)).toBe(true);
    scheduler.flush();

    expect(consumer.pollUnsafe()).toEqual(Exit.succeed(42));
    expect(Queue.sizeUnsafe(queue)).toBe(0);
    expect(queue.state._tag !== "Done" && queue.state.takers.size).toBe(0);
  },
);

test.each([1, 2, 3, 4, 5, 6])(
  "takeAll cannot lose an offered batch with preemption at operation %i",
  (operation) => {
    const { scheduler, fork, run } = runtime();
    const queue = run(Queue.unbounded<number>());
    scheduler.arm(operation);
    const consumer = fork(takeAll(queue));

    expect(Queue.offerAllUnsafe(queue, [1, 2])).toEqual([]);
    scheduler.flush();

    expect(consumer.pollUnsafe()).toEqual(Exit.succeed([1, 2]));
    expect(Queue.sizeUnsafe(queue)).toBe(0);
    expect(queue.state._tag !== "Done" && queue.state.takers.size).toBe(0);
  },
);

test("interrupting an idle take removes its waiter and leaves the next offer available", () => {
  const { scheduler, fork, run } = runtime();
  const queue = run(Queue.unbounded<number>());
  const consumer = fork(take(queue));
  expect(queue.state._tag !== "Done" && queue.state.takers.size).toBe(1);

  consumer.interruptUnsafe();
  scheduler.flush();
  const exit = consumer.pollUnsafe();
  expect(exit !== undefined && Exit.hasInterrupts(exit)).toBe(true);
  expect(queue.state._tag !== "Done" && queue.state.takers.size).toBe(0);

  expect(Queue.offerUnsafe(queue, 7)).toBe(true);
  scheduler.flush();
  expect(run(take(queue))).toBe(7);
});

test("multiple idle takers consume successive offers exactly once", () => {
  const { scheduler, fork, run } = runtime();
  const queue = run(Queue.unbounded<number>());
  const first = fork(take(queue));
  const second = fork(take(queue));
  expect(queue.state._tag !== "Done" && queue.state.takers.size).toBe(2);

  expect(Queue.offerUnsafe(queue, 1)).toBe(true);
  scheduler.flush();
  expect(first.pollUnsafe()).toEqual(Exit.succeed(1));
  expect(second.pollUnsafe()).toBeUndefined();

  expect(Queue.offerUnsafe(queue, 2)).toBe(true);
  scheduler.flush();
  expect(second.pollUnsafe()).toEqual(Exit.succeed(2));
  expect(Queue.sizeUnsafe(queue)).toBe(0);
  expect(queue.state._tag !== "Done" && queue.state.takers.size).toBe(0);
});

test("taking from a full bounded queue admits its suspended producer", () => {
  const { scheduler, fork, run } = runtime();
  const queue = run(Queue.bounded<number>(1));
  expect(Queue.offerUnsafe(queue, 1)).toBe(true);
  const producer = fork(Queue.offer(queue, 2));
  expect(producer.pollUnsafe()).toBeUndefined();

  expect(run(take(queue))).toBe(1);
  scheduler.flush();
  expect(producer.pollUnsafe()).toEqual(Exit.succeed(true));
  expect(run(take(queue))).toBe(2);
  expect(Queue.sizeUnsafe(queue)).toBe(0);
});

test("take receives a pending offer from a zero-capacity queue", () => {
  const { scheduler, fork, run } = runtime();
  const queue = run(Queue.bounded<number>(0));
  const producer = fork(Queue.offer(queue, 42));
  expect(producer.pollUnsafe()).toBeUndefined();

  expect(run(take(queue))).toBe(42);
  scheduler.flush();
  expect(producer.pollUnsafe()).toEqual(Exit.succeed(true));
});

test("take rechecks a zero-capacity offer registered during preemption", () => {
  const { scheduler, fork, run } = runtime();
  const queue = run(Queue.bounded<number>(0));
  scheduler.arm(2);
  const consumer = fork(take(queue));
  expect(scheduler.yielded).toBe(true);
  const producer = fork(Queue.offer(queue, 42));
  expect(queue.state._tag !== "Done" && queue.state.offers.size).toBe(1);

  scheduler.flush();
  expect(consumer.pollUnsafe()).toEqual(Exit.succeed(42));
  expect(producer.pollUnsafe()).toEqual(Exit.succeed(true));
});

test("ending a queue wakes its idle taker with Done", () => {
  const { scheduler, fork, run } = runtime();
  const queue = run(Queue.unbounded<number, Cause.Done>());
  const consumer = fork(take(queue));
  const batchConsumer = fork(takeAll(queue));

  expect(Queue.endUnsafe(queue)).toBe(true);
  scheduler.flush();
  expect(consumer.pollUnsafe()).toEqual(Exit.fail(Cause.Done()));
  expect(batchConsumer.pollUnsafe()).toEqual(Exit.fail(Cause.Done()));
  expect(run(Effect.exit(take(queue)))).toEqual(Exit.fail(Cause.Done()));
  expect(run(Effect.exit(takeAll(queue)))).toEqual(Exit.fail(Cause.Done()));
});

test("a closing queue drains its buffered value before Done", () => {
  const { run } = runtime();
  const queue = run(Queue.unbounded<number, Cause.Done>());
  expect(Queue.offerUnsafe(queue, 42)).toBe(true);
  expect(Queue.endUnsafe(queue)).toBe(true);

  expect(run(take(queue))).toBe(42);
  expect(run(Effect.exit(take(queue)))).toEqual(Exit.fail(Cause.Done()));
});

test("takeAll drains a closing queue's buffered batch before Done", () => {
  const { run } = runtime();
  const queue = run(Queue.unbounded<number, Cause.Done>());
  expect(Queue.offerAllUnsafe(queue, [1, 2])).toEqual([]);
  expect(Queue.endUnsafe(queue)).toBe(true);

  expect(run(takeAll(queue))).toEqual([1, 2]);
  expect(run(Effect.exit(takeAll(queue)))).toEqual(Exit.fail(Cause.Done()));
});

test("queue failure reaches both a parked take and future takes unchanged", () => {
  const { scheduler, fork, run } = runtime();
  const queue = run(Queue.unbounded<number, string>());
  const consumer = fork(take(queue));
  const batchConsumer = fork(takeAll(queue));

  expect(Queue.failCauseUnsafe(queue, Cause.fail("disconnected"))).toBe(true);
  scheduler.flush();
  expect(consumer.pollUnsafe()).toEqual(Exit.fail("disconnected"));
  expect(batchConsumer.pollUnsafe()).toEqual(Exit.fail("disconnected"));
  expect(run(Effect.exit(take(queue)))).toEqual(Exit.fail("disconnected"));
  expect(run(Effect.exit(takeAll(queue)))).toEqual(Exit.fail("disconnected"));
});
