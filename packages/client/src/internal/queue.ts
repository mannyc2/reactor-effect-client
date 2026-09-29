import * as Effect from "effect/Effect";
import * as Queue from "effect/Queue";

/**
 * rc.117 can yield between an empty read and registering its taker, losing an
 * offer's only wakeup. Recheck readiness in the same synchronous callback that
 * registers the waiter. Remove once the pinned Effect includes
 * https://github.com/Effect-TS/effect/pull/8472.
 */
const awaitAvailable = <A, E>(self: Queue.Dequeue<A, E>): Effect.Effect<void, E> =>
  Effect.callback<void, E>((resume) => {
    const state: Queue.Queue.State<A, E> = self.state;
    if (state._tag === "Done") return resume(state.exit);
    if (self.messages.length > 0 || (self.capacity <= 0 && state.offers.size > 0))
      return resume(Effect.void);
    state.takers.add(resume);
    return Effect.sync(() => {
      state.takers.delete(resume);
    });
  });

export const take = <A, E>(self: Queue.Dequeue<A, E>): Effect.Effect<A, E> =>
  Effect.suspend(() => Queue.takeUnsafe(self) ?? Effect.andThen(awaitAvailable(self), take(self)));
