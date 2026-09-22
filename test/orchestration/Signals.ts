import { Deferred, Effect } from "effect";

/** Fixture events retain their history so a barrier cannot miss an earlier notification. */
export const signals = <A>() => {
  const values: A[] = [];
  const waiters = new Set<{
    readonly predicate: (value: A) => boolean;
    readonly ready: Deferred.Deferred<A>;
  }>();
  return {
    values,
    record: (value: A): void => {
      values.push(value);
      for (const waiter of waiters) {
        if (waiter.predicate(value)) {
          waiters.delete(waiter);
          Deferred.doneUnsafe(waiter.ready, Effect.succeed(value));
        }
      }
    },
    wait: (predicate: (value: A) => boolean): Effect.Effect<A> =>
      Effect.suspend(() => {
        const previous = values.find(predicate);
        if (previous !== undefined) return Effect.succeed(previous);
        const waiter = { predicate, ready: Deferred.makeUnsafe<A>() };
        waiters.add(waiter);
        return Deferred.await(waiter.ready).pipe(
          Effect.ensuring(
            Effect.sync(() => {
              waiters.delete(waiter);
            }),
          ),
        );
      }),
  };
};
