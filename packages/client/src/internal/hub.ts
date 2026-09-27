/**
 * Fan-out to bounded observers. Publishing never waits for a reader, and an
 * observer that falls behind fails with `Overflow` rather than silently
 * missing events: it can observe again for a fresh state. `PubSub` offers
 * backpressure, dropping or sliding, none of which fails the one slow reader.
 */
import * as Cause from "effect/Cause";
import * as Effect from "effect/Effect";
import * as Queue from "effect/Queue";
import * as Ref from "effect/Ref";
import type * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import { ReactorError } from "../ReactorError.js";
import { take } from "./queue.js";

type Reader<A> = Queue.Queue<A, ReactorError | Cause.Done>;

interface State<A> {
  readonly readers: ReadonlySet<Reader<A>>;
  /** Set once the source ends (`Done`) or fails; a later observer sees the same end. */
  readonly end: "Done" | Cause.Cause<ReactorError> | undefined;
  readonly overflows: bigint;
}

export interface Hub<A> {
  /** Delivers to every observer; one that is full fails with `Overflow`. */
  readonly publish: (value: A) => Effect.Effect<void>;
  /** An observer registered now; its stream has one reader at a time. */
  readonly subscribe: (
    capacity?: number,
  ) => Effect.Effect<Stream.Stream<A, ReactorError>, ReactorError, Scope.Scope>;
  readonly end: Effect.Effect<void>;
  readonly fail: (cause: Cause.Cause<ReactorError>) => Effect.Effect<void>;
  readonly observers: Effect.Effect<number>;
  /** Observers that fell behind and failed. */
  readonly overflows: Effect.Effect<bigint>;
}

const overflow = Cause.fail(
  ReactorError.fromCode("Overflow", "observation bound exceeded; observe again for a fresh state"),
);

export const make = <A>(maxObservers = 64): Effect.Effect<Hub<A>> =>
  Effect.map(
    Ref.make<State<A>>({ readers: new Set(), end: undefined, overflows: 0n }),
    (state): Hub<A> => ({
      publish: (value) =>
        Effect.gen(function* () {
          const { readers } = yield* Ref.get(state);
          for (const reader of readers) {
            if (yield* Queue.offer(reader, value)) continue;
            yield* Queue.failCause(reader, overflow);
            yield* Ref.update(state, (current) => {
              const remaining = new Set(current.readers);
              remaining.delete(reader);
              return { ...current, readers: remaining, overflows: current.overflows + 1n };
            });
          }
        }),
      subscribe: (capacity = 64) =>
        Effect.gen(function* () {
          const reader = yield* Effect.acquireRelease(
            Effect.gen(function* () {
              const queue = yield* Queue.dropping<A, ReactorError | Cause.Done>(capacity);
              const admitted = yield* Ref.modify(state, (current) => {
                if (current.end !== undefined || current.readers.size >= maxObservers)
                  return [current, current] as const;
                return [
                  undefined,
                  { ...current, readers: new Set(current.readers).add(queue) },
                ] as const;
              });
              if (admitted?.end === "Done") yield* Queue.end(queue);
              else if (admitted?.end !== undefined) yield* Queue.failCause(queue, admitted.end);
              else if (admitted !== undefined)
                return yield* ReactorError.fromCode("Overflow", "observer count bound reached", {
                  outcome: "not-submitted",
                });
              return queue;
            }),
            (queue) =>
              Ref.update(state, (current) => {
                const remaining = new Set(current.readers);
                remaining.delete(queue);
                return { ...current, readers: remaining };
              }).pipe(Effect.andThen(Queue.shutdown(queue))),
          );
          const reading = yield* Ref.make(false);
          return Stream.unwrap(
            Effect.gen(function* () {
              yield* Effect.acquireRelease(
                Ref.getAndSet(reading, true).pipe(
                  Effect.filterOrFail(
                    (busy) => !busy,
                    () =>
                      ReactorError.fromCode(
                        "AlreadyReading",
                        "this observation already has a reader",
                      ),
                  ),
                ),
                () => Ref.set(reading, false),
              );
              return Stream.fromEffectRepeat(take(reader));
            }),
          );
        }),
      end: Effect.gen(function* () {
        const readers = yield* Ref.modify(
          state,
          (current): readonly [ReadonlySet<Reader<A>>, State<A>] =>
            current.end === undefined
              ? [current.readers, { ...current, readers: new Set<Reader<A>>(), end: "Done" }]
              : [new Set<Reader<A>>(), current],
        );
        yield* Effect.forEach(readers, (reader) => Queue.end(reader), { discard: true });
      }),
      fail: (cause) =>
        Effect.gen(function* () {
          const readers = yield* Ref.modify(
            state,
            (current): readonly [ReadonlySet<Reader<A>>, State<A>] =>
              current.end === undefined
                ? [current.readers, { ...current, readers: new Set<Reader<A>>(), end: cause }]
                : [new Set<Reader<A>>(), current],
          );
          yield* Effect.forEach(readers, (reader) => Queue.failCause(reader, cause), {
            discard: true,
          });
        }),
      observers: Effect.map(Ref.get(state), (current) => current.readers.size),
      overflows: Effect.map(Ref.get(state), (current) => current.overflows),
    }),
  );
