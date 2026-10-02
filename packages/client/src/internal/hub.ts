/**
 * Fan-out to bounded observers. Publishing never waits for a reader, and an
 * observer that falls behind fails with `Overflow` rather than silently
 * missing values: it can observe again for a fresh state. `PubSub` offers
 * backpressure, dropping or sliding, none of which fails the one slow reader.
 *
 * A reader is bounded by a count and, when values are weighed, by the weight
 * it holds, so a few large media frames cannot buffer without bound.
 */
import * as Cause from "effect/Cause";
import * as Effect from "effect/Effect";
import * as Queue from "effect/Queue";
import * as Ref from "effect/Ref";
import type * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import { ReactorError } from "../ReactorError.js";

interface Entry<A> {
  readonly value: A;
  readonly weight: number;
}

interface Reader<A> {
  readonly queue: Queue.Queue<Entry<A>, ReactorError | Cause.Done>;
  readonly held: Ref.Ref<number>;
  readonly maxWeight: number;
}

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
    maxWeight?: number,
  ) => Effect.Effect<Stream.Stream<A, ReactorError>, ReactorError, Scope.Scope>;
  readonly end: Effect.Effect<void>;
  readonly fail: (cause: Cause.Cause<ReactorError>) => Effect.Effect<void>;
  readonly observers: Effect.Effect<number>;
  /** Observers that fell behind and failed. */
  readonly overflows: Effect.Effect<bigint>;
}

export interface Options<A> {
  readonly maxObservers?: number;
  /** A value's weight against a reader's `maxWeight`; unweighed values weigh nothing. */
  readonly weigh?: (value: A) => number;
}

const overflow = Cause.fail(
  ReactorError.fromCode("Overflow", "observation bound exceeded; observe again for a fresh state"),
);

export const make = <A>(options: Options<A> = {}): Effect.Effect<Hub<A>> =>
  Effect.map(
    Ref.make<State<A>>({ readers: new Set(), end: undefined, overflows: 0n }),
    (state): Hub<A> => {
      const maxObservers = options.maxObservers ?? 64;
      const drop = (reader: Reader<A>) =>
        Queue.failCause(reader.queue, overflow).pipe(
          Effect.andThen(
            Ref.update(state, (current) => {
              const remaining = new Set(current.readers);
              remaining.delete(reader);
              return { ...current, readers: remaining, overflows: current.overflows + 1n };
            }),
          ),
        );
      return {
        publish: Effect.fnUntraced(function* (value: A) {
          const { readers } = yield* Ref.get(state);
          const weight = options.weigh?.(value) ?? 0;
          for (const reader of readers) {
            const held = yield* Ref.get(reader.held);
            const fits = weight <= reader.maxWeight - held;
            if (fits && (yield* Queue.offer(reader.queue, { value, weight }))) {
              yield* Ref.update(reader.held, (total) => total + weight);
              continue;
            }
            yield* drop(reader);
          }
        }),
        subscribe: Effect.fnUntraced(function* (
          capacity: number = 64,
          maxWeight: number = Number.POSITIVE_INFINITY,
        ) {
          const reader = yield* Effect.acquireRelease(
            Effect.gen(function* () {
              const opened: Reader<A> = {
                queue: yield* Queue.dropping<Entry<A>, ReactorError | Cause.Done>(capacity),
                held: yield* Ref.make(0),
                maxWeight,
              };
              const admitted = yield* Ref.modify(state, (current) => {
                if (current.end !== undefined || current.readers.size >= maxObservers)
                  return [current, current] as const;
                return [
                  undefined,
                  { ...current, readers: new Set(current.readers).add(opened) },
                ] as const;
              });
              if (admitted?.end === "Done") yield* Queue.end(opened.queue);
              else if (admitted?.end !== undefined)
                yield* Queue.failCause(opened.queue, admitted.end);
              else if (admitted !== undefined)
                return yield* ReactorError.fromCode("Overflow", "observer count bound reached", {
                  outcome: "not-submitted",
                });
              return opened;
            }),
            (opened) =>
              Ref.update(state, (current) => {
                const remaining = new Set(current.readers);
                remaining.delete(opened);
                return { ...current, readers: remaining };
              }).pipe(Effect.andThen(Queue.shutdown(opened.queue))),
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
              return Stream.fromEffectRepeat(
                Queue.take(reader.queue).pipe(
                  Effect.tap((entry) => Ref.update(reader.held, (total) => total - entry.weight)),
                  Effect.map((entry) => entry.value),
                ),
              );
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
          yield* Effect.forEach(readers, (reader) => Queue.end(reader.queue), { discard: true });
        }),
        fail: Effect.fnUntraced(function* (cause: Cause.Cause<ReactorError>) {
          const readers = yield* Ref.modify(
            state,
            (current): readonly [ReadonlySet<Reader<A>>, State<A>] =>
              current.end === undefined
                ? [current.readers, { ...current, readers: new Set<Reader<A>>(), end: cause }]
                : [new Set<Reader<A>>(), current],
          );
          yield* Effect.forEach(readers, (reader) => Queue.failCause(reader.queue, cause), {
            discard: true,
          });
        }),
        observers: Effect.map(Ref.get(state), (current) => current.readers.size),
        overflows: Effect.map(Ref.get(state), (current) => current.overflows),
      };
    },
  );
