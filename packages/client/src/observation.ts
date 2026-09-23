import * as Cause from "effect/Cause";
import * as Effect from "effect/Effect";
import * as Queue from "effect/Queue";
import type * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import { parsed, positiveLimit, ReactorError } from "./errors.js";

interface Entry<A> {
  readonly value: A;
  readonly bytes: number;
}
interface Subscriber<A> {
  readonly queue: Queue.Queue<Entry<A>, ReactorError | Cause.Done>;
  readonly maxBytes: number;
  bufferedBytes: number;
}

export interface ObservationOptions {
  readonly capacity?: number;
  readonly maxBytes?: number;
}

/** Slow observation never blocks protocol correlation or silently drops lifecycle events. */
export class Observations<A> {
  private readonly subscribers = new Set<Subscriber<A>>();
  private closed = false;
  private failure: ReactorError | undefined;
  overflowCount = 0n;

  constructor(private readonly maxSubscribers = 64) {
    positiveLimit(maxSubscribers, "observer count", 4096);
  }

  get size(): number {
    return this.subscribers.size;
  }

  /** Acquire before reading a snapshot, then ignore queued revisions covered by that snapshot. */
  subscribe(
    options: ObservationOptions = {},
  ): Effect.Effect<Stream.Stream<A, ReactorError>, ReactorError, Scope.Scope> {
    const observations = this;
    return Effect.gen(function* () {
      const bounds = yield* parsed(() => ({
        capacity: positiveLimit(options.capacity ?? 64, "observation capacity", 4096),
        maxBytes: positiveLimit(
          options.maxBytes ?? 1_048_576,
          "observation bytes",
          64 * 1024 * 1024,
        ),
      }));
      const subscriber = yield* Effect.acquireRelease(
        Effect.gen(function* () {
          if (observations.subscribers.size >= observations.maxSubscribers) {
            return yield* ReactorError.fromCode("Overflow", "observer count bound reached", {
              outcome: "not-submitted",
            });
          }
          const queue = yield* Queue.dropping<Entry<A>, ReactorError | Cause.Done>(bounds.capacity);
          const subscriber: Subscriber<A> = { queue, maxBytes: bounds.maxBytes, bufferedBytes: 0 };
          if (observations.failure !== undefined)
            Queue.failCauseUnsafe(queue, Cause.fail(observations.failure));
          else if (observations.closed) Queue.endUnsafe(queue);
          else observations.subscribers.add(subscriber);
          return subscriber;
        }),
        (subscriber) =>
          Effect.gen(function* () {
            observations.subscribers.delete(subscriber);
            subscriber.bufferedBytes = 0;
            yield* Queue.shutdown(subscriber.queue);
          }),
      );

      let reading = false;
      return Stream.unwrap(
        Effect.gen(function* () {
          yield* Effect.acquireRelease(
            parsed(() => {
              if (reading)
                throw ReactorError.fromCode(
                  "AlreadyReading",
                  "this observation already has an active reader",
                );
              reading = true;
            }),
            () =>
              Effect.sync(() => {
                reading = false;
              }),
          );
          return Stream.fromEffectRepeat(
            Effect.uninterruptibleMask((restore) =>
              restore(Queue.take(subscriber.queue)).pipe(
                Effect.map((entry) => {
                  subscriber.bufferedBytes -= entry.bytes;
                  return entry.value;
                }),
              ),
            ),
          );
        }),
      );
    });
  }

  /**
   * A state and every value emitted after it, with no gap: the subscription is
   * acquired before `state` is read, so a value emitted in between is queued
   * rather than lost (and may repeat what the state already reflects).
   */
  observeWith<S, E>(
    state: Effect.Effect<S, E>,
    options?: ObservationOptions,
  ): Effect.Effect<
    { readonly initial: S; readonly events: Stream.Stream<A, ReactorError> },
    ReactorError | E,
    Scope.Scope
  > {
    return Effect.flatMap(this.subscribe(options), (events) =>
      Effect.map(state, (initial) => ({ initial, events })),
    );
  }

  stream(options?: ObservationOptions): Stream.Stream<A, ReactorError> {
    return Stream.unwrap(this.subscribe(options));
  }

  emit(value: A, bytes = 256): void {
    if (this.closed) return;
    for (const subscriber of this.subscribers) {
      if (
        bytes > subscriber.maxBytes - subscriber.bufferedBytes ||
        !Queue.offerUnsafe(subscriber.queue, { value, bytes })
      ) {
        this.overflowCount++;
        Queue.failCauseUnsafe(
          subscriber.queue,
          Cause.fail(
            ReactorError.fromCode(
              "Overflow",
              "observation bound exceeded; acquire a new observation and snapshot",
            ),
          ),
        );
        this.subscribers.delete(subscriber);
      } else subscriber.bufferedBytes += bytes;
    }
  }

  end(): void {
    if (this.closed) return;
    this.closed = true;
    for (const subscriber of this.subscribers) Queue.endUnsafe(subscriber.queue);
    this.subscribers.clear();
  }

  /** Source failure belongs to each reader, including a later subscription. */
  fail(error: ReactorError): void {
    if (this.closed) return;
    this.failure = error;
    this.closed = true;
    for (const subscriber of this.subscribers)
      Queue.failCauseUnsafe(subscriber.queue, Cause.fail(error));
    this.subscribers.clear();
  }
}
