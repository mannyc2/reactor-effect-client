/**
 * Request/reply correlation for the Reactor wire.
 *
 * Effect's `RpcClient` does not fit this wire: it drops a late or second
 * reply, forgets a request whose caller was interrupted and has no bound. A
 * model command is paid and has no cancel message, so a reply must stay
 * attributable after its caller stops waiting. Requests are registered before
 * they are sent, replies are labelled rather than dropped, and retiring a
 * generation fails its requests with their dispatch outcome.
 */
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Ref from "effect/Ref";
import { ReactorError } from "../ReactorError.js";

/** How a reply relates to the requests this client made. */
export type Correlation =
  | "matched"
  | "late"
  | "duplicate"
  | "unsolicited"
  | "late-or-unknown"
  | "stale-generation";

export interface Pending<A> {
  readonly id: string;
  readonly generation: bigint;
  readonly operation: string;
  readonly deferred: Deferred.Deferred<A, ReactorError>;
}

interface Entry<A> {
  readonly pending: Pending<A>;
  readonly submitted: boolean;
  readonly waiting: boolean;
}

type Completion = "acknowledged" | "replied" | "cancelled";

interface State<A> {
  readonly counter: bigint;
  readonly pending: ReadonlyMap<string, Entry<A>>;
  /** Recently completed ids, so a repeat or late reply is labelled, not mistaken. */
  readonly recent: ReadonlyMap<
    string,
    { readonly generation: bigint; readonly completion: Completion }
  >;
}

export interface Correlator<A> {
  readonly size: Effect.Effect<number>;
  readonly register: (
    generation: bigint,
    operation: string,
  ) => Effect.Effect<Pending<A>, ReactorError>;
  readonly submitted: (pending: Pending<A>) => Effect.Effect<void>;
  readonly isSubmitted: (pending: Pending<A>) => Effect.Effect<boolean>;
  readonly isPending: (pending: Pending<A>) => Effect.Effect<boolean>;
  /** The caller stopped waiting; a later reply is `late`, not `matched`. */
  readonly abandon: (pending: Pending<A>) => Effect.Effect<void>;
  readonly cancel: (pending: Pending<A>) => Effect.Effect<boolean>;
  /**
   * Labels a reply, then builds its result and completes the request if it
   * was pending. `result` runs before the caller wakes, so what it publishes
   * is ordered before anything the caller does next.
   */
  readonly settle: (
    id: string,
    generation: bigint,
    result: (correlation: Correlation) => Effect.Effect<A, ReactorError>,
    stage?: "acknowledged" | "replied",
  ) => Effect.Effect<Correlation>;
  readonly failGeneration: (generation: bigint, failure: ReactorError) => Effect.Effect<void>;
}

const remember = <A>(
  state: State<A>,
  id: string,
  generation: bigint,
  completion: Completion,
): State<A>["recent"] => {
  const recent = new Map(state.recent).set(id, { generation, completion });
  if (recent.size > 256) {
    const oldest = recent.keys().next();
    if (oldest.done !== true) recent.delete(oldest.value);
  }
  return recent;
};

const without = <A>(pending: State<A>["pending"], id: string): State<A>["pending"] => {
  const next = new Map(pending);
  next.delete(id);
  return next;
};

export const make = <A>(options: {
  readonly prefix: "data" | "ctrl";
  /** Requests awaiting a reply at once. */
  readonly limit: number;
  /** Distinguishes this client's ids from another's on a shared session. */
  readonly namespace: string;
}): Effect.Effect<Correlator<A>> =>
  Effect.map(
    Ref.make<State<A>>({ counter: 0n, pending: new Map(), recent: new Map() }),
    (state): Correlator<A> => {
      const update = (pending: Pending<A>, change: (entry: Entry<A>) => Entry<A>) =>
        Ref.update(state, (current) => {
          const entry = current.pending.get(pending.id);
          return entry?.pending === pending
            ? { ...current, pending: new Map(current.pending).set(pending.id, change(entry)) }
            : current;
        });
      const entryOf = (pending: Pending<A>) =>
        Effect.map(Ref.get(state), (current) => {
          const entry = current.pending.get(pending.id);
          return entry?.pending === pending ? entry : undefined;
        });
      return {
        size: Effect.map(Ref.get(state), (current) => current.pending.size),
        register: (generation, operation) =>
          Effect.gen(function* () {
            const deferred = yield* Deferred.make<A, ReactorError>();
            const pending = yield* Ref.modify(state, (current) => {
              if (current.pending.size >= options.limit) return [undefined, current] as const;
              const counter = current.counter + 1n;
              const id = `${options.prefix}_${options.namespace}_${String(counter)}`;
              const pending: Pending<A> = { id, generation, operation, deferred };
              return [
                pending,
                {
                  ...current,
                  counter,
                  pending: new Map(current.pending).set(id, {
                    pending,
                    submitted: false,
                    waiting: true,
                  }),
                },
              ] as const;
            });
            if (pending === undefined)
              return yield* ReactorError.fromCode("Overflow", "pending request bound reached", {
                operation,
                outcome: "not-submitted",
              });
            return pending;
          }),
        submitted: (pending) => update(pending, (entry) => ({ ...entry, submitted: true })),
        isSubmitted: (pending) =>
          Effect.map(entryOf(pending), (entry) => entry?.submitted === true),
        isPending: (pending) => Effect.map(entryOf(pending), (entry) => entry !== undefined),
        abandon: (pending) => update(pending, (entry) => ({ ...entry, waiting: false })),
        cancel: (pending) =>
          Ref.modify(state, (current) =>
            current.pending.get(pending.id)?.pending === pending
              ? [
                  true,
                  {
                    ...current,
                    pending: without(current.pending, pending.id),
                    recent: remember(current, pending.id, pending.generation, "cancelled"),
                  },
                ]
              : [false, current],
          ),
        settle: (id, generation, result, stage = "replied") =>
          Effect.gen(function* () {
            const [correlation, matched] = yield* Ref.modify(
              state,
              (current): readonly [readonly [Correlation, Pending<A> | undefined], State<A>] => {
                const entry = current.pending.get(id);
                const recent = current.recent.get(id);
                const correlation: Correlation =
                  id === ""
                    ? "unsolicited"
                    : entry !== undefined
                      ? entry.pending.generation !== generation
                        ? "stale-generation"
                        : entry.waiting
                          ? "matched"
                          : "late"
                      : recent === undefined
                        ? "late-or-unknown"
                        : recent.generation !== generation
                          ? "stale-generation"
                          : recent.completion === "acknowledged" && stage === "replied"
                            ? "late"
                            : recent.completion === "cancelled"
                              ? "late-or-unknown"
                              : "duplicate";
                if (entry !== undefined && (correlation === "matched" || correlation === "late"))
                  return [
                    [correlation, entry.pending] as const,
                    {
                      ...current,
                      pending: without(current.pending, id),
                      recent: remember(current, id, generation, stage),
                    },
                  ] as const;
                // An acknowledgement and a later model payload are two facts.
                if (correlation === "late" && recent?.completion === "acknowledged")
                  return [
                    [correlation, undefined] as const,
                    { ...current, recent: remember(current, id, generation, "replied") },
                  ] as const;
                return [[correlation, undefined] as const, current] as const;
              },
            );
            const completion = yield* Effect.exit(result(correlation));
            if (matched !== undefined) yield* Deferred.done(matched.deferred, completion);
            return correlation;
          }),
        failGeneration: (generation, failure) =>
          Effect.gen(function* () {
            const retired = yield* Ref.modify(state, (current) => {
              const retired = [...current.pending.values()].filter(
                (entry) => entry.pending.generation === generation,
              );
              let recent = current.recent;
              const pending = new Map(current.pending);
              for (const entry of retired) {
                pending.delete(entry.pending.id);
                recent = remember(
                  { ...current, recent },
                  entry.pending.id,
                  generation,
                  "cancelled",
                );
              }
              return [retired, { ...current, pending, recent }] as const;
            });
            yield* Effect.forEach(
              retired,
              (entry) =>
                Deferred.fail(
                  entry.pending.deferred,
                  ReactorError.make({
                    reason: failure.reason,
                    context: {
                      ...failure.context,
                      operation: entry.pending.operation,
                      requestId: entry.pending.id,
                      generation,
                      outcome: entry.submitted ? "unknown" : "not-submitted",
                    },
                  }),
                ),
              { discard: true },
            );
          }),
      };
    },
  );
