import * as Effect from "effect/Effect";
import * as Deferred from "effect/Deferred";
import { positiveLimit, ReactorError } from "./errors.js";
export interface Pending<A> {
  readonly id: string;
  readonly generation: bigint;
  readonly operation: string;
  readonly deferred: Deferred.Deferred<A, ReactorError>;
  submitted: boolean;
  waiting: boolean;
}
export type Correlation =
  | "matched"
  | "late"
  | "duplicate"
  | "unsolicited"
  | "late-or-unknown"
  | "stale-generation";
type Completion = "acknowledged" | "replied" | "cancelled";
interface Recent {
  readonly generation: bigint;
  readonly completion: Completion;
}
/** Synchronous register-before-send, separate from bounded best-effort observations. */
export class Correlator<A> {
  private counter = 0n;
  private readonly pending = new Map<string, Pending<A>>();
  private readonly recent = new Map<string, Recent>();
  constructor(
    readonly prefix: "data" | "ctrl",
    readonly limit = 128,
    readonly namespace = "",
  ) {
    positiveLimit(limit, "pending request limit", 4096);
  }
  get size(): number {
    return this.pending.size;
  }
  has(pending: Pending<A>): boolean {
    return this.pending.get(pending.id) === pending;
  }
  register(generation: bigint, operation: string): Pending<A> {
    if (this.pending.size >= this.limit)
      throw ReactorError.fromCode("Overflow", "pending request bound reached", {
        operation,
        outcome: "not-submitted",
      });
    if (this.counter === 0xffffffffffffffffn)
      throw ReactorError.fromCode("Overflow", "request identity space exhausted");
    const id = `${this.prefix}_${this.namespace ? `${this.namespace}_` : ""}${++this.counter}`;
    const pending = {
      id,
      generation,
      operation,
      deferred: Deferred.makeUnsafe<A, ReactorError>(),
      submitted: false,
      waiting: true,
    };
    this.pending.set(id, pending);
    return pending;
  }
  private remember(id: string, generation: bigint, completion: Completion): void {
    this.recent.set(id, { generation, completion });
    if (this.recent.size > 256) {
      const first = this.recent.keys().next();
      if (!first.done) this.recent.delete(first.value);
    }
  }
  settle(id: string, generation: bigint, result: Effect.Effect<A, ReactorError>): Correlation {
    return this.settleWith(id, generation, () => result);
  }
  /** Resolve attribution before publishing a result or waking its caller. */
  settleWith(
    id: string,
    generation: bigint,
    result: (correlation: Correlation) => Effect.Effect<A, ReactorError>,
    stage: "acknowledged" | "replied" = "replied",
  ): Correlation {
    const pending = this.pending.get(id);
    const recent = this.recent.get(id);
    const correlation: Correlation =
      id === ""
        ? "unsolicited"
        : pending !== undefined
          ? pending.generation !== generation
            ? "stale-generation"
            : pending.waiting
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
    const matched = pending !== undefined && (correlation === "matched" || correlation === "late");
    if (matched) {
      this.pending.delete(id);
      this.remember(id, generation, stage);
    } else if (correlation === "late" && recent?.completion === "acknowledged") {
      // A transport ACK and a later model payload are two different facts.
      this.remember(id, generation, "replied");
    }
    const completion = result(correlation);
    if (matched) Deferred.doneUnsafe(pending.deferred, completion);
    return correlation;
  }
  cancel(pending: Pending<A>): boolean {
    if (this.pending.get(pending.id) !== pending) return false;
    this.pending.delete(pending.id);
    this.remember(pending.id, pending.generation, "cancelled");
    return true;
  }
  failGeneration(generation: bigint, failure: ReactorError): void {
    for (const pending of this.pending.values()) {
      if (pending.generation !== generation) continue;
      this.pending.delete(pending.id);
      this.remember(pending.id, generation, "cancelled");
      Deferred.doneUnsafe(
        pending.deferred,
        Effect.fail(
          new ReactorError({
            reason: failure.reason,
            context: {
              ...failure.context,
              operation: pending.operation,
              requestId: pending.id,
              generation,
              outcome: pending.submitted ? "unknown" : "not-submitted",
            },
          }),
        ),
      );
    }
  }
}
