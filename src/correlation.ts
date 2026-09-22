import * as Effect from "effect/Effect";
import * as Deferred from "effect/Deferred";
import { positiveLimit, ReactorError } from "./errors.js";
export interface Pending<A> {
  readonly id: string; readonly generation: bigint; readonly operation: string;
  readonly deferred: Deferred.Deferred<A, ReactorError>; submitted: boolean; waiting: boolean;
}
export type Correlation = "matched" | "late" | "duplicate" | "late-or-unknown" | "stale-generation";
/** Synchronous register-before-send, separate from bounded best-effort observations. */
export class Correlator<A> {
  private counter = 0n;
  private readonly pending = new Map<string, Pending<A>>();
  private readonly recent = new Map<string, "replied" | "cancelled">();
  constructor(readonly prefix: "data" | "ctrl", readonly limit = 128, readonly namespace = "") { positiveLimit(limit, "pending request limit", 4096); }
  get size(): number { return this.pending.size; }
  has(pending: Pending<A>): boolean { return this.pending.get(pending.id) === pending; }
  register(generation: bigint, operation: string): Pending<A> {
    if (this.pending.size >= this.limit) throw new ReactorError("Overflow", "pending request bound reached", { operation, outcome: "not-submitted" });
    if (this.counter === 0xffffffffffffffffn) throw new ReactorError("Overflow", "request identity space exhausted");
    const id = `${this.prefix}_${this.namespace ? `${this.namespace}_` : ""}${++this.counter}`;
    const pending = { id, generation, operation, deferred: Deferred.makeUnsafe<A, ReactorError>(), submitted: false, waiting: true };
    this.pending.set(id, pending); return pending;
  }
  private remember(id: string, reason: "replied" | "cancelled"): void {
    this.recent.set(id, reason);
    if (this.recent.size > 256) { const first = this.recent.keys().next(); if (!first.done) this.recent.delete(first.value); }
  }
  settle(id: string, generation: bigint, result: Effect.Effect<A, ReactorError>): Correlation {
    const pending = this.pending.get(id);
    if (pending === undefined) return this.recent.get(id) === "replied" ? "duplicate" : "late-or-unknown";
    if (pending.generation !== generation) return "stale-generation";
    this.pending.delete(id); this.remember(id, "replied"); Deferred.doneUnsafe(pending.deferred, result); return pending.waiting ? "matched" : "late";
  }
  cancel(pending: Pending<A>): boolean {
    if (this.pending.get(pending.id) !== pending) return false;
    this.pending.delete(pending.id); this.remember(pending.id, "cancelled"); return true;
  }
  failGeneration(generation: bigint, failure: ReactorError): void {
    for (const pending of this.pending.values()) {
      if (pending.generation !== generation) continue;
      this.pending.delete(pending.id); this.remember(pending.id, "cancelled");
      Deferred.doneUnsafe(pending.deferred, Effect.fail(new ReactorError(failure.code, failure.message, {
        ...failure.context, operation: pending.operation, requestId: pending.id, generation,
        outcome: pending.submitted ? "unknown" : "not-submitted",
      })));
    }
  }
}
