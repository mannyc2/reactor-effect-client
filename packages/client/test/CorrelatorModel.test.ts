/** The Correlator under random request, reply, cancel and retirement orders. */
import { expect, it } from "@effect/vitest";
import { Deferred, Effect, Schema } from "effect";
import { Correlator } from "../src/correlation.js";
import type { Pending } from "../src/correlation.js";
import { ReactorError } from "../src/errors.js";

const limit = 3;
/** Kinds weighted toward replies, so requests are usually answered in some order. */
const kinds = [
  ...["register", "register", "register"],
  ...["reply", "reply", "reply", "ack", "ack"],
  ...["cancel", "stopWaiting", "retire", "unknownReply"],
] as const;
const Step = Schema.Struct({
  kind: Schema.Int.check(Schema.isBetween({ minimum: 0, maximum: kinds.length - 1 })),
  /** Which registered request, by registration order. */
  request: Schema.Int.check(Schema.isBetween({ minimum: 0, maximum: 7 })),
  generation: Schema.Literals([1n, 2n]),
});

it.prop(
  "a request is matched at most once, in its own generation, and completes only then",
  { steps: Schema.Array(Step) },
  ({ steps }) => {
    const correlator = new Correlator<number>("data", limit);
    const registered: Pending<number>[] = [];
    const matched = new Map<string, number>();
    /** Requests whose result was delivered: settled by a reply or failed with their generation. */
    const delivered = new Set<string>();
    for (const step of steps) {
      const kind = kinds[step.kind]!;
      const target = registered[step.request % Math.max(registered.length, 1)];
      if (kind === "register") {
        const before = correlator.size;
        try {
          registered.push(correlator.register(step.generation, "command"));
        } catch (cause) {
          expect(ReactorError.is(cause) && cause.context.outcome).toBe("not-submitted");
          expect(before).toBe(limit);
        }
      } else if (kind === "unknownReply") {
        const correlation = correlator.settle("data_never", step.generation, Effect.succeed(0));
        expect(correlation).toBe("late-or-unknown");
      } else if (target === undefined) continue;
      else if (kind === "reply" || kind === "ack") {
        const pendingBefore = correlator.has(target);
        const correlation = correlator.settleWith(
          target.id,
          step.generation,
          () => Effect.succeed(1),
          kind === "ack" ? "acknowledged" : "replied",
        );
        if (correlation === "matched") matched.set(target.id, (matched.get(target.id) ?? 0) + 1);
        // A reply for another generation never settles a pending request.
        if (step.generation !== target.generation) {
          expect(correlation).toBe("stale-generation");
          expect(correlator.has(target)).toBe(pendingBefore);
        }
        // Only a pending request is settled; a body after its ACK is late, and changes nothing.
        if (correlation === "matched") expect(pendingBefore).toBe(true);
        if ((correlation === "matched" || correlation === "late") && pendingBefore) {
          expect(correlator.has(target)).toBe(false);
          delivered.add(target.id);
        }
      } else if (kind === "cancel") correlator.cancel(target);
      else if (kind === "stopWaiting") target.waiting = false;
      else {
        for (const pending of registered)
          if (pending.generation === step.generation && correlator.has(pending))
            delivered.add(pending.id);
        correlator.failGeneration(
          step.generation,
          ReactorError.fromCode("Disconnected", "generation retired"),
        );
      }
      expect(correlator.size).toBeLessThanOrEqual(limit);
    }
    for (const pending of registered) {
      expect(matched.get(pending.id) ?? 0).toBeLessThanOrEqual(1);
      // A cancel leaves the result to its canceller; nothing else completes a request.
      expect(Deferred.isDoneUnsafe(pending.deferred)).toBe(delivered.has(pending.id));
    }
  },
  { arbitrary: { runs: 300, size: 40 } },
);
