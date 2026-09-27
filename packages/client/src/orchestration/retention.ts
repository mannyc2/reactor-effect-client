import * as Effect from "effect/Effect";
import { parsed } from "../internal/validation.js";
import { ReactorError } from "../ReactorError.js";
import { completeKind } from "./types.js";
import type { CleanupSummary, SourceCleanup } from "./types.js";
import { summarize } from "../ReactorError.js";

export interface Options {
  readonly retainedSuccessfulCleanups?: number;
  readonly maxUnresolvedCleanups?: number;
}

/** Local completion is independent of a source's canonical remote-cleanup report. */
export type Retirement = CleanupSummary["retained"][number]["retirement"];
export type Record = CleanupSummary["retained"][number];
export type Summary = CleanupSummary;

type CompleteKind = keyof Summary["omittedComplete"];

interface Attempt {
  source?: Record["source"];
  cleanup?: SourceCleanup;
  conflictingCleanup?: SourceCleanup;
  completed?: Record;
}
const attemptState: unique symbol = Symbol("retirement attempt");

/** An attempt carries only factual evidence, never a live Source, Slot or Scope. */
export interface Reservation {
  readonly incarnation: bigint;
  readonly [attemptState]: Attempt;
}

/** Neither range starts at one, which `positiveLimit` requires. */
const limit = (value: number, name: string, minimum: number): number => {
  if (!Number.isSafeInteger(value) || value < minimum || value > 4096)
    throw ReactorError.fromCode("InvalidInput", `${name} must be an integer in ${minimum}..4096`, {
      outcome: "not-submitted",
    });
  return value;
};

/**
 * Reservations precede allocation so even a failed acquisition has room for
 * its final evidence. Completed failures never compete with still-owned sources
 * for that room. All transitions are synchronous; no user work runs here.
 */
export const make = (options: Options = {}) =>
  parsed(() => {
    const successfulLimit = limit(
      options.retainedSuccessfulCleanups ?? 64,
      "retainedSuccessfulCleanups",
      0,
    );
    const unresolvedLimit = limit(options.maxUnresolvedCleanups ?? 16, "maxUnresolvedCleanups", 2);
    const outstanding = new Set<Reservation>();
    const completed: { readonly record: Record; readonly kind: CompleteKind }[] = [];
    const incomplete: Record[] = [];
    const omitted = { noAllocation: 0n, ownedTerminated: 0n, attachedDetached: 0n };
    let incarnation = 0n;
    let ordinal = 0n;
    let exhausted = false;

    const active = (reservation: Reservation): Attempt => {
      if (!outstanding.has(reservation))
        throw new Error("Retirement reservation is not outstanding");
      return reservation[attemptState];
    };
    const reserve = parsed((): Reservation => {
      if (exhausted || incomplete.length + outstanding.size >= unresolvedLimit) {
        exhausted = true;
        throw ReactorError.fromCode("Overflow", "Orchestration cleanup retention exhausted");
      }
      if (outstanding.size >= 2)
        throw ReactorError.fromCode(
          "Overflow",
          "Orchestration already owns two source reservations",
        );
      const reservation: Reservation = Object.freeze({
        incarnation: ++incarnation,
        [attemptState]: {},
      });
      outstanding.add(reservation);
      return reservation;
    });
    const identify = (reservation: Reservation, sessionId: string) =>
      parsed(() => {
        if (typeof sessionId !== "string" || sessionId.length === 0 || sessionId.length > 1024)
          throw ReactorError.fromCode(
            "InvalidInput",
            "Source identity must contain 1..1024 UTF-16 code units",
          );
        const attempt = active(reservation);
        if (attempt.source !== undefined && attempt.source.sessionId !== sessionId)
          throw new Error("Retirement attempt cannot change physical identity");
        attempt.source = Object.freeze({ sessionId, incarnation: reservation.incarnation });
      });
    const record = (reservation: Reservation, cleanup: SourceCleanup) =>
      Effect.sync(() => {
        const attempt = reservation[attemptState];
        // Repeated delivery of one canonical lease is idempotent only within this
        // attempt; independent no-allocation failures can share a singleton lease.
        if (
          attempt.cleanup?.lease === cleanup.lease ||
          attempt.conflictingCleanup?.lease === cleanup.lease
        )
          return;
        active(reservation);
        if (attempt.cleanup === undefined) attempt.cleanup = cleanup;
        else {
          if (attempt.conflictingCleanup !== undefined)
            throw new Error("Retirement attempt produced more than two canonical cleanup reports");
          attempt.conflictingCleanup = cleanup;
          exhausted = true;
        }
      });
    const finish = (reservation: Reservation, retirement: Retirement) =>
      Effect.sync((): Record => {
        const attempt = reservation[attemptState];
        if (attempt.completed !== undefined) return attempt.completed;
        active(reservation);
        const kind = completeKind({ ...attempt, retirement });
        const row: Record = Object.freeze({
          ordinal: ++ordinal,
          ...(attempt.source === undefined ? {} : { source: attempt.source }),
          ...(attempt.cleanup === undefined ? {} : { cleanup: attempt.cleanup }),
          ...(attempt.conflictingCleanup === undefined
            ? {}
            : { conflictingCleanup: attempt.conflictingCleanup }),
          retirement: Object.freeze({
            ...retirement,
            errors: Object.freeze([...retirement.errors]),
          }),
          disposition: kind === undefined ? "incomplete" : "complete",
        });
        attempt.completed = row;
        outstanding.delete(reservation);
        if (kind === undefined) {
          incomplete.push(row);
          if (incomplete.length >= unresolvedLimit) exhausted = true;
        } else {
          completed.push({ record: row, kind });
          if (completed.length > successfulLimit) {
            const evicted = completed.shift();
            if (evicted === undefined)
              throw new Error("Complete retirement eviction has no record");
            omitted[evicted.kind]++;
          }
        }
        return row;
      });
    const summary = Effect.sync((): Summary =>
      Object.freeze({
        format: "reactor-orchestration-cleanup-summary/v1",
        totalRetirements: ordinal,
        omittedComplete: Object.freeze({ ...omitted }),
        retained: Object.freeze(
          [...incomplete, ...completed.map((entry) => entry.record)].sort((a, b) =>
            a.ordinal < b.ordinal ? -1 : a.ordinal > b.ordinal ? 1 : 0,
          ),
        ),
        exhausted,
      }),
    );
    /**
     * The final summary. Its owner joins every attempt first, so an attempt
     * without a retirement record here is still retiring or lost the record to
     * a failure in its bookkeeping. Rather than being omitted, it is recorded
     * as incomplete, with timed-out accounting and failed scope and affinity so
     * that no local stage is reported as completed.
     */
    const conclude = Effect.suspend(() =>
      Effect.forEach(
        [...outstanding],
        (reservation) =>
          finish(reservation, {
            accounting: "timed-out",
            scope: "failed",
            affinity: "failed",
            errors: [
              summarize(
                ReactorError.fromCode(
                  "InvalidState",
                  "Source retirement was not recorded before the final cleanup summary",
                ),
              ),
            ],
            unknownSubmissions: 0n,
          }),
        { discard: true },
      ),
    ).pipe(Effect.andThen(summary));
    return { reserve, identify, record, finish, summary, conclude };
  });
