import * as Effect from "effect/Effect";
import * as Result from "effect/Result";
import { parsed, positiveLimit, ReactorError } from "../errors.js";
import type { SourceCleanup } from "./types.js";

export interface Options {
  readonly retainedSuccessfulCleanups?: number;
  readonly maxUnresolvedCleanups?: number;
}

/** Local completion is independent of a source's canonical remote-cleanup report. */
export interface Retirement {
  readonly accounting: "settled" | "timed-out" | "not-applicable";
  readonly scope: "closed" | "failed";
  readonly affinity: "retired" | "failed" | "not-applicable";
  readonly errors: readonly ReactorError[];
}

export interface Record {
  readonly ordinal: bigint;
  readonly source?: { readonly sessionId: string; readonly incarnation: bigint };
  readonly cleanup?: SourceCleanup;
  readonly conflictingCleanup?: SourceCleanup;
  readonly retirement: Retirement;
  readonly disposition: "complete" | "incomplete";
}

export interface Summary {
  readonly format: "reactor-orchestration-cleanup-summary/v1";
  readonly totalRetirements: bigint;
  readonly omittedComplete: {
    readonly noAllocation: bigint;
    readonly ownedTerminated: bigint;
    readonly attachedDetached: bigint;
  };
  readonly retained: readonly Record[];
  readonly exhausted: boolean;
}

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

const noTermination = (report: SourceCleanup["lease"]["remote"]): boolean =>
  !report.attempted &&
  !report.responseReceived &&
  !report.confirmed &&
  report.evidence === null &&
  report.deleteStatus === null &&
  report.state === null;

const completeKind = (attempt: Attempt, retirement: Retirement): CompleteKind | undefined => {
  const cleanup = attempt.cleanup;
  if (
    cleanup === undefined ||
    attempt.conflictingCleanup !== undefined ||
    retirement.accounting === "timed-out" ||
    retirement.scope !== "closed" ||
    retirement.affinity === "failed" ||
    retirement.errors.length > 0 ||
    !cleanup.lease.localClosed ||
    cleanup.lease.localErrors.length > 0 ||
    cleanup.lease.unresolvedPublications.length > 0 ||
    cleanup.lease.remote.error !== undefined ||
    cleanup.policy.some((policy) => Result.isFailure(policy.result))
  )
    return undefined;
  // A failed acquisition may already have a physical ID without ever creating
  // accounting or affinity owners. Its caller explicitly records not-applicable.
  const lease = cleanup.lease;
  switch (lease.allocation) {
    case "none":
      return noTermination(lease.remote) ? "noAllocation" : undefined;
    case "unknown":
      return undefined;
    case "known":
      if (lease.sessionId === undefined || lease.sessionId.length === 0) return undefined;
      if (lease.ownership === "attached")
        return noTermination(lease.remote) ? "attachedDetached" : undefined;
      if (
        lease.ownership === "owned" &&
        lease.remote.confirmed &&
        (lease.remote.evidence === "absent" || lease.remote.evidence === "terminal")
      )
        return "ownedTerminated";
      return undefined;
  }
};

/**
 * Reservations precede allocation so even a failed acquisition has room for
 * its final evidence. Completed failures never compete with still-owned sources
 * for that room. All transitions are synchronous; no user work runs here.
 */
export const make = (options: Options = {}) =>
  parsed(() => {
    const successfulLimit =
      positiveLimit(
        (options.retainedSuccessfulCleanups ?? 64) + 1,
        "retainedSuccessfulCleanups + 1",
        4097,
      ) - 1;
    const unresolvedLimit = positiveLimit(
      options.maxUnresolvedCleanups ?? 16,
      "maxUnresolvedCleanups",
      4096,
    );
    if (unresolvedLimit < 2)
      throw ReactorError.fromCode("InvalidInput", "maxUnresolvedCleanups must be at least two");
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
        if (sessionId.length === 0 || sessionId.length > 1024)
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
        const kind = completeKind(attempt, retirement);
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
    return { reserve, identify, record, finish, summary };
  });
