import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import type * as Scope from "effect/Scope";
import { ClipEnded, CommandFailure, ReactorError } from "../../errors.js";
import type { CommandReply } from "../../session/index.js";
import type { Clip, DecodedMessage, MessageType } from "../messages.js";
import type { Acceptance, ClipFact, ClipOperation, ClipPhase, OperationFacts } from "../types.js";
import type { AcceptanceIdentity } from "./evidence.js";

type Terminal = "clip_finished" | "clip_stopped" | "clip_failed" | "clip_popped";

interface OperationRecord {
  readonly identity: AcceptanceIdentity;
  acceptance: Acceptance | undefined;
  /** The clip the evidence names, from before the acceptance is decided. */
  clipId: string | undefined;
  generated: ClipFact | undefined;
  started: ClipFact | undefined;
  ended: ClipFact | undefined;
  indeterminate: boolean;
  /** Decided without a clip: the enqueue's failure was definite. */
  rejected: boolean;
  holders: number;
  readonly accepted: Deferred.Deferred<Acceptance, ReactorError | CommandFailure>;
  readonly reachedGenerated: Deferred.Deferred<ClipFact, ReactorError | CommandFailure>;
  readonly reachedStarted: Deferred.Deferred<ClipFact, ReactorError | CommandFailure>;
  readonly finished: Deferred.Deferred<ClipFact, ReactorError | CommandFailure>;
}

/** Nothing more can change it: its clip ended once accepted, or it was decided without one. */
const settledRecord = (record: OperationRecord): boolean =>
  (record.ended !== undefined && record.acceptance !== undefined) ||
  record.indeterminate ||
  record.rejected;

const indeterminate = (): ReactorError =>
  ReactorError.fromCode("Indeterminate", "H3 provider retired before the clip's evidence", {
    operation: "clip operation",
  });

/**
 * Clip operations keyed by submission: a retained, bounded projection of the
 * provider's evidence, written only synchronously, on the provider's own paths.
 * Nothing reads it back: it is neither the provider's acceptance authority nor
 * its clip state.
 */
export class Operations {
  private readonly records = new Map<string, OperationRecord>();
  private readonly byClip = new Map<string, OperationRecord>();

  constructor(private readonly capacity: number) {}

  /**
   * Take a slot for a submission about to dispatch. Operations that ended are
   * evicted first, oldest first; a table of unresolved operations refuses.
   */
  reserve(identity: AcceptanceIdentity): void {
    if (this.records.has(identity.id)) return;
    if (this.records.size >= this.capacity) {
      for (const [id, record] of this.records)
        if (settledRecord(record)) {
          this.release(id, record);
          break;
        }
      if (this.records.size >= this.capacity)
        throw ReactorError.fromCode("Overflow", "H3 clip operation bound reached", {
          operation: "enqueue",
          outcome: "not-submitted",
        });
    }
    this.records.set(identity.id, {
      identity,
      acceptance: undefined,
      clipId: undefined,
      generated: undefined,
      started: undefined,
      ended: undefined,
      indeterminate: false,
      rejected: false,
      holders: 0,
      accepted: Deferred.makeUnsafe(),
      reachedGenerated: Deferred.makeUnsafe(),
      reachedStarted: Deferred.makeUnsafe(),
      finished: Deferred.makeUnsafe(),
    });
  }

  /** A commit that failed after reserving sent nothing. */
  abandon(id: string): void {
    const record = this.records.get(id);
    if (record !== undefined) this.release(id, record);
  }

  /** The enqueue's own result: a definite failure decides the operation without a clip. */
  settle(id: string, exit: Exit.Exit<Acceptance, unknown>): void {
    const record = this.records.get(id);
    if (record === undefined || record.acceptance !== undefined || Exit.isSuccess(exit)) return;
    const failure = Exit.findError(exit);
    if (failure._tag !== "Success" || !CommandFailure.is(failure.success)) return;
    if (failure.success.context.outcome === "unknown") return;
    record.rejected = true;
    this.forget(record);
    // No clip will ever carry this submission: every fact fails with the enqueue's
    // failure, and what a clip showed before the refusal is not its fact.
    record.generated = undefined;
    record.started = undefined;
    record.ended = undefined;
    Deferred.doneUnsafe(record.accepted, Effect.fail(failure.success));
    for (const deferred of [record.reachedGenerated, record.reachedStarted, record.finished])
      Deferred.doneUnsafe(deferred, Effect.fail(failure.success));
  }

  /**
   * Evidence the provider holds until the enqueue's reply decides the
   * acceptance: the clip's facts are recorded meanwhile, and resolve once the
   * acceptance does.
   */
  identify(acceptance: Acceptance): void {
    const record = this.records.get(acceptance.submissionId);
    if (record === undefined || record.clipId !== undefined || record.rejected) return;
    record.clipId = acceptance.clip.clip_id;
    this.byClip.set(record.clipId, record);
  }

  /** The acceptance the provider recorded, on the same path that records it. */
  accept(acceptance: Acceptance): void {
    const record = this.records.get(acceptance.submissionId);
    if (record === undefined || record.acceptance !== undefined) return;
    record.acceptance = acceptance;
    this.identify(acceptance);
    Deferred.doneUnsafe(record.accepted, Effect.succeed(acceptance));
    this.publish(record);
  }

  /** Stop following a record's clip. */
  private forget(record: OperationRecord): void {
    if (record.clipId !== undefined && this.byClip.get(record.clipId) === record)
      this.byClip.delete(record.clipId);
  }

  /**
   * A clip carrying a submission's exact metadata and prompt, after its pending
   * acceptance expired or in a later transport generation of the same session.
   * It resolves the operation only; the provider's acceptances are unchanged.
   */
  lateEvidence(id: string, clip: Clip, source: CommandReply): void {
    const record = this.records.get(id);
    if (
      record === undefined ||
      record.acceptance !== undefined ||
      record.rejected ||
      record.identity.metadata !== clip.metadata ||
      record.identity.prompt !== clip.prompt
    )
      return;
    this.accept(
      Object.freeze({
        submissionId: id,
        clip,
        evidence: Object.freeze({ kind: "metadata" as const, source }),
      }),
    );
  }

  /** Advance operations from a message the reducer applied. */
  observe(message: DecodedMessage, source: CommandReply): void {
    if (this.byClip.size === 0 || message.type === "unknown") return;
    if (message.type === "queue_update") {
      for (const clip of message.data.playout)
        if (clip.ready) this.advance(clip.clip_id, "queue_update", source, 1);
      return;
    }
    if (message.type === "state_update") {
      const playing = message.data.playing_clip_id;
      if (playing !== null) this.advance(playing, "state_update", source, 2);
      return;
    }
    if (!("clip" in message.data)) return;
    const clipId = message.data.clip.clip_id;
    switch (message.type) {
      case "clip_generated":
        return this.advance(clipId, message.type, source, 1);
      case "clip_started":
        return this.advance(clipId, message.type, source, 2);
      case "clip_finished":
      case "clip_stopped":
        // A clip that finished or was stopped had started.
        this.advance(clipId, message.type, source, 2);
        return this.end(clipId, message.type, source);
      case "clip_failed":
      case "clip_popped":
        return this.end(clipId, message.type, source);
      default:
        return;
    }
  }

  private fact(clipId: string, message: MessageType, source: CommandReply): ClipFact {
    return Object.freeze({ clipId, message, transportGeneration: source.generation, source });
  }

  private advance(clipId: string, message: MessageType, source: CommandReply, rank: 1 | 2): void {
    const record = this.byClip.get(clipId);
    if (record === undefined || record.ended !== undefined || record.indeterminate) return;
    const fact = this.fact(clipId, message, source);
    // Started implies generated: every implied phase completes with the first evidence.
    record.generated ??= fact;
    if (rank === 2) record.started ??= fact;
    this.publish(record);
  }

  private end(clipId: string, message: Terminal, source: CommandReply): void {
    const record = this.byClip.get(clipId);
    if (record === undefined || record.ended !== undefined || record.indeterminate) return;
    record.ended = this.fact(clipId, message, source);
    this.publish(record);
  }

  /** Resolve what a record established, once its acceptance is decided. */
  private publish(record: OperationRecord): void {
    if (record.acceptance === undefined) return;
    const { generated, started, ended } = record;
    if (generated !== undefined)
      Deferred.doneUnsafe(record.reachedGenerated, Effect.succeed(generated));
    if (started !== undefined) Deferred.doneUnsafe(record.reachedStarted, Effect.succeed(started));
    if (ended === undefined) return;
    if (ended.message === "clip_failed" || ended.message === "clip_popped") {
      const error = new ReactorError({
        reason: new ClipEnded({
          message: `clip ended by ${ended.message}`,
          clipId: ended.clipId,
          lifecycle: ended.message,
          transportGeneration: ended.transportGeneration,
        }),
        context: { operation: "clip operation" },
      });
      // A phase that was not reached never will be.
      if (generated === undefined) Deferred.doneUnsafe(record.reachedGenerated, Effect.fail(error));
      if (started === undefined) Deferred.doneUnsafe(record.reachedStarted, Effect.fail(error));
    }
    Deferred.doneUnsafe(record.finished, Effect.succeed(ended));
  }

  /** The provider retired: what evidence did not decide is Indeterminate. */
  retire(): void {
    for (const [id, record] of this.records) {
      if (!settledRecord(record)) record.indeterminate = true;
      for (const deferred of [record.reachedGenerated, record.reachedStarted, record.finished])
        Deferred.doneUnsafe(deferred, Effect.fail(indeterminate()));
      Deferred.doneUnsafe(record.accepted, Effect.fail(indeterminate()));
      if (record.holders === 0) this.release(id, record);
    }
  }

  private release(id: string, record: OperationRecord): void {
    this.records.delete(id);
    this.forget(record);
  }

  /** A scoped view; releasing the last holder acknowledges and releases the operation. */
  attach(id: string): Effect.Effect<ClipOperation, ReactorError, Scope.Scope> {
    return Effect.acquireRelease(
      Effect.suspend(() => {
        const record = this.records.get(id);
        if (record === undefined)
          return Effect.fail(
            ReactorError.fromCode(
              "InvalidState",
              "H3 has no clip operation for this submission; it has not committed or was released",
              { operation: "clip operation" },
            ),
          );
        record.holders++;
        return Effect.succeed(record);
      }),
      (record) =>
        Effect.sync(() => {
          record.holders--;
          if (record.holders === 0 && this.records.get(id) === record) this.release(id, record);
        }),
    ).pipe(
      Effect.map((record): ClipOperation =>
        Object.freeze({
          submissionId: id,
          accepted: Deferred.await(record.accepted),
          reached: (phase: ClipPhase) =>
            Deferred.await(phase === "generated" ? record.reachedGenerated : record.reachedStarted),
          ended: Deferred.await(record.finished),
          // A clip's facts are the operation's once its acceptance is decided.
          facts: Effect.sync((): OperationFacts =>
            Object.freeze({
              submissionId: id,
              ...(record.acceptance === undefined
                ? {}
                : {
                    acceptance: record.acceptance,
                    ...(record.generated === undefined ? {} : { generated: record.generated }),
                    ...(record.started === undefined ? {} : { started: record.started }),
                    ...(record.ended === undefined ? {} : { ended: record.ended }),
                  }),
              indeterminate: record.indeterminate,
            }),
          ),
        }),
      ),
    );
  }
}
