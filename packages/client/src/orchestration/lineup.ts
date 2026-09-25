import * as Context from "effect/Context";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Queue from "effect/Queue";
import * as Result from "effect/Result";
import type * as Scope from "effect/Scope";
import * as Semaphore from "effect/Semaphore";
import * as Stream from "effect/Stream";
import { ReactorError } from "../errors.js";
import { captureRequest, ClipRequest, PolicyFailure } from "./request.js";
import type { ClipId } from "./request.js";
import { activeIds, generation } from "./routing.js";
import { Engine } from "./types.js";
import type { ClipRecord, EngineError, EngineEvent, EngineState, RemoveOutcome } from "./types.js";

/**
 * How a lineup clip began playing, or why it never will.
 *
 * - `Started`: `at` is when the start was observed, in epoch milliseconds, as
 *   the engine's `Started` event reports it.
 * - `Failed`: it will not play. Its build failed, it was lost with its
 *   session, it was removed with `remove`, or it was still waiting when the
 *   orchestration failed or closed.
 * - `Unobserved`: it may have played, but its start was never observed. The
 *   lineup lost sight of it (after its observer fell behind, the clip was no
 *   longer queued, or was playing with no start time), or saw it end without
 *   seeing it start. There is no `at`: the lineup never invents a start.
 *
 * `Unobserved` was added in 0.4.0, so a switch over `ClipFate` written for
 * 0.3.4 must handle it.
 */
export type ClipFate =
  | { readonly _tag: "Started"; readonly at: number; readonly durationSeconds: number }
  | { readonly _tag: "Failed"; readonly reason: string }
  | { readonly _tag: "Unobserved" };

/** A clip the lineup enqueued. */
export interface LineupClip {
  readonly clipId: ClipId;
  /** Waits until the clip's fate is known: it started, it will not play, or its start went unobserved. */
  readonly fate: Effect.Effect<ClipFate>;
  /**
   * Takes the clip out of its queue before it starts. A removed clip will not
   * play, even one that was already building (`in_flight`): the source
   * discards that build. Its fate is then `Failed` with reason `Removed`.
   */
  readonly remove: Effect.Effect<RemoveOutcome, EngineError>;
}

export interface LineupOptions {
  /** What plays while no clip is waiting. */
  readonly filler: {
    /** Filler clips kept Ready behind the playing clip, from 1 to 1024. */
    readonly ready: number;
    /**
     * The nth filler clip, counting from zero. A lineup asks for each n once, in
     * order, and enqueues that request until it is admitted, so a refusal before
     * it was sent does not skip an n. A new lineup starts again from zero, so
     * keep `clip` pure: the same n may be asked for again.
     */
    readonly clip: (n: number) => ClipRequest;
  };
}

export interface LineupState {
  /** What is playing: a lineup clip, filler, a clip the lineup did not enqueue, or nothing. */
  readonly playing: "clip" | "filler" | "other" | "none";
  /** Filler clips Ready behind the playing clip. */
  readonly readyFiller: number;
  /** How many times playout ran dry since the lineup started. */
  readonly starved: number;
}

export interface LineupShape {
  /**
   * Enqueues a clip behind the clips already waiting and ahead of all waiting
   * filler. The lineup chooses its place, so a request that names its own
   * `position` or `before` is refused. The request is captured before any of
   * its fields is read, as `Engine.enqueue` captures it.
   */
  readonly enqueue: (request: ClipRequest) => Effect.Effect<LineupClip, EngineError>;
  readonly state: Effect.Effect<LineupState>;
}

/** A lineup over the orchestration's `Engine`; `layerLineup` provides it. */
export class Lineup extends Context.Service<Lineup, LineupShape>()(
  "reactor-effect-client/Orchestration/Lineup",
) {}

const logDebug = (message: string, reason: string) =>
  Effect.logDebug(message, { reason }).pipe(
    Effect.annotateLogs({ module: "reactor.orchestration" }),
  );

/**
 * Clips ahead of filler: the clips an application enqueues play in the order
 * it enqueued them, and filler keeps playout from running dry in between.
 *
 * - A clip is enqueued behind the clips already waiting and ahead of every
 *   waiting filler clip. Once Ready it moves directly behind the Ready clips,
 *   so clips overtake filler only, never each other. A clip the lineup did not
 *   enqueue, such as one in a resumed session's queue, counts as a clip.
 * - `filler.ready` filler clips are kept Ready behind the playing clip and
 *   built one at a time, so a clip waits behind one filler build at worst.
 * - Playback is the orchestration's autoplay, which is on unless the
 *   application turns it off: a Ready clip starts when the playing clip ends,
 *   so nothing is cut.
 *
 * The lineup knows its own clips by the requests it captured, so it reads no
 * clip metadata. It reorders and refills after each engine change
 * and at least once a second, which covers a quiet engine such as a
 * replacement session that starts empty. During a renewal's overlap a clip's
 * place counts both sessions' queues, so it may wait behind one more filler
 * build, and it still passes no clip.
 */
export const makeLineup = (
  options: LineupOptions,
): Effect.Effect<LineupShape, ReactorError | PolicyFailure, Engine | Scope.Scope> =>
  Effect.gen(function* () {
    const engine = yield* Engine;
    const target = options.filler.ready;
    if (!Number.isSafeInteger(target) || target < 1 || target > 1024)
      return yield* ReactorError.fromCode(
        "InvalidInput",
        "A lineup keeps from 1 to 1024 filler clips Ready",
      );
    const fillers = new WeakSet<ClipRequest>();
    const clips = new WeakSet<ClipRequest>();
    const fillerAt = (n: number) =>
      captureRequest(options.filler.clip(n)).pipe(
        Effect.tap((request) => Effect.sync(() => fillers.add(request))),
      );
    // The filler to enqueue next, kept until it is admitted, and the n it came
    // from. The first is captured here, so a malformed filler request fails the
    // lineup rather than every refill, and it is the first filler enqueued.
    let fillerIndex = 0;
    let upcoming: ClipRequest | undefined = yield* fillerAt(0);
    // Admitted: enqueued, or sent with an unknown outcome, which the renewal
    // resolves by replacing the session. A refusal keeps the same request.
    const admitted = Effect.sync(() => {
      fillerIndex++;
      upcoming = undefined;
    });
    const isFiller = (record: ClipRecord) =>
      record.request !== undefined && fillers.has(record.request);
    const isClip = (record: ClipRecord) =>
      record.request !== undefined && clips.has(record.request);
    const playing = (state: EngineState): LineupState["playing"] => {
      if (Option.isNone(state.playing)) return "none";
      const record = state.playing.value.record;
      if (Option.isNone(record)) return "other";
      return isFiller(record.value) ? "filler" : isClip(record.value) ? "clip" : "other";
    };

    const permit = yield* Semaphore.make(1);
    const changed = yield* Queue.dropping<void>(1);
    const waiting = new Map<ClipId, Deferred.Deferred<ClipFate>>();
    // Fates observed while an enqueue waits for its reply, before the reply names
    // the clip: a clip can fail before its enqueue returns.
    let unclaimed: Map<ClipId, ClipFate> | undefined;
    let ended: string | undefined;
    let starved = 0;

    const release = (clipId: ClipId, fate: ClipFate): Effect.Effect<boolean> =>
      Effect.suspend(() => {
        const waiter = waiting.get(clipId);
        if (waiter === undefined) return Effect.succeed(false);
        waiting.delete(clipId);
        return Effect.as(Deferred.succeed(waiter, fate), true);
      });
    const settle = (clipId: ClipId, fate: ClipFate) =>
      release(clipId, fate).pipe(
        Effect.tap((released) =>
          Effect.sync(() => {
            if (!released) unclaimed?.set(clipId, fate);
          }),
        ),
      );
    const settleAll = (reason: string): Effect.Effect<void> =>
      Effect.suspend(() => {
        ended = reason;
        const waiters = [...waiting.values()];
        waiting.clear();
        return Effect.forEach(
          waiters,
          (waiter) => Deferred.succeed(waiter, { _tag: "Failed", reason }),
          { discard: true },
        );
      });
    yield* Effect.addFinalizer(() => settleAll("The lineup closed"));

    // Ready clips take the front of playout in the order they already have: each
    // moves only forward, and only over filler. The moves come from one reading
    // of the state, which each move leaves true for the clips after it. A move
    // that fails means its clip started or left, and that change wakes the next pass.
    const order = Effect.gen(function* () {
      const ready = (yield* engine.state).ready;
      const ahead = ready.flatMap((record, index) => (isFiller(record) ? [] : [{ record, index }]));
      for (const [rank, clip] of ahead.entries()) {
        if (clip.index === rank) continue;
        const moved = yield* Effect.result(engine.move(clip.record.clipId, rank, "playout"));
        if (Result.isFailure(moved)) return;
      }
    });

    // One filler clip at most waits for generation, enqueued only while fewer
    // than `ready` are Ready, so a clip queues behind one filler build at worst.
    const refill = Effect.gen(function* () {
      const state = yield* engine.state;
      if (state.availability !== "Ready") return;
      if (generation(state).some(isFiller)) return;
      if (state.ready.filter(isFiller).length >= target) return;
      const request = upcoming ?? (yield* fillerAt(fillerIndex));
      upcoming = request;
      yield* engine.enqueue(request).pipe(
        Effect.tap(() => admitted),
        Effect.tapError((failure) =>
          failure.context.outcome === "unknown" ? admitted : Effect.void,
        ),
      );
    });

    const reconcile = permit
      .withPermit(Effect.andThen(order, refill))
      .pipe(Effect.catch((failure) => logDebug("lineup reconcile deferred", failure.reason._tag)));

    const record = (event: EngineEvent): Effect.Effect<void> => {
      switch (event._tag) {
        case "Started":
          return Effect.asVoid(
            settle(event.clipId, {
              _tag: "Started",
              at: event.at,
              durationSeconds: event.durationSeconds,
            }),
          );
        case "Failed":
          return Effect.asVoid(settle(event.clipId, { _tag: "Failed", reason: event.reason }));
        case "Ended":
          // Its start settles a clip first; a clip still waiting here ended
          // without its start being seen. Only a registered waiter is settled,
          // so a start held for an enqueue in flight is never overwritten.
          return Effect.asVoid(release(event.clipId, { _tag: "Unobserved" }));
        case "Starved":
          return Effect.sync(() => {
            starved++;
          });
        case "SessionFailed":
          return settleAll(`The orchestration failed: ${event.failure.message}`);
        default:
          return Effect.void;
      }
    };

    // After an observer falls behind, the fresh state settles what it can. A
    // waiting clip that is playing started: at its observed start time, or
    // unobserved when the state has none. One listed failed failed. On a Ready
    // engine, one in no queue, not playing and not failed left while the lineup
    // could not see it, so it may have played: unobserved. A closed or
    // recovering engine's state proves no such thing, and its clips keep
    // waiting for the close or a later observation. Anything queued keeps waiting.
    const recover = (state: EngineState) =>
      Effect.gen(function* () {
        const playing = Option.getOrUndefined(state.playing);
        const active = new Set(activeIds(state));
        const complete = state.availability === "Ready";
        for (const clipId of [...waiting.keys()]) {
          if (playing?.clipId === clipId)
            yield* settle(
              clipId,
              Option.isSome(playing.startedAt) && Option.isSome(playing.record)
                ? {
                    _tag: "Started",
                    at: playing.startedAt.value,
                    durationSeconds: playing.record.value.durationSeconds,
                  }
                : { _tag: "Unobserved" },
            );
          else if (state.failed.includes(clipId))
            yield* settle(clipId, { _tag: "Failed", reason: "The clip failed unobserved" });
          else if (complete && !active.has(clipId)) yield* settle(clipId, { _tag: "Unobserved" });
        }
      });

    const wake = Effect.sync(() => Queue.offerUnsafe(changed, undefined));
    const observe = Effect.scoped(
      Effect.gen(function* () {
        const observation = yield* engine.observe({ capacity: 4096 });
        yield* recover(observation.initial);
        yield* wake;
        yield* observation.events.pipe(
          Stream.runForEach((event) =>
            Effect.andThen(
              record(event),
              // Queued and Building follow the lineup's own enqueues: nothing to reorder.
              event._tag === "Queued" || event._tag === "Building" ? Effect.void : wake,
            ),
          ),
        );
      }),
    );
    // The events end when the orchestration closes. An observer 4096 events behind
    // observes again from a fresh state; any other failure ends the lineup's view.
    const watch = Effect.gen(function* () {
      for (;;) {
        const observed = yield* Effect.result(observe);
        if (Result.isSuccess(observed)) return yield* settleAll("The orchestration closed");
        if (observed.failure.reason._tag !== "Overflow")
          return yield* settleAll(`The engine's events failed: ${observed.failure.message}`);
        yield* logDebug("lineup observer fell behind", "Overflow");
      }
    });
    yield* Effect.forkScoped(watch);
    // The reconciler never holds up the observer, so a fate settles as soon as it
    // is seen, even while an enqueue holds the permit.
    yield* Effect.forkScoped(
      Effect.gen(function* () {
        while (ended === undefined) {
          yield* Effect.timeoutOption(Queue.take(changed), "1 second");
          yield* reconcile;
        }
      }),
    );

    const remove = (clipId: ClipId) =>
      engine
        .remove(clipId)
        .pipe(Effect.tap(() => release(clipId, { _tag: "Failed", reason: "Removed" })));

    const enqueue: LineupShape["enqueue"] = (input) =>
      permit.withPermit(
        Effect.gen(function* () {
          // Captured before any field is read, so a caller's accessor never runs
          // and a malformed request is refused as InvalidRequest.
          const captured = yield* captureRequest(input);
          if (captured.position !== undefined || captured.before !== undefined)
            return yield* PolicyFailure.refuse(
              "InvalidRequest",
              "A lineup chooses where its clips go",
              "enqueue",
            );
          const last = generation(yield* engine.state).findLastIndex((record) => !isFiller(record));
          const request = yield* captureRequest(
            new ClipRequest({ ...captured, position: last + 1 }),
          );
          clips.add(request);
          const fate = yield* Deferred.make<ClipFate>();
          const seen = new Map<ClipId, ClipFate>();
          unclaimed = seen;
          const clipId = yield* engine.enqueue(request).pipe(
            Effect.onExit((exit) =>
              Exit.isFailure(exit)
                ? Effect.sync(() => {
                    unclaimed = undefined;
                  })
                : Effect.void,
            ),
          );
          // From here to registering the waiter nothing yields, so no fate is missed.
          const early: ClipFate | undefined =
            seen.get(clipId) ??
            (ended === undefined ? undefined : { _tag: "Failed", reason: ended });
          unclaimed = undefined;
          if (early === undefined) waiting.set(clipId, fate);
          else yield* Deferred.succeed(fate, early);
          return { clipId, fate: Deferred.await(fate), remove: remove(clipId) };
        }),
      );

    return {
      enqueue,
      state: Effect.map(engine.state, (state): LineupState => ({
        playing: playing(state),
        readyFiller: state.ready.filter(isFiller).length,
        starved,
      })),
    };
  });

/** The lineup as a service, over the `Engine` in context. */
export const layerLineup = (
  options: LineupOptions,
): Layer.Layer<Lineup, ReactorError | PolicyFailure, Engine> =>
  Layer.effect(Lineup, makeLineup(options));
