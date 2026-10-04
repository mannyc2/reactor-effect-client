/**
 * FastH3 over one Reactor session as a playout `Source`: `open` mints a token,
 * allocates, lets a supervisor record the owner, connects and sets the
 * session up; `resume` adopts a session a dead owner recorded. Either keeps
 * the session's token fresh with tokens bound to it. Autoplay stays off until
 * the playout turns it on, `flush_on_clip_end` is set as asked when the session
 * has it otherwise, and the canvas is set before the first enqueue.
 */
import type * as Crypto from "effect/Crypto";
import type * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import type * as Scope from "effect/Scope";
import type { Tokens } from "./CoordinatorClient.js";
import * as FastH3 from "./FastH3.js";
import type { Allocation, Allocated } from "./H3Source.js";
import type { ClipModel, Source } from "./Playout.js";
import type { Reactor, CreateOptions } from "./Reactor.js";
import type { AcquisitionFailure, CommandFailure } from "./ReactorError.js";
import { clipModel } from "./internal/fastH3/clipModel.js";
import * as Source_ from "./internal/h3/source.js";

export { Allocation } from "./H3Source.js";
export type { Allocated } from "./H3Source.js";

/** FastH3 as the playout plans for it: its documented request limits. */
export const model: ClipModel<FastH3.Request> = clipModel;

export interface Options {
  /** Set before the first enqueue; left as the session has it when absent. */
  readonly canvas?: FastH3.CanvasAspect | undefined;
  /** Keep the last frame between clips instead of flushing to black; true by default. */
  readonly holdLastFrame?: boolean | undefined;
  readonly provider?: FastH3.Options | undefined;
  /**
   * Wait for the session to reconnect and FastH3 to be read again, 20 seconds by default.
   * A session that cannot reconnect or has stopped trying loses the source at once.
   */
  readonly recovery?: Duration.Input | undefined;
}

export interface OpenOptions<E = never, R = never> extends Options {
  /**
   * The session's tokens; the create token's cap bounds its lifetime. Without
   * a cap the source never expires, and the playout renews it only when lost.
   */
  readonly tokens: Tokens;
  /**
   * Record the owner after allocation and before connect. A failure closes the
   * session and fails open with an AcquisitionFailure carrying the close report.
   */
  readonly onAllocated?: ((allocated: Allocated) => Effect.Effect<void, E, R>) | undefined;
  readonly create?: Omit<CreateOptions, "model" | "tokens" | "onAllocated"> | undefined;
}

export interface ResumeOptions extends Omit<Options, "canvas"> {
  readonly allocation: Allocation;
  /** Tokens bound to the recorded session, which this process now owns. */
  readonly tokens: Pick<Tokens, "bind">;
}

const continuable = (snapshot: FastH3.ProviderSnapshot): ReadonlyArray<string> => {
  const facts =
    snapshot._tag === "Ready"
      ? { state: snapshot.state, queue: snapshot.queue }
      : snapshot.lastFacts;
  if (facts === null) return [];
  const failed = new Set(
    snapshot.clips
      .filter((entry) => entry.lifecycle === "clip_failed")
      .map((entry) => entry.clip.clip_id),
  );
  const generating = new Set(facts.queue.generation.map((clip) => clip.clip_id));
  const playing = snapshot.clips.find(
    (entry) => entry.clip.clip_id === facts.state.playing_clip_id,
  )?.clip;
  // FastH3 continues from what its lists hold; the playout continues only from built clips.
  return [
    ...(playing === undefined ? [] : [playing]),
    ...facts.queue.playout,
    ...facts.queue.history,
  ]
    .filter((clip) => clip.ready && !failed.has(clip.clip_id) && !generating.has(clip.clip_id))
    .map((clip) => clip.clip_id);
};

const enqueue = Effect.fnUntraced(function* (
  provider: Source_.SourceProvider<FastH3.Request, FastH3.Clip>,
  request: FastH3.Request,
  caller: string,
  continueFrom: string | undefined,
): Effect.fn.Return<string, CommandFailure> {
  let prepared: FastH3.Request = { ...request, metadata: caller };
  if (request.start === undefined && continueFrom !== undefined) {
    if (continuable(yield* provider.snapshot).includes(continueFrom))
      prepared = { ...prepared, start: { continueFrom } };
    else yield* Effect.annotateCurrentSpan("reactor.continuation", "dropped");
  }
  const acceptance = yield* provider.enqueue(prepared);
  return acceptance.clip.clip_id;
});

const fastH3: Source_.SourceModel<FastH3.Request, FastH3.Clip> = {
  model: clipModel,
  modelName: FastH3.modelName,
  spanPrefix: "FastH3Source",
  tracks: FastH3.tracks,
  provider: FastH3.make,
  allocationMismatch: "the allocation is not a FastH3 session",
  continuable,
  enqueue,
};

/**
 * Open a paid FastH3 session, record its owner, connect and set it up. Its
 * lifetime is the remainder of the token's session cap, or unending without
 * one. Failures after allocation close the session and carry the close report.
 * `Playout.make({ model, open: FastH3Source.open({ tokens }), ... })`.
 */
export const open = <E = never, R = never>(
  options: OpenOptions<E, R>,
): Effect.Effect<
  Source<FastH3.Request>,
  AcquisitionFailure,
  R | Reactor | Crypto.Crypto | Scope.Scope
> => Source_.open(fastH3)(options);

/**
 * Adopt a recorded FastH3 session with a token bound to it, preserving its
 * canvas, queue and playback. The record's endsAt bounds its remaining lifetime;
 * a failed resume ends the session this process adopted.
 */
export const resume = (
  options: ResumeOptions,
): Effect.Effect<
  Source<FastH3.Request>,
  AcquisitionFailure,
  Reactor | Crypto.Crypto | Scope.Scope
> => Source_.resume(fastH3)(options);
