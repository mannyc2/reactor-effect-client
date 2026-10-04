/**
 * H3 over one Reactor session as a playout `Source`: `open` mints a token,
 * allocates, lets a supervisor record the owner, connects and sets the
 * session up; `resume` adopts a session a dead owner recorded. Either keeps
 * the session's token fresh with tokens bound to it. Both make sure of
 * every session default the playout depends on, since H3's defaults are not
 * what a playout wants: autoplay is off until the playout turns it on,
 * `flush_on_clip_end` is set as asked when the session has it otherwise, and
 * the canvas is set before the first enqueue, the only moment H3 accepts it.
 */
import type * as Crypto from "effect/Crypto";
import type * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import type * as Scope from "effect/Scope";
import type { TokenGrant, Tokens } from "./CoordinatorClient.js";
import * as H3 from "./H3.js";
import type { ClipModel, Source } from "./Playout.js";
import { clipModel } from "./internal/h3/clipModel.js";
import * as Source_ from "./internal/h3/source.js";
import type { Reactor, CreateOptions } from "./Reactor.js";
import type { AcquisitionFailure } from "./ReactorError.js";
import type { Session } from "./Session.js";

/** H3 Reference Turbo Realtime as the playout plans for it: its documented request limits. */
export const model: ClipModel<H3.Request> = clipModel;

/**
 * A durable owner record of an allocated session, without a token: enough to
 * find it again, adopt it with a token bound to it, or terminate it with one
 * or with the API key, stored as the application decides.
 */
export const Allocation = Schema.Struct({
  sessionId: Schema.String,
  ownership: Schema.Literals(["owned", "attached"]),
  model: Schema.String,
  /**
   * The end of the session's cap counted from the allocation request, in
   * seconds since the epoch; absent for a session without a cap, which runs
   * until it is terminated. The server starts the cap no earlier than the
   * request, so the cap cannot end the session before this: plan to be done
   * by it, and read the session before counting it ended.
   */
  endsAt: Schema.optionalKey(Schema.Finite),
});
export type Allocation = typeof Allocation.Type;

/** A session just allocated, before it connects, and the token that created it. */
export interface Allocated {
  readonly session: Session;
  readonly grant: TokenGrant;
  readonly allocation: Allocation;
}

export interface Options {
  /** Set before the first enqueue; left as the session has it when absent. */
  readonly canvas?: H3.CanvasAspect | undefined;
  /** Keep the last frame between clips instead of flushing to black; true by default. */
  readonly holdLastFrame?: boolean | undefined;
  readonly provider?: H3.Options | undefined;
  /**
   * How long the source waits, from a drop, for its session to reconnect and H3 to be read again
   * before it counts itself lost; 20 seconds by default. The session reconnects itself: a session
   * that does not (`Reactor.Options.reconnect` is `false`, or content moderation ended it), or has
   * stopped trying, loses the source at once, with no `Reconnecting`.
   */
  readonly recovery?: Duration.Input | undefined;
}

export interface OpenOptions<E = never, R = never> extends Options {
  /**
   * The session's tokens; the API key never reaches the opener. The `create`
   * token's cap bounds the session's lifetime: without one the source never
   * expires, and the playout renews it only when it is lost.
   */
  readonly tokens: Tokens;
  /**
   * Runs after allocation and before connect, so a supervisor can record the
   * owner first. Its failure closes the session and fails `open` as
   * `Reactor.create`'s `onAllocated` does: an `AcquisitionFailure` with the
   * close's report, and an error other than the client's own as the `detail` of
   * an `Aborted` one.
   */
  readonly onAllocated?: ((allocated: Allocated) => Effect.Effect<void, E, R>) | undefined;
  readonly create?: Omit<CreateOptions, "model" | "tokens" | "onAllocated"> | undefined;
}

export interface ResumeOptions extends Omit<Options, "canvas"> {
  readonly allocation: Allocation;
  /** Tokens bound to the recorded session: H3 accepts no other for a session it did not create. */
  readonly tokens: Pick<Tokens, "bind">;
}

const h3: Source_.SourceModel<H3.Request, H3.Clip> = {
  model: clipModel,
  modelName: H3.modelName,
  spanPrefix: "H3Source",
  tracks: H3.h3ReferenceTurboRealtime.tracks,
  provider: H3.make,
  allocationMismatch: "the allocation is not an H3 session",
  continuable: (snapshot) =>
    snapshot.clips
      .filter((entry) => entry.clip.ready && entry.lifecycle !== "clip_failed")
      .sort((a, b) =>
        a.source.sequence < b.source.sequence ? -1 : a.source.sequence > b.source.sequence ? 1 : 0,
      )
      .slice(-H3.h3ReferenceTurboRealtime.continuationWindow)
      .map((entry) => entry.clip.clip_id),
  enqueue: (provider, request, caller, continueFrom) =>
    provider
      .enqueue({
        ...request,
        metadata: caller,
        ...(continueFrom === undefined ? {} : { continueFrom }),
      })
      .pipe(Effect.map((acceptance) => acceptance.clip.clip_id)),
};

/**
 * Opens a paid H3 session as a playout source: mint its token, allocate it, run
 * `onAllocated`, connect, and set it up. Its lifetime is what remains, when it
 * returns, of the token's session cap counted from the allocation request, or
 * unending without a cap. Any failure after allocation, `onAllocated`'s
 * included, closes the session and fails with an `AcquisitionFailure` carrying
 * the close's report.
 * `Playout.make({ open: H3Source.open({ tokens }), ... })`.
 */
export const open = <E = never, R = never>(
  options: OpenOptions<E, R>,
): Effect.Effect<Source, AcquisitionFailure, R | Reactor | Crypto.Crypto | Scope.Scope> =>
  Source_.open(h3)(options);

/**
 * Adopts a session `open` allocated, from its owner record and a token bound
 * to it, after its owner died: this process then owns its remote lifetime. It
 * keeps the session's canvas, queue and playback; its lifetime is what remains,
 * when it returns, until the record's `endsAt`, if it has one. A resume that
 * fails ends the session it adopted, as a failed adopting attach does, one
 * whose first connection drops as it becomes ready included.
 */
export const resume = (
  options: ResumeOptions,
): Effect.Effect<Source, AcquisitionFailure, Reactor | Crypto.Crypto | Scope.Scope> =>
  Source_.resume(h3)(options);
