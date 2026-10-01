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
import * as Cause from "effect/Cause";
import * as Clock from "effect/Clock";
import type * as Crypto from "effect/Crypto";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Option from "effect/Option";
import * as Predicate from "effect/Predicate";
import * as PubSub from "effect/PubSub";
import * as Redacted from "effect/Redacted";
import * as Ref from "effect/Ref";
import * as Schema from "effect/Schema";
import type * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import * as SubscriptionRef from "effect/SubscriptionRef";
import type { TokenGrant, Tokens } from "./CoordinatorClient.js";
import * as H3 from "./H3.js";
import type { DecodedMedia, MediaPressure } from "./Media.js";
import type { Source, SourceClip, SourceEvent, SourceState } from "./Playout.js";
import * as Tag from "./internal/playout/tag.js";
import { currentParent, spanOptions } from "./internal/trace.js";
import { noAcquisition, Reactor } from "./Reactor.js";
import type { CreateOptions } from "./Reactor.js";
import { AcquisitionFailure, CommandFailure, ReactorError } from "./ReactorError.js";
import type { Session, Snapshot } from "./Session.js";

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

const clipOf = (clip: (typeof H3.Clip)["Type"]): SourceClip => ({
  clipId: clip.clip_id,
  tag: Tag.decode(clip.metadata),
  seconds: clip.seconds,
});

const factsOf = (snapshot: H3.ProviderSnapshot) =>
  snapshot._tag === "Ready" ? { state: snapshot.state, queue: snapshot.queue } : snapshot.lastFacts;

/** The clip H3 reports playing, or armed to play. */
const playingOf = (snapshot: H3.ProviderSnapshot): string | undefined =>
  factsOf(snapshot)?.state.playing_clip_id ?? undefined;

const stateOf = (snapshot: H3.ProviderSnapshot): SourceState => {
  const facts = factsOf(snapshot);
  const known = new Map(snapshot.clips.map((entry) => [entry.clip.clip_id, entry.clip]));
  const playingId = facts?.state.playing_clip_id ?? null;
  const playing = playingId === null ? undefined : known.get(playingId);
  return {
    available: snapshot._tag === "Ready",
    building: (facts?.queue.generation ?? []).map(clipOf),
    ready: (facts?.queue.playout ?? []).map(clipOf),
    // H3 keeps no history: a clip that started before this provider attached is known by its
    // id alone until its end message brings its metadata and length.
    playing:
      playingId === null
        ? undefined
        : playing === undefined
          ? { clipId: playingId, tag: undefined, seconds: undefined }
          : clipOf(playing),
    continuable: snapshot.clips
      .filter((entry) => entry.clip.ready && entry.lifecycle !== "clip_failed")
      .sort((a, b) =>
        a.source.sequence < b.source.sequence ? -1 : a.source.sequence > b.source.sequence ? 1 : 0,
      )
      .slice(-H3.h3ReferenceTurboRealtime.continuationWindow)
      .map((entry) => entry.clip.clip_id),
  };
};

/**
 * Sends each generation's decoded track, following the session across
 * reconnects. A reader that falls behind its bound fails with `Overflow` and
 * misses the rest, so it reads again from the next frame: the picture goes on,
 * the host counts the loss in `readerOverflows`, and `overflowed` reports it
 * with the pressure that follows.
 */
const track = <A>(
  session: Session,
  read: (media: DecodedMedia) => Stream.Stream<A, ReactorError>,
  overflowed: (pressure: MediaPressure) => Effect.Effect<void>,
): Stream.Stream<A, ReactorError> =>
  session.changes.pipe(
    Stream.filter((snapshot) => snapshot.status === "ready"),
    Stream.map((snapshot) => snapshot.generation),
    Stream.changes,
    Stream.switchMap((generation) => {
      const frames: Stream.Stream<A, ReactorError> = Stream.unwrap(
        Effect.map(session.decoded, (media) =>
          read(media).pipe(
            Stream.catchIf(
              (error) => error.reason._tag === "Overflow",
              () =>
                Stream.concat(
                  Stream.fromEffectDrain(Effect.flatMap(media.pressure, overflowed)),
                  frames,
                ),
            ),
          ),
        ),
      );
      // A retired generation's failure ends its frames until the next is ready. A generation
      // retires before its media fails for it, so a failure while it is still the ready one is
      // the host's or the reader's, such as a host without decoded media, and fails the track.
      // A defect stays one.
      return frames.pipe(
        Stream.catch((error) =>
          Stream.unwrap(
            session.ready.pipe(
              Effect.option,
              Effect.map((ready) =>
                Option.isSome(ready) && ready.value.generation === generation
                  ? Stream.fail(error)
                  : Stream.empty,
              ),
            ),
          ),
        ),
      );
    }),
  );

/** A drop the source came back from: the generation it came back on, and how long that took. */
interface Recovered {
  readonly to: bigint;
  readonly afterMillis: number;
}

/** The source's recoveries, in order, and why it stopped recovering once it has. */
interface Recoveries {
  readonly recovered: ReadonlyArray<Recovered>;
  readonly lost: Cause.Cause<ReactorError> | undefined;
}

/** The session's connection is down and may come back: it dropped, or a new one is on its way. */
const down = (snapshot: Snapshot): boolean =>
  snapshot.status === "connecting" ||
  snapshot.status === "waiting" ||
  snapshot.status === "disconnected";

/** Why a session whose connection dropped will not be ready again: it closes, or stopped trying. */
const unrecoverable = (snapshot: Snapshot): ReactorError | undefined => {
  switch (snapshot.status) {
    case "closing":
    case "closed":
      return ReactorError.fromCode("Closed", "the session closed");
    case "disconnected":
      if (snapshot.reconnecting) return undefined;
      return snapshot.lastError ?? ReactorError.fromCode("Disconnected", "the session dropped");
    default:
      return undefined;
  }
};

/** The first generation a session whose connection is down may be ready on again. */
const readyFrom = (snapshot: Snapshot): bigint =>
  snapshot.status === "connecting" || snapshot.status === "waiting"
    ? snapshot.generation
    : snapshot.generation + 1n;

/** What a reader waiting on a recovery from `from` on can know yet: that recovery, or the loss. */
const outcomeFrom =
  (from: bigint) =>
  (held: Recoveries): Exit.Exit<Recovered, ReactorError> | undefined => {
    const recovered = held.recovered.find((each) => each.to >= from);
    if (recovered !== undefined) return Exit.succeed(recovered);
    return held.lost === undefined ? undefined : Exit.failCause(held.lost);
  };

/** A connected session as a playout source, all but its lifetime. */
const fromSession = Effect.fnUntraced(function* (
  session: Session,
  options: Options & { readonly resumed: boolean },
) {
  const acquisition = yield* currentParent;
  const provider = yield* H3.make(session, options.provider);
  // A resumed session usually has the setting its owner gave it, so resuming only reads.
  const flush = !(options.holdLastFrame ?? true);
  if (factsOf(yield* provider.snapshot)?.state.flush_on_clip_end !== flush)
    yield* provider.setFlushOnClipEnd(flush);
  if (options.canvas !== undefined && !options.resumed) yield* provider.setCanvas(options.canvas);
  // `seconds_sent` is the session's running total, so a clip's air is the difference from its start.
  const sent = yield* Ref.make<{
    readonly total: number;
    readonly starts: ReadonlyMap<string, number>;
  }>({ total: 0, starts: new Map() });
  const recovery = Duration.fromInputUnsafe(options.recovery ?? "20 seconds");
  // The session reconnects a dropped connection itself, and the source follows it once, however
  // many read its events: it gives the session `recovery` to be ready again, then reads H3's
  // facts afresh. Every reader reports that one recovery where it sees the drop, in order, and
  // fails there once the source has stopped recovering.
  const recoveries = yield* SubscriptionRef.make<Recoveries>({ recovered: [], lost: undefined });
  /** The first snapshot of the session `accept` takes, now or once it changes. */
  const first = (accept: (snapshot: Snapshot) => boolean) =>
    session.changes.pipe(Stream.filter(accept), Stream.runHead, Effect.map(Option.getOrThrow));
  /**
   * Waits for a connection ready after the one on `up`, then reads H3 on it; its generation. A
   * connection that drops under the read is not back yet, so the source waits for the next.
   */
  const readBack = (up: bigint | undefined): Effect.Effect<bigint, ReactorError | CommandFailure> =>
    Effect.gen(function* () {
      const back = yield* first(
        (snapshot) =>
          (snapshot.status === "ready" && snapshot.generation !== up) ||
          unrecoverable(snapshot) !== undefined,
      );
      const why = unrecoverable(back);
      if (why !== undefined) return yield* why;
      const read = yield* Effect.result(provider.refresh);
      if (read._tag === "Success") return back.generation;
      // A connection retires before a read on it fails for its drop: a read that failed while its
      // connection is still the ready one failed for another reason.
      const ready = yield* Effect.option(session.ready);
      if (Option.isSome(ready) && ready.value.generation === back.generation)
        return yield* read.failure;
      return yield* readBack(back.generation);
    });
  /** The recovery from a drop of the connection on `up`, within `recovery`; how long it took. */
  const recover = (up: bigint | undefined) =>
    readBack(up).pipe(
      Effect.timeoutOrElse({
        duration: recovery,
        orElse: () => Effect.fail(ReactorError.fromCode("Timeout", "reconnecting took too long")),
      }),
      Effect.mapError((error) =>
        CommandFailure.is(error)
          ? ReactorError.make({ reason: error.reason, context: error.context })
          : error,
      ),
      Effect.timed,
      Effect.withSpan(
        "H3Source.recover",
        {
          ...spanOptions({ acquisition }),
          attributes: { "reactor.session.id": session.id },
        },
        { captureStackTrace: false },
      ),
    );
  // Set up on a ready connection, unless it dropped meanwhile.
  const start = yield* session.snapshot;
  yield* Effect.gen(function* () {
    const up =
      (yield* SubscriptionRef.get(recoveries)).recovered.at(-1)?.to ??
      (start.status === "ready" ? start.generation : undefined);
    // A close is no drop: the source ends with its session.
    if (up !== undefined) yield* first((snapshot) => down(snapshot) || snapshot.generation !== up);
    const [took, to] = yield* recover(up);
    yield* SubscriptionRef.update(recoveries, (held) => ({
      ...held,
      recovered: [...held.recovered, { to, afterMillis: Duration.toMillis(took) }],
    }));
  }).pipe(
    Effect.forever,
    Effect.onError((cause) =>
      Cause.hasInterruptsOnly(cause)
        ? Effect.void
        : SubscriptionRef.update(recoveries, (held) => ({ ...held, lost: cause })),
    ),
    Effect.forkScoped,
  );
  /** The recovery that ended on `from` or later, once there is one, or why the source was lost. */
  const recoveredFrom = (from: bigint): Effect.Effect<Recovered, ReactorError> =>
    SubscriptionRef.changes(recoveries).pipe(
      Stream.map(outcomeFrom(from)),
      Stream.filter(Predicate.isNotUndefined),
      Stream.runHead,
      Effect.flatMap(Option.getOrThrow),
    );
  /**
   * A reader's report of the recovery from `from` on; `past` keeps where it ended. A recovery the
   * session cannot make is the source's loss alone: no `Reconnecting` comes before it.
   */
  const reconnect = (
    from: bigint,
    past: Ref.Ref<bigint>,
  ): Stream.Stream<SourceEvent, ReactorError> =>
    Stream.unwrap(
      Effect.gen(function* () {
        const outcome = Stream.fromIterableEffect(
          Effect.gen(function* () {
            const recovered = yield* recoveredFrom(from);
            yield* Ref.set(past, recovered.to);
            return [
              { _tag: "Reconnected", afterMillis: recovered.afterMillis },
              { _tag: "State", state: stateOf(yield* provider.snapshot) },
            ] satisfies ReadonlyArray<SourceEvent>;
          }),
        );
        // A reader can see the drop after the source has recovered from it.
        const held = yield* SubscriptionRef.get(recoveries);
        const lasting =
          !held.recovered.some((each) => each.to >= from) &&
          unrecoverable(yield* session.snapshot) !== undefined;
        return lasting
          ? outcome
          : Stream.concat(Stream.succeed<SourceEvent>({ _tag: "Reconnecting" }), outcome);
      }),
    );
  /** A reader's report of a drop of `generation`, unless a recovery it reported came back past it. */
  const dropped = (generation: bigint, past: Ref.Ref<bigint>) =>
    past.pipe(
      Ref.get,
      Effect.map((seen) => (generation < seen ? Stream.empty : reconnect(generation + 1n, past))),
      Stream.unwrap,
    );
  const translate = (
    event: H3.ProviderEvent,
  ): Effect.Effect<ReadonlyArray<SourceEvent>, ReactorError> =>
    Effect.gen(function* () {
      if (event._tag === "Session" && event.source._tag === "Moderation")
        return [
          { _tag: "Moderated", action: event.source.action, categories: event.source.categories },
        ];
      if (event._tag !== "Message" || event.disposition !== "applied") return [];
      const message = event.message;
      const state: SourceEvent = { _tag: "State", state: stateOf(yield* provider.snapshot) };
      switch (message.type) {
        case "state_update":
          yield* Ref.update(sent, (value) => ({ ...value, total: message.data.seconds_sent }));
          return [state];
        case "clip_started": {
          yield* Ref.update(sent, (value) => ({
            ...value,
            starts: new Map(value.starts).set(message.data.clip.clip_id, value.total),
          }));
          return [{ _tag: "Started", clip: clipOf(message.data.clip) }, state];
        }
        case "clip_finished":
        case "clip_stopped": {
          const clip = message.data.clip;
          const start = (yield* Ref.get(sent)).starts.get(clip.clip_id);
          yield* Ref.update(sent, (value) => ({ ...value, total: message.data.seconds_sent }));
          return [
            {
              _tag: "Ended",
              clip: clipOf(clip),
              termination: message.type === "clip_finished" ? "finished" : "stopped",
              airedSeconds:
                start === undefined ? undefined : Math.max(0, message.data.seconds_sent - start),
            },
            state,
          ];
        }
        case "clip_failed":
          return [
            {
              _tag: "Failed",
              clip: clipOf(message.data.clip),
              message: "H3 failed the clip",
              provider: Redacted.make(message.data.reason),
            },
            state,
          ];
        default:
          return [state];
      }
    });
  // Overflow reports never hold up a reader: a slow consumer of them loses the oldest.
  const overflows = yield* PubSub.sliding<SourceEvent>(64);
  const overflowed = (track: "video" | "audio") => (pressure: MediaPressure) =>
    Effect.asVoid(PubSub.publish(overflows, { _tag: "ReaderOverflow", track, pressure }));
  const events: Stream.Stream<SourceEvent, ReactorError> = Stream.unwrap(
    Effect.gen(function* () {
      const observation = yield* provider.observe({ capacity: 1024 });
      const initial = Stream.succeed<SourceEvent>({
        _tag: "State",
        state: stateOf(observation.initial),
      });
      // A reader that comes after the source was lost learns it at once.
      const lost = (yield* SubscriptionRef.get(recoveries)).lost;
      if (lost !== undefined) return Stream.concat(initial, Stream.failCause(lost));
      const past = yield* Ref.make(0n);
      // A reader that comes while the connection is down reports the recovery under way first.
      const now = yield* session.snapshot;
      const underway = down(now) ? reconnect(readyFrom(now), past) : Stream.empty;
      return initial.pipe(
        Stream.concat(underway),
        Stream.concat(
          observation.events.pipe(
            Stream.flatMap((event) =>
              event._tag === "Session" &&
              event.source._tag === "Status" &&
              event.source.status === "disconnected"
                ? dropped(event.source.generation, past)
                : Stream.fromIterableEffect(translate(event)),
            ),
          ),
        ),
      );
    }),
  ).pipe(Stream.merge(Stream.fromPubSub(overflows), { haltStrategy: "left" }));
  const replied = (error: CommandFailure) => error.context.outcome === "replied";
  const replyTimeout = Duration.fromInputUnsafe(options.provider?.replyTimeout ?? "15 seconds");
  /**
   * H3 answers `stop` before the clip ends; until H3 reports that, the clip
   * plays and `play` would be refused.
   */
  const landed = (clipId: string, stop: H3.ControlResult) =>
    provider.changes.pipe(
      Stream.filter((snapshot) => playingOf(snapshot) !== clipId),
      Stream.runHead,
      Effect.timeoutOrElse({
        duration: replyTimeout,
        orElse: () =>
          Effect.fail(
            CommandFailure.from(
              ReactorError.fromCode("Timeout", "H3 did not report the stopped clip ended"),
              {
                operation: "stop",
                outcome: "unknown",
                requestId: stop.source.requestId,
                generation: stop.source.generation,
              },
            ),
          ),
      }),
    );
  return {
    sessionId: session.id,
    events,
    enqueue: (request, tag, continueFrom) =>
      Effect.gen(function* () {
        const caller = Tag.encode({ tag, metadata: request.metadata });
        if (caller === undefined)
          return yield* CommandFailure.from(
            ReactorError.fromCode("InvalidInput", "clip metadata could not be encoded"),
            { operation: "enqueue", outcome: "not-submitted" },
          );
        const acceptance = yield* provider.enqueue({
          ...request,
          metadata: caller,
          ...(continueFrom === undefined ? {} : { continueFrom }),
        });
        return acceptance.clip.clip_id;
      }),
    remove: (clipId) => Effect.asVoid(provider.pop(clipId)),
    move: (clipId, position) => Effect.asVoid(provider.move(clipId, position)),
    setAutoplay: (enabled) => Effect.asVoid(provider.setAutoplay(enabled)),
    // With autoplay off nothing starts after the clip, so H3's stop, which names no clip, can
    // only hit it. It may have ended on its own first: a refusal because nothing plays is
    // harmless, and a clip that took its place is not stopped.
    stop: (clipId) =>
      Effect.gen(function* () {
        if (playingOf(yield* provider.snapshot) !== clipId) return;
        yield* provider.stop.pipe(
          Effect.flatMap((stopped) => landed(clipId, stopped)),
          Effect.catchIf(replied, () => Effect.void),
        );
      }),
    play: (clipId) => Effect.asVoid(provider.play(clipId)),
    video: track(
      session,
      (media) => media.video(H3.h3ReferenceTurboRealtime.tracks.video),
      overflowed("video"),
    ),
    audio: track(
      session,
      (media) => media.audio(H3.h3ReferenceTurboRealtime.tracks.audio),
      overflowed("audio"),
    ),
    close: session.close,
  } satisfies Omit<Source, "lifetime">;
});

const failAcquisition = (session: Session) => (error: ReactorError | CommandFailure) =>
  Effect.flatMap(session.close, (report) => Effect.fail(AcquisitionFailure.from(error, report)));

/** What remains now until `endsAt`, in milliseconds since the epoch; unending without one. */
const lifetimeUntil = (endsAt: number | undefined): Effect.Effect<Duration.Duration> =>
  endsAt === undefined
    ? Effect.succeed(Duration.infinity)
    : Effect.map(Clock.currentTimeMillis, (now) => Duration.millis(Math.max(0, endsAt - now)));

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
  Effect.gen(function* () {
    const reactor = yield* Reactor;
    const grant = yield* options.tokens.create.pipe(
      Effect.mapError((error) => AcquisitionFailure.from(error, noAcquisition)),
    );
    const cap = grant.maxSessionSeconds;
    // The server starts the granted length no earlier than the request, so the cap counted
    // from here ends no later than the session.
    const requested = yield* Clock.currentTimeMillis;
    const endsAt = cap === undefined ? undefined : requested + cap * 1000;
    const onAllocated = options.onAllocated;
    const session = yield* reactor.create({
      ...options.create,
      model: H3.modelName,
      tokens: { create: Effect.succeed(grant), bind: options.tokens.bind },
      ...(onAllocated === undefined
        ? {}
        : {
            onAllocated: (allocated: Session) =>
              onAllocated({
                session: allocated,
                grant,
                allocation: {
                  sessionId: allocated.id,
                  ownership: allocated.ownership,
                  model: H3.modelName,
                  ...(cap === undefined ? {} : { endsAt: requested / 1000 + cap }),
                },
              }),
          }),
    });
    const source = yield* fromSession(session, { ...options, resumed: false }).pipe(
      Effect.catch(failAcquisition(session)),
    );
    return { ...source, lifetime: yield* lifetimeUntil(endsAt) };
  }).pipe(Effect.withSpan("H3Source.open", { kind: "client" }, { captureStackTrace: false }));

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
  Effect.gen(function* () {
    const refuse = (message: string) =>
      AcquisitionFailure.from(
        ReactorError.fromCode("InvalidInput", message, {
          operation: "resume",
          outcome: "not-submitted",
        }),
        noAcquisition,
      );
    const { allocation } = options;
    if (allocation.model !== H3.modelName)
      return yield* refuse("the allocation is not an H3 session");
    const endsAt = allocation.endsAt === undefined ? undefined : allocation.endsAt * 1000;
    if (endsAt !== undefined && !(endsAt > (yield* Clock.currentTimeMillis)))
      return yield* refuse("the allocation's granted length has ended");
    const reactor = yield* Reactor;
    const session = yield* reactor.attach({
      sessionId: allocation.sessionId,
      tokens: options.tokens,
      adopt: true,
    });
    const source = yield* fromSession(session, { ...options, resumed: true }).pipe(
      Effect.catch(failAcquisition(session)),
    );
    return { ...source, lifetime: yield* lifetimeUntil(endsAt) };
  }).pipe(Effect.withSpan("H3Source.resume", { kind: "client" }, { captureStackTrace: false }));
