/**
 * H3 over one Reactor session as a playout `Source`: `open` mints a token,
 * allocates, lets a supervisor record the owner, connects and sets the
 * session up; `resume` adopts a session a dead owner recorded. Both make sure of
 * every session default the playout depends on, since H3's defaults are not
 * what a playout wants: autoplay is off until the playout turns it on,
 * `flush_on_clip_end` is set as asked when the session has it otherwise, and
 * the canvas is set before the first enqueue, the only moment H3 accepts it.
 */
import * as Clock from "effect/Clock";
import type * as Crypto from "effect/Crypto";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import type * as Redacted from "effect/Redacted";
import * as Ref from "effect/Ref";
import * as Result from "effect/Result";
import * as Schema from "effect/Schema";
import type * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import type { TokenGrant } from "./Coordinator.js";
import * as H3 from "./H3.js";
import type { DecodedMedia } from "./Media.js";
import type { ClipTag, Source, SourceClip, SourceEvent, SourceState } from "./Playout.js";
import { ItemKey } from "./internal/playout/errors.js";
import { noAcquisition, Reactor } from "./Reactor.js";
import type { CreateOptions } from "./Reactor.js";
import { AcquisitionFailure, CommandFailure, ReactorError } from "./ReactorError.js";
import type { Session } from "./Session.js";

/**
 * A durable owner record of an allocated session, without its token: enough to
 * find it again and terminate it with the grant's token, stored as the
 * application decides.
 */
export const Allocation = Schema.Struct({
  sessionId: Schema.String,
  ownership: Schema.Literals(["owned", "attached"]),
  model: Schema.String,
  /** When the grant's token expires, in seconds since the epoch. */
  expiresAt: Schema.Finite,
  /** When the session's granted length ends at the latest, in seconds since the epoch. */
  endsAt: Schema.Finite,
});
export type Allocation = typeof Allocation.Type;

/** A session just allocated, before it connects, and the grant it runs under. */
export interface Allocated {
  readonly session: Session;
  readonly grant: TokenGrant;
  readonly allocation: Allocation;
}

export interface Options {
  /** Set before the first enqueue; left as the session has it when absent. */
  readonly canvas?: H3.Aspect | undefined;
  /** Keep the last frame between clips instead of flushing to black; true by default. */
  readonly holdLastFrame?: boolean | undefined;
  readonly provider?: H3.Options | undefined;
  /** How long a lost connection may take to reconnect before the session counts as lost; 20 seconds by default. */
  readonly recovery?: Duration.Input | undefined;
}

export interface OpenOptions<E = never, R = never> extends Options {
  /** Mints the session's token. The API key never reaches the opener. */
  readonly mint: Effect.Effect<TokenGrant, ReactorError, R>;
  /** Runs after allocation and before connect, so a supervisor can record the owner first. */
  readonly onAllocated?: ((allocated: Allocated) => Effect.Effect<void, E, R>) | undefined;
  readonly create?: Omit<CreateOptions, "model" | "jwt" | "onAllocated"> | undefined;
}

export interface ResumeOptions extends Omit<Options, "canvas"> {
  readonly allocation: Allocation;
  /** The grant's token, stored beside the record. */
  readonly jwt: Redacted.Redacted<string>;
}

// Playout's identity travels inside the caller part of H3's metadata envelope,
// so a resumed session's clips are recognized by key whatever process sent them.
const Envelope = Schema.fromJsonString(
  Schema.Struct({ reactor_effect_h3: Schema.Literal(1), caller: Schema.String }),
);
const Tag = Schema.fromJsonString(
  Schema.Struct({
    playout: Schema.Literal(1),
    key: Schema.optionalKey(ItemKey),
    filler: Schema.optionalKey(Schema.Int),
    metadata: Schema.optionalKey(Schema.String),
  }),
);

const tagOf = (metadata: string): ClipTag | undefined => {
  const envelope = Schema.decodeResult(Envelope)(metadata);
  if (Result.isFailure(envelope)) return undefined;
  const tag = Schema.decodeResult(Tag)(envelope.success.caller);
  if (Result.isFailure(tag)) return undefined;
  if (tag.success.key !== undefined) return { _tag: "Item", key: tag.success.key };
  return tag.success.filler === undefined
    ? undefined
    : { _tag: "Filler", index: tag.success.filler };
};

const clipOf = (clip: (typeof H3.Clip)["Type"]): SourceClip => ({
  clipId: clip.clip_id,
  tag: tagOf(clip.metadata),
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

/** Sends each generation's decoded track, following the session across reconnects. */
const track = <A>(
  session: Session,
  read: (media: DecodedMedia) => Stream.Stream<A, ReactorError>,
) =>
  session.changes.pipe(
    Stream.filter((snapshot) => snapshot.status === "ready"),
    Stream.map((snapshot) => snapshot.generation),
    Stream.changes,
    Stream.switchMap(() =>
      Stream.unwrap(Effect.map(session.decoded, read)).pipe(Stream.catchCause(() => Stream.empty)),
    ),
  );

/** A connected session as a playout source that lives `lifetime`. */
const fromSession = Effect.fnUntraced(function* (
  session: Session,
  options: Options & { readonly lifetime: Duration.Duration; readonly resumed: boolean },
) {
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
  const recover = session.reconnect.pipe(
    Effect.andThen(provider.refresh),
    Effect.timeoutOrElse({
      duration: recovery,
      orElse: () => Effect.fail(ReactorError.fromCode("Timeout", "reconnecting took too long")),
    }),
    Effect.mapError((error) =>
      CommandFailure.is(error)
        ? ReactorError.make({ reason: error.reason, context: error.context })
        : error,
    ),
  );
  const translate = (
    event: H3.ProviderEvent,
  ): Effect.Effect<ReadonlyArray<SourceEvent>, ReactorError> =>
    Effect.gen(function* () {
      if (
        event._tag === "Session" &&
        event.source._tag === "Status" &&
        event.source.status === "disconnected"
      ) {
        yield* recover;
        return [{ _tag: "State", state: stateOf(yield* provider.snapshot) }];
      }
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
            { _tag: "Failed", clip: clipOf(message.data.clip), reason: message.data.reason },
            state,
          ];
        default:
          return [state];
      }
    });
  const events: Stream.Stream<SourceEvent, ReactorError> = Stream.unwrap(
    Effect.map(provider.observe({ capacity: 1024 }), (observation) =>
      Stream.concat(
        Stream.succeed<SourceEvent>({ _tag: "State", state: stateOf(observation.initial) }),
        observation.events.pipe(
          Stream.mapEffect(translate),
          Stream.flatMap((translated) => Stream.fromIterable(translated)),
        ),
      ),
    ),
  );
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
  // The autoplay the playout last asked for, which a cut puts back when it is done.
  const autoplay = yield* Ref.make(false);
  const withAutoplayOff = <A>(effect: Effect.Effect<A, CommandFailure>) =>
    provider.setAutoplay(false).pipe(
      Effect.andThen(Effect.exit(effect)),
      Effect.flatMap((exit) =>
        Effect.andThen(
          Effect.flatMap(Ref.get(autoplay), (wanted) => provider.setAutoplay(wanted)),
          exit,
        ),
      ),
    );
  return {
    sessionId: session.id,
    lifetime: options.lifetime,
    events,
    enqueue: (request, tag, continueFrom) =>
      Effect.gen(function* () {
        const caller = yield* Schema.encodeResult(Tag)({
          playout: 1,
          ...(tag._tag === "Item" ? { key: tag.key } : { filler: tag.index }),
          ...(request.metadata === undefined ? {} : { metadata: request.metadata }),
        }).pipe(
          Effect.fromResult,
          Effect.mapError(() =>
            CommandFailure.from(
              ReactorError.fromCode("InvalidInput", "clip metadata could not be encoded"),
              { operation: "enqueue", outcome: "not-submitted" },
            ),
          ),
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
    setAutoplay: (enabled) =>
      provider.setAutoplay(enabled).pipe(Effect.andThen(Ref.set(autoplay, enabled))),
    // With autoplay off nothing starts between the stop and the play, so the stop can only hit
    // the clip that was playing. That clip may have ended on its own first: a refusal because
    // nothing plays is harmless, and a clip that took its place is not stopped.
    cut: (clipId, next) =>
      withAutoplayOff(
        Effect.gen(function* () {
          const playing = playingOf(yield* provider.snapshot);
          if (playing !== undefined && playing !== clipId) return;
          if (playing !== undefined)
            yield* provider.stop.pipe(
              Effect.flatMap((stopped) => landed(clipId, stopped)),
              Effect.catchIf(replied, () => Effect.void),
            );
          yield* provider.play(next);
        }),
      ),
    video: track(session, (media) => media.video(H3.h3ReferenceTurboRealtime.tracks.video)),
    audio: track(session, (media) => media.audio(H3.h3ReferenceTurboRealtime.tracks.audio)),
    close: session.close,
  } satisfies Source;
});

const failAcquisition = (session: Session) => (error: ReactorError | CommandFailure) =>
  Effect.flatMap(session.close, (report) => Effect.fail(AcquisitionFailure.from(error, report)));

/**
 * Opens a paid H3 session as a playout source: mint its token, allocate it, run
 * `onAllocated`, connect, and set it up. Its lifetime is the granted length.
 * `Playout.make({ open: H3Source.open({ mint }), ... })`.
 */
export const open = <E = never, R = never>(
  options: OpenOptions<E, R>,
): Effect.Effect<Source, AcquisitionFailure | E, R | Reactor | Crypto.Crypto | Scope.Scope> =>
  Effect.gen(function* () {
    const reactor = yield* Reactor;
    const grant = yield* options.mint.pipe(
      Effect.mapError((error) => AcquisitionFailure.from(error, noAcquisition)),
    );
    // The server starts the granted length no earlier than the request.
    const requested = yield* Clock.currentTimeMillis;
    const onAllocated = options.onAllocated;
    const session = yield* reactor.create({
      ...options.create,
      model: H3.modelName,
      jwt: grant.jwt,
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
                  expiresAt: grant.expiresAt,
                  endsAt: requested / 1000 + grant.granted.maxSessionSeconds,
                },
              }),
          }),
    });
    return yield* fromSession(session, {
      ...options,
      lifetime: Duration.seconds(grant.granted.maxSessionSeconds),
      resumed: false,
    }).pipe(Effect.catch(failAcquisition(session)));
  }).pipe(
    Effect.withSpan("reactor.playout.open", { kind: "client" }, { captureStackTrace: false }),
  );

/**
 * Adopts a session `open` allocated, from its owner record and token, after its
 * owner died: this process then owns its remote lifetime. It keeps the
 * session's canvas, queue and playback; its lifetime is what remains until the
 * record's `endsAt`.
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
    const remainingMs = allocation.endsAt * 1000 - (yield* Clock.currentTimeMillis);
    if (!(remainingMs > 0)) return yield* refuse("the allocation's granted length has ended");
    const reactor = yield* Reactor;
    const session = yield* reactor.attach({
      sessionId: allocation.sessionId,
      jwt: options.jwt,
      adopt: true,
    });
    return yield* fromSession(session, {
      ...options,
      lifetime: Duration.millis(remainingMs),
      resumed: true,
    }).pipe(Effect.catch(failAcquisition(session)));
  }).pipe(
    Effect.withSpan("reactor.playout.resume", { kind: "client" }, { captureStackTrace: false }),
  );
