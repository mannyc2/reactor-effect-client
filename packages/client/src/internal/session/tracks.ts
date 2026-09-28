/**
 * A session's tracks: pausing and resuming received ones, publishing to
 * sent ones, the sender bitrate, and the media a host exposes. One operation
 * runs per track at a time, on the generation it started on.
 */
import * as Cause from "effect/Cause";
import * as Deferred from "effect/Deferred";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Ref from "effect/Ref";
import * as SubscriptionRef from "effect/SubscriptionRef";
import type { Track } from "../../Coordinator.js";
import type { DecodedMedia, TrackMedia } from "../../Media.js";
import type { MediaTrack } from "../../Peer.js";
import { ReactorError } from "../../ReactorError.js";
import * as Wire from "../wire.js";
import type { Generation } from "./generation.js";
import type { Connection, Core, Link, State } from "./model.js";
import { excluding, failureOr, including, timedOut } from "./model.js";
import type { Requests } from "./requests.js";
import { unexpected } from "./requests.js";

const unsupported = (message: string) =>
  ReactorError.fromCode("UnsupportedCapability", message, { outcome: "not-submitted" });

export const make = ({
  core,
  generation,
  requests,
}: {
  readonly core: Core;
  readonly generation: Generation;
  readonly requests: Requests;
}) => {
  const { settings, state } = core;
  const { current, currentReady, guard, fail, fenced } = generation;
  const { notification, controlRequest } = requests;

  /** `body` on a negotiated track of `expected`, or of the ready generation; one per track. */
  const trackOperation = <A>(
    name: string,
    body: (c: Connection, track: Track) => Effect.Effect<A, ReactorError>,
    expected?: Connection,
  ) =>
    Effect.acquireUseRelease(
      Effect.gen(function* () {
        const c = expected ?? (yield* currentReady).c;
        yield* current(c);
        const link = yield* Ref.get(c.link);
        const track = link.negotiated?.descriptor.capabilities.tracks.find((t) => t.name === name);
        if (track === undefined)
          return yield* ReactorError.fromCode("InvalidState", `unknown track: ${name}`);
        const free = yield* Ref.modify(c.link, (held) =>
          held.busy.has(name)
            ? ([false, held] as const)
            : ([true, { ...held, busy: including(name)(held.busy) }] as const),
        );
        if (!free)
          return yield* ReactorError.fromCode(
            "InvalidState",
            `another track operation is in flight: ${name}`,
          );
        return { c, track };
      }),
      ({ c, track }) => guard(c, body(c, track)),
      ({ c }) =>
        Ref.update(c.link, (link): Link => ({ ...link, busy: excluding(name)(link.busy) })),
    );

  const setTrackActive = (name: string, active: boolean, expected?: Connection) =>
    trackOperation(
      name,
      (c) =>
        c.peer.direction(name, active).pipe(
          Effect.andThen(
            Ref.update(c.link, (link): Link => ({
              ...link,
              paused: (active ? excluding(name) : including(name))(link.paused),
            })),
          ),
          Effect.andThen(
            notification(
              c,
              active
                ? { case: "resumeTrack", value: { name } }
                : { case: "pauseTrack", value: { name } },
            ),
          ),
        ),
      expected,
    );

  /**
   * A platform track replacement cannot be cancelled. Recording its owner is
   * masked, and an abandoned wait retires the generation so a late
   * replacement cannot race another sender operation.
   */
  const replaceSender = (c: Connection, name: string, source: MediaTrack | null) =>
    Effect.uninterruptibleMask((restore) =>
      Effect.gen(function* () {
        const media = c.peer.media;
        if (media._tag !== "Tracks") return yield* unsupported("this peer publishes no tracks");
        yield* current(c);
        const clone = source === null ? null : source.clone();
        const invoked = yield* Ref.make(false);
        const expired = yield* Ref.make(false);
        yield* restore(
          current(c).pipe(
            Effect.andThen(Ref.set(invoked, true)),
            Effect.andThen(media.replace(name, clone)),
            Effect.timeoutOrElse({
              duration: settings.replyTimeout,
              orElse: () =>
                Ref.set(expired, true).pipe(
                  Effect.andThen(
                    Effect.fail(
                      ReactorError.fromCode(
                        "Timeout",
                        "sender replacement deadline; generation retired",
                        { operation: name, outcome: "unknown" },
                      ),
                    ),
                  ),
                ),
            }),
          ),
        ).pipe(
          Effect.onExit((exit) =>
            Exit.isSuccess(exit)
              ? Effect.void
              : Effect.gen(function* () {
                  clone?.stop();
                  const deadlined = yield* Ref.get(expired);
                  if ((yield* Ref.get(invoked)) && (deadlined || Cause.hasInterrupts(exit.cause)))
                    yield* fail(
                      c,
                      ReactorError.fromCode(
                        deadlined ? "Timeout" : "Disconnected",
                        "sender replacement abandoned; reconnect before reusing the publication",
                        { operation: name, outcome: "unknown" },
                      ),
                    );
                }),
          ),
        );
        const live = yield* Effect.exit(current(c));
        if (Exit.isFailure(live)) {
          clone?.stop();
          return yield* live;
        }
        const previous = yield* Ref.modify(c.link, (link) => {
          const sending = new Map(link.sending);
          const before = sending.get(name);
          if (clone === null) sending.delete(name);
          else sending.set(name, clone);
          return [before, { ...link, sending }] as const;
        });
        previous?.stop();
      }),
    );

  const publishTrack = (name: string, source: MediaTrack, expected?: Connection) =>
    trackOperation(
      name,
      Effect.fnUntraced(function* (c: Connection, track: Track) {
        if (
          track.direction !== "sendonly" ||
          source.kind !== track.kind ||
          source.readyState !== "live"
        )
          return yield* ReactorError.fromCode(
            "InvalidState",
            "publish requires a live matching input track",
          );
        if (!(yield* Ref.get(c.link)).claimed.has(name)) {
          const reply = yield* controlRequest(
            "publish_track",
            { case: "publishTrack", value: { name } },
            c,
          );
          if (reply._tag !== "TrackPublished" || reply.name !== name)
            return yield* unexpected("publisher claim reply mismatch");
          yield* Ref.update(c.link, (link): Link => ({
            ...link,
            claimed: including(name)(link.claimed),
          }));
        }
        yield* replaceSender(c, name, source);
      }),
      expected,
    );

  const unpublishTrack = (name: string, expected?: Connection) =>
    trackOperation(
      name,
      (c) =>
        replaceSender(c, name, null).pipe(
          Effect.andThen(notification(c, { case: "unpublishTrack", value: { name } })),
          Effect.andThen(
            Ref.update(c.link, (link): Link => ({
              ...link,
              claimed: excluding(name)(link.claimed),
            })),
          ),
        ),
      expected,
    );

  const setMaxBitrate = (name: string, bitsPerSecond: number, expected?: Connection) =>
    trackOperation(
      name,
      (c) =>
        c.peer.maxBitrate(name, bitsPerSecond).pipe(
          Effect.andThen(
            SubscriptionRef.update(state, (session): State => ({
              ...session,
              bitrates: new Map(session.bitrates).set(name, bitsPerSecond),
            })),
          ),
        ),
      expected,
    );

  /** Closing's last notifications: an unpublish for every track `c` claimed. */
  const releasePublications = Effect.fnUntraced(function* (c: Connection | undefined) {
    const submitted: Array<string> = [];
    const errors: Array<ReactorError> = [];
    if (c === undefined) return { submitted, errors };
    const link = yield* Ref.get(c.link);
    if (link.failure !== undefined) return { submitted, errors };
    for (const name of link.claimed) {
      // Closing has fenced ordinary requests; these are the last notifications.
      const sent = yield* Effect.exit(
        Wire.encode(Wire.ControlClientMessageSchema)({
          kind: Wire.MessageKind.NOTIFICATION,
          payload: { case: "unpublishTrack", value: { name } },
        }).pipe(
          Effect.flatMap((bytes) => c.peer.send("control", bytes)),
          Effect.timeoutOrElse({
            duration: Duration.min(Duration.seconds(1), settings.replyTimeout),
            orElse: timedOut("close unpublish"),
          }),
        ),
      );
      if (Exit.isSuccess(sent)) submitted.push(name);
      else
        errors.push(
          failureOr(() =>
            ReactorError.fromCode("Shutdown", "publication cleanup failed", {
              detail: sent.cause,
            }),
          )(sent.cause),
        );
    }
    return { submitted, errors };
  });

  const decoded: Effect.Effect<DecodedMedia, ReactorError> = Effect.gen(function* () {
    const { c, negotiated } = yield* currentReady;
    const media = c.peer.media;
    if (media._tag !== "Decoded") return yield* unsupported("this peer has no decoded media");
    return {
      generation: c.generation,
      tracks: negotiated.descriptor.capabilities.tracks,
      retired: Deferred.await(c.failed),
      video: (name) => fenced(c, media.video(name)),
      audio: (name) => fenced(c, media.audio(name)),
      pressure: media.pressure,
    };
  });

  const tracks: Effect.Effect<TrackMedia, ReactorError> = Effect.gen(function* () {
    const { c, negotiated } = yield* currentReady;
    const media = c.peer.media;
    if (media._tag !== "Tracks") return yield* unsupported("this peer has no platform tracks");
    return {
      generation: c.generation,
      tracks: negotiated.descriptor.capabilities.tracks,
      retired: Deferred.await(c.failed),
      track: (name) => current(c).pipe(Effect.andThen(media.lease(name))),
      publish: (name, source) => publishTrack(name, source, c),
      unpublish: (name) => unpublishTrack(name, c),
      setTrackActive: (name, active) => setTrackActive(name, active, c),
      setMaxBitrate: (name, bits) => setMaxBitrate(name, bits, c),
    };
  });

  return { setTrackActive, releasePublications, decoded, tracks };
};

export type Tracks = ReturnType<typeof make>;
