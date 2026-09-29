/**
 * A connected session's media as the browser's own tracks, and local playback
 * of them. Starting playback does not establish that an audience saw anything.
 */
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import type * as Scope from "effect/Scope";
import type { TrackMedia } from "reactor-effect-client/Media";
import { ReactorError } from "reactor-effect-client/ReactorError";
import type { Session } from "reactor-effect-client/Session";

/** A generation's tracks, typed as the DOM tracks the browser peer produces. */
export interface Tracks extends Omit<TrackMedia, "track" | "publish"> {
  /** A clone of a received track, stopped when the scope closes. */
  readonly track: (name: string) => Effect.Effect<MediaStreamTrack, ReactorError, Scope.Scope>;
  readonly publish: (name: string, source: MediaStreamTrack) => Effect.Effect<void, ReactorError>;
}

/** The session's current tracks. A reconnect produces new ones. */
export const tracks = (session: Session): Effect.Effect<Tracks, ReactorError> =>
  Effect.map(session.tracks, (generation) => ({
    ...generation,
    // The browser peer is the only producer of this session's tracks.
    track: (name) => Effect.map(generation.track(name), (track) => track as MediaStreamTrack),
  }));

export interface PlayOptions {
  /** How long starting playback may take; 10 seconds by default. */
  readonly playTimeout?: Duration.Input | undefined;
}

/**
 * Plays a clone of `track` in `element` until the scope closes, which stops the
 * clone and detaches it. Fails when starting takes longer than `playTimeout`.
 */
export const play = Effect.fnUntraced(function* (
  track: MediaStreamTrack,
  element: Pick<HTMLMediaElement, "srcObject" | "getAttribute" | "play" | "pause">,
  options: PlayOptions = {},
): Effect.fn.Return<void, ReactorError, Scope.Scope> {
  const timeout = yield* Effect.fromOption(
    Duration.fromInput(options.playTimeout ?? "10 seconds"),
  ).pipe(
    Effect.mapError(() =>
      ReactorError.fromCode("InvalidInput", "playTimeout is not a duration", {
        outcome: "not-submitted",
      }),
    ),
  );
  if (track.readyState !== "live")
    return yield* ReactorError.fromCode("InvalidState", "playback needs a live track");
  if (element.srcObject !== null || (element.getAttribute("src") ?? "") !== "")
    return yield* ReactorError.fromCode("InvalidState", "the element already has a source");
  const owned = yield* Effect.acquireRelease(
    Effect.sync(() => track.clone()),
    (clone) => Effect.sync(() => clone.stop()),
  );
  yield* Effect.acquireRelease(
    Effect.try({
      try: () => {
        const stream = new MediaStream([owned]);
        element.srcObject = stream;
        return stream;
      },
      catch: (cause) =>
        ReactorError.fromCode("UnsupportedCapability", "attaching the media element failed", {
          operation: "play",
          detail: cause,
        }),
    }),
    (stream) =>
      Effect.sync(() => {
        if (element.srcObject === stream) {
          element.pause();
          element.srcObject = null;
        }
      }),
  );
  yield* Effect.tryPromise({
    try: () => element.play(),
    catch: (cause) =>
      ReactorError.fromCode("UnsupportedCapability", "HTMLMediaElement.play failed", {
        operation: "play",
        detail: cause,
      }),
  }).pipe(
    Effect.timeoutOrElse({
      duration: timeout,
      orElse: () => Effect.fail(ReactorError.fromCode("Timeout", "playback did not start in time")),
    }),
  );
  if (owned.readyState !== "live")
    return yield* ReactorError.fromCode("Disconnected", "the track ended while playback started");
});
