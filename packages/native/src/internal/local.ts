/**
 * The addon's `NativePeer` in this process. libwebrtc callbacks never enter
 * JavaScript: the addon's readiness callback runs on the JavaScript thread and
 * only opens latches, and each queue is taken as its stream is pulled.
 */
import * as Cause from "effect/Cause";
import * as Effect from "effect/Effect";
import * as FiberSet from "effect/FiberSet";
import * as Latch from "effect/Latch";
import * as Ref from "effect/Ref";
import * as Stream from "effect/Stream";
import type { IceServer, Track } from "reactor-effect-client/Coordinator";
import { ReactorError } from "reactor-effect-client/ReactorError";
import type { Addon } from "./addon.js";
import type * as Binding from "./binding.js";
import { nativeFailure } from "./peer.js";
import type { NativeHandle } from "./peer.js";

/** The readiness bits the addon passes its callback. */
const Ready = { Events: 1, Video: 2, Audio: 4 } as const;

/** An addon peer and a stream over each of its queues. */
export interface Opened {
  readonly peer: Binding.NativePeer;
  readonly events: Stream.Stream<Binding.PeerEvent>;
  readonly video: Stream.Stream<Binding.Video>;
  readonly audio: Stream.Stream<Binding.Audio>;
  /** Fence the peer at once; each stream then ends. */
  readonly close: Effect.Effect<void>;
}

export const open = (addon: Addon): Effect.Effect<Opened, ReactorError> =>
  Effect.gen(function* () {
    const latches = [yield* Latch.make(), yield* Latch.make(), yield* Latch.make()] as const;
    const [events, video, audio] = latches;
    const peer = yield* Effect.try({
      try: () =>
        new addon.NativePeer((ready) => {
          if ((ready & Ready.Events) !== 0) Latch.openUnsafe(events);
          if ((ready & Ready.Video) !== 0) Latch.openUnsafe(video);
          if ((ready & Ready.Audio) !== 0) Latch.openUnsafe(audio);
        }),
      catch: (cause) =>
        ReactorError.fromCode("Native", "native WebRTC peer allocation failed", {
          outcome: "not-submitted",
          detail: cause,
        }),
    });
    const closed = yield* Ref.make(false);
    /**
     * A queue's items as its reader pulls them. Each pull first yields, so
     * readers run between frames and a backlog released by a stalled event
     * loop reaches them at their pace. An empty queue waits on its latch,
     * closed before the close check and the take, so neither a wake nor the
     * close between them is lost.
     */
    const drain = <A>(latch: Latch.Latch, take: () => A | null): Stream.Stream<A> =>
      Stream.fromEffectRepeat(
        Effect.gen(function* () {
          yield* Effect.yieldNow;
          for (;;) {
            yield* latch.close;
            if (yield* Ref.get(closed)) return yield* Cause.done();
            const item = take();
            if (item !== null) return item;
            yield* latch.await;
          }
        }),
      );
    return {
      peer,
      events: drain(events, () => peer.takeEvent()),
      video: drain(video, () => peer.takeVideo()),
      audio: drain(audio, () => peer.takeAudio()),
      close: Ref.set(closed, true).pipe(
        Effect.andThen(Effect.sync(() => peer.close())),
        Effect.andThen(Effect.forEach(latches, (latch) => latch.open, { discard: true })),
      ),
    };
  });

/** A call the addon threw or rejected before answering is an unclassified native failure. */
const unanswered = (cause: unknown): Binding.Failure => ({
  class: "Native",
  message: String(cause),
});

/** A call's reply, or the failure it carries. */
export const settle = (
  call: () => Promise<Binding.Reply>,
): Effect.Effect<Binding.Reply, Binding.Failure> =>
  Effect.gen(function* () {
    const reply = yield* Effect.tryPromise({ try: call, catch: unanswered });
    if (reply.failure !== undefined) return yield* Effect.fail(reply.failure);
    return reply;
  });

/** A prepare's servers and tracks, as the addon takes them. */
export const request = (session: {
  readonly servers: ReadonlyArray<IceServer>;
  readonly tracks: ReadonlyArray<Track>;
}): { readonly servers: Array<Binding.IceServer>; readonly tracks: Array<Binding.Track> } => ({
  servers: session.servers.map((server) => ({ ...server, urls: [...server.urls] })),
  tracks: session.tracks.map((track) => ({ ...track })),
});

/**
 * Owner joins that outlived their deadline, each held until it completes. A
 * factory's peers share one libwebrtc factory, which such a join may have
 * wedged, so the factory makes no peer while one is held. Closing the
 * factory's scope stops waiting for them.
 */
export type Joins = FiberSet.FiberSet;

/** Refuse before a session is allocated for a peer that could not work. */
export const usable = (joins: Joins): Effect.Effect<void, ReactorError> =>
  Effect.flatMap(FiberSet.size(joins), (held) =>
    held === 0
      ? Effect.void
      : ReactorError.fromCode(
          "Native",
          "native WebRTC runtime is degraded: an earlier peer's owner join exceeded its shutdown deadline and is still retained",
          { outcome: "not-submitted" },
        ),
  );

export const local = Effect.fnUntraced(function* (
  addon: Addon,
  joins: Joins,
): Effect.fn.Return<NativeHandle, ReactorError> {
  yield* usable(joins);
  const opened = yield* open(addon);
  const { peer } = opened;
  const call = (operation: string, run: () => Promise<Binding.Reply>) =>
    Effect.mapError(settle(run), nativeFailure(operation));
  // The addon's shutdown joins the owner once: asked again, it answers when
  // that join completes.
  const join = settle(() => peer.shutdown());
  return {
    prepare: (servers, tracks) =>
      call("prepare native WebRTC", () => {
        const prepare = request({ servers, tracks });
        return peer.prepare(prepare.servers, prepare.tracks);
      }).pipe(
        Effect.flatMap(({ prepared }) =>
          prepared === undefined
            ? Effect.fail(
                ReactorError.fromCode("Protocol", "native prepare answered without an offer"),
              )
            : Effect.succeed(prepared),
        ),
      ),
    answer: (sdp) => Effect.asVoid(call("apply native SDP answer", () => peer.answer(sdp))),
    direction: (name, active) =>
      Effect.asVoid(call("set native transceiver direction", () => peer.direction(name, active))),
    maxBitrate: (name, bitsPerSecond) =>
      Effect.asVoid(call("set native sender bitrate", () => peer.maxBitrate(name, bitsPerSecond))),
    send: (channel, bytes) =>
      Effect.asVoid(call(`send native ${channel}`, () => peer.send(channel, bytes))),
    stats: Effect.map(
      call("native WebRTC statistics", () => peer.stats()),
      ({ stats }) => stats ?? [],
    ),
    pressure: Effect.sync(() => peer.pressure()),
    events: opened.events,
    video: opened.video,
    audio: opened.audio,
    close: opened.close,
    shutdown: join.pipe(Effect.mapError(nativeFailure("shutdown native WebRTC")), Effect.asVoid),
    // The join keeps the peer until it completes, and the factory makes no
    // later peer until then.
    abandon: FiberSet.run(joins, join).pipe(
      Effect.as(
        ReactorError.fromCode(
          "Shutdown",
          "native owner join exceeded its deadline; handle retained",
          { operation: "shutdown native WebRTC" },
        ),
      ),
    ),
  };
});
