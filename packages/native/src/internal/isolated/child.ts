/**
 * The isolated native host's child process. The parent forks this module for
 * one peer, with advanced serialization on its IPC channel, and this process
 * serves one in-process native peer to it over Effect RPC. It sees ICE
 * configuration, SDP, channel bytes and media; allocation, credentials,
 * correlation and termination stay in the parent.
 */
import * as NodeRuntime from "@effect/platform-node/NodeRuntime";
import * as NodeWorkerRunner from "@effect/platform-node/NodeWorkerRunner";
import * as Arr from "effect/Array";
import * as Cause from "effect/Cause";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as Queue from "effect/Queue";
import * as Ref from "effect/Ref";
import * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import * as Rpc from "effect/unstable/rpc/Rpc";
import * as RpcServer from "effect/unstable/rpc/RpcServer";
import type { AudioFrame, MediaPressure, VideoFrame } from "reactor-effect-client/Media";
import type { PeerEvent } from "reactor-effect-client/Peer";
import { ReactorError } from "reactor-effect-client/ReactorError";
import { resolve } from "../library.js";
import * as NativePeer from "../peer.js";
import { take, takeAll } from "../queue.js";
import { eventToWire, IsolatedRpcs, toWire } from "./protocol.js";
import type { PrepareItem, WireAudio, WireFailure, WireVideo } from "./protocol.js";

/** The native event queue's own bounds: 1,024 events and 16 MiB. */
const eventCapacity = 1024;
const eventBytes = 16 * 1024 * 1024;
/**
 * Frames held for the parent while its channel is busy, at most the native
 * queues' own depths. A full handoff evicts its oldest frame and counts it, as
 * the native queue does, so a parent that stalls loses frames, not its reader.
 */
const videoHandoff = 8;
const audioHandoff = 256;

const videoToWire = (frame: VideoFrame): WireVideo => ({
  width: frame.width,
  height: frame.height,
  frameId: frame.frameId,
  timestampMicros: frame.timestampMicros,
  sequence: frame.sequence,
  data: frame.data,
  metadata: frame.metadata,
});

const audioToWire = (frame: AudioFrame): WireAudio => ({
  sampleRate: frame.sampleRate,
  channels: frame.channels,
  sequence: frame.sequence,
  samples: frame.samples,
});

const wire = <A>(effect: Effect.Effect<A, ReactorError>): Effect.Effect<A, WireFailure> =>
  Effect.mapError(effect, toWire);

const handlers = IsolatedRpcs.toLayer(
  Effect.gen(function* () {
    // The peer lives until this process ends; its finalizer joins it if Shutdown never ran.
    const scope = yield* Effect.scope;
    const peer = yield* Ref.make<NativePeer.NativePeer | undefined>(undefined);
    /** Ends each open prepare stream once the peer has shut down. */
    const endings = yield* Ref.make<ReadonlyArray<Effect.Effect<boolean>>>([]);
    const evicted = yield* Ref.make({ video: 0n, audio: 0n });

    const current = Ref.get(peer).pipe(
      Effect.filterOrFail(
        (target): target is NativePeer.NativePeer => target !== undefined,
        () =>
          ReactorError.fromCode("InvalidState", "isolated native child has no open peer", {
            outcome: "not-submitted",
          }),
      ),
    );
    const withPeer = <A>(body: (target: NativePeer.NativePeer) => Effect.Effect<A, ReactorError>) =>
      wire(Effect.flatMap(current, body));

    const open = (libraryPath: string | undefined) =>
      wire(
        Effect.gen(function* () {
          if ((yield* Ref.get(peer)) !== undefined) return;
          const library = yield* resolve(libraryPath);
          // The parent's deadline bounds this join, and it kills the child on expiry.
          const opened = yield* NativePeer.make(library, Duration.infinity).pipe(
            Effect.provideService(Scope.Scope, scope),
          );
          yield* Ref.set(peer, opened);
        }),
      );

    /** The offer, then every peer event, bounded as the native event queue is. */
    const prepare = (
      servers: Parameters<NativePeer.NativePeer["prepare"]>[0],
      tracks: Parameters<NativePeer.NativePeer["prepare"]>[1],
    ): Stream.Stream<PrepareItem, WireFailure> =>
      Stream.unwrap(
        Effect.gen(function* () {
          const target = yield* current;
          const queue = yield* Queue.bounded<
            { readonly item: PrepareItem; readonly bytes: number },
            ReactorError | Cause.Done
          >(eventCapacity);
          yield* Ref.update(endings, (all) => [...all, Queue.end(queue)]);
          let queuedBytes = 0;
          // The peer emits synchronously from its pump; this is the host boundary.
          const offer = (event: PeerEvent): void => {
            const bytes = event.type === "message" ? event.bytes.byteLength : 0;
            if (
              queuedBytes + bytes > eventBytes ||
              !Queue.offerUnsafe(queue, {
                item: { _tag: "Event", event: eventToWire(event) },
                bytes,
              })
            ) {
              Queue.failCauseUnsafe(
                queue,
                Cause.fail(
                  ReactorError.fromCode("Overflow", "isolated native event queue bound exceeded"),
                ),
              );
              return;
            }
            queuedBytes += bytes;
          };
          const prepared = yield* target.prepare(servers, tracks, offer);
          const next = Effect.map(takeAll(queue), (entries) =>
            Arr.map(entries, (entry) => {
              queuedBytes -= entry.bytes;
              return entry.item;
            }),
          );
          return Stream.succeed<PrepareItem>({
            _tag: "Prepared",
            sdp: prepared.sdp,
            mapping: prepared.mapping,
          }).pipe(Stream.concat(next.pipe(Effect.succeed, Stream.fromPull)));
        }),
      ).pipe(Stream.mapError(toWire));

    /**
     * One track's frames, one per chunk: the server waits for the parent's
     * acknowledgement of each chunk, so the parent's credit is one frame.
     */
    const frames = <F, W>(
      kind: "video" | "audio",
      capacity: number,
      source: (target: NativePeer.NativePeer) => Stream.Stream<F, ReactorError>,
      encode: (frame: F) => W,
    ): Stream.Stream<W, WireFailure> =>
      Stream.unwrap(
        Effect.gen(function* () {
          const target = yield* current;
          const queue = yield* Queue.sliding<W, ReactorError | Cause.Done>(capacity);
          yield* source(target).pipe(
            Stream.runForEach((frame) =>
              Effect.gen(function* () {
                if (yield* Queue.isFull(queue))
                  yield* Ref.update(evicted, (counts) => ({
                    ...counts,
                    [kind]: counts[kind] + 1n,
                  }));
                yield* Queue.offer(queue, encode(frame));
              }),
            ),
            Effect.onExit((exit) =>
              Exit.isSuccess(exit) ? Queue.end(queue) : Queue.failCause(queue, exit.cause),
            ),
            // Subscribe before the next request is handled: a frame that
            // request releases is then already observed.
            Effect.forkScoped({ startImmediately: true }),
          );
          return Stream.fromEffectRepeat(take(queue));
        }),
      ).pipe(Stream.mapError(toWire));

    /** The native snapshot with the handoffs folded in: an evicted frame was dropped, not delivered. */
    const pressure = (snapshot: MediaPressure) =>
      Effect.map(Ref.get(evicted), ({ video, audio }): MediaPressure => ({
        ...snapshot,
        droppedVideo: snapshot.droppedVideo + video,
        droppedAudio: snapshot.droppedAudio + audio,
        deliveredVideo: snapshot.deliveredVideo > video ? snapshot.deliveredVideo - video : 0n,
        deliveredAudio: snapshot.deliveredAudio > audio ? snapshot.deliveredAudio - audio : 0n,
      }));

    const decoded = (target: NativePeer.NativePeer) =>
      target.media._tag === "Decoded"
        ? Effect.succeed(target.media)
        : Effect.fail(ReactorError.fromCode("InvalidState", "native peer has no decoded media"));

    return {
      Open: ({ libraryPath }) => Rpc.uninterruptible(open(libraryPath)),
      Prepare: ({ servers, tracks }) => prepare(servers, tracks),
      Answer: ({ sdp }) => Rpc.uninterruptible(withPeer((target) => target.answer(sdp))),
      Send: ({ channel, bytes }) =>
        Rpc.uninterruptible(withPeer((target) => target.send(channel, bytes))),
      Direction: ({ name, active }) =>
        Rpc.uninterruptible(withPeer((target) => target.direction(name, active))),
      // Through the peer contract: only the absence of a track crosses processes.
      MaxBitrate: ({ name, bitsPerSecond }) =>
        Rpc.uninterruptible(withPeer((target) => target.maxBitrate(name, bitsPerSecond))),
      Stats: () => withPeer((target) => target.stats),
      Snapshot: () =>
        withPeer((target) =>
          Effect.flatMap(decoded(target), (media) => Effect.flatMap(media.pressure, pressure)),
        ),
      Video: ({ name }) =>
        frames(
          "video",
          videoHandoff,
          (target) => (target.media._tag === "Decoded" ? target.media.video(name) : Stream.empty),
          videoToWire,
        ),
      Audio: ({ name }) =>
        frames(
          "audio",
          audioHandoff,
          (target) => (target.media._tag === "Decoded" ? target.media.audio(name) : Stream.empty),
          audioToWire,
        ),
      Shutdown: () =>
        Ref.get(peer).pipe(
          Effect.flatMap((target) => (target === undefined ? Effect.void : wire(target.shutdown))),
          // The closed peer emits nothing more: end its event streams.
          Effect.ensuring(Ref.get(endings).pipe(Effect.flatMap(Effect.all), Effect.asVoid)),
          Rpc.uninterruptible,
        ),
    };
  }),
);

// The parent owns this process: once its channel closes, nothing can reach this
// peer again, so the child ends at once rather than outlive its parent.
process.once("disconnect", () => {
  process.kill(process.pid, "SIGKILL");
});

// The runner ends when the parent closes the worker, after shutdown.
RpcServer.layer(IsolatedRpcs, { disableFatalDefects: true, disableTracing: true }).pipe(
  Layer.provide(handlers),
  Layer.provide(RpcServer.layerProtocolWorkerRunner),
  Layer.provide(NodeWorkerRunner.layer),
  Layer.launch,
  NodeRuntime.runMain({
    disableErrorReporting: true,
    // The parent ends the runner by closing its worker: that interruption is a
    // clean exit, and it comes at once, before the parent's channel closes and
    // the disconnect guard above would kill this process.
    teardown: (exit, onExit) => {
      const code = Exit.isSuccess(exit) || Cause.hasInterruptsOnly(exit.cause) ? 0 : 1;
      onExit(code);
      process.exit(code);
    },
  }),
);
