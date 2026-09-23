/**
 * The isolated native host's child process. The parent forks this module for
 * one peer, with advanced serialization on its IPC channel, and this process
 * serves one in-process `NativePeer` to it over Effect RPC. It sees ICE
 * configuration, SDP, channel bytes and media; allocation, credentials,
 * correlation and termination stay in the parent.
 */
import process from "node:process";
import * as NodeWorkerRunner from "@effect/platform-node/NodeWorkerRunner";
import * as Arr from "effect/Array";
import * as Cause from "effect/Cause";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as Queue from "effect/Queue";
import * as Stream from "effect/Stream";
import * as Rpc from "effect/unstable/rpc/Rpc";
import * as RpcServer from "effect/unstable/rpc/RpcServer";
import { ReactorError } from "reactor-effect-client";
import type { Track } from "reactor-effect-client";
import { parsed } from "reactor-effect-client/host";
import type {
  AudioFrame,
  IceServer,
  MediaPressure,
  Peer,
  PeerEvent,
  VideoFrame,
} from "reactor-effect-client/host";
import { resolveNativeBridge } from "../bridge.js";
import { NativePeer } from "../peer.js";
import { eventToWire, IsolatedRpcs, toWire } from "./protocol.js";
import type { PrepareItem, WireAudio, WireFailure, WireVideo } from "./protocol.js";

/** The native event queue's own bounds: 1,024 events and 16 MiB. */
const EVENT_CAPACITY = 1024;
const EVENT_BYTES = 16 * 1024 * 1024;
/**
 * Frames held for the parent while its channel is busy, at most the native
 * queues' own depths. A full handoff evicts its oldest frame and counts it, as
 * the native queue does, so a parent that stalls loses frames rather than its
 * reader.
 */
const VIDEO_HANDOFF = 8;
const AUDIO_HANDOFF = 256;

type Kind = "video" | "audio";

const videoToWire = (frame: VideoFrame): WireVideo => ({
  width: frame.width,
  height: frame.height,
  frameId: frame.frameId,
  timestampMicros: frame.timestampMicros,
  data: frame.data as Uint8Array<ArrayBuffer>,
  metadata: frame.metadata as Uint8Array<ArrayBuffer>,
});

const audioToWire = (frame: AudioFrame): WireAudio => ({
  sampleRate: frame.sampleRate,
  channels: frame.channels,
  samples: frame.samples as Int16Array<ArrayBuffer>,
});

const wire = <A>(effect: Effect.Effect<A, ReactorError>): Effect.Effect<A, WireFailure> =>
  Effect.mapError(effect, toWire);

const handlers = IsolatedRpcs.toLayer(
  Effect.sync(() => {
    let peer: NativePeer | undefined;
    /** Ends each open prepare stream once the peer has shut down. */
    const endings = new Set<() => void>();
    const evicted = { video: 0n, audio: 0n };

    const current = Effect.suspend(() =>
      peer === undefined
        ? Effect.fail(
            ReactorError.fromCode("InvalidState", "isolated native child has no open peer", {
              outcome: "not-submitted",
            }),
          )
        : Effect.succeed(peer),
    );
    const withPeer = <A>(body: (peer: NativePeer) => Effect.Effect<A, ReactorError>) =>
      wire(Effect.flatMap(current, body));

    const open = (libraryPath: string | undefined) =>
      wire(
        Effect.tryPromise({
          try: () => resolveNativeBridge(libraryPath),
          catch: (cause) =>
            ReactorError.is(cause)
              ? cause
              : ReactorError.fromCode("Native", "native WebRTC preflight failed", {
                  detail: cause,
                  outcome: "not-submitted",
                }),
        }).pipe(
          Effect.flatMap((resolved) =>
            parsed(() => {
              // The parent's deadline bounds this join, and it kills the child on expiry.
              peer ??= new NativePeer(resolved, Duration.infinity);
            }),
          ),
        ),
      );

    /** The offer, then every peer event, bounded as the native event queue is. */
    const prepare = (
      servers: readonly IceServer[],
      tracks: readonly Track[],
    ): Stream.Stream<PrepareItem, WireFailure> =>
      Stream.unwrap(
        Effect.gen(function* () {
          const target = yield* current;
          const queue = yield* Queue.bounded<
            { readonly item: PrepareItem; readonly bytes: number },
            ReactorError | Cause.Done
          >(EVENT_CAPACITY);
          const end = () => {
            Queue.endUnsafe(queue);
          };
          endings.add(end);
          yield* Effect.addFinalizer(() => Effect.sync(() => endings.delete(end)));
          let queuedBytes = 0;
          const offer = (event: PeerEvent): void => {
            const bytes = event.type === "message" ? event.bytes.byteLength : 0;
            if (
              queuedBytes + bytes > EVENT_BYTES ||
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
          const next = Effect.map(Queue.takeAll(queue), (entries) =>
            Arr.map(entries, (entry) => {
              queuedBytes -= entry.bytes;
              return entry.item;
            }),
          );
          return Stream.succeed<PrepareItem>({
            _tag: "Prepared",
            sdp: prepared.sdp,
            mapping: prepared.mapping,
          }).pipe(Stream.concat(Stream.fromPull(Effect.succeed(next))));
        }),
      ).pipe(Stream.mapError(toWire));

    /**
     * One track's frames, one per chunk: the server waits for the parent's
     * acknowledgement of each chunk, so the parent's credit is one frame.
     */
    const frames = <F, W>(
      kind: Kind,
      capacity: number,
      source: (peer: NativePeer) => Stream.Stream<F, ReactorError>,
      encode: (frame: F) => W,
    ): Stream.Stream<W, WireFailure> =>
      Stream.unwrap(
        Effect.gen(function* () {
          const target = yield* current;
          const queue = yield* Queue.sliding<W, ReactorError | Cause.Done>(capacity);
          yield* source(target).pipe(
            Stream.runForEach((frame) =>
              Effect.sync(() => {
                if (Queue.isFullUnsafe(queue)) evicted[kind]++;
                Queue.offerUnsafe(queue, encode(frame));
              }),
            ),
            Effect.onExit((exit) =>
              Exit.isSuccess(exit) ? Queue.end(queue) : Queue.failCause(queue, exit.cause),
            ),
            // Subscribe before the next request is handled: a frame that
            // request releases is then already observed.
            Effect.forkScoped({ startImmediately: true }),
          );
          return Stream.fromEffectRepeat(Queue.take(queue));
        }),
      ).pipe(Stream.mapError(toWire));

    /**
     * The native snapshot with the handoffs folded in: an evicted frame is
     * dropped rather than delivered, so arrivals still add up.
     */
    const pressure = (snapshot: MediaPressure): MediaPressure => {
      const video = evicted.video,
        audio = evicted.audio;
      return {
        ...snapshot,
        droppedVideo: snapshot.droppedVideo + video,
        droppedAudio: snapshot.droppedAudio + audio,
        deliveredVideo: snapshot.deliveredVideo > video ? snapshot.deliveredVideo - video : 0n,
        deliveredAudio: snapshot.deliveredAudio > audio ? snapshot.deliveredAudio - audio : 0n,
      };
    };

    return {
      Open: ({ libraryPath }) => Rpc.uninterruptible(open(libraryPath)),
      Prepare: ({ servers, tracks }) =>
        prepare(
          servers.map((server) => ({ ...server, urls: [...server.urls] })),
          tracks,
        ),
      Answer: ({ sdp }) => Rpc.uninterruptible(withPeer((target) => target.answer(sdp))),
      Send: ({ channel, bytes }) =>
        Rpc.uninterruptible(withPeer((target) => target.send(channel, bytes))),
      Direction: ({ name, active }) =>
        Rpc.uninterruptible(withPeer((target) => target.direction(name, active))),
      // Through the peer contract: only the absence of a track crosses processes.
      Replace: ({ name }) =>
        Rpc.uninterruptible(withPeer((target: Peer) => target.replace(name, null))),
      MaxBitrate: ({ name, bitsPerSecond }) =>
        Rpc.uninterruptible(withPeer((target) => target.maxBitrate(name, bitsPerSecond))),
      Stats: () => withPeer((target) => target.stats),
      Snapshot: () => withPeer((target) => Effect.map(target.rawMedia.snapshot, pressure)),
      Video: ({ name }) =>
        frames("video", VIDEO_HANDOFF, (target) => target.rawMedia.video(name), videoToWire),
      Audio: ({ name }) =>
        frames("audio", AUDIO_HANDOFF, (target) => target.rawMedia.audio(name), audioToWire),
      Shutdown: () =>
        Rpc.uninterruptible(
          Effect.suspend(() => (peer === undefined ? Effect.void : wire(peer.shutdown))).pipe(
            // The closed peer emits nothing more: end its event streams.
            Effect.ensuring(
              Effect.sync(() => {
                for (const end of endings) end();
              }),
            ),
          ),
        ),
    };
  }),
);

// The parent owns this process: once its channel closes, nothing can reach
// this peer again, so the child ends at once rather than outlive its parent.
process.once("disconnect", () => {
  process.kill(process.pid, "SIGKILL");
});
// A terminal interrupt reaches the whole process group; the parent decides
// when its child ends, through shutdown or its own exit.
process.on("SIGINT", () => undefined);

const server = RpcServer.layer(IsolatedRpcs, {
  disableFatalDefects: true,
  disableTracing: true,
}).pipe(
  Layer.provide(handlers),
  Layer.provide(RpcServer.layerProtocolWorkerRunner),
  Layer.provide(NodeWorkerRunner.layer),
);

// The runner ends when the parent closes the worker, after shutdown.
Effect.runFork(Layer.launch(server)).addObserver((exit) => {
  process.exit(Exit.isSuccess(exit) || Cause.hasInterruptsOnly(exit.cause) ? 0 : 1);
});
