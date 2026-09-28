/**
 * A test peer: the host `Peer` contract over the simulated Reactor instead of WebRTC, with
 * channel messages delivered in order after the channel latency.
 */
import * as Effect from "effect/Effect";
import * as Queue from "effect/Queue";
import * as Stream from "effect/Stream";
import { take } from "../queue.js";
import type { Mapping } from "../../Coordinator.js";
import { ReactorError } from "../../ReactorError.js";
import { h3ReferenceTurboRealtime as profile } from "../h3/profile.js";
import { Observations } from "../../observation.js";
import type { Channel, Peer, PeerEvent } from "../../Peer.js";
import type { AudioFrame, VideoFrame } from "../../Media.js";
import { monotonic, until } from "./playout.js";
import type { Sessions } from "./sessions.js";

export interface Frame {
  readonly data: Uint8Array<ArrayBuffer>;
  readonly frameId: bigint;
  readonly timestampMicros: bigint;
}

/** The simulator's end of a test peer's connection. */
export interface Link {
  readonly id: string;
  /** Connectivity succeeded: the peer is connected and both channels are open. */
  readonly open: Effect.Effect<void>;
  /** The remote ended the connection. */
  readonly drop: (reason: "ended" | "replaced" | "disconnected") => Effect.Effect<void>;
  /** A channel message, delivered after the channel latency. */
  readonly deliver: (channel: Channel, bytes: Uint8Array<ArrayBuffer>) => Effect.Effect<void>;
  readonly video: (frame: Frame) => Effect.Effect<void>;
  readonly audio: (samples: Int16Array<ArrayBuffer>) => Effect.Effect<void>;
}

interface Delivery {
  readonly channel: Channel;
  readonly bytes: Uint8Array<ArrayBuffer>;
  readonly due: number;
}

const offer = (peerId: string, mapping: ReadonlyArray<Mapping>): string =>
  [
    "v=0",
    "o=- 1 2 IN IP4 127.0.0.1",
    "s=reactor-test",
    "t=0 0",
    `a=ice-ufrag:${peerId}`,
    ...mapping.map((track) => `m=${track.kind} 9 UDP/TLS/RTP/SAVPF 96\r\na=mid:${track.mid}`),
    "",
  ].join("\r\n");

const unsupported = (message: string) =>
  ReactorError.fromCode("UnsupportedCapability", message, { outcome: "not-submitted" });

/**
 * One test peer. Its connection state is local to it; everything that waits
 * is an Effect.
 */
export const make = (sessions: Sessions): Peer => {
  let emit: ((event: PeerEvent) => void) | undefined;
  let id: string | undefined;
  let mapping: ReadonlyArray<Mapping> = [];
  let inbound: Queue.Queue<Delivery> | undefined;
  let outbound: Queue.Queue<Delivery> | undefined;
  let opened = false;
  let closed = false;
  const video = new Observations<VideoFrame>();
  const audio = new Observations<AudioFrame>();
  const delivered = { video: 0n, audio: 0n };

  const signal = (event: PeerEvent): void => {
    if (!closed) emit?.(event);
  };
  const receiving = (kind: "video" | "audio") =>
    mapping.find((track) => track.kind === kind && track.direction === "recvonly");
  const publish = (value: VideoFrame | AudioFrame, bytes: number): void => {
    const kind = value._tag === "VideoFrame" ? "video" : "audio";
    const track = receiving(kind);
    if (closed || track === undefined) return;
    if (value._tag === "VideoFrame") video.emit(value, bytes);
    else audio.emit(value, bytes);
    if (delivered[kind]++ === 0n)
      signal({ type: "decoded", kind, name: track.name, mid: track.mid });
  };
  const stream = <A>(kind: "video" | "audio", observations: Observations<A>, name: string) =>
    Stream.unwrap(
      Effect.suspend(() =>
        receiving(kind)?.name === name
          ? Effect.succeed(observations.stream({ capacity: 512, maxBytes: 64 * 1024 * 1024 }))
          : Effect.fail(unsupported(`no receive ${kind} track named ${name}`)),
      ),
    );
  const close = (): void => {
    if (closed) return;
    closed = true;
    opened = false;
    video.end();
    audio.end();
  };
  const link = (peerId: string): Link => ({
    id: peerId,
    open: Effect.sync(() => {
      opened = !closed;
      signal({ type: "state", state: "connected" });
      signal({ type: "channel", channel: "control", open: true });
      signal({ type: "channel", channel: "data", open: true });
      for (const track of mapping)
        if (track.direction === "recvonly")
          signal({ type: "track", name: track.name, mid: track.mid });
    }),
    drop: (reason) =>
      Effect.sync(() => {
        if (reason === "disconnected") signal({ type: "state", state: "disconnected" });
        // An ended session closes both channels, as when SCTP ends.
        else
          for (const channel of ["control", "data"] as const)
            signal({ type: "channel", channel, open: false });
        close();
      }),
    deliver: (channel, bytes) =>
      Effect.gen(function* () {
        const due = (yield* monotonic) + (yield* sessions.timing.delay("channel"));
        return inbound === undefined ? false : yield* Queue.offer(inbound, { channel, bytes, due });
      }),
    video: (frame) =>
      Effect.sync(() =>
        publish(
          {
            _tag: "VideoFrame",
            track: receiving("video")?.name ?? profile.tracks.video,
            width: sessions.options.width,
            height: sessions.options.height,
            frameId: frame.frameId,
            timestampMicros: frame.timestampMicros,
            sequence: delivered.video,
            format: "BGRA",
            data: frame.data,
            metadata: new Uint8Array(0),
          },
          frame.data.byteLength,
        ),
      ),
    audio: (samples) =>
      Effect.sync(() =>
        publish(
          {
            _tag: "AudioFrame",
            track: receiving("audio")?.name ?? profile.tracks.audio,
            sampleRate: profile.audio.sampleRate,
            channels: profile.audio.channels,
            sequence: delivered.audio,
            samples,
          },
          samples.byteLength,
        ),
      ),
  });
  /** Deliver each queued message in order, once its latency has passed. */
  const drain = (
    queue: Queue.Queue<Delivery>,
    deliver: (delivery: Delivery) => Effect.Effect<void>,
  ) =>
    take(queue).pipe(
      Effect.flatMap((delivery) => until(delivery.due).pipe(Effect.andThen(deliver(delivery)))),
      Effect.forever,
      Effect.forkScoped,
    );

  return {
    media: {
      _tag: "Decoded",
      video: (name: string) => stream("video", video, name),
      audio: (name: string) => stream("audio", audio, name),
      pressure: Effect.sync(() => ({
        closed,
        queuedControl: 0,
        queuedVideo: 0,
        queuedAudio: 0,
        queuedBytes: 0,
        droppedVideo: 0n,
        droppedAudio: 0n,
        pendingRequests: 0,
        deliveredVideo: delivered.video,
        deliveredAudio: delivered.audio,
        readerOverflows: video.overflowCount + audio.overflowCount,
      })),
    },
    prepare: (_servers, tracks, onEvent) =>
      Effect.gen(function* () {
        const peerId = yield* sessions.nextPeer;
        id = peerId;
        emit = onEvent;
        mapping = tracks.map((track, index) => ({ ...track, mid: String(index) }));
        inbound = yield* Queue.unbounded<Delivery>();
        outbound = yield* Queue.unbounded<Delivery>();
        yield* Effect.acquireRelease(sessions.attach(link(peerId)), () => sessions.detach(peerId));
        yield* drain(inbound, ({ channel, bytes }) =>
          Effect.sync(() => signal({ type: "message", channel, bytes })),
        );
        yield* drain(outbound, ({ channel, bytes }) => sessions.receive(peerId, channel, bytes));
        signal({
          type: "ice",
          candidate: { candidate: "candidate:1 1 udp 2122260223 127.0.0.1 9 typ host" },
        });
        signal({ type: "ice" });
        return { sdp: offer(peerId, mapping), mapping };
      }),
    answer: (sdp) =>
      Effect.suspend(() => {
        if (closed || id === undefined)
          return Effect.fail(ReactorError.fromCode("Closed", "ReactorTest peer closed"));
        if (!sdp.includes(`a=ice-ufrag:${id}`))
          return Effect.fail(ReactorError.fromCode("SdpRejected", "the answer names another peer"));
        signal({ type: "state", state: "connecting" });
        return sessions.answered(id);
      }),
    send: (channel, bytes) =>
      Effect.suspend(() => {
        const queue = outbound;
        if (closed || !opened || queue === undefined)
          return Effect.fail(
            ReactorError.fromCode("ChannelClosed", `${channel} channel closed`, {
              detail: { channel },
              outcome: "not-submitted",
            }),
          );
        return Effect.gen(function* () {
          const due = (yield* monotonic) + (yield* sessions.timing.delay("channel"));
          yield* Queue.offer(queue, { channel, bytes: new Uint8Array(bytes), due });
        });
      }),
    close: Effect.sync(close),
    direction: () => Effect.void,
    maxBitrate: () => Effect.void,
    stats: Effect.succeed([]),
  };
};
