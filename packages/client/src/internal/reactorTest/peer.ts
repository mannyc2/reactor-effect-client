/**
 * A test peer: the host `Peer` contract over the simulated Reactor instead of WebRTC, with
 * channel messages delivered in order after the channel latency.
 */
import * as Effect from "effect/Effect";
import * as Queue from "effect/Queue";
import * as Stream from "effect/Stream";
import type { Mapping } from "../../Peer.js";
import { ReactorError } from "../../ReactorError.js";
import { trackFeed } from "../../Peer.js";
import type { DataChannel, Peer, PeerEvent } from "../../Peer.js";
import type { AudioFrame, VideoFrame } from "../../Media.js";
import { documented } from "./h3.js";
import { monotonic, until } from "./runner.js";
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
  /**
   * The remote ended the connection. An ended or replaced one closes after
   * what was already sent to it, as the platform tells clients why a session
   * ended before their connection closes; a lost one drops at once.
   */
  readonly drop: (reason: "ended" | "replaced" | "disconnected") => Effect.Effect<void>;
  /** A channel message, delivered after the channel latency. */
  readonly deliver: (channel: DataChannel, bytes: Uint8Array<ArrayBuffer>) => Effect.Effect<void>;
  readonly video: (frame: Frame) => Effect.Effect<void>;
  readonly audio: (samples: Int16Array<ArrayBuffer>) => Effect.Effect<void>;
}

interface Delivery {
  readonly channel: DataChannel;
  readonly bytes: Uint8Array<ArrayBuffer>;
  readonly due: number;
}

/** What reaches the peer in order: a channel message, or its channels closing. */
type Inbound =
  | (Delivery & { readonly _tag: "Message" })
  | { readonly _tag: "End"; readonly due: 0 };

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

/** Each reader holds at most this many frames and bytes before it fails, as a host's reader does. */
const readerBounds = { capacity: 512, maxBytes: 64 * 1024 * 1024 };

/**
 * One test peer. Its connection state is local to it; everything that waits
 * is an Effect.
 */
export const make = Effect.fnUntraced(function* (sessions: Sessions) {
  let emit: ((event: PeerEvent) => void) | undefined;
  let id: string | undefined;
  let mapping: ReadonlyArray<Mapping> = [];
  let inbound: Queue.Queue<Inbound> | undefined;
  let outbound: Queue.Queue<Delivery> | undefined;
  let opened = false;
  let closed = false;
  const video = yield* trackFeed<VideoFrame>({
    ...readerBounds,
    bytes: (frame) => frame.data.byteLength,
  });
  const audio = yield* trackFeed<AudioFrame>({
    ...readerBounds,
    bytes: (frame) => frame.samples.byteLength,
  });
  const delivered = { video: 0n, audio: 0n };
  let receivedBytes = 0;

  const signal = (event: PeerEvent): void => {
    if (!closed) emit?.(event);
  };
  const receiving = (kind: "video" | "audio") =>
    mapping.find((track) => track.kind === kind && track.direction === "recvonly");
  const publish = (value: VideoFrame | AudioFrame) =>
    Effect.suspend(() => {
      const kind = value._tag === "VideoFrame" ? "video" : "audio";
      const track = receiving(kind);
      if (closed || track === undefined) return Effect.void;
      if (delivered[kind]++ === 0n)
        signal({ type: "decoded", kind, name: track.name, mid: track.mid });
      receivedBytes +=
        value._tag === "VideoFrame" ? value.data.byteLength : value.samples.byteLength;
      return value._tag === "VideoFrame" ? video.publish(value) : audio.publish(value);
    });
  const stream = <A>(
    kind: "video" | "audio",
    feed: { readonly stream: Stream.Stream<A, ReactorError> },
    name: string,
  ) =>
    Stream.unwrap(
      Effect.suspend(() =>
        receiving(kind)?.name === name
          ? Effect.succeed(feed.stream)
          : Effect.fail(unsupported(`no receive ${kind} track named ${name}`)),
      ),
    );
  const close = Effect.suspend(() => {
    if (closed) return Effect.void;
    closed = true;
    opened = false;
    return Effect.andThen(video.end, audio.end);
  });
  /** An ended session closes both channels, as when SCTP ends. */
  const ended = Effect.sync(() => {
    for (const channel of ["control", "data"] as const)
      signal({ type: "channel", channel, open: false });
  }).pipe(Effect.andThen(close));
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
      Effect.suspend(() => {
        if (reason === "disconnected")
          return Effect.sync(() => signal({ type: "state", state: "disconnected" })).pipe(
            Effect.andThen(close),
          );
        return inbound === undefined
          ? ended
          : Effect.asVoid(Queue.offer(inbound, { _tag: "End", due: 0 }));
      }),
    deliver: (channel, bytes) =>
      Effect.gen(function* () {
        const due = (yield* monotonic) + (yield* sessions.timing.delay("channel"));
        return inbound === undefined
          ? false
          : yield* Queue.offer(inbound, { _tag: "Message", channel, bytes, due });
      }),
    video: (frame) =>
      publish({
        _tag: "VideoFrame",
        track: receiving("video")?.name ?? documented.tracks.video,
        width: sessions.options.width,
        height: sessions.options.height,
        frameId: frame.frameId,
        timestampMicros: frame.timestampMicros,
        sequence: delivered.video,
        format: "BGRA",
        data: frame.data,
        metadata: new Uint8Array(0),
      }),
    audio: (samples) =>
      publish({
        _tag: "AudioFrame",
        track: receiving("audio")?.name ?? documented.tracks.audio,
        sampleRate: documented.soundtrack.sampleRate,
        channels: documented.soundtrack.channels,
        sequence: delivered.audio,
        samples,
      }),
  });
  /** Deliver each queued message in order, once its latency has passed. */
  const drain = <A extends { readonly due: number }>(
    queue: Queue.Queue<A>,
    deliver: (delivery: A) => Effect.Effect<void>,
  ) =>
    Queue.take(queue).pipe(
      Effect.flatMap((delivery) => until(delivery.due).pipe(Effect.andThen(deliver(delivery)))),
      Effect.forever,
      Effect.forkScoped,
    );

  return {
    media: {
      _tag: "Decoded",
      video: (name: string) => stream("video", video, name),
      audio: (name: string) => stream("audio", audio, name),
      pressure: Effect.map(
        Effect.all([video.overflows, audio.overflows]),
        ([videoOverflows, audioOverflows]) => ({
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
          readerOverflows: videoOverflows + audioOverflows,
        }),
      ),
    },
    prepare: (_servers, tracks, onEvent) =>
      Effect.gen(function* () {
        const peerId = yield* sessions.nextPeer;
        id = peerId;
        emit = onEvent;
        mapping = tracks.map((track, index) => ({ ...track, mid: String(index) }));
        inbound = yield* Queue.unbounded<Inbound>();
        outbound = yield* Queue.unbounded<Delivery>();
        yield* Effect.acquireRelease(sessions.attach(link(peerId)), () => sessions.detach(peerId));
        yield* drain(inbound, (delivery) =>
          delivery._tag === "End"
            ? ended
            : Effect.sync(() =>
                signal({ type: "message", channel: delivery.channel, bytes: delivery.bytes }),
              ),
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
    close,
    direction: () => Effect.void,
    maxBitrate: () => Effect.void,
    // One selected pair, as a browser reports it, carrying every byte the peer received.
    stats: Effect.sync(() =>
      opened
        ? [
            { type: "transport", id: "T", selectedCandidatePairId: "P" },
            {
              type: "candidate-pair",
              id: "P",
              localCandidateId: "L",
              remoteCandidateId: "R",
              state: "succeeded",
              nominated: true,
              bytesSent: 0,
              bytesReceived: receivedBytes,
            },
            { type: "local-candidate", id: "L", candidateType: sessions.options.candidate },
            { type: "remote-candidate", id: "R", candidateType: "host" },
          ]
        : [],
    ),
  } satisfies Peer;
});
