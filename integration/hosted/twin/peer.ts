/**
 * The twin's host peer: the public `Peer` contract, as the native host
 * implements it, over the twin server's link instead of WebRTC. Channel bytes
 * travel over HTTP, a long poll brings the server's messages and model state,
 * and the peer decodes that state into BGRA frames and PCM blocks itself. The
 * model lives in the server, so any process that knows the twin URL can drive
 * a session, and one that is killed simply stops polling.
 */
import { randomUUID } from "node:crypto";
import * as Clock from "effect/Clock";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import type * as Scope from "effect/Scope";
import * as Semaphore from "effect/Semaphore";
import * as Stream from "effect/Stream";
import * as HttpClient from "effect/unstable/http/HttpClient";
import * as HttpClientRequest from "effect/unstable/http/HttpClientRequest";
import { FetchHttp, IceFailed, PeerFactory, ReactorError } from "reactor-effect-client";
import type { Mapping, Track } from "reactor-effect-client";
import { Observations } from "reactor-effect-client/host";
import type {
  AudioFrame,
  Channel,
  IceServer,
  MediaPressure,
  MediaTrack,
  Peer,
  PeerEvent,
  Prepared,
  RawMedia,
  VideoFrame,
} from "reactor-effect-client/host";
import { Events, candidateLine, offer, peerOf } from "./protocol.js";
import type { Candidate, Media, ServerEvent } from "./protocol.js";

const width = 160;
const height = 90;
const fps = 24;
/** The H3 profile's audio: 48 kHz mono, delivered in 10 ms blocks. */
const sampleRate = 48_000;
const channels = 1;
const blockSamples = sampleRate / 100;
/** The media clock releases whatever came due since its last tick. */
const tick = "10 millis";
/** A tick releases at most this many frames or blocks; older ones, due after a stall, are dropped. */
const videoBurst = 4;
const audioBurst = 16;
/** Nominal encoded sizes, for the byte counters a real transport would report. */
const videoPacketBytes = 1200;
const audioPacketBytes = 80;

/** A 32-bit FNV-1a hash, so each clip has its own colour and tone. */
const hash = (text: string): number => {
  let value = 0x811c9dc5;
  for (let index = 0; index < text.length; index++) {
    value ^= text.charCodeAt(index);
    value = Math.imul(value, 0x01000193);
  }
  return value >>> 0;
};

/** One frame of a clip: the clip's own colour, crossed by a bar that moves every frame. */
const render = (clip: string, frame: number): Uint8Array<ArrayBuffer> => {
  const seed = hash(clip);
  const colour = [64 + (seed & 0x7f), 64 + ((seed >>> 8) & 0x7f), 64 + ((seed >>> 16) & 0x7f)];
  const bar = (frame * 3) % width;
  const row = new Uint8Array(width * 4);
  for (let x = 0; x < width; x++) {
    const lit = (x - bar + width) % width < 8;
    for (let channel = 0; channel < 3; channel++)
      row[x * 4 + channel] = lit ? 255 - colour[channel]! : colour[channel]!;
    row[x * 4 + 3] = 255;
  }
  const data = new Uint8Array(width * height * 4);
  for (let y = 0; y < height; y++) data.set(row, y * row.byteLength);
  return data;
};

const black = (): Uint8Array<ArrayBuffer> => {
  const data = new Uint8Array(width * height * 4);
  for (let index = 3; index < data.length; index += 4) data[index] = 255;
  return data;
};

/** A block of a clip's tone, continuous across blocks from sample `first`. */
const tone = (clip: string, first: number): Int16Array<ArrayBuffer> => {
  const samples = new Int16Array(blockSamples * channels);
  const frequency = 220 + (hash(clip) % 440);
  for (let index = 0; index < blockSamples; index++) {
    const value = Math.round(
      6000 * Math.sin((2 * Math.PI * frequency * (first + index)) / sampleRate),
    );
    for (let channel = 0; channel < channels; channel++)
      samples[index * channels + channel] = value;
  }
  return samples;
};

const closedChannel = (channel: Channel): ReactorError =>
  ReactorError.fromCode("ChannelClosed", `${channel} channel closed`, {
    detail: { channel },
    outcome: "not-submitted",
  });

class TwinPeer implements Peer {
  readonly nativeTracks = false;
  readonly mediaSupported = true;
  readonly rawMedia: RawMedia;
  private readonly id = randomUUID();
  private readonly base: string;
  /** Channel messages are ordered, so sends go one at a time. */
  private readonly gate = Semaphore.makeUnsafe(1);
  private readonly answered = Deferred.makeUnsafe<void>();
  private readonly opened = Deferred.makeUnsafe<void>();
  private readonly stopped = Deferred.makeUnsafe<void>();
  private readonly video = new Map<string, Observations<VideoFrame>>();
  private readonly audio = new Map<string, Observations<AudioFrame>>();
  private readonly decoded = new Set<string>();
  private receiving: readonly Mapping[] = [];
  private gathered: readonly Candidate[] = [];
  private emit: ((event: PeerEvent) => void) | undefined;
  private closed = false;
  private failure: ReactorError | undefined;
  private media: { readonly state: Media; readonly anchor: number } | undefined;
  private pair: { readonly local: Candidate; readonly remote: Candidate } | undefined;
  private roundTrip: number | undefined;
  private jitter = 0;
  private lateness: number | undefined;
  private readonly recentFrames: number[] = [];
  private bytesSent = 0n;
  private bytesReceived = 0n;
  private pending = 0;
  private readonly sequence = { video: 0n, audio: 0n };
  private readonly delivered = { video: 0n, audio: 0n };
  private readonly dropped = { video: 0n, audio: 0n };
  private readonly queued = { control: 0, video: 0, audio: 0 };

  constructor(
    apiUrl: string,
    private readonly http: HttpClient.HttpClient,
  ) {
    this.base = apiUrl.replace(/\/$/, "");
    this.rawMedia = Object.freeze({
      video: (name: string) =>
        this.stream(this.video, name, "video", { capacity: 4, maxBytes: 64 * 1024 * 1024 }),
      audio: (name: string) =>
        this.stream(this.audio, name, "audio", { capacity: 128, maxBytes: 4 * 1024 * 1024 }),
      snapshot: Effect.sync(() => this.pressure()),
    });
  }

  private stream<A>(
    feeds: Map<string, Observations<A>>,
    name: string,
    kind: "video" | "audio",
    bounds: { readonly capacity: number; readonly maxBytes: number },
  ): Stream.Stream<A, ReactorError> {
    return Stream.unwrap(
      Effect.suspend(() =>
        this.receiving.some((track) => track.name === name && track.kind === kind)
          ? Effect.succeed(this.feed(feeds, name).stream(bounds))
          : Effect.fail(
              ReactorError.fromCode(
                "InvalidInput",
                `twin media needs a declared receive ${kind} track`,
                { outcome: "not-submitted" },
              ),
            ),
      ),
    );
  }

  private feed<A>(feeds: Map<string, Observations<A>>, name: string): Observations<A> {
    let feed = feeds.get(name);
    if (feed === undefined) {
      feed = new Observations<A>();
      feeds.set(name, feed);
      if (this.failure !== undefined) feed.fail(this.failure);
      else if (this.closed) feed.end();
    }
    return feed;
  }

  prepare(
    servers: readonly IceServer[],
    tracks: readonly Track[],
    emit: (event: PeerEvent) => void,
  ): Effect.Effect<Prepared, ReactorError, Scope.Scope> {
    const self = this;
    return Effect.gen(function* () {
      const mapping = tracks.map((track, index): Mapping =>
        Object.freeze({ ...track, mid: String(index) }),
      );
      self.receiving = mapping.filter((track) => track.direction === "recvonly");
      self.emit = emit;
      // A TURN server with credentials gives a relay candidate beside the host one.
      const relay = servers.some(
        (server) =>
          (typeof server.urls === "string" ? [server.urls] : server.urls).some((url) =>
            /^turns?:/.test(url),
          ) &&
          server.username !== undefined &&
          typeof server.credential === "string",
      );
      const port = 40_000 + (Number.parseInt(self.id.slice(0, 4), 16) % 20_000);
      self.gathered = [
        { type: "host", address: "127.0.0.1", port },
        ...(relay ? [{ type: "relay" as const, address: "127.0.0.1", port: port + 1 }] : []),
      ];
      for (const [index, candidate] of self.gathered.entries())
        emit({
          type: "ice",
          candidate: {
            candidate: candidateLine(index + 1, candidate),
            sdp_mid: "0",
            sdp_mline_index: 0,
          },
        });
      emit({ type: "ice" });
      yield* Effect.forkScoped(self.poll);
      yield* Effect.forkScoped(self.clock);
      return Object.freeze({ sdp: offer(self.id, mapping), mapping: Object.freeze(mapping) });
    });
  }

  answer(sdp: string): Effect.Effect<void, ReactorError> {
    return Effect.suspend(() => {
      if (this.closed)
        return Effect.fail(this.failure ?? ReactorError.fromCode("Closed", "twin peer closed"));
      if (peerOf(sdp) !== this.id)
        return Effect.fail(
          ReactorError.fromCode("SdpRejected", "the answer names another twin peer"),
        );
      Deferred.doneUnsafe(this.answered, Effect.void);
      this.emit?.({ type: "state", state: "connecting" });
      return Effect.void;
    });
  }

  send(channel: Channel, bytes: Uint8Array<ArrayBuffer>): Effect.Effect<void, ReactorError> {
    const self = this;
    const post = Effect.gen(function* () {
      if (self.closed) return yield* closedChannel(channel);
      const started = yield* Clock.currentTimeNanos;
      const response = yield* self.http
        .execute(
          HttpClientRequest.post(`${self.base}/twin/peers/${self.id}/channels/${channel}`).pipe(
            HttpClientRequest.bodyUint8Array(bytes, "application/octet-stream"),
          ),
        )
        .pipe(
          Effect.mapError((cause) =>
            ReactorError.fromCode("Disconnected", `twin ${channel} send failed`, {
              detail: cause,
              outcome: "unknown",
            }),
          ),
        );
      yield* Effect.ignore(response.arrayBuffer);
      // The server refused delivery: the model never saw the message.
      if (response.status === 404 || response.status === 409 || response.status === 410)
        return yield* closedChannel(channel);
      if (response.status !== 204)
        return yield* ReactorError.fromCode("Protocol", `twin ${channel} send refused`, {
          detail: { status: response.status },
          outcome: "not-submitted",
        });
      const finished = yield* Clock.currentTimeNanos;
      const seconds = Number(finished - started) / 1e9;
      self.roundTrip =
        self.roundTrip === undefined ? seconds : self.roundTrip * 0.8 + seconds * 0.2;
      self.bytesSent += BigInt(bytes.byteLength);
    });
    return Effect.suspend(() => {
      self.pending++;
      return self.gate.withPermit(post).pipe(
        Effect.ensuring(
          Effect.sync(() => {
            self.pending--;
          }),
        ),
      );
    });
  }

  /** The server's events after `after`, held by the server until there are some. */
  private events(after: number): Effect.Effect<readonly ServerEvent[], ReactorError> {
    return this.http
      .execute(HttpClientRequest.get(`${this.base}/twin/peers/${this.id}/events?after=${after}`))
      .pipe(
        Effect.mapError((cause) =>
          ReactorError.fromCode("Disconnected", "the twin link failed", { detail: cause }),
        ),
        Effect.flatMap((response) =>
          response.status === 200
            ? response.json.pipe(
                Effect.flatMap(Schema.decodeUnknownEffect(Events)),
                Effect.map((batch) => batch.events),
                Effect.mapError((cause) =>
                  ReactorError.fromCode("Protocol", "the twin sent an invalid event batch", {
                    detail: cause,
                  }),
                ),
              )
            : Effect.ignore(response.arrayBuffer).pipe(
                Effect.andThen(
                  Effect.fail(
                    ReactorError.fromCode("Disconnected", "the twin closed the link", {
                      detail: { status: response.status },
                    }),
                  ),
                ),
              ),
        ),
      );
  }

  private get poll(): Effect.Effect<void> {
    const self = this;
    return Effect.gen(function* () {
      yield* Deferred.await(self.answered);
      let after = 0;
      while (!self.closed) {
        const events = yield* self.events(after);
        const now = yield* Clock.currentTimeMillis;
        self.queued.control = events.length;
        for (const event of events) {
          self.queued.control--;
          after = event.seq;
          self.deliver(event, now);
          if (self.closed) return;
        }
      }
    }).pipe(
      Effect.catch((error) => Effect.sync(() => self.fail(error))),
      Effect.raceFirst(Deferred.await(self.stopped)),
    );
  }

  private deliver(event: ServerEvent, now: number): void {
    switch (event.type) {
      case "open":
        this.pair = { local: event.local, remote: event.remote };
        this.emit?.({ type: "state", state: "connected" });
        this.emit?.({ type: "channel", channel: "control", open: true });
        this.emit?.({ type: "channel", channel: "data", open: true });
        for (const track of this.receiving)
          this.emit?.({ type: "track", name: track.name, mid: track.mid });
        Deferred.doneUnsafe(this.opened, Effect.void);
        return;
      case "message":
        this.bytesReceived += BigInt(event.data.byteLength);
        this.emit?.({ type: "message", channel: event.channel, bytes: event.data });
        return;
      case "media":
        this.media = { state: event.media, anchor: now - (event.media.clip?.elapsedMs ?? 0) };
        return;
      case "closed":
        if (event.reason === "unreachable")
          return this.fail(
            new ReactorError({
              reason: new IceFailed({
                message: "no twin candidate pair worked",
                pairs: 0,
                candidateTypes: [...new Set(this.gathered.map((candidate) => candidate.type))],
              }),
            }),
          );
        // The server ended the connection: both channels close, as when the remote ends SCTP.
        this.emit?.({ type: "channel", channel: "control", open: false });
        this.emit?.({ type: "channel", channel: "data", open: false });
        this.close();
        return;
    }
  }

  /** Decode what came due since the last tick, at the pace of the media clock. */
  private get clock(): Effect.Effect<void> {
    const self = this;
    return Effect.gen(function* () {
      yield* Deferred.await(self.opened);
      const start = yield* Clock.currentTimeMillis;
      let frames = 0;
      let blocks = 0;
      while (!self.closed) {
        yield* Effect.sleep(tick);
        const now = yield* Clock.currentTimeMillis;
        const dueFrames = Math.floor(((now - start) * fps) / 1000);
        const dueBlocks = Math.floor((now - start) / 10);
        const video = self.dueVideo(dueFrames - frames, now, start + (dueFrames * 1000) / fps);
        const audio = self.dueAudio(dueBlocks - blocks, now);
        frames = dueFrames;
        blocks = dueBlocks;
        self.queued.video = video.length;
        for (const frame of video) {
          self.queued.video--;
          self.publish(self.feed(self.video, frame.track), frame, frame.data.byteLength, "video");
          yield* Effect.yieldNow;
        }
        self.queued.audio = audio.length;
        for (const block of audio) {
          self.queued.audio--;
          self.publish(
            self.feed(self.audio, block.track),
            block,
            block.samples.byteLength,
            "audio",
          );
          yield* Effect.yieldNow;
        }
      }
    }).pipe(Effect.raceFirst(Deferred.await(self.stopped)));
  }

  private publish<A extends { readonly track: string }>(
    feed: Observations<A>,
    value: A,
    bytes: number,
    kind: "video" | "audio",
  ): void {
    if (this.closed) return;
    feed.emit(value, bytes + value.track.length * 2);
    this.delivered[kind]++;
    if (!this.decoded.has(value.track)) {
      this.decoded.add(value.track);
      const mid = this.receiving.find((track) => track.name === value.track)?.mid ?? "";
      this.emit?.({ type: "decoded", kind, name: value.track, mid });
    }
  }

  /** The frames due this tick; a bounded queue keeps the newest few, as a real one drops. */
  private dueVideo(count: number, now: number, dueAt: number): VideoFrame[] {
    const track = this.receiving.find((entry) => entry.kind === "video");
    const media = this.media;
    if (track === undefined || count <= 0 || media?.state.paused.includes(track.name) !== false)
      return [];
    const dropped = Math.max(0, count - videoBurst);
    this.dropped.video += BigInt(dropped);
    this.sequence.video += BigInt(dropped);
    // Jitter as RFC 3550 estimates it: the smoothed change in how late frames are released.
    const late = (now - dueAt) / 1000;
    if (this.lateness !== undefined)
      this.jitter += (Math.abs(late - this.lateness) - this.jitter) / 16;
    this.lateness = late;
    this.recentFrames.push(...Array.from({ length: count - dropped }, () => now));
    while (this.recentFrames.length > 0 && this.recentFrames[0]! <= now - 1000)
      this.recentFrames.shift();
    return Array.from({ length: count - dropped }, (_, index) => {
      const at = now - ((count - dropped - 1 - index) * 1000) / fps;
      const { data, frame } = this.picture(media.state, media.anchor, at);
      return Object.freeze({
        _tag: "VideoFrame" as const,
        track: track.name,
        width,
        height,
        frameId: frame === undefined ? 0n : BigInt(frame + 1),
        timestampMicros: BigInt(Math.round(at * 1000)),
        sequence: this.sequence.video++,
        format: "BGRA" as const,
        data,
        metadata: new Uint8Array(0),
      });
    });
  }

  private picture(
    state: Media,
    anchor: number,
    at: number,
  ): { readonly data: Uint8Array<ArrayBuffer>; readonly frame?: number } {
    if (state.video === "black") return { data: black() };
    if (state.clip !== null) {
      const frame =
        state.video === "frozen" ? 0 : Math.max(0, Math.floor(((at - anchor) * fps) / 1000));
      return { data: render(state.clip.id, frame), frame };
    }
    if (state.hold !== null) return { data: render(state.hold.id, state.hold.frame) };
    return { data: black() };
  }

  private dueAudio(count: number, now: number): AudioFrame[] {
    const track = this.receiving.find((entry) => entry.kind === "audio");
    const media = this.media;
    if (
      track === undefined ||
      count <= 0 ||
      media?.state.audio !== true ||
      media.state.paused.includes(track.name)
    )
      return [];
    const dropped = Math.max(0, count - audioBurst);
    this.dropped.audio += BigInt(dropped);
    this.sequence.audio += BigInt(dropped);
    const clip = media.state.clip;
    return Array.from({ length: count - dropped }, (_, index) => {
      const at = now - (count - dropped - 1 - index) * 10;
      return Object.freeze({
        _tag: "AudioFrame" as const,
        track: track.name,
        sampleRate,
        channels,
        sequence: this.sequence.audio++,
        samples:
          clip === null
            ? new Int16Array(blockSamples * channels)
            : tone(clip.id, Math.floor(((at - media.anchor) * sampleRate) / 1000)),
      });
    });
  }

  private pressure(): MediaPressure {
    let overflows = 0n;
    for (const feed of this.video.values()) overflows += feed.overflowCount;
    for (const feed of this.audio.values()) overflows += feed.overflowCount;
    return Object.freeze({
      closed: this.closed,
      queuedControl: this.queued.control,
      queuedVideo: this.queued.video,
      queuedAudio: this.queued.audio,
      queuedBytes: this.queued.video * width * height * 4 + this.queued.audio * blockSamples * 2,
      droppedVideo: this.dropped.video,
      droppedAudio: this.dropped.audio,
      pendingRequests: this.pending,
      deliveredVideo: this.delivered.video,
      deliveredAudio: this.delivered.audio,
      readerOverflows: overflows,
    });
  }

  get stats(): Effect.Effect<readonly unknown[], ReactorError> {
    const self = this;
    return Effect.gen(function* () {
      const timestamp = yield* Clock.currentTimeMillis;
      const pair = self.pair;
      if (pair === undefined) return Object.freeze([]);
      const video = self.receiving.find((track) => track.kind === "video");
      const audio = self.receiving.find((track) => track.kind === "audio");
      const audioPackets = self.delivered.audio / 2n;
      const mediaBytes =
        self.delivered.video * BigInt(videoPacketBytes) + audioPackets * BigInt(audioPacketBytes);
      const candidate = (id: string, type: string, entry: Candidate) => ({
        id,
        type,
        timestamp,
        candidateType: entry.type,
        address: entry.address,
        port: entry.port,
        protocol: "udp",
      });
      return Object.freeze([
        {
          id: "CPtwin",
          type: "candidate-pair",
          timestamp,
          state: "succeeded",
          nominated: true,
          localCandidateId: "ILtwin",
          remoteCandidateId: "IRtwin",
          bytesSent: self.bytesSent,
          bytesReceived: self.bytesReceived + mediaBytes,
          ...(self.roundTrip === undefined ? {} : { currentRoundTripTime: self.roundTrip }),
          availableOutgoingBitrate: 2_500_000,
        },
        candidate("ILtwin", "local-candidate", pair.local),
        candidate("IRtwin", "remote-candidate", pair.remote),
        ...(video === undefined
          ? []
          : [
              {
                id: "ITvideo",
                type: "inbound-rtp",
                timestamp,
                kind: "video",
                mediaType: "video",
                mid: video.mid,
                trackIdentifier: video.name,
                packetsReceived: Number(self.delivered.video),
                packetsLost: 0,
                jitter: self.jitter,
                framesDecoded: Number(self.delivered.video),
                framesDropped: Number(self.dropped.video),
                framesPerSecond: self.recentFrames.length,
                frameWidth: width,
                frameHeight: height,
                bytesReceived: Number(self.delivered.video) * videoPacketBytes,
              },
            ]),
        ...(audio === undefined
          ? []
          : [
              {
                id: "ITaudio",
                type: "inbound-rtp",
                timestamp,
                kind: "audio",
                mediaType: "audio",
                mid: audio.mid,
                trackIdentifier: audio.name,
                packetsReceived: Number(audioPackets),
                packetsLost: 0,
                jitter: 0.001,
                bytesReceived: Number(audioPackets) * audioPacketBytes,
              },
            ]),
      ]);
    });
  }

  private fail(error: ReactorError): void {
    if (this.closed) return;
    this.failure = error;
    // The session closes this peer as it handles the error; closing again is a no-op.
    try {
      this.emit?.({ type: "error", error });
    } finally {
      this.close();
    }
  }

  close(): void {
    if (this.closed) return;
    this.closed = true;
    this.emit = undefined;
    Deferred.doneUnsafe(this.stopped, Effect.void);
    for (const feed of this.video.values())
      if (this.failure === undefined) feed.end();
      else feed.fail(this.failure);
    for (const feed of this.audio.values())
      if (this.failure === undefined) feed.end();
      else feed.fail(this.failure);
  }

  /** Close, and tell the server this side is gone rather than wait for the poll to lapse. */
  get shutdown(): Effect.Effect<void, ReactorError> {
    return Effect.suspend(() => {
      this.close();
      if (!Deferred.isDoneUnsafe(this.answered)) return Effect.void;
      return this.http
        .execute(HttpClientRequest.delete(`${this.base}/twin/peers/${this.id}`))
        .pipe(Effect.flatMap((response) => response.arrayBuffer));
    }).pipe(Effect.timeout("1 second"), Effect.ignore);
  }

  lease(): MediaTrack {
    throw ReactorError.fromCode(
      "UnsupportedCapability",
      "the twin peer exposes decoded frames, not browser track leases",
      { outcome: "not-submitted" },
    );
  }

  release(): void {
    /* lease never succeeds on this host. */
  }

  /** The session notifies the model; the transceiver direction itself is not modelled. */
  direction(): Effect.Effect<void, ReactorError> {
    return Effect.void;
  }

  replace(): Effect.Effect<void, ReactorError> {
    return Effect.fail(
      ReactorError.fromCode("UnsupportedCapability", "the twin peer publishes no tracks", {
        outcome: "not-submitted",
      }),
    );
  }

  maxBitrate(_name: string, bitsPerSecond: number): Effect.Effect<void, ReactorError> {
    return Number.isSafeInteger(bitsPerSecond) && bitsPerSecond >= 1 && bitsPerSecond <= 0x7fffffff
      ? Effect.void
      : Effect.fail(
          ReactorError.fromCode("InvalidInput", "max bitrate must be an integer in 1..2147483647", {
            outcome: "not-submitted",
          }),
        );
  }
}

/**
 * Twin peers for the client layer, in place of the native host:
 * `Reactor.layer({ apiUrl }).pipe(Layer.provide(twinPeers(apiUrl)))`. Media
 * comes from `mediaGeneration(session)`, the accessor `Native.media` wraps.
 */
export const twinPeers = (apiUrl: string): Layer.Layer<PeerFactory> =>
  Layer.effect(
    PeerFactory,
    Effect.gen(function* () {
      const http = yield* HttpClient.HttpClient;
      return PeerFactory.of({ make: () => new TwinPeer(apiUrl, http) });
    }),
  ).pipe(Layer.provide(FetchHttp.layer));
