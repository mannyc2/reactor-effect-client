import * as Cause from "effect/Cause";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Queue from "effect/Queue";
import * as Stream from "effect/Stream";
import * as Client from "../src/session/index.js";
import { mediaGeneration, trackGeneration } from "../src/host.js";
import type { AudioFrame, RawMedia, MediaPressure, VideoFrame } from "../src/session/media.js";
import { PeerFactory } from "../src/PeerFactory.js";
import { ReactorError } from "../src/errors.js";
import { FakeTrack, MockPeer, withFixture, type HttpFixture } from "./fixtures.js";
import { assert, equal, run, test } from "./harness.js";

const videoFrame = (marker: number): VideoFrame => ({
  _tag: "VideoFrame",
  track: "main_video",
  width: 1,
  height: 1,
  frameId: BigInt(marker),
  timestampMicros: BigInt(marker),
  data: new Uint8Array([marker, 0, 0, 255]),
  metadata: new Uint8Array(),
});
const audioFrame = (marker: number): AudioFrame => ({
  _tag: "AudioFrame",
  track: "main_audio",
  sampleRate: 48_000,
  channels: 1,
  samples: new Int16Array([marker]),
});

/** Deliberately retains queued samples on close to test session generation fencing. */
class MediaPeer extends MockPeer {
  readonly nativeTracks = true;
  readonly videos = Effect.runSync(Queue.dropping<VideoFrame, ReactorError>(8));
  readonly audios = Effect.runSync(Queue.dropping<AudioFrame, ReactorError>(8));
  private retired = false;
  private queuedVideo = 0;
  private queuedAudio = 0;
  private droppedVideo: bigint;
  private droppedAudio: bigint;

  constructor(fixture: HttpFixture, index: number) {
    super(fixture);
    this.droppedVideo = index === 0 ? 3n : 1n;
    this.droppedAudio = index === 0 ? 5n : 2n;
  }

  readonly rawMedia: RawMedia = {
    video: () =>
      Stream.fromQueue(this.videos).pipe(
        Stream.tap(() =>
          Effect.sync(() => {
            this.queuedVideo--;
          }),
        ),
      ),
    audio: () =>
      Stream.fromQueue(this.audios).pipe(
        Stream.tap(() =>
          Effect.sync(() => {
            this.queuedAudio--;
          }),
        ),
      ),
    snapshot: Effect.sync((): MediaPressure => ({
      closed: this.retired,
      queuedControl: 0,
      queuedVideo: this.queuedVideo,
      queuedAudio: this.queuedAudio,
      queuedBytes: this.queuedVideo * 4 + this.queuedAudio * 2,
      droppedVideo: this.droppedVideo,
      droppedAudio: this.droppedAudio,
      pendingRequests: 0,
      deliveredVideo: 0n,
      deliveredAudio: 0n,
    })),
  };

  offerVideo(marker: number): Effect.Effect<void> {
    return Effect.sync(() => {
      this.queuedVideo++;
    }).pipe(Effect.andThen(Queue.offer(this.videos, videoFrame(marker))), Effect.asVoid);
  }
  offerAudio(marker: number): Effect.Effect<void> {
    return Effect.sync(() => {
      this.queuedAudio++;
    }).pipe(Effect.andThen(Queue.offer(this.audios, audioFrame(marker))), Effect.asVoid);
  }
  override close(): void {
    super.close();
    if (this.retired) return;
    this.retired = true;
    this.droppedVideo += BigInt(this.queuedVideo);
    this.droppedAudio += BigInt(this.queuedAudio);
    this.queuedVideo = 0;
    this.queuedAudio = 0;
  }
}

const dependencies = (fixture: HttpFixture, peers: MediaPeer[]) => ({
  check: Effect.void,
  make: () => {
    const peer = new MediaPeer(fixture, peers.length);
    peers.push(peer);
    return peer;
  },
});

test("media generations: retirement preserves pressure evidence and fences queued old samples", () =>
  withFixture(async (fixture) => {
    const peers: MediaPeer[] = [];
    await run(
      Effect.scoped(
        Effect.gen(function* () {
          const factory = yield* Client.make({
            apiUrl: "https://coordinator.fixture",
            session: { heartbeatMs: 0 },
          });
          const session = yield* factory.createConnected({ model: "fixture/media" });
          const previous = yield* mediaGeneration(session);
          const old = peers[0];
          assert(old !== undefined);
          yield* old.offerVideo(1);
          yield* old.offerVideo(2);
          yield* old.offerAudio(1);
          const before = yield* previous.snapshot;
          equal(before.queuedVideo, 2);
          equal(before.queuedAudio, 1);

          yield* session.reconnect;
          const current = yield* mediaGeneration(session);
          const retired = yield* previous.snapshot;
          const active = yield* current.snapshot;
          equal(current.generation, previous.generation + 1n);
          equal(retired.closed, true);
          equal(active.queuedVideo, 0);
          equal(active.queuedAudio, 0);
          // The transport owns retirement loss. A caller can aggregate the two
          // explicit generations without an invisible second media queue.
          equal(retired.droppedVideo + active.droppedVideo, 6n);
          equal(retired.droppedAudio + active.droppedAudio, 8n);

          const obsolete = yield* Effect.result(
            Stream.runCollect(previous.video("main_video").pipe(Stream.take(1))),
          );
          assert(obsolete._tag === "Failure");
          const next = peers[1];
          assert(next !== undefined);
          yield* old.offerVideo(9);
          yield* old.offerAudio(9);
          yield* next.offerVideo(3);
          yield* next.offerAudio(3);
          equal(
            (yield* Stream.runCollect(current.video("main_video").pipe(Stream.take(1))))[0]
              ?.data[0],
            3,
          );
          equal(
            (yield* Stream.runCollect(current.audio("main_audio").pipe(Stream.take(1))))[0]
              ?.samples[0],
            3,
          );
        }),
      ).pipe(Effect.provideService(PeerFactory, dependencies(fixture, peers))),
    );
  }));

test("media generations: a failed frame source reaches a frame-only consumer", () =>
  withFixture(async (fixture) => {
    const peers: MediaPeer[] = [];
    await run(
      Effect.scoped(
        Effect.gen(function* () {
          const factory = yield* Client.make({
            apiUrl: "https://coordinator.fixture",
            session: { heartbeatMs: 0 },
          });
          const session = yield* factory.createConnected({ model: "fixture/media" });
          const media = yield* mediaGeneration(session);
          const reading = yield* Effect.forkScoped(
            Effect.result(Stream.runCollect(media.video("main_video"))),
          );
          yield* Effect.yieldNow;
          equal((yield* session.current).subscribers, 0);
          const sourceFailure = new ReactorError("Native", "decoded source fixture failed");
          const peer = peers[0];
          assert(peer !== undefined);
          yield* Effect.sync(() => Queue.failCauseUnsafe(peer.videos, Cause.fail(sourceFailure)));
          const result = yield* Fiber.join(reading).pipe(Effect.timeout("1 second"), Effect.orDie);
          assert(result._tag === "Failure");
          equal(result.failure, sourceFailure);
        }),
      ).pipe(Effect.provideService(PeerFactory, dependencies(fixture, peers))),
    );
  }));

test("media generations: reconnect retires a parked read without requiring a peer EOF", () =>
  withFixture(async (fixture) => {
    const peers: MediaPeer[] = [];
    await run(
      Effect.scoped(
        Effect.gen(function* () {
          const factory = yield* Client.make({
            apiUrl: "https://coordinator.fixture",
            session: { heartbeatMs: 0 },
          });
          const session = yield* factory.createConnected({ model: "fixture/media" });
          const media = yield* mediaGeneration(session);
          const reading = yield* Effect.forkScoped(
            Effect.result(Stream.runCollect(media.audio("main_audio"))),
          );
          yield* Effect.yieldNow;
          yield* session.reconnect;
          const result = yield* Fiber.join(reading).pipe(Effect.timeout("1 second"), Effect.orDie);
          assert(result._tag === "Failure");
          equal(result.failure.code, "Disconnected");
          equal((yield* session.ready).generation, media.generation + 1n);
        }),
      ).pipe(Effect.provideService(PeerFactory, dependencies(fixture, peers))),
    );
  }));

test("browser generation projection: retired capabilities cannot publish into a replacement", () =>
  withFixture(async (fixture) => {
    const peers: MediaPeer[] = [];
    await run(
      Effect.scoped(
        Effect.gen(function* () {
          const factory = yield* Client.make({
            apiUrl: "https://coordinator.fixture",
            session: { heartbeatMs: 0 },
          });
          const session = yield* factory.createConnected({ model: "fixture/browser-capability" });
          const old = yield* trackGeneration(session);
          const source = new FakeTrack("audio");
          const prepared = old.publish("input_audio", source);
          yield* session.reconnect;
          const result = yield* Effect.result(prepared);
          assert(result._tag === "Failure");
          equal(source.clones.length, 0);
          equal(peers[1]?.replacements.length, 0);
          const next = yield* trackGeneration(session);
          yield* next.publish("input_audio", source);
          equal(peers[1]?.replacements.length, 1);
        }),
      ).pipe(Effect.provideService(PeerFactory, dependencies(fixture, peers))),
    );
  }));
