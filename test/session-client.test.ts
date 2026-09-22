import * as Effect from "effect/Effect";
import * as Queue from "effect/Queue";
import * as Redacted from "effect/Redacted";
import * as Stream from "effect/Stream";
import * as SessionClient from "../src/SessionClient.js";
import type { AudioFrame, RawMedia, Snapshot, VideoFrame } from "../src/Model.js";
import { ReactorError } from "../src/errors.js";
import { MockPeer, withFixture } from "./fixtures.js";
import { assert, equal, run, test } from "./harness.js";

const pressure = (videoDrops: bigint, audioDrops: bigint): Snapshot => ({
  closed: false, queuedControl: 0, queuedVideo: 0, queuedAudio: 0, queuedBytes: 0,
  droppedVideo: videoDrops, droppedAudio: audioDrops, pendingRequests: 0,
  deliveredVideo: 0n, deliveredAudio: 0n,
});

test("decoded client: reconnect retires queued media and preserves cumulative loss observations", () => withFixture(async (fixture) => {
  const peers: Array<{
    readonly video: Queue.Queue<VideoFrame, ReactorError>;
    readonly audio: Queue.Queue<AudioFrame, ReactorError>;
  }> = [];
  await run(Effect.scoped(Effect.gen(function* () {
    const client = yield* SessionClient.make({
      check: Effect.void,
      make: () => {
        const video = Effect.runSync(Queue.dropping<VideoFrame, ReactorError>(8));
        const audio = Effect.runSync(Queue.dropping<AudioFrame, ReactorError>(8));
        const index = peers.length;
        peers.push({ video, audio });
        const rawMedia: RawMedia = {
          video: () => Stream.fromQueue(video),
          audio: () => Stream.fromQueue(audio),
          snapshot: Effect.succeed(index === 0 ? pressure(3n, 5n) : pressure(1n, 2n)),
        };
        return Object.assign(new MockPeer(fixture), { rawMedia });
      },
    }, {
      modelName: "fixture/media", jwt: Redacted.make("fixture-token"),
      apiUrl: "https://coordinator.fixture",
    });
    yield* client.connect();
    const old = peers[0];
    assert(old !== undefined);
    const videoFrame = (marker: number): VideoFrame => ({
      _tag: "VideoFrame", track: "main_video", width: 1, height: 1,
      frameId: BigInt(marker), timestampMicros: BigInt(marker),
      data: new Uint8Array([marker, 0, 0, 255]), metadata: new Uint8Array(),
    });
    const audioFrame = (marker: number): AudioFrame => ({
      _tag: "AudioFrame", track: "main_audio", sampleRate: 48_000, channels: 1,
      samples: new Int16Array([marker]),
    });
    yield* Queue.offer(old.video, videoFrame(1));
    yield* Queue.offer(old.video, videoFrame(2));
    yield* Queue.offer(old.audio, audioFrame(1));
    yield* Effect.gen(function* () {
      while (true) {
        const state = yield* client.snapshot;
        if (state.queuedVideo === 2 && state.queuedAudio === 1) break;
        yield* Effect.sleep(1);
      }
    }).pipe(Effect.timeoutOrElse({ duration: 1000, orElse: () => Effect.fail(new ReactorError("Timeout", "fixture media was not buffered")) }));

    yield* client.reconnect;
    const state = yield* client.snapshot;
    equal(state.queuedVideo, 0);
    equal(state.queuedAudio, 0);
    equal(state.droppedVideo, 6n); // 3 old native + 2 retired local + 1 new native.
    equal(state.droppedAudio, 8n); // 5 old native + 1 retired local + 2 new native.

    const current = peers[1];
    assert(current !== undefined);
    // Even a badly behaved retired test peer cannot re-enter the new stream.
    yield* Queue.offer(old.video, videoFrame(9));
    yield* Queue.offer(old.audio, audioFrame(9));
    yield* Queue.offer(current.video, videoFrame(3));
    yield* Queue.offer(current.audio, audioFrame(3));
    const frames = yield* Stream.runCollect(client.video.pipe(Stream.take(1)));
    const samples = yield* Stream.runCollect(client.audio.pipe(Stream.take(1)));
    equal(frames[0]?.data[0], 3);
    equal(samples[0]?.samples[0], 3);
  })));
}));
