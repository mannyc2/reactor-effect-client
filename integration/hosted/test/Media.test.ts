/** A later clip's moving picture cannot qualify an earlier black or frozen playback. */
import { assert, it } from "@effect/vitest";
import { videoLog } from "../Media.js";

it("bounds first frames and live video to the observed playback", () => {
  const video = videoLog();
  const add = (atMs: number, value: number) =>
    video.add(
      {
        _tag: "Frame",
        frame: {
          _tag: "VideoFrame",
          track: "main_video",
          width: 1,
          height: 1,
          frameId: BigInt(atMs),
          timestampMicros: BigInt(atMs) * 1_000n,
          sequence: BigInt(atMs),
          format: "BGRA",
          data: new Uint8Array([value, value, value, 255]),
          metadata: new Uint8Array(),
        },
      },
      atMs,
    );
  for (let at = 10; at < 20; at++) add(at, 0);
  for (let at = 20; at < 30; at++) add(at, 128);
  for (let at = 30; at < 40; at++) add(at, at * 5);
  assert.isUndefined(video.firstAfter(1, 9));
  assert.isDefined(video.live(10, 19));
  assert.isDefined(video.live(20, 29));
  assert.isUndefined(video.live(30, 39));
  assert.isUndefined(video.live(10));
});
