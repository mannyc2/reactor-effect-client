import { describe, expect, test } from "vitest";
import { ReactorError } from "reactor-effect-client";
import type { Track } from "reactor-effect-client";
import { failureCode, type NativeAudio, type NativeVideo } from "../src/_internal/bridge.js";
import { nativePeerTesting } from "../src/_internal/peer.js";

const tracks: readonly Track[] = [
  { name: "main_video", kind: "video", direction: "recvonly" },
  { name: "main_audio", kind: "audio", direction: "recvonly" },
  { name: "input_video", kind: "video", direction: "sendonly" },
];

const video = (fields: Partial<NativeVideo> = {}): NativeVideo => ({
  track: 0,
  width: 1,
  height: 1,
  frameId: 18446744073709551615n,
  timestampMicros: 9007199254740993n,
  data: Uint8Array.of(1, 2, 3, 4),
  metadata: Uint8Array.of(9, 8, 7),
  ...fields,
});

const audio = (fields: Partial<NativeAudio> = {}): NativeAudio => ({
  track: 1,
  sampleRate: 48_000,
  channels: 2,
  samples: Int16Array.of(1, -2, 300, -400),
  ...fields,
});

const protocol = (body: () => unknown): void => {
  try {
    body();
  } catch (error) {
    expect(error).toBeInstanceOf(ReactorError);
    expect((error as ReactorError).code).toBe("Protocol");
    return;
  }
  throw new Error("expected Protocol failure");
};

const packet = (header: Record<string, unknown>) => ({ header, payload: new Uint8Array() });

describe("native media and event decoding", () => {
  test("names frames by their prepare index and hands over the taken bytes without copying", () => {
    const taken = video();
    const frame = nativePeerTesting.videoFrame(tracks, taken);
    expect(frame).toMatchObject({
      _tag: "VideoFrame",
      track: "main_video",
      width: 1,
      height: 1,
      frameId: 18446744073709551615n,
      timestampMicros: 9007199254740993n,
    });
    expect(frame.data).toBe(taken.data);
    expect(frame.metadata).toBe(taken.metadata);

    const block = audio();
    const samples = nativePeerTesting.audioFrame(tracks, block);
    expect(samples).toMatchObject({ track: "main_audio", sampleRate: 48_000, channels: 2 });
    expect(samples.samples).toBe(block.samples);
    expect([...samples.samples]).toEqual([1, -2, 300, -400]);
  });

  test("rejects frames outside their declared receive track or with inconsistent shapes", () => {
    for (const track of [1, 2, 3])
      protocol(() => nativePeerTesting.videoFrame(tracks, video({ track })));
    protocol(() => nativePeerTesting.videoFrame(tracks, video({ width: 2 })));
    protocol(() =>
      nativePeerTesting.videoFrame(tracks, video({ width: 0, data: new Uint8Array() })),
    );
    for (const track of [0, 2])
      protocol(() => nativePeerTesting.audioFrame(tracks, audio({ track })));
    protocol(() =>
      nativePeerTesting.audioFrame(tracks, audio({ samples: Int16Array.of(1, 2, 3) })),
    );
    protocol(() => nativePeerTesting.audioFrame(tracks, audio({ sampleRate: 0 })));
  });

  test("maps every native failure class and keeps its diagnostic out of the message", () => {
    expect([3, -1, -2, -3, -4, -5, -6, -7, 2].map((status) => failureCode(status))).toEqual([
      "Closed",
      "InvalidInput",
      "Native",
      "Overflow",
      "Protocol",
      "SdpRejected",
      "ChannelClosed",
      "Native",
      "Native",
    ]);
    const event = nativePeerTesting.parseEvent(
      packet({ type: "error", status: -3, message: "native transport event queue overflowed" }),
    );
    expect(event).toMatchObject({
      type: "error",
      error: {
        code: "Overflow",
        message: "native peer failed (Overflow)",
        context: { detail: { status: -3, message: "native transport event queue overflowed" } },
      },
    });
    protocol(() => nativePeerTesting.parseEvent(packet({ type: "error", code: "Overflow" })));
    protocol(() => nativePeerTesting.parseEvent(packet({ type: "channel", channel: "data" })));
  });

  test("tells ICE failure from transport failure by the failed connection's candidate pairs", () => {
    const local = { type: "local-candidate", candidateType: "host" };
    const relay = { type: "local-candidate", candidateType: "relay" };
    expect(
      nativePeerTesting.connectionFailure([
        local,
        { type: "candidate-pair", state: "succeeded", nominated: false },
      ]),
    ).toMatchObject({ code: "TransportFailed" });
    expect(
      nativePeerTesting.connectionFailure([
        { type: "candidate-pair", state: "failed", nominated: true },
      ]),
    ).toMatchObject({ code: "TransportFailed" });
    expect(
      nativePeerTesting.connectionFailure([
        local,
        relay,
        local,
        { type: "candidate-pair", state: "failed", nominated: false },
        { type: "candidate-pair", state: "in-progress", nominated: false },
      ]),
    ).toMatchObject({
      code: "IceFailed",
      context: { detail: { pairs: 2, candidateTypes: ["host", "relay"] } },
    });
    expect(nativePeerTesting.connectionFailure([])).toMatchObject({
      code: "IceFailed",
      context: { detail: { pairs: 0, candidateTypes: [] } },
    });
  });

  test("validates snapshot counters and converts 64-bit stat counters without precision loss", () => {
    expect(
      nativePeerTesting.parseSnapshot({
        closed: false,
        queuedControl: 1,
        queuedVideo: 2,
        queuedAudio: 3,
        queuedBytes: 4,
        droppedVideo: "18446744073709551615",
        droppedAudio: "0",
        pendingRequests: 5,
        deliveredVideo: "6",
        deliveredAudio: "7",
      }),
    ).toEqual(
      expect.objectContaining({
        droppedVideo: 18446744073709551615n,
        deliveredAudio: 7n,
      }),
    );
    protocol(() =>
      nativePeerTesting.parseSnapshot({
        closed: false,
        queuedControl: 0,
        queuedVideo: 0,
        queuedAudio: 0,
        queuedBytes: 0,
        droppedVideo: 0,
        droppedAudio: "0",
        pendingRequests: 0,
        deliveredVideo: "0",
        deliveredAudio: "0",
      }),
    );
    const converted = nativePeerTesting.statsValue([
      {
        type: "candidate-pair",
        bytesSent: "18446744073709551615",
        bytesReceived: "9007199254740993",
        priority: "123",
        currentRoundTripTime: 0.01,
      },
    ]) as readonly Record<string, unknown>[];
    expect(converted[0]?.bytesSent).toBe(18446744073709551615n);
    expect(converted[0]?.bytesReceived).toBe(9007199254740993n);
    expect(converted[0]?.priority).toBe(123n);
  });
});
