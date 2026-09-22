import { describe, expect, test } from "vitest";
import { ReactorError } from "../../src/errors.js";
import { nativePeerTesting } from "../../src/native/_internal/peer.js";

const packet = (header: Record<string, unknown>, payload: readonly number[] = []) => ({
  header,
  payload: Uint8Array.from(payload),
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

describe("native packet parser", () => {
  test("preserves uint64 video identity and owns BGRA/metadata bytes", () => {
    const source = Uint8Array.from([1, 2, 3, 4, 9, 8, 7]);
    const frame = nativePeerTesting.parseVideo({
      header: {
        type: "video",
        format: "BGRA",
        track: "main_video",
        width: 1,
        height: 1,
        dataLength: 4,
        metadataLength: 3,
        frameId: "18446744073709551615",
        timestampMicros: "9007199254740993",
      },
      payload: source,
    });
    source.fill(0);
    expect(frame.frameId).toBe(18446744073709551615n);
    expect(frame.timestampMicros).toBe(9007199254740993n);
    expect([...frame.data]).toEqual([1, 2, 3, 4]);
    expect([...frame.metadata]).toEqual([9, 8, 7]);
  });

  test("rejects malformed video/event fields and invalid PCM counts", () => {
    protocol(() =>
      nativePeerTesting.parseVideo(
        packet(
          {
            type: "video",
            format: "BGRA",
            track: "main_video",
            width: 2,
            height: 1,
            dataLength: 4,
            metadataLength: 0,
            frameId: "1",
            timestampMicros: "1",
          },
          [1, 2, 3, 4],
        ),
      ),
    );
    protocol(() =>
      nativePeerTesting.parseVideo(
        packet(
          {
            type: "video",
            format: "BGRA",
            track: "main_video",
            width: 1,
            height: 1,
            dataLength: 4,
            metadataLength: 0,
            frameId: 1,
            timestampMicros: "1",
          },
          [1, 2, 3, 4],
        ),
      ),
    );
    protocol(() => nativePeerTesting.parseEvent(packet({ type: "channel", channel: "data" })));
    protocol(() =>
      nativePeerTesting.parseAudio(
        packet(
          {
            type: "audio",
            format: "s16le",
            track: "main_audio",
            sampleRate: 48_000,
            channels: 2,
            samples: 3,
          },
          [1, 0, 2, 0, 3, 0],
        ),
      ),
    );
  });

  test("decodes signed little-endian PCM and validates snapshot counters", () => {
    const audio = nativePeerTesting.parseAudio(
      packet(
        {
          type: "audio",
          format: "s16le",
          track: "main_audio",
          sampleRate: 48_000,
          channels: 2,
          samples: 4,
        },
        [1, 0, 0xfe, 0xff, 0x2c, 0x01, 0x70, 0xfe],
      ),
    );
    expect([...audio.samples]).toEqual([1, -2, 300, -400]);

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
  });

  test("converts native 64-bit stat counters without precision loss", () => {
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
