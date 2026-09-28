import { describe, expect, test } from "vitest";
import { revealed } from "./support.js";
import * as Cause from "effect/Cause";
import * as Effect from "effect/Effect";
import * as Redacted from "effect/Redacted";
import type { ReactorError } from "reactor-effect-client/ReactorError";
import type { Track } from "reactor-effect-client/Coordinator";
import { failureCode, type NativeAudio, type NativeVideo } from "../src/internal/bridge.js";
import * as Events from "../src/internal/events.js";

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
  sequence: 0n,
  data: Uint8Array.of(1, 2, 3, 4),
  metadata: Uint8Array.of(9, 8, 7),
  ...fields,
});

const audio = (fields: Partial<NativeAudio> = {}): NativeAudio => ({
  track: 1,
  sampleRate: 48_000,
  channels: 2,
  sequence: 0n,
  samples: Int16Array.of(1, -2, 300, -400),
  ...fields,
});

/** A decoder's value. */
const ok = <A>(effect: Effect.Effect<A, ReactorError>): A => Effect.runSync(effect);
const decodeEvent = (packet: {
  readonly header: unknown;
  readonly payload: Uint8Array<ArrayBuffer>;
}) => Events.decodeEvent({ header: JSON.stringify(packet.header), payload: packet.payload });
const event = (packet: Parameters<typeof decodeEvent>[0]) => {
  const decoded = ok(decodeEvent(packet));
  if (decoded === "Failed") throw new Error("unexpected failed state");
  return decoded;
};

/** The decoder refuses its input with a Protocol failure. */
const protocol = (effect: Effect.Effect<unknown, ReactorError>): void => {
  expect(Effect.runSync(Effect.flip(effect)).reason._tag).toBe("Protocol");
};

const packet = (header: Record<string, unknown>) => ({ header, payload: new Uint8Array() });

describe("native media and event decoding", () => {
  test("names frames by their prepare index and hands over the taken bytes without copying", () => {
    const taken = video();
    const frame = ok(Events.videoFrame(tracks)(taken));
    expect(frame).toMatchObject({
      _tag: "VideoFrame",
      format: "BGRA",
      track: "main_video",
      width: 1,
      height: 1,
      frameId: 18446744073709551615n,
      timestampMicros: 9007199254740993n,
    });
    expect(frame.data).toBe(taken.data);
    expect(frame.metadata).toBe(taken.metadata);

    const block = audio();
    const samples = ok(Events.audioFrame(tracks)(block));
    expect(samples).toMatchObject({ track: "main_audio", sampleRate: 48_000, channels: 2 });
    expect(samples.samples).toBe(block.samples);
    expect([...samples.samples]).toEqual([1, -2, 300, -400]);
  });

  test("rejects frames outside their declared receive track or with inconsistent shapes", () => {
    for (const track of [1, 2, 3]) protocol(Events.videoFrame(tracks)(video({ track })));
    protocol(Events.videoFrame(tracks)(video({ width: 2 })));
    protocol(Events.videoFrame(tracks)(video({ width: 0, data: new Uint8Array() })));
    for (const track of [0, 2]) protocol(Events.audioFrame(tracks)(audio({ track })));
    protocol(Events.audioFrame(tracks)(audio({ samples: Int16Array.of(1, 2, 3) })));
    protocol(Events.audioFrame(tracks)(audio({ sampleRate: 0 })));
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
    const overflow = event(
      packet({ type: "error", status: -3, message: "native transport event queue overflowed" }),
    );
    const classified = overflow.type === "error" ? overflow.error : undefined;
    expect(classified === undefined ? undefined : revealed(classified)).toMatchObject({
      reason: { _tag: "Overflow" },
      message: "native peer failed (Overflow)",
      context: { detail: { status: -3 } },
    });
    const detail = (classified === undefined ? undefined : revealed(classified).context.detail) as
      | { readonly backendMessage: unknown }
      | undefined;
    expect(
      Redacted.isRedacted(detail?.backendMessage) && Redacted.value(detail.backendMessage),
    ).toBe("native transport event queue overflowed");
    protocol(decodeEvent(packet({ type: "error", code: "Overflow" })));
    protocol(decodeEvent(packet({ type: "channel", channel: "data" })));
  });

  test("keeps libwebrtc text Redacted, out of the message, diagnostic JSON and the rendered cause", () => {
    const backend = "setRemoteDescription failed: a=ice-pwd:S3CR3TPWD a=fingerprint:sha-256 AB:CD";
    for (const status of [-2, -5]) {
      const failed = event(packet({ type: "error", status, message: backend }));
      if (failed.type !== "error") throw new Error("expected an error event");
      const error = failed.error;
      expect(error.reason._tag).toBe(status === -2 ? "Native" : "SdpRejected");
      if (error.reason._tag === "Native") {
        expect(error.reason.status).toBe(-2);
        expect(error.reason.backendMessage && Redacted.value(error.reason.backendMessage)).toBe(
          backend,
        );
      }
      const rendered = [
        error.message,
        JSON.stringify(error),
        Cause.pretty(Cause.fail(error)),
        Cause.prettyErrors(Cause.fail(error), { includeCauseInStack: true })
          .map((pretty) => `${pretty.message}\n${pretty.stack ?? ""}`)
          .join("\n"),
      ].join("\n");
      expect(rendered).not.toContain("S3CR3TPWD");
    }
  });

  test("tells ICE failure from transport failure by the failed connection's candidate pairs", () => {
    const local = { type: "local-candidate", candidateType: "host" };
    const relay = { type: "local-candidate", candidateType: "relay" };
    expect(
      Events.connectionFailure([
        local,
        { type: "candidate-pair", state: "succeeded", nominated: false },
      ]),
    ).toMatchObject({ reason: { _tag: "TransportFailed" } });
    expect(
      Events.connectionFailure([{ type: "candidate-pair", state: "failed", nominated: true }]),
    ).toMatchObject({ reason: { _tag: "TransportFailed" } });
    expect(
      Events.connectionFailure([
        local,
        relay,
        local,
        { type: "candidate-pair", state: "failed", nominated: false },
        { type: "candidate-pair", state: "in-progress", nominated: false },
      ]),
    ).toMatchObject({
      reason: { _tag: "IceFailed", pairs: 2, candidateTypes: ["host", "relay"] },
    });
    expect(Events.connectionFailure([])).toMatchObject({
      reason: { _tag: "IceFailed", pairs: 0, candidateTypes: [] },
    });
  });

  test("validates snapshot counters and converts 64-bit stat counters without precision loss", () => {
    expect(
      ok(
        Events.decodePressure({
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
      ),
    ).toEqual(
      expect.objectContaining({
        droppedVideo: 18446744073709551615n,
        deliveredAudio: 7n,
      }),
    );
    protocol(
      Events.decodePressure({
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
    const converted = Events.statsValue([
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
