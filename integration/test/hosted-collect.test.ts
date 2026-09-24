/** The hosted qualification's collectors, each fed what a session would give it. */
import { expect, test } from "bun:test";
import * as Clock from "effect/Clock";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Tracer from "effect/Tracer";
import { TestClock } from "effect/testing";
import { Http, ReactorError } from "reactor-effect-client";
import type { Recorded, Statistics } from "reactor-effect-client";
import type * as H3 from "reactor-effect-client/h3";
import type { AudioFrame, VideoFrame } from "reactor-effect-client/host";
import {
  AudioReader,
  ContractTally,
  VideoReader,
  sampleStats,
  spanRecorder,
  statsSample,
  terminationTrail,
} from "../hosted/collect.js";
import type { StatsSample } from "../hosted/evidence.js";

const frame = (
  sequence: number,
  fill: number,
  extra: Partial<VideoFrame> = {},
): Recorded<VideoFrame> => ({
  _tag: "Frame",
  frame: {
    _tag: "VideoFrame",
    track: "main_video",
    width: 4,
    height: 2,
    frameId: 0n,
    timestampMicros: BigInt(sequence * 41_667),
    sequence: BigInt(sequence),
    format: "BGRA",
    data: new Uint8Array(32).fill(fill),
    metadata: new Uint8Array(0),
    ...extra,
  },
});

test("the video reader summarizes frames, their pacing and the recorder's gaps without keeping any", () => {
  const reader = new VideoReader();
  reader.add(frame(0, 0), 1_000);
  reader.add(frame(1, 90), 1_040);
  reader.add({ _tag: "Lost", after: 1n, count: 3n }, 1_050);
  reader.add(frame(5, 120, { metadata: new Uint8Array([1]), frameId: 5n }), 1_210);
  reader.add(frame(6, 120), 1_250);
  expect(reader.firstAfter(1_100)).toBe(1_210);
  expect(reader.summary()).toEqual({
    frames: 4,
    formats: ["BGRA"],
    sizes: ["4x2"],
    firstFrameMs: 1_000,
    fps: 12,
    interval: { p50: 40, p95: 170, max: 170 },
    // The black first frame is not lit; the last two share one image.
    lit: 3,
    distinct: 3,
    meanLuma: 82.5,
    lost: 3,
    gaps: 1,
    withMetadata: 1,
    withFrameId: 1,
    withTimestamp: 3,
  });
});

test("a clip is judged by the frames that arrived while it played", () => {
  const reader = new VideoReader();
  // Before the clip: a black frame and an idle image.
  reader.add(frame(0, 0), 0);
  reader.add(frame(1, 60), 40);
  // The clip, frozen on one image.
  reader.add(frame(2, 120), 100);
  reader.add(frame(3, 120), 140);
  expect(reader.summary().distinct).toBe(3);
  expect(reader.seenSince(100)).toEqual({ frames: 2, formats: ["BGRA"], lit: 2, distinct: 1 });
  expect(reader.seenSince(1_000)).toEqual({ frames: 0, formats: [], lit: 0, distinct: 0 });
});

test("the audio reader keeps rates, shapes, loudness and loss", () => {
  const reader = new AudioReader();
  const block = (sequence: number, value: number): Recorded<AudioFrame> => ({
    _tag: "Frame",
    frame: {
      _tag: "AudioFrame",
      track: "main_audio",
      sampleRate: 48_000,
      channels: 1,
      sequence: BigInt(sequence),
      samples: new Int16Array(480).fill(value),
    },
  });
  reader.add(block(0, 0), 500);
  reader.add({ _tag: "Lost", after: 0n, count: 2n }, 510);
  reader.add(block(3, 16_384), 530);
  expect(reader.summary()).toEqual({
    blocks: 2,
    sampleRates: [48_000],
    channels: [1],
    samplesPerBlock: [480],
    firstBlockMs: 500,
    peakRms: 0.5,
    lost: 2,
  });
});

test("a stats sample keeps the pair, RTT, rates, fps, jitter and loss in evidence units", () => {
  const stats: Statistics = {
    sampledAtMs: 0,
    generation: 1n,
    pair: {
      id: "pair",
      localCandidateType: "srflx",
      remoteCandidateType: "relay",
      availableIncomingBitrate: 2_500_000,
    },
    rates: { sentBitsPerSecond: 0, receivedBitsPerSecond: 4_200_500, intervalMs: 1000 },
    jitterSeconds: 0.0123,
    lossRatio: 0.00012,
    framesPerSecond: 23.97,
    roundTripTimeSeconds: 0.0381,
    warnings: [],
  };
  expect(statsSample(stats, 900)).toEqual({
    atMs: 900,
    local: "srflx",
    remote: "relay",
    rttMs: 38.1,
    receivedKbps: 4200.5,
    availableIncomingKbps: 2500,
    fps: 24,
    jitterMs: 12.3,
    lossRatio: 0.0001,
  });
  expect(statsSample({ sampledAtMs: 0, generation: 1n, warnings: [] }, 5)).toEqual({ atMs: 5 });
});

test("stats are sampled every second and a failed read is skipped", async () => {
  const samples: StatsSample[] = [];
  let reads = 0;
  const session = {
    stats: Effect.suspend(() =>
      ++reads === 2
        ? Effect.fail(ReactorError.fromCode("Disconnected", "between generations"))
        : Effect.succeed<Statistics>({ sampledAtMs: 0, generation: 1n, warnings: [] }),
    ),
  };
  await Effect.runPromise(
    Effect.gen(function* () {
      const fiber = yield* Effect.forkChild(sampleStats(session, 0, samples));
      yield* TestClock.adjust("3500 millis");
      yield* Fiber.interrupt(fiber);
    }).pipe(Effect.provide(TestClock.layer())),
  );
  expect(reads).toBe(4);
  expect(samples.map((sample) => sample.atMs)).toEqual([0, 2_000, 3_000]);
});

test("the tally counts messages, unknown types, repeats, diagnostics and metadata echoes", () => {
  const tally = new ContractTally();
  const clip = { clip_id: "c1", metadata: '{"caller":"hosted-qualification:run-1"}' };
  const message = (
    type: string,
    data: unknown,
    disposition: "applied" | "duplicate" | "stale" = "applied",
  ) =>
    ({
      _tag: "Message",
      message: type.startsWith("x_") ? { type: "unknown", name: type, data } : { type, data },
      disposition,
    }) as unknown as H3.ProviderEvent;
  tally.add(message("clip_queued", { clip }));
  tally.watch("c1", "hosted-qualification:run-1");
  tally.add(message("clip_queued", { clip }, "duplicate"));
  tally.add(message("queue_update", { generation: [], playout: [clip], history: [] }));
  tally.add(message("state_update", { playing: true, width: 1344 }, "stale"));
  tally.add(message("x_telemetry", { clip }));
  tally.add(message("clip_started", { clip: { ...clip, metadata: "rewritten" } }));
  tally.add({
    _tag: "Diagnostic",
    error: ReactorError.fromCode("Protocol", "H3 clip_failed payload is malformed"),
  });
  expect(tally.messages).toEqual({
    clip_queued: 2,
    queue_update: 1,
    state_update: 1,
    clip_started: 1,
  });
  expect(tally.unknown).toEqual({ x_telemetry: 1 });
  expect(tally.duplicates).toBe(1);
  expect(tally.stale).toBe(1);
  expect(tally.diagnostics).toEqual({ Protocol: 1 });
  expect(tally.lastState).toEqual({ playing: true, width: 1344 });
  // Only messages after `watch` that kept the metadata count; a rewrite does not.
  expect(tally.echoes).toEqual({ clip_queued: 1, queue_update: 1, x_telemetry: 1 });
});

test("the span recorder keeps the library's spans and only their reactor attributes", async () => {
  const recorder = spanRecorder(0);
  await Effect.runPromise(
    Effect.gen(function* () {
      yield* Effect.annotateCurrentSpan({
        "reactor.connection.generation": 2n,
        "reactor.session.id": "sess_1",
        "http.request.header.authorization": "Bearer secret-token",
        "error.type": "Timeout",
      });
      const span = yield* Effect.currentSpan;
      span.event("reactor.connect.ready", yield* Clock.currentTimeNanos);
      yield* Effect.void.pipe(Effect.withSpan("application.work"));
      return yield* Effect.fail("boom");
    }).pipe(
      Effect.withSpan("reactor.session.connect"),
      Effect.ignore,
      Effect.provideService(Tracer.Tracer, recorder.tracer),
    ),
  );
  const [span, ...rest] = recorder.records();
  expect(rest).toEqual([]);
  expect(span?.name).toBe("reactor.session.connect");
  expect(span?.status).toBe("error");
  expect(span?.attributes).toEqual({
    "reactor.connection.generation": "2",
    "reactor.session.id": "sess_1",
    "error.type": "Timeout",
  });
  expect(span?.events.map((event) => event.name)).toEqual(["reactor.connect.ready"]);
  expect(JSON.stringify(recorder.records())).not.toContain("secret-token");
});

test("the termination trail follows the coordinator until the session is terminal or gone", async () => {
  const answers = ["ACTIVE", "STOPPING", "STOPPING", "CLOSED"];
  const coordinator = {
    inspect: () =>
      Effect.succeed({
        observedAt: 0,
        state: answers.shift() ?? "CLOSED",
        hasCapabilities: false,
        selectedTransport: null,
        cluster: null,
        zone: null,
        serverVersion: null,
        additional: {},
      }),
  };
  const result = await Effect.runPromise(
    Effect.gen(function* () {
      const fiber = yield* Effect.forkChild(terminationTrail(coordinator, "sess_1", 0, 10_000));
      yield* TestClock.adjust("2 seconds");
      return yield* Fiber.join(fiber);
    }).pipe(Effect.provide(TestClock.layer())),
  );
  expect(result).toEqual({
    trail: [
      { atMs: 0, state: "ACTIVE" },
      { atMs: 500, state: "STOPPING" },
      { atMs: 1_500, state: "CLOSED" },
    ],
    terminalMs: 1_500,
  });
  const gone = await Effect.runPromise(
    terminationTrail(
      {
        inspect: () =>
          Effect.fail(
            new ReactorError({ reason: new Http({ message: "inspect: HTTP 404", status: 404 }) }),
          ),
      },
      "sess_1",
      0,
      Number.MAX_SAFE_INTEGER,
    ).pipe(Effect.provide(TestClock.layer())),
  );
  expect(gone).toEqual({ trail: [{ atMs: 0, state: "gone" }], terminalMs: 0 });
});

test("a refused read is recorded by its status, and never taken for the session's end", async () => {
  const answers: (string | ReactorError)[] = [
    "ACTIVE",
    new ReactorError({ reason: new Http({ message: "inspect: HTTP 403", status: 403 }) }),
    new ReactorError({ reason: new Http({ message: "inspect: no response" }) }),
    ReactorError.fromCode("Protocol", "inspect: malformed reply"),
  ];
  const coordinator = {
    inspect: () => {
      const answer = answers.length > 1 ? answers.shift()! : answers[0]!;
      return typeof answer === "string"
        ? Effect.succeed({
            observedAt: 0,
            state: answer,
            hasCapabilities: false,
            selectedTransport: null,
            cluster: null,
            zone: null,
            serverVersion: null,
            additional: {},
          })
        : Effect.fail(answer);
    },
  };
  const result = await Effect.runPromise(
    Effect.gen(function* () {
      const fiber = yield* Effect.forkChild(terminationTrail(coordinator, "sess_1", 0, 1_800));
      yield* TestClock.adjust("3 seconds");
      return yield* Fiber.join(fiber);
    }).pipe(Effect.provide(TestClock.layer())),
  );
  expect(result).toEqual({
    trail: [
      { atMs: 0, state: "ACTIVE" },
      { atMs: 500, state: "http:403" },
      { atMs: 1_000, state: "error:Http" },
      { atMs: 1_500, state: "error:Protocol" },
    ],
    terminalMs: undefined,
  });
});
