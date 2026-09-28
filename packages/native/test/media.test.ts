/*
 * The installed addon under the load the decision record measured: a real
 * libwebrtc sender at 1344x768 BGRA, 24 fps, and 48 kHz PCM, received on
 * whichever runtime runs this file. scripts/test.sh runs it on Node
 * and on Bun.
 */
import { availableParallelism, loadavg, networkInterfaces } from "node:os";
import { layer } from "@effect/vitest";
import * as Cause from "effect/Cause";
import * as Clock from "effect/Clock";
import * as Console from "effect/Console";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as Redacted from "effect/Redacted";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import * as ChildProcess from "effect/unstable/process/ChildProcess";
import { ChildProcessSpawner } from "effect/unstable/process/ChildProcessSpawner";
import * as Coordinator from "reactor-effect-client/Coordinator";
import { recorder } from "reactor-effect-client/Media";
import type { MediaPressure } from "reactor-effect-client/Media";
import type { PeerEvent } from "reactor-effect-client/Peer";
import type { ReactorError } from "reactor-effect-client/ReactorError";
import { expect } from "vitest";
import { load } from "../src/internal/addon.js";
import { defaultShutdownTimeout } from "../src/internal/peer.js";
import type { NativePeer } from "../src/internal/peer.js";
import {
  assertExactFrames,
  candidateRelay,
  clientServices,
  decoded,
  eventually,
  FarPeer,
  farPeerCoordinator,
  nativeClient,
  nativePeer,
  record,
  withFetch,
} from "./support.js";

const WIDTH = 1344,
  HEIGHT = 768;
/**
 * A healthy close joins its native owner well inside the shutdown deadline,
 * which fires only on a wedged join; each close here must take under a fifth
 * of it, so a deadline that would fire on a slow but healthy join fails.
 */
const closeBound = Duration.toMillis(defaultShutdownTimeout) / 5;

const tracks = [
  { name: "main_video", kind: "video", direction: "recvonly" },
  { name: "main_audio", kind: "audio", direction: "recvonly" },
] as const;

const wallMs = (): number => performance.timeOrigin + performance.now();

const percentile = (values: ReadonlyArray<number>, p: number): number => {
  const sorted = [...values].sort((a, b) => a - b);
  return sorted[Math.min(sorted.length - 1, Math.floor(sorted.length * p))] ?? Number.NaN;
};

/**
 * Block the JavaScript thread, as a long synchronous task in the host would. It
 * waits rather than spins, so on a small runner the stall does not also take a
 * core from libwebrtc's decoders.
 */
const stall = (ms: number): void => {
  Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, ms);
};

const round = (value: number): number => Math.round(value * 100) / 100;

const Json = Schema.fromJsonString(Schema.Unknown);

/** Runs ps off the JavaScript thread, which the tests measure; empty where ps cannot say. */
const ps = (args: ReadonlyArray<string>) =>
  Effect.flatMap(ChildProcessSpawner, (spawner) =>
    spawner.string(ChildProcess.make("ps", [...args])),
  ).pipe(
    Effect.map((output) => output.trim()),
    Effect.orElseSucceed(() => ""),
  );

/** The host's busiest processes, as "percent command" strings. */
const busiest = Effect.map(ps(["-Ao", "pcpu=,comm="]), (table) =>
  table
    .split("\n")
    .map((line) => line.trim())
    .filter((line) => line !== "")
    .sort((a, b) => Number.parseFloat(b) - Number.parseFloat(a))
    .slice(0, 5)
    .map((line) => line.replace(/\s+/, " ").replace(/ .*\//, " ")),
);

/** CPU seconds a process has used: Linux ps prints [dd-]hh:mm:ss and macOS m:ss.ss. */
const cpuSeconds = (pid: number) =>
  Effect.map(ps(["-o", "time=", "-p", String(pid)]), (time) => {
    if (time === "") return Number.NaN;
    const [days, clock] = time.includes("-") ? time.split("-") : ["0", time];
    return (
      Number(days) * 86_400 +
      (clock ?? "").split(":").reduce((total, part) => total * 60 + Number(part), 0)
    );
  });

interface Receiver {
  readonly id: string;
  readonly peer: NativePeer;
  readonly scope: Scope.Closeable;
  /** Arrival time, sender-to-arrival latency and admission sequence of every frame, in order. */
  readonly frames: Array<{
    readonly at: number;
    readonly latencyMs: number;
    readonly sequence: bigint;
  }>;
  readonly sizes: Map<string, number>;
  readonly rtts: Array<number>;
  /** Sampled frames the bridge held for this receiver; see sample(). */
  readonly held: Array<number>;
  readonly failures: Array<ReactorError>;
  audio: number;
  connected: boolean;
  closed: boolean;
  readonly channels: Set<string>;
}

/**
 * Close the receiver's scope, which shuts its peer down, and require a clean
 * close: the native owner joined, without the Shutdown a session's close would
 * record in localErrors, inside closeBound. Returns milliseconds taken.
 */
const close = (receiver: Pick<Receiver, "id" | "scope" | "closed">) =>
  Effect.gen(function* () {
    const far = yield* FarPeer;
    receiver.closed = true;
    const started = performance.now();
    const exit = yield* Effect.exit(Scope.close(receiver.scope, Exit.void));
    const elapsed = performance.now() - started;
    yield* far.close(receiver.id);
    expect(
      Exit.isSuccess(exit),
      `${receiver.id} shutdown ${Exit.isFailure(exit) ? Cause.pretty(exit.cause) : ""}`,
    ).toBe(true);
    expect(elapsed, `${receiver.id} close milliseconds`).toBeLessThan(closeBound);
    return elapsed;
  });

/**
 * Open one receiving peer on a scope of its own and wait until both channels
 * are open. A receiver the test has not closed is closed with the test.
 */
const open = (id: string) =>
  Effect.gen(function* () {
    const far = yield* FarPeer;
    const scope = yield* Scope.make();
    const peer = yield* nativePeer().pipe(Scope.provide(scope));
    const receiver: Receiver = {
      id,
      peer,
      scope,
      frames: [],
      sizes: new Map(),
      rtts: [],
      held: [],
      failures: [],
      audio: 0,
      connected: false,
      closed: false,
      channels: new Set(),
    };
    yield* Effect.addFinalizer(() =>
      receiver.closed ? Effect.void : Effect.andThen(Scope.close(scope, Exit.void), far.close(id)),
    );
    yield* Effect.gen(function* () {
      const relay = yield* candidateRelay({ far, id });
      const prepared = yield* peer.prepare([], tracks, (event) => {
        if (event.type === "ice" && event.candidate !== undefined) relay(event.candidate);
        else if (event.type === "state") receiver.connected = event.state === "connected";
        else if (event.type === "channel" && event.open) receiver.channels.add(event.channel);
        else if (event.type === "message" && event.channel === "control")
          receiver.rtts.push(
            wallMs() - new DataView(event.bytes.buffer, event.bytes.byteOffset).getFloat64(0, true),
          );
        else if (event.type === "error") receiver.failures.push(event.error);
      });
      // Subscribe before the answer: every frame the far peer encodes is counted.
      yield* Effect.forkScoped(
        decoded(peer)
          .video("main_video")
          .pipe(
            Stream.runForEach((frame) =>
              Effect.sync(() => {
                const sent = new DataView(frame.metadata.buffer, frame.metadata.byteOffset);
                receiver.frames.push({
                  at: performance.now(),
                  latencyMs: wallMs() - Number(sent.getBigUint64(0, true)) / 1000,
                  sequence: frame.sequence,
                });
                const size = `${frame.width}x${frame.height}`;
                receiver.sizes.set(size, (receiver.sizes.get(size) ?? 0) + 1);
              }),
            ),
            Effect.catch((error) => Effect.sync(() => receiver.failures.push(error))),
          ),
      );
      yield* Effect.forkScoped(
        decoded(peer)
          .audio("main_audio")
          .pipe(
            Stream.runForEach(() => Effect.sync(() => receiver.audio++)),
            Effect.catch((error) => Effect.sync(() => receiver.failures.push(error))),
          ),
      );
      yield* Effect.yieldNow;
      yield* peer.answer(yield* far.answer(id, prepared.sdp));
    }).pipe(Scope.provide(scope));
    yield* eventually({
      condition: () => receiver.connected && receiver.channels.size === 2,
      message: `${id} did not connect`,
      timeout: "15 seconds",
    });
    return receiver;
  });

const pressure = (receiver: Receiver) => decoded(receiver.peer).pressure;

/** Frames that entered a receiver's native video queue, whatever became of them. */
const arrived = (snapshot: MediaPressure): number =>
  snapshot.queuedVideo + Number(snapshot.deliveredVideo + snapshot.droppedVideo);

/** The same for audio blocks. */
const arrivedAudio = (snapshot: MediaPressure): number =>
  snapshot.queuedAudio + Number(snapshot.deliveredAudio + snapshot.droppedAudio);

/** Wait until nothing is queued in the receiver's native video queue, for up to a second. */
const drained = (receiver: Receiver) =>
  Effect.gen(function* () {
    for (let attempt = 0; ; attempt++) {
      const snapshot = yield* pressure(receiver);
      if (snapshot.queuedVideo === 0 || attempt === 100) return snapshot;
      yield* Effect.sleep("10 millis");
    }
  });

/**
 * Record the frames the bridge holds for a receiver: queued natively, or taken
 * but not yet seen by its subscriber. Each is one frame interval of delay the
 * bridge adds; one is normal while a frame is being delivered. End-to-end
 * latency also carries the far peer's encoder and pacer and the receiver's
 * jitter buffer, which follow how the host schedules both processes, so the
 * report prints it and the tests assert this.
 */
const sample = (receiver: Receiver) =>
  Effect.tap(pressure(receiver), (snapshot) =>
    Effect.sync(() =>
      receiver.held.push(
        snapshot.queuedVideo +
          Math.max(0, Number(snapshot.deliveredVideo) - receiver.frames.length),
      ),
    ),
  );

/** The start of a measured window: its time and both processes' CPU use. */
interface Window {
  readonly at: number;
  readonly cpu: NodeJS.CpuUsage;
  readonly farCpu: number;
}

const begin = Effect.gen(function* () {
  const far = yield* FarPeer;
  const window: Window = {
    at: performance.now(),
    cpu: process.cpuUsage(),
    farCpu: yield* cpuSeconds(far.pid),
  };
  return window;
});

const runtime =
  process.versions.bun === undefined
    ? `node ${process.versions.node}`
    : `bun ${process.versions.bun}`;

/**
 * Print what a load test measured over its window before it asserts, so every
 * CI log records what that runner achieved: both processes' CPU use, the far
 * peer's pacing, encoder and path counters, and each receiver's delivery,
 * latency, decoder and loss counters.
 */
const report = (measured: {
  readonly label: string;
  readonly window: Window;
  readonly receivers: ReadonlyArray<Receiver>;
}) =>
  Effect.gen(function* () {
    const far = yield* FarPeer;
    const { window } = measured;
    const seconds = (performance.now() - window.at) / 1000;
    const cpu = process.cpuUsage(window.cpu);
    const sessions = yield* Effect.forEach(measured.receivers, (receiver) =>
      Effect.gen(function* () {
        const frames = receiver.frames.filter((frame) => frame.at >= window.at);
        const latencies = frames.map((frame) => frame.latencyMs);
        const native = (yield* receiver.peer.stats).map(record);
        const media = yield* pressure(receiver);
        const inbound = native.find(
          (entry) => entry.type === "inbound-rtp" && entry.kind === "video",
        );
        const pair = native.find(
          (entry) => entry.type === "candidate-pair" && entry.nominated === true,
        );
        const counter = (name: string): number => Number(inbound?.[name] ?? Number.NaN);
        return {
          id: receiver.id,
          fps: round(frames.length / seconds),
          latencyMs: [0.5, 0.95, 1].map((p) => round(percentile(latencies, p))),
          heldFrames: [0.5, 0.95, 1].map((p) => percentile(receiver.held, p)),
          maxGapMs: round(
            Math.max(0, ...frames.slice(1).map((frame, index) => frame.at - frames[index]!.at)),
          ),
          rttP95Ms: round(percentile(receiver.rtts, 0.95)),
          audio: {
            received: receiver.audio,
            delivered: Number(media.deliveredAudio),
            dropped: Number(media.droppedAudio),
            queued: media.queuedAudio,
          },
          far: yield* far.stats(receiver.id),
          inbound: {
            framesDecoded: counter("framesDecoded"),
            framesDropped: counter("framesDropped"),
            decodeMs: round((counter("totalDecodeTime") * 1000) / counter("framesDecoded")),
            packetsLost: counter("packetsLost"),
            nackCount: counter("nackCount"),
            pliCount: counter("pliCount"),
            availableIncomingBitrate: Number(pair?.availableIncomingBitrate ?? Number.NaN),
          },
        };
      }),
    );
    const line = yield* Schema.encodeEffect(Json)({
      label: measured.label,
      runtime,
      host: `${process.platform}-${process.arch}`,
      cores: availableParallelism(),
      load: round(loadavg()[0] ?? Number.NaN),
      seconds: round(seconds),
      cpu: {
        host: round((cpu.user + cpu.system) / 1e6 / seconds),
        far: round(((yield* cpuSeconds(far.pid)) - window.farCpu) / seconds),
      },
      // macOS runs throttled background processes at priority 4 and utility at 20.
      priority: {
        host: yield* ps(["-o", "pri=", "-p", String(process.pid)]),
        far: yield* ps(["-o", "pri=", "-p", String(far.pid)]),
      },
      busiest: yield* busiest,
      clockSkewMs: round((yield* Clock.currentTimeMillis) - wallMs()),
      sessions,
    });
    yield* Console.log(`media-load ${line}`);
  });

/**
 * A documentation address (RFC 5737) on none of this host's networks, which
 * may use one of those blocks itself: a container can sit on 192.0.2.0/24.
 */
const unreachable = (): string => {
  const local = Object.values(networkInterfaces())
    .flat()
    .map((entry) => entry?.address ?? "");
  const block = ["192.0.2", "198.51.100", "203.0.113"].find(
    (prefix) => !local.some((address) => address.startsWith(`${prefix}.`)),
  );
  if (block === undefined) throw new Error("every documentation block is a local network");
  return `${block}.1`;
};

/**
 * Point every UDP candidate in an answer at `address` and drop the TCP ones.
 * An unanswered UDP pair times out in libwebrtc's 15 s write timeout; a TCP
 * pair waits for its connect, which a dropped SYN holds for minutes.
 */
const unreachableAnswer = (sdp: string, address: string): string =>
  sdp
    .split("\r\n")
    .filter((line) => !(line.startsWith("a=candidate:") && / tcp /i.test(line)))
    .map((line) =>
      line.startsWith("a=candidate:")
        ? line.replace(/^(a=candidate:\S+ \d+ \S+ \d+ )\S+/, `$1${address}`)
        : line.startsWith("c=IN IP4 ")
          ? `c=IN IP4 ${address}`
          : line,
    )
    .join("\r\n");

const ping = (receiver: Receiver) =>
  Effect.suspend(() => {
    const bytes = new Uint8Array(8);
    new DataView(bytes.buffer).setFloat64(0, wallMs(), true);
    return receiver.peer.send("control", bytes);
  });

/** Ping and sample `receivers` every 250 ms for 10 s from `window`. */
const measure = (window: Window, receivers: ReadonlyArray<Receiver>) =>
  Effect.gen(function* () {
    while (performance.now() - window.at < 10_000) {
      yield* Effect.forEach(receivers, ping, { concurrency: "unbounded" });
      yield* Effect.forEach(receivers, sample, { concurrency: "unbounded" });
      yield* Effect.sleep("250 millis");
    }
  });

/** The addon loads before the far peer starts, so a host that cannot load it fails at once. */
const services = Layer.mergeAll(
  clientServices,
  FarPeer.layer,
  load(undefined).pipe(Effect.orDie, Layer.effectDiscard),
);

layer(services, { excludeTestServices: true, timeout: "30 seconds" })(
  "native media under load",
  (it) => {
    it.effect(
      "receives 1344x768 at 24 fps, dropping at most 1% and holding at most two frames at p95",
      () =>
        Effect.gen(function* () {
          const far = yield* FarPeer;
          const receiver = yield* open("throughput");
          yield* eventually({
            condition: () => receiver.frames.length > 0,
            message: "no first frame",
            timeout: "15 seconds",
          });
          const measured = yield* begin;
          const before = yield* pressure(receiver);
          yield* measure(measured, [receiver]);
          const seconds = (performance.now() - measured.at) / 1000;
          const snapshot = yield* pressure(receiver);
          const sent = yield* far.sent(receiver.id);
          yield* report({ label: "throughput", window: measured, receivers: [receiver] });
          expect(receiver.failures).toEqual([]);
          expect([sent.width, sent.height]).toEqual([WIDTH, HEIGHT]);
          expect(receiver.sizes.get(`${WIDTH}x${HEIGHT}`) ?? 0).toBeGreaterThanOrEqual(
            Math.floor(receiver.frames.length * 0.9),
          );
          const reached = arrived(snapshot) - arrived(before);
          expect(reached / seconds, "frames per second reaching the bridge").toBeGreaterThanOrEqual(
            20,
          );
          expect(Number(snapshot.droppedVideo - before.droppedVideo)).toBeLessThanOrEqual(
            Math.floor(reached * 0.01),
          );
          expect(percentile(receiver.held, 0.95)).toBeLessThanOrEqual(2);
          // Audio reaches the bridge at the pace of libwebrtc's playout clock, which
          // a throttled host slows; the bridge must pass on all of it, all the time.
          expect(
            (arrivedAudio(snapshot) - arrivedAudio(before)) / seconds,
            "audio blocks per second reaching the bridge",
          ).toBeGreaterThanOrEqual(20);
          expect(snapshot.droppedAudio).toBe(0n);
          // The control channel is not queued behind media on a shared thread pool.
          expect(receiver.rtts.length).toBeGreaterThanOrEqual(30);
          expect(percentile(receiver.rtts, 0.95)).toBeLessThanOrEqual(100);
          yield* close(receiver);
        }),
      60_000,
    );

    it.effect(
      "sustains two concurrent sessions at full load for 10 s without dropping audio",
      () =>
        Effect.gen(function* () {
          const sessions = [yield* open("pair-a"), yield* open("pair-b")];
          yield* eventually({
            condition: () => sessions.every((session) => session.frames.length >= 24),
            message: "both sessions did not stream",
            timeout: "15 seconds",
          });
          const measured = yield* begin;
          const start = yield* Effect.forEach(sessions, pressure);
          yield* measure(measured, sessions);
          const seconds = (performance.now() - measured.at) / 1000;
          const end = yield* Effect.forEach(sessions, pressure);
          yield* report({ label: "two sessions", window: measured, receivers: sessions });
          for (const [index, session] of sessions.entries()) {
            const before = start[index]!,
              after = end[index]!;
            const reached = arrived(after) - arrived(before);
            expect(session.failures).toEqual([]);
            expect(
              reached / seconds,
              `${session.id} frames per second reaching the bridge`,
            ).toBeGreaterThanOrEqual(20);
            expect(Number(after.droppedVideo - before.droppedVideo)).toBeLessThanOrEqual(
              Math.floor(reached * 0.01),
            );
            expect(percentile(session.held, 0.95)).toBeLessThanOrEqual(2);
            // Two sessions' readers never compete for a shared thread pool.
            expect(after.droppedAudio - before.droppedAudio).toBe(0n);
            expect(percentile(session.rtts, 0.95)).toBeLessThanOrEqual(100);
          }
          for (const session of sessions) yield* close(session);
        }),
      60_000,
    );

    it.effect(
      "evicts only what overflows the queue across a 250 ms and a 2 s stall, without losing audio",
      () =>
        Effect.gen(function* () {
          const receiver = yield* open("stall");
          yield* eventually({
            condition: () => receiver.frames.length >= 24,
            message: "media did not start",
            timeout: "15 seconds",
          });
          const measured = yield* begin;
          const before = yield* drained(receiver);
          stall(250);
          // What reached the bridge while JavaScript was blocked. The native video
          // queue holds 8 frames, 333 ms at 24 fps, so at that rate nothing is
          // evicted; a far peer that falls behind and then catches up can deliver
          // more in the same 250 ms, and only those beyond 8 may be evicted.
          const released = yield* pressure(receiver);
          yield* Effect.sleep("2 seconds");
          const short = yield* drained(receiver);
          const burst = arrived(released) - arrived(before);
          // A frame arriving before the pump resumes can evict one more.
          expect(Number(short.droppedVideo - before.droppedVideo)).toBeLessThanOrEqual(
            Math.max(0, burst - 8) + 1,
          );
          expect(short.droppedAudio).toBe(0n);

          const stalledAt = performance.now();
          stall(2000);
          const stalled = yield* pressure(receiver);
          yield* Effect.sleep("2 seconds");
          const long = yield* pressure(receiver);
          yield* report({ label: "stall", window: measured, receivers: [receiver] });
          // About 48 frames reach the bridge in 2 s; the queue keeps the newest 8
          // and counts each eviction. The 256-block audio queue rides through 2.56 s.
          const reached = arrived(stalled) - arrived(short);
          expect(reached, "frames reaching the bridge during the stall").toBeGreaterThanOrEqual(30);
          // A frame already queued, or taken, around either edge moves this by one.
          expect(
            Math.abs(Number(long.droppedVideo - short.droppedVideo) - (reached - 8)),
          ).toBeLessThanOrEqual(2);
          expect(long.droppedAudio).toBe(0n);
          // Every eviction is a gap in the admission sequence, at its position: the
          // recorder's Lost runs add up to the bridge's own count. The reader
          // subscribed before the answer, so its first frame is the track's first.
          const recorded = yield* Stream.fromIterable([...receiver.frames]).pipe(
            recorder,
            Stream.runCollect,
          );
          expect(receiver.frames[0]?.sequence).toBe(0n);
          const lost = recorded.flatMap((entry) => (entry._tag === "Lost" ? [entry] : []));
          expect(lost.reduce((sum, entry) => sum + entry.count, 0n)).toBe(long.droppedVideo);
          expect(lost.length).toBeGreaterThanOrEqual(1);
          // The backlog reached the bounded observation queue at its reader's pace,
          // drained, and delivery went on.
          expect(receiver.failures).toEqual([]);
          expect(Number(long.deliveredVideo) - receiver.frames.length).toBeLessThanOrEqual(4);
          expect(long.queuedVideo).toBeLessThanOrEqual(1);
          const recovered = receiver.frames.filter((frame) => frame.at > stalledAt + 3000);
          expect(recovered.length).toBeGreaterThanOrEqual(12);
          yield* close(receiver);
        }),
      60_000,
    );

    it.effect(
      "renews a session while its predecessor streams, three times, on one process factory",
      () =>
        Effect.gen(function* () {
          let current = yield* open("renewal-0");
          yield* eventually({
            condition: () => current.frames.length >= 24,
            message: "first session did not stream",
            timeout: "15 seconds",
          });
          for (let cycle = 1; cycle <= 3; cycle++) {
            const next = yield* open(`renewal-${cycle}`);
            yield* eventually({
              condition: () => next.frames.length >= 24,
              message: `renewal ${cycle} did not stream`,
              timeout: "15 seconds",
            });
            // Both sessions stream at full load before the old one drains.
            const overlap = yield* pressure(current);
            expect(overlap.droppedAudio).toBe(0n);
            const measured = yield* begin;
            const during = next.frames.length;
            const before = yield* pressure(next);
            const shutdownMs = yield* close(current);
            yield* Effect.sleep("4 seconds");
            const after = yield* pressure(next);
            const seconds = (performance.now() - measured.at) / 1000;
            yield* report({ label: `renewal ${cycle}`, window: measured, receivers: [next] });
            const window = next.frames.slice(during);
            expect(shutdownMs).toBeLessThan(2000);
            expect(current.failures).toEqual([]);
            // From its predecessor's shutdown on, the replacement keeps receiving
            // at load, drops at most one frame and never freezes. The rate covers
            // 4 s, the shutdown and the recovery after it, so a busy runner that
            // delays frames for a second without losing them still meets it; the
            // gap bound is what catches a freeze.
            expect(
              (arrived(after) - arrived(before)) / seconds,
              "frames per second reaching the replacement",
            ).toBeGreaterThanOrEqual(15);
            expect(after.droppedVideo - before.droppedVideo).toBeLessThanOrEqual(1n);
            expect(
              Math.max(...window.slice(1).map((frame, index) => frame.at - window[index]!.at)),
            ).toBeLessThan(500);
            expect(after.droppedAudio).toBe(0n);
            current = next;
          }
          expect(current.failures).toEqual([]);
          yield* close(current);
        }),
      120_000,
    );

    it.effect(
      "delivers exact frames through a canonical session over real libwebrtc and closes it cleanly",
      () =>
        Effect.gen(function* () {
          const far = yield* FarPeer;
          const id = "canonical";
          yield* Effect.addFinalizer(() => far.close(id));
          const relay = farPeerCoordinator({ far, id, tracks });
          const result = yield* Effect.scoped(
            Effect.gen(function* () {
              const factory = yield* nativeClient({
                settings: { apiUrl: "https://coordinator.far-peer" },
              });
              const client = yield* factory.create({
                model: "far-peer",
                tokens: Coordinator.fixedTokens({
                  jwt: Redacted.make("far-peer-token"),
                  expiresAt: Number.MAX_SAFE_INTEGER,
                  maxSessionSeconds: undefined,
                }),
              });
              const media = yield* client.decoded;
              const frames = yield* media
                .video("main_video")
                .pipe(Stream.take(8), Stream.runCollect);
              const started = performance.now();
              const closed = yield* client.close;
              return { frames, report: closed, closeMs: performance.now() - started };
            }),
          ).pipe(withFetch(relay.fetch));
          yield* Console.log(`native-close ${runtime} closeMs=${round(result.closeMs)}`);
          expect(result.frames).toHaveLength(8);
          // Each frame is its own exact BGRA allocation, as the bridge took it.
          assertExactFrames({ frames: result.frames, bytes: (frame) => frame.data });
          for (const frame of result.frames)
            expect(frame.data.byteLength).toBe(frame.width * frame.height * 4);
          expect(result.report.localClosed).toBe(true);
          expect(result.report.localErrors).toEqual([]);
          expect(result.report.remote).toMatchObject({ attempted: true, confirmed: true });
          expect(result.closeMs).toBeLessThan(closeBound);
        }),
      60_000,
    );

    it.effect(
      "reports a real ICE failure as IceFailed with its candidate-pair detail through the events pump",
      () =>
        Effect.gen(function* () {
          const far = yield* FarPeer;
          const id = "ice-failure";
          const scope = yield* Scope.make();
          const receiver = { id, scope, closed: false };
          yield* Effect.addFinalizer(() => (receiver.closed ? Effect.void : close(receiver)));
          const peer = yield* nativePeer().pipe(Scope.provide(scope));
          const errors: Array<ReactorError> = [];
          const states: Array<string> = [];
          const checking = yield* Effect.gen(function* () {
            // The bridge's own candidates never reach the far peer, so it cannot
            // reach the bridge either and teach it a peer-reflexive candidate.
            const prepared = yield* peer.prepare([], tracks, (event: PeerEvent) => {
              if (event.type === "state") states.push(event.state);
              else if (event.type === "error") errors.push(event.error);
            });
            const answer = yield* far.answer(id, prepared.sdp);
            yield* peer.answer(unreachableAnswer(answer, unreachable()));
            // While ICE checks the pairs, they are there to see.
            yield* Effect.sleep("1 second");
            return (yield* peer.stats).map(record);
          }).pipe(Scope.provide(scope));
          expect(checking.some((entry) => entry.type === "candidate-pair")).toBe(true);
          expect(
            checking.filter(
              (entry) => entry.type === "candidate-pair" && entry.state === "succeeded",
            ),
          ).toEqual([]);
          yield* eventually({
            condition: () => errors.length > 0,
            message: "ICE never failed",
            timeout: "45 seconds",
          });
          yield* Console.log(
            `ice-failure ${runtime} states=${states.join(",")} reason=${errors[0]?.reason._tag ?? ""}`,
          );
          expect(states).not.toContain("connected");
          // libwebrtc reports failure once it has pruned the last timed-out pair,
          // so the pairs the classification reads may already be gone.
          expect(errors).toEqual([
            expect.objectContaining({
              reason: expect.objectContaining({
                _tag: "IceFailed",
                pairs: expect.any(Number),
                candidateTypes: expect.any(Array),
              }),
              message: "native peer found no working ICE candidate pair",
            }),
          ]);
          yield* close(receiver);
        }),
      60_000,
    );
  },
);
