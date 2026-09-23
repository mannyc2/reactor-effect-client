import { spawn, spawnSync } from "node:child_process";
import type { ChildProcessByStdio } from "node:child_process";
import { existsSync } from "node:fs";
import { availableParallelism, loadavg } from "node:os";
import { createInterface } from "node:readline";
import type { Readable, Writable } from "node:stream";
import { fileURLToPath } from "node:url";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import { afterAll, beforeAll, describe, expect, test } from "vitest";
import type { ReactorError } from "reactor-effect-client";
import type { IceCandidate, MediaPressure } from "reactor-effect-client/host";
import { checkNativeBridge } from "../src/_internal/bridge.js";
import { NativePeer } from "../src/_internal/peer.js";
import { libraryPath, until } from "./support.js";

/*
 * The shipped library under the load the decision record measured: a real
 * libwebrtc sender at 1344x768 BGRA, 24 fps, and 48 kHz PCM, received through
 * Koffi on whichever runtime runs this file. scripts/test.sh runs it on Node
 * and on Bun.
 */
const farPeerPath =
  process.env.REACTOR_NATIVE_FAR_PEER ??
  fileURLToPath(new URL("../rust/target/release/examples/far_peer", import.meta.url));
const WIDTH = 1344,
  HEIGHT = 768;

const tracks = [
  { name: "main_video", kind: "video", direction: "recvonly" },
  { name: "main_audio", kind: "audio", direction: "recvonly" },
] as const;

const wallMs = (): number => performance.timeOrigin + performance.now();

const percentile = (values: readonly number[], p: number): number => {
  const sorted = [...values].sort((a, b) => a - b);
  return sorted[Math.min(sorted.length - 1, Math.floor(sorted.length * p))] ?? Number.NaN;
};

/** Block the JavaScript thread, as a long synchronous task in the host would. */
const stall = (ms: number): void => {
  const end = performance.now() + ms;
  while (performance.now() < end) {
    /* busy */
  }
};

const sleep = (ms: number): Promise<void> => new Promise((resolve) => setTimeout(resolve, ms));

const round = (value: number): number => Math.round(value * 100) / 100;

type Message = Readonly<Record<string, unknown>>;

const record = (value: unknown): Message =>
  typeof value === "object" && value !== null && !Array.isArray(value) ? (value as Message) : {};

/** One ps field for a process, trimmed; empty where ps cannot say. */
const ps = (pid: number | undefined, field: string): string =>
  spawnSync("ps", ["-o", `${field}=`, "-p", String(pid)], { encoding: "utf8" }).stdout?.trim() ??
  "";

/** CPU seconds a process has used: Linux ps prints [dd-]hh:mm:ss and macOS m:ss.ss. */
const cpuSeconds = (pid: number | undefined): number => {
  const time = ps(pid, "time");
  if (time === "") return Number.NaN;
  const [days, clock] = time.includes("-") ? time.split("-") : ["0", time];
  return (
    Number(days) * 86_400 +
    (clock ?? "").split(":").reduce((total, part) => total * 60 + Number(part), 0)
  );
};

class FarPeer {
  private readonly waiters: {
    readonly op: string;
    readonly id: string;
    readonly resolve: (message: Message) => void;
  }[] = [];
  private readonly exited: Promise<unknown>;

  private constructor(private readonly child: ChildProcessByStdio<Writable, Readable, null>) {
    this.exited = new Promise((resolve) => child.once("exit", resolve));
    createInterface({ input: child.stdout }).on("line", (line) => {
      const message = JSON.parse(line) as Message;
      const index = this.waiters.findIndex(
        (waiter) => waiter.op === message.op && waiter.id === (message.id ?? ""),
      );
      if (index >= 0) this.waiters.splice(index, 1)[0]?.resolve(message);
    });
  }

  static async start(): Promise<FarPeer> {
    if (!existsSync(farPeerPath))
      throw new Error(
        `missing far peer ${farPeerPath}; build it with cargo build --release --example far_peer (scripts/test.sh does)`,
      );
    const far = new FarPeer(spawn(farPeerPath, [], { stdio: ["pipe", "pipe", "inherit"] }));
    await far.next("ready", "");
    return far;
  }

  private next(op: string, id: string): Promise<Message> {
    return new Promise((resolve) => this.waiters.push({ op, id, resolve }));
  }

  private send(message: Message): void {
    this.child.stdin.write(`${JSON.stringify(message)}\n`);
  }

  async answer(id: string, sdp: string): Promise<string> {
    const reply = this.next("answer", id);
    this.send({ op: "offer", id, sdp });
    return String((await reply).sdp);
  }

  candidate(id: string, candidate: IceCandidate): void {
    this.send({
      op: "candidate",
      id,
      candidate: candidate.candidate,
      sdpMid: candidate.sdp_mid,
      sdpMLineIndex: candidate.sdp_mline_index,
    });
  }

  /** One session's stats as far_peer.rs reports them: pacing, encoder and path. */
  async stats(id: string): Promise<Message> {
    const reply = this.next("stats", id);
    this.send({ op: "stats", id });
    return record((await reply).stats);
  }

  /** Frames the far peer's encoder actually sent, and their size. */
  async sent(id: string): Promise<{ frames: number; width: number; height: number }> {
    const video = record((await this.stats(id)).video);
    return {
      frames: Number(video.framesSent ?? 0),
      width: Number(video.frameWidth ?? 0),
      height: Number(video.frameHeight ?? 0),
    };
  }

  get pid(): number | undefined {
    return this.child.pid;
  }

  async close(id: string): Promise<void> {
    const reply = this.next("closed", id);
    this.send({ op: "close", id });
    await reply;
  }

  async quit(): Promise<void> {
    this.child.stdin.end();
    await this.exited;
  }
}

interface Receiver {
  readonly id: string;
  readonly peer: NativePeer;
  readonly scope: Scope.Closeable;
  /** Arrival time and sender-to-arrival latency of every frame, in order. */
  readonly frames: { readonly at: number; readonly latencyMs: number }[];
  readonly sizes: Map<string, number>;
  readonly rtts: number[];
  readonly failures: ReactorError[];
  audio: number;
  connected: boolean;
  readonly channels: Set<string>;
}

const run = <A>(effect: Effect.Effect<A, ReactorError>): Promise<A> => Effect.runPromise(effect);

/** Open one receiving peer on its own scope and wait until both channels are open. */
const open = async (far: FarPeer, id: string): Promise<Receiver> => {
  const scope = await run(Scope.make());
  const peer = new NativePeer(libraryPath);
  const receiver: Receiver = {
    id,
    peer,
    scope,
    frames: [],
    sizes: new Map(),
    rtts: [],
    failures: [],
    audio: 0,
    connected: false,
    channels: new Set(),
  };
  try {
    await run(
      Effect.gen(function* () {
        yield* Effect.addFinalizer(() => peer.shutdown().pipe(Effect.orDie));
        const prepared = yield* peer.prepare([], tracks, (event) => {
          if (event.type === "ice" && event.candidate !== undefined)
            far.candidate(id, event.candidate);
          else if (event.type === "state") receiver.connected = event.state === "connected";
          else if (event.type === "channel" && event.open) receiver.channels.add(event.channel);
          else if (event.type === "message" && event.channel === "control")
            receiver.rtts.push(
              wallMs() -
                new DataView(event.bytes.buffer, event.bytes.byteOffset).getFloat64(0, true),
            );
          else if (event.type === "error") receiver.failures.push(event.error);
        });
        // Subscribe before the answer: every frame the far peer encodes is counted.
        yield* Effect.forkScoped(
          peer.rawMedia.video("main_video").pipe(
            Stream.runForEach((frame) =>
              Effect.sync(() => {
                const sent = new DataView(frame.metadata.buffer, frame.metadata.byteOffset);
                receiver.frames.push({
                  at: performance.now(),
                  latencyMs: wallMs() - Number(sent.getBigUint64(0, true)) / 1000,
                });
                const size = `${frame.width}x${frame.height}`;
                receiver.sizes.set(size, (receiver.sizes.get(size) ?? 0) + 1);
              }),
            ),
            Effect.catch((error) => Effect.sync(() => receiver.failures.push(error))),
          ),
        );
        yield* Effect.forkScoped(
          peer.rawMedia.audio("main_audio").pipe(
            Stream.runForEach(() => Effect.sync(() => receiver.audio++)),
            Effect.catch((error) => Effect.sync(() => receiver.failures.push(error))),
          ),
        );
        yield* Effect.yieldNow;
        const answer = yield* Effect.promise(() => far.answer(id, prepared.sdp));
        yield* peer.answer(answer);
      }).pipe(Scope.provide(scope)),
    );
    await until(
      () => receiver.connected && receiver.channels.size === 2,
      `${id} did not connect`,
      15_000,
    );
    return receiver;
  } catch (error) {
    await run(Scope.close(scope, Exit.void));
    throw error;
  }
};

const pressure = (receiver: Receiver): Promise<MediaPressure> =>
  run(receiver.peer.rawMedia.snapshot);

/** Close the receiver's scope, which shuts its peer down; returns milliseconds taken. */
const close = async (far: FarPeer, receiver: Receiver): Promise<number> => {
  const started = performance.now();
  await run(Scope.close(receiver.scope, Exit.void));
  const elapsed = performance.now() - started;
  await far.close(receiver.id);
  return elapsed;
};

/** The start of a measured window: its time and both processes' CPU use. */
interface Window {
  readonly at: number;
  readonly cpu: NodeJS.CpuUsage;
  readonly farCpu: number;
}

const begin = (far: FarPeer): Window => ({
  at: performance.now(),
  cpu: process.cpuUsage(),
  farCpu: cpuSeconds(far.pid),
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
const report = async (
  label: string,
  far: FarPeer,
  window: Window,
  receivers: readonly Receiver[],
): Promise<void> => {
  const seconds = (performance.now() - window.at) / 1000;
  const cpu = process.cpuUsage(window.cpu);
  const sessions = await Promise.all(
    receivers.map(async (receiver) => {
      const frames = receiver.frames.filter((frame) => frame.at >= window.at);
      const latencies = frames.map((frame) => frame.latencyMs);
      const native = (await run(receiver.peer.stats())).map(record);
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
        maxGapMs: round(
          Math.max(0, ...frames.slice(1).map((frame, index) => frame.at - frames[index]!.at)),
        ),
        rttP95Ms: round(percentile(receiver.rtts, 0.95)),
        far: await far.stats(receiver.id),
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
  console.log(
    `media-load ${JSON.stringify({
      label,
      runtime,
      host: `${process.platform}-${process.arch}`,
      cores: availableParallelism(),
      load: round(loadavg()[0] ?? Number.NaN),
      seconds: round(seconds),
      cpu: {
        host: round((cpu.user + cpu.system) / 1e6 / seconds),
        far: round((cpuSeconds(far.pid) - window.farCpu) / seconds),
      },
      // macOS runs throttled background processes at priority 4.
      priority: { host: ps(process.pid, "pri"), far: ps(far.pid, "pri") },
      clockSkewMs: round(Date.now() - wallMs()),
      sessions,
    })}`,
  );
};

const ping = (receiver: Receiver): Promise<void> => {
  const bytes = new Uint8Array(8);
  new DataView(bytes.buffer).setFloat64(0, wallMs(), true);
  return run(receiver.peer.send("control", bytes));
};

describe("native media under load", () => {
  let far: FarPeer;
  beforeAll(async () => {
    await checkNativeBridge(libraryPath);
    far = await FarPeer.start();
  });
  afterAll(async () => {
    await far?.quit();
  });

  test("delivers at least 95% of encoded 1344x768 frames at 24 fps with p95 latency of at most 150 ms", async () => {
    const receiver = await open(far, "throughput");
    try {
      await until(() => receiver.frames.length > 0, "no first frame", 15_000);
      const measured = begin(far);
      while (performance.now() - measured.at < 10_000) {
        await ping(receiver);
        await sleep(250);
      }
      const sent = await far.sent(receiver.id);
      await sleep(300); // frames already sent are still in flight
      const snapshot = await pressure(receiver);
      await report("throughput", far, measured, [receiver]);
      const latencies = receiver.frames.map((frame) => frame.latencyMs);
      expect(receiver.failures).toEqual([]);
      expect([sent.width, sent.height]).toEqual([WIDTH, HEIGHT]);
      expect(receiver.frames.length).toBeGreaterThanOrEqual(Math.floor(sent.frames * 0.95));
      expect(receiver.sizes.get(`${WIDTH}x${HEIGHT}`) ?? 0).toBeGreaterThanOrEqual(
        Math.floor(receiver.frames.length * 0.9),
      );
      expect(percentile(latencies, 0.95)).toBeLessThanOrEqual(150);
      expect(receiver.audio).toBeGreaterThan(900);
      expect(snapshot.droppedAudio).toBe(0n);
      // The control channel is not queued behind media on a shared thread pool.
      expect(receiver.rtts.length).toBeGreaterThanOrEqual(30);
      expect(percentile(receiver.rtts, 0.95)).toBeLessThanOrEqual(100);
    } finally {
      await close(far, receiver);
    }
  }, 60_000);

  test("sustains two concurrent sessions at full load for 10 s without dropping audio", async () => {
    const sessions = [await open(far, "pair-a")];
    try {
      sessions.push(await open(far, "pair-b"));
      await until(
        () => sessions.every((session) => session.frames.length >= 24),
        "both sessions did not stream",
        15_000,
      );
      const start = await Promise.all(
        sessions.map(async (session) => ({
          received: session.frames.length,
          sent: (await far.sent(session.id)).frames,
          pressure: await pressure(session),
        })),
      );
      const measured = begin(far);
      while (performance.now() - measured.at < 10_000) {
        await Promise.all(sessions.map(ping));
        await sleep(250);
      }
      const sent = await Promise.all(sessions.map((session) => far.sent(session.id)));
      await sleep(300); // frames already sent are still in flight
      await report("two sessions", far, measured, sessions);
      for (const [index, session] of sessions.entries()) {
        const before = start[index]!,
          after = await pressure(session);
        const window = session.frames.slice(before.received);
        expect(session.failures).toEqual([]);
        expect(window.length).toBeGreaterThanOrEqual(
          Math.floor((sent[index]!.frames - before.sent) * 0.95),
        );
        expect(
          percentile(
            window.map((frame) => frame.latencyMs),
            0.95,
          ),
        ).toBeLessThanOrEqual(150);
        // Two sessions' readers never compete for a shared thread pool.
        expect(after.droppedAudio - before.pressure.droppedAudio).toBe(0n);
        expect(percentile(session.rtts, 0.95)).toBeLessThanOrEqual(100);
      }
    } finally {
      for (const session of sessions) await close(far, session);
    }
  }, 60_000);

  test("drops at most 1% of frames across a 250 ms stall and counts drops across a 2 s stall without losing audio", async () => {
    const receiver = await open(far, "stall");
    try {
      await until(() => receiver.frames.length >= 24, "media did not start", 15_000);
      const measured = begin(far);
      const before = await pressure(receiver);
      stall(250);
      await sleep(2000);
      const short = await pressure(receiver);
      // The native video queue holds 8 frames, 333 ms at 24 fps; about 96
      // frames arrive in this window, so 1% is one frame.
      expect(short.droppedVideo - before.droppedVideo).toBeLessThanOrEqual(1n);
      expect(short.droppedAudio).toBe(0n);

      const stalledAt = performance.now();
      stall(2000);
      await sleep(2000);
      const long = await pressure(receiver);
      await report("stall", far, measured, [receiver]);
      const dropped = Number(long.droppedVideo - short.droppedVideo);
      // About 48 frames arrive in 2 s; the queue keeps the newest 8 and counts
      // each eviction. The 256-block audio queue rides through 2.56 s.
      expect(dropped).toBeGreaterThanOrEqual(30);
      expect(dropped).toBeLessThanOrEqual(48);
      expect(long.droppedAudio).toBe(0n);
      // The backlog reached the bounded observation queue at its reader's pace.
      expect(receiver.failures).toEqual([]);
      expect(Number(long.deliveredVideo) - receiver.frames.length).toBeLessThanOrEqual(4);
      const recovered = receiver.frames.filter((frame) => frame.at > stalledAt + 3000);
      expect(recovered.length).toBeGreaterThanOrEqual(12);
      expect(
        percentile(
          recovered.map((frame) => frame.latencyMs),
          0.95,
        ),
      ).toBeLessThanOrEqual(150);
    } finally {
      await close(far, receiver);
    }
  }, 60_000);

  test("renews a session while its predecessor streams, three times, on one process factory", async () => {
    let current = await open(far, "renewal-0");
    try {
      await until(() => current.frames.length >= 24, "first session did not stream", 15_000);
      for (let cycle = 1; cycle <= 3; cycle++) {
        const next = await open(far, `renewal-${cycle}`);
        try {
          await until(() => next.frames.length >= 24, `renewal ${cycle} did not stream`, 15_000);
          // Both sessions stream at full load before the old one drains.
          const overlap = await pressure(current);
          expect(overlap.droppedAudio).toBe(0n);
          const measured = begin(far);
          const during = next.frames.length;
          const sentDuring = (await far.sent(next.id)).frames;
          const shutdownMs = await close(far, current);
          await sleep(2000);
          const sent = (await far.sent(next.id)).frames - sentDuring;
          await sleep(300); // frames already sent are still in flight
          await report(`renewal ${cycle}`, far, measured, [next]);
          const window = next.frames.slice(during);
          expect(shutdownMs).toBeLessThan(2000);
          expect(current.failures).toEqual([]);
          // The replacement receives what its sender encodes (which is still
          // ramping up) and never freezes while its predecessor shuts down.
          expect(sent).toBeGreaterThanOrEqual(24);
          expect(window.length).toBeGreaterThanOrEqual(Math.floor(sent * 0.95));
          expect(
            Math.max(...window.slice(1).map((frame, index) => frame.at - window[index]!.at)),
          ).toBeLessThan(500);
          expect((await pressure(next)).droppedAudio).toBe(0n);
        } catch (error) {
          await close(far, next);
          throw error;
        }
        current = next;
      }
      expect(current.failures).toEqual([]);
    } finally {
      await close(far, current);
    }
  }, 120_000);
});
