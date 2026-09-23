import { execFile, spawn } from "node:child_process";
import type { ChildProcessByStdio } from "node:child_process";
import { existsSync } from "node:fs";
import { availableParallelism, loadavg, networkInterfaces } from "node:os";
import { createInterface } from "node:readline";
import type { Readable, Writable } from "node:stream";
import { fileURLToPath } from "node:url";
import { promisify } from "node:util";
import * as Cause from "effect/Cause";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as Redacted from "effect/Redacted";
import * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import * as FetchHttpClient from "effect/unstable/http/FetchHttpClient";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { afterAll, beforeAll, describe, expect, test } from "vitest";
import { FetchHttp } from "reactor-effect-client";
import type { ReactorError } from "reactor-effect-client";
import type { IceCandidate, MediaPressure, PeerEvent } from "reactor-effect-client/host";
import { checkNativeBridge } from "../src/_internal/bridge.js";
import { NativePeer, defaultShutdownTimeout } from "../src/_internal/peer.js";
import * as Native from "../src/index.js";
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

const percentile = (values: readonly number[], p: number): number => {
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

const sleep = (ms: number): Promise<void> => new Promise((resolve) => setTimeout(resolve, ms));

const round = (value: number): number => Math.round(value * 100) / 100;

type Message = Readonly<Record<string, unknown>>;

const record = (value: unknown): Message =>
  typeof value === "object" && value !== null && !Array.isArray(value) ? (value as Message) : {};

const execFileAsync = promisify(execFile);

/**
 * Run ps off the JavaScript thread, which the tests measure; empty where ps
 * cannot say.
 */
const ps = async (...args: readonly string[]): Promise<string> => {
  try {
    return (await execFileAsync("ps", args, { encoding: "utf8" })).stdout.trim();
  } catch {
    return "";
  }
};

/** The host's busiest processes, as "percent command" strings. */
const busiest = async (): Promise<string[]> => {
  const table = await ps("-Ao", "pcpu=,comm=");
  return table
    .split("\n")
    .map((line) => line.trim())
    .filter((line) => line !== "")
    .sort((a, b) => Number.parseFloat(b) - Number.parseFloat(a))
    .slice(0, 5)
    .map((line) => line.replace(/\s+/, " ").replace(/ .*\//, " "));
};

/** CPU seconds a process has used: Linux ps prints [dd-]hh:mm:ss and macOS m:ss.ss. */
const cpuSeconds = async (pid: number | undefined): Promise<number> => {
  const time = await ps("-o", "time=", "-p", String(pid));
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
  /** Sampled frames the bridge held for this receiver; see sample(). */
  readonly held: number[];
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
    held: [],
    failures: [],
    audio: 0,
    connected: false,
    channels: new Set(),
  };
  try {
    await run(
      Effect.gen(function* () {
        yield* Effect.addFinalizer(() => peer.shutdown.pipe(Effect.orDie));
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

/** Frames that entered a receiver's native video queue, whatever became of them. */
const arrived = (snapshot: MediaPressure): number =>
  snapshot.queuedVideo + Number(snapshot.deliveredVideo + snapshot.droppedVideo);

/** The same for audio blocks. */
const arrivedAudio = (snapshot: MediaPressure): number =>
  snapshot.queuedAudio + Number(snapshot.deliveredAudio + snapshot.droppedAudio);

/** Wait until nothing is queued in the receiver's native video queue. */
const drained = async (receiver: Receiver): Promise<MediaPressure> => {
  for (let attempt = 0; ; attempt++) {
    const snapshot = await pressure(receiver);
    if (snapshot.queuedVideo === 0 || attempt === 100) return snapshot;
    await sleep(10);
  }
};

/**
 * Record the frames the bridge holds for a receiver: queued natively, or taken
 * but not yet seen by its subscriber. Each is one frame interval of delay the
 * bridge adds; one is normal while a frame is being delivered. End-to-end
 * latency also carries the far peer's encoder and pacer and the receiver's
 * jitter buffer, which follow how the host schedules both processes, so the
 * report prints it and the tests assert this.
 */
const sample = async (receiver: Receiver): Promise<MediaPressure> => {
  const snapshot = await pressure(receiver);
  receiver.held.push(
    snapshot.queuedVideo + Math.max(0, Number(snapshot.deliveredVideo) - receiver.frames.length),
  );
  return snapshot;
};

/**
 * Close the receiver's scope, which shuts its peer down, and require a clean
 * close: the native owner joined, without the Shutdown a session's close would
 * record in localErrors, inside closeBound. Returns milliseconds taken.
 */
const close = async (far: FarPeer, receiver: Pick<Receiver, "id" | "scope">): Promise<number> => {
  const started = performance.now();
  const exit = await Effect.runPromiseExit(Scope.close(receiver.scope, Exit.void));
  const elapsed = performance.now() - started;
  await far.close(receiver.id);
  expect(
    Exit.isSuccess(exit),
    `${receiver.id} shutdown ${Exit.isFailure(exit) ? Cause.pretty(exit.cause) : ""}`,
  ).toBe(true);
  expect(elapsed, `${receiver.id} close milliseconds`).toBeLessThan(closeBound);
  return elapsed;
};

/** The start of a measured window: its time and both processes' CPU use. */
interface Window {
  readonly at: number;
  readonly cpu: NodeJS.CpuUsage;
  readonly farCpu: number;
}

const begin = async (far: FarPeer): Promise<Window> => ({
  at: performance.now(),
  cpu: process.cpuUsage(),
  farCpu: await cpuSeconds(far.pid),
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
      const native = (await run(receiver.peer.stats)).map(record);
      const media = await pressure(receiver);
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
        far: round(((await cpuSeconds(far.pid)) - window.farCpu) / seconds),
      },
      // macOS runs throttled background processes at priority 4 and utility at 20.
      priority: {
        host: await ps("-o", "pri=", "-p", String(process.pid)),
        far: await ps("-o", "pri=", "-p", String(far.pid)),
      },
      busiest: await busiest(),
      clockSkewMs: round(Date.now() - wallMs()),
      sessions,
    })}`,
  );
};

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

  test("receives 1344x768 at 24 fps, dropping at most 1% and holding at most two frames at p95", async () => {
    const receiver = await open(far, "throughput");
    try {
      await until(() => receiver.frames.length > 0, "no first frame", 15_000);
      const measured = await begin(far);
      const before = await pressure(receiver);
      while (performance.now() - measured.at < 10_000) {
        await ping(receiver);
        await sample(receiver);
        await sleep(250);
      }
      const seconds = (performance.now() - measured.at) / 1000;
      const snapshot = await pressure(receiver);
      const sent = await far.sent(receiver.id);
      await report("throughput", far, measured, [receiver]);
      expect(receiver.failures).toEqual([]);
      expect([sent.width, sent.height]).toEqual([WIDTH, HEIGHT]);
      expect(receiver.sizes.get(`${WIDTH}x${HEIGHT}`) ?? 0).toBeGreaterThanOrEqual(
        Math.floor(receiver.frames.length * 0.9),
      );
      const reached = arrived(snapshot) - arrived(before);
      expect(reached / seconds, "frames per second reaching the bridge").toBeGreaterThanOrEqual(20);
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
      const measured = await begin(far);
      const start = await Promise.all(sessions.map(pressure));
      while (performance.now() - measured.at < 10_000) {
        await Promise.all(sessions.map(ping));
        await Promise.all(sessions.map(sample));
        await sleep(250);
      }
      const seconds = (performance.now() - measured.at) / 1000;
      const end = await Promise.all(sessions.map(pressure));
      await report("two sessions", far, measured, sessions);
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
    } finally {
      for (const session of sessions) await close(far, session);
    }
  }, 60_000);

  test("drops at most one frame across a 250 ms stall and evicts only the overflow of a 2 s stall, without losing audio", async () => {
    const receiver = await open(far, "stall");
    try {
      await until(() => receiver.frames.length >= 24, "media did not start", 15_000);
      const measured = await begin(far);
      const before = await drained(receiver);
      stall(250);
      await sleep(2000);
      const short = await drained(receiver);
      // The native video queue holds 8 frames, 333 ms at 24 fps.
      expect(short.droppedVideo - before.droppedVideo).toBeLessThanOrEqual(1n);
      expect(short.droppedAudio).toBe(0n);

      const stalledAt = performance.now();
      stall(2000);
      const stalled = await pressure(receiver);
      await sleep(2000);
      const long = await pressure(receiver);
      await report("stall", far, measured, [receiver]);
      // About 48 frames reach the bridge in 2 s; the queue keeps the newest 8
      // and counts each eviction. The 256-block audio queue rides through 2.56 s.
      const reached = arrived(stalled) - arrived(short);
      expect(reached, "frames reaching the bridge during the stall").toBeGreaterThanOrEqual(30);
      // A frame already queued, or taken, around either edge moves this by one.
      expect(
        Math.abs(Number(long.droppedVideo - short.droppedVideo) - (reached - 8)),
      ).toBeLessThanOrEqual(2);
      expect(long.droppedAudio).toBe(0n);
      // The backlog reached the bounded observation queue at its reader's pace,
      // drained, and delivery went on.
      expect(receiver.failures).toEqual([]);
      expect(Number(long.deliveredVideo) - receiver.frames.length).toBeLessThanOrEqual(4);
      expect(long.queuedVideo).toBeLessThanOrEqual(1);
      const recovered = receiver.frames.filter((frame) => frame.at > stalledAt + 3000);
      expect(recovered.length).toBeGreaterThanOrEqual(12);
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
          const measured = await begin(far);
          const during = next.frames.length;
          const before = await pressure(next);
          const shutdownMs = await close(far, current);
          await sleep(2000);
          const after = await pressure(next);
          const seconds = (performance.now() - measured.at) / 1000;
          await report(`renewal ${cycle}`, far, measured, [next]);
          const window = next.frames.slice(during);
          expect(shutdownMs).toBeLessThan(2000);
          expect(current.failures).toEqual([]);
          // While its predecessor shuts down, the replacement keeps receiving
          // at load, drops at most one frame and never freezes.
          expect(
            (arrived(after) - arrived(before)) / seconds,
            "frames per second reaching the replacement",
          ).toBeGreaterThanOrEqual(15);
          expect(after.droppedVideo - before.droppedVideo).toBeLessThanOrEqual(1n);
          expect(
            Math.max(...window.slice(1).map((frame, index) => frame.at - window[index]!.at)),
          ).toBeLessThan(500);
          expect(after.droppedAudio).toBe(0n);
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

  test("closes a canonical session over real libwebrtc cleanly, well inside the shutdown deadline", async () => {
    const id = "canonical";
    const descriptor = {
      session_id: id,
      state: "ACTIVE",
      capabilities: {
        protocol_version: "1.0",
        tracks,
        commands: [{ name: "echo", schema: {} }],
      },
      selected_transport: { protocol: "webrtc", version: "1.0" },
    };
    let answer: Promise<string> | undefined;
    let deleted = false;
    // A coordinator that relays signaling to the far peer, which drops
    // candidates sent before the offer; its own arrive in its answer.
    const coordinator = async (
      input: string | Request | URL,
      init?: RequestInit,
    ): Promise<Response> => {
      const request = new Request(input, init);
      const path = new URL(request.url).pathname;
      if (path === "/sessions" && request.method === "POST") return Response.json(descriptor);
      if (path === `/sessions/${id}` && request.method === "GET")
        return deleted
          ? Response.json({ error: "session not found" }, { status: 404 })
          : Response.json(descriptor);
      if (path === `/sessions/${id}` && request.method === "DELETE") {
        deleted = true;
        return new Response(null, { status: 202 });
      }
      if (path.endsWith("/ice_servers")) return Response.json({ ice_servers: [] });
      if (path.endsWith("/connections")) return Response.json({ connection_id: 1 });
      if (path.endsWith("/ice_candidates")) {
        const body = record(await request.json());
        for (const candidate of Array.isArray(body.candidates) ? body.candidates : [])
          far.candidate(id, candidate as IceCandidate);
        return new Response(null, { status: 204 });
      }
      if (path.endsWith("/sdp_params")) {
        if (request.method === "GET") return Response.json({ sdp_answer: await answer });
        answer = far.answer(id, String(record(await request.json()).sdp_offer));
        return new Response(null, { status: 204 });
      }
      return Response.json({ error: `unhandled route ${path}` }, { status: 404 });
    };
    try {
      const result = await Effect.runPromise(
        Effect.scoped(
          Effect.gen(function* () {
            const factory = yield* Native.make(
              { apiUrl: "https://coordinator.far-peer" },
              { libraryPath },
            );
            const client = yield* factory.create({
              model: "far-peer",
              jwt: Redacted.make("far-peer-token"),
            });
            yield* client.connect;
            const media = yield* Native.media(client);
            const frames = yield* media.video("main_video").pipe(Stream.take(24), Stream.runCount);
            const started = performance.now();
            const report = yield* client.close;
            return { frames, report, closeMs: performance.now() - started };
          }),
        ).pipe(
          Effect.provide(Layer.merge(FetchHttp.layer, NodeServices.layer)),
          Effect.provideService(FetchHttpClient.Fetch, coordinator),
        ),
      );
      console.log(`native-close ${JSON.stringify({ runtime, closeMs: round(result.closeMs) })}`);
      expect(result.frames).toBe(24);
      expect(result.report.localClosed).toBe(true);
      expect(result.report.localErrors).toEqual([]);
      expect(result.report.remote).toMatchObject({ attempted: true, confirmed: true });
      expect(result.closeMs).toBeLessThan(closeBound);
    } finally {
      await far.close(id);
    }
  }, 60_000);

  test("reports a real ICE failure as IceFailed with its candidate-pair detail through the events pump", async () => {
    const id = "ice-failure";
    const peer = new NativePeer(libraryPath);
    const scope = await run(Scope.make());
    const errors: ReactorError[] = [];
    const states: string[] = [];
    try {
      const checking = await run(
        Effect.gen(function* () {
          yield* Effect.addFinalizer(() => peer.shutdown.pipe(Effect.orDie));
          // The bridge's own candidates never reach the far peer, so it cannot
          // reach the bridge either and teach it a peer-reflexive candidate.
          const prepared = yield* peer.prepare([], tracks, (event: PeerEvent) => {
            if (event.type === "state") states.push(event.state);
            else if (event.type === "error") errors.push(event.error);
          });
          const answer = yield* Effect.promise(() => far.answer(id, prepared.sdp));
          yield* peer.answer(unreachableAnswer(answer, unreachable()));
          // While ICE checks the pairs, they are there to see.
          yield* Effect.promise(() => sleep(1000));
          return (yield* peer.stats).map(record);
        }).pipe(Scope.provide(scope)),
      );
      expect(checking.some((entry) => entry.type === "candidate-pair")).toBe(true);
      expect(
        checking.filter((entry) => entry.type === "candidate-pair" && entry.state === "succeeded"),
      ).toEqual([]);
      await until(() => errors.length > 0, "ICE never failed", 45_000);
      console.log(
        `ice-failure ${JSON.stringify({ runtime, states, detail: errors[0]?.context.detail })}`,
      );
      expect(states).not.toContain("connected");
      // libwebrtc reports failure once it has pruned the last timed-out pair,
      // so the pairs the classification reads may already be gone.
      expect(errors).toEqual([
        expect.objectContaining({
          code: "IceFailed",
          message: "native peer found no working ICE candidate pair",
          context: expect.objectContaining({
            detail: { pairs: expect.any(Number), candidateTypes: expect.any(Array) },
          }),
        }),
      ]);
    } finally {
      await close(far, { id, scope });
    }
  }, 60_000);
});
