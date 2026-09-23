// Shared measurement harness for the native-host spikes. Runs on Node 24 and Bun 1.4.2.
import { spawn } from "node:child_process";
import { appendFileSync, readFileSync } from "node:fs";
import { stat } from "node:fs/promises";
import { dirname, join } from "node:path";
import process from "node:process";
import { createInterface } from "node:readline";
import { fileURLToPath } from "node:url";

export const here = dirname(fileURLToPath(import.meta.url));
export const runtime =
  typeof globalThis.Bun !== "undefined"
    ? `bun ${globalThis.Bun.version}`
    : `node ${process.version}`;
export const wallMicros = () => (performance.timeOrigin + performance.now()) * 1000;
export const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

export const arg = (name, fallback) => {
  const index = process.argv.indexOf(name);
  if (index < 0) return fallback;
  const value = process.argv[index + 1];
  return typeof fallback === "number" ? Number(value) : value;
};

/** Spawn the libwebrtc far peer (rust/src/bin/far_peer.rs) with JSON-lines signaling. */
export const farPeer = (args = []) => {
  const bin = join(here, "../rust/target/release/far_peer");
  const child = spawn(bin, args, { stdio: ["pipe", "pipe", "inherit"] });
  const waiters = [];
  const lines = createInterface({ input: child.stdout });
  lines.on("line", (line) => {
    let message;
    try {
      message = JSON.parse(line);
    } catch {
      return;
    }
    for (let i = 0; i < waiters.length; i++) {
      const w = waiters[i];
      if (w.op === message.op && (w.id === undefined || w.id === message.id)) {
        waiters.splice(i, 1);
        w.resolve(message);
        return;
      }
    }
  });
  const next = (op, id) => new Promise((resolve) => waiters.push({ op, id, resolve }));
  const send = (message) => child.stdin.write(`${JSON.stringify(message)}\n`);
  return {
    pid: child.pid,
    ready: next("ready"),
    send,
    next,
    answer: async (id, sdp) => {
      const reply = next("answer", id);
      send({ op: "offer", id, sdp });
      return (await reply).sdp;
    },
    candidate: (id, c) =>
      send({
        op: "candidate",
        id,
        candidate: c.candidate,
        sdpMid: c.sdp_mid ?? c.sdpMid ?? null,
        sdpMLineIndex: c.sdp_mline_index ?? c.sdpMLineIndex ?? null,
      }),
    stats: async () => {
      const reply = next("stats");
      send({ op: "stats" });
      return reply;
    },
    close: async (id) => {
      const reply = next("closed", id);
      send({ op: "close", id });
      return reply;
    },
    quit: async () => {
      const exited = new Promise((resolve) => child.once("exit", resolve));
      send({ op: "quit" });
      const timer = setTimeout(() => child.kill("SIGKILL"), 10_000);
      await exited;
      clearTimeout(timer);
    },
  };
};

export class Distribution {
  values = [];
  add(value) {
    this.values.push(value);
  }
  summary() {
    const v = [...this.values].sort((a, b) => a - b);
    const q = (p) => (v.length === 0 ? null : v[Math.min(v.length - 1, Math.floor(p * v.length))]);
    const round = (x) => (x === null ? null : Math.round(x * 10) / 10);
    return {
      n: v.length,
      p50: round(q(0.5)),
      p95: round(q(0.95)),
      p99: round(q(0.99)),
      max: round(v.length === 0 ? null : v[v.length - 1]),
    };
  }
}

const ticks = 100; // USER_HZ on Linux
const procCpu = (pid) => {
  try {
    const fields = readFileSync(`/proc/${pid}/stat`, "utf8").split(") ")[1].split(" ");
    return (Number(fields[11]) + Number(fields[12])) / ticks;
  } catch {
    return undefined;
  }
};
const procRss = (pid) => {
  try {
    const match = /VmRSS:\s+(\d+) kB/.exec(readFileSync(`/proc/${pid}/status`, "utf8"));
    return match ? Number(match[1]) / 1024 : undefined;
  } catch {
    return undefined;
  }
};
const procThreads = (pid) => {
  try {
    const match = /Threads:\s+(\d+)/.exec(readFileSync(`/proc/${pid}/status`, "utf8"));
    return match ? Number(match[1]) : undefined;
  } catch {
    return undefined;
  }
};

/**
 * Samples this process's CPU and RSS, extra child processes (ffmpeg), the
 * event-loop lag, and the latency of a libuv-threadpool task (fs.stat), which
 * is where Koffi's async calls run on Node.
 */
export const sampler = ({ children = () => [] } = {}) => {
  const rss = new Distribution();
  const lag = new Distribution();
  const pool = new Distribution();
  const childRss = new Distribution();
  let peakRss = 0;
  let peakChildRss = 0;
  let peakThreads = 0;
  const startCpu = process.cpuUsage();
  const startWall = performance.now();
  const childStart = new Map();
  const childLast = new Map();
  let expected = performance.now() + 20;
  const lagTimer = setInterval(() => {
    const now = performance.now();
    lag.add(Math.max(0, now - expected));
    expected = now + 20;
  }, 20);
  let probing = false;
  const poolTimer = setInterval(() => {
    if (probing) return;
    probing = true;
    const started = performance.now();
    stat("/").then(() => {
      pool.add(performance.now() - started);
      probing = false;
    });
  }, 50);
  const sampleTimer = setInterval(() => {
    const r = process.memoryUsage().rss / 1048576;
    rss.add(r);
    peakRss = Math.max(peakRss, r);
    peakThreads = Math.max(peakThreads, procThreads(process.pid) ?? 0);
    let c = 0;
    for (const pid of children()) {
      if (!childStart.has(pid)) childStart.set(pid, procCpu(pid) ?? 0);
      const cpuNow = procCpu(pid);
      if (cpuNow !== undefined) childLast.set(pid, cpuNow);
      c += procRss(pid) ?? 0;
    }
    if (c > 0) {
      childRss.add(c);
      peakChildRss = Math.max(peakChildRss, c);
    }
  }, 250);
  const touch = () => {
    for (const pid of children()) {
      const cpuNow = procCpu(pid);
      if (cpuNow !== undefined) childLast.set(pid, cpuNow);
    }
  };
  return {
    lag,
    pool,
    touch,
    stop() {
      clearInterval(lagTimer);
      clearInterval(poolTimer);
      clearInterval(sampleTimer);
      const cpu = process.cpuUsage(startCpu);
      const wall = (performance.now() - startWall) / 1000;
      let childCpu = 0;
      for (const [pid, start] of childStart)
        childCpu += (procCpu(pid) ?? childLast.get(pid) ?? start) - start;
      return {
        wallSeconds: Math.round(wall * 10) / 10,
        cpuSeconds: Math.round(((cpu.user + cpu.system) / 1e6) * 100) / 100,
        cpuPercentOfOneCore: Math.round(((cpu.user + cpu.system) / 1e6 / wall) * 1000) / 10,
        childCpuSeconds: Math.round(childCpu * 100) / 100,
        rssMiB: { mean: Math.round(avg(rss.values)), peak: Math.round(peakRss) },
        childRssMiB: { mean: Math.round(avg(childRss.values)), peak: Math.round(peakChildRss) },
        peakThreads,
        eventLoopLagMs: lag.summary(),
        threadpoolStatMs: pool.summary(),
      };
    },
    farCpu(pid) {
      return procCpu(pid);
    },
  };
};

const avg = (values) =>
  values.length === 0 ? 0 : values.reduce((a, b) => a + b, 0) / values.length;

/** Block the JavaScript thread. */
export const block = (ms) => {
  const until = performance.now() + ms;
  let spins = 0;
  while (performance.now() < until) spins++;
  return spins;
};

/** Parse the far peer's 16-byte user_data: [wall micros u64 LE][sequence u64 LE]. */
export const userData = (bytes) => {
  if (bytes === undefined || bytes.byteLength < 16) return undefined;
  const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  return {
    sentMicros: Number(view.getBigUint64(0, true)),
    sequence: Number(view.getBigUint64(8, true)),
  };
};

/** A data-channel round-trip probe payload: [wall micros f64][nonce u32]. */
export const probe = (nonce) => {
  const bytes = new Uint8Array(12);
  const view = new DataView(bytes.buffer);
  view.setFloat64(0, wallMicros(), true);
  view.setUint32(8, nonce, true);
  return bytes;
};
export const probeRtt = (bytes) => {
  if (bytes.byteLength !== 12) return undefined;
  const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  return (wallMicros() - view.getFloat64(0, true)) / 1000;
};

/**
 * The common spike schedule. `spike` supplies:
 *   start(): connect and begin receiving (resolves once media flows)
 *   ping(): send one control-channel probe
 *   interrupt(): interrupt the video reader while packets are in flight, then resume
 *   shutdown(): close everything; resolves when native quiescence is reached
 *   counters(): { video, audio, latency: Distribution, rtt: Distribution, drops: {...} }
 */
const phase = (name) =>
  process.env.SPIKE_DEBUG &&
  appendFileSync(
    process.env.SPIKE_DEBUG,
    `${(performance.now() / 1000).toFixed(2)} phase ${name}\n`,
  );

export const schedule = async (spike, options) => {
  const {
    seconds = 60,
    blockAt = 20,
    blockMs = 250,
    interruptAt = 40,
    far,
    children = () => [],
    label,
  } = options;
  const sample = sampler({ children });
  const farCpuStart = sample.farCpu(far.pid) ?? 0;
  const started = performance.now();
  phase("start");
  let startTimer;
  await Promise.race([
    spike.start(),
    new Promise((_, reject) => {
      startTimer = setTimeout(
        () => reject(new Error("spike did not connect and receive media within 30 s")),
        30_000,
      );
    }),
  ]);
  clearTimeout(startTimer);
  phase("started");
  const connectedMs = Math.round(performance.now() - started);
  const t0 = performance.now();
  const at = (s) => t0 + s * 1000;
  const marks = {};
  let pings = 0;
  const pingTimer = setInterval(() => {
    spike.ping(pings++);
  }, 1000);
  while (performance.now() < at(blockAt)) await sleep(50);
  const before = spike.counters();
  phase("block");
  marks.blockStart = performance.now();
  block(blockMs);
  marks.blockEnd = performance.now();
  await sleep(1000);
  const afterBlock = spike.counters();
  while (performance.now() < at(interruptAt)) await sleep(50);
  const beforeInterrupt = spike.counters();
  phase("interrupt");
  const interrupted = await spike.interrupt();
  phase("interrupted");
  await sleep(1000);
  const afterInterrupt = spike.counters();
  while (performance.now() < at(seconds)) await sleep(50);
  clearInterval(pingTimer);
  const end = spike.counters();
  phase("far stats");
  const farStats = await far.stats();
  phase("far stats done");
  sample.touch();
  const shutdownStarted = performance.now();
  const shutdown = await spike.shutdown();
  phase("shutdown done");
  const shutdownMs = Math.round(performance.now() - shutdownStarted);
  const farCpu = (sample.farCpu(far.pid) ?? farCpuStart) - farCpuStart;
  const resources = sample.stop();
  await far.quit();
  const session = farStats.sessions[0] ?? {};
  const video = session.outbound?.find((o) => /video/i.test(o.kind)) ?? {};
  const sum = (f) => farStats.sessions.reduce((a, s) => a + (f(s) ?? 0), 0);
  const videoOf = (s) => s.outbound?.find((o) => /video/i.test(o.kind)) ?? {};
  return {
    label,
    runtime,
    connectedMs,
    seconds,
    video: {
      farSessions: farStats.sessions.length,
      pushedByFarPeer: sum((s) => s.videoPushed),
      farEncodedFramesSent: sum((s) => videoOf(s).framesSent),
      farFrameSize: `${video.frameWidth}x${video.frameHeight}`,
      farRetransmittedPackets: video.retransmittedPacketsSent,
      farNackCount: video.nackCount,
      farTargetBitrate: video.targetBitrate,
      bytesSent: video.bytesSent,
      received: end.video,
      latencyMs: end.latency.summary(),
      aroundBlock: {
        framesDuring: afterBlock.video - before.video,
        latencyMaxMs: maxSince(end.latency, before.latencyCount),
      },
    },
    audio: { pushedByFarPeer: sum((s) => s.audioPushed), received: end.audio },
    dataChannel: { pings, rttMs: end.rtt.summary(), echoedByFar: sum((s) => s.echoed) },
    drops: end.drops,
    block: { requestedMs: blockMs, measuredMs: Math.round(marks.blockEnd - marks.blockStart) },
    interrupt: {
      ...interrupted,
      videoDuringSecondAfter: afterInterrupt.video - beforeInterrupt.video,
    },
    shutdown: { ms: shutdownMs, ...shutdown },
    relay: farStats.relay ?? null,
    farPeerCpuSeconds: Math.round(farCpu * 100) / 100,
    resources,
  };
};

const maxSince = (distribution, from) => {
  const slice = distribution.values.slice(from, from + 60);
  return slice.length === 0 ? null : Math.round(Math.max(...slice));
};
