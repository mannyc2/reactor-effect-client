// Spike A: today's bridge exactly as shipped (packages/native dist: NativePeer ->
// NativeBridge -> Koffi async polls -> Rust cdylib) against the libwebrtc far peer.
//
//   node js/spike-a-bridge.mjs [--sessions 1|2] [--seconds 60] [--loss 0.02 --delay-ms 40]
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import { resolveNativeBridge } from "../../../packages/native/dist/_internal/bridge.js";
import { NativePeer } from "../../../packages/native/dist/_internal/peer.js";
import {
  arg,
  Distribution,
  farPeer,
  probe,
  probeRtt,
  schedule,
  userData,
  wallMicros,
} from "./harness.mjs";

const sessions = arg("--sessions", 1);
const seconds = arg("--seconds", 60);
const loss = arg("--loss", 0);
const delayMs = arg("--delay-ms", 0);
const farArgs = ["--width", "1344", "--height", "768", "--fps", "24"];
if (loss > 0 || delayMs > 0) farArgs.push("--loss", String(loss), "--delay-ms", String(delayMs));
const far = farPeer(farArgs);
await far.ready;
const path = await resolveNativeBridge();

const tracks = [
  { name: "main_video", kind: "video", direction: "recvonly" },
  { name: "main_audio", kind: "audio", direction: "recvonly" },
];

const latency = new Distribution();
const rtt = new Distribution();
const counts = { video: 0, audio: 0, badSize: 0, noMetadata: 0, readerFailures: [] };

const open = async (id) => {
  const peer = new NativePeer(path);
  const scope = Effect.runSync(Scope.make());
  let channelsOpen = 0;
  let resolveOpen;
  const opened = new Promise((resolve) => (resolveOpen = resolve));
  const emit = (event) => {
    if (event.type === "ice" && event.candidate) far.candidate(id, event.candidate);
    else if (event.type === "channel" && event.open && ++channelsOpen === 2) resolveOpen();
    else if (event.type === "message" && event.channel === "control") {
      const ms = probeRtt(event.bytes);
      if (ms !== undefined) rtt.add(ms);
    } else if (event.type === "error")
      counts.readerFailures.push(`peer ${id}: ${event.error.code}`);
  };
  const prepared = await Effect.runPromise(Scope.provide(scope)(peer.prepare([], tracks, emit)));
  const answer = await far.answer(id, prepared.sdp);
  await Effect.runPromise(peer.answer(answer));
  await opened;
  const readVideo = () =>
    Effect.runFork(
      Stream.runForEach(peer.rawMedia.video("main_video"), (frame) =>
        Effect.sync(() => {
          counts.video++;
          if (frame.width !== 1344 || frame.height !== 768) counts.badSize++;
          const meta = userData(frame.metadata);
          if (meta === undefined) counts.noMetadata++;
          else latency.add((wallMicros() - meta.sentMicros) / 1000);
        }),
      ),
    );
  const readAudio = () =>
    Effect.runFork(
      Stream.runForEach(peer.rawMedia.audio("main_audio"), () => Effect.sync(() => counts.audio++)),
    );
  const watch = (fiber, what) =>
    fiber.addObserver((exit) => {
      if (Exit.isFailure(exit) && !Exit.hasInterrupts(exit))
        counts.readerFailures.push(`${what} ${id}: ${String(exit.cause).slice(0, 160)}`);
    });
  let video = readVideo();
  watch(video, "video");
  const audio = readAudio();
  watch(audio, "audio");
  return {
    peer,
    scope,
    get video() {
      return video;
    },
    restartVideo: () => {
      video = readVideo();
      watch(video, "video");
    },
    audio,
  };
};

const live = [];
let nonce = 0;
// Koffi async calls registered by NativeBridge (its private `active` set),
// sampled every 10 ms: this is the foreign-call occupancy of the pool.
const inFlight = new Distribution();
setInterval(() => {
  let n = 0;
  for (const s of live) n += s.peer.bridge.active.size;
  inFlight.add(n);
}, 10).unref();
const spike = {
  async start() {
    for (let i = 0; i < sessions; i++) live.push(await open(`s${i}`));
    while (counts.video < 5) await new Promise((r) => setTimeout(r, 20));
  },
  ping() {
    for (const s of live) Effect.runPromise(s.peer.send("control", probe(nonce++))).catch(() => {});
  },
  async interrupt() {
    const s = live[0];
    const before = counts.video;
    const started = performance.now();
    await Effect.runPromise(Fiber.interrupt(s.video));
    const interruptMs = performance.now() - started;
    s.restartVideo();
    return { interruptMs: Math.round(interruptMs * 10) / 10, framesBefore: before };
  },
  async shutdown() {
    const snapshots = [];
    for (const s of live) snapshots.push(await Effect.runPromise(s.peer.rawMedia.snapshot));
    const bridges = live.map((s) => s.peer.bridge);
    const inFlight = bridges.map((b) => b.active.size);
    const results = await Promise.allSettled(
      live.map(async (s) => {
        await Effect.runPromise(Fiber.interrupt(s.video));
        await Effect.runPromise(Fiber.interrupt(s.audio));
        await Effect.runPromise(s.peer.shutdown());
        await Effect.runPromise(Scope.close(s.scope, Exit.void));
      }),
    );
    return {
      ok: results.every((r) => r.status === "fulfilled"),
      errors: results.filter((r) => r.status === "rejected").map((r) => String(r.reason)),
      koffiCallsInFlightAtShutdown: inFlight,
      nativeSnapshots: snapshots.map((s) => ({
        droppedVideo: String(s.droppedVideo),
        droppedAudio: String(s.droppedAudio),
        deliveredVideo: String(s.deliveredVideo),
        deliveredAudio: String(s.deliveredAudio),
      })),
    };
  },
  counters() {
    return {
      video: counts.video,
      audio: counts.audio,
      latency,
      latencyCount: latency.values.length,
      rtt,
      drops: {
        badSize: counts.badSize,
        noMetadata: counts.noMetadata,
        readerFailures: counts.readerFailures,
        koffiCallsInFlight: inFlight.summary(),
      },
    };
  },
};

const result = await schedule(spike, {
  seconds,
  blockMs: arg("--block-ms", 250),
  far,
  label: `A bridge (sessions=${sessions}${loss || delayMs ? `, loss=${loss}, delay=${delayMs}ms` : ""})`,
});
console.log(JSON.stringify(result, null, 2));
process.exit(0);
