// Spike B: the shrunk Koffi bridge (bridge-b/). Native threads never wait on
// JavaScript: libwebrtc callbacks copy once into bounded typed queues and set
// a readiness bit; one notifier thread per peer invokes a Koffi callback, and
// JavaScript drains synchronously with one nonblocking call (one copy) per
// frame. One PeerConnectionFactory per process.
//
//   node|bun js/spike-b-bridge.mjs [--sessions 1|2] [--seconds 60] [--loss 0.02 --delay-ms 40]
import koffi from "koffi";
import { appendFileSync } from "node:fs";
import { join } from "node:path";
import {
  arg,
  Distribution,
  farPeer,
  here,
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

const lib = koffi.load(join(here, "../bridge-b/target/release/libreactor_effect_native_b.so"));
const NotifyFn = koffi.proto("void NotifyFn(uint32_t mask)");
const api = {
  abi: lib.func("uint32_t reactor_effect_abi_version(void)"),
  create: lib.func("void *reactor_effect_peer_create(void)"),
  call: lib.func(
    "int reactor_effect_peer_call(void *peer, uint32_t operation, const uint8_t *request, size_t request_len, uint8_t *response, size_t response_cap, _Out_ size_t *response_len)",
  ),
  send: lib.func(
    "int reactor_effect_peer_send(void *peer, uint32_t channel, const uint8_t *data, size_t data_len, uint8_t *error, size_t error_cap, _Out_ size_t *error_len)",
  ),
  pollEvent: lib.func(
    "int reactor_effect_peer_poll_event(void *peer, uint32_t timeout_ms, uint8_t *out, size_t out_cap, _Out_ size_t *out_len)",
  ),
  setNotify: lib.func("int reactor_effect_peer_set_notify(void *peer, NotifyFn *callback)"),
  takeVideo: lib.func(
    "int reactor_effect_peer_take_video(void *peer, uint8_t *header, uint8_t *bgra, size_t bgra_cap, uint8_t *meta, size_t meta_cap)",
  ),
  takeAudio: lib.func(
    "int reactor_effect_peer_take_audio(void *peer, uint8_t *header, int16_t *pcm, size_t pcm_cap)",
  ),
  close: lib.func("void reactor_effect_peer_close(void *peer)"),
  shutdown: lib.func(
    "int reactor_effect_peer_shutdown(void *peer, uint8_t *error, size_t error_cap, _Out_ size_t *error_len)",
  ),
  destroy: lib.func("void reactor_effect_peer_destroy(void *peer)"),
};
if (api.abi() !== 3) throw new Error("bridge-b ABI mismatch");

// Status codes shared with packages/native: AGAIN (1) and CLOSED (3) both end a drain.
const OK = 0,
  TOO_SMALL = 2;
const asyncCall = (fn, ...args) =>
  new Promise((resolve, reject) =>
    fn.async(...args, (error, status) => (error ? reject(error) : resolve(status))),
  );
const text = new TextDecoder();
const encode = (value) =>
  new TextEncoder().encode(typeof value === "string" ? value : JSON.stringify(value));

const latency = new Distribution();
const rtt = new Distribution();
const counts = {
  video: 0,
  audio: 0,
  badSize: 0,
  noMetadata: 0,
  detachedDrops: 0,
  callbacks: 0,
  readerFailures: [],
};
let consumerAttached = true;
const trace = (...a) =>
  process.env.SPIKE_DEBUG &&
  appendFileSync(
    process.env.SPIKE_DEBUG,
    `${(performance.now() / 1000).toFixed(2)} ${a.join(" ")}\n`,
  );
if (process.env.SPIKE_DEBUG)
  setInterval(
    () => trace(JSON.stringify({ v: counts.video, a: counts.audio, cb: counts.callbacks })),
    1000,
  ).unref();

class Peer {
  handle = api.create();
  eventScratch = new Uint8Array(64 * 1024);
  videoHeader = new Uint8Array(40);
  videoView = new DataView(this.videoHeader.buffer);
  audioHeader = new Uint8Array(16);
  audioView = new DataView(this.audioHeader.buffer);
  bgra = new Uint8Array(1344 * 768 * 4);
  meta = new Uint8Array(4096);
  pcm = new Int16Array(480);
  closed = false;
  channelsOpen = 0;
  constructor(id) {
    this.id = id;
    this.opened = new Promise((resolve) => (this.resolveOpen = resolve));
    this.notify = koffi.register((mask) => this.drain(mask), koffi.pointer(NotifyFn));
    if (api.setNotify(this.handle, this.notify) !== OK) throw new Error("set_notify failed");
  }
  async request(operation, body) {
    const request = encode(body);
    const response = Buffer.allocUnsafe(4 * 1024 * 1024);
    const length = [0];
    const status = await asyncCall(
      api.call,
      this.handle,
      operation,
      request,
      request.length,
      response,
      response.length,
      length,
    );
    const json = JSON.parse(text.decode(response.subarray(0, Number(length[0]))));
    if (status !== OK)
      throw Object.assign(new Error(`native call ${operation} failed`), { native: json });
    return json;
  }
  send(channel, bytes) {
    const error = Buffer.allocUnsafe(4096);
    return asyncCall(
      api.send,
      this.handle,
      channel === "control" ? 0 : 1,
      bytes,
      bytes.length,
      error,
      error.length,
      [0],
    );
  }
  drain(mask) {
    counts.callbacks++;
    if (this.closed) return;
    if (mask & 1) this.drainEvents();
    if (mask & 2) this.drainVideo();
    if (mask & 4) this.drainAudio();
  }
  drainEvents() {
    const length = [0];
    for (;;) {
      const status = api.pollEvent(
        this.handle,
        0,
        this.eventScratch,
        this.eventScratch.length,
        length,
      );
      if (status === TOO_SMALL) {
        this.eventScratch = new Uint8Array(Number(length[0]));
        continue;
      }
      if (status !== OK) return;
      const bytes = this.eventScratch.subarray(0, Number(length[0]));
      const headerLength = new DataView(bytes.buffer, bytes.byteOffset).getUint32(0, true);
      const header = JSON.parse(text.decode(bytes.subarray(4, 4 + headerLength)));
      const payload = bytes.slice(4 + headerLength);
      if (header.type === "ice" && header.candidate) far.candidate(this.id, header.candidate);
      else if (header.type === "channel" && header.open && ++this.channelsOpen === 2)
        this.resolveOpen();
      else if (header.type === "message" && header.channel === "control") {
        const ms = probeRtt(payload);
        if (ms !== undefined) rtt.add(ms);
      } else if (header.type === "error") counts.readerFailures.push(`${this.id}: ${header.code}`);
    }
  }
  drainVideo() {
    for (;;) {
      const status = api.takeVideo(
        this.handle,
        this.videoHeader,
        this.bgra,
        this.bgra.length,
        this.meta,
        this.meta.length,
      );
      if (status === TOO_SMALL) {
        const v = this.videoView;
        const dataLength = v.getUint32(8, true),
          metaLength = v.getUint32(12, true);
        if (dataLength > this.bgra.length) this.bgra = new Uint8Array(dataLength);
        if (metaLength > this.meta.length) this.meta = new Uint8Array(metaLength);
        continue;
      }
      if (status !== OK) return;
      const v = this.videoView;
      const width = v.getUint32(0, true),
        height = v.getUint32(4, true),
        dataLength = v.getUint32(8, true),
        metaLength = v.getUint32(12, true);
      // The consumer owns this frame's bytes; the next take gets a fresh buffer.
      const frame = {
        width,
        height,
        frameId: v.getBigUint64(16, true),
        timestampMicros: v.getBigUint64(24, true),
        data: dataLength === this.bgra.length ? this.bgra : this.bgra.subarray(0, dataLength),
        metadata: this.meta.slice(0, metaLength),
      };
      this.bgra = new Uint8Array(this.bgra.length);
      deliverVideo(frame);
    }
  }
  drainAudio() {
    for (;;) {
      const status = api.takeAudio(this.handle, this.audioHeader, this.pcm, this.pcm.length);
      if (status === TOO_SMALL) {
        this.pcm = new Int16Array(this.audioView.getUint32(8, true));
        continue;
      }
      if (status !== OK) return;
      const samples = this.audioView.getUint32(8, true);
      const frame = {
        sampleRate: this.audioView.getUint32(0, true),
        channels: this.audioView.getUint32(4, true),
        samples: samples === this.pcm.length ? this.pcm : this.pcm.subarray(0, samples),
      };
      this.pcm = new Int16Array(this.pcm.length);
      counts.audio++;
      void frame;
    }
  }
  async shutdown() {
    trace("shutdown", this.id, "begin");
    this.closed = true;
    api.close(this.handle);
    const error = Buffer.allocUnsafe(4096);
    const status = await asyncCall(api.shutdown, this.handle, error, error.length, [0]);
    trace("shutdown", this.id, "joined", status);
    api.destroy(this.handle);
    koffi.unregister(this.notify);
    return status;
  }
}

const deliverVideo = (frame) => {
  if (!consumerAttached) {
    counts.detachedDrops++;
    return;
  }
  counts.video++;
  if (frame.width !== 1344 || frame.height !== 768) counts.badSize++;
  const meta = userData(frame.metadata);
  if (meta === undefined) counts.noMetadata++;
  else latency.add((wallMicros() - meta.sentMicros) / 1000);
};

const tracks = [
  { name: "main_video", kind: "video", direction: "recvonly" },
  { name: "main_audio", kind: "audio", direction: "recvonly" },
];
const peers = [];
let nonce = 0;
const spike = {
  async start() {
    for (let i = 0; i < sessions; i++) {
      const peer = new Peer(`s${i}`);
      const prepared = await peer.request(1, { servers: [], tracks });
      const answer = await far.answer(peer.id, prepared.sdp);
      await peer.request(2, answer);
      await peer.opened;
      peers.push(peer);
    }
    while (counts.video < 5) await new Promise((r) => setTimeout(r, 20));
  },
  ping() {
    for (const p of peers) p.send("control", probe(nonce++)).catch(() => {});
  },
  async interrupt() {
    // No foreign call is in flight on the media path, so interruption is a
    // JavaScript-only detach; drops while detached are counted here.
    const before = counts.video;
    const started = performance.now();
    consumerAttached = false;
    const interruptMs = performance.now() - started;
    setTimeout(() => (consumerAttached = true), 0);
    return { interruptMs: Math.round(interruptMs * 10) / 10, framesBefore: before };
  },
  async shutdown() {
    const snapshots = [];
    trace("snapshots");
    for (const p of peers) snapshots.push(await p.request(6));
    trace("snapshots done");
    const results = await Promise.allSettled(peers.map((p) => p.shutdown()));
    return {
      ok: results.every((r) => r.status === "fulfilled" && r.value === OK),
      errors: results.filter((r) => r.status === "rejected").map((r) => String(r.reason)),
      notifyCallbacks: counts.callbacks,
      nativeSnapshots: snapshots,
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
        detachedDrops: counts.detachedDrops,
        readerFailures: counts.readerFailures,
      },
    };
  },
};

const result = await schedule(spike, {
  seconds,
  blockMs: arg("--block-ms", 250),
  far,
  label: `B shrunk bridge (sessions=${sessions}${loss || delayMs ? `, loss=${loss}, delay=${delayMs}ms` : ""})`,
});
console.log(JSON.stringify(result, null, 2));
process.exit(0);
