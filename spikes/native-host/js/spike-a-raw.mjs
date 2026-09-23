// Spike A-raw: today's NativeBridge (async probe+copy polls on Koffi workers)
// and today's packet parsers, without Effect streams or Observations, so the
// FFI/copy cost can be separated from the Effect delivery cost of spike A.
//   node|bun js/spike-a-raw.mjs [--seconds 60]
import {
  NativeBridge,
  NativeCall,
  encodeNativeJson,
  encodeNativeText,
  resolveNativeBridge,
} from "../../../packages/native/dist/_internal/bridge.js";
import { nativePeerTesting } from "../../../packages/native/dist/_internal/peer.js";
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

const seconds = arg("--seconds", 60);
const far = farPeer(["--width", "1344", "--height", "768", "--fps", "24"]);
await far.ready;
const bridge = new NativeBridge(await resolveNativeBridge());
const latency = new Distribution();
const rtt = new Distribution();
const counts = { video: 0, audio: 0, detachedDrops: 0, readerFailures: [] };
let attached = true;
let resolveOpen,
  channels = 0;
const opened = new Promise((r) => (resolveOpen = r));
const inFlight = new Distribution();
setInterval(() => inFlight.add(bridge.active.size), 10).unref();

const pump = async (poll, handle) => {
  for (;;) {
    let result;
    try {
      result = await poll();
    } catch (e) {
      counts.readerFailures.push(String(e));
      return;
    }
    if (result._tag === "Closed") return;
    if (result._tag === "Packet") handle(result.packet);
  }
};
const tracks = [
  { name: "main_video", kind: "video", direction: "recvonly" },
  { name: "main_audio", kind: "audio", direction: "recvonly" },
];
let nonce = 0;
const spike = {
  async start() {
    const prepared = await bridge.call(
      NativeCall.Prepare,
      encodeNativeJson({ servers: [], tracks }),
    );
    void pump(
      () => bridge.pollEvent(),
      (packet) => {
        const event = nativePeerTesting.parseEvent(packet);
        if (event.type === "ice" && event.candidate) far.candidate("s0", event.candidate);
        else if (event.type === "channel" && event.open && ++channels === 2) resolveOpen();
        else if (event.type === "message" && event.channel === "control") {
          const ms = probeRtt(event.bytes);
          if (ms !== undefined) rtt.add(ms);
        }
      },
    );
    void pump(
      () => bridge.pollVideo(),
      (packet) => {
        const frame = nativePeerTesting.parseVideo(packet);
        if (!attached) return void counts.detachedDrops++;
        counts.video++;
        const meta = userData(frame.metadata);
        if (meta !== undefined) latency.add((wallMicros() - meta.sentMicros) / 1000);
      },
    );
    void pump(
      () => bridge.pollAudio(),
      (packet) => {
        nativePeerTesting.parseAudio(packet);
        counts.audio++;
      },
    );
    await bridge.call(NativeCall.Answer, encodeNativeText(await far.answer("s0", prepared.sdp)));
    await opened;
    while (counts.video < 5) await new Promise((r) => setTimeout(r, 20));
  },
  ping() {
    bridge.send("control", probe(nonce++)).catch(() => {});
  },
  async interrupt() {
    const before = counts.video;
    attached = false;
    setTimeout(() => (attached = true), 0);
    return { interruptMs: 0, framesBefore: before };
  },
  async shutdown() {
    const snapshot = await bridge.call(NativeCall.MediaSnapshot);
    await bridge.shutdown();
    return { ok: true, nativeSnapshots: [snapshot] };
  },
  counters() {
    return {
      video: counts.video,
      audio: counts.audio,
      latency,
      latencyCount: latency.values.length,
      rtt,
      drops: {
        detachedDrops: counts.detachedDrops,
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
  label: "A-raw bridge without Effect delivery",
});
console.log(JSON.stringify(result, null, 2));
process.exit(0);
