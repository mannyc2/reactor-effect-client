// Smoke check H: @roamhq/wrtc (libwebrtc M106 W3C API) against the libwebrtc far
// peer, reading decoded frames through the nonstandard sinks.
//   node|bun js/smoke-h-wrtc.mjs [--seconds 15] [--metadata]
import wrtc from "@roamhq/wrtc";
import { arg, farPeer, probe, probeRtt, runtime, sleep } from "./harness.mjs";

const seconds = arg("--seconds", 15);
const metadata = process.argv.includes("--metadata");
const { RTCPeerConnection, nonstandard } = wrtc;
const far = farPeer(["--width", "1344", "--height", "768", "--fps", "24"]);
await far.ready;
const pc = new RTCPeerConnection({ iceServers: [] });
pc.addTransceiver("video", { direction: "recvonly" });
pc.addTransceiver("audio", { direction: "recvonly" });
const control = pc.createDataChannel("control");
pc.createDataChannel("data");
control.binaryType = "arraybuffer";
const counts = {
  frames: 0,
  sizes: new Set(),
  frameType: undefined,
  audio: 0,
  audioShape: undefined,
  rtt: [],
  i420Ms: 0,
};
const sinks = [];
pc.ontrack = ({ track }) => {
  if (track.kind === "video") {
    const sink = new nonstandard.RTCVideoSink(track);
    sink.onframe = ({ frame }) => {
      counts.frames++;
      counts.sizes.add(`${frame.width}x${frame.height}`);
      counts.frameType ??= `${frame.data.constructor.name}(${frame.data.byteLength})`;
      // I420 -> RGBA via the addon (there is no BGRA output); time it.
      if (counts.frames % 24 === 0) {
        const rgba = new Uint8ClampedArray(frame.width * frame.height * 4);
        const t = performance.now();
        nonstandard.i420ToRgba(frame, { width: frame.width, height: frame.height, data: rgba });
        counts.i420Ms = Math.round((performance.now() - t) * 100) / 100;
      }
    };
    sinks.push(sink);
  } else {
    const sink = new nonstandard.RTCAudioSink(track);
    sink.ondata = (d) => {
      counts.audio++;
      counts.audioShape ??= `${d.samples.constructor.name}(${d.samples.length}) ${d.sampleRate}Hz x${d.channelCount}`;
    };
    sinks.push(sink);
  }
};
control.onmessage = (e) => {
  const ms = probeRtt(new Uint8Array(e.data));
  if (ms !== undefined) counts.rtt.push(Math.round(ms * 10) / 10);
};
const offer = await pc.createOffer();
await pc.setLocalDescription(offer);
while (pc.iceGatheringState !== "complete") await sleep(20);
let sdp = pc.localDescription.sdp;
if (metadata) sdp = sdp.replace(/(\r\nm=)/, "\r\na=x-reactor-frame-metadata:1$1");
const answer = await far.answer("h0", sdp);
await pc.setRemoteDescription({ type: "answer", sdp: answer });
const opened = Date.now();
while (control.readyState !== "open" && Date.now() - opened < 15_000) await sleep(20);
for (let i = 0; i < seconds; i++) {
  if (control.readyState === "open") control.send(probe(i));
  await sleep(1000);
}
const farStats = await far.stats();
const v = farStats.sessions[0]?.outbound?.find((o) => /video/i.test(o.kind)) ?? {};
for (const s of sinks) s.stop();
pc.close();
await far.quit();
console.log(
  JSON.stringify(
    {
      label: `H @roamhq/wrtc smoke${metadata ? " (metadata trailer negotiated)" : ""}`,
      runtime,
      connection: pc.connectionState,
      seconds,
      decodedFrames: counts.frames,
      frameSizes: [...counts.sizes],
      frameType: counts.frameType,
      i420ToRgbaMsPerFrame: counts.i420Ms,
      audioCallbacks: counts.audio,
      audioShape: counts.audioShape,
      farFramesSent: v.framesSent,
      farTargetBitrate: v.targetBitrate,
      dataChannelRttMs: counts.rtt,
    },
    null,
    2,
  ),
);
process.exit(0);
