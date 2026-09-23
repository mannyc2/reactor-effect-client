// Smoke check G: node-datachannel (libdatachannel N-API) as the offerer against
// the libwebrtc far peer. Media reaches JavaScript as raw RTP only.
//   node|bun js/smoke-g-node-datachannel.mjs [--seconds 15]
import nodeDataChannel from "node-datachannel";
import { arg, farPeer, probe, probeRtt, runtime, sleep } from "./harness.mjs";

const seconds = arg("--seconds", 15);
const far = farPeer(["--width", "1344", "--height", "768", "--fps", "24"]);
await far.ready;
const pc = new nodeDataChannel.PeerConnection("g", { iceServers: [] });
// libdatachannel negotiates as soon as a channel exists; observe gathering first.
let gatheringComplete = false;
const gathered = new Promise((r) =>
  pc.onGatheringStateChange((s) => s === "complete" && ((gatheringComplete = true), r())),
);
const counts = { videoRtp: 0, videoBytes: 0, audioRtp: 0, markers: 0, rtcpOrOther: 0, rtt: [] };
const video = new nodeDataChannel.Video("0", "RecvOnly");
video.addVP8Codec(97);
const videoTrack = pc.addTrack(video);
videoTrack.onMessage((m) => {
  const pt = m[1] & 0x7f;
  if (pt === 97) {
    counts.videoRtp++;
    counts.videoBytes += m.length;
    if (m[1] & 0x80) counts.markers++;
  } else counts.rtcpOrOther++;
});
const audio = new nodeDataChannel.Audio("1", "RecvOnly");
audio.addOpusCodec(111);
const audioTrack = pc.addTrack(audio);
audioTrack.onMessage(() => counts.audioRtp++);
const control = pc.createDataChannel("control");
pc.createDataChannel("data");
control.onMessage((m) => {
  const ms = probeRtt(new Uint8Array(m));
  if (ms !== undefined) counts.rtt.push(Math.round(ms * 10) / 10);
});
pc.setLocalDescription();
if (!gatheringComplete) await gathered;
const local = pc.localDescription();
const sdp = local.sdp.replace(/(\r\nm=)/, "\r\na=x-reactor-frame-metadata:1$1");
const answer = await far.answer("g0", sdp);
pc.setRemoteDescription(answer, "answer");
const opened = Date.now();
while (!control.isOpen() && Date.now() - opened < 15_000) await sleep(20);
const started = Date.now();
for (let i = 0; i < seconds; i++) {
  if (control.isOpen()) control.sendMessageBinary(Buffer.from(probe(i)));
  await sleep(1000);
}
const farStats = await far.stats();
const v = farStats.sessions[0]?.outbound?.find((o) => /video/i.test(o.kind)) ?? {};
pc.close();
await far.quit();
console.log(
  JSON.stringify(
    {
      label: "G node-datachannel smoke",
      runtime,
      channelOpenMs: started - opened,
      state: pc.state(),
      seconds,
      videoRtpPackets: counts.videoRtp,
      videoMarkerPackets: counts.markers,
      videoMbps: Math.round((counts.videoBytes * 8) / seconds / 1e4) / 100,
      audioRtpPackets: counts.audioRtp,
      farFramesSent: v.framesSent,
      farTargetBitrate: v.targetBitrate,
      farNackCount: v.nackCount,
      dataChannelRttMs: counts.rtt,
      note: "raw RTP only: no depacketizer, jitter buffer, NACK requester or decoder is reachable from JavaScript",
    },
    null,
    2,
  ),
);
process.exit(0);
