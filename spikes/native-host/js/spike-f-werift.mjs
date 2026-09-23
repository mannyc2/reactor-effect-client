// Spike F: werift (pure TypeScript ICE/DTLS/SCTP/SRTP/RTP) receiving from the
// libwebrtc far peer, with decoding in ffmpeg subprocesses. Reactor's frame
// metadata trailer is parsed here, on the encoded frame, before decode.
//
//   node|bun js/spike-f-werift.mjs [--seconds 60] [--jitter-ms 100] [--loss 0.02 --delay-ms 40]
import { spawn } from "node:child_process";
import { appendFileSync } from "node:fs";
import {
  RecvDelta,
  RtcpTransportLayerFeedback,
  RunLengthChunk,
  TransportWideCC,
  RTCPeerConnection,
  RTCRtpCodecParameters,
  useNACK,
  usePLI,
  useTransportWideCC,
  useTWCC,
} from "werift";
import { DepacketizeCallback, JitterBufferCallback, RtpSourceCallback } from "werift/nonstandard";
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
const jitterMs = arg("--jitter-ms", 100);
const fixTwcc = process.argv.includes("--fix-twcc");
// App-level loss handling werift leaves to the caller: wait for a keyframe,
// request one (PLI, at most every 300 ms) whenever the depacketizer needs it,
// and keep ffmpeg alive across undecodable frames.
const recover = process.argv.includes("--recover");
const loss = arg("--loss", 0);
const delayMs = arg("--delay-ms", 0);
const W = 1344,
  H = 768,
  FRAME = W * H * 4;
const minKbps = arg("--min-kbps", 300);
const farArgs = [
  "--width",
  String(W),
  "--height",
  String(H),
  "--fps",
  "24",
  "--min-kbps",
  String(minKbps),
];
if (loss > 0 || delayMs > 0) farArgs.push("--loss", String(loss), "--delay-ms", String(delayMs));
const far = farPeer(farArgs);
await far.ready;

const latency = new Distribution();
if (process.env.SPIKE_DEBUG)
  setInterval(
    () => appendFileSync(process.env.SPIKE_DEBUG, JSON.stringify(counts) + "\n"),
    2000,
  ).unref();
const rtt = new Distribution();
const counts = {
  video: 0,
  audio: 0,
  encodedFrames: 0,
  keyframes: 0,
  noTrailer: 0,
  badSize: 0,
  detachedDrops: 0,
  ffmpegBacklogMax: 0,
  readerFailures: [],
};
let consumerAttached = true;

// --- Reactor frame-metadata trailer: [payload][protobuf][u32 LE len]["RXMT"] ---
const varint = (bytes, offset) => {
  let value = 0n,
    shift = 0n,
    i = offset;
  for (;;) {
    const b = bytes[i++];
    value |= BigInt(b & 0x7f) << shift;
    if ((b & 0x80) === 0) return [value, i];
    shift += 7n;
  }
};
const stripTrailer = (data) => {
  const n = data.length;
  if (
    n < 8 ||
    data[n - 4] !== 0x52 ||
    data[n - 3] !== 0x58 ||
    data[n - 2] !== 0x4d ||
    data[n - 1] !== 0x54
  )
    return undefined;
  const protoLength = data.readUInt32LE(n - 8);
  const start = n - 8 - protoLength;
  if (start < 0) return undefined;
  const meta = { frameId: 0n, captureTimeUs: 0n, userData: undefined };
  for (let i = start; i < n - 8;) {
    const [key, next] = varint(data, i);
    i = next;
    const field = Number(key >> 3n),
      wire = Number(key & 7n);
    if (wire === 0) {
      const [v, after] = varint(data, i);
      i = after;
      if (field === 1) meta.frameId = v;
      if (field === 2) meta.captureTimeUs = v;
    } else if (wire === 2) {
      const [len, after] = varint(data, i);
      i = after + Number(len);
      if (field === 3) meta.userData = data.subarray(after, i);
    } else return undefined;
  }
  return { meta, payload: data.subarray(0, start) };
};

// --- ffmpeg video decoder: IVF in, raw BGRA out ---
const video = spawn(
  "ffmpeg",
  [
    "-hide_banner",
    "-loglevel",
    "error",
    "-probesize",
    "32",
    "-analyzeduration",
    "0",
    "-threads",
    "1",
    "-flags",
    "low_delay",
    "-f",
    "ivf",
    "-i",
    "pipe:0",
    "-fps_mode",
    "passthrough",
    ...(recover ? ["-max_error_rate", "1"] : []),
    "-f",
    "rawvideo",
    "-pix_fmt",
    "bgra",
    "pipe:1",
  ],
  { stdio: ["pipe", "pipe", "inherit"] },
);
const ivfHeader = Buffer.alloc(32);
ivfHeader.write("DKIF", 0);
ivfHeader.writeUInt16LE(0, 4);
ivfHeader.writeUInt16LE(32, 6);
ivfHeader.write("VP80", 8);
ivfHeader.writeUInt16LE(W, 12);
ivfHeader.writeUInt16LE(H, 14);
ivfHeader.writeUInt32LE(90000, 16);
ivfHeader.writeUInt32LE(1, 20);
video.stdin.write(ivfHeader);
const pendingMeta = [];
let ivfTimestamp = 0n;
const decode = (frame) => {
  const stripped = stripTrailer(frame.data);
  if (stripped === undefined) counts.noTrailer++;
  const payload = stripped?.payload ?? frame.data;
  const header = Buffer.alloc(12);
  header.writeUInt32LE(payload.length, 0);
  header.writeBigUInt64LE(ivfTimestamp++, 4);
  pendingMeta.push(stripped?.meta);
  counts.ffmpegBacklogMax = Math.max(counts.ffmpegBacklogMax, pendingMeta.length);
  video.stdin.write(Buffer.concat([header, payload]));
};
let staging = Buffer.allocUnsafe(FRAME),
  filled = 0;
video.stdout.on("data", (chunk) => {
  let offset = 0;
  while (offset < chunk.length) {
    const n = Math.min(FRAME - filled, chunk.length - offset);
    chunk.copy(staging, filled, offset, offset + n);
    filled += n;
    offset += n;
    if (filled === FRAME) {
      const meta = pendingMeta.shift();
      deliverVideo({ width: W, height: H, data: staging, meta });
      staging = Buffer.allocUnsafe(FRAME);
      filled = 0;
    }
  }
});
const deliverVideo = (frame) => {
  if (!consumerAttached) {
    counts.detachedDrops++;
    return;
  }
  counts.video++;
  const meta = frame.meta?.userData ? userData(frame.meta.userData) : undefined;
  if (meta !== undefined) latency.add((wallMicros() - meta.sentMicros) / 1000);
};

// --- ffmpeg audio decoder: a minimal Ogg Opus stream in, s16le out ---
const crcTable = new Uint32Array(256).map((_, i) => {
  let r = i << 24;
  for (let j = 0; j < 8; j++) r = r & 0x80000000 ? (r << 1) ^ 0x04c11db7 : r << 1;
  return r >>> 0;
});
const oggCrc = (bytes) => {
  let crc = 0;
  for (const b of bytes) crc = ((crc << 8) ^ crcTable[((crc >>> 24) & 0xff) ^ b]) >>> 0;
  return crc;
};
let oggSequence = 0,
  granule = 0n;
const oggPage = (packet, flags) => {
  const segments = [];
  let left = packet.length;
  while (left >= 255) {
    segments.push(255);
    left -= 255;
  }
  segments.push(left);
  const header = Buffer.alloc(27 + segments.length);
  header.write("OggS", 0);
  header[5] = flags;
  header.writeBigUInt64LE(granule, 6);
  header.writeUInt32LE(0x5eac7, 14);
  header.writeUInt32LE(oggSequence++, 18);
  header[26] = segments.length;
  segments.forEach((s, i) => (header[27 + i] = s));
  const page = Buffer.concat([header, packet]);
  page.writeUInt32LE(oggCrc(page), 22);
  return page;
};
const audio = spawn(
  "ffmpeg",
  [
    "-hide_banner",
    "-loglevel",
    "error",
    "-probesize",
    "32",
    "-analyzeduration",
    "0",
    "-f",
    "ogg",
    "-i",
    "pipe:0",
    "-f",
    "s16le",
    "-ac",
    "1",
    "-ar",
    "48000",
    "pipe:1",
  ],
  { stdio: ["pipe", "pipe", "inherit"] },
);
const opusHead = Buffer.alloc(19);
opusHead.write("OpusHead", 0);
opusHead[8] = 1;
opusHead[9] = 2;
opusHead.writeUInt16LE(312, 10);
opusHead.writeUInt32LE(48000, 12);
audio.stdin.write(oggPage(opusHead, 2));
const tags = Buffer.alloc(16);
tags.write("OpusTags", 0);
audio.stdin.write(oggPage(tags, 0));
audio.stdout.on("data", (chunk) => {
  counts.audio += chunk.length / 960; // 10 ms of mono s16 at 48 kHz
});

// --- werift offerer shaped like packages/native: two channels, recvonly A/V ---
const pc = new RTCPeerConnection({
  bundlePolicy: "max-bundle",
  codecs: {
    video: [
      new RTCRtpCodecParameters({
        mimeType: "video/VP8",
        clockRate: 90000,
        rtcpFeedback: [useNACK(), usePLI(), useTWCC()],
      }),
      new RTCRtpCodecParameters({ mimeType: "video/rtx", clockRate: 90000 }),
    ],
    audio: [new RTCRtpCodecParameters({ mimeType: "audio/opus", clockRate: 48000, channels: 2 })],
  },
  headerExtensions: { video: [useTransportWideCC()], audio: [] },
});
const control = pc.createDataChannel("control");
const data = pc.createDataChannel("data");
let open = 0,
  resolveOpen;
const opened = new Promise((r) => (resolveOpen = r));
for (const channel of [control, data])
  channel.stateChanged.subscribe((s) => s === "open" && ++open === 2 && resolveOpen());
control.onMessage.subscribe((message) => {
  const ms = probeRtt(new Uint8Array(message));
  if (ms !== undefined) rtt.add(ms);
});
const videoTransceiver = pc.addTransceiver("video", { direction: "recvonly" });
const audioTransceiver = pc.addTransceiver("audio", { direction: "recvonly" });

// werift 0.24.4 ReceiverTWCC (media/receiver/receiverTwcc.ts) measures the first
// delta of each feedback from the previous unquantized arrival while stating a
// 64 ms-quantized reference time, never reports lost packets as NotReceived,
// and does not unwrap the 16-bit transport sequence. --fix-twcc replaces it
// with a conforming encoder to show what a werift consumer would need upstream.
const patchTwcc = (twcc) => {
  let lastExt;
  twcc.handleTWCC = function (tsn) {
    const ext =
      lastExt === undefined
        ? tsn
        : lastExt + (((tsn - (lastExt & 0xffff) + 0x8000) & 0xffff) - 0x8000);
    lastExt = lastExt === undefined ? ext : Math.max(lastExt, ext);
    this.extensionInfo[ext] = { ext, timestamp: process.hrtime.bigint() / 1000n };
    if (Object.keys(this.extensionInfo).length > 20) this.sendTWCC();
  };
  twcc.sendTWCC = function () {
    const entries = Object.values(this.extensionInfo).sort((a, b) => a.ext - b.ext);
    if (entries.length === 0) return;
    const base = entries[0].ext,
      last = entries.at(-1).ext;
    const reference = entries[0].timestamp / 64000n;
    let previous = reference * 64000n;
    const byExt = new Map(entries.map((e) => [e.ext, e]));
    const statuses = [],
      deltas = [];
    for (let s = base; s <= last; s++) {
      const e = byExt.get(s);
      if (!e) {
        statuses.push(0);
        continue;
      }
      const delta = new RecvDelta({ delta: Number(e.timestamp - previous) });
      delta.parseDelta();
      previous += BigInt(delta.delta * 250);
      statuses.push(delta.type);
      deltas.push(delta);
    }
    const chunks = [];
    for (let i = 0; i < statuses.length;) {
      let j = i;
      while (j < statuses.length && statuses[j] === statuses[i] && j - i < 8191) j++;
      chunks.push(new RunLengthChunk({ packetStatus: statuses[i], runLength: j - i }));
      i = j;
    }
    const packet = new RtcpTransportLayerFeedback({
      feedback: new TransportWideCC({
        senderSsrc: this.rtcpSsrc,
        mediaSourceSsrc: this.mediaSourceSsrc,
        baseSequenceNumber: base & 0xffff,
        packetStatusCount: last - base + 1,
        referenceTime: Number(reference & 0xffffffn),
        fbPktCount: this.fbPktCount,
        recvDeltas: deltas,
        packetChunks: chunks,
      }),
    });
    this.dtlsTransport.sendRtcp([packet]).catch(() => {});
    this.extensionInfo = {};
    this.fbPktCount = (this.fbPktCount + 1) & 0xff;
  };
};
{
  const receiver = videoTransceiver.receiver;
  const setup = receiver.setupTWCC.bind(receiver);
  receiver.setupTWCC = (ssrc) => {
    setup(ssrc);
    if (fixTwcc && receiver.receiverTWCC && !receiver.receiverTWCC.patched) {
      patchTwcc(receiver.receiverTWCC);
      receiver.receiverTWCC.patched = true;
    }
  };
}
videoTransceiver.onTrack.subscribe((track) => {
  const source = new RtpSourceCallback();
  const jitter = new JitterBufferCallback(90000, { latency: jitterMs });
  const depacketizer = new DepacketizeCallback("vp8", {
    isFinalPacketInSequence: (h) => h.marker,
    waitForKeyframe: recover,
  });
  if (recover) {
    let lastPli = 0;
    depacketizer.onNeedKeyFrame.subscribe(() => {
      if (performance.now() - lastPli < 300) return;
      lastPli = performance.now();
      counts.plis = (counts.plis ?? 0) + 1;
      videoTransceiver.receiver.sendRtcpPLI(track.ssrc);
    });
  }
  track.onReceiveRtp.subscribe((rtp) => source.input(rtp.clone()));
  source.pipe(jitter.input);
  jitter.pipe(depacketizer.input);
  depacketizer.pipe((output) => {
    if (!output.frame) return;
    counts.encodedFrames++;
    if (output.frame.isKeyframe) counts.keyframes++;
    decode(output.frame);
  });
  // werift sends PLI only when asked; do so on start so the decoder gets a keyframe.
  setTimeout(() => videoTransceiver.receiver.sendRtcpPLI(track.ssrc), 200);
});
audioTransceiver.onTrack.subscribe((track) => {
  track.onReceiveRtp.subscribe((rtp) => {
    granule += 960n;
    audio.stdin.write(oggPage(rtp.payload, 0));
  });
});

let nonce = 0;
const spike = {
  async start() {
    const offer = await pc.createOffer();
    await pc.setLocalDescription(offer);
    // werift gathers inside setLocalDescription; the description carries candidates.
    const sdp = pc.localDescription.sdp.replace(/(\r\nm=)/, "\r\na=x-reactor-frame-metadata:1$1");
    const answer = await far.answer("s0", sdp);
    await pc.setRemoteDescription({ type: "answer", sdp: answer });
    await opened;
    while (counts.video < 5) await new Promise((r) => setTimeout(r, 20));
  },
  ping() {
    try {
      control.send(Buffer.from(probe(nonce++)));
    } catch {}
  },
  async interrupt() {
    const before = counts.video;
    consumerAttached = false;
    setTimeout(() => (consumerAttached = true), 0);
    return { interruptMs: 0, framesBefore: before };
  },
  async shutdown() {
    const receiver = videoTransceiver.receiver;
    const receiverStats = {
      nackRequestsSent: Object.values(receiver.nackCountBySsrc ?? {}).reduce((a, b) => a + b, 0),
      nackOutstanding: receiver.nack?.lostSeqNumbers?.length ?? null,
      twccEnabled: Boolean(receiver.twccEnabled),
      nackEnabled: Boolean(receiver.nackEnabled),
    };
    await pc.close();
    video.stdin.end();
    audio.stdin.end();
    const exited = (p) => new Promise((r) => (p.exitCode !== null ? r() : p.once("exit", r)));
    await Promise.race([
      Promise.all([exited(video), exited(audio)]),
      new Promise((r) => setTimeout(r, 5000)),
    ]);
    return {
      ok: true,
      ffmpegExited: video.exitCode !== null && audio.exitCode !== null,
      receiverStats,
    };
  },
  counters() {
    return {
      video: counts.video,
      audio: Math.round(counts.audio),
      latency,
      latencyCount: latency.values.length,
      rtt,
      drops: {
        encodedFrames: counts.encodedFrames,
        keyframes: counts.keyframes,
        noTrailer: counts.noTrailer,
        detachedDrops: counts.detachedDrops,
        ffmpegBacklogMax: counts.ffmpegBacklogMax,
        plisSent: counts.plis ?? 0,
        readerFailures: counts.readerFailures,
      },
    };
  },
};

const result = await schedule(spike, {
  seconds,
  blockMs: arg("--block-ms", 250),
  far,
  children: () => [video.pid, audio.pid],
  label: `F werift+ffmpeg (jitter=${jitterMs}ms${fixTwcc ? ", patched TWCC" : ""}${recover ? ", keyframe recovery" : ""}${minKbps !== 300 ? `, far min ${minKbps} kbps` : ""}${loss || delayMs ? `, loss=${loss}, delay=${delayMs}ms` : ""})`,
});
console.log(JSON.stringify(result, null, 2));
process.exit(0);
