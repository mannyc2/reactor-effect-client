import * as Effect from "effect/Effect";
import { BrowserPeer } from "../../src/peer.js";
import * as Browser from "../../src/browser.js";

type Mapping = { readonly name: string; readonly kind: "video" | "audio"; readonly direction: "recvonly" | "sendonly"; readonly mid: string };
type FixtureConfig = { readonly forceRelay: boolean; readonly iceServers: readonly RTCIceServer[] };
type NativeOffer = { readonly sdp: string; readonly mapping: readonly Mapping[]; readonly fixture: FixtureConfig };

const delay = (ms: number): Promise<void> => new Promise((resolve) => setTimeout(resolve, ms));

const waitUntil = async (check: () => boolean | Promise<boolean>, timeoutMs: number, label: string): Promise<void> => {
  const end = performance.now() + timeoutMs;
  while (!await check()) {
    if (performance.now() >= end) throw new Error(`${label}: deadline`);
    await delay(25);
  }
};

const bytes = (value: ArrayBuffer): readonly number[] => [...new Uint8Array(value)];
const equalBytes = (actual: readonly number[] | undefined, expected: readonly number[]): boolean =>
  actual !== undefined && actual.length === expected.length && actual.every((value, index) => value === expected[index]);

const waitIceGathering = async (peer: RTCPeerConnection, timeoutMs = 5000): Promise<void> => {
  if (peer.iceGatheringState === "complete") return;
  await new Promise<void>((resolve, reject) => {
    const timer = setTimeout(() => { cleanup(); reject(new Error("browser ICE gathering deadline")); }, timeoutMs);
    const changed = (): void => { if (peer.iceGatheringState === "complete") { cleanup(); resolve(); } };
    const cleanup = (): void => { clearTimeout(timer); peer.removeEventListener("icegatheringstatechange", changed); };
    peer.addEventListener("icegatheringstatechange", changed);
  });
};

interface SelectedCandidateEvidence {
  readonly pairId: string;
  readonly localCandidateId: string;
  readonly remoteCandidateId: string;
  readonly localCandidateType: string;
  readonly remoteCandidateType: string;
  readonly localProtocol?: string;
  readonly remoteProtocol?: string;
  readonly localRelayProtocol?: string;
  readonly remoteRelayProtocol?: string;
}

const selectedCandidateEvidence = async (peer: RTCPeerConnection): Promise<SelectedCandidateEvidence> => {
  const report = await peer.getStats(), entries = new Map<string, Record<string, unknown>>();
  report.forEach((value) => entries.set(value.id, value as unknown as Record<string, unknown>));
  let pair: Record<string, unknown> | undefined;
  for (const entry of entries.values()) {
    if (entry.type !== "transport" || typeof entry.selectedCandidatePairId !== "string") continue;
    pair = entries.get(entry.selectedCandidatePairId);
    if (pair !== undefined) break;
  }
  if (pair === undefined || typeof pair.id !== "string" || typeof pair.localCandidateId !== "string" || typeof pair.remoteCandidateId !== "string")
    throw new Error("browser stats omitted transport.selectedCandidatePairId or its selected ICE candidate pair");
  const local = entries.get(pair.localCandidateId), remote = entries.get(pair.remoteCandidateId);
  if (local === undefined || remote === undefined || typeof local.candidateType !== "string" || typeof remote.candidateType !== "string")
    throw new Error("browser stats omitted selected ICE candidate details");
  const optional = (value: unknown): string | undefined => typeof value === "string" && value.length > 0 ? value : undefined;
  const localProtocol = optional(local.protocol), remoteProtocol = optional(remote.protocol);
  const localRelayProtocol = optional(local.relayProtocol), remoteRelayProtocol = optional(remote.relayProtocol);
  return {
    pairId: pair.id,
    localCandidateId: pair.localCandidateId,
    remoteCandidateId: pair.remoteCandidateId,
    localCandidateType: local.candidateType,
    remoteCandidateType: remote.candidateType,
    ...(localProtocol === undefined ? {} : { localProtocol }),
    ...(remoteProtocol === undefined ? {} : { remoteProtocol }),
    ...(localRelayProtocol === undefined ? {} : { localRelayProtocol }),
    ...(remoteRelayProtocol === undefined ? {} : { remoteRelayProtocol }),
  };
};

const localBrowserPeerCheck = async (): Promise<object> => {
  const raw = new RTCPeerConnection();
  const peer = new BrowserPeer();
  const pendingIce: RTCIceCandidateInit[] = [];
  const rawChannels = new Map<string, RTCDataChannel>();
  const fromPeer = new Map<string, readonly number[]>();
  const fromRaw = new Map<string, readonly number[]>();
  const opened = new Set<string>();
  let rawReady = false;
  let peerFailure: Error | undefined;

  const addPeerCandidate = (candidate: RTCIceCandidateInit): void => {
    if (!rawReady) { pendingIce.push(candidate); return; }
    void raw.addIceCandidate(candidate).catch((error) => { peerFailure ??= error instanceof Error ? error : new Error(String(error)); });
  };

  raw.ondatachannel = (event) => {
    const channel = event.channel;
    channel.binaryType = "arraybuffer";
    rawChannels.set(channel.label, channel);
    channel.onmessage = (message) => {
      if (!(message.data instanceof ArrayBuffer)) { peerFailure ??= new Error(`local ${channel.label} received nonbinary payload`); return; }
      fromPeer.set(channel.label, bytes(message.data));
    };
  };

  try {
    const prepared = await Effect.runPromise(peer.prepare([], [], (event) => {
      if (event.type === "ice" && event.candidate !== undefined) addPeerCandidate({
        candidate: event.candidate.candidate,
        ...(event.candidate.sdp_mid === undefined ? {} : { sdpMid: event.candidate.sdp_mid }),
        ...(event.candidate.sdp_mline_index === undefined ? {} : { sdpMLineIndex: event.candidate.sdp_mline_index }),
      });
      if (event.type === "channel" && event.open) opened.add(event.channel);
      if (event.type === "message") fromRaw.set(event.channel, [...event.bytes]);
      if (event.type === "error") peerFailure ??= event.error;
    }));

    await raw.setRemoteDescription({ type: "offer", sdp: prepared.sdp });
    rawReady = true;
    for (const candidate of pendingIce.splice(0)) await raw.addIceCandidate(candidate);
    const answer = await raw.createAnswer();
    await raw.setLocalDescription(answer);
    await waitIceGathering(raw);
    if (raw.localDescription?.sdp === undefined) throw new Error("local browser peer produced no gathered answer SDP");
    await Effect.runPromise(peer.answer(raw.localDescription.sdp));

    await waitUntil(() => peerFailure !== undefined || (raw.connectionState === "connected" && rawChannels.size === 2 && opened.size === 2), 8000,
      "real BrowserPeer local data channels");
    if (peerFailure !== undefined) throw peerFailure;

    const expectedPeer = { control: [0x31, 0x32, 0x33], data: [0x41, 0x42, 0x43, 0x44] } as const;
    const expectedRaw = { control: [0x51, 0x52], data: [0x61, 0x62, 0x63] } as const;
    await Effect.runPromise(peer.send("control", Uint8Array.from(expectedPeer.control)));
    await Effect.runPromise(peer.send("data", Uint8Array.from(expectedPeer.data)));
    rawChannels.get("control")?.send(Uint8Array.from(expectedRaw.control));
    rawChannels.get("data")?.send(Uint8Array.from(expectedRaw.data));
    await waitUntil(() => peerFailure !== undefined || (
      equalBytes(fromPeer.get("control"), expectedPeer.control) && equalBytes(fromPeer.get("data"), expectedPeer.data) &&
      equalBytes(fromRaw.get("control"), expectedRaw.control) && equalBytes(fromRaw.get("data"), expectedRaw.data)
    ), 5000, "real BrowserPeer bidirectional binary messages");
    if (peerFailure !== undefined) throw peerFailure;

    return {
      connectionState: raw.connectionState,
      channels: [...rawChannels.keys()].sort(),
      peerToRaw: Object.fromEntries(fromPeer), rawToPeer: Object.fromEntries(fromRaw),
    };
  } finally {
    peer.close();
    raw.close();
  }
};

interface BrowserMedia {
  readonly video: MediaStreamTrack;
  readonly audio: MediaStreamTrack;
  readonly close: () => Promise<void>;
}

const makeBrowserMedia = async (): Promise<BrowserMedia> => {
  const canvas = document.createElement("canvas");
  canvas.width = 160; canvas.height = 96;
  const context = canvas.getContext("2d");
  if (context === null) throw new Error("Canvas 2D unavailable for browser/native media source");
  let frame = 0;
  const timer = setInterval(() => {
    frame++;
    context.fillStyle = `rgb(${(frame * 29) % 255},${(frame * 47) % 255},${(frame * 71) % 255})`;
    context.fillRect(0, 0, canvas.width, canvas.height);
    context.fillStyle = "white";
    context.fillRect((frame * 7) % 140, 18, 20, 24);
  }, 50);
  const videoStream = canvas.captureStream(20);
  const video = videoStream.getVideoTracks()[0];
  if (video === undefined) throw new Error("canvas.captureStream produced no video track");

  const audioContext = new AudioContext({ sampleRate: 48_000 });
  await audioContext.resume();
  const oscillator = audioContext.createOscillator();
  const gain = audioContext.createGain();
  const destination = audioContext.createMediaStreamDestination();
  oscillator.frequency.value = 440;
  gain.gain.value = 0.2;
  oscillator.connect(gain); gain.connect(destination); oscillator.start();
  const audio = destination.stream.getAudioTracks()[0];
  if (audio === undefined) throw new Error("Web Audio destination produced no audio track");

  return {
    video, audio,
    close: async () => {
      clearInterval(timer);
      video.stop(); audio.stop(); oscillator.stop(); oscillator.disconnect(); gain.disconnect(); destination.disconnect();
      await audioContext.close();
    },
  };
};

const browserNativeCheck = async (): Promise<{ readonly report: object; readonly close: () => Promise<void> }> => {
  const offerResponse = await fetch("/native-offer", { cache: "no-store" });
  if (!offerResponse.ok) throw new Error(`native offer HTTP ${offerResponse.status}`);
  const offer = await offerResponse.json() as NativeOffer;
  const peer = new RTCPeerConnection({
    iceServers: [...offer.fixture.iceServers],
    ...(offer.fixture.forceRelay ? { iceTransportPolicy: "relay" as RTCIceTransportPolicy } : {}),
  });
  const media = await makeBrowserMedia();
  const channels = new Map<string, RTCDataChannel>();
  const fromNative = new Map<string, readonly number[]>();
  let channelFailure: Error | undefined;
  let iceCursor = 0;
  let iceComplete = false;

  const close = async (): Promise<void> => { peer.close(); await media.close(); };
  peer.ondatachannel = (event) => {
    const channel = event.channel;
    channel.binaryType = "arraybuffer";
    channel.onmessage = (message) => {
      if (!(message.data instanceof ArrayBuffer)) { channelFailure ??= new Error(`native ${channel.label} delivered nonbinary browser payload`); return; }
      fromNative.set(channel.label, bytes(message.data));
    };
    channel.onerror = () => { channelFailure ??= new Error(`native ${channel.label} browser data channel error`); };
    channels.set(channel.label, channel);
  };

  try {
    await peer.setRemoteDescription({ type: "offer", sdp: offer.sdp });
    for (const mapping of offer.mapping.filter((entry) => entry.direction === "recvonly")) {
      const transceiver = peer.getTransceivers().find((entry) => entry.mid === mapping.mid);
      if (transceiver === undefined) throw new Error(`browser answerer has no transceiver for native MID ${mapping.mid}`);
      const track = mapping.kind === "video" ? media.video : media.audio;
      await transceiver.sender.replaceTrack(track);
      transceiver.direction = "sendonly";
    }

    const icePump = (async () => {
      while (!iceComplete) {
        const response = await fetch(`/native-ice?cursor=${iceCursor}`, { cache: "no-store" });
        if (!response.ok) throw new Error(`native ICE HTTP ${response.status}`);
        const value = await response.json() as { readonly candidates: readonly RTCIceCandidateInit[]; readonly complete: boolean };
        for (const candidate of value.candidates) { await peer.addIceCandidate(candidate); iceCursor++; }
        iceComplete = value.complete;
        if (!iceComplete) await delay(25);
      }
    })();

    const answer = await peer.createAnswer();
    await peer.setLocalDescription(answer);
    await waitIceGathering(peer);
    if (peer.localDescription?.sdp === undefined) throw new Error("browser/native answer has no gathered SDP");
    const answerResponse = await fetch("/native-answer", { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ sdp: peer.localDescription.sdp }) });
    if (!answerResponse.ok) throw new Error(`native answer HTTP ${answerResponse.status}: ${await answerResponse.text()}`);
    await icePump;

    await waitUntil(() => channelFailure !== undefined || (peer.connectionState === "connected" && channels.size === 2 && [...channels.values()].every((channel) => channel.readyState === "open")),
      10000, "browser/native WebRTC connection and channels");
    if (channelFailure !== undefined) throw channelFailure;

    const browserToNative = { control: [0xb1, 0x01, 0x02], data: [0xb2, 0x03, 0x04, 0x05] } as const;
    channels.get("control")?.send(Uint8Array.from(browserToNative.control));
    channels.get("data")?.send(Uint8Array.from(browserToNative.data));
    const nativeToBrowser = { control: [0xa1, 0x10, 0x11], data: [0xa2, 0x20, 0x21, 0x22] } as const;
    await waitUntil(() => channelFailure !== undefined || (
      equalBytes(fromNative.get("control"), nativeToBrowser.control) && equalBytes(fromNative.get("data"), nativeToBrowser.data)
    ), 7000, "native-to-browser binary channel delivery");
    if (channelFailure !== undefined) throw channelFailure;

    const relay = offer.fixture.forceRelay ? await selectedCandidateEvidence(peer) : undefined;
    if (relay !== undefined && (relay.localCandidateType !== "relay" || relay.remoteCandidateType !== "relay"))
      throw new Error(`forced TURN selected non-relay browser pair: local=${relay.localCandidateType} remote=${relay.remoteCandidateType}`);

    return {
      report: {
        connectionState: peer.connectionState,
        channels: [...channels.keys()].sort(),
        nativeToBrowser: Object.fromEntries(fromNative), browserToNative,
        relay: relay === undefined ? { forced: false } : { forced: true, selected: relay },
        source: { video: { width: 160, height: 96, framesPerSecond: 20 }, audio: { sampleRate: 48_000, frequencyHz: 440 } },
      },
      close,
    };
  } catch (error) {
    await close();
    throw error;
  }
};

const post = async (path: string, body: unknown): Promise<void> => {
  const response = await fetch(path, { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(body) });
  if (!response.ok) throw new Error(`${path} HTTP ${response.status}`);
};

const main = async (): Promise<void> => {
  let native: Awaited<ReturnType<typeof browserNativeCheck>> | undefined;
  try {
    if (typeof Browser.connect !== "function" || Browser.layer === undefined) throw new Error("bundled browser public entry did not load its host surface");
    const localPeer = await localBrowserPeerCheck();
    native = await browserNativeCheck();
    await post("/browser-report", { ok: true, browserEntry: { connect: "function", layer: "present" }, localPeer, native: native.report });
    await waitUntil(async () => {
      const response = await fetch("/finish", { cache: "no-store" });
      return response.ok && (await response.json() as { finish?: boolean }).finish === true;
    }, 15000, "browser/native runner finish signal");
  } catch (error) {
    await post("/browser-report", { ok: false, error: error instanceof Error ? `${error.message}\n${error.stack ?? ""}` : String(error) }).catch(() => undefined);
  } finally {
    await native?.close().catch(() => undefined);
    await post("/browser-closed", { closed: true }).catch(() => undefined);
  }
};

void main();
