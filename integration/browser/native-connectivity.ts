import * as Effect from "effect/Effect";
import * as Crypto from "effect/Crypto";
import * as Stream from "effect/Stream";
import * as FetchHttpClient from "effect/unstable/http/FetchHttpClient";
import type * as PlatformHttp from "effect/unstable/http/HttpClient";
import { FetchHttp } from "reactor-effect-client";
import type { ReactorFailure } from "reactor-effect-client";
import * as Browser from "reactor-effect-browser";
import * as W from "reactor-effect-client/wire";
import { structFromObject, objectFromStruct } from "reactor-effect-client/wire";

type Mapping = {
  readonly name: string;
  readonly kind: "video" | "audio";
  readonly direction: "recvonly" | "sendonly";
  readonly mid: string;
};
type FixtureConfig = { readonly forceRelay: boolean; readonly iceServers: readonly RTCIceServer[] };
type NativeOffer = {
  readonly sdp: string;
  readonly mapping: readonly Mapping[];
  readonly fixture: FixtureConfig;
};

const delay = (ms: number): Promise<void> => new Promise((resolve) => setTimeout(resolve, ms));

const waitUntil = async (
  check: () => boolean | Promise<boolean>,
  timeoutMs: number,
  label: string,
): Promise<void> => {
  const end = performance.now() + timeoutMs;
  while (!(await check())) {
    if (performance.now() >= end) throw new Error(`${label}: deadline`);
    await delay(25);
  }
};

const assert: (condition: unknown, message: string) => asserts condition = (condition, message) => {
  if (!condition) throw new Error(message);
};

const waitIceGathering = async (peer: RTCPeerConnection, timeoutMs = 5000): Promise<void> => {
  if (peer.iceGatheringState === "complete") return;
  await new Promise<void>((resolve, reject) => {
    const timer = setTimeout(() => {
      cleanup();
      reject(new Error("browser ICE gathering deadline"));
    }, timeoutMs);
    const changed = (): void => {
      if (peer.iceGatheringState === "complete") {
        cleanup();
        resolve();
      }
    };
    const cleanup = (): void => {
      clearTimeout(timer);
      peer.removeEventListener("icegatheringstatechange", changed);
    };
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

/** A track's state now; a check made earlier in the run does not narrow it. */
const trackState = (track: MediaStreamTrack): MediaStreamTrackState => track.readyState;

/** Surfaces a failure the protocol fixture recorded while the harness waited. */
const rethrow = (failure: Error | undefined): void => {
  if (failure !== undefined) throw failure;
};

const selectedCandidateEvidence = async (
  peer: RTCPeerConnection,
): Promise<SelectedCandidateEvidence> => {
  const report = await peer.getStats(),
    entries = new Map<string, Record<string, unknown>>();
  report.forEach((value: Record<string, unknown>, id: string) => entries.set(id, value));
  let pair: Record<string, unknown> | undefined;
  for (const entry of entries.values()) {
    if (entry.type !== "transport" || typeof entry.selectedCandidatePairId !== "string") continue;
    pair = entries.get(entry.selectedCandidatePairId);
    if (pair !== undefined) break;
  }
  if (
    pair === undefined ||
    typeof pair.id !== "string" ||
    typeof pair.localCandidateId !== "string" ||
    typeof pair.remoteCandidateId !== "string"
  )
    throw new Error(
      "browser stats omitted transport.selectedCandidatePairId or its selected ICE candidate pair",
    );
  const local = entries.get(pair.localCandidateId),
    remote = entries.get(pair.remoteCandidateId);
  if (
    local === undefined ||
    remote === undefined ||
    typeof local.candidateType !== "string" ||
    typeof remote.candidateType !== "string"
  )
    throw new Error("browser stats omitted selected ICE candidate details");
  const optional = (value: unknown): string | undefined =>
    typeof value === "string" && value.length > 0 ? value : undefined;
  const localProtocol = optional(local.protocol),
    remoteProtocol = optional(remote.protocol);
  const localRelayProtocol = optional(local.relayProtocol),
    remoteRelayProtocol = optional(remote.relayProtocol);
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

/** The raw WebRTC peer is the unpaid provider fixture, never the SDK client. */
const providerChannels = (peer: RTCPeerConnection) => {
  const channels = new Map<string, RTCDataChannel>();
  const requests: { channel: string; type: string; requestId: string; bytes: number }[] = [];
  let failure: Error | undefined;
  let retired = false;
  peer.ondatachannel = ({ channel }) => {
    channel.binaryType = "arraybuffer";
    channels.set(channel.label, channel);
    channel.onerror = () => {
      if (!retired) failure ??= new Error(`provider ${channel.label} channel failed`);
    };
    channel.onmessage = (event) => {
      if (retired) return;
      try {
        assert(event.data instanceof ArrayBuffer, "provider received a nonbinary SCTP message");
        const bytes = new Uint8Array(event.data);
        if (channel.label === "control") {
          const request = W.ControlClientMessage.decode(bytes);
          if (request.kind !== 1 || request.payload === undefined) return;
          const type = request.payload.case;
          requests.push({
            channel: "control",
            type,
            requestId: request.request_id,
            bytes: bytes.byteLength,
          });
          if (type === "request_schema")
            channel.send(
              W.ControlServerMessage.encode({
                request_id: request.request_id,
                kind: 2,
                payload: {
                  case: "model_schema",
                  value: { openapi: structFromObject({ openapi: "3.1.0", paths: {} }) },
                },
              }),
            );
          else if (request.payload.case === "publish_track")
            channel.send(
              W.ControlServerMessage.encode({
                request_id: request.request_id,
                kind: 2,
                payload: { case: "publish_track", value: { name: request.payload.value.name } },
              }),
            );
        } else if (channel.label === "data") {
          const request = W.DataClientMessage.decode(bytes);
          assert(
            request.kind === 1 && request.payload?.case === "command",
            "provider received an invalid model command",
          );
          const command = request.payload.value;
          requests.push({
            channel: "data",
            type: command.type,
            requestId: request.request_id,
            bytes: bytes.byteLength,
          });
          channel.send(
            W.DataServerMessage.encode({
              request_id: request.request_id,
              kind: 2,
              ...(command.type === "ack"
                ? {}
                : {
                    payload: {
                      case: "message" as const,
                      value: {
                        type: command.type,
                        ...(command.data === undefined ? {} : { data: command.data }),
                      },
                    },
                  }),
            }),
          );
          if (command.type === "echo")
            assert(
              JSON.stringify(objectFromStruct(command.data!)) ===
                JSON.stringify({ bytes: [0xa2, 0x20, 0x21, 0x22] }),
              "provider command data changed in transit",
            );
        } else throw new Error("provider received an unknown data channel");
      } catch (cause) {
        failure ??= cause instanceof Error ? cause : new Error(String(cause));
      }
    };
  };
  return {
    channels,
    requests,
    get failure() {
      return failure;
    },
    retire: () => {
      retired = true;
      for (const channel of channels.values()) {
        channel.onmessage = null;
        channel.onerror = null;
      }
    },
  };
};

const browserCrypto = Crypto.make({
  randomBytes: (size) => crypto.getRandomValues(new Uint8Array(size)),
  digest: (algorithm, data) =>
    Effect.promise(
      async () => new Uint8Array(await crypto.subtle.digest(algorithm, Uint8Array.from(data))),
    ),
});
const runBrowser = <A, E extends ReactorFailure>(
  effect: Effect.Effect<A, E, PlatformHttp.HttpClient | Crypto.Crypto>,
  fetchImpl: typeof fetch,
): Promise<A> =>
  Effect.runPromise(
    effect.pipe(
      Effect.provide(FetchHttp.layer),
      Effect.provideService(FetchHttpClient.Fetch, fetchImpl),
      Effect.provideService(Crypto.Crypto, browserCrypto),
    ),
  );

const localBrowserPeerCheck = async (): Promise<object> => {
  const original = globalThis.RTCPeerConnection;
  const provider = new original();
  const protocol = providerChannels(provider);
  const source = await makeBrowserMedia();
  const clients: RTCPeerConnection[] = [];
  const pendingIce: RTCIceCandidateInit[] = [];
  let remoteReady = false,
    closed = false,
    failedClosed = false,
    deletes = 0,
    failedDeletes = 0;
  let answer: string | undefined;
  const tracks = [
    { name: "browser_video", kind: "video", direction: "recvonly" },
    { name: "browser_audio", kind: "audio", direction: "recvonly" },
    { name: "input_audio", kind: "audio", direction: "sendonly" },
  ];
  const descriptor = (failed: boolean) => ({
    session_id: failed ? "sess_browser_failure" : "sess_browser_local",
    state: (failed ? failedClosed : closed) ? "CLOSED" : "ACTIVE",
    capabilities: {
      protocol_version: "1.0",
      tracks,
      commands: [
        { name: "echo", schema: {} },
        { name: "ack", schema: {} },
      ],
    },
    selected_transport: { protocol: "webrtc", version: "1.0" },
  });
  const fetchImpl: typeof fetch = async (input, init) => {
    const request = new Request(input, init),
      path = new URL(request.url).pathname;
    const failed = path.startsWith("/failure");
    if (path.endsWith("/sessions") && request.method === "POST")
      return Response.json(descriptor(failed));
    if (/\/sessions\/sess_browser_(local|failure)$/.test(path)) {
      if (request.method === "DELETE") {
        if (failed) {
          failedClosed = true;
          failedDeletes++;
        } else {
          closed = true;
          deletes++;
        }
        return new Response(null, { status: 202 });
      }
      return Response.json(descriptor(failed));
    }
    if (path.endsWith("/ice_servers")) return Response.json({ ice_servers: [] });
    if (path.endsWith("/connections")) return Response.json({ connection_id: 1001 });
    if (path.endsWith("/ice_candidates")) {
      const body = (await request.json()) as {
        candidates: { candidate: string; sdp_mid?: string; sdp_mline_index?: number }[];
      };
      for (const candidate of body.candidates) {
        const value = {
          candidate: candidate.candidate,
          ...(candidate.sdp_mid === undefined ? {} : { sdpMid: candidate.sdp_mid }),
          ...(candidate.sdp_mline_index === undefined
            ? {}
            : { sdpMLineIndex: candidate.sdp_mline_index }),
        };
        if (!failed) {
          if (remoteReady) await provider.addIceCandidate(value);
          else pendingIce.push(value);
        }
      }
      return new Response(null, { status: 204 });
    }
    if (path.endsWith("/sdp_params")) {
      if (request.method === "GET")
        return Response.json({ sdp_answer: failed ? "fixture deliberately invalid SDP" : answer });
      if (!failed) {
        const body = (await request.json()) as { sdp_offer: string; track_mapping: Mapping[] };
        await provider.setRemoteDescription({ type: "offer", sdp: body.sdp_offer });
        remoteReady = true;
        for (const candidate of pendingIce.splice(0)) await provider.addIceCandidate(candidate);
        for (const mapping of body.track_mapping) {
          const transceiver = provider.getTransceivers().find((entry) => entry.mid === mapping.mid);
          assert(transceiver !== undefined, "browser fixture lost a negotiated transceiver");
          if (mapping.direction === "recvonly") {
            await transceiver.sender.replaceTrack(
              mapping.kind === "video" ? source.video : source.audio,
            );
            transceiver.direction = "sendonly";
          } else transceiver.direction = "recvonly";
        }
        await provider.setLocalDescription(await provider.createAnswer());
        await waitIceGathering(provider);
        answer = provider.localDescription!.sdp;
      }
      return new Response(null, { status: 204 });
    }
    return Response.json({ error: "unhandled browser fixture request" }, { status: 404 });
  };
  // Track real browser handles allocated by the public owner. The provider was
  // allocated above; subclassing retains Chrome's actual WebRTC implementation.
  globalThis.RTCPeerConnection = class extends original {
    constructor(configuration?: RTCConfiguration) {
      super(configuration);
      clients.push(this);
    }
  };
  try {
    const result = await runBrowser(
      Effect.scoped(
        Effect.gen(function* () {
          const factory = yield* Browser.make({
            apiUrl: "http://browser.fixture",
            session: { commandTimeoutMs: 5000 },
          });
          const session = yield* factory.createConnected({ model: "fixture/browser" });
          const ready = yield* session.ready;
          assert(ready.remote.ownership === "owned", "browser acquisition lost remote ownership");
          const media = yield* Browser.media(session);
          assert(
            media.generation === ready.generation && media.tracks.length === tracks.length,
            "browser media did not capture negotiated generation/tracks",
          );
          const lease = yield* media.track("browser_video");
          assert(
            lease instanceof MediaStreamTrack &&
              lease.kind === "video" &&
              lease.readyState === "live",
            "browser public media lease is not a live real track",
          );
          yield* media.publish("input_audio", source.audio);
          yield* media.setMaxBitrate("input_audio", 96_000);
          yield* media.unpublish("input_audio");
          assert(
            source.audio.readyState === "live",
            "browser publication stopped the borrowed source",
          );
          const events: unknown[] = [];
          yield* Effect.forkScoped(
            session.events().pipe(
              Stream.runForEach((event) =>
                Effect.sync(() => {
                  events.push(event);
                }),
              ),
            ),
          );
          yield* Effect.yieldNow;
          const schema = yield* session.schema;
          const ack = yield* session.command("ack", {});
          const reply = yield* session.command("echo", { bytes: [0xa2, 0x20, 0x21, 0x22] });
          yield* Effect.promise(() =>
            waitUntil(
              () => events.includes(ack) && events.includes(reply),
              1000,
              "browser model observation delivery",
            ),
          );
          assert(
            schema.openapi?.openapi === "3.1.0",
            "browser control channel lost schema response",
          );
          assert(
            ack.kind === "ack" && reply.kind === "message",
            "browser session collapsed ACK and model reply",
          );
          assert(
            events.includes(ack) && events.includes(reply),
            "browser command/event attribution diverged",
          );
          assert(
            reply.generation === ready.generation && ack.sequence < reply.sequence,
            "browser reply ordering/generation invalid",
          );
          assert(
            JSON.stringify(reply.kind === "message" ? reply.data : undefined) ===
              JSON.stringify({ bytes: [0xa2, 0x20, 0x21, 0x22] }),
            "browser echo payload changed",
          );
          rethrow(protocol.failure);
          protocol.retire();
          const report = yield* session.close;
          assert(
            report.localClosed && report.localErrors.length === 0 && report.remote.confirmed,
            "browser owned close did not complete and confirm termination",
          );
          assert(
            trackState(lease) === "ended",
            "browser session close did not retire its leased track",
          );
          const stale = yield* Effect.result(media.setTrackActive("browser_video", true));
          assert(
            stale._tag === "Failure",
            "retired browser media generation accepted a track operation",
          );
          assert(
            clients.length === 1 && clients[0]?.connectionState === "closed",
            "browser client retained its peer after close",
          );
          return {
            generation: String(ready.generation),
            ack: ack.kind,
            reply: reply.kind,
            sameAttributedObjects: true,
            media: {
              realTrackLease: true,
              publicationBorrowedSourcePreserved: true,
              leaseRetired: true,
              retiredOperationRejected: true,
            },
            close: report,
          };
        }),
      ),
      fetchImpl,
    );
    await runBrowser(
      Effect.scoped(
        Effect.gen(function* () {
          const factory = yield* Browser.make({ apiUrl: "http://browser.fixture/failure" });
          const failed = yield* Effect.result(
            factory.createConnected({ model: "fixture/browser-failure" }),
          );
          assert(failed._tag === "Failure", "invalid browser answer unexpectedly connected");
          assert(
            failedDeletes === 1 && failedClosed,
            "failed acquisition did not release owned remote lifetime before returning",
          );
          assert(
            clients.length === 2 && clients[1]?.connectionState === "closed",
            "failed acquisition retained its real browser peer",
          );
        }),
      ),
      fetchImpl,
    );
    assert(deletes === 1, "browser owned session terminated more than once");
    rethrow(protocol.failure);
    return {
      ...result,
      channels: [...protocol.channels.keys()].sort(),
      requests: protocol.requests,
      failureCleanup: { localClosed: true, remoteDeleteCount: failedDeletes },
      realPeerHandlesClosed: clients.length,
    };
  } finally {
    globalThis.RTCPeerConnection = original;
    for (const client of clients) if (client.connectionState !== "closed") client.close();
    provider.close();
    await source.close();
  }
};

interface BrowserMedia {
  readonly video: MediaStreamTrack;
  readonly audio: MediaStreamTrack;
  readonly close: () => Promise<void>;
}

const makeBrowserMedia = async (): Promise<BrowserMedia> => {
  const canvas = document.createElement("canvas");
  canvas.width = 160;
  canvas.height = 96;
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
  oscillator.connect(gain);
  gain.connect(destination);
  oscillator.start();
  const audio = destination.stream.getAudioTracks()[0];
  if (audio === undefined) throw new Error("Web Audio destination produced no audio track");

  return {
    video,
    audio,
    close: async () => {
      clearInterval(timer);
      video.stop();
      audio.stop();
      oscillator.stop();
      oscillator.disconnect();
      gain.disconnect();
      destination.disconnect();
      await audioContext.close();
    },
  };
};

const browserNativeCheck = async (): Promise<{
  readonly report: object;
  readonly close: () => Promise<void>;
}> => {
  let offer: NativeOffer | undefined;
  await waitUntil(
    async () => {
      const response = await fetch("/native-offer", { cache: "no-store" });
      if (response.status === 202) return false;
      if (!response.ok) throw new Error(`native offer HTTP ${response.status}`);
      offer = (await response.json()) as NativeOffer;
      return true;
    },
    20_000,
    "native public session offer",
  );
  assert(offer !== undefined, "native session omitted offer");
  const peer = new RTCPeerConnection({
    iceServers: [...offer.fixture.iceServers],
    ...(offer.fixture.forceRelay ? { iceTransportPolicy: "relay" as RTCIceTransportPolicy } : {}),
  });
  const media = await makeBrowserMedia();
  const protocol = providerChannels(peer);
  const channels = protocol.channels;
  let iceCursor = 0;
  let iceComplete = false;
  let stopping = false;
  let icePump: Promise<void> | undefined;
  const close = async (): Promise<void> => {
    stopping = true;
    protocol.retire();
    await icePump?.catch(() => undefined);
    peer.close();
    await media.close();
  };

  try {
    await peer.setRemoteDescription({ type: "offer", sdp: offer.sdp });
    for (const mapping of offer.mapping.filter((entry) => entry.direction === "recvonly")) {
      const transceiver = peer.getTransceivers().find((entry) => entry.mid === mapping.mid);
      if (transceiver === undefined)
        throw new Error(`browser answerer has no transceiver for native MID ${mapping.mid}`);
      const track = mapping.kind === "video" ? media.video : media.audio;
      await transceiver.sender.replaceTrack(track);
      transceiver.direction = "sendonly";
    }

    icePump = (async () => {
      const deadline = performance.now() + 15_000;
      while (!iceComplete && !stopping) {
        if (performance.now() > deadline) throw new Error("native candidate forwarding deadline");
        const response = await fetch(`/native-ice?cursor=${iceCursor}`, { cache: "no-store" });
        if (!response.ok) throw new Error(`native ICE HTTP ${response.status}`);
        const value = (await response.json()) as {
          readonly candidates: readonly RTCIceCandidateInit[];
          readonly complete: boolean;
        };
        for (const candidate of value.candidates) {
          await peer.addIceCandidate(candidate);
          iceCursor++;
        }
        iceComplete = value.complete;
        if (!iceComplete) await delay(25);
      }
    })();

    const answer = await peer.createAnswer();
    await peer.setLocalDescription(answer);
    await waitIceGathering(peer);
    if (peer.localDescription?.sdp === undefined)
      throw new Error("browser/native answer has no gathered SDP");
    const answerResponse = await fetch("/native-answer", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ sdp: peer.localDescription.sdp }),
    });
    if (!answerResponse.ok)
      throw new Error(
        `native answer HTTP ${answerResponse.status}: ${await answerResponse.text()}`,
      );
    await icePump;

    await waitUntil(
      () =>
        protocol.failure !== undefined ||
        (peer.connectionState === "connected" &&
          channels.size === 2 &&
          [...channels.values()].every((channel) => channel.readyState === "open")),
      10000,
      "browser/native WebRTC connection and channels",
    );
    rethrow(protocol.failure);
    await waitUntil(
      () =>
        protocol.failure !== undefined ||
        (protocol.requests.some(
          (request) => request.channel === "control" && request.type === "request_schema",
        ) &&
          protocol.requests.filter((request) => request.channel === "data").length >= 2),
      7000,
      "native public session control/model exchanges",
    );
    rethrow(protocol.failure);
    const modelRequests = protocol.requests.filter((request) => request.channel === "data");
    assert(
      modelRequests[0]?.type === "ack" && modelRequests[1]?.type === "echo",
      "model commands lost ordered channel delivery",
    );
    assert(
      new Set(modelRequests.map((request) => request.requestId)).size === 2,
      "model requests lost unique correlation identities",
    );
    for (const channel of channels.values())
      assert(channel.ordered, "native negotiated an unordered channel");

    const relay = offer.fixture.forceRelay ? await selectedCandidateEvidence(peer) : undefined;
    if (
      relay !== undefined &&
      (relay.localCandidateType !== "relay" || relay.remoteCandidateType !== "relay")
    )
      throw new Error(
        `forced TURN selected non-relay browser pair: local=${relay.localCandidateType} remote=${relay.remoteCandidateType}`,
      );

    return {
      report: {
        connectionState: peer.connectionState,
        channels: [...channels.keys()].sort(),
        requests: protocol.requests,
        replies: { control: "schema", data: ["ack", "echo"] },
        orderedBinaryChannels: true,
        relay: relay === undefined ? { forced: false } : { forced: true, selected: relay },
        source: {
          video: { width: 160, height: 96, framesPerSecond: 20 },
          audio: { sampleRate: 48_000, frequencyHz: 440 },
        },
      },
      close,
    };
  } catch (error) {
    await close();
    throw error;
  }
};

const post = async (path: string, body: unknown): Promise<void> => {
  const response = await fetch(path, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  if (!response.ok) throw new Error(`${path} HTTP ${response.status}`);
};

const main = async (): Promise<void> => {
  let native: Awaited<ReturnType<typeof browserNativeCheck>> | undefined;
  try {
    if (
      typeof Browser.make !== "function" ||
      typeof Browser.media !== "function" ||
      Browser.layer === undefined
    )
      throw new Error("bundled browser public entry did not load its host surface");
    const localPeer = await localBrowserPeerCheck();
    native = await browserNativeCheck();
    await post("/browser-report", {
      ok: true,
      browserEntry: { make: "function", media: "function", layer: "present" },
      localPeer,
      native: native.report,
    });
    await waitUntil(
      async () => {
        const response = await fetch("/finish", { cache: "no-store" });
        return response.ok && ((await response.json()) as { finish?: boolean }).finish === true;
      },
      15000,
      "browser/native runner finish signal",
    );
  } catch (error) {
    await post("/browser-report", {
      ok: false,
      error: error instanceof Error ? `${error.message}\n${error.stack ?? ""}` : String(error),
    }).catch(() => undefined);
  } finally {
    await native?.close().catch(() => undefined);
    await post("/browser-closed", { closed: true }).catch(() => undefined);
  }
};

void main();
