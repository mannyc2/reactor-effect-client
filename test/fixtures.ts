import * as Effect from "effect/Effect";
import * as PlatformHttp from "effect/unstable/http/HttpClient";
import * as FetchHttpClient from "effect/unstable/http/FetchHttpClient";
import * as FetchHttp from "../src/FetchHttp.js";
import {
  CoordinatorClient as ProtocolHttpClient,
  type HttpOptions,
} from "../src/coordinator/_internal/client.js";

/** Concrete HTTP selection belongs to the test composition, not the SDK. */
export const httpForTests = (): PlatformHttp.HttpClient =>
  Effect.runSync(
    PlatformHttp.HttpClient.pipe(
      Effect.provide(FetchHttp.layer),
      Effect.provideService(FetchHttpClient.Fetch, globalThis.fetch),
    ),
  );
export class TestHttpClient extends ProtocolHttpClient {
  constructor(options: HttpOptions) {
    super(options, httpForTests());
  }
}
import { Session } from "../src/session.js";
import type { SessionOptions } from "../src/session.js";
import { ReactorError } from "../src/errors.js";
import { structFromObject } from "../src/json.js";
import type { Peer, PeerEvent, Prepared, Channel } from "../src/PeerTypes.js";
import type { Track } from "../src/contract.js";
import * as W from "../src/wire.generated.js";
export interface Call {
  readonly method: string;
  readonly url: URL;
  readonly headers: Headers;
  readonly body: string;
  readonly signal: AbortSignal;
}
export const jsonResponse = (value: unknown, status = 200): Response =>
  new Response(JSON.stringify(value), { status, headers: { "content-type": "application/json" } });
export class HttpFixture {
  readonly calls: Call[] = [];
  readonly order: string[] = [];
  closed = false;
  terminateConfirms = true;
  createSlim = false;
  readsUntilReady = 0;
  sdpPollsBeforeReady = 0;
  state = "ACTIVE";
  sessionId = "sess_fixture";
  hook: ((call: Call) => Response | undefined | Promise<Response | undefined>) | undefined;
  tracks: readonly Track[] = [
    { name: "main_video", kind: "video", direction: "recvonly" },
    { name: "main_audio", kind: "audio", direction: "recvonly" },
    { name: "input_audio", kind: "audio", direction: "sendonly" },
  ];
  get descriptor(): unknown {
    return {
      session_id: this.sessionId,
      state: this.closed ? "CLOSED" : this.state,
      capabilities: {
        protocol_version: "1.0",
        tracks: this.tracks,
        commands: [{ name: "echo", schema: {} }],
      },
      selected_transport: { protocol: "webrtc", version: "1.0" },
      future_extension: { retained: true },
    };
  }
  readonly fetch: typeof fetch = Object.assign(
    async (input: Parameters<typeof fetch>[0], init?: Parameters<typeof fetch>[1]) => {
      const request = new Request(input, init);
      const call = {
        method: request.method,
        url: new URL(request.url),
        headers: request.headers,
        body: await request.text(),
        signal: request.signal,
      };
      this.calls.push(call);
      this.order.push(`${call.method} ${call.url.pathname}`);
      const custom = await this.hook?.(call);
      if (custom !== undefined) return custom;
      const path = call.url.pathname;
      if (path === "/tokens") return jsonResponse({ jwt: "fixture-jwt" });
      if ((path === "/sessions" && call.method === "POST") || path === "/start_session")
        return jsonResponse(
          this.createSlim ? { session_id: this.sessionId, state: "CREATED" } : this.descriptor,
        );
      if (
        (path === `/sessions/${this.sessionId}` && call.method === "DELETE") ||
        path === "/stop_session"
      ) {
        if (this.terminateConfirms) this.closed = true;
        return new Response(null, { status: 202 });
      }
      if (path === `/sessions/${this.sessionId}` || path === "/session") {
        if (this.readsUntilReady-- > 0)
          return jsonResponse({ session_id: this.sessionId, state: "WAITING" });
        return jsonResponse(this.descriptor);
      }
      if (path.endsWith("/ice_servers")) return jsonResponse({ ice_servers: [] });
      if (path.endsWith("/connections")) return jsonResponse({ connection_id: 1001 });
      if (path.endsWith("/ice_candidates")) return new Response(null, { status: 204 });
      if (path.endsWith("/sdp_params")) {
        if (call.method !== "GET") return new Response(null, { status: 204 });
        if (this.sdpPollsBeforeReady-- > 0) return new Response(null, { status: 202 });
        return jsonResponse({ sdp_answer: "fixture policy SDP; NOT a real peer" });
      }
      if (path.endsWith("/uploads"))
        return jsonResponse({
          presigned_id: "up_fixture",
          presigned_url: "https://upload.fixture/blob",
          path: "uploads/file",
        });
      if (path === "/blob") return new Response(null, { status: 204 });
      return jsonResponse({ error: `unhandled fixture route ${path}` }, 404);
    },
    { preconnect: (_url: string | URL) => undefined },
  );
}
export class FakeTrack extends EventTarget implements MediaStreamTrack {
  readonly id = "fake-track";
  readonly label = "policy-only mock";
  readonly muted = false;
  contentHint = "";
  enabled = true;
  readyState: MediaStreamTrackState = "live";
  onended: ((this: MediaStreamTrack, event: Event) => void) | null = null;
  onmute: ((this: MediaStreamTrack, event: Event) => void) | null = null;
  onunmute: ((this: MediaStreamTrack, event: Event) => void) | null = null;
  readonly clones: FakeTrack[] = [];
  constructor(readonly kind = "audio") {
    super();
  }
  clone(): FakeTrack {
    const clone = new FakeTrack(this.kind);
    this.clones.push(clone);
    return clone;
  }
  stop(): void {
    this.readyState = "ended";
  }
  applyConstraints(): Promise<void> {
    return Promise.resolve();
  }
  getCapabilities(): MediaTrackCapabilities {
    return {};
  }
  getConstraints(): MediaTrackConstraints {
    return {};
  }
  getSettings(): MediaTrackSettings {
    return {};
  }
}
export class MockPeer implements Peer {
  emit: (event: PeerEvent) => void = () => undefined;
  closes = 0;
  answers = 0;
  prepares = 0;
  readonly sent: { readonly channel: Channel; readonly bytes: Uint8Array<ArrayBuffer> }[] = [];
  readonly replacements: (MediaStreamTrack | null)[] = [];
  readonly bitrateCalls: { readonly name: string; readonly rate: number }[] = [];
  readonly leases = new Set<MediaStreamTrack>();
  source = new FakeTrack("video");
  autoReply = true;
  sendHook: ((channel: Channel, bytes: Uint8Array<ArrayBuffer>) => void) | undefined;
  answerHook: (() => Effect.Effect<void, ReactorError>) | undefined;
  prepareHook: (() => void) | undefined;
  directionHook: (() => Effect.Effect<void, ReactorError>) | undefined;
  statsEntries: readonly unknown[] = [];
  replaceHook:
    | ((name: string, track: MediaStreamTrack | null) => Effect.Effect<void, ReactorError>)
    | undefined;
  constructor(readonly fixture: HttpFixture) {}
  prepare(
    _servers: readonly RTCIceServer[],
    tracks: readonly Track[],
    emit: (event: PeerEvent) => void,
  ): Effect.Effect<Prepared, ReactorError> {
    return Effect.sync(() => {
      this.prepares++;
      this.emit = emit;
      this.fixture.order.push("peer.prepare");
      this.prepareHook?.();
      emit({
        type: "ice",
        candidate: { candidate: "candidate:fixture-policy-only", sdp_mid: "0", sdp_mline_index: 0 },
      });
      emit({ type: "ice" });
      return {
        sdp: "fixture policy offer; NOT a real peer",
        mapping: tracks.map((track, i) => ({ ...track, mid: String(i) })),
      };
    });
  }
  answer(_sdp: string): Effect.Effect<void, ReactorError> {
    return Effect.suspend(() => {
      this.answers++;
      this.fixture.order.push("peer.answer");
      if (this.answerHook !== undefined) return this.answerHook();
      return Effect.sync(() => {
        this.emit({ type: "state", state: "connected" });
        this.emit({ type: "channel", channel: "data", open: true });
        this.emit({ type: "channel", channel: "control", open: true });
      });
    });
  }
  replyData(message: W.DataServerMessage): void {
    this.emit({ type: "message", channel: "data", bytes: W.DataServerMessage.encode(message) });
  }
  replyControl(message: W.ControlServerMessage): void {
    this.emit({
      type: "message",
      channel: "control",
      bytes: W.ControlServerMessage.encode(message),
    });
  }
  send(channel: Channel, bytes: Uint8Array<ArrayBuffer>): Effect.Effect<void, ReactorError> {
    return Effect.try({
      try: () => {
        this.sent.push({ channel, bytes });
        this.sendHook?.(channel, bytes);
        if (!this.autoReply) return;
        if (channel === "data") {
          const request = W.DataClientMessage.decode(bytes);
          this.replyData({ request_id: request.request_id, kind: 2 });
        } else {
          const request = W.ControlClientMessage.decode(bytes),
            payload = request.payload;
          if (request.kind !== 1 || payload === undefined) return;
          if (payload.case === "request_schema")
            this.replyControl({
              request_id: request.request_id,
              kind: 2,
              payload: {
                case: "model_schema",
                value: { openapi: structFromObject({ openapi: "3.1.0", paths: {} }) },
              },
            });
          else if (payload.case === "publish_track")
            this.replyControl({
              request_id: request.request_id,
              kind: 2,
              payload: { case: "publish_track", value: { name: payload.value.name } },
            });
          else if (payload.case === "request_clip" || payload.case === "request_recording")
            this.replyControl({
              request_id: request.request_id,
              kind: 2,
              payload: {
                case: "clip_ready",
                value: {
                  session_id: this.fixture.sessionId,
                  kind: "snap",
                  start_marker: 0,
                  end_marker: 1,
                  now_marker: 1,
                  predicted_ready_at_ms: 1n,
                  playlist_url: "/clip.m3u8",
                },
              },
            });
        }
      },
      catch: (cause) =>
        cause instanceof ReactorError ? cause : new ReactorError("Disconnected", String(cause)),
    });
  }
  close(): void {
    this.closes++;
    for (const track of this.leases) track.stop();
    this.leases.clear();
  }
  lease(): MediaStreamTrack {
    const clone = this.source.clone();
    this.leases.add(clone);
    return clone;
  }
  release(track: MediaStreamTrack): void {
    track.stop();
    this.leases.delete(track);
  }
  direction(): Effect.Effect<void, ReactorError> {
    return this.directionHook?.() ?? Effect.void;
  }
  replace(name: string, track: MediaStreamTrack | null): Effect.Effect<void, ReactorError> {
    return Effect.suspend(() => {
      this.replacements.push(track);
      return this.replaceHook?.(name, track) ?? Effect.void;
    });
  }
  maxBitrate(name: string, rate: number): Effect.Effect<void, ReactorError> {
    return Effect.sync(() => {
      this.bitrateCalls.push({ name, rate });
    });
  }
  stats(): Effect.Effect<readonly unknown[], ReactorError> {
    return Effect.succeed(this.statsEntries);
  }
}
export const withFixture = async (body: (fixture: HttpFixture) => Promise<void>): Promise<void> => {
  const fixture = new HttpFixture(),
    original = globalThis.fetch;
  globalThis.fetch = fixture.fetch;
  try {
    await body(fixture);
  } finally {
    globalThis.fetch = original;
  }
};
export const makeSession = (
  fixture: HttpFixture,
  options: Partial<SessionOptions> = {},
  configure?: (peer: MockPeer) => void,
): { session: Session; peers: MockPeer[] } => {
  const peers: MockPeer[] = [];
  const configured: SessionOptions = {
    apiUrl: "https://coordinator.fixture",
    intent: { _tag: "Create", model: { name: "owner/model" } },
    heartbeatMs: 0,
    credential: Effect.succeed("fixture-token"),
    requestTimeoutMs: 100,
    commandTimeoutMs: 50,
    readyTimeoutMs: 200,
    connectTimeoutMs: 1000,
    sessionPoll: { attempts: 5, initialMs: 1, maxMs: 4 },
    sdpPoll: { attempts: 5, initialMs: 1, maxMs: 4 },
    ...options,
  };
  const session = new Session(
    configured,
    () => {
      const peer = new MockPeer(fixture);
      configure?.(peer);
      peers.push(peer);
      return peer;
    },
    new ProtocolHttpClient(configured, httpForTests()),
  );
  return { session, peers };
};
export const stall = (signal: AbortSignal): Promise<Response> =>
  new Promise((_resolve, reject) => {
    if (signal.aborted) {
      reject(new Error("aborted"));
      return;
    }
    signal.addEventListener("abort", () => reject(new Error("aborted")), { once: true });
  });
