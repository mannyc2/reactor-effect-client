import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Scope from "effect/Scope";
import * as Exit from "effect/Exit";
import type { PeerEvent } from "reactor-effect-client/Peer";
import { afterEach, vi } from "vitest";
import * as BrowserPeer from "../src/BrowserPeer.js";

/** Only what the peer uses of a media track. */
class FakeTrack {
  readyState: "live" | "ended" = "live";
  readonly clones: FakeTrack[] = [];
  constructor(readonly kind: string) {}
  clone(): FakeTrack {
    const clone = new FakeTrack(this.kind);
    this.clones.push(clone);
    return clone;
  }
  stop(): void {
    this.readyState = "ended";
  }
}

class FakeChannel {
  binaryType = "blob";
  readyState = "open";
  bufferedAmount = 0;
  readonly sent: Uint8Array[] = [];
  onopen: (() => void) | null = null;
  onclose: (() => void) | null = null;
  onerror: (() => void) | null = null;
  onmessage: ((event: { readonly data: unknown }) => void) | null = null;
  constructor(readonly label: string) {}
  send(bytes: Uint8Array): void {
    this.sent.push(bytes);
  }
  close(): void {
    this.readyState = "closed";
  }
}

interface FakeTransceiver {
  mid: string | null;
  direction: string;
}

/** Records what the peer asked of the platform, and lets a test drive its callbacks. */
class FakePeerConnection {
  static opened: FakePeerConnection[] = [];
  readonly channels: FakeChannel[] = [];
  readonly transceivers: FakeTransceiver[] = [];
  connectionState = "new";
  closed = false;
  readonly sctp = { maxMessageSize: 65_536 };
  onconnectionstatechange: (() => void) | null = null;
  onicecandidate: ((event: { readonly candidate: null }) => void) | null = null;
  ontrack:
    | ((event: { readonly track: FakeTrack; readonly transceiver: FakeTransceiver }) => void)
    | null = null;
  constructor() {
    FakePeerConnection.opened.push(this);
  }
  createDataChannel(label: string): FakeChannel {
    const channel = new FakeChannel(label);
    this.channels.push(channel);
    return channel;
  }
  addTransceiver(_kind: string, init: { readonly direction: string }): FakeTransceiver {
    const transceiver = { mid: null, direction: init.direction };
    this.transceivers.push(transceiver);
    return transceiver;
  }
  createOffer(): Promise<{ readonly type: string; readonly sdp: string }> {
    return Promise.resolve({ type: "offer", sdp: "v=0" });
  }
  setLocalDescription(): Promise<void> {
    this.transceivers.forEach((transceiver, index) => {
      transceiver.mid = String(index);
    });
    return Promise.resolve();
  }
  setRemoteDescription(): Promise<void> {
    return Promise.resolve();
  }
  close(): void {
    this.closed = true;
  }
}

const tracks = [
  { name: "main_video", kind: "video", direction: "recvonly" },
  { name: "main_audio", kind: "audio", direction: "recvonly" },
] as const;

const install = () => {
  FakePeerConnection.opened = [];
  vi.stubGlobal("RTCPeerConnection", FakePeerConnection);
  vi.stubGlobal("MediaStream", class {});
};
afterEach(() => {
  vi.unstubAllGlobals();
});

/** A prepared peer, its platform connection and the events it emitted. */
const prepared = Effect.gen(function* () {
  install();
  const peer = yield* BrowserPeer.make;
  const events: PeerEvent[] = [];
  const offer = yield* peer.prepare([{ urls: ["stun:stun.fixture"] }], tracks, (event) => {
    events.push(event);
  });
  const pc = FakePeerConnection.opened.at(-1);
  assert.isDefined(pc);
  return { peer, events, offer, pc };
});

it.effect("building the layer without WebRTC fails before any session is allocated", () =>
  Effect.gen(function* () {
    const error = yield* Layer.build(BrowserPeer.layer).pipe(Effect.flip);
    assert.strictEqual(error.reason._tag, "UnsupportedHost");
  }),
);

it.effect("prepare opens control before data and maps every declared track", () =>
  Effect.gen(function* () {
    const { offer, pc } = yield* prepared;
    assert.deepStrictEqual(
      pc.channels.map((channel) => channel.label),
      ["control", "data"],
    );
    assert.deepStrictEqual(
      offer.mapping.map((entry) => [entry.name, entry.mid]),
      [
        ["main_video", "0"],
        ["main_audio", "1"],
      ],
    );
  }),
);

it.effect(
  "a message over the negotiated bound is an Overflow, and nothing is emitted after close",
  () =>
    Effect.gen(function* () {
      const { peer, events, pc } = yield* prepared;
      yield* peer.answer("v=0");
      const data = pc.channels[1];
      assert.isDefined(data);
      data.onmessage?.({ data: new ArrayBuffer(8) });
      data.onmessage?.({ data: new ArrayBuffer(65_537) });
      yield* peer.close;
      data.onmessage?.({ data: new ArrayBuffer(8) });
      assert.deepStrictEqual(
        events.map((event) => (event.type === "error" ? event.error.reason._tag : event.type)),
        ["message", "Overflow"],
      );
      assert.isTrue(pc.closed);
    }),
);

it.effect("a send over the buffered bound is refused before submission", () =>
  Effect.gen(function* () {
    const { peer, pc } = yield* prepared;
    const control = pc.channels[0];
    assert.isDefined(control);
    control.bufferedAmount = 1_048_576;
    const error = yield* peer.send("control", new Uint8Array(4)).pipe(Effect.flip);
    assert.strictEqual(error.reason._tag, "Overflow");
    assert.strictEqual(error.context.outcome, "not-submitted");
    assert.strictEqual(control.sent.length, 0);
  }),
);

it.effect(
  "a lease is a clone that stops with its scope; closing the peer stops the received track",
  () =>
    Effect.gen(function* () {
      const { peer, pc } = yield* prepared;
      const received = new FakeTrack("video");
      const transceiver = pc.transceivers[0];
      assert.isDefined(transceiver);
      pc.ontrack?.({ track: received, transceiver });
      assert.strictEqual(peer.media._tag, "Tracks");
      if (peer.media._tag !== "Tracks") return;
      const scope = yield* Scope.make();
      const lease = yield* peer.media.lease("main_video").pipe(Scope.provide(scope));
      assert.strictEqual(lease.readyState, "live");
      assert.deepStrictEqual(received.clones, [lease]);
      yield* Scope.close(scope, Exit.void);
      assert.strictEqual(lease.readyState, "ended");
      assert.strictEqual(received.readyState, "live");
      yield* peer.close;
      assert.strictEqual(received.readyState, "ended");
    }),
);
