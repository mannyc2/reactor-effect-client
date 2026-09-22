/** MODELED native host. Proves the independent fixture's assertions and ownership,
 * NOT actual WebRTC, decoded pixels or playback. test:browser supplies that evidence. */
import { PublicationReceiver } from "../integration/browser/publication.js";
import { FakeTrack } from "./fixtures.js";
import { assert, equal, eventually, test, withGlobals } from "./harness.js";
const realInterval = globalThis.setInterval,
  realClearInterval = globalThis.clearInterval;
interface Options {
  readonly play?: () => Promise<void>;
  readonly silent?: boolean;
  readonly inputFailure?: boolean;
  readonly readbackFailure?: boolean;
  readonly noCallbacks?: boolean;
}
class WatchedTarget extends EventTarget {
  readonly listeners = new Set<EventListenerOrEventListenerObject | null>();
  override addEventListener(
    name: string,
    body: EventListenerOrEventListenerObject | null,
    options?: AddEventListenerOptions | boolean,
  ): void {
    this.listeners.add(body);
    super.addEventListener(name, body, options);
  }
  override removeEventListener(
    name: string,
    body: EventListenerOrEventListenerObject | null,
    options?: EventListenerOptions | boolean,
  ): void {
    this.listeners.delete(body);
    super.removeEventListener(name, body, options);
  }
}
class NodeModel {
  disconnected = false;
  connect(_node: unknown): void {}
  disconnect(): void {
    this.disconnected = true;
  }
}
class AnalyserModel extends NodeModel {
  fftSize = 2048;
  energy = 0.08;
  getFloatTimeDomainData(samples: Float32Array): void {
    samples.fill(this.energy);
  }
}
class ContextModel extends WatchedTarget {
  state: AudioContextState = "running";
  currentTime = 0;
  sampleRate = 48000;
  readonly destination = new NodeModel();
  readonly nodes: NodeModel[] = [];
  readonly streams: unknown[] = [];
  readonly analyser = new AnalyserModel();
  constructor(private readonly options: Options) {
    super();
    if (options.silent) this.analyser.energy = 0;
  }
  createMediaStreamSource(stream: unknown): NodeModel {
    if (this.options.inputFailure) throw new Error("model graph failure");
    this.streams.push(stream);
    const node = new NodeModel();
    this.nodes.push(node);
    return node;
  }
  createAnalyser(): AnalyserModel {
    this.nodes.push(this.analyser);
    return this.analyser;
  }
  createGain(): NodeModel & { gain: { value: number } } {
    const node = Object.assign(new NodeModel(), { gain: { value: 1 } });
    this.nodes.push(node);
    return node;
  }
}
class SinkModel extends WatchedTarget {
  srcObject: MediaProvider | null = null;
  muted = false;
  paused = true;
  playsInline = false;
  autoplay = false;
  videoWidth = 160;
  videoHeight = 96;
  byte = 1;
  plays = 0;
  loads = 0;
  removed = false;
  isConnected = false;
  error: { message: string } | null = null;
  readonly callbacks = new Map<number, VideoFrameRequestCallback>();
  private next = 0;
  constructor(private readonly options: Options) {
    super();
  }
  play(): Promise<void> {
    this.plays++;
    this.paused = false;
    return this.options.play?.() ?? Promise.resolve();
  }
  pause(): void {
    this.paused = true;
  }
  load(): void {
    this.loads++;
  }
  remove(): void {
    this.removed = true;
  }
  setAttribute(_name: string, _value: string): void {}
  removeAttribute(_name: string): void {}
  getAttribute(_name: string): null {
    return null;
  }
  requestVideoFrameCallback(callback: VideoFrameRequestCallback): number {
    this.callbacks.set(++this.next, callback);
    return this.next;
  }
  cancelVideoFrameCallback(id: number): void {
    this.callbacks.delete(id);
  }
  frame(time: number, byte: number): void {
    this.byte = byte;
    const callbacks = [...this.callbacks.values()];
    this.callbacks.clear();
    for (const callback of callbacks)
      callback(time * 1000, {
        presentationTime: time * 1000,
        expectedDisplayTime: time * 1000,
        mediaTime: time,
        presentedFrames: this.next,
        width: this.videoWidth,
        height: this.videoHeight,
      });
  }
}
class PeerModel extends WatchedTarget {
  connectionState: RTCPeerConnectionState = "connected";
}
class StreamModel {
  constructor(readonly tracks: MediaStreamTrack[]) {}
}
interface Host {
  readonly context: AudioContext;
  readonly pc: RTCPeerConnection;
  readonly model: ContextModel;
  readonly peer: PeerModel;
  readonly video: SinkModel;
  readonly audio: SinkModel;
  readonly tracks: { readonly audio: FakeTrack; readonly video: FakeTrack };
  readonly intervals: Map<number, () => void>;
  tickAudio(): void;
  frames(count?: number, changing?: boolean): void;
  open(signal?: AbortSignal, timeout?: number): Promise<PublicationReceiver>;
}
const host = async (options: Options, body: (host: Host) => Promise<void>): Promise<void> => {
  const video = new SinkModel(options),
    audio = new SinkModel(options),
    models: ContextModel[] = [],
    peers: PeerModel[] = [];
  const tracks = { video: new FakeTrack("video"), audio: new FakeTrack("audio") },
    intervals = new Map<number, () => void>();
  if (options.noCallbacks)
    Object.defineProperty(video, "requestVideoFrameCallback", { value: undefined });
  const pixels = {
    drawImage(source: unknown) {
      assert(source === video, "must copy the RECEIVING playback element, not publisher canvas");
    },
    getImageData() {
      if (options.readbackFailure) throw new Error("model readback failure");
      return { data: new Uint8ClampedArray(160 * 96 * 4).fill(video.byte) };
    },
  };
  class Context extends ContextModel {
    constructor() {
      super(options);
      models.push(this);
    }
  }
  class Peer extends PeerModel {
    constructor() {
      super();
      peers.push(this);
    }
  }
  let time = 0,
    next = 0;
  await withGlobals(
    {
      AudioContext: Context,
      RTCPeerConnection: Peer,
      MediaStream: StreamModel,
      document: {
        createElement(name: string) {
          if (name === "video") return video;
          if (name === "audio") return audio;
          assert(name === "canvas");
          return { width: 0, height: 0, getContext: () => pixels };
        },
      },
      setInterval(callback: () => void) {
        intervals.set(++next, callback);
        return next;
      },
      clearInterval(id: number) {
        intervals.delete(id);
      },
    },
    async () => {
      const context = new AudioContext(),
        pc = new RTCPeerConnection(),
        model = models[0],
        peer = peers[0];
      assert(model !== undefined && peer !== undefined);
      const tickAudio = (): void => {
        model.currentTime += 0.05;
        for (const tick of [...intervals.values()]) tick();
      };
      const h: Host = {
        context,
        pc,
        model,
        peer,
        video,
        audio,
        tracks,
        intervals,
        tickAudio,
        frames(count = 12, changing = true) {
          for (let i = 0; i < count; i++) {
            time += 0.05;
            video.frame(time, changing ? (Math.round(time * 20) % 250) + 1 : 1);
            tickAudio();
          }
        },
        open: (signal = new AbortController().signal, timeout) =>
          PublicationReceiver.open(
            tracks,
            context,
            pc,
            signal,
            timeout === undefined ? {} : { activationTimeoutMs: timeout },
          ),
      };
      await body(h);
      equal(intervals.size, 0);
      equal(video.callbacks.size, 0);
      equal(video.listeners.size, 0);
      equal(audio.listeners.size, 0);
      equal(model.listeners.size, 0);
      equal(peer.listeners.size, 0);
      assert(model.nodes.every((n) => n.disconnected));
      assert(
        [...tracks.audio.clones, ...tracks.video.clones].every((t) => t.readyState === "ended"),
      );
      for (const sink of [video, audio])
        if (sink.plays > 0) assert(sink.srcObject === null && sink.paused && sink.removed);
      // The owner, not the fixture, may intentionally end a track/context/peer in a test.
    },
  );
};
const rejected = async (promise: Promise<unknown>, text: string): Promise<void> => {
  try {
    await promise;
  } catch (error) {
    assert(String(error).includes(text), `expected ${text}: ${String(error)}`);
    return;
  }
  throw new Error("expected rejection: " + text);
};
const wait = (ms: number): Promise<void> => new Promise((resolve) => setTimeout(resolve, ms));
const observe = (r: PublicationReceiver, h: Host): Promise<unknown> => {
  const result = r.changing(new AbortController().signal, 1000);
  h.frames();
  return result;
};

test("publication harness policy: receiver playback pixels/audio, explicit clocks, cloned tracks and idempotent cleanup", () =>
  host({}, async (h) => {
    const r = await h.open();
    assert(h.video.muted && h.audio.muted && h.video.plays === 1 && h.audio.plays === 1);
    equal(h.model.streams, [h.audio.srcObject]);
    const result = r.changing(new AbortController().signal, 1000);
    h.frames();
    const report = await result;
    equal(report.video.frames.length, 12);
    equal(report.video.distinctHashes, 12);
    equal(report.audio.windows.length, 12);
    equal(report.video.clock, "media-element-timeline");
    equal(report.audio.clock, "audio-context-observation");
    assert(report.audio.maximumWindowRms > 0.001 && report.timestamps.includes("no sample PTS"));
    await r.dispose();
    await r.dispose();
    equal(h.video.loads, 1);
    equal(h.audio.loads, 1);
    equal(h.tracks.video.readyState, "live");
    equal(h.tracks.audio.readyState, "live");
    equal(h.context.state, "running");
  }));
test("publication harness policy: packet-like progress/static pictures cannot pass changing decoded video", () =>
  host({}, async (h) => {
    const r = await h.open();
    try {
      const result = rejected(r.changing(new AbortController().signal, 100), "hashes=1");
      h.frames(12, false);
      await result;
    } finally {
      await r.dispose();
    }
  }));
test("publication harness policy: silent decoded audio cannot pass despite changing video", () =>
  host({ silent: true }, async (h) => {
    const r = await h.open();
    try {
      const result = rejected(r.changing(new AbortController().signal, 100), "rms=0");
      h.frames();
      await result;
    } finally {
      await r.dispose();
    }
  }));
test("publication harness policy: activation rejection releases partial owners without touching borrowed tracks", () =>
  host({ play: () => Promise.reject(new Error("model play rejection")) }, async (h) => {
    await rejected(h.open(), "play rejection");
    equal(h.tracks.audio.readyState, "live");
    equal(h.tracks.video.readyState, "live");
  }));
test("publication harness policy: activation deadline and late play cannot recreate graph/callbacks", async () => {
  const resolve: (() => void)[] = [];
  await host({ play: () => new Promise<void>((done) => resolve.push(done)) }, async (h) => {
    await rejected(h.open(undefined, 15), "deadline");
    for (const done of resolve) done();
    await wait(10);
    equal(h.video.callbacks.size, 0);
    equal(h.model.nodes.length, 3);
  });
});
test("publication harness policy: activation interruption releases both sinks and consumes late promises", async () => {
  const resolve: (() => void)[] = [];
  await host({ play: () => new Promise<void>((done) => resolve.push(done)) }, async (h) => {
    const abort = new AbortController(),
      result = rejected(h.open(abort.signal), "aborted");
    await eventually(() => h.video.plays === 1 && h.audio.plays === 1);
    abort.abort();
    await result;
    for (const done of resolve) done();
    await wait(10);
    equal(h.intervals.size, 0);
  });
});
test("publication harness policy: interrupted observation joins cleanup; late video callback cannot retain pixels", () =>
  host({}, async (h) => {
    const r = await h.open(),
      abort = new AbortController(),
      result = rejected(r.changing(abort.signal), "interrupted");
    const callback = [...h.video.callbacks.values()][0];
    assert(callback !== undefined);
    abort.abort();
    await r.dispose();
    await result;
    callback(100, {
      presentationTime: 100,
      expectedDisplayTime: 100,
      mediaTime: 0.1,
      presentedFrames: 1,
      width: 160,
      height: 96,
    });
    equal(h.tracks.audio.readyState, "live");
    equal(h.tracks.video.readyState, "live");
  }));
test("publication harness policy: partial graph failure stops owned clones and detaches elements", () =>
  host({ inputFailure: true }, async (h) => {
    await rejected(h.open(), "graph failure");
    assert(h.video.srcObject === null && h.audio.srcObject === null);
  }));
test("publication harness policy: absent native frame callbacks fails explicitly, not packet-only success", () =>
  host({ noCallbacks: true }, async (h) => {
    await rejected(h.open(), "requestVideoFrameCallback");
    equal(h.tracks.video.clones.length, 0);
  }));
test("publication harness policy: failed decoded pixel readback immediately closes native owners", () =>
  host({ readbackFailure: true }, async (h) => {
    const r = await h.open(),
      result = rejected(r.changing(new AbortController().signal), "readback failure");
    h.frames(1);
    await result;
    await r.dispose();
    equal(h.video.callbacks.size, 0);
  }));
test("publication harness policy: regressing video timestamps cannot be relabelled as received presentation", () =>
  host({}, async (h) => {
    const r = await h.open();
    h.video.frame(1, 1);
    h.video.frame(0.5, 2);
    await rejected(r.changing(new AbortController().signal), "timeline did not advance");
    await r.dispose();
  }));
test("publication harness policy: closed peer is not evidence of successful unpublication", () =>
  host({ silent: true }, async (h) => {
    const r = await h.open();
    h.peer.connectionState = "closed";
    h.peer.dispatchEvent(new Event("connectionstatechange"));
    await rejected(r.quiet(new AbortController().signal), "peer retired");
    await r.dispose();
  }));
test("publication harness policy: suspended context is not evidence of remote silence", () =>
  host({ silent: true }, async (h) => {
    const r = await h.open();
    h.model.state = "suspended";
    h.model.dispatchEvent(new Event("statechange"));
    await rejected(r.quiet(new AbortController().signal), "AudioContext stopped");
    await r.dispose();
  }));
test("publication harness policy: finite quiet window keeps receiver active and permits fresh decoded observations", () =>
  host({}, async (h) => {
    const r = await h.open();
    try {
      await observe(r, h);
      h.model.analyser.energy = 0;
      const clock = realInterval(h.tickAudio, 25);
      try {
        await r.quiet(new AbortController().signal, { quietMs: 300, timeoutMs: 650 });
      } finally {
        realClearInterval(clock);
      }
      assert(!h.video.paused && h.video.srcObject !== null && h.tracks.video.readyState === "live");
      h.model.analyser.energy = 0.08;
      await observe(r, h);
    } finally {
      await r.dispose();
    }
  }));
test("publication harness policy: continuing native presentation cannot pass retirement", () =>
  host({ silent: true }, async (h) => {
    const r = await h.open(),
      ticking = realInterval(() => h.frames(1), 25);
    try {
      await rejected(
        r.quiet(new AbortController().signal, { quietMs: 300, timeoutMs: 500 }),
        "retirement deadline",
      );
    } finally {
      realClearInterval(ticking);
      await r.dispose();
    }
  }));
test("publication harness policy: non-silent audio alone prevents false retirement", () =>
  host({}, async (h) => {
    const r = await h.open(),
      ticking = realInterval(h.tickAudio, 25);
    try {
      await rejected(
        r.quiet(new AbortController().signal, { quietMs: 300, timeoutMs: 500 }),
        "retirement deadline",
      );
    } finally {
      realClearInterval(ticking);
      await r.dispose();
    }
  }));
test("publication harness policy: missing audio windows cannot count as silence", () =>
  host({ silent: true }, async (h) => {
    const r = await h.open();
    try {
      await rejected(
        r.quiet(new AbortController().signal, { quietMs: 300, timeoutMs: 450 }),
        "retirement deadline",
      );
    } finally {
      await r.dispose();
    }
  }));
test("publication harness policy: slow observer retention overflow is explicit", () =>
  host({}, async (h) => {
    const r = await h.open();
    try {
      const result = rejected(r.changing(new AbortController().signal), "retention bound");
      h.frames(257);
      await result;
    } finally {
      await r.dispose();
    }
  }));
test("publication harness policy: stopped borrowed lease without ended event is caught by liveness polling", () =>
  host({}, async (h) => {
    const r = await h.open();
    h.tracks.video.stop();
    h.tickAudio();
    await rejected(r.changing(new AbortController().signal), "borrowed receiver track ended");
    await r.dispose();
  }));
