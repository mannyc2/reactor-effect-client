/** MODELED native host: regression tests of the recording HARNESS's assertions and
 * ownership. These are not decoded recording playback evidence. test:browser must
 * exercise actual HTML decoding of bytes returned by public downloadClip(). */
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import { recordingPlayback } from "../integration/browser/recording.js";
import { assert, equal, eventually, failure, run, test, withGlobals } from "./harness.js";

interface Options {
  readonly play?: () => Promise<void>;
  readonly silent?: boolean;
  readonly inputFailure?: boolean;
  readonly readbackFailure?: boolean;
  readonly noFrameCallbacks?: boolean;
}
class AudioNodeModel {
  disconnected = false;
  connect(_target: unknown): void {}
  disconnect(): void {
    this.disconnected = true;
  }
}
class AnalyserModel extends AudioNodeModel {
  fftSize = 2048;
  constructor(private readonly silent: boolean) {
    super();
  }
  getFloatTimeDomainData(samples: Float32Array): void {
    samples.fill(this.silent ? 0 : 0.08);
  }
}
class ContextModel extends EventTarget {
  state: AudioContextState = "running";
  currentTime = 0;
  sampleRate = 48000;
  readonly destination = new AudioNodeModel();
  readonly nodes: AudioNodeModel[] = [];
  readonly elements: unknown[] = [];
  readonly listeners = new Set<EventListenerOrEventListenerObject | null>();
  constructor(private readonly options: Options) {
    super();
  }
  createMediaElementSource(element: unknown): AudioNodeModel {
    if (this.options.inputFailure) throw new Error("model graph acquisition failure");
    this.elements.push(element);
    const node = new AudioNodeModel();
    this.nodes.push(node);
    return node;
  }
  createAnalyser(): AnalyserModel {
    const node = new AnalyserModel(this.options.silent ?? false);
    this.nodes.push(node);
    return node;
  }
  createGain(): AudioNodeModel & { gain: { value: number } } {
    const node = Object.assign(new AudioNodeModel(), { gain: { value: 1 } });
    this.nodes.push(node);
    return node;
  }
  override addEventListener(
    type: string,
    callback: EventListenerOrEventListenerObject | null,
    options?: boolean | AddEventListenerOptions,
  ): void {
    this.listeners.add(callback);
    super.addEventListener(type, callback, options);
  }
  override removeEventListener(
    type: string,
    callback: EventListenerOrEventListenerObject | null,
    options?: boolean | EventListenerOptions,
  ): void {
    this.listeners.delete(callback);
    super.removeEventListener(type, callback, options);
  }
}
class VideoModel extends EventTarget {
  srcObject: MediaProvider | null = null;
  src = "";
  muted = true;
  volume = 1;
  loop = false;
  playsInline = false;
  preload = "";
  currentTime = 0;
  duration = 2.021333;
  ended = false;
  paused = true;
  videoWidth = 96;
  videoHeight = 64;
  readyState = 4;
  error: { code: number; message: string } | null = null;
  byte = 1;
  plays = 0;
  pauses = 0;
  loads = 0;
  removed = false;
  readonly listeners = new Set<EventListenerOrEventListenerObject | null>();
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
    this.pauses++;
    this.paused = true;
  }
  load(): void {
    this.loads++;
  }
  remove(): void {
    this.removed = true;
  }
  setAttribute(_name: string, _value: string): void {}
  removeAttribute(name: string): void {
    if (name === "src") this.src = "";
  }
  canPlayType(_mime: string): string {
    return "probably";
  }
  requestVideoFrameCallback(callback: VideoFrameRequestCallback): number {
    this.callbacks.set(++this.next, callback);
    return this.next;
  }
  cancelVideoFrameCallback(id: number): void {
    this.callbacks.delete(id);
  }
  frame(time: number, byte = 1): void {
    this.currentTime = time;
    this.byte = byte;
    const calls = [...this.callbacks.values()];
    this.callbacks.clear();
    for (const callback of calls)
      callback(time * 1000, {
        presentationTime: time * 1000,
        expectedDisplayTime: time * 1000,
        mediaTime: time,
        presentedFrames: this.next,
        width: this.videoWidth,
        height: this.videoHeight,
      });
  }
  finish(): void {
    this.ended = true;
    this.currentTime = this.duration;
    this.dispatchEvent(new Event("ended"));
  }
  override addEventListener(
    type: string,
    callback: EventListenerOrEventListenerObject | null,
    options?: boolean | AddEventListenerOptions,
  ): void {
    this.listeners.add(callback);
    super.addEventListener(type, callback, options);
  }
  override removeEventListener(
    type: string,
    callback: EventListenerOrEventListenerObject | null,
    options?: boolean | EventListenerOptions,
  ): void {
    this.listeners.delete(callback);
    super.removeEventListener(type, callback, options);
  }
}
interface Host {
  readonly video: VideoModel;
  readonly model: ContextModel;
  readonly blobs: Blob[];
  readonly revoked: string[];
}
const host = async (
  options: Options,
  body: (context: AudioContext, host: Host) => Promise<void>,
): Promise<void> => {
  const video = new VideoModel(options),
    contexts: ContextModel[] = [],
    blobs: Blob[] = [],
    revoked: string[] = [];
  class Context extends ContextModel {
    constructor() {
      super(options);
      contexts.push(this);
    }
  }
  const pixels = {
    drawImage(source: unknown) {
      assert(source === video, "must copy the actual playback element");
    },
    getImageData() {
      if (options.readbackFailure) throw new Error("model readback failure");
      return { data: new Uint8ClampedArray(96 * 64 * 4).fill(video.byte) };
    },
  };
  const canvas = { width: 0, height: 0, getContext: () => pixels };
  if (options.noFrameCallbacks)
    Object.defineProperty(video, "requestVideoFrameCallback", { value: undefined });
  await withGlobals(
    {
      AudioContext: Context,
      document: {
        createElement(tag: unknown) {
          if (tag === "video") return video;
          assert(tag === "canvas");
          return canvas;
        },
      },
      URL: {
        createObjectURL(blob: Blob) {
          blobs.push(blob);
          return "blob:model-owned-recording";
        },
        revokeObjectURL(url: string) {
          revoked.push(url);
        },
      },
    },
    async () => {
      const context = new AudioContext(),
        model = contexts[0];
      assert(model !== undefined);
      await body(context, { video, model, blobs, revoked });
      // The fixture borrows its context but owns everything it acquired beneath it.
      equal(context.state, "running");
      equal(video.callbacks.size, 0);
      equal(video.listeners.size, 0);
      equal(model.listeners.size, 0);
      assert(model.nodes.every((node) => node.disconnected));
      if (blobs.length > 0) {
        equal(revoked, ["blob:model-owned-recording"]);
        equal(video.src, "");
        equal(video.srcObject, null);
        equal(video.pauses, 1);
        equal(video.loads, 1);
        assert(video.removed && video.paused);
      }
    },
  );
};
const bytes = new Uint8Array([3, 7, 9]); // POLICY MODEL only; not an MP4 and never represented as decoder evidence.
const advance = (h: Host, changes = true): void => {
  for (let i = 1; i <= 9; i++) {
    h.model.currentTime = i / 20;
    h.video.frame(i / 20, changes ? i : 1);
  }
  h.video.finish();
};
test("recording harness policy: uses downloaded bytes and one element, with independent timeline/observation labels and cleanup", () =>
  host({}, async (context, h) => {
    const result = run(recordingPlayback(bytes, context, { timeoutMs: 500 }));
    await eventually(() => h.video.plays === 1 && h.video.callbacks.size === 1);
    assert(!h.video.muted && !h.video.loop);
    equal(h.model.elements, [h.video]);
    const blob = h.blobs[0];
    assert(blob !== undefined);
    equal(blob.type, "video/mp4");
    equal(new Uint8Array(await blob.arrayBuffer()), bytes);
    advance(h);
    const report = await result;
    equal(report.ended, true);
    equal(report.video.distinctHashes, 9);
    equal(report.video.clock, "media-element-timeline");
    equal(report.audio.clock, "audio-context-observation");
    equal(report.audio.windowFrames, 2048);
    assert(
      report.audio.maximumWindowRms > 0.001 && report.audio.timestamps.includes("no sample PTS"),
    );
  }));
test("recording harness policy: silent decoded-window observations cannot pass", () =>
  host({ silent: true }, async (context, h) => {
    const result = failure(recordingPlayback(bytes, context, { timeoutMs: 500 }));
    await eventually(() => h.video.callbacks.size === 1);
    advance(h);
    assert((await result).message.includes("silent"));
  }));
test("recording harness policy: repeated picture cannot pass solely because audio or container bytes exist", () =>
  host({}, async (context, h) => {
    const result = failure(recordingPlayback(bytes, context, { timeoutMs: 500 }));
    await eventually(() => h.video.callbacks.size === 1);
    advance(h, false);
    assert((await result).message.includes("changing"));
  }));
test("recording harness policy: rejected normal playback releases URL, element, listener and graph", () =>
  host(
    {
      play: () => Promise.reject(new DOMException("activation rejected", "NotAllowedError")),
    },
    async (context) => {
      const error = await failure(recordingPlayback(bytes, context, { timeoutMs: 500 }));
      equal(error.code, "UnsupportedCapability");
      assert(error.message.includes("activation"));
    },
  ));
test("recording harness policy: stalled play has a distinct activation deadline and cleanup", () =>
  host({ play: () => new Promise(() => {}) }, async (context) => {
    const error = await failure(
      recordingPlayback(bytes, context, { activationTimeoutMs: 20, timeoutMs: 500 }),
    );
    equal(error.code, "Timeout");
    assert(error.message.includes("activation"));
  }));
test("recording harness policy: play resolution alone is not playback evidence; missing callbacks time out", () =>
  host({}, async (context) => {
    const error = await failure(recordingPlayback(bytes, context, { timeoutMs: 20 }));
    equal(error.code, "Timeout");
  }));
test("recording harness policy: interruption releases resources and late play fulfillment cannot rearm callbacks", async () => {
  let finish: (() => void) | undefined;
  await host(
    {
      play: () =>
        new Promise((resolve) => {
          finish = resolve;
        }),
    },
    async (context, h) => {
      const fiber = Effect.runFork(recordingPlayback(bytes, context, { timeoutMs: 5000 }));
      await eventually(() => h.video.plays === 1 && h.video.callbacks.size === 1);
      await Effect.runPromise(Fiber.interrupt(fiber));
      finish?.();
      await new Promise((resolve) => setTimeout(resolve, 10));
      equal(h.video.callbacks.size, 0);
      equal(h.revoked.length, 1);
    },
  );
});
test("recording harness policy: media error events fail with observed diagnostics and release native owners", () =>
  host({}, async (context, h) => {
    const result = failure(recordingPlayback(bytes, context, { timeoutMs: 500 }));
    await eventually(() => h.video.callbacks.size === 1);
    h.video.error = { code: 3, message: "model codec failure" };
    h.video.dispatchEvent(new Event("error"));
    const error = await result;
    equal(error.code, "UnsupportedCapability");
    assert(error.context.detail !== undefined);
  }));
test("recording harness policy: stopped context fails instead of relabelling a static audio clock", () =>
  host({}, async (context, h) => {
    const result = failure(recordingPlayback(bytes, context, { timeoutMs: 500 }));
    await eventually(() => h.video.callbacks.size === 1);
    h.model.state = "suspended";
    h.model.dispatchEvent(new Event("statechange"));
    equal((await result).code, "Disconnected");
    h.model.state = "running"; // owner, not helper, restores its context
  }));
test("recording harness policy: failed graph acquisition still revokes the already-created URL", () =>
  host({ inputFailure: true }, async (context) => {
    assert((await failure(recordingPlayback(bytes, context))).message.includes("acquisition"));
  }));
test("recording harness policy: unsupported frame callbacks do not become a byte-only playback pass", () =>
  host({ noFrameCallbacks: true }, async (context) => {
    const error = await failure(recordingPlayback(bytes, context));
    equal(error.code, "UnsupportedCapability");
    assert(error.message.includes("requestVideoFrameCallback"));
  }));
test("recording harness policy: canvas readback failure unwinds the element and its audio route", () =>
  host({ readbackFailure: true }, async (context, h) => {
    const result = failure(recordingPlayback(bytes, context));
    await eventually(() => h.video.callbacks.size === 1);
    h.video.frame(0.1);
    assert((await result).message.includes("readback"));
  }));
test("recording harness policy: observation storage is bounded even for an unexpectedly long recording", () =>
  host({}, async (context, h) => {
    const result = failure(recordingPlayback(bytes, context));
    await eventually(() => h.video.callbacks.size === 1);
    for (let i = 1; i <= 129; i++) h.video.frame(i / 20, i);
    equal((await result).code, "Overflow");
  }));
test("recording harness policy: invalid limits or empty input do not allocate a recording URL", () =>
  host({}, async (context, h) => {
    equal((await failure(recordingPlayback(bytes, context, { timeoutMs: 0 }))).code, "Protocol");
    equal((await failure(recordingPlayback(new Uint8Array(), context))).code, "Overflow");
    equal(h.blobs.length, 0);
  }));
