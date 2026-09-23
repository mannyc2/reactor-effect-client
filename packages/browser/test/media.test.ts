/** Simulated media-host POLICY tests. These do not establish browser WebRTC/codec support. */
import * as Effect from "effect/Effect";
import * as Stream from "effect/Stream";
import * as Fiber from "effect/Fiber";
import {
  audioContext,
  audioSamples,
  videoFrames,
  webAudioSamples,
  play,
  nextPresentation,
} from "../src/_internal/media.js";
import { decodeAudioPacket } from "../src/_internal/audio-packet.js";
import { ReactorError } from "reactor-effect-client";
import { errorOf, fromOwnedReadableStream } from "reactor-effect-client/host";
import { FakeTrack } from "reactor-effect-test-kit";
import {
  assert,
  equal,
  eventually,
  failure,
  run,
  test,
  throws,
  withGlobals as globals,
} from "./harness.js";
const pause = (ms = 10): Promise<void> => new Promise((resolve) => setTimeout(resolve, ms));

const packet = (overrides: Readonly<Record<string, unknown>> = {}): Record<string, unknown> => ({
  type: "pcm",
  frames: 4,
  sampleRate: 48000,
  frame: 48000,
  skipped: 128,
  planes: [new Float32Array([0.1, -0.2, 0.3, -0.4]).buffer],
  ...overrides,
});
test("media policy: acquisition exceptions are typed failures, not stream defects", async () => {
  const error = await failure(
    fromOwnedReadableStream({
      evaluate: () => {
        throw new ReactorError("UnsupportedCapability", "not exposed in this realm");
      },
      onError: errorOf,
    }).pipe(Stream.runDrain),
  );
  equal(error.code, "UnsupportedCapability");
});
test("media policy: early stream completion cancels AND releases the underlying lock", async () => {
  let cancels = 0;
  const readable = new ReadableStream<number>({
    start(c) {
      c.enqueue(7);
    },
    cancel() {
      cancels++;
    },
  });
  equal(
    await run(
      fromOwnedReadableStream({ evaluate: () => readable, onError: errorOf }).pipe(
        Stream.take(1),
        Stream.runCollect,
      ),
    ),
    [7],
  );
  equal(cancels, 1);
  equal(readable.locked, false);
});
test("media policy: interrupting a blocked reader cancels and releases without waiting for a sample", async () => {
  let cancels = 0;
  const readable = new ReadableStream<number>({
    cancel() {
      cancels++;
    },
  });
  const task = Effect.runFork(
    fromOwnedReadableStream({ evaluate: () => readable, onError: errorOf }).pipe(Stream.runDrain),
  );
  await eventually(() => readable.locked);
  await Effect.runPromise(Fiber.interrupt(task));
  equal(cancels, 1);
  equal(readable.locked, false);
});
test("media packet: render clock and skipped frames are retained, not relabelled RTP time", () => {
  const sample = decodeAudioPacket(packet(), 1024);
  equal(sample.timestampUs, 1000000);
  equal(sample.clock, "audio-context-render");
  equal(sample.skippedFrames, 128);
  equal(sample.frames, 4);
  assert(
    sample.planes[0] !== undefined && sample.planes[0][1] !== undefined && sample.planes[0][1] < 0,
  );
  equal(sample.planes[0].byteLength, 16);
});
test("media packet: malformed, nonfinite, over-bound and invalid timebase values fail", () => {
  for (const invalid of [
    null,
    [],
    packet({ type: "other" }),
    packet({ frames: 0 }),
    packet({ frames: 1.5 }),
    packet({ sampleRate: 0 }),
    packet({ frame: Number.MAX_SAFE_INTEGER + 1 }),
    packet({ skipped: -1 }),
    packet({ planes: [] }),
    packet({ planes: [new ArrayBuffer(3)] }),
    packet({ planes: [new Float32Array([NaN, 1, 2, 3]).buffer] }),
    packet({ planes: [new Uint8Array(16)] }),
  ])
    throws(() => decodeAudioPacket(invalid, 1024), "Protocol");
  throws(() => decodeAudioPacket(packet(), 8), "Overflow");
});
test("media policy: processor constructor presence does not prove audio-track support", () =>
  globals(
    {
      MediaStreamTrackProcessor: class {
        constructor() {
          throw new TypeError("audio is unsupported");
        }
      },
    },
    async () => {
      const track = new FakeTrack("audio");
      const error = await failure(audioSamples(track).pipe(Stream.take(1), Stream.runCollect));
      equal(error.code, "UnsupportedCapability");
      equal(track.clones.length, 1);
      equal(track.clones[0]?.readyState, "ended");
      equal(track.readyState, "live");
    },
  ));
class Frame {
  readonly displayWidth = 2;
  readonly displayHeight = 2;
  readonly codedWidth = 2;
  readonly codedHeight = 2;
  readonly visibleRect = { x: 0, y: 0, width: 2, height: 2 };
  readonly timestamp = 12345;
  closes = 0;
  copyHook: (() => Promise<void>) | undefined;
  allocationSize(): number {
    return 16;
  }
  async copyTo(destination: Uint8Array): Promise<void> {
    destination.fill(7);
    await this.copyHook?.();
  }
  close(): void {
    this.closes++;
  }
}
const frameHost = async (
  frame: Frame,
  body: (readable: ReadableStream<Frame>) => Promise<void>,
): Promise<void> => {
  const readable = new ReadableStream<Frame>(
    {
      start(c) {
        c.enqueue(frame);
      },
    },
    { highWaterMark: 0 },
  );
  await globals(
    {
      VideoFrame: Frame,
      MediaStreamTrackProcessor: class {
        readonly readable = readable;
      },
    },
    () => body(readable),
  );
};
test("media policy: copied video closes native samples and releases cloned track and reader", async () => {
  const frame = new Frame(),
    source = new FakeTrack("video");
  await frameHost(frame, async (readable) => {
    const samples = await run(videoFrames(source).pipe(Stream.take(1), Stream.runCollect));
    equal(samples[0]?.clock, "video-track-processor");
    equal(samples[0]?.timestampUs, 12345);
    equal(samples[0]?.data.length, 16);
    equal(frame.closes, 1);
    equal(source.clones[0]?.readyState, "ended");
    equal(source.readyState, "live");
    equal(readable.locked, false);
  });
});
test("media policy: video overflow closes the rejected native sample and reader", async () => {
  const frame = new Frame(),
    source = new FakeTrack("video");
  await frameHost(frame, async (readable) => {
    equal(
      (await failure(videoFrames(source, { maxSampleBytes: 8 }).pipe(Stream.runDrain))).code,
      "Overflow",
    );
    await eventually(() => frame.closes === 1);
    equal(readable.locked, false);
    equal(source.clones[0]?.readyState, "ended");
  });
});
test("media policy: interrupted asynchronous video copy cannot enqueue late or retain its clone", async () => {
  const frame = new Frame(),
    source = new FakeTrack("video");
  let started = false,
    finish: (() => void) | undefined;
  frame.copyHook = () => {
    started = true;
    return new Promise((resolve) => {
      finish = resolve;
    });
  };
  await frameHost(frame, async (readable) => {
    let delivered = 0;
    const task = Effect.runFork(
      videoFrames(source, { readTimeoutMs: 5000 }).pipe(
        Stream.runForEach(() =>
          Effect.sync(() => {
            delivered++;
          }),
        ),
      ),
    );
    await eventually(() => started);
    await Effect.runPromise(Fiber.interrupt(task));
    await eventually(() => frame.closes === 1);
    equal(readable.locked, false);
    equal(source.clones[0]?.readyState, "ended");
    finish?.();
    await pause();
    equal(delivered, 0);
    equal(frame.closes, 1);
  });
});
class Node {
  disconnected = false;
  connect(_node: unknown): void {}
  disconnect(): void {
    this.disconnected = true;
  }
}
interface HostOptions {
  module?: () => Promise<void>;
  message?: () => unknown;
  reply?: boolean;
  resume?: () => Promise<void>;
  play?: () => Promise<void>;
  createSink?: () => void;
  input?: () => void;
}
class ModelAudioElement {
  srcObject: MediaProvider | null = null;
  sourceAtPlay: MediaProvider | null = null;
  muted = false;
  pauses = 0;
  plays = 0;
  constructor(private readonly options: HostOptions) {}
  play(): Promise<void> {
    this.plays++;
    this.sourceAtPlay = this.srcObject;
    assert(
      this.muted && this.srcObject !== null,
      "PCM must attach a MUTED owned stream before play",
    );
    return this.options.play?.() ?? Promise.resolve();
  }
  pause(): void {
    this.pauses++;
  }
}
const withAudioHost = async (
  options: HostOptions,
  body: (
    context: AudioContext,
    model: ModelContext,
    nodes: ModelWorklet[],
    sinks: ModelAudioElement[],
  ) => Promise<void>,
): Promise<void> => {
  const contexts: ModelContext[] = [],
    nodes: ModelWorklet[] = [],
    sinks: ModelAudioElement[] = [];
  const interval = globalThis.setInterval,
    clear = globalThis.clearInterval;
  const timers = new Set<ReturnType<typeof setInterval> | number>();
  class Context extends ModelContext {
    constructor() {
      super(options);
      contexts.push(this);
    }
  }
  class Worklet extends ModelWorklet {
    constructor() {
      super(options);
      nodes.push(this);
    }
  }
  await globals(
    {
      AudioContext: Context,
      AudioWorkletNode: Worklet,
      MediaStream: class {
        constructor(readonly tracks: MediaStreamTrack[]) {}
      },
      document: {
        createElement(tag: unknown) {
          equal(tag, "audio");
          options.createSink?.();
          const sink = new ModelAudioElement(options);
          sinks.push(sink);
          return sink;
        },
      },
      setInterval: (handler: TimerHandler, ms?: number) => {
        const id = interval(handler, ms);
        timers.add(id);
        return id;
      },
      clearInterval: (id?: ReturnType<typeof setInterval> | number) => {
        if (id !== undefined) timers.delete(id);
        clear(id);
      },
    },
    async () => {
      try {
        await run(
          Effect.scoped(
            Effect.gen(function* () {
              const context = yield* audioContext({ timeoutMs: 100 });
              const model = contexts[0];
              assert(model !== undefined);
              yield* Effect.tryPromise({
                try: () => body(context, model, nodes, sinks),
                catch: errorOf,
              });
            }),
          ),
        );
      } finally {
        for (const node of nodes) node.shutdown();
        for (const timer of timers) clear(timer);
      }
      assert(contexts.every((c) => c.state === "closed" && c.closed === 1));
      assert(
        sinks.every((sink) => sink.srcObject === null && sink.pauses === 1),
        "owned PCM sink must detach exactly once",
      );
      equal(timers.size, 0);
    },
  );
};
class ModelContext extends EventTarget {
  state: AudioContextState = "suspended";
  readonly sampleRate = 48000;
  readonly destination = new Node();
  readonly inputs: Node[] = [];
  readonly streams: unknown[] = [];
  loads = 0;
  closed = 0;
  listeners = 0;
  readonly audioWorklet: { addModule(url: string, options: WorkletOptions): Promise<void> };
  constructor(private readonly options: HostOptions) {
    super();
    this.audioWorklet = {
      addModule: async (url, request) => {
        equal(url, "https://local.test/pcm-worklet.js");
        equal(request.credentials, "omit");
        this.loads++;
        await options.module?.();
      },
    };
  }
  async resume(): Promise<void> {
    await this.options.resume?.();
    this.state = "running";
  }
  close(): Promise<void> {
    this.closed++;
    this.state = "closed";
    return Promise.resolve();
  }
  createMediaStreamSource(stream: unknown): Node {
    this.options.input?.();
    const node = new Node();
    this.inputs.push(node);
    this.streams.push(stream);
    return node;
  }
  override addEventListener(
    type: string,
    callback: EventListenerOrEventListenerObject | null,
    options?: boolean | AddEventListenerOptions,
  ): void {
    this.listeners++;
    super.addEventListener(type, callback, options);
  }
  override removeEventListener(
    type: string,
    callback: EventListenerOrEventListenerObject | null,
    options?: boolean | EventListenerOptions,
  ): void {
    this.listeners--;
    super.removeEventListener(type, callback, options);
  }
}
class ModelWorklet extends Node {
  readonly channel = new MessageChannel();
  readonly port = this.channel.port1;
  onprocessorerror: (() => void) | null = null;
  pulls = 0;
  stops = 0;
  constructor(options: HostOptions) {
    super();
    this.channel.port2.onmessage = (event: MessageEvent<unknown>) => {
      if (event.data === "pull") {
        this.pulls++;
        if (options.reply !== false)
          this.channel.port2.postMessage(options.message?.() ?? packet());
      }
      if (event.data === "stop") this.stops++;
    };
  }
  shutdown(): void {
    this.channel.port1.close();
    this.channel.port2.close();
    this.channel.port2.onmessage = null;
  }
}
const pcmOptions = (context: AudioContext) => ({
  context,
  workletUrl: "https://local.test/pcm-worklet.js",
  readTimeoutMs: 100,
});
test("Web Audio policy: explicit PCM path copies samples and owns graph, clones, port and listeners", () =>
  withAudioHost({}, async (context, model, nodes) => {
    const source = new FakeTrack("audio");
    const samples = await run(
      webAudioSamples(source, pcmOptions(context)).pipe(Stream.take(3), Stream.runCollect),
    );
    equal(samples.length, 3);
    equal(samples[0]?.clock, "audio-context-render");
    equal(source.readyState, "live");
    assert(source.clones.every((t) => t.readyState === "ended"));
    equal(model.listeners, 0);
    assert(model.inputs.every((n) => n.disconnected));
    const node = nodes[0];
    assert(node !== undefined);
    assert(
      node.disconnected &&
        node.port.onmessage === null &&
        node.port.onmessageerror === null &&
        node.onprocessorerror === null,
    );
    equal(model.state, "running"); // borrowed context is not closed by the PCM stream.
  }));
test("Web Audio policy: sample overflow is a typed failure and closes the graph", () =>
  withAudioHost({}, async (context, model, nodes) => {
    const source = new FakeTrack("audio");
    equal(
      (
        await failure(
          webAudioSamples(source, { ...pcmOptions(context), maxSampleBytes: 8 }).pipe(
            Stream.runDrain,
          ),
        )
      ).code,
      "Overflow",
    );
    equal(source.clones[0]?.readyState, "ended");
    equal(model.listeners, 0);
    assert(nodes.every((n) => n.disconnected));
  }));
test("Web Audio policy: missing render blocks time out and remove all callbacks", () =>
  withAudioHost({ reply: false }, async (context, model, nodes) => {
    const source = new FakeTrack("audio");
    equal(
      (
        await failure(
          webAudioSamples(source, { ...pcmOptions(context), readTimeoutMs: 20 }).pipe(
            Stream.runDrain,
          ),
        )
      ).code,
      "Timeout",
    );
    equal(source.clones[0]?.readyState, "ended");
    equal(model.listeners, 0);
    assert(nodes.every((n) => n.port.onmessage === null));
  }));
test("Web Audio policy: interruption during module load prevents a late graph", async () => {
  let finish: (() => void) | undefined;
  await withAudioHost(
    {
      module: () =>
        new Promise((resolve) => {
          finish = resolve;
        }),
    },
    async (context, model, nodes) => {
      const source = new FakeTrack("audio");
      const task = Effect.runFork(
        webAudioSamples(source, pcmOptions(context)).pipe(Stream.runDrain),
      );
      await eventually(() => model.loads === 1);
      await Effect.runPromise(Fiber.interrupt(task));
      equal(source.clones[0]?.readyState, "ended");
      finish?.();
      await pause();
      equal(nodes.length, 0);
      equal(model.inputs.length, 0);
    },
  );
});
test("Web Audio policy: stream interruption removes a pending PCM read and releases its clone", () =>
  withAudioHost({ reply: false }, async (context, model, nodes) => {
    const source = new FakeTrack("audio");
    const task = Effect.runFork(
      webAudioSamples(source, { ...pcmOptions(context), readTimeoutMs: 5000 }).pipe(
        Stream.runDrain,
      ),
    );
    await eventually(() => (nodes[0]?.pulls ?? 0) > 0);
    await Effect.runPromise(Fiber.interrupt(task));
    equal(source.clones[0]?.readyState, "ended");
    equal(model.listeners, 0);
    assert(nodes.every((n) => n.disconnected && n.port.onmessage === null));
  }));
test("Web Audio policy: context suspension fails PCM instead of manufacturing timestamps", () =>
  withAudioHost({ reply: false }, async (context, model, nodes) => {
    const source = new FakeTrack("audio");
    const result = failure(webAudioSamples(source, pcmOptions(context)).pipe(Stream.runDrain));
    await eventually(() => (nodes[0]?.pulls ?? 0) > 0);
    model.state = "suspended";
    model.dispatchEvent(new Event("statechange"));
    equal((await result).code, "Disconnected");
    equal(source.clones[0]?.readyState, "ended");
    equal(model.listeners, 0);
  }));
test("Web Audio policy: worklet registration is cached per borrowed context and module", () =>
  withAudioHost({}, async (context, model) => {
    for (let i = 0; i < 2; i++)
      await run(
        webAudioSamples(new FakeTrack("audio"), pcmOptions(context)).pipe(
          Stream.take(1),
          Stream.runDrain,
        ),
      );
    equal(model.loads, 1);
    equal(model.listeners, 0);
  }));
test("Web Audio policy: no data/blob worklet or stopped source silently creates a graph", () =>
  withAudioHost({}, async (context, model) => {
    const source = new FakeTrack("audio");
    equal(
      (
        await failure(
          webAudioSamples(source, {
            ...pcmOptions(context),
            workletUrl: "data:text/javascript,0",
          }).pipe(Stream.runDrain),
        )
      ).code,
      "Protocol",
    );
    source.stop();
    equal(
      (await failure(webAudioSamples(source, pcmOptions(context)).pipe(Stream.runDrain))).code,
      "InvalidState",
    );
    equal(model.inputs.length, 0);
    equal(source.clones.length, 0);
  }));
test("worklet algorithm: one credit, explicit skipped frames, silent output and stop cleanup", async () => {
  const channel = new MessageChannel();
  let registered: unknown, name: unknown;
  class Processor {
    readonly port = channel.port1;
  }
  const replies: unknown[] = [];
  channel.port2.onmessage = (event: MessageEvent<unknown>) => {
    replies.push(event.data);
  };
  await globals(
    {
      AudioWorkletProcessor: Processor,
      currentFrame: 48000,
      sampleRate: 48000,
      registerProcessor: (n: unknown, constructor: unknown) => {
        name = n;
        registered = constructor;
      },
    },
    async () => {
      try {
        await import("../src/_internal/pcm-worklet.js");
        equal(name, "reactor-pcm-tap-v1");
        assert(typeof registered === "function");
        const instance: unknown = Reflect.construct(registered, []);
        assert(
          typeof instance === "object" &&
            instance !== null &&
            "process" in instance &&
            typeof instance.process === "function",
        );
        const process = instance.process;
        const processBlock = (): unknown =>
          Reflect.apply(process, instance, [
            [[new Float32Array([0.1, -0.1, 0.2, -0.2])]],
            [[output]],
          ]);
        const output = new Float32Array([1, 1, 1, 1]);
        equal(processBlock(), true);
        equal([...output], [0, 0, 0, 0]);
        equal(replies.length, 0);
        channel.port2.postMessage("pull");
        await pause();
        equal(processBlock(), true);
        await eventually(() => replies.length === 1);
        const sample = decodeAudioPacket(replies[0], 1024);
        equal(sample.skippedFrames, 4);
        equal(sample.clock, "audio-context-render");
        for (let i = 0; i < 5; i++) processBlock();
        await pause();
        equal(replies.length, 1);
        channel.port2.postMessage("stop");
        await pause();
        equal(processBlock(), false);
        equal(channel.port1.onmessage, null);
      } finally {
        channel.port1.close();
        channel.port2.close();
        channel.port2.onmessage = null;
      }
    },
  );
});

test("Web Audio policy: a slow consumer does not accumulate an unbounded PCM message history", () =>
  withAudioHost({}, async (context, _model, nodes) => {
    const task = Effect.runFork(
      webAudioSamples(new FakeTrack("audio"), pcmOptions(context)).pipe(
        Stream.runForEach(() => Effect.never),
      ),
    );
    await eventually(() => (nodes[0]?.pulls ?? 0) >= 1);
    await pause(25);
    assert((nodes[0]?.pulls ?? 0) <= 2, "PCM exceeded one consumer block plus one queued block");
    await Effect.runPromise(Fiber.interrupt(task));
  }));
test("Web Audio policy: malformed transferred packets close the graph instead of returning PCM", () =>
  withAudioHost(
    { message: () => packet({ planes: [new ArrayBuffer(2)] }) },
    async (context, model, nodes) => {
      equal(
        (
          await failure(
            webAudioSamples(new FakeTrack("audio"), pcmOptions(context)).pipe(Stream.runDrain),
          )
        ).code,
        "Protocol",
      );
      equal(model.listeners, 0);
      assert(nodes.every((node) => node.disconnected));
    },
  ));
test("Web Audio policy: blocked resume remains a failure and closes the newly-owned context", async () => {
  const contexts: ModelContext[] = [];
  class Context extends ModelContext {
    constructor() {
      super({
        resume: async () => {
          throw new Error("user activation required");
        },
      });
      contexts.push(this);
    }
  }
  await globals({ AudioContext: Context, AudioWorkletNode: class {} }, async () => {
    const error = await failure(Effect.scoped(audioContext({ timeoutMs: 100 })));
    assert(error.message.includes("user activation"));
    equal(contexts[0]?.state, "closed");
    equal(contexts[0]?.closed, 1);
  });
});
class Element {
  srcObject: MediaProvider | null = null;
  pauses = 0;
  cancelled: number[] = [];
  getAttribute(_name: string): string | null {
    return null;
  }
  play(): Promise<void> {
    return new Promise(() => {});
  }
  pause(): void {
    this.pauses++;
  }
  requestVideoFrameCallback(_callback: unknown): number {
    return 42;
  }
  cancelVideoFrameCallback(id: number): void {
    this.cancelled.push(id);
  }
}
test("playback policy: a never-resolving play request has a deadline and detaches owned media", async () => {
  const elements: Element[] = [];
  class HtmlElement extends Element {
    constructor() {
      super();
      elements.push(this);
    }
  }
  await globals({ HTMLMediaElement: HtmlElement, MediaStream: class {} }, async () => {
    const source = new FakeTrack("video");
    equal((await failure(Effect.scoped(play(source, new HTMLMediaElement(), 20)))).code, "Timeout");
    equal(source.clones[0]?.readyState, "ended");
    equal(source.readyState, "live");
    equal(elements[0]?.srcObject, null);
    equal(elements[0]?.pauses, 1);
  });
});
test("playback policy: failed MediaStream acquisition does not leak its clone", () =>
  globals(
    {
      HTMLMediaElement: Element,
      MediaStream: class {
        constructor() {
          throw new Error("stream construction failed");
        }
      },
    },
    async () => {
      const source = new FakeTrack("video");
      await failure(Effect.scoped(play(source, new HTMLMediaElement(), 20)));
      equal(source.clones[0]?.readyState, "ended");
      equal(source.readyState, "live");
    },
  ));
test("presentation policy: deadline removes the browser frame callback rather than claiming a display", async () => {
  const elements: Element[] = [];
  class VideoElement extends Element {
    constructor() {
      super();
      elements.push(this);
    }
  }
  await globals({ HTMLVideoElement: VideoElement }, async () => {
    equal((await failure(nextPresentation(new HTMLVideoElement(), 20))).code, "Timeout");
    equal(elements[0]?.cancelled, [42]);
  });
});

// These models exercise activation/ownership policy. Only the separate real browser
// integration establishes that a native decoder produced changing video/non-silent audio.
test("PCM activation: the public stream owns muted playback on the SAME cloned stream as Web Audio", () =>
  withAudioHost({}, async (context, model, nodes, sinks) => {
    const source = new FakeTrack("audio");
    const pcm = await run(
      webAudioSamples(source, pcmOptions(context)).pipe(Stream.take(1), Stream.runCollect),
    );
    equal(pcm.length, 1);
    equal(sinks.length, 1);
    equal(sinks[0]?.plays, 1);
    equal(sinks[0]?.sourceAtPlay, model.streams[0]);
    equal(sinks[0]?.srcObject, null);
    equal(source.clones.length, 1);
    equal(source.clones[0]?.readyState, "ended");
    equal(source.readyState, "live");
    equal(context.state, "running");
    equal(model.listeners, 0);
    assert(nodes.every((node) => node.disconnected && node.port.onmessage === null));
  }));
test("PCM activation: a rejected play promise is typed, detaches its sink, and never opens the worklet", () =>
  withAudioHost(
    {
      play: () => Promise.reject(new DOMException("ordinary activation denied", "NotAllowedError")),
    },
    async (context, model, nodes, sinks) => {
      const source = new FakeTrack("audio");
      const error = await failure(
        webAudioSamples(source, pcmOptions(context)).pipe(Stream.runDrain),
      );
      equal(error.code, "UnsupportedCapability");
      assert(error.message.includes("activation"));
      equal(sinks[0]?.srcObject, null);
      equal(model.loads, 0);
      equal(nodes.length, 0);
      equal(model.listeners, 0);
      equal(source.readyState, "live");
      equal(source.clones[0]?.readyState, "ended");
    },
  ));
test("PCM activation: a synchronous play exception cannot leak a clone or playback attachment", () =>
  withAudioHost(
    {
      play: () => {
        throw new Error("synchronous play failure");
      },
    },
    async (context, model, _nodes, sinks) => {
      const source = new FakeTrack("audio");
      equal(
        (await failure(webAudioSamples(source, pcmOptions(context)).pipe(Stream.runDrain))).code,
        "UnsupportedCapability",
      );
      equal(sinks[0]?.pauses, 1);
      equal(source.clones[0]?.readyState, "ended");
      equal(model.loads, 0);
      equal(model.listeners, 0);
    },
  ));
test("PCM activation: play deadline is independent of the read deadline and late fulfillment cannot open a graph", async () => {
  let finish: (() => void) | undefined;
  await withAudioHost(
    {
      play: () =>
        new Promise((resolve) => {
          finish = resolve;
        }),
    },
    async (context, model, nodes, sinks) => {
      const source = new FakeTrack("audio");
      const error = await failure(
        webAudioSamples(source, {
          ...pcmOptions(context),
          activationTimeoutMs: 20,
          readTimeoutMs: 5000,
        }).pipe(Stream.runDrain),
      );
      equal(error.code, "Timeout");
      assert(error.message.includes("activation"));
      equal(sinks[0]?.srcObject, null);
      equal(source.clones[0]?.readyState, "ended");
      equal(source.readyState, "live");
      finish?.();
      await pause();
      equal(model.loads, 0);
      equal(nodes.length, 0);
      equal(model.listeners, 0);
    },
  );
});
test("PCM activation: interrupting pending play detaches immediately; a late success cannot revive the stream", async () => {
  let finish: (() => void) | undefined;
  await withAudioHost(
    {
      play: () =>
        new Promise((resolve) => {
          finish = resolve;
        }),
    },
    async (context, model, nodes, sinks) => {
      const source = new FakeTrack("audio");
      let delivered = 0;
      const fiber = Effect.runFork(
        webAudioSamples(source, pcmOptions(context)).pipe(
          Stream.runForEach(() =>
            Effect.sync(() => {
              delivered++;
            }),
          ),
        ),
      );
      await eventually(() => sinks[0]?.plays === 1);
      await Effect.runPromise(Fiber.interrupt(fiber));
      equal(sinks[0]?.srcObject, null);
      equal(source.clones[0]?.readyState, "ended");
      equal(model.listeners, 0);
      finish?.();
      await pause();
      equal(delivered, 0);
      equal(nodes.length, 0);
      equal(model.loads, 0);
    },
  );
});
test("PCM activation: missing Window document fails before allocating a clone or worklet", () =>
  withAudioHost({}, async (context, model) => {
    await globals({ document: undefined }, async () => {
      const source = new FakeTrack("audio");
      equal(
        (await failure(webAudioSamples(source, pcmOptions(context)).pipe(Stream.runDrain))).code,
        "UnsupportedCapability",
      );
      equal(source.clones.length, 0);
      equal(model.loads, 0);
      equal(model.listeners, 0);
    });
  }));
test("PCM activation: invalid timeout is rejected without creating a sink", () =>
  withAudioHost({}, async (context, model, _nodes, sinks) => {
    const source = new FakeTrack("audio");
    for (const activationTimeoutMs of [0, -1, 0.5, 60001, NaN])
      equal(
        (
          await failure(
            webAudioSamples(source, { ...pcmOptions(context), activationTimeoutMs }).pipe(
              Stream.runDrain,
            ),
          )
        ).code,
        "InvalidInput",
      );
    equal(sinks.length, 0);
    equal(source.clones.length, 0);
    equal(model.loads, 0);
  }));
test("PCM activation: failed element acquisition still stops the clone and removes liveness resources", () =>
  withAudioHost(
    {
      createSink: () => {
        throw new Error("element acquisition failed");
      },
    },
    async (context, model) => {
      const source = new FakeTrack("audio");
      await failure(webAudioSamples(source, pcmOptions(context)).pipe(Stream.runDrain));
      equal(source.clones[0]?.readyState, "ended");
      equal(source.readyState, "live");
      equal(model.listeners, 0);
    },
  ));
test("PCM activation: a graph acquisition failure unwinds successful playback", () =>
  withAudioHost(
    {
      input: () => {
        throw new Error("media source refused");
      },
    },
    async (context, model, nodes, sinks) => {
      const source = new FakeTrack("audio");
      await failure(webAudioSamples(source, pcmOptions(context)).pipe(Stream.runDrain));
      equal(sinks[0]?.plays, 1);
      equal(sinks[0]?.srcObject, null);
      equal(nodes.length, 0);
      equal(model.listeners, 0);
      equal(source.clones[0]?.readyState, "ended");
    },
  ));
test("PCM activation: ending the borrowed lease during pending play cancels without relying on ended dispatch", async () => {
  let finish: (() => void) | undefined;
  await withAudioHost(
    {
      play: () =>
        new Promise((resolve) => {
          finish = resolve;
        }),
    },
    async (context, model, nodes, sinks) => {
      const source = new FakeTrack("audio"),
        result = failure(webAudioSamples(source, pcmOptions(context)).pipe(Stream.runDrain));
      await eventually(() => sinks[0]?.plays === 1);
      source.stop(); // Native stop() also omits ended.
      equal((await result).code, "Disconnected");
      equal(source.clones[0]?.readyState, "ended");
      equal(sinks[0]?.srcObject, null);
      finish?.();
      await pause();
      equal(nodes.length, 0);
      equal(model.loads, 0);
      equal(model.listeners, 0);
    },
  );
});
test("PCM activation: source ended event fails an active read and releases playback", () =>
  withAudioHost({ reply: false }, async (context, model, nodes, sinks) => {
    const source = new FakeTrack("audio");
    const result = failure(webAudioSamples(source, pcmOptions(context)).pipe(Stream.runDrain));
    await eventually(() => (nodes[0]?.pulls ?? 0) > 0);
    source.stop();
    source.dispatchEvent(new Event("ended"));
    equal((await result).code, "Disconnected");
    equal(sinks[0]?.srcObject, null);
    equal(model.listeners, 0);
  }));
test("PCM activation: context suspension during pending play cleans the sink and fences late completion", async () => {
  let finish: (() => void) | undefined;
  await withAudioHost(
    {
      play: () =>
        new Promise((resolve) => {
          finish = resolve;
        }),
    },
    async (context, model, nodes, sinks) => {
      const result = failure(
        webAudioSamples(new FakeTrack("audio"), pcmOptions(context)).pipe(Stream.runDrain),
      );
      await eventually(() => sinks[0]?.plays === 1);
      model.state = "suspended";
      model.dispatchEvent(new Event("statechange"));
      equal((await result).code, "Disconnected");
      equal(sinks[0]?.srcObject, null);
      equal(model.listeners, 0);
      finish?.();
      await pause();
      equal(nodes.length, 0);
    },
  );
});
test("PCM reconnect policy: stopping an old lease retires its reader and sink; a new lease opens fresh on the same context", () =>
  withAudioHost({}, async (context, model, nodes, sinks) => {
    const old = new FakeTrack("audio");
    let oldBlocks = 0;
    const result = failure(
      webAudioSamples(old, pcmOptions(context)).pipe(
        Stream.runForEach(() =>
          Effect.sync(() => {
            oldBlocks++;
          }),
        ),
      ),
    );
    await eventually(() => oldBlocks > 0);
    old.stop();
    equal((await result).code, "Disconnected");
    const atClose = oldBlocks;
    equal(sinks[0]?.srcObject, null);
    equal(old.clones[0]?.readyState, "ended");
    const next = new FakeTrack("audio");
    equal(
      (
        await run(
          webAudioSamples(next, pcmOptions(context)).pipe(Stream.take(2), Stream.runCollect),
        )
      ).length,
      2,
    );
    await pause();
    equal(oldBlocks, atClose);
    equal(sinks.length, 2);
    assert(sinks[0] !== sinks[1]);
    equal(model.loads, 1);
    equal(model.listeners, 0);
    equal(next.readyState, "live");
    assert(nodes.every((node) => node.disconnected && node.port.onmessage === null));
  }));
test("PCM reconnect policy: lease stop releases activation even when a downstream consumer never asks for another block", () =>
  withAudioHost({}, async (context, model, nodes, sinks) => {
    const source = new FakeTrack("audio");
    let downstream = false;
    const fiber = Effect.runFork(
      webAudioSamples(source, pcmOptions(context)).pipe(
        Stream.runForEach(() => {
          downstream = true;
          return Effect.never;
        }),
      ),
    );
    await eventually(() => downstream);
    source.stop();
    await eventually(() => sinks[0]?.srcObject === null);
    equal(source.clones[0]?.readyState, "ended");
    equal(model.listeners, 0);
    assert(nodes.every((node) => node.disconnected));
    await Effect.runPromise(Fiber.interrupt(fiber));
  }));
