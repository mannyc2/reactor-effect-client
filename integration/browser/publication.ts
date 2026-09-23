/** Development-only native receiver for tracks received by the INDEPENDENT peer.
 * No Reactor/client/media-helper/codec imports. Never reads the publisher canvas.
 * One scoped owner, bounded observations. Not a portable media backend. */
import { assert, bounded, delay, hash, rms, until } from "./util.js";

export interface PublishedFrame {
  readonly sequence: number;
  readonly mediaTimeSeconds: number;
  readonly callbackTimeMs: number;
  readonly presentedFrames: number;
  readonly width: number;
  readonly height: number;
  readonly pixelHash: string;
}
export interface PublishedAudio {
  readonly sequence: number;
  readonly contextObservationSeconds: number;
  readonly rms: number;
}
export interface PublicationMark {
  readonly frames: number;
  readonly audio: number;
}
export interface PublishedMedia {
  readonly input: "independent RTCRtpReceiver tracks through owned native playback";
  readonly video: {
    readonly clock: "media-element-timeline";
    readonly callbackClock: "performance-time-origin";
    readonly distinctHashes: number;
    readonly frames: readonly PublishedFrame[];
  };
  readonly audio: {
    readonly clock: "audio-context-observation";
    readonly maximumWindowRms: number;
    readonly sampleRate: number;
    readonly windowFrames: number;
    readonly windows: readonly PublishedAudio[];
  };
  readonly timestamps: string;
  readonly ownership: string;
}
export interface PublicationOptions {
  readonly activationTimeoutMs?: number;
  readonly preview?: HTMLElement;
}
/** This fixture is 160x96 @20fps, 440Hz audio. Bounds are deliberate assertion limits,
 * not the public client's media limits. No raw buffers are retained between callbacks. */
export class PublicationReceiver {
  private readonly cancel = new AbortController();
  private readonly clones: MediaStreamTrack[] = [];
  private readonly sinks: HTMLMediaElement[] = [];
  private readonly nodes: AudioNode[] = [];
  private readonly listeners: (() => void)[] = [];
  private readonly frames: PublishedFrame[] = [];
  private readonly windows: PublishedAudio[] = [];
  private readonly observationLimit = 256;
  private frameCount = 0;
  private audioCount = 0;
  private frameCallback: number | undefined;
  private interval: ReturnType<typeof setInterval> | undefined;
  private video: HTMLVideoElement | undefined;
  private canvas: HTMLCanvasElement | undefined;
  private pixels: CanvasRenderingContext2D | undefined;
  private analyser: AnalyserNode | undefined;
  private fault: Error | undefined;
  private closed = false;
  private activated = false;
  private operations = 0;
  private readonly cleanupErrors: string[] = [];
  private constructor(
    private readonly tracks: { readonly video: MediaStreamTrack; readonly audio: MediaStreamTrack },
    private readonly context: AudioContext,
    private readonly peer: RTCPeerConnection,
  ) {}

  static async open(
    tracks: { readonly video: MediaStreamTrack; readonly audio: MediaStreamTrack },
    context: AudioContext,
    peer: RTCPeerConnection,
    signal: AbortSignal,
    options: PublicationOptions = {},
  ): Promise<PublicationReceiver> {
    const self = new PublicationReceiver(tracks, context, peer);
    try {
      await self.activate(signal, options);
      return self;
    } catch (error) {
      self.close();
      throw error;
    }
  }
  private listen(target: EventTarget, name: string, body: () => void): void {
    target.addEventListener(name, body);
    this.listeners.push(() => target.removeEventListener(name, body));
  }
  private check(): void {
    if (this.fault !== undefined) throw this.fault;
    assert(!this.closed, "publication receiver is closed");
    assert(this.context.state === "running", "publication receiver AudioContext stopped");
    assert(this.peer.connectionState === "connected", "publication receiver peer is not connected");
    assert(
      this.tracks.audio.readyState === "live" && this.tracks.video.readyState === "live",
      "borrowed receiver track ended; not unpublication evidence",
    );
    assert(
      this.clones.every((t) => t.readyState === "live" && t.enabled),
      "publication observer's own clone stopped/disabled",
    );
    if (this.activated)
      assert(
        this.sinks.every((s) => !s.paused && s.srcObject !== null),
        "publication observer's own playback stopped; not remote retirement",
      );
  }
  private fail(error: unknown): void {
    this.fault ??= error instanceof Error ? error : new Error(String(error));
    this.close();
  }
  private async activate(signal: AbortSignal, options: PublicationOptions): Promise<void> {
    const timeout = options.activationTimeoutMs ?? 5000;
    assert(
      Number.isSafeInteger(timeout) && timeout > 0 && timeout <= 10000,
      "invalid publication activation deadline",
    );
    assert(!signal.aborted, "publication activation interrupted");
    this.check();
    assert(
      this.tracks.video.kind === "video" && this.tracks.audio.kind === "audio",
      "incorrect publication track kinds",
    );
    const video = document.createElement("video");
    this.video = video;
    this.sinks.push(video);
    const audio = document.createElement("audio");
    this.sinks.push(audio);
    assert(
      typeof video.requestVideoFrameCallback === "function" &&
        typeof video.cancelVideoFrameCallback === "function",
      "independent publication readback requires native requestVideoFrameCallback",
    );
    const canvas = document.createElement("canvas");
    this.canvas = canvas;
    const pixels = canvas.getContext("2d", { willReadFrequently: true });
    assert(pixels !== null, "publication Canvas 2D unavailable");
    this.pixels = pixels;
    // Borrow the RTCRtpReceiver tracks. Own independent clones: observing or interrupting
    // this fixture must not stop the peer's receiver, the publisher or its source.
    const vt = this.tracks.video.clone();
    this.clones.push(vt);
    const at = this.tracks.audio.clone();
    this.clones.push(at);
    video.muted = true;
    video.playsInline = true;
    video.autoplay = false;
    video.setAttribute("aria-label", "Independent peer: decoded published video");
    video.srcObject = new MediaStream([vt]);
    const stream = new MediaStream([at]);
    audio.muted = true;
    audio.srcObject = stream;
    const input = this.context.createMediaStreamSource(stream);
    this.nodes.push(input);
    const analyser = this.context.createAnalyser();
    this.nodes.push(analyser);
    this.analyser = analyser;
    analyser.fftSize = 2048;
    const gain = this.context.createGain();
    this.nodes.push(gain);
    gain.gain.value = 0;
    input.connect(analyser);
    analyser.connect(gain);
    gain.connect(this.context.destination);
    this.listen(video, "error", () =>
      this.fail(new Error(`published video playback error: ${video.error?.message ?? "unknown"}`)),
    );
    this.listen(audio, "error", () =>
      this.fail(new Error(`published audio playback error: ${audio.error?.message ?? "unknown"}`)),
    );
    this.listen(this.context, "statechange", () => {
      if (this.context.state !== "running")
        this.fail(new Error("publication receiver AudioContext stopped"));
    });
    this.listen(this.peer, "connectionstatechange", () => {
      if (this.peer.connectionState !== "connected")
        this.fail(new Error("publication peer retired"));
    });
    options.preview?.append(video);
    const combined = AbortSignal.any([signal, this.cancel.signal]);
    // Both ordinary play() calls are observed. No autoplay-policy change, fabricated
    // sample or separately decoded file can make this activation/receiver pass.
    await bounded(
      Promise.all([video.play(), audio.play()]),
      timeout,
      "activate independent published video/audio",
      combined,
    );
    assert(!combined.aborted, "publication activation interrupted");
    this.check();
    this.activated = true;
    this.frameCallback = video.requestVideoFrameCallback(this.onFrame);
    const samples = new Float32Array(analyser.fftSize);
    this.interval = setInterval(() => {
      if (this.closed) return;
      try {
        this.check();
        analyser.getFloatTimeDomainData(samples);
        assert(samples.every(Number.isFinite), "nonfinite received published audio");
        const time = this.context.currentTime;
        assert(Number.isFinite(time) && time >= 0, "invalid publication audio observation clock");
        const previous = this.windows.at(-1);
        assert(
          previous === undefined || time >= previous.contextObservationSeconds,
          "publication audio observation clock regressed",
        );
        if (this.windows.length === this.observationLimit) this.windows.shift();
        this.windows.push({
          sequence: ++this.audioCount,
          contextObservationSeconds: time,
          rms: rms(samples),
        });
      } catch (error) {
        this.fail(error);
      }
    }, 50);
  }
  private readonly onFrame: VideoFrameRequestCallback = (at, metadata) => {
    this.frameCallback = undefined;
    if (this.closed) return;
    try {
      this.check();
      const video = this.video,
        canvas = this.canvas,
        pixels = this.pixels;
      assert(
        video !== undefined && canvas !== undefined && pixels !== undefined,
        "missing publication readback resources",
      );
      assert(
        video.videoWidth === 160 && video.videoHeight === 96,
        "unexpected published fixture video dimensions",
      );
      assert(
        Number.isFinite(metadata.mediaTime) && metadata.mediaTime >= 0 && Number.isFinite(at),
        "invalid publication video timestamp",
      );
      assert(
        Number.isSafeInteger(metadata.presentedFrames) && metadata.presentedFrames > 0,
        "invalid presented-frame counter",
      );
      const last = this.frames.at(-1);
      assert(
        last === undefined || metadata.mediaTime > last.mediaTimeSeconds,
        "published video media timeline did not advance",
      );
      canvas.width = video.videoWidth;
      canvas.height = video.videoHeight;
      pixels.drawImage(video, 0, 0);
      const image = pixels.getImageData(0, 0, canvas.width, canvas.height);
      assert(image.data.length === 160 * 96 * 4, "published decoded RGBA extent mismatch");
      if (this.frames.length === this.observationLimit) this.frames.shift();
      this.frames.push({
        sequence: ++this.frameCount,
        mediaTimeSeconds: metadata.mediaTime,
        callbackTimeMs: at,
        presentedFrames: metadata.presentedFrames,
        width: canvas.width,
        height: canvas.height,
        pixelHash: hash(image.data),
      });
      this.frameCallback = video.requestVideoFrameCallback(this.onFrame);
    } catch (error) {
      this.fail(error);
    }
  };
  mark(): PublicationMark {
    this.check();
    return { frames: this.frameCount, audio: this.audioCount };
  }
  private since(mark: PublicationMark): { frames: PublishedFrame[]; windows: PublishedAudio[] } {
    assert(
      this.frameCount - mark.frames <= this.observationLimit &&
        this.audioCount - mark.audio <= this.observationLimit,
      "publication observation retention bound exceeded (256 per kind)",
    );
    return {
      frames: this.frames.filter((f) => f.sequence > mark.frames),
      windows: this.windows.filter((w) => w.sequence > mark.audio),
    };
  }
  async changing(signal: AbortSignal, timeoutMs = 8000): Promise<PublishedMedia> {
    assert(
      Number.isSafeInteger(timeoutMs) && timeoutMs > 0 && timeoutMs <= 10000,
      "invalid publication observation deadline",
    );
    const mark = this.mark(),
      end = performance.now() + timeoutMs;
    this.operations++;
    try {
      for (;;) {
        assert(!signal.aborted, "published observation interrupted");
        this.check();
        const { frames, windows } = this.since(mark),
          hashes = new Set(frames.map((f) => f.pixelHash)),
          energy = Math.max(0, ...windows.map((w) => w.rms));
        if (frames.length >= 12 && hashes.size >= 3 && windows.length >= 12 && energy > 0.001)
          return {
            input: "independent RTCRtpReceiver tracks through owned native playback",
            video: {
              clock: "media-element-timeline",
              callbackClock: "performance-time-origin",
              distinctHashes: hashes.size,
              frames,
            },
            audio: {
              clock: "audio-context-observation",
              maximumWindowRms: energy,
              sampleRate: this.context.sampleRate,
              windowFrames: this.analyser?.fftSize ?? 0,
              windows,
            },
            timestamps:
              "mediaTime is the receiving HTML video's media timeline, not capture/RTP time. Audio context time is the analyser query time; overlapping windows have no sample PTS. No A/V synchronization or human display assertion.",
            ownership:
              "Only hashes and scalar observations are retained, at most 256 of each. RGBA ImageData and audio query buffers are not native sample handles. Scope releases cloned receiver tracks, sinks, callbacks, listeners and nodes; borrowed receiver tracks/context stay live. No VideoFrame/AudioData, stream reader or object URL is acquired.",
          };
        assert(
          performance.now() < end,
          `published decoded media deadline: frames=${frames.length}, hashes=${hashes.size}, audioWindows=${windows.length}, rms=${energy}`,
        );
        await delay(25, AbortSignal.any([signal, this.cancel.signal]));
      }
    } catch (error) {
      throw this.fault ?? error;
    } finally {
      this.operations--;
    }
  }
  /** Finite quiescence, NOT permanent remote termination. The SAME activated receiver
   * continues observing; neither fixture/oracle disables nor stops its input tracks. */
  async quiet(
    signal: AbortSignal,
    options: { readonly timeoutMs?: number; readonly quietMs?: number } = {},
  ): Promise<object> {
    const timeout = options.timeoutMs ?? 6500,
      quiet = options.quietMs ?? 1200;
    assert(
      Number.isSafeInteger(timeout) &&
        timeout > 0 &&
        timeout <= 10000 &&
        Number.isSafeInteger(quiet) &&
        quiet >= 300 &&
        quiet < timeout,
      "invalid publication quiet-window deadlines",
    );
    const end = performance.now() + timeout;
    let mark = this.mark(),
      start = performance.now(),
      contextAt = this.context.currentTime;
    this.operations++;
    try {
      for (;;) {
        assert(!signal.aborted, "publication retirement observation interrupted");
        this.check();
        const { frames, windows } = this.since(mark);
        const energy = Math.max(0, ...windows.map((w) => w.rms));
        if (frames.length > 0 || energy > 0.001) {
          mark = this.mark();
          start = performance.now();
          contextAt = this.context.currentTime;
        } else if (
          performance.now() - start >= quiet &&
          windows.length >= 6 &&
          this.context.currentTime - contextAt >= quiet / 2000
        )
          return {
            outcome:
              "no new decoded presentation callbacks and only silent decoded audio windows during a bounded live-receiver interval",
            quietDurationMs: performance.now() - start,
            newVideoCallbacks: frames.length,
            maximumWindowRms: energy,
            audioWindows: windows,
            audioClock: "audio-context-observation",
            contextAdvanceSeconds: this.context.currentTime - contextAt,
            receiverVideoState: this.tracks.video.readyState,
            receiverVideoMuted: this.tracks.video.muted,
            receiverAudioState: this.tracks.audio.readyState,
            receiverAudioMuted: this.tracks.audio.muted,
            peerState: this.peer.connectionState,
            sinksStillPlaying: this.sinks.every((s) => !s.paused),
            limitation:
              "Quiescence in this interval, not a wire acknowledgment, permanently ended track, stopped inference or billing. Last picture may remain visible. Re-publication must resume changing decoded media on this same receiver.",
          };
        assert(
          performance.now() < end,
          `publication retirement deadline: frames=${frames.length}, audioWindows=${windows.length}, rms=${energy}`,
        );
        await delay(25, AbortSignal.any([signal, this.cancel.signal]));
      }
    } catch (error) {
      throw this.fault ?? error;
    } finally {
      this.operations--;
    }
  }
  snapshot(): object {
    return {
      closed: this.closed,
      activated: this.activated,
      operations: this.operations,
      frames: this.frameCount,
      audioWindows: this.audioCount,
      retainedFrames: this.frames.length,
      retainedAudioWindows: this.windows.length,
      capacityPerKind: this.observationLimit,
      clones: this.clones.map((t) => ({ kind: t.kind, readyState: t.readyState })),
      borrowed: {
        video: this.tracks.video.readyState,
        audio: this.tracks.audio.readyState,
        context: this.context.state,
      },
      sinks: this.sinks.map((s) => ({
        paused: s.paused,
        detached: s.srcObject === null && !s.getAttribute("src"),
        removed: !s.isConnected,
      })),
      frameCallbackPending: this.frameCallback !== undefined,
      timerPending: this.interval !== undefined,
      listeners: this.listeners.length,
      cleanupErrors: [...this.cleanupErrors],
      fault: this.fault?.message,
    };
  }
  async dispose(): Promise<void> {
    this.close();
    // Effect cancellation aborts a JS waiter but cannot synchronously join its Promise.
    // Bound and join our observation continuations before declaring fixture cleanup.
    await until(() => this.operations === 0, 1500, "publication observation cleanup join");
    assert(
      this.cleanupErrors.length === 0,
      "publication receiver cleanup errors: " + this.cleanupErrors.join("; "),
    );
  }
  close(): void {
    if (this.closed) return;
    this.closed = true;
    this.cancel.abort();
    const releases: (() => void)[] = [
      () => {
        if (this.frameCallback !== undefined)
          this.video?.cancelVideoFrameCallback(this.frameCallback);
        this.frameCallback = undefined;
      },
      () => {
        if (this.interval !== undefined) clearInterval(this.interval);
        this.interval = undefined;
      },
      ...this.listeners.splice(0),
      ...this.sinks.flatMap((s) => [
        () => s.pause(),
        () => {
          s.srcObject = null;
          s.removeAttribute("src");
          s.load();
        },
        () => s.remove(),
      ]),
      ...this.nodes.map((n) => () => n.disconnect()),
      ...this.clones.map((t) => () => t.stop()),
    ];
    for (const release of releases)
      try {
        release();
      } catch (error) {
        this.cleanupErrors.push(String(error));
      }
    this.canvas = undefined;
    this.pixels = undefined;
  }
}
