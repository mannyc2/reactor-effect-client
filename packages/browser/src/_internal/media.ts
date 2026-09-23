export { audioContext, webAudioSamples } from "./audio.js";
export type { WebAudioOptions, WebAudioSample } from "./audio.js";
import * as Effect from "effect/Effect";
import * as Stream from "effect/Stream";
import type * as Scope from "effect/Scope";
import { ReactorError } from "reactor-effect-client";
import {
  errorOf,
  finite,
  fromOwnedReadableStream,
  positiveLimit,
  record,
} from "reactor-effect-client/host";

export interface MediaOptions {
  readonly maxSampleBytes?: number;
  readonly readTimeoutMs?: number;
}
export interface VideoSample {
  readonly format: "RGBA";
  readonly width: number;
  readonly height: number;
  readonly data: Uint8Array<ArrayBuffer>;
  readonly timestampUs: number;
  readonly clock: "video-track-processor";
}
export interface AudioSample {
  readonly format: "f32-planar";
  readonly planes: readonly Float32Array<ArrayBuffer>[];
  readonly sampleRate: number;
  readonly frames: number;
  readonly timestampUs: number;
  readonly clock: "audio-track-processor";
}

/** Presence is only a hint. Acquiring a processor and copying a sample tests the capability. */
export const mediaFacilities = (): Readonly<Record<string, boolean>> =>
  Object.freeze({
    peer: typeof RTCPeerConnection === "function",
    videoFrame: typeof VideoFrame === "function",
    trackProcessor: typeof record(globalThis)["MediaStreamTrackProcessor"] === "function",
    audioData: "AudioData" in globalThis && typeof globalThis.AudioData === "function",
    playback: typeof HTMLMediaElement === "function",
    audioContext: typeof AudioContext === "function",
    audioWorklet: typeof AudioWorkletNode === "function",
  });

const unsupported = (message: string): never => {
  throw new ReactorError({ code: "UnsupportedCapability", message });
};
const timestamp = (input: unknown): number => {
  const value = finite(input, "media timestamp");
  if (!Number.isSafeInteger(value))
    throw new ReactorError({
      code: "Protocol",
      message: "media timestamp cannot be represented exactly in this host",
    });
  return value;
};

const deadline = async <A>(promise: Promise<A>, ms: number, signal?: AbortSignal): Promise<A> => {
  let handle: ReturnType<typeof setTimeout> | undefined;
  let aborted: (() => void) | undefined;
  try {
    return await Promise.race([
      promise,
      new Promise<never>((_, reject) => {
        handle = setTimeout(
          () =>
            reject(new ReactorError({ code: "Timeout", message: "media read/copy/play deadline" })),
          ms,
        );
        aborted = () =>
          reject(new ReactorError({ code: "Aborted", message: "media operation cancelled" }));
        if (signal?.aborted) aborted();
        else signal?.addEventListener("abort", aborted, { once: true });
      }),
    ]);
  } finally {
    if (handle !== undefined) clearTimeout(handle);
    if (aborted !== undefined) signal?.removeEventListener("abort", aborted);
  }
};

interface Block {
  close(): void;
}

const processor = (track: MediaStreamTrack): unknown => {
  const constructor = record(globalThis)["MediaStreamTrackProcessor"];
  if (typeof constructor !== "function")
    return unsupported("MediaStreamTrackProcessor is unavailable in this realm");
  return Reflect.construct(constructor, [{ track, maxBufferSize: 1 }]);
};

const nativeAudio = (
  input: unknown,
): {
  readonly timestamp: number;
  readonly frames: number;
  readonly channels: number;
  readonly sampleRate: number;
  copy(plane: Float32Array<ArrayBuffer>, index: number): void;
} => {
  if (
    !("AudioData" in globalThis) ||
    typeof globalThis.AudioData !== "function" ||
    !(input instanceof globalThis.AudioData)
  ) {
    return unsupported("the track processor did not yield native AudioData in this realm");
  }
  const raw = record(input, "AudioData"),
    copy = raw.copyTo;
  if (typeof copy !== "function" || typeof raw.close !== "function")
    return unsupported("AudioData copyTo/close are missing");
  return {
    timestamp: timestamp(raw.timestamp),
    frames: positiveLimit(finite(raw.numberOfFrames, "audio frames"), "audio frames", 1_048_576),
    channels: positiveLimit(finite(raw.numberOfChannels, "audio channels"), "audio channels", 32),
    sampleRate: positiveLimit(finite(raw.sampleRate, "audio sample rate"), "sample rate", 384_000),
    copy: (plane, index) => {
      Reflect.apply(copy, input, [plane, { planeIndex: index, format: "f32-planar" }]);
    },
  };
};

const closable = (raw: unknown): Block | undefined => {
  if (
    typeof raw !== "object" ||
    raw === null ||
    !("close" in raw) ||
    typeof raw.close !== "function"
  )
    return undefined;
  const close = raw.close;
  return {
    close: () => {
      Reflect.apply(close, raw, []);
    },
  };
};

/** Acquisition, native output and copied output remain separate diagnostic facts. */
export interface NativeMediaFailure {
  readonly phase: string;
  readonly realm: "Window" | "other";
  readonly processorConstructed: boolean;
  readonly readerAcquired: boolean;
  readonly nativeSamplesRead: number;
  readonly copiedSamples: number;
  readonly source: Readonly<{
    kind: string;
    readyState: MediaStreamTrackState;
    enabled: boolean;
    muted: boolean;
  }>;
  readonly ownedTrack: Readonly<{ readyState: MediaStreamTrackState }>;
  readonly nativeErrorName?: string;
  readonly cleanupFailures: readonly Readonly<{ phase: string; message: string }>[];
}

interface OwnedSamples<A> {
  readonly readable: ReadableStream<A>;
  close(): Promise<void>;
}

function copiedSamples<A>(
  source: MediaStreamTrack,
  kind: "video" | "audio",
  options: MediaOptions,
  copy: (raw: unknown, maxBytes: number, timeoutMs: number) => Promise<A>,
): OwnedSamples<A> {
  const maxBytes = positiveLimit(
    options.maxSampleBytes ?? (kind === "video" ? 8_294_400 : 1_048_576),
    "media sample bytes",
    64 * 1024 * 1024,
  );
  const timeoutMs = positiveLimit(options.readTimeoutMs ?? 10_000, "media read timeout", 600_000);
  if (source.kind !== kind || source.readyState !== "live")
    throw new ReactorError({ code: "InvalidState", message: `expected a live ${kind} track` });
  const operation = kind === "audio" ? "audioSamples" : "videoFrames";
  const track = source.clone();
  let constructed = false,
    acquired = false,
    readCount = 0,
    copiedCount = 0;
  const cleanupFailures: { phase: string; message: string }[] = [];

  const annotate = (cause: unknown, phase: string): ReactorError => {
    const name =
      cause instanceof Error && !(cause instanceof ReactorError) ? cause.name : undefined;
    const error = errorOf(
      cause,
      name === "NotSupportedError" || (name === "TypeError" && phase === "processor")
        ? "UnsupportedCapability"
        : name === "InvalidStateError" || (name === "TypeError" && phase === "reader")
          ? "InvalidState"
          : "Protocol",
    );
    const detail: NativeMediaFailure = {
      phase,
      realm: typeof document === "object" ? "Window" : "other",
      processorConstructed: constructed,
      readerAcquired: acquired,
      nativeSamplesRead: readCount,
      copiedSamples: copiedCount,
      source: {
        kind: source.kind,
        readyState: source.readyState,
        enabled: source.enabled,
        muted: source.muted,
      },
      ownedTrack: { readyState: track.readyState },
      ...(name === undefined ? {} : { nativeErrorName: name }),
      cleanupFailures,
    };
    return new ReactorError({
      code: error.code,
      message: error.message,
      context: { ...error.context, operation: `${operation}.${phase}`, detail },
    });
  };
  const noteCleanup = (phase: string, cause: unknown): void => {
    if (cleanupFailures.length < 4)
      cleanupFailures.push({
        phase,
        message: cause instanceof Error ? cause.message : String(cause),
      });
  };

  let reader: ReadableStreamDefaultReader<unknown>;
  let acquisitionPhase = "sample-type";
  try {
    if (
      kind === "audio" &&
      (!("AudioData" in globalThis) || typeof globalThis.AudioData !== "function")
    ) {
      throw new ReactorError({
        code: "UnsupportedCapability",
        message: "AudioData is not exposed in this realm",
      });
    }
    acquisitionPhase = "processor";
    const native = processor(track);
    constructed = true;
    acquisitionPhase = "readable";
    const readable: unknown = record(native, "track processor").readable;
    if (!(readable instanceof ReadableStream))
      throw new ReactorError({
        code: "UnsupportedCapability",
        message: "track processor did not supply a ReadableStream",
      });
    acquisitionPhase = "reader";
    reader = readable.getReader();
    acquired = true;
  } catch (cause) {
    const error = annotate(cause, acquisitionPhase);
    try {
      track.stop();
    } catch (stopError) {
      noteCleanup("stop-clone", stopError);
    }
    throw error;
  }

  let closed = false;
  let cleanupTask: Promise<void> | undefined, pullTask: Promise<void> | undefined;
  let controller: ReadableStreamDefaultController<A>;
  let watch: ReturnType<typeof setInterval> | undefined;
  const lifetime = new AbortController();

  const cleanup = (): Promise<void> => {
    if (cleanupTask !== undefined) return cleanupTask;
    closed = true;
    lifetime.abort();
    if (watch !== undefined) {
      clearInterval(watch);
      watch = undefined;
    }
    source.removeEventListener("ended", ended);
    track.removeEventListener("ended", ended);
    // Install one shared promise before invoking native code; every exit joins it.
    cleanupTask = Promise.resolve().then(async () => {
      try {
        track.stop();
      } catch (cause) {
        noteCleanup("stop-clone", cause);
      }
      try {
        await deadline(reader.cancel(), timeoutMs);
      } catch (cause) {
        noteCleanup("cancel-reader", cause);
      } finally {
        try {
          reader.releaseLock();
        } catch (cause) {
          noteCleanup("release-reader", cause);
        }
      }
    });
    return cleanupTask;
  };
  const fail = (error: ReactorError): void => {
    if (closed) return;
    controller.error(error);
    void cleanup();
  };
  const ended = (): void =>
    fail(
      annotate(
        new ReactorError({
          code: "Disconnected",
          message: "media source lease ended; reacquire after reconnect",
        }),
        "track",
      ),
    );
  const live = (): boolean => {
    if (!closed && (source.readyState !== "live" || track.readyState !== "live")) ended();
    return !closed;
  };
  const closeLate = (raw: unknown): void => {
    try {
      closable(raw)?.close();
    } catch (cause) {
      noteCleanup("close-late-sample", cause);
    }
  };

  const pull = async (): Promise<void> => {
    let block: Block | undefined;
    let phase = "read";
    let failure: ReactorError | undefined;
    const releaseSample = (): void => {
      const owned = block;
      block = undefined;
      owned?.close();
    };
    try {
      if (!live()) return;
      const received = await deadline(
        reader.read().then((result) => {
          const disposed = closed && !result.done;
          if (disposed) closeLate(result.value);
          return { result, disposed };
        }),
        timeoutMs,
        lifetime.signal,
      );
      const next = received.result;
      if (!next.done && !received.disposed) block = closable(next.value);
      if (!live()) return;
      if (next.done) {
        controller.close();
        await cleanup();
        return;
      }
      readCount++;
      phase = "copy";
      const sample = await deadline(
        copy(next.value, maxBytes, timeoutMs),
        timeoutMs,
        lifetime.signal,
      );
      phase = "close-sample";
      releaseSample();
      if (live()) {
        copiedCount++;
        controller.enqueue(sample);
      }
    } catch (cause) {
      if (phase === "read" && !live()) return;
      failure = annotate(cause, phase);
    } finally {
      try {
        releaseSample();
      } catch (cause) {
        if (failure === undefined) failure = annotate(cause, "close-sample");
        else noteCleanup("close-sample", cause);
      }
      if (failure !== undefined) fail(failure);
    }
  };
  const close = async (): Promise<void> => {
    await cleanup();
    await pullTask;
  };
  const readable = new ReadableStream<A>(
    {
      start(c) {
        controller = c;
        source.addEventListener("ended", ended);
        track.addEventListener("ended", ended);
        // stop() does not emit ended or stop a clone. Check while consumers pause too.
        watch = setInterval(live, 100);
      },
      pull() {
        pullTask = pull();
        return pullTask;
      },
      cancel: close,
    },
    { highWaterMark: 0 },
  );
  return { readable, close };
}

const nativeSamples = <A>(
  source: MediaStreamTrack,
  kind: "video" | "audio",
  options: MediaOptions,
  copy: (raw: unknown, maxBytes: number, timeoutMs: number) => Promise<A>,
): Stream.Stream<A, ReactorError> =>
  Stream.unwrap(
    Effect.gen(function* () {
      const owned = yield* Effect.acquireRelease(
        Effect.try({ try: () => copiedSamples(source, kind, options, copy), catch: errorOf }),
        (owned) => Effect.promise(() => owned.close()),
      );
      return fromOwnedReadableStream({ evaluate: () => owned.readable, onError: errorOf });
    }),
  );

/** One native queue slot and one demand-driven copy. No borrowed frame escapes. */
export const videoFrames = (
  track: MediaStreamTrack,
  options: MediaOptions = {},
): Stream.Stream<VideoSample, ReactorError> =>
  nativeSamples(track, "video", options, async (raw, maxBytes) => {
    if (typeof VideoFrame !== "function" || !(raw instanceof VideoFrame))
      return unsupported("processor did not yield a native VideoFrame");
    const width = positiveLimit(raw.displayWidth, "video width", 16_384);
    const height = positiveLimit(raw.displayHeight, "video height", 16_384);
    if (
      raw.codedWidth !== width ||
      raw.codedHeight !== height ||
      raw.visibleRect?.x !== 0 ||
      raw.visibleRect.y !== 0
    ) {
      return unsupported("cropped or non-square-pixel frame is outside the compact RGBA contract");
    }
    const size = raw.allocationSize({ format: "RGBA" });
    if (!Number.isSafeInteger(size) || size !== width * height * 4 || size > maxBytes)
      throw new ReactorError({
        code: "Overflow",
        message: "RGBA sample byte bound/layout mismatch",
      });
    const data = new Uint8Array(size);
    await raw.copyTo(data, { format: "RGBA" });
    return Object.freeze({
      format: "RGBA" as const,
      width,
      height,
      data,
      timestampUs: timestamp(raw.timestamp),
      clock: "video-track-processor" as const,
    });
  });

/** Optional native AudioData in the current realm. No worker or Web Audio fallback. */
export const audioSamples = (
  track: MediaStreamTrack,
  options: MediaOptions = {},
): Stream.Stream<AudioSample, ReactorError> =>
  nativeSamples(track, "audio", options, async (raw, maxBytes) => {
    const audio = nativeAudio(raw);
    if (audio.frames * audio.channels * 4 > maxBytes)
      throw new ReactorError({ code: "Overflow", message: "PCM sample byte bound" });
    const planes: Float32Array<ArrayBuffer>[] = [];
    for (let channel = 0; channel < audio.channels; channel++) {
      const plane = new Float32Array(audio.frames);
      audio.copy(plane, channel);
      planes.push(plane);
    }
    return Object.freeze({
      format: "f32-planar" as const,
      planes: Object.freeze(planes),
      sampleRate: audio.sampleRate,
      frames: audio.frames,
      timestampUs: audio.timestamp,
      clock: "audio-track-processor" as const,
    });
  });

/** Scoped local playback. Starting playback does not establish audience output. */
export const play = (
  track: MediaStreamTrack,
  element: HTMLMediaElement,
  timeoutMs = 10_000,
): Effect.Effect<void, ReactorError, Scope.Scope> =>
  Effect.gen(function* () {
    const timeout = yield* Effect.try({
      try: () => positiveLimit(timeoutMs, "playback timeout", 600_000),
      catch: errorOf,
    });
    const owned = yield* Effect.acquireRelease(
      Effect.try({
        try: () => {
          if (track.readyState !== "live")
            throw new ReactorError({
              code: "InvalidState",
              message: "playback requires a live track",
            });
          if (element.srcObject !== null || element.getAttribute("src"))
            throw new ReactorError({
              code: "InvalidState",
              message: "playback element already has a source",
            });
          const clone = track.clone();
          try {
            const stream = new MediaStream([clone]);
            element.srcObject = stream;
            return { clone, stream };
          } catch (error) {
            clone.stop();
            throw error;
          }
        },
        catch: errorOf,
      }),
      ({ clone, stream }) =>
        Effect.sync(() => {
          clone.stop();
          if (element.srcObject === stream) {
            element.pause();
            element.srcObject = null;
          }
        }),
    );
    yield* Effect.tryPromise({
      try: (signal) => deadline(element.play(), timeout, signal),
      catch: (e) => errorOf(e, "UnsupportedCapability", "HTMLMediaElement.play"),
    });
    if (owned.clone.readyState !== "live")
      return yield* Effect.fail(
        new ReactorError({ code: "Disconnected", message: "playback track ended during start" }),
      );
  });

export interface Presentation {
  readonly callbackTimeMs: number;
  readonly mediaTimeSeconds: number;
  readonly presentedFrames: number;
  readonly width: number;
  readonly height: number;
}

export const nextPresentation = (
  element: HTMLVideoElement,
  timeoutMs = 10_000,
): Effect.Effect<Presentation, ReactorError> =>
  Effect.suspend(() =>
    Effect.gen(function* () {
      const timeout = yield* Effect.try({
        try: () => positiveLimit(timeoutMs, "presentation timeout", 600_000),
        catch: errorOf,
      });
      return yield* Effect.callback<Presentation, ReactorError>((resume) => {
        if (typeof element.requestVideoFrameCallback !== "function") {
          resume(
            Effect.fail(
              new ReactorError({
                code: "UnsupportedCapability",
                message: "requestVideoFrameCallback unavailable",
              }),
            ),
          );
          return;
        }
        const id = element.requestVideoFrameCallback((at, m) =>
          resume(
            Effect.succeed({
              callbackTimeMs: at,
              mediaTimeSeconds: m.mediaTime,
              presentedFrames: m.presentedFrames,
              width: m.width,
              height: m.height,
            }),
          ),
        );
        return Effect.sync(() => element.cancelVideoFrameCallback(id));
      }).pipe(
        Effect.timeoutOrElse({
          duration: timeout,
          orElse: () =>
            Effect.fail(
              new ReactorError({
                code: "Timeout",
                message: "no local video presentation callback",
              }),
            ),
        }),
      );
    }),
  );
