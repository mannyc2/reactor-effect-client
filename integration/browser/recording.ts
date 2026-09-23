/** Development integration assertion, not a production decoder or a fixture byte check.
 * Its ONLY media input is the byte array returned by the public downloadClip operation.
 * Native HTML playback decodes the recording; Web Audio inspects that element's output. */
import * as Effect from "effect/Effect";
import { ReactorError } from "reactor-effect-client";
import { assert, hash, rms } from "./util.js";

interface FrameObservation {
  readonly mediaTimeSeconds: number;
  readonly callbackTimeMs: number;
  readonly presentedFrames: number;
  readonly width: number;
  readonly height: number;
  readonly pixelHash: string;
}
interface AudioObservation {
  /** Time at the query, NOT the timestamp of the first/last sample in the analyser window. */
  readonly contextObservationSeconds: number;
  readonly rms: number;
}
export interface RecordedPlayback {
  readonly input: "downloadClip.bytes via owned video/mp4 Blob URL";
  readonly ended: true;
  readonly durationSeconds: number;
  readonly currentTimeSeconds: number;
  readonly canPlayTypeHint: string;
  readonly video: {
    readonly clock: "media-element-timeline";
    readonly callbackClock: "performance-time-origin";
    readonly distinctHashes: number;
    readonly frames: readonly FrameObservation[];
  };
  readonly audio: {
    readonly clock: "audio-context-observation";
    readonly windowFrames: number;
    readonly sampleRate: number;
    readonly maximumWindowRms: number;
    readonly windows: readonly AudioObservation[];
    readonly timestamps: string;
  };
  readonly ownership: string;
}
interface Resources {
  readonly video: HTMLVideoElement;
  readonly canvas: HTMLCanvasElement;
  readonly pixels: CanvasRenderingContext2D;
  readonly analyser: AnalyserNode;
  readonly context: AudioContext;
  close(): void;
}
const failure = (error: unknown): ReactorError =>
  error instanceof ReactorError
    ? error
    : new ReactorError({
        code: "UnsupportedCapability",
        message: error instanceof Error ? error.message : String(error),
        context: { operation: "recording browser playback" },
      });
const open = (
  bytes: Uint8Array<ArrayBuffer>,
  context: AudioContext,
  preview?: HTMLElement,
): Resources => {
  if (context.state !== "running")
    throw new ReactorError({
      code: "InvalidState",
      message: "recording playback requires a running caller-owned AudioContext",
    });
  if (bytes.length === 0 || bytes.length > 1048576)
    throw new ReactorError({
      code: "Overflow",
      message: "recording fixture input must be 1..1048576 bytes",
    });
  const video = document.createElement("video"),
    canvas = document.createElement("canvas");
  let url: string | undefined,
    input: MediaElementAudioSourceNode | undefined,
    analyser: AnalyserNode | undefined,
    gain: GainNode | undefined;
  let closed = false;
  const close = (): void => {
    if (closed) return;
    closed = true;
    const errors: string[] = [];
    // Each release is attempted even if another browser cleanup method throws.
    for (const release of [
      () => video.pause(),
      () => {
        video.srcObject = null;
        video.removeAttribute("src");
        video.load();
      },
      () => video.remove(),
      () => input?.disconnect(),
      () => analyser?.disconnect(),
      () => gain?.disconnect(),
      () => {
        if (url !== undefined) URL.revokeObjectURL(url);
      },
    ]) {
      try {
        release();
      } catch (error) {
        errors.push(String(error));
      }
    }
    if (errors.length !== 0)
      throw new ReactorError({
        code: "UnsupportedCapability",
        message: "recording cleanup failed",
        context: { detail: errors },
      });
  };
  try {
    const pixels = canvas.getContext("2d", { willReadFrequently: true });
    if (pixels === null)
      throw new ReactorError({
        code: "UnsupportedCapability",
        message: "recording pixel readback requires Canvas 2D",
      });
    video.playsInline = true;
    video.preload = "auto";
    video.loop = false;
    video.muted = false;
    video.volume = 1;
    video.setAttribute("aria-label", "Downloaded synthetic recording playback");
    url = URL.createObjectURL(new Blob([bytes], { type: "video/mp4" }));
    video.src = url;
    input = context.createMediaElementSource(video);
    analyser = context.createAnalyser();
    gain = context.createGain();
    analyser.fftSize = 2048;
    gain.gain.value = 0;
    // Keep the ELEMENT unmuted: inspect decoded audio upstream of the zero-gain output.
    // createMediaElementSource routes its audio into this graph. No audible monitor, new
    // AudioContext, independent WAV, decodeAudioData substitute or captureStream is used.
    input.connect(analyser);
    analyser.connect(gain);
    gain.connect(context.destination);
    preview?.append(video);
    return { video, canvas, pixels, analyser, context, close };
  } catch (error) {
    close();
    throw error;
  }
};
const observe = ({
  video,
  canvas,
  pixels,
  analyser,
  context,
}: Resources): Effect.Effect<RecordedPlayback, ReactorError> =>
  Effect.callback<RecordedPlayback, ReactorError>((resume) => {
    const frames: FrameObservation[] = [],
      windows: AudioObservation[] = [],
      samples = new Float32Array(analyser.fftSize);
    let callback: number | undefined,
      settled = false;
    const release = (): void => {
      if (settled) return;
      settled = true;
      if (callback !== undefined) {
        video.cancelVideoFrameCallback(callback);
        callback = undefined;
      }
      video.removeEventListener("ended", ended);
      video.removeEventListener("error", error);
      context.removeEventListener("statechange", state);
    };
    const finish = (result: Effect.Effect<RecordedPlayback, ReactorError>): void => {
      if (!settled) {
        release();
        resume(result);
      }
    };
    const diagnostic = (cause: unknown): ReactorError => {
      const error = failure(cause);
      return new ReactorError({
        code: error.code,
        message: error.message,
        context: {
          ...error.context,
          detail: {
            cause: error.context.detail,
            frames,
            windows,
            readyState: video.readyState,
            currentTimeSeconds: video.currentTime,
            ended: video.ended,
            contextState: context.state,
            mediaErrorCode: video.error?.code,
            mediaError: video.error?.message,
          },
        },
      });
    };
    const fail = (cause: unknown): void => finish(Effect.fail(diagnostic(cause)));
    const error = (): void =>
      fail(
        new ReactorError({
          code: "UnsupportedCapability",
          message: "recording HTML decoder failed",
          context: { detail: { mediaErrorCode: video.error?.code, message: video.error?.message } },
        }),
      );
    const state = (): void => {
      if (context.state !== "running")
        fail(new ReactorError({ code: "Disconnected", message: "recording AudioContext stopped" }));
    };
    const ended = (): void =>
      finish(
        Effect.try({
          try: () => {
            const hashes = new Set(frames.map((frame) => frame.pixelHash));
            const energy = Math.max(0, ...windows.map((window) => window.rms));
            assert(video.ended, "recording ended event without terminal playback state");
            assert(
              frames.length >= 8 && hashes.size >= 3,
              "recording did not produce changing decoded video",
            );
            assert(
              windows.length >= 8 && energy > 0.001,
              "recording decoded audio was absent or silent",
            );
            assert(
              Number.isFinite(video.duration) && video.duration > 0,
              "recording duration unavailable",
            );
            assert(
              frames.every(
                (f, i) => i === 0 || f.mediaTimeSeconds > (frames[i - 1]?.mediaTimeSeconds ?? -1),
              ),
              "recording media timeline did not advance",
            );
            assert(
              windows.every(
                (w, i) =>
                  i === 0 ||
                  w.contextObservationSeconds >= (windows[i - 1]?.contextObservationSeconds ?? 0),
              ),
              "recording audio observation clock regressed",
            );
            return {
              input: "downloadClip.bytes via owned video/mp4 Blob URL" as const,
              ended: true as const,
              durationSeconds: video.duration,
              currentTimeSeconds: video.currentTime,
              canPlayTypeHint: video.canPlayType("video/mp4"),
              video: {
                clock: "media-element-timeline" as const,
                callbackClock: "performance-time-origin" as const,
                distinctHashes: hashes.size,
                frames,
              },
              audio: {
                clock: "audio-context-observation" as const,
                windowFrames: analyser.fftSize,
                sampleRate: context.sampleRate,
                maximumWindowRms: energy,
                windows,
                timestamps:
                  "contextObservationSeconds is when the analyser was queried; overlapping recent windows have no sample PTS. Not RTP/capture time or evidence of A/V synchronization.",
              },
              ownership:
                "Scope releases Blob URL, source element, frame callback, listeners and audio nodes. No VideoFrame/AudioData or ReadableStream reader is acquired; resetting the element requests native decoder release; engine memory reclamation is not measured.",
            };
          },
          catch: diagnostic,
        }),
      );
    const frame = (at: number, metadata: VideoFrameCallbackMetadata): void => {
      callback = undefined;
      if (settled) return;
      try {
        state();
        if (settled) return;
        if (frames.length >= 128)
          throw new ReactorError({
            code: "Overflow",
            message: "recording callback observation bound (128)",
          });
        // Fixture-specific dimensions, not a permissive arbitrary-media decoder test.
        assert(
          video.videoWidth === 96 && video.videoHeight === 64,
          "unexpected recording dimensions",
        );
        assert(
          Number.isFinite(metadata.mediaTime) && metadata.mediaTime >= 0 && Number.isFinite(at),
          "invalid recording callback timestamp",
        );
        canvas.width = video.videoWidth;
        canvas.height = video.videoHeight;
        pixels.drawImage(video, 0, 0);
        const image = pixels.getImageData(0, 0, canvas.width, canvas.height);
        assert(image.data.length === 96 * 64 * 4, "recording RGBA readback length mismatch");
        // Copy at the native frame callback, rather than hashing bytes or a source canvas.
        frames.push({
          mediaTimeSeconds: metadata.mediaTime,
          callbackTimeMs: at,
          presentedFrames: metadata.presentedFrames,
          width: video.videoWidth,
          height: video.videoHeight,
          pixelHash: hash(image.data),
        });
        analyser.getFloatTimeDomainData(samples);
        assert(samples.every(Number.isFinite), "recording audio readback was nonfinite");
        const observed = context.currentTime;
        assert(Number.isFinite(observed) && observed >= 0, "invalid audio observation time");
        windows.push({ contextObservationSeconds: observed, rms: rms(samples) });
        callback = video.requestVideoFrameCallback(frame);
      } catch (cause) {
        fail(cause);
      }
    };
    if (typeof video.requestVideoFrameCallback !== "function") {
      resume(
        Effect.fail(
          new ReactorError({
            code: "UnsupportedCapability",
            message: "recording check requires requestVideoFrameCallback",
          }),
        ),
      );
      return;
    }
    video.addEventListener("ended", ended);
    video.addEventListener("error", error);
    context.addEventListener("statechange", state);
    try {
      state();
      if (!settled) callback = video.requestVideoFrameCallback(frame);
    } catch (cause) {
      fail(cause);
    }
    return Effect.sync(release);
  });
/** Invoke after the ordinary Start click; rejected play is a failed assertion, never bypassed.
 * Only a bound of observed frames/windows is retained. This function has no fixture HTTP access. */
export const recordingPlayback = (
  bytes: Uint8Array<ArrayBuffer>,
  context: AudioContext,
  options: {
    readonly preview?: HTMLElement;
    readonly timeoutMs?: number;
    readonly activationTimeoutMs?: number;
  } = {},
): Effect.Effect<RecordedPlayback, ReactorError> =>
  Effect.scoped(
    Effect.gen(function* () {
      const timeout = options.timeoutMs ?? 10000,
        activation = options.activationTimeoutMs ?? 5000;
      if (![timeout, activation].every((n) => Number.isSafeInteger(n) && n > 0 && n <= 60000))
        return yield* Effect.fail(
          new ReactorError({
            code: "Protocol",
            message: "recording check deadlines must be 1..60000 ms",
          }),
        );
      const resources = yield* Effect.acquireRelease(
        Effect.try({ try: () => open(bytes, context, options.preview), catch: failure }),
        (resources) => Effect.sync(() => resources.close()),
      );
      const [result] = yield* Effect.all(
        [
          observe(resources),
          Effect.tryPromise({
            try: () => resources.video.play(),
            catch: failure,
          }).pipe(
            Effect.timeoutOrElse({
              duration: activation,
              orElse: () =>
                Effect.fail(
                  new ReactorError({
                    code: "Timeout",
                    message: "recording play activation deadline",
                  }),
                ),
            }),
          ),
        ],
        { concurrency: 2 },
      ).pipe(
        Effect.timeoutOrElse({
          duration: timeout,
          orElse: () =>
            Effect.fail(
              new ReactorError({
                code: "Timeout",
                message: "recording playback/decoded-observation deadline",
              }),
            ),
        }),
      );
      return result;
    }),
  );
