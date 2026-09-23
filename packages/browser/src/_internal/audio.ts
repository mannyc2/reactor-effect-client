import * as Effect from "effect/Effect";
import * as Scope from "effect/Scope";
import type * as Stream from "effect/Stream";
import { ReactorError } from "reactor-effect-client";
import { errorOf, fromOwnedReadableStream, positiveLimit } from "reactor-effect-client/host";
import { decodeAudioPacket } from "./audio-packet.js";
import type { WebAudioSample } from "./audio-packet.js";
export type { WebAudioSample } from "./audio-packet.js";

const bounded = async <A>(
  work: Promise<A>,
  timeout: number,
  label: string,
  signal?: AbortSignal,
): Promise<A> => {
  let timer: ReturnType<typeof setTimeout> | undefined;
  let rejectAbort: (() => void) | undefined;
  try {
    return await Promise.race([
      work,
      new Promise<never>((_, reject) => {
        timer = setTimeout(
          () => reject(new ReactorError({ code: "Timeout", message: label })),
          timeout,
        );
        rejectAbort = () => reject(new ReactorError({ code: "Aborted", message: label }));
        if (signal?.aborted) rejectAbort();
        else signal?.addEventListener("abort", rejectAbort, { once: true });
      }),
    ]);
  } finally {
    if (timer !== undefined) clearTimeout(timer);
    if (rejectAbort !== undefined) signal?.removeEventListener("abort", rejectAbort);
  }
};
/** Call from an ordinary start click. No autoplay-policy workaround is attempted.
 * The scope owns this context; audio streams borrow it and own their nodes/track clones. */
export const audioContext = (
  options: { readonly sampleRate?: number; readonly timeoutMs?: number } = {},
): Effect.Effect<AudioContext, ReactorError, Scope.Scope> =>
  Effect.gen(function* () {
    const timeout = yield* Effect.try({
      try: () => positiveLimit(options.timeoutMs ?? 5000, "audio context timeout", 60000),
      catch: errorOf,
    });
    const context = yield* Effect.acquireRelease(
      Effect.try({
        try: () => {
          if (typeof AudioContext !== "function" || typeof AudioWorkletNode !== "function")
            throw new ReactorError({
              code: "UnsupportedCapability",
              message: "built-in AudioContext and AudioWorkletNode are required",
            });
          return new AudioContext(
            options.sampleRate === undefined ? {} : { sampleRate: options.sampleRate },
          );
        },
        catch: (e) => errorOf(e, "UnsupportedCapability"),
      }),
      (context) =>
        Effect.tryPromise({
          try: () => bounded(context.close(), timeout, "AudioContext close deadline"),
          catch: errorOf,
        }).pipe(Effect.ignore),
    );
    yield* Effect.tryPromise({
      try: (signal) =>
        bounded(
          context.resume(),
          timeout,
          "AudioContext resume deadline; supply user activation",
          signal,
        ),
      catch: (cause) =>
        cause instanceof ReactorError
          ? cause
          : new ReactorError({
              code: "UnsupportedCapability",
              message: "AudioContext resume failed; ordinary user activation is required",
              context: { detail: cause },
            }),
    });
    if (context.state !== "running")
      return yield* new ReactorError({
        code: "UnsupportedCapability",
        message: `AudioContext is ${context.state}, not running`,
      });
    return context;
  });
export interface WebAudioOptions {
  /** A running built-in context; caller or audioContext() scope retains ownership. */
  readonly context: AudioContext;
  /** Serve dist/browser/_internal/pcm-worklet.js locally. A bundler must preserve/copy this asset. */
  readonly workletUrl?: string | URL;
  readonly maxSampleBytes?: number;
  /** Each consumption owns a muted HTMLAudioElement and waits for its ordinary play().
   * This deadline is separate from worklet registration and individual PCM reads. */
  readonly activationTimeoutMs?: number;
  readonly readTimeoutMs?: number;
}
// A worklet is registered once per context and URL. Failed registration is retryable, not cached success.
const modules = new WeakMap<AudioContext, Map<string, Promise<void>>>();
const load = (context: AudioContext, url: string): Promise<void> => {
  let cache = modules.get(context);
  if (cache === undefined) {
    cache = new Map();
    modules.set(context, cache);
  }
  const previous = cache.get(url);
  if (previous !== undefined) return previous;
  const task = context.audioWorklet.addModule(url, { credentials: "omit" });
  cache.set(url, task);
  task.catch(() => {
    cache?.delete(url);
  });
  return task;
};
/** Window-realm PCM with OWNED muted playback activation, not a bare Web Audio tap.
 * Each consumption clones the borrowed source once, attaches that clone to an owned muted
 * HTMLAudioElement, awaits ordinary play(), and uses the same MediaStream for Web Audio.
 * Activation rejection/timeout is an error, not silently successful PCM. No extra play()
 * call is required; no autoplay policy is overridden. The caller owns the running context.
 * One JS output slot and one credited worklet block; skipped render frames are reported.
 * Closing/interrupting the stream releases the sink, clone, nodes, ports and timers.
 * A stopped source lease fails Disconnected; reacquire a track/stream after reconnect.
 * No ScriptProcessorNode, getUserMedia, device permission or native engine is used. */
export const webAudioSamples = (
  source: MediaStreamTrack,
  options: WebAudioOptions,
): Stream.Stream<WebAudioSample, ReactorError> =>
  fromOwnedReadableStream({
    evaluate: () => {
      const timeout = positiveLimit(options.readTimeoutMs ?? 10000, "PCM read timeout", 600000);
      const activationTimeout = positiveLimit(
        options.activationTimeoutMs ?? 5000,
        "PCM activation timeout",
        60000,
      );
      const maxBytes = positiveLimit(
        options.maxSampleBytes ?? 1048576,
        "PCM sample bound",
        64 * 1024 * 1024,
      );
      const context = options.context;
      if (source.kind !== "audio" || source.readyState !== "live")
        throw new ReactorError({ code: "InvalidState", message: "expected a live audio track" });
      if (typeof AudioWorkletNode !== "function" || context.audioWorklet === undefined)
        throw new ReactorError({
          code: "UnsupportedCapability",
          message: "built-in AudioWorklet is unavailable in this realm",
        });
      if (
        typeof document === "undefined" ||
        typeof document.createElement !== "function" ||
        typeof MediaStream !== "function"
      )
        throw new ReactorError({
          code: "UnsupportedCapability",
          message: "PCM activation requires a Window document and built-in HTML audio playback",
        });
      if (context.state !== "running")
        throw new ReactorError({
          code: "UnsupportedCapability",
          message: "PCM requires an explicitly resumed, running AudioContext",
        });
      const url = new URL(
        options.workletUrl ?? new URL("./pcm-worklet.js", import.meta.url),
        import.meta.url,
      );
      if (!/^(https?:|file:)$/.test(url.protocol))
        throw new ReactorError({
          code: "Protocol",
          message: "serve the PCM worklet as a normal local module, not a data/blob URL",
        });
      const track = source.clone(),
        lifetime = new AbortController();
      let input: MediaStreamAudioSourceNode | undefined, node: AudioWorkletNode | undefined;
      let sink: HTMLAudioElement | undefined, watch: ReturnType<typeof setInterval> | undefined;
      let closed = false,
        pending:
          | { resolve(value: WebAudioSample): void; reject(error: ReactorError): void }
          | undefined;
      let controller: ReadableStreamDefaultController<WebAudioSample> | undefined;
      const close = (): void => {
        if (closed) return;
        closed = true;
        lifetime.abort();
        if (watch !== undefined) {
          clearInterval(watch);
          watch = undefined;
        }
        source.removeEventListener("ended", ended);
        track.removeEventListener("ended", ended);
        context.removeEventListener("statechange", state);
        // The element was created here and is never offered to another owner.
        if (sink !== undefined) {
          sink.pause();
          sink.srcObject = null;
        }
        pending?.reject(new ReactorError({ code: "Aborted", message: "PCM reader closed" }));
        pending = undefined;
        input?.disconnect();
        if (node !== undefined) {
          node.port.onmessage = null;
          node.port.onmessageerror = null;
          node.onprocessorerror = null;
          node.port.postMessage("stop");
          node.port.close();
          node.disconnect();
        }
        track.stop();
      };
      const fail = (error: ReactorError): void => {
        if (!closed) {
          controller?.error(error);
          close();
        }
      };
      const ended = (): void =>
        fail(new ReactorError({ code: "Disconnected", message: "PCM track ended" }));
      const state = (): void => {
        if (context.state !== "running")
          fail(new ReactorError({ code: "Disconnected", message: `PCM context ${context.state}` }));
      };
      const live = (): boolean => {
        if (source.readyState !== "live" || track.readyState !== "live") ended();
        state();
        return !closed;
      };
      const initialize = async (): Promise<void> => {
        source.addEventListener("ended", ended);
        track.addEventListener("ended", ended);
        context.addEventListener("statechange", state);
        // MediaStreamTrack.stop() sets readyState without dispatching ended, and does not
        // stop its clones. Session reconnect stops borrowed leases this way. Inspect the
        // lease on each read/message and every 100ms even while a consumer is stalled.
        // Timer throttling can delay this check; it is not a wall-clock termination proof.
        watch = setInterval(live, 100);
        if (!live()) return;
        const stream = new MediaStream([track]);
        sink = document.createElement("audio");
        sink.muted = true;
        sink.srcObject = stream;
        try {
          await bounded(
            sink.play(),
            activationTimeout,
            "PCM playback activation deadline; supply normal user activation",
            lifetime.signal,
          );
        } catch (error) {
          throw errorOf(error, "UnsupportedCapability", "PCM HTMLAudioElement.play activation");
        }
        if (!live()) return;
        await bounded(
          load(context, url.href),
          timeout,
          "AudioWorklet module deadline",
          lifetime.signal,
        );
        // Cancellation while addModule is pending must not create a late graph.
        if (!live()) return;
        input = context.createMediaStreamSource(stream);
        node = new AudioWorkletNode(context, "reactor-pcm-tap-v1", {
          numberOfInputs: 1,
          numberOfOutputs: 1,
          outputChannelCount: [1],
        });
        node.port.onmessage = (event: MessageEvent<unknown>) => {
          if (!live()) return;
          try {
            if (pending === undefined)
              throw new ReactorError({
                code: "Protocol",
                message: "unsolicited PCM block without a read credit",
              });
            const sample = decodeAudioPacket(event.data, maxBytes),
              waiter = pending;
            pending = undefined;
            waiter.resolve(sample);
          } catch (error) {
            fail(errorOf(error));
          }
        };
        node.port.onmessageerror = () =>
          fail(
            new ReactorError({
              code: "Protocol",
              message: "PCM transfer could not be deserialized",
            }),
          );
        node.onprocessorerror = () =>
          fail(
            new ReactorError({
              code: "UnsupportedCapability",
              message: "PCM AudioWorklet processor failed",
            }),
          );
        input.connect(node);
        node.connect(context.destination);
      };
      let ready: Promise<void>;
      return new ReadableStream<WebAudioSample>(
        {
          start(c) {
            controller = c;
            ready = initialize().catch((error: unknown) =>
              fail(errorOf(error, "UnsupportedCapability", "open PCM")),
            );
          },
          async pull(c) {
            try {
              await ready;
              if (!live()) return;
              const sample = await bounded(
                new Promise<WebAudioSample>((resolve, reject) => {
                  pending = { resolve, reject };
                  node?.port.postMessage("pull");
                }),
                timeout,
                "PCM read deadline",
                lifetime.signal,
              );
              if (live()) c.enqueue(sample);
            } catch (error) {
              fail(errorOf(error));
            }
          },
          cancel: close,
        },
        { highWaterMark: 1 },
      );
    },
    onError: (error) => errorOf(error, "UnsupportedCapability", "webAudioSamples"),
  });
