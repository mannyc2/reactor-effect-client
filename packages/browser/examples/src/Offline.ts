/**
 * The Studio on Reactor simulated in the tab: `ReactorTest` stands in for the
 * coordinator and an H3 model at the network edge, at timings paid hosted
 * runs measured, so every layer above it runs unchanged and nothing is billed.
 * Its media arrive decoded, so the page draws frames itself.
 */
import { Effect, Layer, Redacted, Stream } from "effect";
import * as CoordinatorClient from "reactor-effect-client/CoordinatorClient";
import * as H3 from "reactor-effect-client/H3";
import type { AudioFrame, VideoFrame } from "reactor-effect-client/Media";
import * as Reactor from "reactor-effect-client/Reactor";
import type { ReactorError } from "reactor-effect-client/ReactorError";
import * as ReactorTest from "reactor-effect-client/ReactorTest";
import type { Session } from "reactor-effect-client/Session";
import { Stage } from "./Stage.ts";
import type { Services } from "./Stage.ts";
import { WebCrypto } from "./WebCrypto.ts";

/** The key the simulated coordinator accepts; it opens nothing outside this tab. */
const apiKey = "h3-studio-offline";

/**
 * A quarter of H3's 1344x768 canvas each way. The simulator draws each clip as
 * one flat colour, so a larger frame would only cost the page more to draw.
 */
const frameSize = { width: 336, height: 192 };

/** Nothing bills offline, so a simulated session keeps longer than a live one. */
const sessionCap = "5 minutes";

/**
 * Draws frames into the canvas, as RGBA whatever order a frame's bytes are
 * in, and blanks it when the generation they came from ends.
 */
const painter = (canvas: HTMLCanvasElement, caption: HTMLElement) => {
  const context = canvas.getContext("2d");
  let pixels: ImageData | undefined;
  const blank = (): void => {
    context?.clearRect(0, 0, canvas.width, canvas.height);
    caption.textContent = "";
  };
  const paint = (frame: VideoFrame): void => {
    if (context === null) return;
    if (pixels?.width !== frame.width || pixels.height !== frame.height) {
      canvas.width = frame.width;
      canvas.height = frame.height;
      pixels = context.createImageData(frame.width, frame.height);
    }
    const target = pixels.data;
    if (frame.format === "RGBA") target.set(frame.data);
    else
      for (let index = 0; index < frame.data.length; index += 4) {
        target[index] = frame.data[index + 2] ?? 0;
        target[index + 1] = frame.data[index + 1] ?? 0;
        target[index + 2] = frame.data[index] ?? 0;
        target[index + 3] = frame.data[index + 3] ?? 255;
      }
    context.putImageData(pixels, 0, 0);
    // The simulator writes each frame's clip and index into its first pixels.
    const carried = ReactorTest.frameOf(frame);
    caption.textContent =
      carried === undefined
        ? `black · ${String(frame.width)}×${String(frame.height)} ${frame.format}`
        : `clip …${carried.clipId.slice(-4)} · frame ${String(carried.index)} · ${String(frame.width)}×${String(frame.height)} ${frame.format}`;
  };
  return { paint, blank };
};

/** Schedules decoded PCM blocks back to back on a Web Audio context. */
const speaker = (context: AudioContext) => {
  let next = 0;
  return (block: AudioFrame): void => {
    const length = block.samples.length / block.channels;
    const buffer = context.createBuffer(block.channels, length, block.sampleRate);
    for (let channel = 0; channel < block.channels; channel++) {
      const samples = buffer.getChannelData(channel);
      for (let index = 0; index < length; index++)
        samples[index] = (block.samples[index * block.channels + channel] ?? 0) / 32768;
    }
    const source = context.createBufferSource();
    source.buffer = buffer;
    source.connect(context.destination);
    // A little headroom absorbs the jitter of blocks arriving on the event loop.
    next = Math.max(next, context.currentTime + 0.1);
    source.start(next);
    next += buffer.duration;
  };
};

const stage = (screen: HTMLElement, reason: string | undefined) =>
  Layer.effect(
    Stage,
    Effect.gen(function* () {
      const test = yield* ReactorTest.ReactorTest;
      const canvas = document.createElement("canvas");
      const caption = document.createElement("div");
      caption.className = "frame-caption";
      screen.prepend(canvas, caption);
      const { paint, blank } = painter(canvas, caption);
      return Stage.of({
        mode: "Offline",
        about: [
          "Reactor's coordinator and an H3 model run in this tab (ReactorTest), at timings measured on paid hosted runs, and nothing is billed.",
          "Its picture is one flat colour per clip; the caption reads the clip and frame each picture carries.",
          ...(reason === undefined ? [] : [reason]),
        ].join(" "),
        picture: Effect.fnUntraced(function* (session: Session) {
          const media = yield* session.decoded;
          // A generation's last frame goes with it: the next connection draws its own.
          yield* Effect.addFinalizer(() => Effect.sync(blank));
          yield* media.video("main_video").pipe(
            // A preview wants the newest frame: a reader that falls behind drops
            // the older ones rather than painting them late.
            Stream.buffer({ capacity: 1, strategy: "sliding" }),
            Stream.runForEach((frame) => Effect.sync(() => paint(frame))),
          );
        }),
        sound: Effect.fnUntraced(function* (session: Session) {
          const media = yield* session.decoded;
          const context = yield* Effect.acquireRelease(
            Effect.sync(() => new AudioContext()),
            (context) => Effect.promise(() => context.close()),
          );
          const play = speaker(context);
          yield* media
            .audio("main_audio")
            .pipe(Stream.runForEach((block) => Effect.sync(() => play(block))));
        }),
        faults: [
          {
            label: "Fail the next build",
            shows: "H3 broadcasts clip_failed: the clip never reaches generated or started.",
            arm: test.inject({ _tag: "FailBuild", nth: 1 }),
          },
          {
            label: "Lose the next enqueue's reply",
            shows:
              "H3 queues the clip but its reply never comes. The SDK proves the clip from its metadata, and never sends it twice.",
            arm: test.inject({ _tag: "DropReply", command: "enqueue", applied: true, nth: 1 }),
          },
          {
            label: "Drop the next enqueue",
            shows:
              "Nothing reaches H3 and no reply comes, so after its 15 s deadline the enqueue fails with outcome unknown.",
            arm: test.inject({ _tag: "DropReply", command: "enqueue", nth: 1 }),
          },
          {
            label: "Moderate the next prompt",
            shows: "Content moderation ends the session a second after the enqueue.",
            arm: test.inject({ _tag: "Moderate", nth: 1 }),
          },
        ],
      });
    }),
  );

/** Each session's tokens, minted by the simulated coordinator with its own key. */
const reactor = Layer.unwrap(
  CoordinatorClient.CoordinatorClient.useSync((coordinator) =>
    Reactor.layer({
      tokens: coordinator.tokens({ modelName: H3.modelName, maxSessionDuration: sessionCap }),
    }),
  ),
);

/**
 * The Studio's services on the simulated Reactor. `reason` says why the page
 * runs offline, when it was not asked to.
 */
export const layer = (options: {
  readonly screen: HTMLElement;
  readonly reason?: string;
}): Layer.Layer<Services, ReactorError> =>
  Layer.mergeAll(reactor, stage(options.screen, options.reason)).pipe(
    Layer.provide(CoordinatorClient.layer({ apiKey: Redacted.make(apiKey) })),
    Layer.provide(ReactorTest.layer({ timing: ReactorTest.Timing.hosted, apiKey, ...frameSize })),
    Layer.merge(WebCrypto),
  );
