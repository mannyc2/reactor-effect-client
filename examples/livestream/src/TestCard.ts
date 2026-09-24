import { Clock, Effect } from "effect";
import type * as Orchestration from "reactor-effect-client/orchestration";
import type * as Simulation from "reactor-effect-client/simulation";

/** The H3 profile's frame rate, its 16:9 canvas and its 48 kHz mono audio. */
const fps = 24;
const sampleRate = 48_000;
const samplesPerFrame = sampleRate / fps;

const hash = (text: string): number => {
  let value = 2166136261;
  for (let index = 0; index < text.length; index++)
    value = Math.imul(value ^ text.charCodeAt(index), 16777619);
  return value >>> 0;
};

/** An opaque BGRA pixel as one little-endian 32-bit word: bytes B, G, R, A. */
const bgra = (hue: number, value: number): number => {
  const channel = (n: number) => {
    const k = (n + hue / 60) % 6;
    return Math.round(255 * value * (1 - 0.55 * Math.max(0, Math.min(k, 4 - k, 1))));
  };
  return ((255 << 24) | (channel(5) << 16) | (channel(3) << 8) | channel(1)) >>> 0;
};

/**
 * An unpaid stand-in for the model: `Simulation.source`'s `present` hook draws
 * each clip as a moving test card (its color from the prompt, a sweeping band
 * and a progress bar) with a tone, in real time. Make one per source: frames
 * and audio blocks are numbered on their track from 0 for the source's life,
 * as a host numbers them for one connection generation.
 */
export const make = (width = 1344, height = 768) => {
  let videoSequence = 0n;
  let audioSequence = 0n;
  let phase = 0;
  const present: NonNullable<Simulation.SimOptions["present"]> = Effect.fnUntraced(function* (
    record: Orchestration.LocalClipRecord,
    startedAt: number,
    sink: Simulation.SimulatedMediaSink,
  ) {
    const seed = hash(record.request.prompt);
    const background = bgra(seed % 360, 0.45);
    const band = bgra(seed % 360, 0.9);
    const tone = (2 * Math.PI * 220 * 2 ** ((seed % 12) / 12)) / sampleRate;
    const frames = Math.round(record.durationSeconds * fps);
    for (let index = 0; index < frames; index++) {
      const data = new Uint8Array(width * height * 4);
      const pixels = new Uint32Array(data.buffer);
      pixels.fill(background);
      const x = Math.floor((index * 12) % width);
      const barEnd = Math.floor((width * (index + 1)) / frames);
      for (let row = 0; row < height; row++) {
        const start = row * width;
        pixels.fill(band, start + x, start + Math.min(width, x + 96));
        if (row >= height - 24) pixels.fill(band, start, start + barEnd);
      }
      const samples = new Int16Array(samplesPerFrame);
      for (let sample = 0; sample < samples.length; sample++) {
        samples[sample] = Math.round(Math.sin(phase) * 3000);
        phase = (phase + tone) % (2 * Math.PI);
      }
      yield* sink.video({
        _tag: "VideoFrame",
        track: "main_video",
        width,
        height,
        frameId: videoSequence + 1n,
        timestampMicros: 0n,
        sequence: videoSequence++,
        format: "BGRA",
        data,
        metadata: new Uint8Array(0),
      });
      yield* sink.audio({
        _tag: "AudioFrame",
        track: "main_audio",
        sampleRate,
        channels: 1,
        sequence: audioSequence++,
        samples,
      });
      // Present in real time: frame n is due n/24 s after the clip started.
      const due = startedAt + ((index + 1) * 1000) / fps;
      const now = yield* Clock.currentTimeMillis;
      if (due > now) yield* Effect.sleep(due - now);
    }
  });
  return { present };
};
