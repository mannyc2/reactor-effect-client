/**
 * What the decoded media showed, summarized as it arrives: a digest, a luma
 * and a coarse thumbnail per video frame, and levels per audio block. Frames
 * and samples are never kept, except half-size copies inside the windows a
 * check opens around the boundaries it expects, which it may write beside the
 * run for a person to look at.
 */
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import type { AudioFrame, Recorded, VideoFrame } from "reactor-effect-client/Media";
import type { AudioSummary, Jump, Pause, VideoSummary } from "./Evidence.js";

const round = (value: number, places = 1) => Math.round(value * 10 ** places) / 10 ** places;

const spread = (values: ReadonlyArray<number>) => {
  const sorted = [...values].sort((a, b) => a - b);
  const pick = (q: number) =>
    sorted[Math.min(sorted.length - 1, Math.floor(q * sorted.length))] ?? 0;
  return { p50: round(pick(0.5)), p95: round(pick(0.95)), max: round(sorted.at(-1) ?? 0) };
};

/** Mean luma above which a frame counts as lit: a decoder's black is not exactly zero. */
const litLuma = 12;
/** Lit frames a live clip must still show at the end of its window, and change across. */
export const motionFrames = 8;
const thumbColumns = 64;
const thumbRows = 36;

interface Seen {
  readonly atMs: number;
  readonly digest: number;
  readonly lit: boolean;
  readonly format: string;
  readonly thumb: Uint8Array;
}

/** BGRA luma at one pixel. */
const luma = (data: Uint8Array, index: number) =>
  0.0722 * (data[index] ?? 0) + 0.7152 * (data[index + 1] ?? 0) + 0.2126 * (data[index + 2] ?? 0);

/** FNV-1a over about 16K sampled pixels, their mean luma, and a 64×36 luma thumbnail. */
const sample = (frame: VideoFrame) => {
  const { data, width, height } = frame;
  const pixels = Math.floor(data.length / 4);
  const step = Math.max(1, Math.floor(pixels / 16_384));
  let hash = 2166136261;
  let total = 0;
  let count = 0;
  for (let pixel = 0; pixel < pixels; pixel += step) {
    const index = pixel * 4;
    for (let channel = 0; channel < 3; channel++)
      hash = Math.imul(hash ^ (data[index + channel] ?? 0), 16777619);
    total += luma(data, index);
    count++;
  }
  const thumb = new Uint8Array(thumbColumns * thumbRows);
  for (let row = 0; row < thumbRows; row++) {
    const y = Math.min(height - 1, Math.floor(((row + 0.5) * height) / thumbRows));
    for (let column = 0; column < thumbColumns; column++) {
      const x = Math.min(width - 1, Math.floor(((column + 0.5) * width) / thumbColumns));
      thumb[row * thumbColumns + column] = Math.round(luma(data, (y * width + x) * 4));
    }
  }
  return { digest: hash >>> 0, luma: count === 0 ? 0 : total / count, thumb };
};

/** Mean absolute luma difference between two thumbnails, 0 to 255. */
const difference = (a: Uint8Array, b: Uint8Array) => {
  let total = 0;
  for (let index = 0; index < a.length; index++)
    total += Math.abs((a[index] ?? 0) - (b[index] ?? 0));
  return total / a.length;
};

/** RGB pixels, rows packed. */
export interface Image {
  readonly width: number;
  readonly height: number;
  readonly rgb: Uint8Array;
}

/** Half resolution, as RGB. */
const half = (frame: VideoFrame): Image => {
  const width = Math.floor(frame.width / 2);
  const height = Math.floor(frame.height / 2);
  const rgb = new Uint8Array(width * height * 3);
  for (let y = 0; y < height; y++)
    for (let x = 0; x < width; x++) {
      const from = (y * 2 * frame.width + x * 2) * 4;
      const to = (y * width + x) * 3;
      rgb[to] = frame.data[from + 2] ?? 0;
      rgb[to + 1] = frame.data[from + 1] ?? 0;
      rgb[to + 2] = frame.data[from] ?? 0;
    }
  return { width, height, rgb };
};

/** A video track as it arrived. Bounded: at most 100,000 frames are remembered. */
export const videoLog = () => {
  const seen: Array<Seen> = [];
  const windows = new Map<
    number,
    {
      readonly fromMs: number;
      readonly toMs: number;
      readonly frames: Array<{ readonly atMs: number; readonly image: Image }>;
    }
  >();
  const formats = new Set<string>();
  const sizes = new Set<string>();
  const digests = new Set<number>();
  let frames = 0;
  let lit = 0;
  let lost = 0;
  let gaps = 0;
  let nextWindow = 0;
  const within = (fromMs: number, toMs: number) =>
    seen.filter((frame) => frame.atMs >= fromMs && frame.atMs <= toMs);
  return {
    add: (element: Recorded<VideoFrame>, atMs: number): void => {
      if (element._tag === "Lost") {
        gaps++;
        lost += Number(element.count);
        return;
      }
      const frame = element.frame;
      frames++;
      formats.add(frame.format);
      sizes.add(`${frame.width}x${frame.height}`);
      const sampled = sample(frame);
      if (sampled.luma > litLuma) lit++;
      if (seen.length < 100_000) {
        digests.add(sampled.digest);
        seen.push({
          atMs,
          digest: sampled.digest,
          lit: sampled.luma > litLuma,
          format: frame.format,
          thumb: sampled.thumb,
        });
      }
      for (const window of windows.values())
        if (atMs >= window.fromMs && atMs <= window.toMs && window.frames.length < 120)
          window.frames.push({ atMs, image: half(frame) });
    },
    get count() {
      return frames;
    },
    firstAfter: (atMs: number) => seen.find((frame) => frame.atMs >= atMs)?.atMs,
    framesBetween: (fromMs: number, toMs: number) => within(fromMs, toMs).length,
    /** Frames that arrived at or after `atMs`: changing, lit, BGRA, and still moving at the end. */
    live: (atMs: number): string | undefined => {
      const since = seen.filter((frame) => frame.atMs >= atMs);
      if (since.length < 2) return "fewer than two frames arrived";
      if (since.some((frame) => frame.format !== "BGRA")) return "a frame was not BGRA";
      if (!since.some((frame) => frame.lit)) return "every frame was black";
      if (new Set(since.map((frame) => frame.digest)).size < 2) return "the frames never changed";
      const recent = since.slice(-motionFrames).filter((frame) => frame.lit);
      if (recent.length < motionFrames) return "fewer than eight recent lit frames arrived";
      let changes = 0;
      for (let index = 1; index < recent.length; index++)
        if (recent[index]?.digest !== recent[index - 1]?.digest) changes++;
      return changes > 1 ? undefined : "the recent frames did not keep changing";
    },
    dark: (fromMs: number, toMs: number) =>
      within(fromMs, toMs).filter((frame) => !frame.lit).length,
    /** The longest stretch from `fromMs` to `toMs` with no new picture. */
    pause: (fromMs: number, toMs: number): Pause | undefined => {
      const window = within(fromMs, toMs);
      const changed: Array<number> = [];
      for (let index = 1; index < window.length; index++)
        if (window[index]?.digest !== window[index - 1]?.digest) changed.push(index);
      let longest: Pause | undefined;
      for (let index = 1; index < changed.length; index++) {
        const from = changed[index - 1] ?? 0;
        const to = changed[index] ?? 0;
        const durationMs = (window[to]?.atMs ?? 0) - (window[from]?.atMs ?? 0);
        if (longest !== undefined && durationMs <= longest.durationMs) continue;
        const inside = window.slice(from + 1, to);
        longest = {
          lastNewFrameMs: window[from]?.atMs ?? 0,
          firstNewFrameMs: window[to]?.atMs ?? 0,
          durationMs,
          frames: inside.length,
          dark: inside.filter((frame) => !frame.lit).length,
        };
      }
      return longest;
    },
    /** The largest change from `fromMs` to `toMs`, against the median change of the 2 s before it. */
    jump: (fromMs: number, toMs: number): Jump | undefined => {
      let best: { readonly index: number; readonly change: number } | undefined;
      for (let index = 1; index < seen.length; index++) {
        const frame = seen[index];
        const previous = seen[index - 1];
        if (
          frame === undefined ||
          previous === undefined ||
          frame.atMs < fromMs ||
          frame.atMs > toMs
        )
          continue;
        const change = difference(frame.thumb, previous.thumb);
        if (best === undefined || change > best.change) best = { index, change };
      }
      if (best === undefined) return undefined;
      const atMs = seen[best.index]?.atMs ?? 0;
      const changes: Array<number> = [];
      for (let index = 1; index < best.index; index++) {
        const frame = seen[index];
        const previous = seen[index - 1];
        if (frame !== undefined && previous !== undefined && frame.atMs >= atMs - 2_000)
          changes.push(difference(frame.thumb, previous.thumb));
      }
      changes.sort((a, b) => a - b);
      const typical = changes[Math.floor(changes.length / 2)] ?? 0;
      return {
        atMs,
        change: round(best.change, 2),
        typical: round(typical, 2),
        ratio: round(best.change / Math.max(typical, 0.25), 2),
      };
    },
    /** Keeps half-size frames arriving from `fromMs` to `toMs`; the window's id. */
    watch: (fromMs: number, toMs: number): number => {
      const id = nextWindow++;
      windows.set(id, { fromMs, toMs, frames: [] });
      return id;
    },
    /** Frees a window, returning the kept frames on either side of `atMs`. */
    release: (id: number, atMs?: number) => {
      const window = windows.get(id);
      windows.delete(id);
      if (window === undefined || atMs === undefined) return undefined;
      const before = window.frames.findLast((frame) => frame.atMs < atMs);
      const after = window.frames.find((frame) => frame.atMs >= atMs);
      return before === undefined || after === undefined
        ? undefined
        : ([before.image, after.image] as const);
    },
    summary: (): VideoSummary => {
      const arrivals = seen.map((frame) => frame.atMs);
      const intervals = arrivals.slice(1).map((at, index) => at - (arrivals[index] ?? at));
      const first = arrivals[0];
      const last = arrivals.at(-1);
      const elapsed = first === undefined || last === undefined ? 0 : last - first;
      return {
        frames,
        formats: [...formats],
        sizes: [...sizes],
        ...(first === undefined ? {} : { firstFrameMs: first }),
        ...(elapsed > 0 ? { fps: round(((arrivals.length - 1) * 1000) / elapsed) } : {}),
        ...(intervals.length === 0 ? {} : { interval: spread(intervals) }),
        lit,
        distinct: digests.size,
        lost,
        gaps,
      };
    },
  };
};
export type VideoLog = ReturnType<typeof videoLog>;

/** An audio track as it arrived. */
export const audioLog = () => {
  const rates = new Set<number>();
  const channels = new Set<number>();
  let blocks = 0;
  let lost = 0;
  let peak = 0;
  return {
    add: (element: Recorded<AudioFrame>): void => {
      if (element._tag === "Lost") {
        lost += Number(element.count);
        return;
      }
      const block = element.frame;
      blocks++;
      rates.add(block.sampleRate);
      channels.add(block.channels);
      let sum = 0;
      for (const value of block.samples) sum += (value / 32768) ** 2;
      peak = Math.max(peak, Math.sqrt(sum / Math.max(1, block.samples.length)));
    },
    get count() {
      return blocks;
    },
    summary: (): AudioSummary => ({
      blocks,
      sampleRates: [...rates],
      channels: [...channels],
      peakRms: round(peak, 4),
      lost,
    }),
  };
};
export type AudioLog = ReturnType<typeof audioLog>;

const crcTable = Array.from({ length: 256 }, (_, n) => {
  let c = n;
  for (let k = 0; k < 8; k++) c = (c & 1) === 1 ? 0xedb88320 ^ (c >>> 1) : c >>> 1;
  return c >>> 0;
});
const crc32 = (bytes: Uint8Array) => {
  let c = 0xffffffff;
  for (const byte of bytes) c = (crcTable[(c ^ byte) & 0xff] ?? 0) ^ (c >>> 8);
  return (c ^ 0xffffffff) >>> 0;
};

/** An RGB PNG, its pixels in stored DEFLATE blocks: no compressor needed. */
export const png = ({ width, height, rgb }: Image): Uint8Array => {
  const raw = new Uint8Array(height * (width * 3 + 1));
  for (let y = 0; y < height; y++)
    raw.set(rgb.subarray(y * width * 3, (y + 1) * width * 3), y * (width * 3 + 1) + 1);
  const blocks = Math.max(1, Math.ceil(raw.length / 65535));
  const deflate = new Uint8Array(2 + raw.length + blocks * 5 + 4);
  const view = new DataView(deflate.buffer);
  deflate.set([0x78, 0x01]);
  let cursor = 2;
  let a = 1;
  let b = 0;
  for (let block = 0, offset = 0; block < blocks; block++) {
    const length = Math.min(65535, raw.length - offset);
    deflate[cursor] = block === blocks - 1 ? 1 : 0;
    view.setUint16(cursor + 1, length, true);
    view.setUint16(cursor + 3, length ^ 0xffff, true);
    deflate.set(raw.subarray(offset, offset + length), cursor + 5);
    cursor += 5 + length;
    offset += length;
  }
  for (const byte of raw) {
    a = (a + byte) % 65521;
    b = (b + a) % 65521;
  }
  view.setUint32(cursor, ((b << 16) | a) >>> 0);
  const chunk = (type: string, data: Uint8Array) => {
    const out = new Uint8Array(12 + data.length);
    const chunkView = new DataView(out.buffer);
    chunkView.setUint32(0, data.length);
    out.set(new TextEncoder().encode(type), 4);
    out.set(data, 8);
    chunkView.setUint32(8 + data.length, crc32(out.subarray(4, 8 + data.length)));
    return out;
  };
  const header = new Uint8Array(13);
  const headerView = new DataView(header.buffer);
  headerView.setUint32(0, width);
  headerView.setUint32(4, height);
  header.set([8, 2, 0, 0, 0], 8);
  const parts = [
    new Uint8Array([0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a]),
    chunk("IHDR", header),
    chunk("IDAT", deflate),
    chunk("IEND", new Uint8Array(0)),
  ];
  const out = new Uint8Array(parts.reduce((total, part) => total + part.length, 0));
  let at = 0;
  for (const part of parts) {
    out.set(part, at);
    at += part.length;
  }
  return out;
};

/** A solid mid-gray image: a reference that cannot make a clip black. */
export const grayPng = (size: { readonly width: number; readonly height: number }): Uint8Array =>
  png({ ...size, rgb: new Uint8Array(size.width * size.height * 3).fill(128) });

/** A mono 16-bit WAV tone. */
export const tone = (input: {
  readonly seconds: number;
  readonly sampleRate: number;
  readonly frequency: number;
}): Uint8Array => {
  const samples = Math.round(input.seconds * input.sampleRate);
  const out = new Uint8Array(44 + samples * 2);
  const view = new DataView(out.buffer);
  const text = (at: number, value: string) => out.set(new TextEncoder().encode(value), at);
  text(0, "RIFF");
  view.setUint32(4, 36 + samples * 2, true);
  text(8, "WAVEfmt ");
  view.setUint32(16, 16, true);
  view.setUint16(20, 1, true);
  view.setUint16(22, 1, true);
  view.setUint32(24, input.sampleRate, true);
  view.setUint32(28, input.sampleRate * 2, true);
  view.setUint16(32, 2, true);
  view.setUint16(34, 16, true);
  text(36, "data");
  view.setUint32(40, samples * 2, true);
  for (let index = 0; index < samples; index++)
    view.setInt16(
      44 + index * 2,
      Math.round(8000 * Math.sin((2 * Math.PI * input.frequency * index) / input.sampleRate)),
      true,
    );
  return out;
};

/** Writes a seam's two sides as `<label>-before.png` and `<label>-after.png`; the file names. */
export const writeSeam = (input: {
  readonly directory: string;
  readonly label: string;
  readonly images: readonly [Image, Image];
}) =>
  Effect.gen(function* () {
    const { directory, label, images } = input;
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    yield* fs.makeDirectory(directory, { recursive: true });
    const names = [`${label}-before.png`, `${label}-after.png`] as const;
    yield* Effect.forEach([0, 1] as const, (index) =>
      fs.writeFile(path.join(directory, names[index]), png(images[index])),
    );
    return names;
  });
