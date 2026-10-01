/**
 * What the decoded media showed, summarized as it arrives: a digest, a luma
 * and a coarse thumbnail per video frame, and levels per audio block. Frames
 * and samples are never kept, except half-size copies inside the windows a
 * check opens around the boundaries it expects, which it may write beside the
 * run for a person to look at, and a showreel's recording, which hands them
 * to ffmpeg as they arrive.
 */
import type * as Cause from "effect/Cause";
import * as Clock from "effect/Clock";
import * as Deferred from "effect/Deferred";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as FileSystem from "effect/FileSystem";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as Queue from "effect/Queue";
import * as Stream from "effect/Stream";
import * as ChildProcess from "effect/unstable/process/ChildProcess";
import * as ChildProcessSpawner from "effect/unstable/process/ChildProcessSpawner";
import { recorder } from "reactor-effect-client/Media";
import type { AudioFrame, Recorded, VideoFormat, VideoFrame } from "reactor-effect-client/Media";
import type { AudioSummary, Jump, Pause, Recording, VideoSummary } from "./Evidence.js";

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
      // A clip ends on one black frame unless the session holds its last frame (H3's
      // `flush_on_clip_end`, on by default), so one of the latest frames may be dark.
      const recent = since
        .slice(-(motionFrames + 1))
        .filter((frame) => frame.lit)
        .slice(-motionFrames);
      if (recent.length < motionFrames)
        return "fewer than eight of the latest nine frames were lit";
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

/** A reel's frame rate: H3's own. */
const reelFps = 24;
const frameMs = 1000 / reelFps;
/** A frame goes to ffmpeg in pieces of at most this many bytes, so counting pieces bounds bytes. */
const pieceBytes = 64 * 1024;
/**
 * The picture waiting for ffmpeg: at most 2048 pieces, 128 MiB, about 32 of
 * hosted H3's 1344x768 frames, or every frame of a rehearsal's small ones. A
 * rehearsal's clock runs ahead while ffmpeg works in real time, and its
 * picture's reader must still never wait on ffmpeg.
 */
const queuedPieces = 2048;
/** The sound waiting for ffmpeg: at most 8192 blocks, 82 s of 10 ms ones. */
const queuedBlocks = 8192;
/** How long ffmpeg waits for the first block of sound, once the picture began, before it records none. */
const soundWait = Duration.seconds(1);

/** A frame or block as it reached a reader. */
interface Arrival<A> {
  readonly element: A;
  /** In epoch milliseconds, on Effect's clock. */
  readonly atMs: number;
}

/** A track's elements as they arrive, until `until` completes; a track that fails ends there. */
const arriving = <A, E>(track: Stream.Stream<A, E>, until: Effect.Effect<unknown>) =>
  track.pipe(
    Stream.catch(() => Stream.empty),
    Stream.mapEffect((element) =>
      Effect.map(Clock.currentTimeMillis, (atMs): Arrival<A> => ({ element, atMs })),
    ),
    Stream.interruptWhen(until),
  );

/** What a recording counts as it goes, to report once ffmpeg is done. */
interface Counts {
  frames: number;
  repeated: number;
  superseded: number;
  mismatched: number;
  blocks: number;
  /** Samples of silence written, a channel's worth each. */
  silence: number;
}

/** A picture's size and pixel format, which raw video holds to throughout. */
interface Shape {
  readonly width: number;
  readonly height: number;
  readonly format: VideoFormat;
}

interface Slots {
  /** The first frame's size and format, which every frame written keeps. */
  readonly shape: Shape | undefined;
  /** The latest frame to arrive, and whether it is yet to be written. */
  readonly last: VideoFrame | undefined;
  readonly fresh: boolean;
  /** The next frame's index from the recording's start. */
  readonly slot: number;
}

/**
 * The picture at 24 frames a second from frames as they arrived: each frame
 * written is the latest to have arrived by the middle of its time, so a held
 * frame at a seam, or frames the host dropped, last as long in the file as
 * they did on air. The first frame also stands for the time before it, back
 * to `fromMs`. A frame of another size or format than the first is dropped
 * and counted: hosted H3 has changed a session's size mid-session, and raw
 * video cannot.
 */
const resample = (fromMs: number, counts: Counts) =>
  Stream.mapAccum(
    (): Slots => ({ shape: undefined, last: undefined, fresh: false, slot: 0 }),
    (state, arrival: Arrival<VideoFrame>): readonly [Slots, ReadonlyArray<VideoFrame>] => {
      const frame = arrival.element;
      const shape = state.shape ?? {
        width: frame.width,
        height: frame.height,
        format: frame.format,
      };
      if (
        frame.width !== shape.width ||
        frame.height !== shape.height ||
        frame.format !== shape.format
      ) {
        counts.mismatched++;
        return [state, []];
      }
      const written: Array<VideoFrame> = [];
      let { fresh, slot } = state;
      if (state.last !== undefined) {
        while (fromMs + (slot + 0.5) * frameMs <= arrival.atMs) {
          written.push(state.last);
          if (!fresh) counts.repeated++;
          fresh = false;
          slot++;
        }
        if (fresh) counts.superseded++;
      }
      return [{ shape, last: frame, fresh: true, slot }, written];
    },
    { onHalt: (state) => (state.last !== undefined && state.fresh ? [state.last] : []) },
  );

/**
 * The sound as 16-bit samples: silence first, so that it starts with the
 * picture at `fromMs` (the first block ends as it arrives), and silence for
 * each block the host dropped. Sound that arrives keeps its own clock.
 */
const align = (fromMs: number, counts: Counts) =>
  Stream.mapAccum(
    (): AudioFrame | undefined => undefined,
    (
      last,
      arrival: Arrival<Recorded<AudioFrame>>,
    ): readonly [AudioFrame | undefined, ReadonlyArray<Int16Array<ArrayBuffer>>] => {
      const element = arrival.element;
      if (element._tag === "Lost") {
        if (last === undefined) return [last, []];
        const count = Number(element.count);
        counts.silence += (count * last.samples.length) / last.channels;
        return [last, Array.from({ length: count }, () => new Int16Array(last.samples.length))];
      }
      const block = element.frame;
      counts.blocks++;
      if (last !== undefined) return [block, [block.samples]];
      const lead =
        Math.round(((arrival.atMs - fromMs) * block.sampleRate) / 1000) -
        block.samples.length / block.channels;
      if (lead <= 0) return [block, [block.samples]];
      counts.silence += lead;
      return [block, [new Int16Array(lead * block.channels), block.samples]];
    },
  );

/** A frame's bytes in pieces of at most `pieceBytes`, sharing its buffer. */
const piecesOf = (frame: VideoFrame): ReadonlyArray<Uint8Array> => {
  const pieces: Array<Uint8Array> = [];
  for (let at = 0; at < frame.data.length; at += pieceBytes)
    pieces.push(frame.data.subarray(at, at + pieceBytes));
  return pieces;
};

/** A recording under way; `finished` waits for ffmpeg to finish the file. */
export interface Recorder {
  readonly finished: Effect.Effect<Recording>;
}

/**
 * Records a picture and its sound to an MP4 through ffmpeg (H.264 and AAC),
 * from `fromMs` on Effect's clock until `until` completes, keeping that
 * clock's timing: 24 frames a second, each the latest to have arrived by its
 * time, with the sound starting with the picture.
 *
 * Both tracks are read at once into queues that ffmpeg's pipes drain, so a
 * reader never waits on the encoder until a queue is full. ffmpeg starts once
 * the first frame and the first block of sound (or a second without one) give
 * it their formats, and finishes the file when the tracks end. The readers
 * and ffmpeg belong to the caller's scope, so the file can be finished after
 * the scope that carried the tracks has closed.
 */
export const record = Effect.fnUntraced(function* <E, E2>(input: {
  readonly file: string;
  readonly video: Stream.Stream<VideoFrame, E>;
  readonly audio: Stream.Stream<AudioFrame, E2>;
  readonly fromMs: number;
  readonly until: Effect.Effect<unknown>;
}) {
  const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
  const counts: Counts = {
    frames: 0,
    repeated: 0,
    superseded: 0,
    mismatched: 0,
    blocks: 0,
    silence: 0,
  };
  const picture = yield* Deferred.make<
    { readonly width: number; readonly height: number; readonly format: VideoFormat } | undefined
  >();
  const sound = yield* Deferred.make<
    { readonly sampleRate: number; readonly channels: number } | undefined
  >();
  const pieces = yield* Queue.bounded<Uint8Array, Cause.Done>(queuedPieces);
  const blocks = yield* Queue.bounded<Uint8Array, Cause.Done>(queuedBlocks);
  yield* arriving(input.video, input.until).pipe(
    resample(input.fromMs, counts),
    Stream.tap((frame) =>
      Effect.andThen(
        Deferred.succeed(picture, {
          width: frame.width,
          height: frame.height,
          format: frame.format,
        }),
        Effect.sync(() => {
          counts.frames++;
        }),
      ),
    ),
    Stream.map(piecesOf),
    Stream.flattenIterable,
    Stream.runIntoQueue(pieces),
    Effect.ensuring(Deferred.succeed(picture, undefined)),
    Effect.forkScoped({ startImmediately: true }),
  );
  yield* arriving(recorder(input.audio), input.until).pipe(
    Stream.tap(({ element }) =>
      element._tag === "Frame"
        ? Deferred.succeed(sound, {
            sampleRate: element.frame.sampleRate,
            channels: element.frame.channels,
          })
        : Effect.void,
    ),
    align(input.fromMs, counts),
    Stream.map((samples) => new Uint8Array(samples.buffer, samples.byteOffset, samples.byteLength)),
    Stream.runIntoQueue(blocks),
    Effect.ensuring(Deferred.succeed(sound, undefined)),
    Effect.forkScoped({ startImmediately: true }),
  );
  const encoder = yield* Effect.gen(function* () {
    const format = yield* Deferred.await(picture);
    if (format === undefined)
      return { width: 0, height: 0, ...frameCounts(counts), failure: "no frame arrived" };
    const audio = yield* Deferred.await(sound).pipe(
      Effect.timeoutOption(soundWait),
      Effect.map(Option.getOrUndefined),
    );
    const exit = yield* Effect.scoped(
      Effect.flatMap(
        spawner.spawn(
          ChildProcess.make(
            "ffmpeg",
            [
              ...["-hide_banner", "-loglevel", "error", "-y"],
              ...["-f", "rawvideo", "-pix_fmt", format.format === "BGRA" ? "bgra" : "rgba"],
              ...["-s", `${format.width}x${format.height}`, "-r", String(reelFps), "-i", "pipe:0"],
              ...(audio === undefined
                ? []
                : [
                    ...["-f", "s16le", "-ar", String(audio.sampleRate)],
                    ...["-ac", String(audio.channels), "-i", "pipe:3"],
                  ]),
              ...["-c:v", "libx264", "-preset", "veryfast", "-crf", "18", "-pix_fmt", "yuv420p"],
              ...(audio === undefined ? [] : ["-c:a", "aac", "-b:a", "160k"]),
              ...["-movflags", "+faststart", input.file],
            ],
            {
              stdin: Stream.fromQueue(pieces),
              stdout: "ignore",
              stderr: "inherit",
              additionalFds:
                audio === undefined
                  ? {}
                  : { fd3: { type: "input", stream: Stream.fromQueue(blocks) } },
            },
          ),
        ),
        (handle) => handle.exitCode,
      ),
    ).pipe(
      Effect.match({
        onFailure: () => ({ failure: "ffmpeg could not run" }),
        onSuccess: (code) => ({ exitCode: Number(code) }),
      }),
    );
    return {
      width: format.width,
      height: format.height,
      ...frameCounts(counts),
      ...(audio === undefined
        ? {}
        : {
            audio: {
              ...audio,
              blocks: counts.blocks,
              silenceMs: round((counts.silence * 1000) / audio.sampleRate),
            },
          }),
      ...exit,
    };
  }).pipe(
    // ffmpeg is gone: nothing more is read into the queues, so their readers wait on nothing.
    Effect.ensuring(Effect.andThen(Queue.shutdown(pieces), Queue.shutdown(blocks))),
    Effect.forkScoped,
  );
  return { finished: Fiber.join(encoder) } satisfies Recorder;
});

const frameCounts = (counts: Counts) => ({
  frames: counts.frames,
  repeated: counts.repeated,
  superseded: counts.superseded,
  mismatched: counts.mismatched,
});

/** The encoders and filters a reel, its poster and its loop need, by how ffmpeg lists each kind. */
const needs = [
  { list: "-encoders", flags: 6, names: ["libx264", "aac", "png", "gif"] },
  { list: "-filters", flags: 3, names: ["fps", "scale", "split", "palettegen", "paletteuse"] },
] as const;

/**
 * Why this machine cannot record a reel, or undefined when it can: ffmpeg must
 * run from the PATH with the encoders and filters that the reel, its poster
 * and its loop use.
 */
export const cannotRecord = Effect.gen(function* () {
  const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
  const lacking: Array<string> = [];
  for (const need of needs) {
    const listed = yield* spawner
      .string(
        ChildProcess.make("ffmpeg", ["-hide_banner", need.list], {
          stdin: "ignore",
          stderr: "ignore",
        }),
      )
      .pipe(Effect.option);
    if (Option.isNone(listed)) return "no ffmpeg runs from the PATH";
    for (const name of need.names)
      if (!new RegExp(`^ [A-Z.]{${need.flags}} ${name} `, "m").test(listed.value))
        lacking.push(name);
  }
  return lacking.length === 0 ? undefined : `ffmpeg lacks ${lacking.join(", ")}`;
});

/** ffmpeg on files, with no input from this process: its exit code, undefined when it cannot run. */
const ffmpeg = (args: ReadonlyArray<string>) =>
  Effect.flatMap(ChildProcessSpawner.ChildProcessSpawner, (spawner) =>
    spawner.exitCode(
      ChildProcess.make(
        "ffmpeg",
        ["-hide_banner", "-loglevel", "error", "-nostdin", "-y", ...args],
        {
          stdin: "ignore",
          stdout: "ignore",
          stderr: "inherit",
        },
      ),
    ),
  ).pipe(
    Effect.match({
      onFailure: (): number | undefined => undefined,
      onSuccess: (code) => Number(code),
    }),
  );

/** One frame of `reel`, `atSeconds` in, as a PNG of the reel's size. */
export const poster = (input: {
  readonly reel: string;
  readonly file: string;
  readonly atSeconds: number;
}) =>
  ffmpeg([
    ...["-ss", input.atSeconds.toFixed(3), "-i", input.reel],
    ...["-frames:v", "1", "-update", "1", input.file],
  ]);

/**
 * A loop for a README: `seconds` of `reel` from `fromSeconds`, 720 pixels
 * wide at 12 frames a second, as a GIF with a palette made from those frames,
 * each frame after the first storing only the rectangle that changed.
 */
export const loop = (input: {
  readonly reel: string;
  readonly file: string;
  readonly fromSeconds: number;
  readonly seconds: number;
}) =>
  ffmpeg([
    ...["-ss", input.fromSeconds.toFixed(3), "-t", input.seconds.toFixed(3), "-i", input.reel],
    "-filter_complex",
    "[0:v]fps=12,scale=720:-1:flags=lanczos,split[a][b];[a]palettegen=stats_mode=diff[p];[b][p]paletteuse=dither=bayer:bayer_scale=5:diff_mode=rectangle",
    ...["-loop", "0", input.file],
  ]);
