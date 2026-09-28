/**
 * Reference media for tests: a black PNG and a WAV tone that H3's local checks
 * accept, built without a compressor or a platform codec so they run anywhere.
 */
/** Opaque black fixture pixels need no general-purpose compressor. Stored
 * DEFLATE blocks (RFC 1951 §3.2.4) keep this synchronous helper host-neutral. */
const zeroPixels = (size: number): Uint8Array => {
  const raw = new Uint8Array(size);
  const blocks = Math.max(1, Math.ceil(raw.length / 65535));
  const output = new Uint8Array(2 + raw.length + blocks * 5 + 4);
  const view = new DataView(output.buffer);
  output.set([0x78, 0x01]); // RFC 1950: DEFLATE, no preset dictionary.
  let cursor = 2;
  for (let block = 0, offset = 0; block < blocks; block++) {
    const length = Math.min(65535, raw.length - offset);
    output[cursor] = block === blocks - 1 ? 1 : 0;
    view.setUint16(cursor + 1, length, true);
    view.setUint16(cursor + 3, length ^ 0xffff, true);
    // The freshly allocated payload is already zero, including each row's filter byte.
    cursor += 5 + length;
    offset += length;
  }
  // Adler-32 for N zero bytes: s1 = 1, s2 = N mod 65521.
  view.setUint32(cursor, (raw.length % 65521) * 65536 + 1);
  return output;
};

/** A complete black PNG image of the given size: an image reference H3 accepts. */
export const pngBytes = ({
  width,
  height,
}: {
  readonly width: number;
  readonly height: number;
}): Uint8Array => {
  const crcTable = new Uint32Array(256).map((_, n) => {
    let c = n;
    for (let k = 0; k < 8; k++) c = (c & 1) === 1 ? 0xedb88320 ^ (c >>> 1) : c >>> 1;
    return c >>> 0;
  });
  const crc = (bytes: Uint8Array) => {
    let c = 0xffffffff;
    for (const b of bytes) c = crcTable[(c ^ b) & 0xff]! ^ (c >>> 8);
    return (c ^ 0xffffffff) >>> 0;
  };
  const chunk = (type: string, data: Uint8Array) => {
    const out = new Uint8Array(12 + data.length);
    const view = new DataView(out.buffer);
    view.setUint32(0, data.length);
    out.set(new TextEncoder().encode(type), 4);
    out.set(data, 8);
    view.setUint32(8 + data.length, crc(out.subarray(4, 8 + data.length)));
    return out;
  };
  const ihdr = new Uint8Array(13);
  const v = new DataView(ihdr.buffer);
  v.setUint32(0, width);
  v.setUint32(4, height);
  ihdr[8] = 8;
  ihdr[9] = 2;
  ihdr[10] = 0;
  ihdr[11] = 0;
  ihdr[12] = 0;
  const idat = chunk("IDAT", zeroPixels(height * (1 + width * 3)));
  const signature = new Uint8Array([0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a]);
  const parts = [signature, chunk("IHDR", ihdr), idat, chunk("IEND", new Uint8Array())];
  const total = parts.reduce((n, p) => n + p.length, 0);
  const out = new Uint8Array(total);
  let o = 0;
  for (const p of parts) {
    out.set(p, o);
    o += p.length;
  }
  return out;
};

export interface WavOptions {
  /** The clip's length. */
  readonly seconds: number;
  /** Samples a second, 16,000 by default. */
  readonly sampleRate?: number;
  /** 1 (mono, the default) or 2. */
  readonly channels?: number;
  /** The tone's pitch in hertz, 440 by default; 0 writes silence. */
  readonly frequency?: number;
}

/**
 * A 16-bit PCM WAV of a steady tone, `seconds` long: an audio reference whose
 * header gives its length and channels, so H3's local checks read them.
 */
export const wavBytes = (options: WavOptions): Uint8Array => {
  const seconds = options.seconds;
  const sampleRate = options.sampleRate ?? 16_000;
  const channels = options.channels ?? 1;
  const frequency = options.frequency ?? 440;
  const frames = Math.round(seconds * sampleRate);
  const data = frames * channels * 2;
  const bytes = new Uint8Array(44 + data);
  const view = new DataView(bytes.buffer);
  const text = (offset: number, value: string) => {
    for (let i = 0; i < value.length; i++) bytes[offset + i] = value.charCodeAt(i);
  };
  text(0, "RIFF");
  view.setUint32(4, 36 + data, true);
  text(8, "WAVE");
  text(12, "fmt ");
  view.setUint32(16, 16, true);
  view.setUint16(20, 1, true); // PCM
  view.setUint16(22, channels, true);
  view.setUint32(24, sampleRate, true);
  view.setUint32(28, sampleRate * channels * 2, true);
  view.setUint16(32, channels * 2, true);
  view.setUint16(34, 16, true);
  text(36, "data");
  view.setUint32(40, data, true);
  for (let frame = 0; frame < frames; frame++) {
    const sample = Math.round(8_000 * Math.sin((2 * Math.PI * frequency * frame) / sampleRate));
    for (let channel = 0; channel < channels; channel++)
      view.setInt16(44 + (frame * channels + channel) * 2, sample, true);
  }
  return bytes;
};
