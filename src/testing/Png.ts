import { encodeBase64 } from "effect/Encoding";

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

export const pngBytes = (width: number, height: number): Uint8Array => {
  const crcTable = new Uint32Array(256).map((_, n) => {
    let c = n;
    for (let k = 0; k < 8; k++) c = c & 1 ? 0xedb88320 ^ (c >>> 1) : c >>> 1;
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

export const dataUri = (bytes: Uint8Array) => `data:image/png;base64,${encodeBase64(bytes)}`;
