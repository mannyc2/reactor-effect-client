import { describe, expect, test } from "vitest";
import { inflateSync } from "node:zlib";
import { pngBytes } from "../src/ReactorTest.js";

describe("ReactorTest.pngBytes", () => {
  for (const [width, height] of [
    [1, 1],
    [32, 24],
    [1456, 15],
    [21845, 1],
    [160, 160],
    [512, 128],
  ] as const)
    test(`decodes ${width}x${height} pixels with valid checksums across stored-block boundaries`, () => {
      const bytes = pngBytes({ width, height });
      const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
      expect(Array.from(bytes.subarray(0, 8))).toEqual([137, 80, 78, 71, 13, 10, 26, 10]);
      expect(view.getUint32(16)).toBe(width);
      expect(view.getUint32(20)).toBe(height);
      const types: string[] = [];
      let cursor = 8;
      while (cursor < bytes.length) {
        const length = view.getUint32(cursor);
        const type = new TextDecoder().decode(bytes.subarray(cursor + 4, cursor + 8));
        types.push(type);
        let crc = 0xffffffff;
        for (const byte of bytes.subarray(cursor + 4, cursor + 8 + length)) {
          crc ^= byte;
          for (let bit = 0; bit < 8; bit++) crc = (crc >>> 1) ^ ((crc & 1) === 1 ? 0xedb88320 : 0);
        }
        expect(view.getUint32(cursor + 8 + length)).toBe((crc ^ 0xffffffff) >>> 0);
        if (type === "IDAT") {
          const decoded = inflateSync(bytes.subarray(cursor + 8, cursor + 8 + length));
          expect(decoded.length).toBe(height * (1 + width * 3));
          expect(decoded.every((byte) => byte === 0)).toBe(true);
        }
        cursor += length + 12;
      }
      expect(types).toEqual(["IHDR", "IDAT", "IEND"]);
      expect(cursor).toBe(bytes.length);
    });
});
