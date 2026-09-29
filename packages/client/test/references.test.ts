/** The reference media ReactorTest makes: a PNG any decoder reads. */
import { assert, describe, it } from "@effect/vitest";
import { inflateSync } from "node:zlib";
import { pngBytes } from "../src/ReactorTest.js";

describe("ReactorTest.pngBytes", () => {
  it.each([
    [1, 1],
    [32, 24],
    [1456, 15],
    [21845, 1],
    [160, 160],
    [512, 128],
  ] as const)(
    "decodes %ix%i pixels with valid checksums across stored-block boundaries",
    (width, height) => {
      const bytes = pngBytes({ width, height });
      const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
      assert.deepStrictEqual(Array.from(bytes.subarray(0, 8)), [137, 80, 78, 71, 13, 10, 26, 10]);
      assert.deepStrictEqual([view.getUint32(16), view.getUint32(20)], [width, height]);
      const types: Array<string> = [];
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
        assert.strictEqual(view.getUint32(cursor + 8 + length), (crc ^ 0xffffffff) >>> 0);
        if (type === "IDAT") {
          const decoded = inflateSync(bytes.subarray(cursor + 8, cursor + 8 + length));
          assert.strictEqual(decoded.length, height * (1 + width * 3));
          assert.isTrue(decoded.every((byte) => byte === 0));
        }
        cursor += length + 12;
      }
      assert.deepStrictEqual(types, ["IHDR", "IDAT", "IEND"]);
      assert.strictEqual(cursor, bytes.length);
    },
  );
});
