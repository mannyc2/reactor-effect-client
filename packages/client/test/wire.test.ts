import { assert, describe, it } from "@effect/vitest";
import { Effect, Encoding, Result } from "effect";
import * as Wire from "../src/internal/wire.js";

const bytes = (hex: string): Uint8Array =>
  Result.getOrThrowWith(Encoding.decodeHex(hex), () => new globalThis.Error(`invalid hex ${hex}`));

/** The failure's reason, or undefined when the effect succeeded. */
const refusal = <A>(effect: Effect.Effect<A, { readonly reason: { readonly _tag: string } }>) =>
  Effect.map(Effect.result(effect), (result) =>
    Result.isFailure(result) ? result.failure.reason._tag : undefined,
  );

/** Nested one `google.protobuf.Struct` inside another `levels` times. */
const nested = (levels: number): Wire.ModelMessage["data"] => {
  let data = {};
  for (let level = 0; level < levels; level++) data = { child: data };
  return data;
};

describe("the wire codec", () => {
  // Written by Google's protobuf 6.33.6, the reference implementation.
  it.effect("reads what the reference implementation wrote", () =>
    Effect.gen(function* () {
      const command = yield* Wire.decode(
        Wire.DataClientMessageSchema,
        bytes(
          "0a19646174615f3138343436373434303733373039353531363135100152ae010a0a7365745f70726f6d707412710a1b0a095f5f70726f746f5f5f120e2a0c0a0a0a0473616665120220010a260a056172726179121d321b0a0220010a0220000a0911000000000000f43f0a0232000a022a000a0a0a046e756c6c120208000a1e0a0670726f6d707412141a126120636174202f20e78cab20f09fa7aa0a001a2d0a05696d61676512240a0475705f391207e59bbe2e706e671a09696d6167652f706e6720ffffffffffffffff7f",
        ),
      );
      assert.strictEqual(command.requestId, "data_18446744073709551615");
      assert.strictEqual(command.kind, Wire.MessageKind.REQUEST);
      assert.strictEqual(command.payload.case, "command");
      if (command.payload.case !== "command") return;
      const { data, uploads } = command.payload.value;
      // A `__proto__` key is dropped rather than made the object's prototype.
      assert.deepStrictEqual(data, {
        array: [true, false, 1.25, [], {}],
        prompt: "a cat / 猫 🧪\n\u0000",
        null: null,
      });
      assert.strictEqual(Object.getPrototypeOf(data), Object.prototype);
      const image = uploads.image;
      assert.deepStrictEqual(image && [image.uploadId, image.name, image.mimeType, image.size], [
        "up_9",
        "图.png",
        "image/png",
        (1n << 63n) - 1n,
      ]);

      const clip = bytes(
        "0a066374726c5f3110025a4b0a08736573735f6162631204736e617019000000000000f43f2152b81e85eb511f402952b81e85eb511f4030ffffffffffffffff7f3a142f636c6970732f736573735f6162632e6d337538",
      );
      const reply = yield* Wire.decode(Wire.ControlServerMessageSchema, clip);
      assert.strictEqual(reply.payload.case, "clipReady");
      if (reply.payload.case !== "clipReady") return;
      assert.strictEqual(reply.payload.value.playlistUrl, "/clips/sess_abc.m3u8");
      assert.strictEqual(reply.payload.value.predictedReadyAtMs, (1n << 63n) - 1n);
      assert.deepStrictEqual(yield* Wire.encode(Wire.ControlServerMessageSchema, reply), clip);
      assert.strictEqual(
        Encoding.encodeHex(
          yield* Wire.encode(Wire.DataServerMessageSchema, {
            requestId: "data_1",
            kind: Wire.MessageKind.RESPONSE,
          }),
        ),
        "0a06646174615f311002",
      );
    }),
  );

  it.effect.each([
    ["a truncated varint", "80"],
    ["field number zero", "00"],
    ["an undefined wire type", "0e"],
    ["a length past the end", "0affffffff7f"],
    ["invalid UTF-8", "0a02c0af"],
    ["an unterminated group", "a306"],
    ["a mismatched group", "a306ac06"],
  ] as const)("refuses %s as Protocol", ([, hex]) =>
    Effect.gen(function* () {
      assert.strictEqual(
        yield* refusal(Wire.decode(Wire.DataServerMessageSchema, bytes(hex))),
        "Protocol",
      );
    }),
  );

  it.effect("bounds untrusted messages by size and nesting", () =>
    Effect.gen(function* () {
      const tooLarge = new Uint8Array(Wire.maxMessageBytes + 1);
      assert.strictEqual(
        yield* refusal(Wire.decode(Wire.DataServerMessageSchema, tooLarge)),
        "Protocol",
      );
      assert.strictEqual(
        yield* refusal(
          Wire.encode(Wire.ModelMessageSchema, { type: "x".repeat(Wire.maxMessageBytes) }),
        ),
        "InvalidInput",
      );
      // Each JSON level is two nested messages, Struct and Value.
      const shallow = yield* Wire.encode(Wire.ModelMessageSchema, { data: nested(40) });
      const deep = yield* Wire.encode(Wire.ModelMessageSchema, { data: nested(60) });
      assert.isUndefined(yield* refusal(Wire.decode(Wire.ModelMessageSchema, shallow)));
      assert.strictEqual(yield* refusal(Wire.decode(Wire.ModelMessageSchema, deep)), "Protocol");
    }),
  );

  it.effect("refuses to encode a value the protocol cannot carry", () =>
    Effect.gen(function* () {
      assert.strictEqual(
        yield* refusal(
          Wire.encode(Wire.UploadReferenceSchema, {
            uploadId: "u",
            name: "n",
            mimeType: "m",
            size: 1n << 63n,
          }),
        ),
        "InvalidInput",
      );
    }),
  );

  it.effect("holds a received Struct to JSON", () =>
    Effect.gen(function* () {
      const message = yield* Wire.decode(
        Wire.ModelMessageSchema,
        yield* Wire.encode(Wire.ModelMessageSchema, {
          type: "result",
          data: { finite: 1, unset: null, infinite: Number.POSITIVE_INFINITY },
        }),
      );
      const data = message.data ?? {};
      assert.strictEqual(yield* refusal(Wire.json(data)), "Protocol");
      assert.deepStrictEqual(yield* Wire.json({ finite: 1, unset: null }), {
        finite: 1,
        unset: null,
      });
    }),
  );
});
