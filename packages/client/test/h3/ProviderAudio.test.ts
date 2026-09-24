import { readFileSync } from "node:fs";
import { describe, expect, test } from "vitest";
import { Effect, Result } from "effect";
import * as H3 from "../../src/h3/index.js";
import { validateDeployment } from "../../src/h3/_internal/deployment.js";
import { CommandFailure } from "../../src/session/commands.js";
import type { JsonObject } from "../../src/json.js";
import { pngBytes } from "../../src/testing/Png.js";
import { wavBytes } from "../../src/testing/Wav.js";
import { fixture } from "./ProviderSession.js";
import type { Script } from "./ProviderSession.js";
import { at, providerSchema } from "./ProviderSchema.js";
import { run } from "./Clock.js";

/** The provider's published schema, frozen as the record this adapter follows. */
const frozen = readFileSync(new URL("./upstream/h3-schema.md", import.meta.url), "utf8").replaceAll(
  /\s+/g,
  " ",
);

const options: H3.Options = { replyTimeout: 80, setupTimeout: 500, reconcileWindow: 30 };
const setup = (script: Script = {}) =>
  Effect.gen(function* () {
    const fake = yield* fixture(script);
    const provider = yield* H3.make(fake.session, options);
    return { fake, provider };
  });
const image = (): H3.Reference => ({ _tag: "Bytes", bytes: pngBytes(64, 48) });
const voice = (seconds = 3, channels = 1): H3.Reference => ({
  _tag: "Bytes",
  bytes: wavBytes(seconds, { channels }),
});
const request = (fields: Partial<H3.Request> = {}): H3.Request => ({
  prompt: "Audio 1 is the host's voice; Picture 1 is the host.",
  seconds: 7,
  ...fields,
});
const enqueueArgs = (fake: { calls: readonly { command: string; args: JsonObject }[] }) =>
  fake.calls.filter((call) => call.command === "enqueue").map((call) => call.args);
const enqueueSchema = (document: JsonObject) =>
  at(
    document,
    "paths",
    "/events/enqueue",
    "post",
    "requestBody",
    "content",
    "application/json",
    "schema",
    "properties",
  );
/** Bytes whose container is recognized but whose length only H3 can read. */
const headed = (...head: number[]): H3.Reference => {
  const bytes = new Uint8Array(4096);
  bytes.set(head);
  return { _tag: "Bytes", bytes };
};
const flac = (seconds: number, channels: number, rate = 44_100): H3.Reference => {
  const bytes = new Uint8Array(64);
  bytes.set([0x66, 0x4c, 0x61, 0x43, 0x80, 0, 0, 34]);
  const samples = Math.round(seconds * rate);
  // STREAMINFO from byte 8: rate (20 bits), channels - 1 (3), bits - 1 (5), samples (36).
  bytes[18] = (rate >> 12) & 255;
  bytes[19] = (rate >> 4) & 255;
  bytes[20] = ((rate & 15) << 4) | ((channels - 1) << 1);
  bytes[21] = (15 << 4) | Math.floor(samples / 0x100000000);
  new DataView(bytes.buffer).setUint32(22, samples >>> 0);
  return { _tag: "Bytes", bytes };
};
const ogg = (channels: number): H3.Reference => {
  const bytes = new Uint8Array(4096);
  bytes.set([0x4f, 0x67, 0x67, 0x53]);
  bytes[26] = 1;
  bytes[27] = 19;
  bytes.set([...new TextEncoder().encode("OpusHead"), 1, channels], 28);
  return { _tag: "Bytes", bytes };
};

describe("H3 reference audio against the frozen schema", () => {
  test("the adapter's audio bounds are the ones the frozen schema states", () => {
    const limits = H3.audioReferenceLimits;
    expect(frozen).toContain("up to three optional reference audio clips");
    expect(limits.maxAudio).toBe(3);
    expect(frozen).toContain("A clip carries at most twelve references in total");
    expect(limits.maxTotal).toBe(12);
    expect(frozen).toContain("so it accepts at most two of your own");
    expect(limits.maxAudioWithContinuation).toBe(2);
    expect(frozen).toContain(
      "Each audio reference is a WAV, MP3, AAC/M4A, OGG/Opus, FLAC, or WebM upload, at most 25 MiB, mono or stereo, and 2–15 seconds long.",
    );
    expect([limits.maxBytes, limits.minSeconds, limits.maxSeconds, limits.maxChannels]).toEqual([
      25 * 1024 * 1024,
      2,
      15,
      2,
    ]);
    expect(frozen).toContain(
      "A clip that has audio needs at least one image, or a `continue_from_clip_id` whose clip the session still holds.",
    );
    expect(frozen).toContain("`reference_audios` | Upload reference array or null");
    expect(frozen).toContain("`has_reference_audio` | boolean | No");
    expect(frozen).toContain("`reference_audio_count` | integer | No");
    expect(H3.h3ReferenceTurboRealtime.audioReferences).toMatchObject({
      max: 3,
      maxWithContinuation: 2,
      maxTotal: 12,
    });
  });

  test("a deployment that declares reference_audios admits audio; one that does not is still admitted", () => {
    expect(validateDeployment(providerSchema()).referenceAudio).toBe(true);
    const without = providerSchema();
    delete enqueueSchema(without).reference_audios;
    delete enqueueSchema(without).reference_audio;
    const contract = validateDeployment(without);
    expect(contract.referenceAudio).toBe(false);
    expect(contract.subset).toBe("prompt-and-images");
  });

  test("a deployment declaring reference_audios in another shape, or fewer than three, is refused", () => {
    const shapes: readonly JsonObject[] = [
      { type: "string" },
      { type: "array", items: { type: "string" } },
      {
        type: "array",
        maxItems: 2,
        items: { $ref: "#/components/schemas/ReactorUploadReference" },
      },
      {
        type: "array",
        minItems: 1,
        items: { $ref: "#/components/schemas/ReactorUploadReference" },
      },
    ];
    for (const shape of shapes) {
      const document = providerSchema();
      enqueueSchema(document).reference_audios = shape;
      expect(() => validateDeployment(document)).toThrow(
        /reference_audios|Deployment does not structurally support/,
      );
    }
    const required = providerSchema();
    at(
      required,
      "paths",
      "/events/enqueue",
      "post",
      "requestBody",
      "content",
      "application/json",
      "schema",
    ).required = ["reference_audios"];
    expect(() => validateDeployment(required)).toThrow(/unsupported required argument/);
  });

  test("audio uploads with its container's MIME type and is sent as reference_audios, which the clip reports", () =>
    run(
      Effect.gen(function* () {
        const { fake, provider } = yield* setup();
        const accepted = yield* provider.enqueue(
          request({ references: [image()], audio: [voice(), voice(4)] }),
        );
        expect(fake.uploaded.map((upload) => upload.mimeType)).toEqual([
          "image/png",
          "audio/wav",
          "audio/wav",
        ]);
        const [args] = enqueueArgs(fake);
        const audio = args!.reference_audios as readonly JsonObject[];
        expect(audio).toHaveLength(2);
        expect(audio[0]!.mime_type).toBe("audio/wav");
        expect(audio[0]!.upload_id).not.toBe(audio[1]!.upload_id);
        expect(Object.keys(audio[0]!).sort()).toEqual(["mime_type", "name", "size", "upload_id"]);
        expect(args!.reference_audio).toBeUndefined();
        expect(accepted.clip.has_reference_audio).toBe(true);
        expect(accepted.clip.reference_audio_count).toBe(2);
      }),
    ));

  test("a request without audio sends no reference_audios, as before", () =>
    run(
      Effect.gen(function* () {
        const { fake, provider } = yield* setup();
        yield* provider.enqueue(request({ references: [image()] }));
        yield* provider.enqueue(request({ references: [image()], audio: [] }));
        for (const args of enqueueArgs(fake))
          expect(Object.hasOwn(args, "reference_audios")).toBe(false);
      }),
    ));

  test("identical audio bytes upload once and a validated reference is reused without revalidation", () =>
    run(
      Effect.gen(function* () {
        const { fake, provider } = yield* setup();
        const validated = yield* H3.validateAudioReference(voice());
        expect(validated).toMatchObject({ mimeType: "audio/wav", channels: 1 });
        expect(validated.seconds).toBeCloseTo(3, 6);
        yield* provider.enqueue(request({ references: [image()], audio: [validated] }));
        yield* provider.enqueue(request({ references: [image()], audio: [voice()] }));
        expect(fake.uploaded.filter((upload) => upload.mimeType === "audio/wav")).toHaveLength(1);
        const [first, second] = enqueueArgs(fake);
        expect(first!.reference_audios).toEqual(second!.reference_audios);
      }),
    ));

  test("an existing audio upload is forwarded without uploading, and its MIME type must be audio", () =>
    run(
      Effect.gen(function* () {
        const { fake, provider } = yield* setup();
        const file = {
          upload_id: "22222222-2222-4222-8222-222222222222",
          name: "voice.mp3",
          mime_type: "audio/mpeg",
          size: 1234n,
        };
        yield* provider.enqueue(
          request({ references: [image()], audio: [{ _tag: "Uploaded", file }] }),
        );
        expect(fake.uploaded.map((upload) => upload.mimeType)).toEqual(["image/png"]);
        expect(enqueueArgs(fake)[0]!.reference_audios).toEqual([{ ...file, size: 1234 }]);
        for (const bad of [
          { ...file, mime_type: "image/png" },
          { ...file, mime_type: "video/mp4" },
          { ...file, size: 25n * 1024n * 1024n + 1n },
        ]) {
          const result = yield* Effect.result(
            H3.validateAudioReference({ _tag: "Uploaded", file: bad }),
          );
          expect(Result.isFailure(result) && result.failure.reason._tag).toBe("InvalidInput");
        }
        // An image reference is not an audio reference, nor the other way round.
        const picture = yield* H3.validateReference(image());
        const sound = yield* H3.validateAudioReference(voice());
        for (const input of [
          request({ references: [image()], audio: [picture as never] }),
          request({ references: [sound as never] }),
        ]) {
          const result = yield* Effect.result(provider.enqueue(input));
          expect(Result.isFailure(result) && result.failure.context.outcome).toBe("not-submitted");
        }
      }),
    ));

  test("each documented container is recognized, and WAV, FLAC and Ogg headers are read", () =>
    run(
      Effect.gen(function* () {
        const cases: readonly [H3.Reference, string, number | null][] = [
          [voice(2), "audio/wav", 1],
          [voice(15, 2), "audio/wav", 2],
          [flac(10, 2), "audio/flac", 2],
          [ogg(1), "audio/ogg", 1],
          [headed(0x49, 0x44, 0x33, 4), "audio/mpeg", null],
          [headed(0xff, 0xfb, 0x90), "audio/mpeg", null],
          [headed(0xff, 0xf1, 0x50), "audio/aac", null],
          [headed(0, 0, 0, 32, 0x66, 0x74, 0x79, 0x70, 0x4d, 0x34, 0x41, 0x20), "audio/mp4", null],
          [headed(0x1a, 0x45, 0xdf, 0xa3), "audio/webm", null],
        ];
        for (const [reference, mimeType, channels] of cases) {
          const validated = yield* H3.validateAudioReference(reference);
          expect(validated.mimeType).toBe(mimeType);
          expect(validated.channels).toBe(channels);
        }
        expect((yield* H3.validateAudioReference(flac(10, 2))).seconds).toBeCloseTo(10, 4);
        expect((yield* H3.validateAudioReference(ogg(2))).seconds).toBeNull();
      }),
    ));

  test("audio outside the documented bounds is refused before anything is uploaded or sent", () =>
    run(
      Effect.gen(function* () {
        const { fake, provider } = yield* setup();
        const continueFrom = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa";
        const invalid: readonly H3.Request[] = [
          // Counts and companions.
          request({ references: [image()], audio: [voice(), voice(), voice(), voice()] }),
          request({ continueFrom, audio: [voice(), voice(), voice()] }),
          request({ audio: [voice()] }),
          // Length, channels, container and size.
          request({ references: [image()], audio: [voice(1.9)] }),
          request({ references: [image()], audio: [voice(15.1)] }),
          request({ references: [image()], audio: [voice(3, 3)] }),
          request({ references: [image()], audio: [flac(1, 1)] }),
          request({ references: [image()], audio: [flac(3, 4)] }),
          request({ references: [image()], audio: [{ _tag: "Bytes", bytes: pngBytes(8, 8) }] }),
          request({ references: [image()], audio: [{ _tag: "Bytes", bytes: new Uint8Array() }] }),
          request({
            references: [image()],
            audio: [{ _tag: "Bytes", bytes: new Uint8Array(25 * 1024 * 1024 + 1) }],
          }),
          request({
            references: [image()],
            audio: [{ _tag: "Bytes", bytes: wavBytes(3).slice(0, 30) }],
          }),
          request({ references: [image()], audio: {} as never }),
        ];
        for (const input of invalid) {
          const result = yield* Effect.result(provider.enqueue(input));
          expect(Result.isFailure(result)).toBe(true);
          if (Result.isFailure(result)) {
            expect(result.failure).toBeInstanceOf(CommandFailure);
            expect(result.failure.context).toMatchObject({
              operation: "enqueue",
              outcome: "not-submitted",
            });
          }
        }
        expect(fake.uploaded).toHaveLength(0);
        expect(enqueueArgs(fake)).toHaveLength(0);
      }),
    ));

  test("audio with only a continuation, two with a continuation and twelve references in all are sent", () =>
    run(
      Effect.gen(function* () {
        const { fake, provider } = yield* setup();
        const continueFrom = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa";
        yield* provider.enqueue(request({ continueFrom, audio: [voice()] }));
        yield* provider.enqueue(
          request({ continueFrom, references: [image()], audio: [voice(), voice(5)] }),
        );
        yield* provider.enqueue(
          request({
            references: Array.from({ length: 9 }, image),
            audio: [voice(), voice(4), voice(5)],
          }),
        );
        const sent = enqueueArgs(fake);
        expect(sent.map((args) => (args.reference_audios as unknown[]).length)).toEqual([1, 2, 3]);
        expect(sent[0]!.continue_from_clip_id).toBe(continueFrom);
        expect(sent[0]!.reference_images).toEqual([]);
      }),
    ));

  test("a deployment without reference_audios refuses audio as UnsupportedCapability, uploading nothing", () =>
    run(
      Effect.gen(function* () {
        const schema = providerSchema();
        delete enqueueSchema(schema).reference_audios;
        const { fake, provider } = yield* setup({ schema });
        expect(provider.contract.referenceAudio).toBe(false);
        const result = yield* Effect.result(
          provider.enqueue(request({ references: [image()], audio: [voice()] })),
        );
        expect(Result.isFailure(result)).toBe(true);
        if (Result.isFailure(result)) {
          expect(result.failure.reason._tag).toBe("UnsupportedCapability");
          expect(result.failure.context.outcome).toBe("not-submitted");
        }
        expect(fake.uploaded).toHaveLength(0);
        yield* provider.enqueue(request({ references: [image()] }));
        expect(enqueueArgs(fake)).toHaveLength(1);
      }),
    ));
});
