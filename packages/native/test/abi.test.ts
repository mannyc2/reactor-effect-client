import { createHash } from "node:crypto";
import { readFileSync, renameSync, rmSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { describe, expect, test } from "vitest";
import type { ReactorError } from "reactor-effect-client/ReactorError";
import * as Effect from "effect/Effect";
import { encodeText, NativeBridge, NativeCall } from "../src/internal/bridge.js";
import * as Library from "../src/internal/library.js";
import {
  compileFixture,
  compileLibrary,
  libraryName,
  libraryPath,
  nativePeer,
  revealed,
} from "./support.js";

const run = <A>(effect: Effect.Effect<A, ReactorError>): Promise<A> => Effect.runPromise(effect);
/** The ReactorError an Effect fails with. */
const rejection = (effect: Effect.Effect<unknown, ReactorError>): Promise<ReactorError> =>
  Effect.runPromise(Effect.flip(effect));
const json = (value: unknown) => encodeText(JSON.stringify(value));

describe("native C ABI", () => {
  test("uses the same source-identified staged artifact as installed-package preflight", async () => {
    const manifest = await run(Library.verifyStaged(libraryPath));
    expect(manifest.sha256).toMatch(/^[0-9a-f]{64}$/);
    expect(manifest.build).toMatchObject({
      abiVersion: 4,
      profile: "release",
      sourceSha256: expect.stringMatching(/^[0-9a-f]{64}$/),
    });
    expect((await run(Library.resolve(undefined))).path).toBe(libraryPath);
  });

  test("rejects a staged binary replaced after loading even when its embedded source/build identity is unchanged", async () => {
    if (process.platform === "win32") return;
    // Use the explicit ABI fixture for image-cache identity. Loading a second
    // copy of macOS libwebrtc into one process registers duplicate ObjC classes;
    // the actual codec artifact is independently exercised by the other tests.
    const build = {
      schemaVersion: 1,
      abiVersion: 4,
      profile: "release",
      sourceSha256: "0".repeat(64),
    };
    const identity = `reactor-effect-native:build-identity:${JSON.stringify(build)}:end`;
    const fixture = compileFixture(
      `#define REACTOR_EFFECT_FIXTURE_BUILD_IDENTITY ${JSON.stringify(identity)}\n`,
    );
    const directory = fixture.directory;
    const path = join(directory, libraryName),
      sidecar = join(directory, "native-identity.json");
    renameSync(fixture.path, path);
    const binary = readFileSync(path);
    const manifest = {
      schemaVersion: 1,
      platform: `${process.platform}-${process.arch}`,
      library: libraryName,
      sha256: createHash("sha256").update(binary).digest("hex"),
      build,
    };
    try {
      writeFileSync(sidecar, JSON.stringify(manifest));
      await run(Library.verifyStaged(path));
      const replaced = Buffer.concat([binary, Buffer.from([0])]);
      // Atomic replacement preserves the already mapped image's inode. Never
      // truncate a library that is mapped into the running test process.
      writeFileSync(`${path}.replacement`, replaced);
      renameSync(`${path}.replacement`, path);
      writeFileSync(
        sidecar,
        JSON.stringify({
          ...manifest,
          sha256: createHash("sha256").update(replaced).digest("hex"),
        }),
      );
      await expect(run(Library.verifyStaged(path))).rejects.toMatchObject({
        reason: { _tag: "Native" },
        context: { outcome: "not-submitted" },
      });
    } finally {
      rmSync(directory, { recursive: true, force: true });
    }
  });

  test("rejects ambiguous same-kind receive mappings before native negotiation in either order", async () => {
    const first = { name: "video-a", kind: "video" as const, direction: "recvonly" as const };
    const second = { name: "video-b", kind: "video" as const, direction: "recvonly" as const };
    for (const tracks of [
      [first, second],
      [second, first],
    ] as const) {
      await expect(
        Effect.runPromise(
          Effect.scoped(
            Effect.flatMap(nativePeer(libraryPath), (peer) =>
              peer.prepare([], tracks, () => undefined),
            ),
          ),
        ),
      ).rejects.toMatchObject({
        reason: { _tag: "UnsupportedCapability" },
        context: expect.objectContaining({ outcome: "not-submitted" }),
      });
    }
  });

  test("rejects an ABI 3 library and missing required symbols before peer allocation", async () => {
    if (process.platform === "win32") return;
    const fixtures: string[] = [];
    try {
      const previous = compileLibrary(
        "#include <stdint.h>\nuint32_t reactor_effect_abi_version(void) { return 3; }\n",
        "abi_three",
      );
      fixtures.push(previous.directory);
      await expect(run(Library.load(previous.path))).rejects.toMatchObject({
        reason: { _tag: "Native" },
        message: "native WebRTC ABI mismatch: expected 4, received 3",
        context: expect.objectContaining({ outcome: "not-submitted" }),
      });

      const missing = compileLibrary(
        "#include <stdint.h>\nuint32_t reactor_effect_abi_version(void) { return 4; }\n",
        "missing_symbols",
      );
      fixtures.push(missing.directory);
      await expect(run(Library.load(missing.path))).rejects.toMatchObject({
        reason: { _tag: "Native" },
        context: expect.objectContaining({ outcome: "not-submitted" }),
      });
    } finally {
      for (const directory of fixtures) rmSync(directory, { recursive: true, force: true });
    }
  });

  test("negotiates an offer, reports typed failure classes, fences takes, and joins idempotently", async () => {
    const bridge = await run(
      Effect.flatMap(Library.load(libraryPath), (library) =>
        NativeBridge.make(library, () => undefined),
      ),
    );
    try {
      // These errors came back after entering the ABI. A native failure class
      // alone cannot establish execution history, unlike the local fence below.
      expect(revealed(await rejection(bridge.send("data", Uint8Array.of(1))))).toMatchObject({
        reason: { _tag: "ChannelClosed" },
        context: expect.objectContaining({
          outcome: "unknown",
          detail: expect.objectContaining({ channel: "data" }),
        }),
      });
      const prepared = (await run(
        bridge.call(
          NativeCall.Prepare,
          json({
            servers: [],
            tracks: [{ name: "video", kind: "video", direction: "recvonly" }],
          }),
        ),
      )) as { readonly sdp?: unknown; readonly mapping?: unknown };
      expect(typeof prepared.sdp).toBe("string");
      expect(prepared.sdp).toContain("m=video");
      expect(prepared.mapping).toEqual([
        expect.objectContaining({ name: "video", kind: "video", direction: "recvonly" }),
      ]);
      expect(
        revealed(
          await rejection(bridge.call(NativeCall.Answer, encodeText("v=0\r\nnot an answer\r\n"))),
        ),
      ).toMatchObject({
        reason: { _tag: "SdpRejected" },
        context: expect.objectContaining({
          outcome: "unknown",
          detail: expect.objectContaining({ status: -5 }),
        }),
      });
      await expect(run(bridge.call(NativeCall.Prepare, json({})))).rejects.toMatchObject({
        reason: { _tag: "InvalidInput" },
      });

      const snapshot = (await run(bridge.call(NativeCall.MediaSnapshot))) as {
        readonly closed?: unknown;
      };
      expect(snapshot.closed).toBe(false);
      expect(bridge.takeEvent()).not.toBeNull();

      bridge.close();
      await expect(run(bridge.call(NativeCall.MediaSnapshot))).rejects.toMatchObject({
        reason: { _tag: "Closed" },
        context: expect.objectContaining({ outcome: "not-submitted" }),
      });
      await expect(run(bridge.send("data", Uint8Array.of(1)))).rejects.toMatchObject({
        reason: { _tag: "Closed" },
        context: expect.objectContaining({ outcome: "not-submitted" }),
      });
      expect(bridge.takeEvent()).toBeNull();
      expect(bridge.takeVideo()).toBeNull();
      expect(bridge.takeAudio()).toBeNull();
      await run(bridge.shutdown);
      await run(bridge.shutdown);
    } finally {
      await run(bridge.shutdown);
    }
  });
});
