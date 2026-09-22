import { spawnSync } from "node:child_process";
import { createHash } from "node:crypto";
import { mkdtempSync, readFileSync, renameSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { describe, expect, test } from "vitest";
import * as Effect from "effect/Effect";
import {
  checkNativeBridge,
  encodeNativeJson,
  NativeBridge,
  NativeCall,
  resolveNativeBridge,
  verifyStagedNativeBridge,
} from "../../src/native/_internal/bridge.js";
import { NativePeer } from "../../src/native/_internal/peer.js";

const libraryName =
  process.platform === "darwin"
    ? "libreactor_effect_native.dylib"
    : process.platform === "win32"
      ? "reactor_effect_native.dll"
      : "libreactor_effect_native.so";
const libraryPath = fileURLToPath(
  new URL(`../../dist/native/${process.platform}-${process.arch}/${libraryName}`, import.meta.url),
);

const compileFixture = (
  body: string,
  name: string,
): { readonly directory: string; readonly path: string } => {
  const directory = mkdtempSync(join(tmpdir(), "reactor-native-abi-"));
  const source = join(directory, `${name}.c`);
  const extension = process.platform === "darwin" ? "dylib" : "so";
  const path = join(directory, `lib${name}.${extension}`);
  writeFileSync(source, body);
  const compiler = process.env.CC ?? "cc";
  const flags = process.platform === "darwin" ? ["-dynamiclib"] : ["-shared", "-fPIC"];
  const result = spawnSync(compiler, [...flags, source, "-o", path], { encoding: "utf8" });
  if (result.error !== undefined) throw result.error;
  if (result.status !== 0) throw new Error(`${compiler} failed: ${result.stderr}`);
  return { directory, path };
};

describe("native C ABI", () => {
  test("uses the same source-identified staged artifact as installed-package preflight", async () => {
    const manifest = await verifyStagedNativeBridge(libraryPath);
    expect(manifest.sha256).toMatch(/^[0-9a-f]{64}$/);
    expect(manifest.build).toMatchObject({
      abiVersion: 2,
      profile: "release",
      sourceSha256: expect.stringMatching(/^[0-9a-f]{64}$/),
    });
    expect(await resolveNativeBridge()).toBe(libraryPath);
  });

  test("rejects a staged binary replaced after loading even when its embedded source/build identity is unchanged", async () => {
    if (process.platform === "win32") return;
    // Use the explicit ABI fixture for image-cache identity. Loading a second
    // copy of macOS libwebrtc into one process registers duplicate ObjC classes;
    // the actual codec artifact is independently exercised by the other tests.
    const build = {
      schemaVersion: 1,
      abiVersion: 2,
      profile: "release",
      sourceSha256: "0".repeat(64),
    };
    const identity = `reactor-effect-native:build-identity:${JSON.stringify(build)}:end`;
    const source = readFileSync(new URL("../native-session-fixture.c", import.meta.url), "utf8");
    const fixture = compileFixture(
      `#define REACTOR_EFFECT_FIXTURE_BUILD_IDENTITY ${JSON.stringify(identity)}\n${source}`,
      "identity",
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
      await verifyStagedNativeBridge(path);
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
      await expect(verifyStagedNativeBridge(path)).rejects.toMatchObject({
        code: "Native",
        context: { outcome: "not-submitted" },
      });
    } finally {
      rmSync(directory, { recursive: true, force: true });
    }
  });

  test("rejects ambiguous same-kind receive mappings before native negotiation in either order", async () => {
    await checkNativeBridge(libraryPath);
    const first = { name: "video-a", kind: "video" as const, direction: "recvonly" as const };
    const second = { name: "video-b", kind: "video" as const, direction: "recvonly" as const };
    for (const tracks of [
      [first, second],
      [second, first],
    ] as const) {
      const peer = new NativePeer(libraryPath);
      try {
        await expect(
          Effect.runPromise(Effect.scoped(peer.prepare([], tracks, () => undefined))),
        ).rejects.toMatchObject({
          code: "UnsupportedCapability",
          context: expect.objectContaining({ outcome: "not-submitted" }),
        });
      } finally {
        peer.close();
        await Effect.runPromise(peer.shutdown());
      }
    }
  });

  test("rejects ABI mismatch and missing required symbols before peer allocation", async () => {
    if (process.platform === "win32") return;
    const fixtures: string[] = [];
    try {
      const wrong = compileFixture(
        "#include <stdint.h>\nuint32_t reactor_effect_abi_version(void) { return 99; }\n",
        "wrong_abi",
      );
      fixtures.push(wrong.directory);
      await expect(checkNativeBridge(wrong.path)).rejects.toMatchObject({
        code: "Native",
        context: expect.objectContaining({ outcome: "not-submitted" }),
      });

      const missing = compileFixture(
        "#include <stdint.h>\nuint32_t reactor_effect_abi_version(void) { return 2; }\n",
        "missing_symbols",
      );
      fixtures.push(missing.directory);
      await expect(checkNativeBridge(missing.path)).rejects.toMatchObject({
        code: "Native",
        context: expect.objectContaining({ outcome: "not-submitted" }),
      });
    } finally {
      for (const directory of fixtures) rmSync(directory, { recursive: true, force: true });
    }
  });

  test("preflights, negotiates an offer, fences polls, and joins idempotently", async () => {
    await checkNativeBridge(libraryPath);
    const bridge = new NativeBridge(libraryPath);
    try {
      // This error came back after entering the ABI. A native error code alone
      // cannot establish execution history, unlike the local closed fence below.
      await expect(bridge.send("data", Uint8Array.of(1))).rejects.toMatchObject({
        code: "Closed",
        context: expect.objectContaining({ outcome: "unknown" }),
      });
      const prepared = (await bridge.call(
        NativeCall.Prepare,
        encodeNativeJson({
          servers: [],
          tracks: [{ name: "video", kind: "video", direction: "recvonly" }],
        }),
      )) as { readonly sdp?: unknown; readonly mapping?: unknown };
      expect(typeof prepared.sdp).toBe("string");
      expect(prepared.sdp).toContain("m=video");
      expect(prepared.mapping).toEqual([
        expect.objectContaining({ name: "video", kind: "video", direction: "recvonly" }),
      ]);

      const snapshot = (await bridge.call(NativeCall.MediaSnapshot)) as {
        readonly closed?: unknown;
      };
      expect(snapshot.closed).toBe(false);

      bridge.close();
      await expect(bridge.call(NativeCall.MediaSnapshot)).rejects.toMatchObject({
        code: "Closed",
        context: expect.objectContaining({ outcome: "not-submitted" }),
      });
      await expect(bridge.send("data", Uint8Array.of(1))).rejects.toMatchObject({
        code: "Closed",
        context: expect.objectContaining({ outcome: "not-submitted" }),
      });
      expect((await bridge.pollEvent(0))._tag).toBe("Closed");
      expect((await bridge.pollVideo(0))._tag).toBe("Closed");
      expect((await bridge.pollAudio(0))._tag).toBe("Closed");
      await bridge.shutdown();
      await bridge.shutdown();
    } finally {
      await bridge.shutdown();
    }
  });
});
