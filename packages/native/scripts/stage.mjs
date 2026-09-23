import { createHash } from "node:crypto";
import { mkdirSync, readFileSync, readdirSync, renameSync, writeFileSync } from "node:fs";
import { dirname, isAbsolute, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const root = fileURLToPath(new URL("../", import.meta.url));
const prefix = "reactor-effect-native:build-identity:";
const suffix = ":end\0";
/** @param {Uint8Array} bytes */
const sha256 = (bytes) => createHash("sha256").update(bytes).digest("hex");

/** @param {string} directory @param {string} [prefix] @returns {string[]} */
const sources = (directory, prefix = "") =>
  readdirSync(directory, { withFileTypes: true }).flatMap((entry) => {
    const relative = `${prefix}${entry.name}`;
    return entry.isDirectory()
      ? sources(join(directory, entry.name), `${relative}/`)
      : entry.isFile()
        ? [relative]
        : [];
  });

const sourceHash = () => {
  const native = join(root, "rust");
  const files = [
    "Cargo.toml",
    "Cargo.lock",
    "build.rs",
    ".cargo/config.toml",
    "include/reactor_effect_native.h",
    ...sources(join(native, "src"), "src/"),
  ].sort();
  const hash = createHash("sha256");
  for (const file of files)
    hash
      .update(file)
      .update("\0")
      .update(readFileSync(join(native, file)))
      .update("\0");
  return hash.digest("hex");
};

const targets = /** @type {const} */ ({
  "darwin-arm64": ["aarch64-apple-darwin", "libreactor_effect_native.dylib"],
  "darwin-x64": ["x86_64-apple-darwin", "libreactor_effect_native.dylib"],
  "linux-x64": ["x86_64-unknown-linux-gnu", "libreactor_effect_native.so"],
  "linux-arm64": ["aarch64-unknown-linux-gnu", "libreactor_effect_native.so"],
  "win32-x64": ["x86_64-pc-windows-msvc", "reactor_effect_native.dll"],
  "win32-arm64": ["aarch64-pc-windows-msvc", "reactor_effect_native.dll"],
});

/** The shipped SBOM of each platform's Reactor libwebrtc prebuilt. */
const sboms = /** @type {Record<string, string>} */ ({
  "darwin-arm64": "reactor-webrtc-mac-arm64-release.sbom.json",
  "linux-x64": "reactor-webrtc-linux-x64-release.sbom.json",
});

/**
 * The prebuilt the library links must be the one its notices describe: the
 * NOTICE's tag exactly, and the platform SBOM's WebRTC milestone and commit.
 * @param {unknown} linked @param {string} platform
 */
const checkPrebuilt = (linked, platform) => {
  const notices = join(root, "notices");
  const notice = readFileSync(join(notices, "reactor-webrtc-NOTICE.md"), "utf8");
  const declared = /Reactor prebuilt tag: `([^`]+)`/.exec(notice)?.[1];
  const pinned = /Pinned WebRTC commit: `([0-9a-f]{40})`/.exec(notice)?.[1];
  const tag = typeof linked === "string" ? /^webrtc-(\d+)-([0-9a-f]{8})-p\d+$/.exec(linked) : null;
  if (tag === null || linked !== declared)
    throw new Error(
      `native library links WebRTC prebuilt ${JSON.stringify(linked)}, but its NOTICE declares ${JSON.stringify(declared)}; rebuild without a libwebrtc override or update the notices`,
    );
  const sbom = sboms[platform];
  if (sbom === undefined) throw new Error(`no WebRTC SBOM ships for ${platform}`);
  const component = JSON.parse(readFileSync(join(notices, "reactor-webrtc", sbom), "utf8"))
    ?.metadata?.component;
  const version = /^branch-heads\/(\d+)\+([0-9a-f]{8,40})$/.exec(String(component?.version));
  const [, milestone, commit] = tag;
  if (
    version === null ||
    version[1] !== milestone ||
    !version[2].startsWith(commit) ||
    pinned === undefined ||
    !pinned.startsWith(version[2])
  )
    throw new Error(
      `${sbom} describes WebRTC ${JSON.stringify(component?.version)}, not the linked ${tag[0]} at ${String(pinned)}`,
    );
};

const [input, platform] = process.argv.slice(2);
if (input === "--source-hash" && platform === undefined) {
  // CI keys its staged-artifact cache on this identity; a cached library whose
  // embedded identity differs is rejected by the staging check below.
  console.log(sourceHash());
  process.exit(0);
}
if (input === undefined || platform === undefined || !(platform in targets)) {
  throw new Error("usage: scripts/stage.mjs <shared-library> <supported-platform-arch>");
}
const [target, library] = targets[/** @type {keyof typeof targets} */ (platform)];
const source = isAbsolute(input) ? input : resolve(root, input);
const bytes = readFileSync(source);
const start = bytes.indexOf(prefix);
const end = start < 0 ? -1 : bytes.indexOf(suffix, start + prefix.length);
if (start < 0 || end < 0 || bytes.indexOf(prefix, end + suffix.length) !== -1) {
  throw new Error(
    "native staging requires exactly one embedded source/build identity; rebuild the native library",
  );
}
const build = JSON.parse(bytes.subarray(start + prefix.length, end).toString("utf8"));
if (
  build.schemaVersion !== 1 ||
  build.abiVersion !== 4 ||
  build.profile !== "release" ||
  build.target !== target
) {
  throw new Error(
    "native artifact ABI, release profile or target does not match the requested package platform",
  );
}
const expected = sourceHash();
if (build.sourceSha256 !== expected) {
  throw new Error(
    `native source identity mismatch: artifact ${build.sourceSha256}; current sources ${expected}; rebuild before staging`,
  );
}
checkPrebuilt(build.webrtcPrebuilt, platform);
const manifest = { schemaVersion: 1, platform, library, sha256: sha256(bytes), build };
const destination = join(root, "lib", platform, library);
mkdirSync(dirname(destination), { recursive: true });
// Readers verify both files, so an interrupted staging operation fails closed.
// Rename each complete file rather than exposing a partially copied library.
const temporary = `${destination}.${process.pid}.stage`;
writeFileSync(temporary, bytes);
renameSync(temporary, destination);
const identity = join(dirname(destination), "native-identity.json");
writeFileSync(`${identity}.${process.pid}.stage`, `${JSON.stringify(manifest, null, 2)}\n`);
renameSync(`${identity}.${process.pid}.stage`, identity);
console.log(
  JSON.stringify({
    artifact: destination,
    identity,
    sha256: manifest.sha256,
    sourceSha256: expected,
  }),
);
