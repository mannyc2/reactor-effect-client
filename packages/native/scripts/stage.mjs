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
