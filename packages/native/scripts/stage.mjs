import { createHash } from "node:crypto";
import { copyFileSync, cpSync, mkdirSync, readFileSync, readdirSync, renameSync, writeFileSync } from "node:fs";
import { createRequire } from "node:module";
import { isAbsolute, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const root = fileURLToPath(new URL("../", import.meta.url));
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

/** The source identity `rust/build.rs` embeds: every build input, hashed in path order. */
const sourceHash = () => {
  const native = join(root, "rust");
  const files = [
    "Cargo.toml",
    "Cargo.lock",
    "build.rs",
    ".cargo/config.toml",
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

/** Each published platform package's Rust target and the SBOM of its libwebrtc prebuilt. */
const platforms = /** @type {const} */ ({
  "darwin-arm64": {
    target: "aarch64-apple-darwin",
    sbom: "reactor-webrtc-mac-arm64-release.sbom.json",
  },
  "linux-x64-gnu": {
    target: "x86_64-unknown-linux-gnu",
    sbom: "reactor-webrtc-linux-x64-release.sbom.json",
  },
});

/**
 * The prebuilt the addon links must be the one its notices describe: the
 * NOTICE's tag exactly, and the platform SBOM's WebRTC milestone and commit.
 * @param {unknown} linked @param {string} sbom
 */
const checkPrebuilt = (linked, sbom) => {
  const notices = join(root, "notices");
  const notice = readFileSync(join(notices, "reactor-webrtc-NOTICE.md"), "utf8");
  const declared = /Reactor prebuilt tag: `([^`]+)`/.exec(notice)?.[1];
  const pinned = /Pinned WebRTC commit: `([0-9a-f]{40})`/.exec(notice)?.[1];
  const tag = typeof linked === "string" ? /^webrtc-(\d+)-([0-9a-f]{8})-p\d+$/.exec(linked) : null;
  if (tag === null || linked !== declared)
    throw new Error(
      `native addon links WebRTC prebuilt ${JSON.stringify(linked)}, but its NOTICE declares ${JSON.stringify(declared)}; rebuild without a libwebrtc override or update the notices`,
    );
  const component = JSON.parse(readFileSync(join(notices, "reactor-webrtc", sbom), "utf8"))
    ?.metadata?.component;
  const [, milestone = "", commit = ""] = tag;
  const [, sbomMilestone, sbomCommit = ""] =
    /^branch-heads\/(\d+)\+([0-9a-f]{8,40})$/.exec(String(component?.version)) ?? [];
  if (
    sbomMilestone !== milestone ||
    !sbomCommit.startsWith(commit) ||
    pinned === undefined ||
    !pinned.startsWith(sbomCommit)
  )
    throw new Error(
      `${sbom} describes WebRTC ${JSON.stringify(component?.version)}, not the linked ${tag[0]} at ${String(pinned)}`,
    );
};

/** Write `contents` so a reader sees the old file or the complete new one. */
const replace = (/** @type {string} */ path, /** @type {string | Uint8Array} */ contents) => {
  writeFileSync(`${path}.${process.pid}.stage`, contents);
  renameSync(`${path}.${process.pid}.stage`, path);
};

const [input, platform, declarations] = process.argv.slice(2);
if (input === "--source-hash" && platform === undefined) {
  // CI keys its staged-addon cache on this identity; a cached addon whose
  // embedded identity differs is rejected by the check below.
  console.log(sourceHash());
  process.exit(0);
}
if (input === undefined || platform === undefined || !(platform in platforms))
  throw new Error(
    `usage: scripts/stage.mjs <addon.node> <${Object.keys(platforms).join("|")}> [binding.d.ts]`,
  );
const { target, sbom } = platforms[/** @type {keyof typeof platforms} */ (platform)];
const source = isAbsolute(input) ? input : resolve(root, input);

// The staging host runs the platform it stages, so the addon reports its own identity.
const addon = /** @type {{ buildIdentity(): string }} */ (createRequire(import.meta.url)(source));
const build = JSON.parse(addon.buildIdentity());
if (build.schemaVersion !== 2 || build.profile !== "release" || build.target !== target)
  throw new Error("native addon profile or target does not match the requested package platform");
const expected = sourceHash();
if (build.sourceSha256 !== expected)
  throw new Error(
    `native source identity mismatch: addon ${build.sourceSha256}; current sources ${expected}; rebuild before staging`,
  );
checkPrebuilt(build.webrtcPrebuilt, sbom);

const packageDirectory = join(root, "npm", platform);
const file = `reactor-effect-native.${platform}.node`;
const bytes = readFileSync(source);
replace(join(packageDirectory, file), bytes);
const identity = { schemaVersion: 2, platform, file, sha256: sha256(bytes), build };
replace(join(packageDirectory, "native-identity.json"), `${JSON.stringify(identity, null, 2)}\n`);
// The binary carries libwebrtc and the crates it links, so its package carries their notices.
for (const notice of ["LICENSE", "NOTICE"]) copyFileSync(join(root, notice), join(packageDirectory, notice));
mkdirSync(join(packageDirectory, "notices"), { recursive: true });
cpSync(join(root, "notices"), join(packageDirectory, "notices"), { recursive: true });

if (declarations !== undefined) {
  const generated = readFileSync(isAbsolute(declarations) ? declarations : resolve(root, declarations), "utf8");
  replace(
    join(root, "src/internal/binding.ts"),
    `// The addon's surface as \`napi build\` declares it from rust/src/binding.rs. Generated by
// \`bun run native:build\`; do not edit.
${generated.replace(/^\/\* auto-generated by NAPI-RS \*\/\n\/\* eslint-disable \*\/\n/, "")}`,
  );
}
console.log(JSON.stringify({ package: packageDirectory, file, sha256: identity.sha256, sourceSha256: expected }));
