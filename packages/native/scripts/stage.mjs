import { createRequire } from "node:module";
import * as NodeRuntime from "@effect/platform-node/NodeRuntime";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as Console from "effect/Console";
import * as Crypto from "effect/Crypto";
import * as Effect from "effect/Effect";
import * as Encoding from "effect/Encoding";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";
import * as Stdio from "effect/Stdio";

// JavaScript can't pass a type argument in a call, so the error's own type is given here.
/** @type {typeof Schema.TaggedError<StageError>} */
const TaggedError = Schema.TaggedError;

/** Staging refused the addon, its platform or its notices. */
class StageError extends TaggedError("reactor-effect-native/scripts/stage/StageError")(
  "StageError",
  { message: Schema.String },
) {}

/** The published platform packages, by the suffix of their names. */
const Platform = Schema.Literals(["darwin-arm64", "linux-x64-gnu"]);
/**
 * Each platform package's Rust target and the SBOM of its libwebrtc prebuilt.
 * @type {Readonly<Record<typeof Platform.Type, { readonly target: string, readonly sbom: string }>>}
 */
const platforms = {
  "darwin-arm64": {
    target: "aarch64-apple-darwin",
    sbom: "reactor-webrtc-mac-arm64-release.sbom.json",
  },
  "linux-x64-gnu": {
    target: "x86_64-unknown-linux-gnu",
    sbom: "reactor-webrtc-linux-x64-release.sbom.json",
  },
};

/** The addon's own build identity, as JSON; staging keeps it whole in the package's identity. */
const BuildJson = Schema.fromJsonString(Schema.Unknown);
/** The fields of that identity staging checks. */
const Build = Schema.Struct({
  schemaVersion: Schema.Finite,
  sourceSha256: Schema.String,
  profile: Schema.String,
  target: Schema.String,
  webrtcPrebuilt: Schema.String,
});
/** The WebRTC version a platform SBOM describes. */
const Sbom = Schema.fromJsonString(
  Schema.Struct({
    metadata: Schema.Struct({ component: Schema.Struct({ version: Schema.String }) }),
  }),
);
/** The package's `native-identity.json`. */
const Identity = Schema.fromJsonString(
  Schema.Struct({
    schemaVersion: Schema.Literal(2),
    platform: Schema.String,
    file: Schema.String,
    sha256: Schema.String,
    build: Schema.Unknown,
  }),
  { space: 2 },
);
/** The line staging prints for its caller. */
const Staged = Schema.fromJsonString(
  Schema.Struct({
    package: Schema.String,
    file: Schema.String,
    sha256: Schema.String,
    sourceSha256: Schema.String,
  }),
);

/** @param {Uint8Array} bytes */
const sha256 = (bytes) =>
  Crypto.Crypto.pipe(
    Effect.flatMap((crypto) => crypto.digest("SHA-256", bytes)),
    Effect.map(Encoding.encodeHex),
  );

const program = Effect.gen(function* () {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const root = yield* path.fromFileUrl(new URL("../", import.meta.url));

  /** The source identity `rust/build.rs` embeds: every build input, hashed in path order. */
  const sourceHash = Effect.gen(function* () {
    const native = path.join(root, "rust");
    const sources = [];
    for (const entry of yield* fs.readDirectory(path.join(native, "src"), { recursive: true }))
      if ((yield* fs.stat(path.join(native, "src", entry))).type === "File")
        sources.push(`src/${entry.split(path.sep).join("/")}`);
    const files = ["Cargo.toml", "Cargo.lock", "build.rs", ".cargo/config.toml", ...sources];
    const encoder = new TextEncoder();
    const parts = [];
    for (const file of files.sort())
      parts.push(
        encoder.encode(`${file}\0`),
        yield* fs.readFile(path.join(native, file)),
        encoder.encode("\0"),
      );
    const input = new Uint8Array(parts.reduce((total, part) => total + part.length, 0));
    let offset = 0;
    for (const part of parts) {
      input.set(part, offset);
      offset += part.length;
    }
    return yield* sha256(input);
  });

  /**
   * The prebuilt the addon links must be the one its notices describe: the
   * NOTICE's tag exactly, and the platform SBOM's WebRTC milestone and commit.
   */
  const checkPrebuilt = Effect.fnUntraced(
    function* (/** @type {string} */ linked, /** @type {string} */ sbom) {
      const notices = path.join(root, "notices");
      const notice = yield* fs.readFileString(path.join(notices, "reactor-webrtc-NOTICE.md"));
      const declared = /Reactor prebuilt tag: `([^`]+)`/.exec(notice)?.[1];
      const pinned = /Pinned WebRTC commit: `([0-9a-f]{40})`/.exec(notice)?.[1];
      const tag = /^webrtc-(\d+)-([0-9a-f]{8})-p\d+$/.exec(linked);
      if (tag === null || linked !== declared)
        return yield* StageError.make({
          message: `native addon links WebRTC prebuilt "${linked}", but its NOTICE declares ${declared === undefined ? "none" : `"${declared}"`}; rebuild without a libwebrtc override or update the notices`,
        });
      const described = yield* fs
        .readFileString(path.join(notices, "reactor-webrtc", sbom))
        .pipe(Effect.flatMap(Schema.decodeEffect(Sbom)));
      const version = described.metadata.component.version;
      const [, milestone = "", commit = ""] = tag;
      const [, sbomMilestone, sbomCommit = ""] =
        /^branch-heads\/(\d+)\+([0-9a-f]{8,40})$/.exec(version) ?? [];
      if (
        sbomMilestone !== milestone ||
        !sbomCommit.startsWith(commit) ||
        pinned === undefined ||
        !pinned.startsWith(sbomCommit)
      )
        return yield* StageError.make({
          message: `${sbom} describes WebRTC "${version}", not the linked ${tag[0]} at ${pinned ?? "no pinned commit"}`,
        });
    },
  );

  /** Write `contents` so a reader sees the old file or the complete new one. */
  const replace = Effect.fnUntraced(
    function* (/** @type {string} */ file, /** @type {string | Uint8Array} */ contents) {
      const staged = `${file}.${process.pid}.stage`;
      if (typeof contents === "string") yield* fs.writeFileString(staged, contents);
      else yield* fs.writeFile(staged, contents);
      yield* fs.rename(staged, file);
    },
  );

  const [input, platform, declarations] = yield* (yield* Stdio.Stdio).args;
  if (input === "--source-hash" && platform === undefined) {
    // CI keys its staged-addon cache on this identity; a cached addon whose
    // embedded identity differs is rejected by the check below.
    return yield* Console.log(yield* sourceHash);
  }
  if (input === undefined || !Schema.is(Platform)(platform))
    return yield* StageError.make({
      message: `usage: scripts/stage.mjs <addon.node> <${Platform.literals.join("|")}> [binding.d.ts]`,
    });
  const { target, sbom } = platforms[platform];
  const source = path.isAbsolute(input) ? input : path.resolve(root, input);

  // The staging host runs the platform it stages, so the addon reports its own identity.
  // Node loads an addon untyped; staging calls only this one function of it.
  const addon = /** @type {{ buildIdentity(): unknown }} */ (
    createRequire(import.meta.url)(source)
  );
  const reported = yield* Schema.decodeUnknownEffect(BuildJson)(addon.buildIdentity());
  const build = yield* Schema.decodeUnknownEffect(Build)(reported);
  if (build.schemaVersion !== 2 || build.profile !== "release" || build.target !== target)
    return yield* StageError.make({
      message: "native addon profile or target does not match the requested package platform",
    });
  const expected = yield* sourceHash;
  if (build.sourceSha256 !== expected)
    return yield* StageError.make({
      message: `native source identity mismatch: addon ${build.sourceSha256}; current sources ${expected}; rebuild before staging`,
    });
  yield* checkPrebuilt(build.webrtcPrebuilt, sbom);

  const packageDirectory = path.join(root, "npm", platform);
  const file = `reactor-effect-native.${platform}.node`;
  const bytes = yield* fs.readFile(source);
  yield* replace(path.join(packageDirectory, file), bytes);
  const digest = yield* sha256(bytes);
  const identity = yield* Schema.encodeEffect(Identity)({
    schemaVersion: 2,
    platform,
    file,
    sha256: digest,
    build: reported,
  });
  yield* replace(path.join(packageDirectory, "native-identity.json"), `${identity}\n`);
  // The binary carries libwebrtc and the crates it links, so its package carries their notices.
  for (const notice of ["LICENSE", "NOTICE"])
    yield* fs.copyFile(path.join(root, notice), path.join(packageDirectory, notice));
  yield* fs.makeDirectory(path.join(packageDirectory, "notices"), { recursive: true });
  yield* fs.copy(path.join(root, "notices"), path.join(packageDirectory, "notices"), {
    overwrite: true,
  });

  if (declarations !== undefined) {
    const generated = yield* fs.readFileString(
      path.isAbsolute(declarations) ? declarations : path.resolve(root, declarations),
    );
    yield* replace(
      path.join(root, "src/internal/binding.ts"),
      `// The addon's surface as \`napi build\` declares it from rust/src/binding.rs. Generated by
// \`bun run native:build\`; do not edit.
${generated.replace(/^\/\* auto-generated by NAPI-RS \*\/\n\/\* eslint-disable \*\/\n/, "")}`,
    );
  }
  yield* Console.log(
    yield* Schema.encodeEffect(Staged)({
      package: packageDirectory,
      file,
      sha256: digest,
      sourceSha256: expected,
    }),
  );
});

program.pipe(
  // The script's entry point.
  // @effect-diagnostics-next-line strictEffectProvide:off
  Effect.provide(NodeServices.layer),
  NodeRuntime.runMain,
);
