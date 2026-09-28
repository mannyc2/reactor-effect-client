// Effect's FileSystem cannot open with O_NOFOLLOW, which keeps a candidate-supplied symlink
// from being followed, so release inputs are read through node:fs.
// @effect-diagnostics-next-line nodeBuiltinImport:off
import { constants, closeSync, fstatSync, openSync, readFileSync } from "node:fs";
import { Crypto, Effect, Encoding, Schema, Struct } from "effect";
import { ReleaseError } from "@mannyc1/ts-release";
import * as Npm from "@mannyc1/ts-release-npm";

export const repository = "mannyc2/reactor-effect-client";
export const principal = "reactor-npm-publisher";
export const journalRemote = `https://github.com/${repository}.git`;
export const effectPin = "^4.0.0-rc.117";
/** @typedef {"reactor-effect-client" | "reactor-effect-browser" | "reactor-effect-native-linux-x64-gnu" | "reactor-effect-native-darwin-arm64" | "reactor-effect-native"} PackageName */
/** @typedef {{ readonly name: PackageName, readonly exports: readonly string[], readonly dependsOn: readonly PackageName[] }} WorkspacePackage */
/** @type {PackageName} */
export const nativePackage = "reactor-effect-native";
/** Each platform package's addon, keyed by the platform its name ends with. */
export const nativePlatforms = Object.freeze({
  "darwin-arm64": "reactor-effect-native.darwin-arm64.node",
  "linux-x64-gnu": "reactor-effect-native.linux-x64-gnu.node",
});
/** @typedef {keyof typeof nativePlatforms} NativePlatform */
/** @param {NativePlatform} platform @returns {PackageName} */
export const platformPackage = (platform) => `${nativePackage}-${platform}`;

/**
 * The published workspace packages, in publication order. The host packages
 * pin the client as an exact peer, so the client is published first and each
 * later publication depends on it. The native binding pins each platform
 * package exactly as an optional dependency, so those precede it. Every
 * package carries one release version.
 */
/** @type {readonly WorkspacePackage[]} */
export const packages = Object.freeze([
  {
    name: "reactor-effect-client",
    exports: [
      ".",
      "./Coordinator",
      "./H3",
      "./H3Source",
      "./LocalSource",
      "./Media",
      "./Peer",
      "./Playout",
      "./Reactor",
      "./ReactorError",
      "./ReactorTest",
      "./Session",
    ],
    dependsOn: [],
  },
  {
    name: "reactor-effect-browser",
    exports: [".", "./BrowserMedia", "./BrowserPeer"],
    dependsOn: ["reactor-effect-client"],
  },
  {
    name: "reactor-effect-native-linux-x64-gnu",
    exports: [],
    dependsOn: ["reactor-effect-client"],
  },
  { name: "reactor-effect-native-darwin-arm64", exports: [], dependsOn: ["reactor-effect-client"] },
  {
    name: nativePackage,
    exports: [".", "./NativePeer"],
    dependsOn: [
      "reactor-effect-client",
      "reactor-effect-native-linux-x64-gnu",
      "reactor-effect-native-darwin-arm64",
    ],
  },
]);
export const packageNames = packages.map((entry) => entry.name);

export const digest = Schema.String.check(Schema.isPattern(/^[a-f0-9]{64}$/));
export const commit = Schema.String.check(Schema.isPattern(/^[a-f0-9]{40}$/));
export const runId = Schema.String.check(Schema.isPattern(/^[1-9][0-9]*$/));
export const version = Schema.String.check(
  Schema.isPattern(/^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)(?:-[0-9A-Za-z.-]+)?$/),
);
const text = Schema.String.check(Schema.isMinLength(1));
const PackageName = Schema.Literals([
  "reactor-effect-client",
  "reactor-effect-browser",
  "reactor-effect-native-linux-x64-gnu",
  "reactor-effect-native-darwin-arm64",
  nativePackage,
]);

/** One archive's publication coordinates as bound by main CI. */
export const QualifiedPackage = Schema.Struct({ name: PackageName, tarball: text, sha256: digest });
export const Qualification = Schema.Struct({
  format: Schema.Literal("reactor-qualified-ci/v2"),
  repository: Schema.Literal(repository),
  sourceCommit: commit,
  sourceTree: commit,
  ciRunId: runId,
  ciRunAttempt: runId,
  version,
  // Every reader (CI stamp, preparation, admission, confirmation) sees the
  // canonical package set in publication order, never a subset or reordering.
  packages: Schema.Array(QualifiedPackage).check(
    Schema.makeFilter((entries) =>
      entries.length === packageNames.length &&
      entries.every((entry, index) => entry.name === packageNames[index])
        ? undefined
        : "Qualified packages must list every workspace package in publication order",
    ),
  ),
});
/** The native-identity.json staging writes into each platform package. */
const NativeIdentity = Schema.Struct({
  schemaVersion: Schema.Literal(2),
  platform: text,
  file: text,
  sha256: digest,
  build: Schema.Struct({
    schemaVersion: Schema.Literal(2),
    sourceSha256: digest,
    profile: Schema.Literal("release"),
    target: text,
    /** The Reactor libwebrtc prebuilt linked in, which staging checked against its SBOM. */
    webrtcPrebuilt: Schema.String.check(Schema.isPattern(/^webrtc-\d+-[0-9a-f]{8}-p\d+$/)),
  }),
});
const PackageEntry = Schema.Struct({
  version,
  tarball: text,
  sha256: digest,
  exports: Schema.Array(Schema.String),
  files: Schema.Array(text),
  fileSha256: Schema.Record(Schema.String, digest),
});
/** The pack smoke's package-identity.json for the full profile. */
export const PackageIdentity = Schema.Struct({
  profile: Schema.Literal("full"),
  installer: Schema.Literals(["npm", "bun"]),
  effect: Schema.Literal(effectPin),
  packages: Schema.Struct({
    "reactor-effect-client": PackageEntry,
    "reactor-effect-browser": PackageEntry,
    "reactor-effect-native-linux-x64-gnu": PackageEntry,
    "reactor-effect-native-darwin-arm64": PackageEntry,
    "reactor-effect-native": PackageEntry,
  }),
  nativeSourceSha256: digest,
  native: Schema.Struct({ "darwin-arm64": NativeIdentity, "linux-x64-gnu": NativeIdentity }),
});
export const CandidateIdentity = Schema.Struct({
  format: Schema.Literal("reactor-ts-release/v3"),
  // Immutable original preparation application, also bound by signed provenance.
  applicationCommit: commit,
  bundleSha256: digest,
  planId: digest,
  qualification: Qualification,
  provenance: Npm.ProvenanceSource,
});
export const ApplicationInput = Schema.Struct({
  candidateDirectory: text,
  candidateRunId: runId,
  bundleSha256: digest,
  planId: digest,
  applicationCommit: commit,
  // Runtime evidence only; never substitutes for any retained candidate identity.
  executionHostCommit: commit,
  ciRunId: runId,
  sourceCommit: commit,
  authorize: Schema.Boolean,
});

// JavaScript cannot pass the class type argument Schema.TaggedError asks for,
// so the constructor is annotated with it instead.
/** @type {typeof Schema.TaggedError<Rejection>} */
const RejectionClass = Schema.TaggedError;
/** A release input that breaks one of this workspace's release rules. */
export class Rejection extends RejectionClass()("Rejection", { message: Schema.String }) {}
/** @param {string} message */
export const reject = (message) => Rejection.make({ message });
/** @param {string} phase */
export const releaseFailure = (phase) =>
  ReleaseError.make({
    code: `reactor-release-${phase}`,
    message: `Release ${phase} validation failed`,
  });
/** Every failure of a release step reports as that phase's release failure.
 * @param {string} phase */
export const checked = (phase) => Effect.mapError(() => releaseFailure(phase));

export const sha256 = Effect.fnUntraced(
  /** @param {Uint8Array} bytes */
  function* (bytes) {
    const crypto = yield* Crypto.Crypto;
    // Node's SHA-256 digest of bytes in memory cannot fail.
    const hash = yield* crypto.digest("SHA-256", bytes).pipe(Effect.orDie);
    return Encoding.encodeHex(hash);
  },
);

const maximumReleaseFile = 32 * 1024 * 1024;
/** Read only a bounded regular file; never follow a candidate-supplied symlink.
 * @param {string} path */
export const readBytes = (path) => {
  const unreadable = () => reject(`Cannot read release input ${path}`);
  return Effect.acquireUseRelease(
    Effect.try({
      try: () => openSync(path, constants.O_RDONLY | constants.O_NOFOLLOW | constants.O_NONBLOCK),
      catch: unreadable,
    }),
    (fd) =>
      Effect.gen(function* () {
        const stat = yield* Effect.try({ try: () => fstatSync(fd), catch: unreadable });
        if (!stat.isFile() || stat.size > maximumReleaseFile)
          return yield* reject("Expected a bounded regular release file");
        const bytes = yield* Effect.try({ try: () => readFileSync(fd), catch: unreadable });
        if (bytes.length !== stat.size)
          return yield* reject("Release input changed during reading");
        return bytes;
      }),
    (fd) => Effect.sync(() => closeSync(fd)),
  );
};

/** JSON text as JSON.stringify writes it: compact for one line, indented for a file. */
const JsonLine = Schema.fromJsonString(Schema.Unknown);
const JsonDocument = Schema.fromJsonString(Schema.Unknown, { space: 2 });
/** @param {string} text */
export const decodeJson = (text) => Schema.decodeEffect(JsonLine)(text);
/** @param {unknown} value */
export const jsonLine = (value) => Schema.encodeEffect(JsonLine)(value);
/** @param {unknown} value */
export const jsonDocument = (value) => Schema.encodeEffect(JsonDocument)(value);
/** The JSON value of one bounded release file.
 * @param {string} path */
export const readJson = (path) =>
  readBytes(path).pipe(Effect.flatMap((bytes) => decodeJson(bytes.toString())));

/** Whether two lists hold the same strings in any order.
 * @param {{ readonly left: readonly string[], readonly right: readonly string[] }} lists */
export const sameStrings = ({ left, right }) => {
  const expected = [...right].sort();
  return (
    left.length === right.length &&
    [...left].sort().every((value, index) => value === expected[index])
  );
};

export const validatePackageIdentity = Effect.fnUntraced(
  /** @param {unknown} raw */
  function* (raw) {
    const value = yield* Schema.decodeUnknownEffect(PackageIdentity)(raw);
    const versions = new Set(packages.map((entry) => value.packages[entry.name].version));
    if (versions.size !== 1)
      return yield* reject("Workspace packages must share one release version");
    for (const entry of packages) {
      const identity = value.packages[entry.name];
      if (identity.tarball !== `${entry.name}-${identity.version}.tgz`)
        return yield* reject("Unexpected archive filename");
      if (!sameStrings({ left: identity.exports, right: entry.exports }))
        return yield* reject("Public exports changed");
      if (
        new Set(identity.files).size !== identity.files.length ||
        !sameStrings({ left: identity.files, right: Object.keys(identity.fileSha256) })
      )
        return yield* reject("Incomplete package file inventory");
      const platform = entry.name.startsWith(`${nativePackage}-`);
      if (!platform && identity.files.some((path) => path.endsWith(".node")))
        return yield* reject("A portable package carries native libraries");
    }
    const prebuilts = new Set(
      Object.values(value.native).map((identity) => identity.build.webrtcPrebuilt),
    );
    for (const platform of Struct.keys(nativePlatforms)) {
      const file = nativePlatforms[platform];
      const identity = value.native[platform];
      const archive = value.packages[platformPackage(platform)];
      if (
        identity.platform !== platform ||
        identity.file !== file ||
        identity.build.sourceSha256 !== value.nativeSourceSha256 ||
        prebuilts.size !== 1 ||
        archive.fileSha256[file] !== identity.sha256 ||
        archive.fileSha256["native-identity.json"] === undefined
      )
        return yield* reject("Native platform qualification identity differs");
    }
    return value;
  },
);
/** @typedef {Effect.Success<ReturnType<typeof validatePackageIdentity>>} ValidatedIdentity */
/** @param {ValidatedIdentity} identity */
export const releaseVersion = (identity) => identity.packages["reactor-effect-client"].version;
/** Publication coordinates in publication order.
 * @param {ValidatedIdentity} identity */
export const qualifiedPackages = (identity) =>
  packages.map((entry) => ({
    name: entry.name,
    tarball: identity.packages[entry.name].tarball,
    sha256: identity.packages[entry.name].sha256,
  }));
/** A qualification names exactly the archives of one validated identity.
 * @param {{ readonly qualification: typeof Qualification.Type, readonly identity: ValidatedIdentity }} candidate */
export const qualificationMatches = ({ qualification, identity }) => {
  const expected = qualifiedPackages(identity);
  return (
    qualification.version === releaseVersion(identity) &&
    qualification.packages.length === expected.length &&
    expected.every((entry, index) => {
      const actual = qualification.packages[index];
      return (
        actual !== undefined &&
        actual.name === entry.name &&
        actual.tarball === entry.tarball &&
        actual.sha256 === entry.sha256
      );
    })
  );
};
/** The exact operator confirmation for one qualified release.
 * @param {typeof Qualification.Type} qualification */
export const confirmationFor = (qualification) =>
  `publish ${qualification.packages.map((entry) => `${entry.name}@${qualification.version}`).join(" ")}`;
/** Dependency evidence for an offline provider preparation: declared, never dispatched. */
export const preparationContext = Effect.fnUntraced(
  /** @param {import("@mannyc1/ts-release").Plan} plan @param {import("@mannyc1/ts-release").Operation} operation */
  function* (plan, operation) {
    const dependencies = [];
    for (const id of operation.dependsOn) {
      const dependency = plan.operations.find((entry) => entry.operationId === id);
      // A loaded Plan has no dangling dependency.
      if (dependency === undefined) return yield* Effect.die("Plan dependency is absent");
      dependencies.push({ operation: dependency, receipts: [], observations: [] });
    }
    return { own: { operation, receipts: [], observations: [] }, dependencies };
  },
);
/** A release coordinate keeps one journal even when an operator prepares different bytes.
 * @param {string} selectedVersion */
export const journalId = (selectedVersion) => `reactor-npm:${repository}:${selectedVersion}`;

const WorkflowRun = Schema.Struct({
  id: Schema.Int,
  run_attempt: Schema.Int,
  path: text,
  status: Schema.Literal("completed"),
  conclusion: Schema.Literal("success"),
  head_branch: Schema.Literal("main"),
  head_sha: commit,
  event: Schema.Literals(["push", "workflow_dispatch"]),
  repository: Schema.Struct({ full_name: Schema.Literal(repository) }),
  head_repository: Schema.Struct({ full_name: Schema.Literal(repository) }),
});
/** Only artifacts from the real successful workflow on this repository's main are eligible. */
export const validateRun = Effect.fnUntraced(
  /** @param {unknown} raw @param {string} id @param {"ci" | "release"} kind */
  function* (raw, id, kind) {
    yield* Schema.decodeEffect(runId)(id);
    const run = yield* Schema.decodeUnknownEffect(WorkflowRun)(raw);
    if (
      String(run.id) !== id ||
      !Number.isSafeInteger(run.id) ||
      run.run_attempt < 1 ||
      run.path !== `.github/workflows/${kind}.yml` ||
      (kind === "release" && run.event !== "workflow_dispatch")
    )
      return yield* reject("Run identity or workflow differs");
    return run;
  },
);
