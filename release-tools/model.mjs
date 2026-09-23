import { createHash } from "node:crypto";
import { constants, closeSync, fstatSync, openSync, readFileSync } from "node:fs";
import { Effect, Schema } from "effect";
import { ReleaseError } from "@mannyc1/ts-release";
import * as Npm from "@mannyc1/ts-release-npm";

export const repository = "mannyc2/reactor-effect-client";
export const principal = "reactor-npm-publisher";
export const journalRemote = `https://github.com/${repository}.git`;
export const effectPin = "4.0.0-rc.115";
/** @typedef {"reactor-effect-client" | "reactor-effect-browser" | "reactor-effect-native"} PackageName */
/** @typedef {{ readonly name: PackageName, readonly exports: readonly string[], readonly dependsOn: readonly PackageName[] }} WorkspacePackage */
/** @type {PackageName} */
export const nativePackage = "reactor-effect-native";

/**
 * The published workspace packages, in publication order. The host packages
 * pin the client as an exact peer, so the client is published first and each
 * host publication depends on it. Every package carries one release version.
 */
/** @type {readonly WorkspacePackage[]} */
export const packages = Object.freeze([
  {
    name: "reactor-effect-client",
    exports: [".", "./h3", "./host", "./orchestration", "./simulation", "./testing", "./wire"],
    dependsOn: [],
  },
  { name: "reactor-effect-browser", exports: ["."], dependsOn: ["reactor-effect-client"] },
  { name: nativePackage, exports: ["."], dependsOn: ["reactor-effect-client"] },
]);
export const packageNames = packages.map((entry) => entry.name);
/** Native libraries the qualified native package must carry, keyed by platform. */
export const nativePlatforms = Object.freeze({
  "darwin-arm64": "libreactor_effect_native.dylib",
  "linux-x64": "libreactor_effect_native.so",
});

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
const NativeIdentity = Schema.Struct({
  schemaVersion: Schema.Literal(1),
  platform: text,
  library: text,
  sha256: digest,
  build: Schema.Struct({
    abiVersion: Schema.Literal(4),
    sourceSha256: digest,
    profile: Schema.Literal("release"),
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
    "reactor-effect-native": PackageEntry,
  }),
  nativeSourceSha256: digest,
  native: Schema.Struct({ "darwin-arm64": NativeIdentity, "linux-x64": NativeIdentity }),
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

/** @param {string} message @returns {never} */
export function reject(message) {
  throw new Error(message);
}
/** @param {string} phase */
export const releaseFailure = (phase) =>
  new ReleaseError({
    code: `reactor-release-${phase}`,
    message: `Release ${phase} validation failed`,
  });
/** @param {Uint8Array | string} bytes */
export const sha256 = (bytes) => createHash("sha256").update(bytes).digest("hex");
/** Read only a bounded regular file; never follow a candidate-supplied symlink.
 * @param {string} path @param {number} [maximum] */
export const readBytes = (path, maximum = 32 * 1024 * 1024) => {
  const fd = openSync(path, constants.O_RDONLY | constants.O_NOFOLLOW | constants.O_NONBLOCK);
  try {
    const stat = fstatSync(fd);
    if (!stat.isFile() || stat.size > maximum) reject("Expected a bounded regular release file");
    const bytes = readFileSync(fd);
    if (bytes.length !== stat.size) reject("Release input changed during reading");
    return bytes;
  } finally {
    closeSync(fd);
  }
};
/** @template A @param {string} phase @param {() => A} body */
export const checked = (phase, body) =>
  Effect.try({
    try: body,
    catch: () => releaseFailure(phase),
  });

/** @param {unknown} raw */
export const validatePackageIdentity = (raw) => {
  const value = Schema.decodeUnknownSync(PackageIdentity)(raw);
  const versions = new Set(packages.map((entry) => value.packages[entry.name].version));
  if (versions.size !== 1) reject("Workspace packages must share one release version");
  for (const entry of packages) {
    const identity = value.packages[entry.name];
    if (identity.tarball !== `${entry.name}-${identity.version}.tgz`)
      reject("Unexpected archive filename");
    if (JSON.stringify([...identity.exports].sort()) !== JSON.stringify([...entry.exports].sort()))
      reject("Public exports changed");
    if (
      new Set(identity.files).size !== identity.files.length ||
      JSON.stringify([...identity.files].sort()) !==
        JSON.stringify(Object.keys(identity.fileSha256).sort())
    )
      reject("Incomplete package file inventory");
    if (entry.name !== nativePackage && identity.files.some((path) => path.startsWith("lib/")))
      reject("A portable package carries native libraries");
  }
  const native = value.packages[nativePackage];
  for (const [platform, library] of Object.entries(nativePlatforms)) {
    const identity = value.native[/** @type {"darwin-arm64" | "linux-x64"} */ (platform)];
    if (
      identity.platform !== platform ||
      identity.library !== library ||
      identity.build.sourceSha256 !== value.nativeSourceSha256 ||
      native.fileSha256[`lib/${platform}/${library}`] !== identity.sha256 ||
      native.fileSha256[`lib/${platform}/native-identity.json`] === undefined
    )
      reject("Native platform qualification identity differs");
  }
  return value;
};
/** @typedef {ReturnType<typeof validatePackageIdentity>} ValidatedIdentity */
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
 * @param {typeof Qualification.Type} qualification @param {ValidatedIdentity} identity */
export const qualificationMatches = (qualification, identity) => {
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
/** Dependency evidence for an offline provider preparation: declared, never dispatched.
 * @param {import("@mannyc1/ts-release").Plan} plan @param {import("@mannyc1/ts-release").Operation} operation */
export const preparationContext = (plan, operation) => ({
  own: { operation, receipts: [], observations: [] },
  dependencies: operation.dependsOn.map((id) => {
    const dependency = plan.operations.find((entry) => entry.operationId === id);
    if (dependency === undefined) reject("Plan dependency is absent");
    return { operation: dependency, receipts: [], observations: [] };
  }),
});
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
/** Only artifacts from the real successful workflow on this repository's main are eligible.
 * @param {unknown} raw @param {string} id @param {"ci" | "release"} kind */
export const validateRun = (raw, id, kind) => {
  Schema.decodeSync(runId)(id);
  const run = Schema.decodeUnknownSync(WorkflowRun)(raw);
  if (
    String(run.id) !== id ||
    !Number.isSafeInteger(run.id) ||
    run.run_attempt < 1 ||
    run.path !== `.github/workflows/${kind}.yml` ||
    (kind === "release" && run.event !== "workflow_dispatch")
  )
    reject("Run identity or workflow differs");
  return run;
};
