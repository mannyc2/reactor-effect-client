import { createHash } from "node:crypto";
import { constants, closeSync, fstatSync, openSync, readFileSync } from "node:fs";
import { Effect, Schema } from "effect";
import { ReleaseError } from "@mannyc1/ts-release";

export const repository = "mannyc2/reactor-effect-client";
export const packageName = "reactor-effect-client";
export const principal = "reactor-npm-publisher";
export const journalRemote = `https://github.com/${repository}.git`;
export const publicExports = [
  ".",
  "./browser",
  "./native",
  "./h3",
  "./orchestration",
  "./simulation",
  "./testing",
  "./wire",
].sort();
export const digest = Schema.String.check(Schema.isPattern(/^[a-f0-9]{64}$/));
export const commit = Schema.String.check(Schema.isPattern(/^[a-f0-9]{40}$/));
export const runId = Schema.String.check(Schema.isPattern(/^[1-9][0-9]*$/));
export const version = Schema.String.check(
  Schema.isPattern(/^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)(?:-[0-9A-Za-z.-]+)?$/),
);
const text = Schema.String.check(Schema.isMinLength(1));

export const Qualification = Schema.Struct({
  format: Schema.Literal("reactor-qualified-ci/v1"),
  repository: Schema.Literal(repository),
  sourceCommit: commit,
  sourceTree: commit,
  ciRunId: runId,
  ciRunAttempt: runId,
  name: Schema.Literal(packageName),
  version,
  sha256: digest,
});
const NativeIdentity = Schema.Struct({
  schemaVersion: Schema.Literal(1),
  platform: text,
  library: text,
  sha256: digest,
  build: Schema.Struct({
    abiVersion: Schema.Literal(2),
    sourceSha256: digest,
    profile: Schema.Literal("release"),
  }),
});
export const PackageIdentity = Schema.Struct({
  name: Schema.Literal(packageName),
  version,
  tarball: text,
  sha256: digest,
  exports: Schema.Array(Schema.String),
  nativeSourceSha256: digest,
  native: Schema.Struct({ "darwin-arm64": NativeIdentity, "linux-x64": NativeIdentity }),
  files: Schema.Array(text),
  fileSha256: Schema.Record(Schema.String, digest),
});
export const CandidateIdentity = Schema.Struct({
  format: Schema.Literal("reactor-ts-release/v1"),
  applicationCommit: commit,
  bundleSha256: digest,
  planId: digest,
  qualification: Qualification,
});
export const ApplicationInput = Schema.Struct({
  candidateDirectory: text,
  bundleSha256: digest,
  planId: digest,
  applicationCommit: commit,
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
  if (value.tarball !== `${packageName}-${value.version}.tgz`)
    reject("Unexpected archive filename");
  if (JSON.stringify([...value.exports].sort()) !== JSON.stringify(publicExports))
    reject("Public exports changed");
  if (
    new Set(value.files).size !== value.files.length ||
    JSON.stringify([...value.files].sort()) !== JSON.stringify(Object.keys(value.fileSha256).sort())
  )
    reject("Incomplete package file inventory");
  for (const [platform, library] of [
    ["darwin-arm64", "libreactor_effect_native.dylib"],
    ["linux-x64", "libreactor_effect_native.so"],
  ]) {
    const native = value.native[/** @type {"darwin-arm64" | "linux-x64"} */ (platform)];
    if (
      native.platform !== platform ||
      native.library !== library ||
      native.build.sourceSha256 !== value.nativeSourceSha256 ||
      value.fileSha256[`dist/native/${platform}/${library}`] !== native.sha256 ||
      value.fileSha256[`dist/native/${platform}/native-identity.json`] === undefined
    )
      reject("Native platform qualification identity differs");
  }
  return value;
};
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
  Schema.decodeUnknownSync(runId)(id);
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
