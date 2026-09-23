import { execFileSync } from "node:child_process";
import { createHash } from "node:crypto";
import { mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import assert from "node:assert/strict";
import { Effect } from "effect";
import * as Npm from "@mannyc1/ts-release-npm";
import { loadCandidate } from "../application.mjs";
import { prepareCandidate } from "../prepare.mjs";

// Independent fixture coordinates. These are not live CI runs or native libraries.
export const sourceCommit = "1".repeat(40);
export const applicationCommit = sourceCommit;
export const sourceTree = "3".repeat(40);
export const ciRunId = "7101";
export const packageName = "reactor-effect-client";
export const repository = "mannyc2/reactor-effect-client";

/** @param {string | Uint8Array} bytes @param {string} [algorithm] */
export const digest = (bytes, algorithm = "sha256") =>
  createHash(algorithm).update(bytes).digest("hex");

export const workflowRun = () => ({
  id: 7101,
  run_attempt: 1,
  path: ".github/workflows/ci.yml",
  status: "completed",
  conclusion: "success",
  head_branch: "main",
  head_sha: sourceCommit,
  event: "push",
  repository: { full_name: repository },
  head_repository: { full_name: repository },
});

/** @param {string} path @param {unknown} value */
export const writeJson = (path, value) =>
  writeFileSync(path, JSON.stringify(value, null, 2) + "\n");

/** @typedef {{ version?: string, manifest?: Record<string, unknown> }} FixtureOptions */

/** A real, tiny npm archive with independently hashed dummy native artifacts.
 * Only the newly allocated directory is removed by withFixture.
 * @param {FixtureOptions} [options] */
const fixture = (options = {}) => {
  const temporaryRoot = fileURLToPath(new URL("../../.check/", import.meta.url));
  mkdirSync(temporaryRoot, { recursive: true });
  const directory = mkdtempSync(join(temporaryRoot, "ts-release-offline-test-"));
  const qualifiedDirectory = join(directory, "qualified");
  const packageDirectory = join(directory, "source", "package");
  const candidateDirectory = join(directory, "candidate");
  mkdirSync(qualifiedDirectory);
  mkdirSync(packageDirectory, { recursive: true });
  const version = options.version ?? "0.2.0";
  const exports = {
    ".": { types: "./dist/index.d.ts", import: "./dist/index.js" },
    "./browser": { types: "./dist/browser/index.d.ts", import: "./dist/browser/index.js" },
    "./native": { types: "./dist/native/index.d.ts", import: "./dist/native/index.js" },
    "./h3": { types: "./dist/h3/index.d.ts", import: "./dist/h3/index.js" },
    "./orchestration": {
      types: "./dist/orchestration/index.d.ts",
      import: "./dist/orchestration/index.js",
    },
    "./simulation": { types: "./dist/simulation/index.d.ts", import: "./dist/simulation/index.js" },
    "./testing": { types: "./dist/testing/index.d.ts", import: "./dist/testing/index.js" },
    "./wire": { types: "./dist/wire.d.ts", import: "./dist/wire.js" },
  };
  const manifest = {
    name: packageName,
    version,
    type: "module",
    exports,
    peerDependencies: { effect: "4.0.0-rc.115" },
    repository: { type: "git", url: `git+https://github.com/${repository}.git` },
    publishConfig: { access: "public", provenance: true },
    // Neither preparing nor loading an archive should execute its lifecycle scripts.
    scripts: { prepublishOnly: "exit 99", prepare: "exit 99" },
    ...options.manifest,
  };
  /** @type {Record<string, string>} */
  const contents = { "package.json": JSON.stringify(manifest) + "\n" };
  for (const entry of Object.values(exports)) {
    contents[entry.import.slice(2)] = "export const offline = true;\n";
    contents[entry.types.slice(2)] = "export declare const offline: true;\n";
  }
  const nativeSourceSha256 = digest("independent offline native source");
  /** @param {string} platform @param {string} library */
  const nativeIdentity = (platform, library) => {
    const bytes = `offline fixture only: ${platform}\n`;
    const identity = {
      schemaVersion: 1,
      platform,
      library,
      sha256: digest(bytes),
      build: { abiVersion: 2, sourceSha256: nativeSourceSha256, profile: "release" },
    };
    contents[`dist/native/${platform}/${library}`] = bytes;
    contents[`dist/native/${platform}/native-identity.json`] = JSON.stringify(identity) + "\n";
    return identity;
  };
  const native = {
    "darwin-arm64": nativeIdentity("darwin-arm64", "libreactor_effect_native.dylib"),
    "linux-x64": nativeIdentity("linux-x64", "libreactor_effect_native.so"),
  };
  for (const [path, bytes] of Object.entries(contents)) {
    const destination = join(packageDirectory, path);
    mkdirSync(dirname(destination), { recursive: true });
    writeFileSync(destination, bytes);
  }
  const tarball = `${packageName}-${version}.tgz`;
  const tarballPath = join(qualifiedDirectory, tarball);
  try {
    execFileSync(
      "/usr/bin/tar",
      ["--format=ustar", "-czf", tarballPath, "-C", dirname(packageDirectory), "package"],
      { env: { ...process.env, COPYFILE_DISABLE: "1" }, stdio: "pipe", timeout: 10_000 },
    );
  } catch (error) {
    rmSync(directory, { recursive: true, force: true });
    throw error;
  }
  const bytes = readFileSync(tarballPath);
  const identity = {
    name: packageName,
    version,
    tarball,
    sha256: digest(bytes),
    exports: Object.keys(exports).sort(),
    nativeSourceSha256,
    native,
    files: Object.keys(contents).sort(),
    fileSha256: Object.fromEntries(
      Object.entries(contents).map(([path, value]) => [path, digest(value)]),
    ),
  };
  const qualification = {
    format: "reactor-qualified-ci/v1",
    repository,
    sourceCommit,
    sourceTree,
    ciRunId,
    ciRunAttempt: "1",
    name: packageName,
    version,
    sha256: identity.sha256,
  };
  const identityPath = join(qualifiedDirectory, "package-identity.json");
  const qualificationPath = join(qualifiedDirectory, "qualification.json");
  writeJson(identityPath, identity);
  writeJson(qualificationPath, qualification);
  return {
    directory,
    qualifiedDirectory,
    candidateDirectory,
    tarballPath,
    identityPath,
    qualificationPath,
    bytes,
    identity,
    qualification,
    options: {
      qualifiedDirectory,
      candidateDirectory,
      applicationCommit,
      run: workflowRun(),
      ciRunId,
      source: provenanceSource,
    },
  };
};

/** @typedef {ReturnType<typeof fixture>} Fixture */

/** @template A @param {(fixture: Fixture) => A | Promise<A>} body
 * @param {FixtureOptions} [options] */
export const withFixture = async (body, options) => {
  const value = fixture(options);
  try {
    return await body(value);
  } finally {
    rmSync(value.directory, { recursive: true, force: true });
  }
};

/** @param {Fixture} fixture */
export const prepared = async (fixture) => {
  const identity = await Effect.runPromise(prepareOffline(fixture.options));
  return {
    identity,
    input: {
      candidateDirectory: fixture.candidateDirectory,
      candidateRunId: provenanceSource.runId,
      bundleSha256: identity.bundleSha256,
      planId: identity.planId,
      applicationCommit,
      ciRunId,
      sourceCommit,
      authorize: false,
    },
  };
};

// Structural witnesses only: the fake signer/verifier exercise exact-byte contracts,
// never claim signature trust, and are not passed to the production application.
export const provenanceSource = new Npm.ProvenanceSource({
  format: "npm-github-actions-provenance-source/v1",
  serverUrl: "https://github.com",
  repository,
  workflow: ".github/workflows/release.yml",
  workflowRef: "refs/heads/main",
  sourceRef: "refs/heads/main",
  sourceCommit,
  eventName: "workflow_dispatch",
  repositoryId: "11",
  repositoryOwnerId: "12",
  runnerEnvironment: "github-hosted",
  runId: "8101",
  runAttempt: "1",
  repositoryVisibility: "public",
});
/** @type {Npm.Attest} */
export const attestOffline = ({ payload }) =>
  Effect.succeed({
    bundleBytes: new TextEncoder().encode(
      JSON.stringify({
        mediaType: "application/vnd.dev.sigstore.bundle.v0.3+json",
        dsseEnvelope: {
          payloadType: "application/vnd.in-toto+json",
          payload: Buffer.from(payload).toString("base64"),
          signatures: [{ sig: "AA==" }],
        },
        verificationMaterial: {
          certificate: { rawBytes: "AA==" },
          tlogEntries: [
            {
              canonicalizedBody: "AA==",
              logId: { keyId: "AA==" },
              integratedTime: "1",
              logIndex: "0",
              kindVersion: { kind: "dsse", version: "0.0.1" },
              inclusionProof: {
                logIndex: "0",
                treeSize: "1",
                hashes: [],
                rootHash: Buffer.alloc(32).toString("base64"),
                checkpoint: {
                  envelope: `untrusted-fixture\n1\n${Buffer.alloc(32).toString("base64")}\n\n`,
                },
              },
            },
          ],
        },
      }),
    ),
  });
/** @type {Npm.VerifyProvenance} */
export const verifyOffline = ({ source, bundleBytes }) =>
  Effect.sync(() => {
    assert.deepEqual(source, provenanceSource);
    const bundle = JSON.parse(Buffer.from(bundleBytes).toString());
    const statement = JSON.parse(Buffer.from(bundle.dsseEnvelope.payload, "base64").toString());
    assert.equal(
      statement.predicate.buildDefinition.resolvedDependencies[0].digest.gitCommit,
      sourceCommit,
    );
    assert.equal(
      statement.predicate.runDetails.metadata.invocationId,
      `https://github.com/${repository}/actions/runs/8101/attempts/1`,
    );
  });
/** @param {Parameters<typeof prepareCandidate>[0]} options */
export const prepareOffline = (options) =>
  prepareCandidate(options, {
    attest: attestOffline,
    verifyProvenance: verifyOffline,
  });
/** @param {unknown} input */
export const loadOffline = (input) => loadCandidate(input, verifyOffline);
