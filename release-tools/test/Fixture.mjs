import { execFileSync } from "node:child_process";
import { createHash } from "node:crypto";
import { mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import assert from "node:assert/strict";
import { Effect, Schema } from "effect";
import * as Npm from "@mannyc1/ts-release-npm";
import { loadCandidate } from "../application.mjs";
import { prepareCandidate } from "../prepare.mjs";

// Independent fixture coordinates. These are not live CI runs or native libraries.
export const sourceCommit = "1".repeat(40);
export const applicationCommit = sourceCommit;
export const sourceTree = "3".repeat(40);
export const ciRunId = "7101";
export const repository = "mannyc2/reactor-effect-client";
export const effectPin = "4.0.0-rc.115";
/** @typedef {"reactor-effect-client" | "reactor-effect-browser" | "reactor-effect-native"} PackageName */
/** @type {PackageName} */
export const clientPackage = "reactor-effect-client";
/** @type {PackageName} */
export const browserPackage = "reactor-effect-browser";
/** @type {PackageName} */
export const nativePackage = "reactor-effect-native";
/** The workspace packages in publication order: the client, then the hosts that pin it. */
/** @type {readonly PackageName[]} */
export const packageNames = Object.freeze([clientPackage, browserPackage, nativePackage]);
/** @type {readonly PackageName[]} */
export const hostPackages = Object.freeze([browserPackage, nativePackage]);
/** Native libraries the fixture's native archive carries, keyed by platform. */
export const nativePlatforms = Object.freeze({
  "darwin-arm64": "libreactor_effect_native.dylib",
  "linux-x64": "libreactor_effect_native.so",
});

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

export const preparationRun = () => ({
  ...workflowRun(),
  id: 8101,
  event: "workflow_dispatch",
  path: ".github/workflows/release.yml",
});
/** @param {string} [executionHostCommit] */
export const executionEnvironment = (executionHostCommit = applicationCommit) => ({
  GITHUB_REPOSITORY: repository,
  GITHUB_EVENT_NAME: "workflow_dispatch",
  GITHUB_REF: "refs/heads/main",
  GITHUB_SHA: executionHostCommit,
});

/** @param {string} path @param {unknown} value */
export const writeJson = (path, value) =>
  writeFileSync(path, JSON.stringify(value, null, 2) + "\n");

/** @param {string} path */
const entry = (path) => ({ types: `./dist/${path}.d.ts`, import: `./dist/${path}.js` });
/** Each package's workspace directory and public export map, as its real manifest declares. */
/** @type {Record<PackageName, { directory: string, exports: Record<string, { types: string, import: string }> }>} */
const workspace = {
  "reactor-effect-client": {
    directory: "packages/client",
    exports: {
      ".": entry("index"),
      "./h3": entry("h3/index"),
      "./host": entry("host"),
      "./orchestration": entry("orchestration/index"),
      "./simulation": entry("simulation/index"),
      "./testing": entry("testing/index"),
      "./wire": entry("wire"),
    },
  },
  "reactor-effect-browser": { directory: "packages/browser", exports: { ".": entry("index") } },
  "reactor-effect-native": { directory: "packages/native", exports: { ".": entry("index") } },
};

/**
 * @typedef {{
 *   version?: string,
 *   manifest?: Record<string, unknown>,
 *   manifestPackage?: PackageName,
 * }} FixtureOptions
 * A manifest override applies to one package only: the client unless manifestPackage selects a host.
 */
/** @typedef {{ name: PackageName, tarball: string, tarballPath: string, bytes: Buffer, files: string[] }} FixtureArchive */
/** @typedef {{ version: string, tarball: string, sha256: string, exports: string[], files: string[], fileSha256: Record<string, string> }} IdentityEntry */
/** @typedef {{ schemaVersion: number, platform: string, library: string, sha256: string, build: { abiVersion: number, sourceSha256: string, profile: string } }} NativeIdentity */

/** Three real, tiny npm archives sharing one version. Only the native archive carries
 * independently hashed dummy native libraries; the portable archives have no lib/ files.
 * Only the newly allocated directory is removed by withFixture.
 * @param {FixtureOptions} [options] */
const fixture = (options = {}) => {
  const temporaryRoot = fileURLToPath(new URL("../../.check/", import.meta.url));
  mkdirSync(temporaryRoot, { recursive: true });
  const directory = mkdtempSync(join(temporaryRoot, "ts-release-offline-test-"));
  const qualifiedDirectory = join(directory, "qualified");
  const candidateDirectory = join(directory, "candidate");
  mkdirSync(qualifiedDirectory);
  const version = options.version ?? "0.2.0";
  const overridden = options.manifestPackage ?? clientPackage;
  const nativeSourceSha256 = digest("independent offline native source");
  /** @type {Record<string, NativeIdentity>} */
  const native = {};
  /** @type {Record<PackageName, FixtureArchive>} */
  const packages = /** @type {Record<PackageName, FixtureArchive>} */ ({});
  /** @type {Record<PackageName, IdentityEntry>} */
  const entries = /** @type {Record<PackageName, IdentityEntry>} */ ({});
  try {
    for (const name of packageNames) {
      const { directory: workspaceDirectory, exports } = workspace[name];
      const manifest = {
        name,
        version,
        type: "module",
        exports,
        peerDependencies:
          name === clientPackage
            ? { effect: effectPin }
            : { effect: effectPin, [clientPackage]: version },
        repository: {
          type: "git",
          url: `git+https://github.com/${repository}.git`,
          directory: workspaceDirectory,
        },
        publishConfig: { access: "public", provenance: true },
        // Neither preparing nor loading an archive should execute its lifecycle scripts.
        scripts: { prepublishOnly: "exit 99", prepare: "exit 99" },
        ...(name === overridden ? options.manifest : {}),
      };
      /** @type {Record<string, string>} */
      const contents = { "package.json": JSON.stringify(manifest) + "\n" };
      for (const target of Object.values(exports)) {
        contents[target.import.slice(2)] = `export const offline = ${JSON.stringify(name)};\n`;
        contents[target.types.slice(2)] =
          `export declare const offline: ${JSON.stringify(name)};\n`;
      }
      if (name === nativePackage)
        for (const [platform, library] of Object.entries(nativePlatforms)) {
          const bytes = `offline fixture only: ${platform}\n`;
          const identity = {
            schemaVersion: 1,
            platform,
            library,
            sha256: digest(bytes),
            build: { abiVersion: 4, sourceSha256: nativeSourceSha256, profile: "release" },
          };
          contents[`lib/${platform}/${library}`] = bytes;
          contents[`lib/${platform}/native-identity.json`] = JSON.stringify(identity) + "\n";
          native[platform] = identity;
        }
      const packageDirectory = join(directory, "source", name, "package");
      for (const [path, bytes] of Object.entries(contents)) {
        const destination = join(packageDirectory, path);
        mkdirSync(dirname(destination), { recursive: true });
        writeFileSync(destination, bytes);
      }
      const tarball = `${name}-${version}.tgz`;
      const tarballPath = join(qualifiedDirectory, tarball);
      execFileSync(
        "/usr/bin/tar",
        ["--format=ustar", "-czf", tarballPath, "-C", dirname(packageDirectory), "package"],
        { env: { ...process.env, COPYFILE_DISABLE: "1" }, stdio: "pipe", timeout: 10_000 },
      );
      const bytes = readFileSync(tarballPath);
      const files = Object.keys(contents).sort();
      packages[name] = { name, tarball, tarballPath, bytes, files };
      entries[name] = {
        version,
        tarball,
        sha256: digest(bytes),
        exports: Object.keys(exports).sort(),
        files,
        fileSha256: Object.fromEntries(files.map((path) => [path, digest(contents[path] ?? "")])),
      };
    }
  } catch (error) {
    rmSync(directory, { recursive: true, force: true });
    throw error;
  }
  const identity = {
    profile: "full",
    installer: "npm",
    effect: effectPin,
    packages: entries,
    nativeSourceSha256,
    native,
  };
  const qualification = {
    format: "reactor-qualified-ci/v2",
    repository,
    sourceCommit,
    sourceTree,
    ciRunId,
    ciRunAttempt: "1",
    version,
    /** @type {Array<{ name: string, tarball: string, sha256: string }>} */
    packages: packageNames.map((name) => ({
      name,
      tarball: entries[name].tarball,
      sha256: entries[name].sha256,
    })),
  };
  const identityPath = join(qualifiedDirectory, "package-identity.json");
  const qualificationPath = join(qualifiedDirectory, "qualification.json");
  writeJson(identityPath, identity);
  writeJson(qualificationPath, qualification);
  return {
    directory,
    qualifiedDirectory,
    candidateDirectory,
    identityPath,
    qualificationPath,
    version,
    packages,
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
      executionHostCommit: applicationCommit,
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
/** The in-toto statement and its single npm subject inside one attestation bundle.
 * @param {Uint8Array} bundleBytes */
export const attestedStatement = (bundleBytes) => {
  const bundle = JSON.parse(Buffer.from(bundleBytes).toString());
  const statement = JSON.parse(Buffer.from(bundle.dsseEnvelope.payload, "base64").toString());
  assert.equal(statement.subject.length, 1);
  const subject = statement.subject[0];
  const purl = /^pkg:npm\/([^@]+)@(.+)$/.exec(subject.name);
  assert.ok(purl?.[1] !== undefined && purl[2] !== undefined);
  return {
    statement,
    packageName: purl[1],
    version: purl[2],
    sha512: String(subject.digest.sha512),
  };
};
/** Invoked once per retained package attestation: each must bind one workspace archive
 * to this fixture's exact source invocation.
 * @type {Npm.VerifyProvenance} */
export const verifyOffline = ({ source, bundleBytes }) =>
  Effect.sync(() => {
    assert.deepEqual(source, provenanceSource);
    const { statement, packageName, sha512 } = attestedStatement(bundleBytes);
    assert.ok(packageNames.some((name) => name === packageName));
    assert.match(sha512, /^[0-9a-f]{128}$/);
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

/** @typedef {Effect.Success<ReturnType<typeof loadOffline>>} Candidate */

/** The admitted publication intent of one workspace package.
 * @param {Candidate} candidate @param {string} name */
export const intentFor = (candidate, name) => {
  const intent = candidate.intents.get(name);
  assert.ok(intent, `missing admitted publication for ${name}`);
  return intent;
};
/** The package name an npm.publish operation's durable intent names.
 * @param {import("@mannyc1/ts-release").Operation} operation */
export const publishedName = (operation) =>
  Schema.decodeUnknownSync(Npm.PublishIntent)(operation.intent).name;
/** The Plan operation publishing one workspace package.
 * @param {Candidate} candidate @param {string} name */
export const operationFor = (candidate, name) => {
  const operation = candidate.plan.operations.find((entry) => publishedName(entry) === name);
  assert.ok(operation, `missing Plan operation for ${name}`);
  return operation;
};
/** One operation's line of a core release report.
 * @param {{ operations: readonly { operationId: string, status: string, dispatches: number, receipts: number, observations: number }[] }} report
 * @param {import("@mannyc1/ts-release").Operation} operation */
export const reportFor = (report, operation) => {
  const line = report.operations.find((entry) => entry.operationId === operation.operationId);
  assert.ok(line, "operation is absent from the report");
  return line;
};
