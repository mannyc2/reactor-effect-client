import assert from "node:assert/strict";
import * as NodeServices from "@effect/platform-node/NodeServices";
import {
  ConfigProvider,
  Crypto,
  Effect,
  Encoding,
  FileSystem,
  ManagedRuntime,
  Path,
  Schema,
  Struct,
} from "effect";
import { ChildProcess, ChildProcessSpawner } from "effect/unstable/process";
import * as Npm from "@mannyc1/ts-release-npm";
import { loadCandidate } from "../application.mjs";
import { jsonDocument, jsonLine } from "../model.mjs";
import { prepareCandidate } from "../prepare.mjs";

// Independent fixture coordinates. These are not live CI runs or native libraries.
export const sourceCommit = "1".repeat(40);
export const applicationCommit = sourceCommit;
export const sourceTree = "3".repeat(40);
export const ciRunId = "7101";
export const repository = "mannyc2/reactor-effect-client";
export const effectPin = "4.0.0";
/** @typedef {import("../model.mjs").PackageName} PackageName */
/** @type {PackageName} */
export const clientPackage = "reactor-effect-client";
/** @type {PackageName} */
export const browserPackage = "reactor-effect-browser";
/** @type {PackageName} */
export const nativePackage = "reactor-effect-native";
/** Each platform package's addon, keyed by platform. */
export const nativePlatforms = Object.freeze({
  "darwin-arm64": "reactor-effect-native.darwin-arm64.node",
  "linux-x64-gnu": "reactor-effect-native.linux-x64-gnu.node",
});
/** @type {PackageName} */
export const linuxPackage = "reactor-effect-native-linux-x64-gnu";
/** @type {PackageName} */
export const darwinPackage = "reactor-effect-native-darwin-arm64";
/** @type {readonly PackageName[]} */
export const platformPackages = Object.freeze([linuxPackage, darwinPackage]);
/** The workspace packages in publication order: the client, the packages that follow it,
 * and last the native binding, which pins the platform packages. */
/** @type {readonly PackageName[]} */
export const packageNames = Object.freeze([
  clientPackage,
  browserPackage,
  linuxPackage,
  darwinPackage,
  nativePackage,
]);
/** Every package published after the client. */
/** @type {readonly PackageName[]} */
export const hostPackages = Object.freeze([
  browserPackage,
  linuxPackage,
  darwinPackage,
  nativePackage,
]);

const services = ManagedRuntime.make(NodeServices.layer);
/** Run one test's program with the Node services release-tools uses, then close its scope.
 * @template A, E
 * @param {Effect.Effect<A, E, NodeServices.NodeServices | import("effect/Scope").Scope>} program */
export const runTest = (program) => services.runPromise(Effect.scoped(program));

/** The release failure code an effect must fail with.
 * @template A, R
 * @param {Effect.Effect<A, import("@mannyc1/ts-release/bundle").AdoptionError | import("@mannyc1/ts-release").ReleaseError, R>} effect */
export const failureCode = (effect) =>
  Effect.flip(effect).pipe(
    Effect.map((error) => {
      assert.ok(error._tag === "ReleaseError", error.message);
      return error.code;
    }),
  );

/** Read environment variables from this record instead of the process environment.
 * @param {Record<string, string>} env */
export const withEnvironment = (env) =>
  Effect.provideService(ConfigProvider.ConfigProvider, ConfigProvider.fromEnv({ env }));

export const digest = Effect.fnUntraced(
  /** @param {string | Uint8Array} bytes @param {Crypto.DigestAlgorithm} [algorithm] */
  function* (bytes, algorithm = "SHA-256") {
    const crypto = yield* Crypto.Crypto;
    const data = typeof bytes === "string" ? new TextEncoder().encode(bytes) : bytes;
    return Encoding.encodeHex(yield* crypto.digest(algorithm, data));
  },
);

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

export const writeJson = Effect.fnUntraced(
  /** @param {string} path @param {unknown} value */
  function* (path, value) {
    const fs = yield* FileSystem.FileSystem;
    yield* fs.writeFileString(path, `${yield* jsonDocument(value)}\n`);
  },
);

/** @param {string} path */
const entry = (path) => ({ types: `./dist/${path}.d.ts`, import: `./dist/${path}.js` });
/** Each package's workspace directory and public export map, as its real manifest declares. */
/** @type {Record<PackageName, { directory: string, exports: Record<string, { types: string, import: string }> }>} */
const workspace = {
  "reactor-effect-client": {
    directory: "packages/client",
    exports: {
      ".": entry("index"),
      ...Object.fromEntries(
        [
          "CoordinatorClient",
          "H3",
          "H3Source",
          "LocalSource",
          "Media",
          "Peer",
          "Playout",
          "Reactor",
          "ReactorError",
          "ReactorTest",
          "Session",
          "ViduS2Avatar",
          "FastH3",
          "FastH3Source",
          "Ledger",
          "References",
        ].map((module) => [`./${module}`, entry(module)]),
      ),
    },
  },
  "reactor-effect-browser": {
    directory: "packages/browser",
    exports: {
      ".": entry("index"),
      "./BrowserMedia": entry("BrowserMedia"),
      "./BrowserPeer": entry("BrowserPeer"),
    },
  },
  "reactor-effect-native-linux-x64-gnu": {
    directory: "packages/native/npm/linux-x64-gnu",
    exports: {},
  },
  "reactor-effect-native-darwin-arm64": {
    directory: "packages/native/npm/darwin-arm64",
    exports: {},
  },
  "reactor-effect-native": {
    directory: "packages/native",
    exports: { ".": entry("index"), "./NativePeer": entry("NativePeer") },
  },
};
/** One value for each workspace package, in publication order.
 * @template T
 * @param {(name: PackageName) => T} value
 * @returns {Record<PackageName, T>} */
const perPackage = (value) => ({
  "reactor-effect-client": value("reactor-effect-client"),
  "reactor-effect-browser": value("reactor-effect-browser"),
  "reactor-effect-native-linux-x64-gnu": value("reactor-effect-native-linux-x64-gnu"),
  "reactor-effect-native-darwin-arm64": value("reactor-effect-native-darwin-arm64"),
  "reactor-effect-native": value("reactor-effect-native"),
});

/**
 * @typedef {{
 *   version?: string,
 *   manifest?: Record<string, unknown>,
 *   manifestPackage?: PackageName,
 * }} FixtureOptions
 * A manifest override applies to one package only: the client unless manifestPackage selects a host.
 */
/** @typedef {{ name: PackageName, tarball: string, tarballPath: string, bytes: Uint8Array, files: string[] }} FixtureArchive */
/** @typedef {{ version: string, tarball: string, sha256: string, exports: string[], files: string[], fileSha256: Record<string, string> }} IdentityEntry */
/** @typedef {{ schemaVersion: number, platform: string, file: string, sha256: string, build: { schemaVersion: number, sourceSha256: string, profile: string, target: string, webrtcPrebuilt: string } }} NativeIdentity */

/** Five real, tiny npm archives sharing one version. Only the platform archives carry
 * independently hashed dummy addons; the others have no .node files.
 * Only the newly allocated directory is removed, when the test's scope closes. */
export const makeFixture = Effect.fnUntraced(
  /** @param {FixtureOptions} [options] */
  function* (options = {}) {
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
    const temporaryRoot = yield* path.fromFileUrl(new URL("../../.check/", import.meta.url));
    yield* fs.makeDirectory(temporaryRoot, { recursive: true });
    const directory = yield* fs.makeTempDirectoryScoped({
      directory: temporaryRoot,
      prefix: "ts-release-offline-test-",
    });
    const qualifiedDirectory = path.join(directory, "qualified");
    const candidateDirectory = path.join(directory, "candidate");
    yield* fs.makeDirectory(qualifiedDirectory);
    const version = options.version ?? "0.2.0";
    const overridden = options.manifestPackage ?? clientPackage;
    const nativeSourceSha256 = yield* digest("independent offline native source");
    /** @param {PackageName} name */
    const pack = (name) =>
      Effect.gen(function* () {
        const { directory: workspaceDirectory, exports } = workspace[name];
        const platform = Struct.keys(nativePlatforms).find(
          (key) => name === `${nativePackage}-${key}`,
        );
        const manifest = {
          name,
          version,
          type: "module",
          exports,
          ...(platform === undefined
            ? {
                peerDependencies:
                  name === clientPackage
                    ? { effect: `~${effectPin}` }
                    : { effect: `~${effectPin}`, [clientPackage]: version },
              }
            : {}),
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
        const contents = { "package.json": `${yield* jsonLine(manifest)}\n` };
        const literal = yield* jsonLine(name);
        for (const target of Object.values(exports)) {
          contents[target.import.slice(2)] = `export const offline = ${literal};\n`;
          contents[target.types.slice(2)] = `export declare const offline: ${literal};\n`;
        }
        /** @type {NativeIdentity | undefined} */
        let native;
        if (platform !== undefined) {
          const file = nativePlatforms[platform];
          const bytes = `offline fixture only: ${platform}\n`;
          native = {
            schemaVersion: 2,
            platform,
            file,
            sha256: yield* digest(bytes),
            build: {
              schemaVersion: 2,
              sourceSha256: nativeSourceSha256,
              profile: "release",
              target:
                platform === "darwin-arm64" ? "aarch64-apple-darwin" : "x86_64-unknown-linux-gnu",
              webrtcPrebuilt: "webrtc-7907-a5ddff60-p9",
            },
          };
          contents[file] = bytes;
          contents["native-identity.json"] = `${yield* jsonLine(native)}\n`;
        }
        const packageDirectory = path.join(directory, "source", name, "package");
        for (const [file, text] of Object.entries(contents)) {
          const destination = path.join(packageDirectory, file);
          yield* fs.makeDirectory(path.dirname(destination), { recursive: true });
          yield* fs.writeFileString(destination, text);
        }
        const tarball = `${name}-${version}.tgz`;
        const tarballPath = path.join(qualifiedDirectory, tarball);
        const exitCode = yield* spawner
          .exitCode(
            ChildProcess.make(
              "/usr/bin/tar",
              [
                "--format=ustar",
                "-czf",
                tarballPath,
                "-C",
                path.dirname(packageDirectory),
                "package",
              ],
              { env: { COPYFILE_DISABLE: "1" }, extendEnv: true, stderr: "inherit" },
            ),
          )
          .pipe(Effect.timeout("10 seconds"));
        assert.equal(exitCode, 0);
        const bytes = yield* fs.readFile(tarballPath);
        const files = Object.keys(contents).sort();
        /** @type {Record<string, string>} */
        const fileSha256 = {};
        for (const file of files) fileSha256[file] = yield* digest(contents[file] ?? "");
        /** @type {FixtureArchive} */
        const archive = { name, tarball, tarballPath, bytes, files };
        /** @type {IdentityEntry} */
        const identity = {
          version,
          tarball,
          sha256: yield* digest(bytes),
          exports: Object.keys(exports).sort(),
          files,
          fileSha256,
        };
        return { archive, identity, native };
      });
    const built = yield* Effect.all(perPackage(pack));
    const packages = perPackage((name) => built[name].archive);
    const entries = perPackage((name) => built[name].identity);
    /** @type {Record<string, NativeIdentity>} */
    const native = {};
    for (const name of packageNames) {
      const platform = built[name].native;
      if (platform !== undefined) native[platform.platform] = platform;
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
    const identityPath = path.join(qualifiedDirectory, "package-identity.json");
    const qualificationPath = path.join(qualifiedDirectory, "qualification.json");
    yield* writeJson(identityPath, identity);
    yield* writeJson(qualificationPath, qualification);
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
  },
);

/** @typedef {Effect.Success<ReturnType<typeof makeFixture>>} Fixture */

export const prepared = Effect.fnUntraced(
  /** @param {Fixture} fixture */
  function* (fixture) {
    const identity = yield* prepareOffline(fixture.options);
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
  },
);

// Structural witnesses only: the fake signer/verifier exercise exact-byte contracts,
// never claim signature trust, and are not passed to the production application.
export const provenanceSource = Npm.ProvenanceSource.make({
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

/** The parts of an npm in-toto statement the offline checks read. */
const Statement = Schema.fromJsonString(
  Schema.Struct({
    subject: Schema.Array(
      Schema.Struct({ name: Schema.String, digest: Schema.Struct({ sha512: Schema.String }) }),
    ),
    predicate: Schema.Struct({
      buildDefinition: Schema.Struct({
        resolvedDependencies: Schema.Array(
          Schema.Struct({ digest: Schema.Struct({ gitCommit: Schema.String }) }),
        ),
      }),
      runDetails: Schema.Struct({ metadata: Schema.Struct({ invocationId: Schema.String }) }),
    }),
  }),
);
const AttestationBundle = Schema.fromJsonString(
  Schema.Struct({ dsseEnvelope: Schema.Struct({ payload: Schema.Uint8ArrayFromBase64 }) }),
);
/** The statement one attestation payload signs. A malformed fixture statement is a test bug.
 * @param {Uint8Array} payload */
export const decodeStatement = (payload) =>
  Schema.decodeEffect(Statement)(new TextDecoder().decode(payload)).pipe(Effect.orDie);
/** The in-toto statement and its single npm subject inside one attestation bundle. */
export const attestedStatement = Effect.fnUntraced(
  /** @param {Uint8Array} bundleBytes */
  function* (bundleBytes) {
    const bundle = yield* Schema.decodeEffect(AttestationBundle)(
      new TextDecoder().decode(bundleBytes),
    ).pipe(Effect.orDie);
    const statement = yield* decodeStatement(bundle.dsseEnvelope.payload);
    assert.equal(statement.subject.length, 1);
    const subject = statement.subject[0];
    assert.ok(subject !== undefined);
    const purl = /^pkg:npm\/([^@]+)@(.+)$/.exec(subject.name);
    assert.ok(purl?.[1] !== undefined && purl[2] !== undefined);
    return {
      statement,
      packageName: purl[1],
      version: purl[2],
      sha512: subject.digest.sha512,
    };
  },
);
/** Invoked once per retained package attestation: each must bind one workspace archive
 * to this fixture's exact source invocation.
 * @type {Npm.VerifyProvenance} */
export const verifyOffline = ({ source, bundleBytes }) =>
  Effect.gen(function* () {
    assert.deepEqual(source, provenanceSource);
    const { statement, packageName, sha512 } = yield* attestedStatement(bundleBytes);
    assert.ok(packageNames.some((name) => name === packageName));
    assert.match(sha512, /^[0-9a-f]{128}$/);
    assert.equal(
      statement.predicate.buildDefinition.resolvedDependencies[0]?.digest.gitCommit,
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
 * @param {{ candidate: Candidate, name: string }} selection */
export const intentFor = ({ candidate, name }) => {
  const intent = candidate.intents.get(name);
  assert.ok(intent, `missing admitted publication for ${name}`);
  return intent;
};
/** The Plan operation publishing one workspace package.
 * @param {{ candidate: Candidate, name: string }} selection */
export const operationFor = ({ candidate, name }) => {
  const publishes = Schema.is(
    Schema.Struct({ intent: Schema.Struct({ name: Schema.Literal(name) }) }),
  );
  const operation = candidate.plan.operations.find((entry) => publishes(entry));
  assert.ok(operation, `missing Plan operation for ${name}`);
  return operation;
};
/** One operation's line of a core release report.
 * @param {{ report: { operations: readonly { operationId: string, status: string, dispatches: number, receipts: number, observations: number }[] }, operation: import("@mannyc1/ts-release").Operation }} selection */
export const reportFor = ({ report, operation }) => {
  const line = report.operations.find((entry) => entry.operationId === operation.operationId);
  assert.ok(line, "operation is absent from the report");
  return line;
};
