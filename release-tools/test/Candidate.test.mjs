import assert from "node:assert/strict";
import { test } from "bun:test";
import { Effect, FileSystem, Path, Result, Schema } from "effect";
import { Plan, createPlan } from "@mannyc1/ts-release";
import * as Npm from "@mannyc1/ts-release-npm";
import { decodeJson, readBytes, readJson, validatePackageIdentity } from "../model.mjs";
import {
  browserPackage,
  clientPackage,
  digest,
  effectPin,
  failureCode,
  hostPackages,
  intentFor,
  loadOffline,
  darwinPackage,
  linuxPackage,
  makeFixture,
  nativePackage,
  platformPackages,
  operationFor,
  packageNames,
  prepareOffline,
  prepared,
  runTest,
  writeJson,
} from "./Fixture.mjs";

/** The JSON value of one retained owned file. */
const retainedJson = Effect.fnUntraced(
  /** @param {import("./Fixture.mjs").Candidate} loaded @param {string} logicalName */
  function* (loaded, logicalName) {
    const file = loaded.bundle.artifacts.find((entry) => entry.logicalName === logicalName);
    assert.ok(file?._tag === "OwnedFile", `${logicalName} is not a retained owned file`);
    const bytes = yield* loaded.readContent(file.content);
    return yield* decodeJson(new TextDecoder().decode(bytes));
  },
);
/** @param {Uint8Array} bytes */
const flippedLastByte = (bytes) => {
  const changed = Uint8Array.from(bytes);
  const last = changed.at(-1);
  assert.ok(last !== undefined);
  changed[changed.length - 1] = last ^ 1;
  return changed;
};

test("qualification stack metadata survives candidate retention as exact raw identity bytes while Effect stays pinned exactly", () =>
  runTest(
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const fixture = yield* makeFixture();
      const version = "4.0.0-rc.117";
      const node = "@effect/platform-node";
      const shared = "@effect/platform-node-shared";
      const qualificationStack = {
        format: "reactor-effect-qualification-stack/v1",
        selection: "frozen-workspace",
        lockfileVersion: 2,
        lockfileSha256: yield* digest("frozen qualification fixture"),
        requirements: { effect: effectPin, nodePlatform: effectPin, nodeSharedOverride: effectPin },
        selected: { effect: version, nodePlatform: version, nodeShared: version },
        workspaceResolution: ["client", "browser", "native"].flatMap((directory) => {
          const owner = `packages/${directory}`;
          return [
            [owner, "effect"],
            ...(directory === "browser"
              ? []
              : [
                  [owner, node],
                  [`${owner} -> ${node}`, "effect"],
                  [`${owner} -> ${node}`, shared],
                  [`${owner} -> ${node} -> ${shared}`, "effect"],
                ]),
          ].map(([owner, name]) => ({
            owner,
            requested: name,
            name,
            version,
            lockCoordinate: `${name}@${version}`,
          }));
        }),
        consumers: ["portable-node", "browser", "native"].map((name) => ({
          name,
          installer: "bun",
          instances: (name === "native" ? ["effect", node, shared] : ["effect"]).map((name) => ({
            path: `./dependencies/${name}`,
            name,
            version,
          })),
        })),
      };
      const augmented = { ...fixture.identity, qualificationStack };
      assert.equal((yield* validatePackageIdentity(augmented)).effect, effectPin);
      assert.match(
        String(
          yield* Effect.flip(validatePackageIdentity({ ...augmented, effect: `^${version}` })),
        ),
        /SchemaError/,
      );
      // The release schema validates its original fields. Pack owns validation of
      // the additive record; retaining original bytes keeps that evidence intact.
      const text = yield* Schema.encodeEffect(Schema.fromJsonString(Schema.Unknown, { space: 4 }))(
        augmented,
      );
      const original = new TextEncoder().encode(`${text}\n\n`);
      yield* fs.writeFile(fixture.identityPath, original);
      const { input } = yield* prepared(fixture);
      const loaded = yield* loadOffline(input);
      const retained = loaded.bundle.artifacts.find(
        (entry) => entry.logicalName === "package-identity.json",
      );
      assert.ok(retained?._tag === "OwnedFile");
      assert.deepEqual(
        Uint8Array.from(yield* loaded.readContent(retained.content)),
        Uint8Array.from(original),
      );
      assert.deepEqual(yield* retainedJson(loaded, "package-identity.json"), augmented);
    }),
  ));

test("prepare and load retain the exact qualified archives, both native identities and three dependent npm operations", () =>
  runTest(
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const fixture = yield* makeFixture();
      const { identity, input } = yield* prepared(fixture);
      const loaded = yield* loadOffline(input);
      const { bundle, plan, intents } = loaded;
      assert.equal(
        identity.bundleSha256,
        yield* digest(yield* fs.readFile(path.join(fixture.candidateDirectory, "bundle.json"))),
      );
      assert.equal(plan.planId, identity.planId);
      assert.equal(plan.journalId, "reactor-npm:mannyc2/reactor-effect-client:0.2.0");
      assert.equal(plan.operations.length, packageNames.length);
      assert.ok(plan.operations.every((operation) => operation.definitionId === "npm.publish"));
      assert.deepEqual([...intents.keys()].sort(), [...packageNames].sort());
      // Every later publication depends on the client's, and the binding also on
      // the platform packages it pins; the client depends on nothing.
      const client = operationFor({ candidate: loaded, name: clientPackage });
      assert.deepEqual(client.dependsOn, []);
      for (const name of hostPackages)
        assert.deepEqual(
          [...operationFor({ candidate: loaded, name }).dependsOn].sort(),
          (name === nativePackage ? [clientPackage, ...platformPackages] : [clientPackage])
            .map((dependency) => operationFor({ candidate: loaded, name: dependency }).operationId)
            .sort(),
        );
      assert.equal(
        new Set(plan.operations.map((operation) => operation.operationId)).size,
        packageNames.length,
      );
      for (const name of packageNames) {
        const intent = intentFor({ candidate: loaded, name });
        const archive = fixture.packages[name];
        assert.equal(intent.name, name);
        assert.equal(intent.version, "0.2.0");
        assert.equal(intent.registry, "https://registry.npmjs.org/");
        assert.equal(intent.access, "public");
        assert.equal(intent.initialTag, "latest");
        assert.equal(intent.authorization._tag, "TrustedAuthorization");
        assert.equal(intent.authorization.principal, "reactor-npm-publisher");
        assert.equal(intent.provenance._tag, "GitHubActionsProvenance");
        assert.equal(intent.tarball.logicalName, archive.tarball);
        assert.equal(intent.tarball.content.sha256, yield* digest(archive.bytes));
        assert.equal(intent.shasum, yield* digest(archive.bytes, "SHA-1"));
        assert.equal(
          intent.integrity,
          `sha512-${Buffer.from(yield* digest(archive.bytes, "SHA-512"), "hex").toString("base64")}`,
        );
        assert.deepEqual(
          Uint8Array.from(yield* loaded.readContent(intent.tarball.content)),
          Uint8Array.from(archive.bytes),
        );
        assert.deepEqual(yield* fs.readFile(archive.tarballPath), archive.bytes);
      }
      assert.deepEqual(bundle.artifacts.map((file) => file.logicalName).sort(), [
        "package-identity.json",
        "qualification.json",
        "reactor-effect-browser-0.2.0.tgz",
        "reactor-effect-browser-0.2.0.tgz.sigstore.json",
        "reactor-effect-client-0.2.0.tgz",
        "reactor-effect-client-0.2.0.tgz.sigstore.json",
        "reactor-effect-native-0.2.0.tgz",
        "reactor-effect-native-0.2.0.tgz.sigstore.json",
        "reactor-effect-native-darwin-arm64-0.2.0.tgz",
        "reactor-effect-native-darwin-arm64-0.2.0.tgz.sigstore.json",
        "reactor-effect-native-linux-x64-gnu-0.2.0.tgz",
        "reactor-effect-native-linux-x64-gnu-0.2.0.tgz.sigstore.json",
      ]);
      assert.deepEqual(yield* retainedJson(loaded, "package-identity.json"), fixture.identity);
      assert.deepEqual(yield* retainedJson(loaded, "qualification.json"), fixture.qualification);
      assert.deepEqual(identity.qualification, fixture.qualification);
      assert.deepEqual(Object.keys(fixture.identity.native).sort(), [
        "darwin-arm64",
        "linux-x64-gnu",
      ]);
      // Only the platform archives carry an addon and its identity.
      for (const file of ["reactor-effect-native.darwin-arm64.node", "native-identity.json"])
        assert.ok(fixture.packages[darwinPackage].files.includes(file));
      for (const file of ["reactor-effect-native.linux-x64-gnu.node", "native-identity.json"])
        assert.ok(fixture.packages[linuxPackage].files.includes(file));
      for (const name of [clientPackage, browserPackage, nativePackage])
        assert.equal(
          fixture.packages[name].files.some((file) => file.endsWith(".node")),
          false,
        );
      assert.deepEqual((yield* fs.readDirectory(fixture.candidateDirectory)).sort(), [
        "bundle.json",
        "content",
        "identity.json",
        "plan.json",
      ]);
      assert.equal(
        (yield* fs.readDirectory(path.join(fixture.candidateDirectory, "content"))).length,
        packageNames.length * 2 + 2,
      );
    }),
  ));

test("prereleases select next rather than latest for every package without changing their bytes", () =>
  runTest(
    Effect.gen(function* () {
      const fixture = yield* makeFixture({ version: "0.2.0-rc.1" });
      const { identity, input } = yield* prepared(fixture);
      const loaded = yield* loadOffline(input);
      assert.equal(loaded.plan.journalId, "reactor-npm:mannyc2/reactor-effect-client:0.2.0-rc.1");
      assert.equal(identity.qualification.version, "0.2.0-rc.1");
      for (const name of packageNames) {
        const intent = intentFor({ candidate: loaded, name });
        assert.equal(intent.initialTag, "next");
        assert.equal(intent.version, "0.2.0-rc.1");
        assert.equal(intent.tarball.logicalName, `${name}-0.2.0-rc.1.tgz`);
        assert.equal(intent.tarball.content.sha256, yield* digest(fixture.packages[name].bytes));
      }
    }),
  ));

test("a candidate destination cannot be overwritten and unchanged preparation stays deterministic", () =>
  runTest(
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const fixture = yield* makeFixture();
      const { identity, input } = yield* prepared(fixture);
      const planPath = path.join(fixture.candidateDirectory, "plan.json");
      const before = yield* fs.readFile(planPath);
      assert.equal(
        yield* failureCode(prepareOffline(fixture.options)),
        "reactor-release-destination",
      );
      assert.deepEqual(yield* fs.readFile(planPath), before);
      assert.equal((yield* loadOffline(input)).plan.planId, identity.planId);
      const other = yield* prepareOffline({
        ...fixture.options,
        candidateDirectory: path.join(fixture.directory, "second-candidate"),
      });
      assert.equal(other.planId, identity.planId);
      assert.equal(other.bundleSha256, identity.bundleSha256);
      assert.deepEqual(other.qualification, identity.qualification);
    }),
  ));

/** @param {import("./Fixture.mjs").Fixture} f @param {number} index */
const qualified = (f, index) => {
  const entry = f.qualification.packages[index];
  assert.ok(entry);
  return entry;
};
/** @type {Array<[string, (value: import("./Fixture.mjs").Fixture) => void]>} */
const invalidQualifications = [
  [
    "source commit",
    (f) => {
      f.qualification.sourceCommit = "4".repeat(40);
    },
  ],
  [
    "CI run",
    (f) => {
      f.qualification.ciRunId = "7102";
    },
  ],
  [
    "CI attempt",
    (f) => {
      f.qualification.ciRunAttempt = "2";
    },
  ],
  [
    "version",
    (f) => {
      f.qualification.version = "0.2.1";
    },
  ],
  [
    "repository",
    (f) => {
      f.qualification.repository = "other/reactor-effect-client";
    },
  ],
  [
    "format",
    (f) => {
      f.qualification.format = "reactor-qualified-ci/v1";
    },
  ],
  [
    "host archive digest",
    (f) => {
      qualified(f, 1).sha256 = "0".repeat(64);
    },
  ],
  [
    "client archive digest",
    (f) => {
      qualified(f, 0).sha256 = "0".repeat(64);
    },
  ],
  [
    "archive filename",
    (f) => {
      qualified(f, 2).tarball = "reactor-effect-native-0.2.1.tgz";
    },
  ],
  [
    "package name",
    (f) => {
      qualified(f, 0).name = browserPackage;
    },
  ],
  [
    "packages order",
    (f) => {
      f.qualification.packages.reverse();
    },
  ],
  [
    "packages list missing the native package",
    (f) => {
      f.qualification.packages.pop();
    },
  ],
  [
    "packages list with a repeated entry",
    (f) => {
      f.qualification.packages.push({ ...qualified(f, 0) });
    },
  ],
  [
    "packages list without any package",
    (f) => {
      f.qualification.packages.length = 0;
    },
  ],
];
for (const [name, change] of invalidQualifications)
  test(`preparation rejects a mismatched qualification ${name} before creating a candidate`, () =>
    runTest(
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const fixture = yield* makeFixture();
        change(fixture);
        yield* writeJson(fixture.qualificationPath, fixture.qualification);
        assert.equal(
          yield* failureCode(prepareOffline(fixture.options)),
          "reactor-release-qualification",
        );
        assert.equal(yield* fs.exists(fixture.candidateDirectory), false);
      }),
    ));

for (const name of packageNames)
  test(`preparation detects changed incoming ${name} bytes even at the same filename and length`, () =>
    runTest(
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const fixture = yield* makeFixture();
        const archive = fixture.packages[name];
        yield* fs.writeFile(archive.tarballPath, flippedLastByte(archive.bytes));
        assert.equal(
          yield* failureCode(prepareOffline(fixture.options)),
          "reactor-release-qualification",
        );
        assert.equal(yield* fs.exists(fixture.candidateDirectory), false);
      }),
    ));

/** @typedef {import("./Fixture.mjs").Fixture["identity"]} FixtureIdentity */
// Each rejection must happen for its own reason: the model's message, or a schema failure.
const schema = /SchemaError/;
const nativeIdentity = /Native platform qualification identity differs/;
const inventory = /Incomplete package file inventory/;
/** Any well-formed digest that no packaged file has. */
const unknownDigest = "a".repeat(64);
/** @type {Array<[string, (identity: FixtureIdentity) => void, RegExp]>} */
const invalidIdentities = [
  [
    "a missing native platform",
    (identity) => {
      Reflect.deleteProperty(identity.native, "linux-x64-gnu");
    },
    schema,
  ],
  [
    "a native platform built from other source",
    (identity) => {
      const platform = identity.native["linux-x64-gnu"];
      assert.ok(platform);
      platform.build.sourceSha256 = "0".repeat(64);
    },
    nativeIdentity,
  ],
  [
    "native platforms linking different WebRTC prebuilts",
    (identity) => {
      const platform = identity.native["linux-x64-gnu"];
      assert.ok(platform);
      platform.build.webrtcPrebuilt = "webrtc-7907-a5ddff60-p8";
    },
    nativeIdentity,
  ],
  [
    "a native platform naming no WebRTC prebuilt",
    (identity) => {
      const platform = identity.native["darwin-arm64"];
      assert.ok(platform);
      Reflect.deleteProperty(platform.build, "webrtcPrebuilt");
    },
    schema,
  ],
  [
    "a mislabelled native platform",
    (identity) => {
      const platform = identity.native["linux-x64-gnu"];
      assert.ok(platform);
      platform.platform = "linux-arm64";
    },
    nativeIdentity,
  ],
  [
    "another native library name",
    (identity) => {
      const platform = identity.native["darwin-arm64"];
      assert.ok(platform);
      platform.file = "reactor-effect-native.linux-x64-gnu.node";
    },
    nativeIdentity,
  ],
  [
    "native library bytes differing from the packaged file",
    (identity) => {
      identity.packages[darwinPackage].fileSha256["reactor-effect-native.darwin-arm64.node"] =
        "0".repeat(64);
    },
    nativeIdentity,
  ],
  [
    "a native identity differing from the packaged library",
    (identity) => {
      const platform = identity.native["darwin-arm64"];
      assert.ok(platform);
      platform.sha256 = "0".repeat(64);
    },
    nativeIdentity,
  ],
  [
    "a missing native identity sidecar",
    (identity) => {
      const sidecar = "native-identity.json";
      const native = identity.packages[linuxPackage];
      native.files = native.files.filter((file) => file !== sidecar);
      delete native.fileSha256[sidecar];
    },
    nativeIdentity,
  ],
  [
    "mixed package versions",
    (identity) => {
      identity.packages[browserPackage].version = "0.2.1";
    },
    /Workspace packages must share one release version/,
  ],
  [
    "a native library inside the browser package",
    (identity) => {
      const file = "reactor-effect-native.linux-x64-gnu.node";
      identity.packages[browserPackage].files.push(file);
      identity.packages[browserPackage].fileSha256[file] = unknownDigest;
    },
    /A portable package carries native libraries/,
  ],
  [
    "a client export change",
    (identity) => {
      identity.packages[clientPackage].exports.push("./unexpected");
    },
    /Public exports changed/,
  ],
  [
    "a host export change",
    (identity) => {
      identity.packages[nativePackage].exports.push("./browser");
    },
    /Public exports changed/,
  ],
  [
    "a duplicated inventory entry",
    (identity) => {
      identity.packages[clientPackage].files.push("package.json");
    },
    inventory,
  ],
  [
    "an inventory entry without a digest",
    (identity) => {
      identity.packages[clientPackage].files.push("dist/extra.js");
    },
    inventory,
  ],
  [
    "a digest without an inventory entry",
    (identity) => {
      identity.packages[browserPackage].fileSha256["dist/extra.js"] = unknownDigest;
    },
    inventory,
  ],
  [
    "an unexpected archive filename",
    (identity) => {
      identity.packages[nativePackage].tarball = "reactor-effect-native-0.2.1.tgz";
    },
    /Unexpected archive filename/,
  ],
  [
    "a missing workspace package",
    (identity) => {
      Reflect.deleteProperty(identity.packages, browserPackage);
    },
    schema,
  ],
  [
    "the portable pack profile",
    (identity) => {
      identity.profile = "portable";
    },
    schema,
  ],
  [
    "another Effect pin",
    (identity) => {
      identity.effect = "4.0.0-rc.116";
    },
    schema,
  ],
];
test("qualification admits the fixture identity and rejects incomplete inventories, export changes, mixed versions and inconsistent native identities", () =>
  runTest(
    Effect.gen(function* () {
      const fixture = yield* makeFixture();
      const admitted = yield* validatePackageIdentity(fixture.identity);
      for (const name of packageNames) {
        assert.deepEqual(admitted.packages[name].exports, fixture.identity.packages[name].exports);
        assert.equal(admitted.packages[name].sha256, yield* digest(fixture.packages[name].bytes));
      }
      for (const [name, change, reason] of invalidIdentities) {
        const identity = structuredClone(fixture.identity);
        change(identity);
        assert.match(String(yield* Effect.flip(validatePackageIdentity(identity))), reason, name);
      }
    }),
  ));

/** Only what the real npm provider or metadata inspection actually rejects is asserted here;
 * a wrong peer pin, for example, is not an npm publication policy.
 * @type {Array<[string, Record<string, unknown>, import("./Fixture.mjs").PackageName]>} */
const invalidManifests = [
  ["a different npm name", { name: "different-package" }, clientPackage],
  ["a different npm name for the native package", { name: "different-package" }, nativePackage],
  ["a different version", { version: "0.2.1" }, clientPackage],
  ["a different version for the browser package", { version: "0.2.1" }, browserPackage],
  ["private publication", { private: true }, clientPackage],
  ["private publication of the native package", { private: true }, nativePackage],
  [
    "provenance disabled by the archive",
    { publishConfig: { access: "public", provenance: false } },
    clientPackage,
  ],
  [
    "provenance disabled by the browser archive",
    { publishConfig: { access: "public", provenance: false } },
    browserPackage,
  ],
  [
    "another registry",
    { publishConfig: { registry: "https://registry.example.invalid/", provenance: false } },
    clientPackage,
  ],
  [
    "another registry for the native package",
    { publishConfig: { registry: "https://registry.example.invalid/", provenance: false } },
    nativePackage,
  ],
];
for (const [name, manifest, manifestPackage] of invalidManifests)
  test(`the real npm provider rejects ${name} before a publishable candidate is retained`, () =>
    runTest(
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        const fixture = yield* makeFixture({ manifest, manifestPackage });
        assert.match(
          yield* failureCode(prepareOffline(fixture.options)),
          /^(npm|reactor-release-npm)/,
        );
        for (const file of ["identity.json", "plan.json"])
          assert.equal(yield* fs.exists(path.join(fixture.candidateDirectory, file)), false);
        for (const archive of Object.values(fixture.packages))
          assert.deepEqual(yield* fs.readFile(archive.tarballPath), archive.bytes);
      }),
    ));

for (const file of ["bundle.json", "plan.json", "plan.json dependency edge", "identity.json"])
  test(`loading detects ${file} tampering without changing the original input`, () =>
    runTest(
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        const fixture = yield* makeFixture();
        const { identity, input } = yield* prepared(fixture);
        const target = path.join(fixture.candidateDirectory, file.split(" ")[0] ?? file);
        if (file === "bundle.json")
          yield* fs.writeFile(target, new Uint8Array([...(yield* fs.readFile(target)), 10]));
        else if (file === "identity.json")
          yield* writeJson(target, { ...identity, applicationCommit: "4".repeat(40) });
        else {
          const plan = yield* readJson(target).pipe(
            Effect.flatMap(Schema.decodeUnknownEffect(Plan)),
          );
          if (file === "plan.json") {
            const [first, ...rest] = plan.operations;
            assert.ok(first);
            const intent = yield* Schema.decodeUnknownEffect(
              Schema.Record(Schema.String, Schema.Unknown),
            )(first.intent);
            yield* writeJson(target, {
              ...plan,
              operations: [{ ...first, intent: { ...intent, initialTag: "unexpected" } }, ...rest],
            });
          } else {
            const host = plan.operations.find((operation) => operation.dependsOn.length === 1);
            assert.ok(host);
            yield* writeJson(target, {
              ...plan,
              operations: plan.operations.map((operation) =>
                operation === host ? { ...operation, dependsOn: [] } : operation,
              ),
            });
          }
        }
        yield* Effect.flip(loadOffline(input));
        for (const archive of Object.values(fixture.packages))
          assert.deepEqual(yield* fs.readFile(archive.tarballPath), archive.bytes);
      }),
    ));

test("loading refuses same-size owned byte corruption of any archive and symlink substitutions", () =>
  runTest(
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const fixture = yield* makeFixture();
      const { input } = yield* prepared(fixture);
      const loaded = yield* loadOffline(input);
      for (const name of packageNames) {
        const content = intentFor({ candidate: loaded, name }).tarball.content;
        const owned = path.join(fixture.candidateDirectory, "content", content.sha256);
        const original = yield* fs.readFile(owned);
        assert.deepEqual(original, fixture.packages[name].bytes);
        yield* fs.chmod(owned, 0o600);
        yield* fs.writeFile(owned, flippedLastByte(original));
        yield* Effect.flip(loadOffline(input));
        yield* fs.writeFile(owned, original);
        yield* fs.chmod(owned, 0o400);
      }
      assert.equal((yield* loadOffline(input)).plan.planId, loaded.plan.planId);
      const alias = path.join(fixture.directory, "bundle-alias.json");
      yield* fs.symlink(path.join(fixture.candidateDirectory, "bundle.json"), alias);
      assert.equal((yield* Effect.flip(readBytes(alias)))._tag, "Rejection");
    }),
  ));

test("a validly rehashed Plan cannot change the npm principal, the journal, the package set or the dependency edges", () =>
  runTest(
    Effect.gen(function* () {
      const path = yield* Path.Path;
      const fixture = yield* makeFixture();
      const { identity, input } = yield* prepared(fixture);
      const loaded = yield* loadOffline(input);
      const journal = loaded.plan.journalId;
      const client = operationFor({ candidate: loaded, name: clientPackage });
      const browser = operationFor({ candidate: loaded, name: browserPackage });
      const native = operationFor({ candidate: loaded, name: nativePackage });
      const platforms = platformPackages.map((name) => operationFor({ candidate: loaded, name }));
      const platformIds = platforms.map((operation) => operation.operationId);
      const otherPrincipal = Npm.TokenAuthorization.make({ principal: "other-publisher" });
      /** @param {import("./Fixture.mjs").PackageName} name @param {Partial<Npm.PublishIntent>} changes @param {readonly string[]} dependsOn */
      const republish = (name, changes, dependsOn) =>
        Npm.publish(
          Npm.PublishIntent.make({ ...intentFor({ candidate: loaded, name }), ...changes }),
          dependsOn,
        );
      // Control: rehashing the unchanged operations reproduces the retained Plan exactly.
      const same = yield* createPlan(input.bundleSha256, loaded.plan.operations, journal);
      assert.equal(same.planId, loaded.plan.planId);
      assert.equal(
        (yield* republish(browserPackage, {}, [client.operationId])).operationId,
        browser.operationId,
      );
      const otherClient = yield* republish(clientPackage, { authorization: otherPrincipal }, []);
      const otherPlatforms = yield* Effect.forEach(platformPackages, (name) =>
        republish(name, {}, [otherClient.operationId]),
      );
      /** @type {Array<[string, readonly import("@mannyc1/ts-release").Operation[], string]>} */
      const variants = [
        [
          "another npm principal for the client",
          [
            otherClient,
            yield* republish(browserPackage, {}, [otherClient.operationId]),
            ...otherPlatforms,
            yield* republish(nativePackage, {}, [
              otherClient.operationId,
              ...otherPlatforms.map((operation) => operation.operationId),
            ]),
          ],
          journal,
        ],
        [
          "another npm principal for a host",
          [
            client,
            browser,
            ...platforms,
            yield* republish(nativePackage, { authorization: otherPrincipal }, [
              client.operationId,
              ...platformIds,
            ]),
          ],
          journal,
        ],
        ["another journal", loaded.plan.operations, `${journal}:replacement`],
        ["a dropped host publication", [client, browser, ...platforms], journal],
        ["only the client publication", [client], journal],
        [
          "a host publication without its client dependency",
          [client, yield* republish(browserPackage, {}, []), ...platforms, native],
          journal,
        ],
        [
          "a host depending on the other host instead of the client",
          [
            client,
            browser,
            ...platforms,
            yield* republish(nativePackage, {}, [browser.operationId, ...platformIds]),
          ],
          journal,
        ],
        [
          "a host depending on the client and the other host",
          [
            client,
            browser,
            ...platforms,
            yield* republish(nativePackage, {}, [
              client.operationId,
              browser.operationId,
              ...platformIds,
            ]),
          ],
          journal,
        ],
        [
          "a host intent carrying the other host's archive and digests",
          [
            client,
            yield* republish(
              browserPackage,
              {
                tarball: intentFor({ candidate: loaded, name: nativePackage }).tarball,
                integrity: intentFor({ candidate: loaded, name: nativePackage }).integrity,
                shasum: intentFor({ candidate: loaded, name: nativePackage }).shasum,
              },
              [client.operationId],
            ),
            ...platforms,
            native,
          ],
          journal,
        ],
        [
          "a host intent carrying the other host's signed provenance",
          [
            client,
            yield* republish(
              browserPackage,
              { provenance: intentFor({ candidate: loaded, name: nativePackage }).provenance },
              [client.operationId],
            ),
            ...platforms,
            native,
          ],
          journal,
        ],
      ];
      for (const [name, operations, journalId] of variants) {
        const plan = yield* createPlan(input.bundleSha256, operations, journalId);
        assert.notEqual(plan.planId, loaded.plan.planId, name);
        yield* writeJson(path.join(fixture.candidateDirectory, "plan.json"), plan);
        yield* writeJson(path.join(fixture.candidateDirectory, "identity.json"), {
          ...identity,
          planId: plan.planId,
        });
        const result = yield* Effect.result(loadOffline({ ...input, planId: plan.planId }));
        assert.ok(Result.isFailure(result), name);
        assert.equal(result.failure.code, "reactor-release-publication-policy", name);
      }
    }),
  ));

test("preparation refuses a qualified directory missing one host archive and creates no candidate", () =>
  runTest(
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const fixture = yield* makeFixture();
      yield* fs.remove(path.join(fixture.qualifiedDirectory, `${nativePackage}-0.2.0.tgz`));
      assert.equal(
        yield* failureCode(prepareOffline(fixture.options)),
        "reactor-release-qualification",
      );
      assert.equal(yield* fs.exists(fixture.candidateDirectory), false);
    }),
  ));
