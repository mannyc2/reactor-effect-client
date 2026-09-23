import assert from "node:assert/strict";
import {
  chmodSync,
  existsSync,
  readFileSync,
  readdirSync,
  rmSync,
  symlinkSync,
  writeFileSync,
} from "node:fs";
import { join } from "node:path";
import { test } from "bun:test";
import { Effect, Result } from "effect";
import { createPlan } from "@mannyc1/ts-release";
import * as Npm from "@mannyc1/ts-release-npm";
import { validatePackageIdentity } from "../model.mjs";
import {
  browserPackage,
  clientPackage,
  digest,
  hostPackages,
  intentFor,
  loadOffline,
  nativePackage,
  operationFor,
  packageNames,
  prepareOffline,
  prepared,
  withFixture,
  writeJson,
} from "./Fixture.mjs";

/** @param {import("./Fixture.mjs").Candidate} loaded @param {string} logicalName */
const retainedJson = async (loaded, logicalName) => {
  const file = loaded.bundle.artifacts.find((entry) => entry.logicalName === logicalName);
  assert.ok(file?._tag === "OwnedFile", `${logicalName} is not a retained owned file`);
  return JSON.parse(
    Buffer.from(await Effect.runPromise(loaded.readContent(file.content))).toString(),
  );
};
/** @param {Buffer} bytes */
const flippedLastByte = (bytes) => {
  const changed = Buffer.from(bytes);
  const last = changed.at(-1);
  assert.ok(last !== undefined);
  changed[changed.length - 1] = last ^ 1;
  return changed;
};

test("prepare and load retain the exact qualified archives, both native identities and three dependent npm operations", () =>
  withFixture(async (fixture) => {
    const { identity, input } = await prepared(fixture);
    const loaded = await Effect.runPromise(loadOffline(input));
    const { bundle, plan, intents } = loaded;
    assert.equal(
      identity.bundleSha256,
      digest(readFileSync(join(fixture.candidateDirectory, "bundle.json"))),
    );
    assert.equal(plan.planId, identity.planId);
    assert.equal(plan.journalId, "reactor-npm:mannyc2/reactor-effect-client:0.2.0");
    assert.equal(plan.operations.length, 3);
    assert.ok(plan.operations.every((operation) => operation.definitionId === "npm.publish"));
    assert.deepEqual([...intents.keys()].sort(), [...packageNames].sort());
    // Both host publications depend on the client's; the client depends on nothing.
    const client = operationFor(loaded, clientPackage);
    assert.deepEqual(client.dependsOn, []);
    for (const name of hostPackages)
      assert.deepEqual(operationFor(loaded, name).dependsOn, [client.operationId]);
    assert.equal(new Set(plan.operations.map((operation) => operation.operationId)).size, 3);
    for (const name of packageNames) {
      const intent = intentFor(loaded, name);
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
      assert.equal(intent.tarball.content.sha256, digest(archive.bytes));
      assert.equal(intent.shasum, digest(archive.bytes, "sha1"));
      assert.equal(
        intent.integrity,
        `sha512-${Buffer.from(digest(archive.bytes, "sha512"), "hex").toString("base64")}`,
      );
      assert.deepEqual(
        Buffer.from(await Effect.runPromise(loaded.readContent(intent.tarball.content))),
        archive.bytes,
      );
      assert.deepEqual(readFileSync(archive.tarballPath), archive.bytes);
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
    ]);
    assert.deepEqual(await retainedJson(loaded, "package-identity.json"), fixture.identity);
    assert.deepEqual(await retainedJson(loaded, "qualification.json"), fixture.qualification);
    assert.deepEqual(identity.qualification, fixture.qualification);
    assert.deepEqual(Object.keys(fixture.identity.native).sort(), ["darwin-arm64", "linux-x64"]);
    // Only the native archive carries libraries and their identity sidecars.
    for (const [platform, library] of Object.entries({
      "darwin-arm64": "libreactor_effect_native.dylib",
      "linux-x64": "libreactor_effect_native.so",
    }))
      for (const path of [`lib/${platform}/${library}`, `lib/${platform}/native-identity.json`])
        assert.ok(fixture.packages[nativePackage].files.includes(path));
    for (const name of [clientPackage, browserPackage])
      assert.equal(
        fixture.packages[name].files.some((path) => path.startsWith("lib/")),
        false,
      );
    assert.deepEqual(readdirSync(fixture.candidateDirectory).sort(), [
      "bundle.json",
      "content",
      "identity.json",
      "plan.json",
    ]);
    assert.equal(readdirSync(join(fixture.candidateDirectory, "content")).length, 8);
  }));

test("prereleases select next rather than latest for every package without changing their bytes", () =>
  withFixture(
    async (fixture) => {
      const { identity, input } = await prepared(fixture);
      const loaded = await Effect.runPromise(loadOffline(input));
      assert.equal(loaded.plan.journalId, "reactor-npm:mannyc2/reactor-effect-client:0.2.0-rc.1");
      assert.equal(identity.qualification.version, "0.2.0-rc.1");
      for (const name of packageNames) {
        const intent = intentFor(loaded, name);
        assert.equal(intent.initialTag, "next");
        assert.equal(intent.version, "0.2.0-rc.1");
        assert.equal(intent.tarball.logicalName, `${name}-0.2.0-rc.1.tgz`);
        assert.equal(intent.tarball.content.sha256, digest(fixture.packages[name].bytes));
      }
    },
    { version: "0.2.0-rc.1" },
  ));

test("a candidate destination cannot be overwritten and unchanged preparation stays deterministic", () =>
  withFixture(async (fixture) => {
    const { identity, input } = await prepared(fixture);
    const before = readFileSync(join(fixture.candidateDirectory, "plan.json"));
    await assert.rejects(Effect.runPromise(prepareOffline(fixture.options)), /destination/);
    assert.deepEqual(readFileSync(join(fixture.candidateDirectory, "plan.json")), before);
    assert.equal((await Effect.runPromise(loadOffline(input))).plan.planId, identity.planId);
    const other = await Effect.runPromise(
      prepareOffline({
        ...fixture.options,
        candidateDirectory: join(fixture.directory, "second-candidate"),
      }),
    );
    assert.equal(other.planId, identity.planId);
    assert.equal(other.bundleSha256, identity.bundleSha256);
    assert.deepEqual(other.qualification, identity.qualification);
  }));

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
    withFixture(async (fixture) => {
      change(fixture);
      writeJson(fixture.qualificationPath, fixture.qualification);
      await assert.rejects(Effect.runPromise(prepareOffline(fixture.options)), /qualification/);
      assert.equal(existsSync(fixture.candidateDirectory), false);
    }));

for (const name of packageNames)
  test(`preparation detects changed incoming ${name} bytes even at the same filename and length`, () =>
    withFixture(async (fixture) => {
      const archive = fixture.packages[name];
      writeFileSync(archive.tarballPath, flippedLastByte(archive.bytes));
      await assert.rejects(Effect.runPromise(prepareOffline(fixture.options)), /qualification/);
      assert.equal(existsSync(fixture.candidateDirectory), false);
    }));

/** @typedef {import("./Fixture.mjs").Fixture["identity"]} FixtureIdentity */
// Each rejection must happen for its own reason: the model's message, or a schema failure.
const schema = /SchemaError/;
const nativeIdentity = /Native platform qualification identity differs/;
const inventory = /Incomplete package file inventory/;
/** @type {Array<[string, (identity: FixtureIdentity) => void, RegExp]>} */
const invalidIdentities = [
  [
    "a missing native platform",
    (identity) => {
      Reflect.deleteProperty(identity.native, "linux-x64");
    },
    schema,
  ],
  [
    "a native platform built from other source",
    (identity) => {
      const platform = identity.native["linux-x64"];
      assert.ok(platform);
      platform.build.sourceSha256 = "0".repeat(64);
    },
    nativeIdentity,
  ],
  [
    "native platforms linking different WebRTC prebuilts",
    (identity) => {
      const platform = identity.native["linux-x64"];
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
      const platform = identity.native["linux-x64"];
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
      platform.library = "libreactor_effect_native.so";
    },
    nativeIdentity,
  ],
  [
    "native library bytes differing from the packaged file",
    (identity) => {
      identity.packages[nativePackage].fileSha256[
        "lib/darwin-arm64/libreactor_effect_native.dylib"
      ] = "0".repeat(64);
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
      const sidecar = "lib/linux-x64/native-identity.json";
      const native = identity.packages[nativePackage];
      native.files = native.files.filter((path) => path !== sidecar);
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
      const path = "lib/linux-x64/libreactor_effect_native.so";
      identity.packages[browserPackage].files.push(path);
      identity.packages[browserPackage].fileSha256[path] = digest("smuggled library");
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
      identity.packages[browserPackage].fileSha256["dist/extra.js"] = digest("extra");
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
  withFixture((fixture) => {
    const admitted = validatePackageIdentity(fixture.identity);
    for (const name of packageNames) {
      assert.deepEqual(admitted.packages[name].exports, fixture.identity.packages[name].exports);
      assert.equal(admitted.packages[name].sha256, digest(fixture.packages[name].bytes));
    }
    for (const [name, change, reason] of invalidIdentities) {
      const identity = structuredClone(fixture.identity);
      change(identity);
      assert.throws(() => validatePackageIdentity(identity), reason, name);
    }
  }));

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
    withFixture(
      async (fixture) => {
        const result = await Effect.runPromise(Effect.result(prepareOffline(fixture.options)));
        assert.ok(Result.isFailure(result));
        assert.equal(result.failure._tag, "ReleaseError");
        assert.match(result.failure.code, /^(npm|reactor-release-npm)/);
        assert.equal(existsSync(join(fixture.candidateDirectory, "identity.json")), false);
        assert.equal(existsSync(join(fixture.candidateDirectory, "plan.json")), false);
        for (const archive of Object.values(fixture.packages))
          assert.deepEqual(readFileSync(archive.tarballPath), archive.bytes);
      },
      { manifest, manifestPackage },
    ));

for (const file of ["bundle.json", "plan.json", "plan.json dependency edge", "identity.json"])
  test(`loading detects ${file} tampering without changing the original input`, () =>
    withFixture(async (fixture) => {
      const { input } = await prepared(fixture);
      const path = join(fixture.candidateDirectory, file.split(" ")[0] ?? file);
      if (file === "bundle.json")
        writeFileSync(path, Buffer.concat([readFileSync(path), Buffer.from("\n")]));
      else {
        const value = JSON.parse(readFileSync(path, "utf8"));
        if (file === "plan.json") value.operations[0].intent.initialTag = "unexpected";
        else if (file === "plan.json dependency edge") {
          const host = value.operations.find(
            (/** @type {{ dependsOn: string[] }} */ operation) => operation.dependsOn.length === 1,
          );
          assert.ok(host);
          host.dependsOn = [];
        } else value.applicationCommit = "4".repeat(40);
        writeJson(path, value);
      }
      await assert.rejects(Effect.runPromise(loadOffline(input)));
      for (const archive of Object.values(fixture.packages))
        assert.deepEqual(readFileSync(archive.tarballPath), archive.bytes);
    }));

test("loading refuses same-size owned byte corruption of any archive and symlink substitutions", () =>
  withFixture(async (fixture) => {
    const { input } = await prepared(fixture);
    const loaded = await Effect.runPromise(loadOffline(input));
    for (const name of packageNames) {
      const content = intentFor(loaded, name).tarball.content;
      const path = join(fixture.candidateDirectory, "content", content.sha256);
      const original = readFileSync(path);
      assert.deepEqual(original, fixture.packages[name].bytes);
      chmodSync(path, 0o600);
      writeFileSync(path, flippedLastByte(original));
      await assert.rejects(Effect.runPromise(loadOffline(input)));
      writeFileSync(path, original);
      chmodSync(path, 0o400);
    }
    assert.equal((await Effect.runPromise(loadOffline(input))).plan.planId, loaded.plan.planId);
    const alias = join(fixture.directory, "bundle-alias.json");
    symlinkSync(join(fixture.candidateDirectory, "bundle.json"), alias);
    const { readBytes } = await import("../model.mjs");
    assert.throws(() => readBytes(alias));
  }));

test("a validly rehashed Plan cannot change the npm principal, the journal, the package set or the dependency edges", () =>
  withFixture(async (fixture) => {
    const { identity, input } = await prepared(fixture);
    const loaded = await Effect.runPromise(loadOffline(input));
    const journal = loaded.plan.journalId;
    const client = operationFor(loaded, clientPackage);
    const browser = operationFor(loaded, browserPackage);
    const native = operationFor(loaded, nativePackage);
    const otherPrincipal = new Npm.TokenAuthorization({ principal: "other-publisher" });
    /** @param {import("./Fixture.mjs").PackageName} name @param {Partial<Npm.PublishIntent>} changes @param {readonly string[]} dependsOn */
    const republish = (name, changes, dependsOn) =>
      Effect.runPromise(
        Npm.publish(new Npm.PublishIntent({ ...intentFor(loaded, name), ...changes }), dependsOn),
      );
    // Control: rehashing the unchanged operations reproduces the retained Plan exactly.
    const same = await Effect.runPromise(
      createPlan(input.bundleSha256, loaded.plan.operations, journal),
    );
    assert.equal(same.planId, loaded.plan.planId);
    assert.equal(
      (await republish(browserPackage, {}, [client.operationId])).operationId,
      browser.operationId,
    );
    const otherClient = await republish(clientPackage, { authorization: otherPrincipal }, []);
    /** @type {Array<[string, readonly import("@mannyc1/ts-release").Operation[], string]>} */
    const variants = [
      [
        "another npm principal for the client",
        [
          otherClient,
          await republish(browserPackage, {}, [otherClient.operationId]),
          await republish(nativePackage, {}, [otherClient.operationId]),
        ],
        journal,
      ],
      [
        "another npm principal for a host",
        [
          client,
          browser,
          await republish(nativePackage, { authorization: otherPrincipal }, [client.operationId]),
        ],
        journal,
      ],
      ["another journal", loaded.plan.operations, `${journal}:replacement`],
      ["a dropped host publication", [client, browser], journal],
      ["only the client publication", [client], journal],
      [
        "a host publication without its client dependency",
        [client, await republish(browserPackage, {}, []), native],
        journal,
      ],
      [
        "a host depending on the other host instead of the client",
        [client, browser, await republish(nativePackage, {}, [browser.operationId])],
        journal,
      ],
      [
        "a host depending on the client and the other host",
        [
          client,
          browser,
          await republish(nativePackage, {}, [client.operationId, browser.operationId]),
        ],
        journal,
      ],
      [
        "a host intent carrying the other host's archive and digests",
        [
          client,
          await republish(
            browserPackage,
            {
              tarball: intentFor(loaded, nativePackage).tarball,
              integrity: intentFor(loaded, nativePackage).integrity,
              shasum: intentFor(loaded, nativePackage).shasum,
            },
            [client.operationId],
          ),
          native,
        ],
        journal,
      ],
      [
        "a host intent carrying the other host's signed provenance",
        [
          client,
          await republish(
            browserPackage,
            { provenance: intentFor(loaded, nativePackage).provenance },
            [client.operationId],
          ),
          native,
        ],
        journal,
      ],
    ];
    for (const [name, operations, journalId] of variants) {
      const plan = await Effect.runPromise(createPlan(input.bundleSha256, operations, journalId));
      assert.notEqual(plan.planId, loaded.plan.planId, name);
      writeJson(join(fixture.candidateDirectory, "plan.json"), plan);
      writeJson(join(fixture.candidateDirectory, "identity.json"), {
        ...identity,
        planId: plan.planId,
      });
      const result = await Effect.runPromise(
        Effect.result(loadOffline({ ...input, planId: plan.planId })),
      );
      assert.ok(Result.isFailure(result), name);
      assert.equal(result.failure.code, "reactor-release-publication-policy", name);
    }
  }));

test("preparation refuses a qualified directory missing one host archive and creates no candidate", () =>
  withFixture(async (fixture) => {
    rmSync(join(fixture.qualifiedDirectory, `${nativePackage}-0.2.0.tgz`));
    await assert.rejects(Effect.runPromise(prepareOffline(fixture.options)), /qualification/);
    assert.equal(existsSync(fixture.candidateDirectory), false);
  }));
