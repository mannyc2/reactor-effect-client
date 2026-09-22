import assert from "node:assert/strict";
import {
  chmodSync,
  existsSync,
  readFileSync,
  readdirSync,
  symlinkSync,
  writeFileSync,
} from "node:fs";
import { join } from "node:path";
import { test } from "bun:test";
import { Effect, Result } from "effect";
import { createPlan } from "@mannyc1/ts-release";
import * as Npm from "@mannyc1/ts-release-npm";
import { loadCandidate } from "../application.mjs";
import { prepareCandidate } from "../prepare.mjs";
import { validatePackageIdentity } from "../model.mjs";
import { digest, prepared, withFixture, writeJson } from "./Fixture.mjs";

test("prepare and load retain the exact qualified archive, both native identities and one npm operation", () =>
  withFixture(async (fixture) => {
    const { identity, input } = await prepared(fixture);
    const loaded = await Effect.runPromise(loadCandidate(input));
    const { intent, bundle, plan } = loaded;
    assert.equal(
      identity.bundleSha256,
      digest(readFileSync(join(fixture.candidateDirectory, "bundle.json"))),
    );
    assert.equal(plan.planId, identity.planId);
    assert.equal(plan.journalId, "reactor-npm:mannyc2/reactor-effect-client:0.2.0");
    assert.equal(plan.operations.length, 1);
    assert.equal(plan.operations[0]?.definitionId, "npm.publish");
    assert.equal(intent.name, "reactor-effect-client");
    assert.equal(intent.version, "0.2.0");
    assert.equal(intent.initialTag, "latest");
    assert.equal(intent.authorization._tag, "TokenAuthorization");
    assert.equal(intent.provenance._tag, "NoProvenance");
    assert.equal(intent.shasum, digest(fixture.bytes, "sha1"));
    assert.equal(
      intent.integrity,
      `sha512-${Buffer.from(digest(fixture.bytes, "sha512"), "hex").toString("base64")}`,
    );
    assert.deepEqual(bundle.artifacts.map((file) => file.logicalName).sort(), [
      "package-identity.json",
      "qualification.json",
      "reactor-effect-client-0.2.0.tgz",
    ]);
    assert.deepEqual(
      Buffer.from(await Effect.runPromise(loaded.readContent(intent.tarball.content))),
      fixture.bytes,
    );
    const retainedIdentity = bundle.artifacts.find(
      (file) => file.logicalName === "package-identity.json",
    );
    assert.ok(retainedIdentity?._tag === "OwnedFile");
    assert.deepEqual(
      JSON.parse(
        Buffer.from(
          await Effect.runPromise(loaded.readContent(retainedIdentity.content)),
        ).toString(),
      ),
      fixture.identity,
    );
    assert.deepEqual(Object.keys(fixture.identity.native).sort(), ["darwin-arm64", "linux-x64"]);
    assert.deepEqual(readdirSync(fixture.candidateDirectory).sort(), [
      "bundle.json",
      "content",
      "identity.json",
      "plan.json",
    ]);
    assert.deepEqual(readFileSync(fixture.tarballPath), fixture.bytes);
  }));

test("prereleases select next rather than latest without changing their bytes", () =>
  withFixture(
    async (fixture) => {
      const { input } = await prepared(fixture);
      const loaded = await Effect.runPromise(loadCandidate(input));
      assert.equal(loaded.intent.initialTag, "next");
      assert.equal(loaded.intent.version, "0.2.0-rc.1");
      assert.equal(loaded.intent.tarball.content.sha256, digest(fixture.bytes));
    },
    { version: "0.2.0-rc.1" },
  ));

test("a candidate destination cannot be overwritten and unchanged preparation stays deterministic", () =>
  withFixture(async (fixture) => {
    const { identity, input } = await prepared(fixture);
    const before = readFileSync(join(fixture.candidateDirectory, "plan.json"));
    await assert.rejects(Effect.runPromise(prepareCandidate(fixture.options)), /destination/);
    assert.deepEqual(readFileSync(join(fixture.candidateDirectory, "plan.json")), before);
    assert.equal((await Effect.runPromise(loadCandidate(input))).plan.planId, identity.planId);
    const other = await Effect.runPromise(
      prepareCandidate({
        ...fixture.options,
        candidateDirectory: join(fixture.directory, "second-candidate"),
      }),
    );
    assert.equal(other.planId, identity.planId);
    assert.equal(other.bundleSha256, identity.bundleSha256);
  }));

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
    "archive digest",
    (f) => {
      f.qualification.sha256 = "0".repeat(64);
    },
  ],
  [
    "repository",
    (f) => {
      f.qualification.repository = "other/reactor-effect-client";
    },
  ],
];
for (const [name, change] of invalidQualifications)
  test(`preparation rejects a mismatched qualification ${name} before creating a candidate`, () =>
    withFixture(async (fixture) => {
      change(fixture);
      writeJson(fixture.qualificationPath, fixture.qualification);
      await assert.rejects(Effect.runPromise(prepareCandidate(fixture.options)), /qualification/);
      assert.equal(existsSync(fixture.candidateDirectory), false);
    }));

test("preparation detects changed incoming bytes even at the same filename and length", () =>
  withFixture(async (fixture) => {
    const changed = Buffer.from(fixture.bytes);
    const last = changed.at(-1);
    assert.ok(last !== undefined);
    changed[changed.length - 1] = last ^ 1;
    writeFileSync(fixture.tarballPath, changed);
    await assert.rejects(Effect.runPromise(prepareCandidate(fixture.options)), /qualification/);
    assert.equal(existsSync(fixture.candidateDirectory), false);
  }));

test("qualification rejects incomplete inventories, export changes and inconsistent native identities", () =>
  withFixture((fixture) => {
    assert.deepEqual(validatePackageIdentity(fixture.identity).exports, fixture.identity.exports);
    const missingPlatform = structuredClone(fixture.identity);
    Reflect.deleteProperty(missingPlatform.native, "linux-x64");
    assert.throws(() => validatePackageIdentity(missingPlatform));
    const wrongNativeSource = structuredClone(fixture.identity);
    wrongNativeSource.native["linux-x64"].build.sourceSha256 = "0".repeat(64);
    assert.throws(() => validatePackageIdentity(wrongNativeSource));
    const wrongNativeBytes = structuredClone(fixture.identity);
    wrongNativeBytes.fileSha256["dist/native/darwin-arm64/libreactor_effect_native.dylib"] =
      "0".repeat(64);
    assert.throws(() => validatePackageIdentity(wrongNativeBytes));
    const missingSidecar = structuredClone(fixture.identity);
    delete missingSidecar.fileSha256["dist/native/linux-x64/native-identity.json"];
    assert.throws(() => validatePackageIdentity(missingSidecar));
    const exports = structuredClone(fixture.identity);
    exports.exports.push("./unexpected");
    assert.throws(() => validatePackageIdentity(exports));
    const duplicate = structuredClone(fixture.identity);
    duplicate.files.push("package.json");
    assert.throws(() => validatePackageIdentity(duplicate));
  }));

/** @type {Array<[string, Record<string, unknown>]>} */
const invalidManifests = [
  ["a different npm name", { name: "different-package" }],
  ["a different version", { version: "0.2.1" }],
  ["private publication", { private: true }],
  [
    "provenance requested by the archive",
    { publishConfig: { access: "public", provenance: true } },
  ],
  [
    "another registry",
    { publishConfig: { registry: "https://registry.example.invalid/", provenance: false } },
  ],
];
for (const [name, manifest] of invalidManifests)
  test(`the real npm provider rejects ${name} before a publishable candidate is retained`, () =>
    withFixture(
      async (fixture) => {
        const result = await Effect.runPromise(Effect.result(prepareCandidate(fixture.options)));
        assert.ok(Result.isFailure(result));
        assert.equal(result.failure._tag, "ReleaseError");
        assert.match(result.failure.code, /^(npm|reactor-release-npm)/);
        assert.equal(existsSync(join(fixture.candidateDirectory, "identity.json")), false);
        assert.deepEqual(readFileSync(fixture.tarballPath), fixture.bytes);
      },
      { manifest },
    ));

for (const file of ["bundle.json", "plan.json", "identity.json"])
  test(`loading detects ${file} tampering without changing the original input`, () =>
    withFixture(async (fixture) => {
      const { input } = await prepared(fixture);
      const path = join(fixture.candidateDirectory, file);
      if (file === "bundle.json")
        writeFileSync(path, Buffer.concat([readFileSync(path), Buffer.from("\n")]));
      else {
        const value = JSON.parse(readFileSync(path, "utf8"));
        if (file === "plan.json") value.operations[0].intent.initialTag = "unexpected";
        else value.applicationCommit = "4".repeat(40);
        writeJson(path, value);
      }
      await assert.rejects(Effect.runPromise(loadCandidate(input)));
    }));

test("loading refuses same-size owned byte corruption and symlink substitutions", () =>
  withFixture(async (fixture) => {
    const { input } = await prepared(fixture);
    const loaded = await Effect.runPromise(loadCandidate(input));
    const path = join(fixture.candidateDirectory, "content", loaded.intent.tarball.content.sha256);
    const changed = Buffer.from(fixture.bytes);
    const last = changed.at(-1);
    assert.ok(last !== undefined);
    changed[changed.length - 1] = last ^ 1;
    chmodSync(path, 0o600);
    writeFileSync(path, changed);
    await assert.rejects(Effect.runPromise(loadCandidate(input)));
    const alias = join(fixture.directory, "bundle-alias.json");
    symlinkSync(join(fixture.candidateDirectory, "bundle.json"), alias);
    const { readBytes } = await import("../model.mjs");
    assert.throws(() => readBytes(alias));
  }));

test("a validly rehashed Plan still cannot change the application's npm principal or journal", () =>
  withFixture(async (fixture) => {
    const { identity, input } = await prepared(fixture);
    const loaded = await Effect.runPromise(loadCandidate(input));
    const otherPrincipal = await Effect.runPromise(
      Npm.publish(
        new Npm.PublishIntent({
          ...loaded.intent,
          authorization: new Npm.TokenAuthorization({ principal: "other-publisher" }),
        }),
      ),
    );
    /** @type {Array<[readonly import("@mannyc1/ts-release").Operation[], string]>} */
    const variants = [
      [[otherPrincipal], loaded.plan.journalId],
      [loaded.plan.operations, `${loaded.plan.journalId}:replacement`],
    ];
    for (const [operations, journal] of variants) {
      const plan = await Effect.runPromise(createPlan(input.bundleSha256, operations, journal));
      writeJson(join(fixture.candidateDirectory, "plan.json"), plan);
      writeJson(join(fixture.candidateDirectory, "identity.json"), {
        ...identity,
        planId: plan.planId,
      });
      const result = await Effect.runPromise(
        Effect.result(loadCandidate({ ...input, planId: plan.planId })),
      );
      assert.ok(Result.isFailure(result));
      assert.equal(result.failure.code, "reactor-release-publication-policy");
    }
  }));
