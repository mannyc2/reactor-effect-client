import assert from "node:assert/strict";
import { test } from "bun:test";
import { Effect, FileSystem, Redacted, Result, Schema } from "effect";
import { loadCandidate, npmCredentials } from "../application.mjs";
import { prepareCandidate } from "../prepare.mjs";
import { jsonLine, preparationContext, releaseFailure } from "../model.mjs";
import { sourceFromEnvironment, authorization } from "../provenance.mjs";
import * as Npm from "@mannyc1/ts-release-npm";
import {
  attestOffline,
  attestedStatement,
  clientPackage,
  decodeStatement,
  digest,
  failureCode,
  intentFor,
  loadOffline,
  makeFixture,
  nativePackage,
  operationFor,
  packageNames,
  prepared,
  provenanceSource,
  runTest,
  verifyOffline,
  withEnvironment,
} from "./Fixture.mjs";

const environment = () => ({
  GITHUB_ACTIONS: "true",
  GITHUB_SERVER_URL: "https://github.com",
  GITHUB_REPOSITORY: authorization.repository,
  GITHUB_WORKFLOW_REF: `${authorization.repository}/${authorization.workflow}@${authorization.workflowRef}`,
  GITHUB_REF: "refs/heads/main",
  GITHUB_EVENT_NAME: "workflow_dispatch",
  RUNNER_ENVIRONMENT: "github-hosted",
  REPOSITORY_VISIBILITY: "public",
  GITHUB_SHA: provenanceSource.sourceCommit,
  GITHUB_REPOSITORY_ID: "11",
  GITHUB_REPOSITORY_OWNER_ID: "12",
  GITHUB_RUN_ID: "8101",
  GITHUB_RUN_ATTEMPT: "1",
});

test("signing is bound to the public main GitHub-hosted release invocation", () => {
  assert.deepEqual(
    Effect.runSync(sourceFromEnvironment.pipe(withEnvironment(environment()))),
    provenanceSource,
  );
  for (const changed of [
    { REPOSITORY_VISIBILITY: "private" },
    { GITHUB_EVENT_NAME: "pull_request" },
    { GITHUB_REF: "refs/heads/other" },
    { GITHUB_SHA: "short" },
    { RUNNER_ENVIRONMENT: "self-hosted" },
    { GITHUB_REPOSITORY: "other/repo" },
    {
      GITHUB_WORKFLOW_REF: `${authorization.repository}/.github/workflows/other.yml@refs/heads/main`,
    },
  ])
    assert.throws(() =>
      Effect.runSync(sourceFromEnvironment.pipe(withEnvironment({ ...environment(), ...changed }))),
    );
});

test("preparation rejects another signing commit or workflow before invoking the signer", () =>
  runTest(
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const fixture = yield* makeFixture();
      let signed = 0;
      const signing = {
        /** @type {Npm.Attest} */
        attest: (request) => {
          signed++;
          return attestOffline(request);
        },
        verifyProvenance: verifyOffline,
      };
      for (const options of [
        { ...fixture.options, applicationCommit: "4".repeat(40) },
        {
          ...fixture.options,
          source: Npm.ProvenanceSource.make({ ...provenanceSource, sourceCommit: "4".repeat(40) }),
        },
        {
          ...fixture.options,
          source: Npm.ProvenanceSource.make({
            ...provenanceSource,
            workflow: ".github/workflows/other.yml",
          }),
        },
      ])
        assert.equal(
          yield* failureCode(prepareCandidate(options, signing)),
          "reactor-release-qualification",
        );
      assert.equal(signed, 0);
      assert.equal(yield* fs.exists(fixture.candidateDirectory), false);
    }),
  ));

test("preparation signs exactly one statement per package, each naming that archive's sha512", () =>
  runTest(
    Effect.gen(function* () {
      const fixture = yield* makeFixture();
      /** @type {Map<string, string>} */
      const signedDigests = new Map();
      const signing = {
        /** @type {Npm.Attest} */
        attest: (request) =>
          Effect.gen(function* () {
            assert.equal(request.payloadType, "application/vnd.in-toto+json");
            const statement = yield* decodeStatement(request.payload);
            const subject = statement.subject[0];
            assert.equal(statement.subject.length, 1);
            assert.ok(subject !== undefined);
            assert.equal(signedDigests.has(subject.name), false);
            signedDigests.set(subject.name, subject.digest.sha512);
            return yield* attestOffline(request);
          }),
        verifyProvenance: verifyOffline,
      };
      const identity = yield* prepareCandidate(fixture.options, signing);
      assert.equal(identity.format, "reactor-ts-release/v3");
      /** @type {Map<string, string>} */
      const expected = new Map();
      for (const name of packageNames)
        expected.set(
          `pkg:npm/${name}@0.2.0`,
          yield* digest(fixture.packages[name].bytes, "SHA-512"),
        );
      assert.deepEqual(expected, signedDigests);
    }),
  ));

test("retained provenance binds each qualified archive and requires trust verification on restore before registry access", () =>
  runTest(
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const fixture = yield* makeFixture();
      const { input } = yield* prepared(fixture);
      const loaded = yield* loadOffline(input);
      for (const name of packageNames) {
        const intent = intentFor({ candidate: loaded, name });
        const proof = intent.provenance;
        assert.ok(proof._tag === "GitHubActionsProvenance");
        assert.equal(proof.bundle.logicalName, `${name}-0.2.0.tgz.sigstore.json`);
        assert.equal(proof.mediaType, "application/vnd.dev.sigstore.bundle.v0.3+json");
        assert.deepEqual(proof.source, provenanceSource);
        const attested = yield* attestedStatement(yield* loaded.readContent(proof.bundle.content));
        assert.equal(attested.packageName, name);
        assert.equal(attested.version, "0.2.0");
        assert.equal(
          attested.sha512,
          Buffer.from(intent.integrity.slice(7), "base64").toString("hex"),
        );
        assert.equal(attested.sha512, yield* digest(fixture.packages[name].bytes, "SHA-512"));
      }
      // Restoring verifies every retained attestation once, and each names a distinct package.
      /** @type {string[]} */
      const verifiedPackages = [];
      yield* loadCandidate(input, (request) =>
        attestedStatement(request.bundleBytes).pipe(
          Effect.flatMap(({ packageName }) => {
            verifiedPackages.push(packageName);
            return verifyOffline(request);
          }),
        ),
      );
      assert.deepEqual([...verifiedPackages].sort(), [...packageNames].sort());
      let rejected = 0;
      const untrusted = loadCandidate(input, () => {
        rejected++;
        return Effect.fail(releaseFailure("untrusted-provenance"));
      });
      assert.equal(yield* failureCode(untrusted), "reactor-release-untrusted-provenance");
      assert.equal(rejected, 1);
      assert.equal(
        yield* failureCode(loadOffline({ ...input, candidateRunId: "9999" })),
        "reactor-release-identity",
      );
      for (const archive of Object.values(fixture.packages))
        assert.deepEqual(yield* fs.readFile(archive.tarballPath), archive.bytes);
    }),
  ));

/** The binding scope's fields; the intent names the package a credential is for. */
const BindingScope = Schema.fromJsonString(Schema.Record(Schema.String, Schema.Unknown));
const Intent = Schema.Record(Schema.String, Schema.Unknown);

test("npm OIDC is requested only for an admitted package's authorized PUT and never for observations or foreign packages", () =>
  runTest(
    Effect.gen(function* () {
      const fixture = yield* makeFixture();
      const { input } = yield* prepared(fixture);
      const candidate = yield* loadOffline(input);
      const providers = Npm.definitions({
        ...candidate,
        read: () => Effect.fail(releaseFailure("unexpected-read")),
        verifyProvenance: verifyOffline,
      });
      const provider =
        providers.find((entry) => entry.definitionId === "npm.publish") ??
        assert.fail("missing npm provider");
      let identities = 0;
      let exchanges = 0;
      /** @type {string[]} */
      const exchanged = [];
      /** @type {import("@mannyc1/ts-release/http").TrustedPublisherHost} */
      const trusted = {
        oidc: (selection) =>
          Effect.sync(() => {
            identities++;
            assert.equal(selection.audience, "npm:registry.npmjs.org");
            assert.equal(selection.workflow, ".github/workflows/release.yml");
            assert.equal(selection.repository, "mannyc2/reactor-effect-client");
            return Redacted.make("offline-oidc-token");
          }),
        exchange: (exchange) =>
          Effect.sync(() => {
            exchanges++;
            const prefix = "https://registry.npmjs.org/-/npm/v1/oidc/token/exchange/package/";
            assert.ok(exchange.url.startsWith(prefix), exchange.url);
            exchanged.push(exchange.url.slice(prefix.length));
            return {
              status: 201,
              headers: {},
              body: new TextEncoder().encode(
                '{"token_type":"oidc","token":"offline-package-token"}',
              ),
            };
          }),
      };
      const options = { intents: candidate.intents, authorize: false, trusted };
      /** @type {Map<string, import("@mannyc1/ts-release").RequestFacts>} */
      const bindings = new Map();
      for (const name of packageNames) {
        const operation = operationFor({ candidate, name });
        const context = yield* preparationContext(candidate.plan, operation);
        const request = yield* provider.prepare(operation, context);
        const binding = request.facts;
        bindings.set(name, binding);
        assert.equal(binding.endpoint, `https://registry.npmjs.org/${name}`);
        for (const method of ["GET", "HEAD"])
          assert.deepEqual(yield* npmCredentials({ ...binding, method }, options), {});
        assert.equal(
          yield* failureCode(npmCredentials(binding, options)),
          "reactor-release-authorization",
        );
        yield* Effect.flip(
          npmCredentials(
            { ...binding, endpoint: "https://other.invalid/" },
            { ...options, authorize: true },
          ),
        );
        yield* Effect.flip(
          npmCredentials(
            { ...binding, principal: "other-publisher" },
            { ...options, authorize: true },
          ),
        );
      }
      assert.equal(identities, 0);
      assert.equal(exchanges, 0);
      const clientBinding = bindings.get(clientPackage);
      const nativeBinding = bindings.get(nativePackage);
      assert.ok(clientBinding && nativeBinding);
      // A package outside the workspace is refused even when the run is authorized: the
      // binding is otherwise well formed for npm, only its name is not an admitted intent.
      const scope = yield* Schema.decodeEffect(BindingScope)(clientBinding.scope);
      const intent = yield* Schema.decodeUnknownEffect(Intent)(scope.intent);
      const foreign = {
        ...clientBinding,
        scope: yield* jsonLine({ ...scope, intent: { ...intent, name: "some-other-package" } }),
        endpoint: "https://registry.npmjs.org/some-other-package",
      };
      assert.equal((yield* Npm.authorizationBinding(foreign)).packageName, "some-other-package");
      const refused = yield* Effect.result(
        npmCredentials(foreign, { ...options, authorize: true }),
      );
      assert.ok(Result.isFailure(refused));
      assert.equal(refused.failure.code, "reactor-release-credential-binding");
      // A workspace package the loaded Plan did not admit is refused the same way.
      const partial = yield* Effect.result(
        npmCredentials(nativeBinding, {
          intents: new Map([[clientPackage, intentFor({ candidate, name: clientPackage })]]),
          authorize: true,
          trusted,
        }),
      );
      assert.ok(Result.isFailure(partial));
      assert.equal(partial.failure.code, "reactor-release-credential-binding");
      assert.equal(identities, 0);
      assert.equal(exchanges, 0);
      for (const name of packageNames) {
        const binding = bindings.get(name);
        assert.ok(binding);
        assert.deepEqual(yield* npmCredentials(binding, { ...options, authorize: true }), {
          authorization: "Bearer offline-package-token",
        });
      }
      assert.equal(identities, packageNames.length);
      assert.equal(exchanges, packageNames.length);
      assert.deepEqual(exchanged, [...packageNames]);
    }),
  ));
