import assert from "node:assert/strict";
import { existsSync, readFileSync } from "node:fs";
import { test } from "bun:test";
import { Effect, Redacted } from "effect";
import { loadCandidate, npmCredentials } from "../application.mjs";
import { prepareCandidate } from "../prepare.mjs";
import { sourceFromEnvironment, authorization } from "../provenance.mjs";
import { releaseFailure } from "../model.mjs";
import * as Npm from "@mannyc1/ts-release-npm";
import {
  attestOffline,
  loadOffline,
  prepared,
  provenanceSource,
  verifyOffline,
  withFixture,
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
  assert.deepEqual(sourceFromEnvironment(environment()), provenanceSource);
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
    assert.throws(() => sourceFromEnvironment({ ...environment(), ...changed }));
});

test("preparation rejects another signing commit or workflow before invoking the signer", () =>
  withFixture(async (fixture) => {
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
        source: new Npm.ProvenanceSource({ ...provenanceSource, sourceCommit: "4".repeat(40) }),
      },
      {
        ...fixture.options,
        source: new Npm.ProvenanceSource({
          ...provenanceSource,
          workflow: ".github/workflows/other.yml",
        }),
      },
    ])
      await assert.rejects(Effect.runPromise(prepareCandidate(options, signing)), /qualification/);
    assert.equal(signed, 0);
    assert.equal(existsSync(fixture.candidateDirectory), false);
  }));

test("retained provenance binds the actual qualified archive and requires trust verification on restore", () =>
  withFixture(async (fixture) => {
    const { input } = await prepared(fixture);
    const loaded = await Effect.runPromise(loadOffline(input));
    assert.equal(loaded.intent.provenance._tag, "GitHubActionsProvenance");
    const proof = loaded.intent.provenance;
    assert.ok(proof._tag === "GitHubActionsProvenance");
    const bytes = await Effect.runPromise(loaded.readContent(proof.bundle.content));
    const bundle = JSON.parse(Buffer.from(bytes).toString());
    const statement = JSON.parse(Buffer.from(bundle.dsseEnvelope.payload, "base64").toString());
    assert.equal(
      statement.subject[0].digest.sha512,
      Buffer.from(loaded.intent.integrity.slice(7), "base64").toString("hex"),
    );
    let verified = 0;
    await assert.rejects(
      Effect.runPromise(
        loadCandidate(input, () => {
          verified++;
          return Effect.fail(releaseFailure("untrusted-provenance"));
        }),
      ),
      /untrusted-provenance/,
    );
    assert.equal(verified, 1);
    await assert.rejects(
      Effect.runPromise(loadOffline({ ...input, candidateRunId: "9999" })),
      /identity/,
    );
    assert.deepEqual(readFileSync(fixture.tarballPath), fixture.bytes);
  }));

test("npm OIDC is requested only for the admitted authorized PUT and never for observations", () =>
  withFixture(async (fixture) => {
    const { input } = await prepared(fixture);
    const candidate = await Effect.runPromise(loadOffline(input));
    const providers = Npm.definitions({
      ...candidate,
      read: () => Effect.fail(releaseFailure("unexpected-read")),
      verifyProvenance: verifyOffline,
    });
    const provider = providers.find((entry) => entry.definitionId === "npm.publish");
    const operation = candidate.plan.operations[0];
    assert.ok(provider && operation);
    const request = await Effect.runPromise(
      provider.prepare(operation, {
        own: { operation, receipts: [], observations: [] },
        dependencies: [],
      }),
    );
    const binding = request.facts;
    let identities = 0;
    let exchanges = 0;
    /** @type {import("@mannyc1/ts-release/http").TrustedPublisherHost} */
    const trusted = {
      oidc: (selection) =>
        Effect.sync(() => {
          identities++;
          assert.equal(selection.audience, "npm:registry.npmjs.org");
          assert.equal(selection.workflow, ".github/workflows/release.yml");
          return Redacted.make("offline-oidc-token");
        }),
      exchange: (exchange) =>
        Effect.sync(() => {
          exchanges++;
          assert.equal(
            exchange.url,
            "https://registry.npmjs.org/-/npm/v1/oidc/token/exchange/package/reactor-effect-client",
          );
          return {
            status: 201,
            headers: {},
            body: Buffer.from(
              JSON.stringify({ token_type: "oidc", token: "offline-package-token" }),
            ),
          };
        }),
    };
    const options = { intent: candidate.intent, authorize: false, trusted };
    assert.deepEqual(
      await Effect.runPromise(npmCredentials({ ...binding, method: "GET" }, options)),
      {},
    );
    await assert.rejects(Effect.runPromise(npmCredentials(binding, options)), /authorization/);
    await assert.rejects(
      Effect.runPromise(
        npmCredentials(
          { ...binding, endpoint: "https://other.invalid/" },
          { ...options, authorize: true },
        ),
      ),
    );
    assert.equal(identities, 0);
    assert.equal(exchanges, 0);
    assert.deepEqual(
      await Effect.runPromise(npmCredentials(binding, { ...options, authorize: true })),
      { authorization: "Bearer offline-package-token" },
    );
    assert.equal(identities, 1);
    assert.equal(exchanges, 1);
  }));
