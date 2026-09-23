import { mkdirSync, writeFileSync } from "node:fs";
import { join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { Effect, Schema } from "effect";
import { makeGithubOidcTokenSource } from "@mannyc1/ts-release/node";
import * as Npm from "@mannyc1/ts-release-npm";
import { checked, principal, readBytes, reject, repository } from "./model.mjs";

export const workflow = ".github/workflows/release.yml";
export const workflowRef = "refs/heads/main";
export const authorization = new Npm.TrustedAuthorization({
  principal,
  repository,
  workflow,
  workflowRef,
  issuer: "https://token.actions.githubusercontent.com",
  audience: "npm:registry.npmjs.org",
});

/** @param {NodeJS.ProcessEnv} env */
export const sourceFromEnvironment = (env) => {
  if (
    env.GITHUB_ACTIONS !== "true" ||
    env.GITHUB_SERVER_URL !== "https://github.com" ||
    env.GITHUB_REPOSITORY !== repository ||
    env.GITHUB_WORKFLOW_REF !== `${repository}/${workflow}@${workflowRef}` ||
    env.GITHUB_REF !== workflowRef ||
    env.GITHUB_EVENT_NAME !== "workflow_dispatch" ||
    env.RUNNER_ENVIRONMENT !== "github-hosted" ||
    env.REPOSITORY_VISIBILITY !== "public"
  )
    reject("Provenance requires this public repository's manual main release workflow");
  return Schema.decodeUnknownSync(Npm.ProvenanceSource)({
    format: "npm-github-actions-provenance-source/v1",
    serverUrl: "https://github.com",
    repository,
    workflow,
    workflowRef,
    sourceRef: env.GITHUB_REF,
    sourceCommit: env.GITHUB_SHA,
    eventName: env.GITHUB_EVENT_NAME,
    repositoryId: env.GITHUB_REPOSITORY_ID,
    repositoryOwnerId: env.GITHUB_REPOSITORY_OWNER_ID,
    runnerEnvironment: env.RUNNER_ENVIRONMENT,
    runId: env.GITHUB_RUN_ID,
    runAttempt: env.GITHUB_RUN_ATTEMPT,
    repositoryVisibility: env.REPOSITORY_VISIBILITY,
  });
};

/** Start from the pinned Sigstore client's bundled production TUF trust root. */
const trust = () =>
  checked("sigstore-trust", () => {
    const directory = resolve(".release/sigstore");
    mkdirSync(directory, { recursive: true });
    const seeds = JSON.parse(
      readBytes(fileURLToPath(import.meta.resolve("@sigstore/tuf/seeds.json"))).toString(),
    );
    const root = seeds["https://tuf-repo-cdn.sigstore.dev"]["root.json"];
    if (typeof root !== "string" || !root) reject("Missing pinned Sigstore trust root");
    const tufRootPath = join(directory, "root.json");
    writeFileSync(tufRootPath, Buffer.from(root, "base64"), { mode: 0o600 });
    return { tufRootPath, tufCachePath: join(directory, "cache"), timeoutMilliseconds: 30_000 };
  });

/** @type {Npm.VerifyProvenance} */
export const verifyProvenance = (input) =>
  Effect.gen(function* () {
    return yield* Npm.makeSigstoreVerifier(yield* trust())(input);
  });

/** @param {Npm.ProvenanceSource} source */
export const makeAttester = (source) =>
  Effect.gen(function* () {
    return Npm.makeSigstoreAttester({
      source,
      ...(yield* trust()),
      oidc: makeGithubOidcTokenSource({
        timeoutMilliseconds: 30_000,
        maximumResponseBytes: 1024 * 1024,
      }),
      fulcioUrl: "https://fulcio.sigstore.dev",
      rekorUrl: "https://rekor.sigstore.dev",
    });
  });
