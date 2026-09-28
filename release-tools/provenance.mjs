import { Config, Effect, FileSystem, Path, Schema } from "effect";
import { makeGithubOidcTokenSource } from "@mannyc1/ts-release/node";
import * as Npm from "@mannyc1/ts-release-npm";
import { checked, principal, readJson, reject, repository } from "./model.mjs";
import { variable } from "./host.mjs";

export const workflow = ".github/workflows/release.yml";
export const workflowRef = "refs/heads/main";
export const authorization = Npm.TrustedAuthorization.make({
  principal,
  repository,
  workflow,
  workflowRef,
  issuer: "https://token.actions.githubusercontent.com",
  audience: "npm:registry.npmjs.org",
});

/** The signing invocation, which must be this public repository's manual main release workflow. */
export const sourceFromEnvironment = Effect.gen(function* () {
  const environment = yield* Config.all({
    actions: variable("GITHUB_ACTIONS"),
    serverUrl: variable("GITHUB_SERVER_URL"),
    repository: variable("GITHUB_REPOSITORY"),
    workflowRef: variable("GITHUB_WORKFLOW_REF"),
    ref: variable("GITHUB_REF"),
    eventName: variable("GITHUB_EVENT_NAME"),
    runnerEnvironment: variable("RUNNER_ENVIRONMENT"),
    visibility: variable("REPOSITORY_VISIBILITY"),
    sha: variable("GITHUB_SHA"),
    repositoryId: variable("GITHUB_REPOSITORY_ID"),
    repositoryOwnerId: variable("GITHUB_REPOSITORY_OWNER_ID"),
    runId: variable("GITHUB_RUN_ID"),
    runAttempt: variable("GITHUB_RUN_ATTEMPT"),
  });
  if (
    environment.actions !== "true" ||
    environment.serverUrl !== "https://github.com" ||
    environment.repository !== repository ||
    environment.workflowRef !== `${repository}/${workflow}@${workflowRef}` ||
    environment.ref !== workflowRef ||
    environment.eventName !== "workflow_dispatch" ||
    environment.runnerEnvironment !== "github-hosted" ||
    environment.visibility !== "public"
  )
    return yield* reject(
      "Provenance requires this public repository's manual main release workflow",
    );
  return yield* Schema.decodeEffect(Npm.ProvenanceSource)({
    format: "npm-github-actions-provenance-source/v1",
    serverUrl: "https://github.com",
    repository,
    workflow,
    workflowRef,
    sourceRef: environment.ref,
    sourceCommit: environment.sha,
    eventName: environment.eventName,
    repositoryId: environment.repositoryId,
    repositoryOwnerId: environment.repositoryOwnerId,
    runnerEnvironment: environment.runnerEnvironment,
    runId: environment.runId,
    runAttempt: environment.runAttempt,
    repositoryVisibility: environment.visibility,
  });
});

/** The part of @sigstore/tuf's bundled seeds that holds the production trust root. */
const SigstoreSeeds = Schema.Struct({
  "https://tuf-repo-cdn.sigstore.dev": Schema.Struct({
    "root.json": Schema.String.check(Schema.isMinLength(1)),
  }),
});

/** Start from the pinned Sigstore client's bundled production TUF trust root. */
const trust = Effect.gen(function* () {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const directory = path.resolve(".release/sigstore");
  yield* fs.makeDirectory(directory, { recursive: true });
  const seedsPath = yield* path.fromFileUrl(
    new URL(import.meta.resolve("@sigstore/tuf/seeds.json")),
  );
  const seeds = yield* readJson(seedsPath).pipe(
    Effect.flatMap(Schema.decodeUnknownEffect(SigstoreSeeds)),
  );
  const root = seeds["https://tuf-repo-cdn.sigstore.dev"]["root.json"];
  const tufRootPath = path.join(directory, "root.json");
  yield* fs.writeFile(tufRootPath, Buffer.from(root, "base64"), { mode: 0o600 });
  return { tufRootPath, tufCachePath: path.join(directory, "cache"), timeoutMilliseconds: 30_000 };
}).pipe(checked("sigstore-trust"));

/** Checks each attestation against the pinned trust root. ts-release calls the verifier
 * without services, so it keeps the ones it was built with.
 * @type {Effect.Effect<Npm.VerifyProvenance, never, FileSystem.FileSystem | Path.Path>} */
export const provenanceVerifier = Effect.map(
  Effect.context(),
  (services) => (input) =>
    trust.pipe(
      Effect.flatMap((options) => Npm.makeSigstoreVerifier(options)(input)),
      Effect.provideContext(services),
    ),
);

/** @param {Npm.ProvenanceSource} source */
export const makeAttester = (source) =>
  Effect.gen(function* () {
    return Npm.makeSigstoreAttester({
      source,
      ...(yield* trust),
      oidc: makeGithubOidcTokenSource({
        timeoutMilliseconds: 30_000,
        maximumResponseBytes: 1024 * 1024,
      }),
      fulcioUrl: "https://fulcio.sigstore.dev",
      rekorUrl: "https://rekor.sigstore.dev",
    });
  });
