import { randomUUID } from "node:crypto";
import { join, resolve } from "node:path";
import { Config, Effect, Schema } from "effect";
import { loadPlan, ReleaseError } from "@mannyc1/ts-release";
import { loadBundle, verifiedArtifacts } from "@mannyc1/ts-release/bundle";
import { sameData } from "@mannyc1/ts-release/http";
import {
  fileContentOwner,
  makeHttpRead,
  makeGithubTrustedPublisherHost,
  makeHttpTransport,
  openGitJournal,
} from "@mannyc1/ts-release/node";
import * as Npm from "@mannyc1/ts-release-npm";
import {
  ApplicationInput,
  CandidateIdentity,
  Qualification,
  checked,
  journalId,
  journalRemote,
  packages,
  preparationContext,
  qualificationMatches,
  readBytes,
  reject,
  releaseFailure,
  sha256,
  validatePackageIdentity,
} from "./model.mjs";
import { authorization, verifyProvenance, workflowRef } from "./provenance.mjs";
import { currentExecutionHost } from "./host.mjs";

/** Admit saved bytes and the workspace's npm operations before acquiring any credentials or journal.
 * @param {unknown} raw
 * @param {Npm.VerifyProvenance} [verify] */
export const loadCandidate = (raw, verify = verifyProvenance) =>
  Effect.gen(function* () {
    const input = yield* checked("input", () =>
      Schema.decodeUnknownSync(ApplicationInput, { onExcessProperty: "error" })(raw),
    );
    const directory = resolve(input.candidateDirectory);
    const saved = yield* checked("identity", () => {
      const identity = Schema.decodeUnknownSync(CandidateIdentity, { onExcessProperty: "error" })(
        JSON.parse(readBytes(join(directory, "identity.json")).toString()),
      );
      const bundleBytes = readBytes(join(directory, "bundle.json"));
      if (
        sha256(bundleBytes) !== input.bundleSha256 ||
        identity.bundleSha256 !== input.bundleSha256 ||
        identity.planId !== input.planId ||
        identity.applicationCommit !== input.applicationCommit ||
        identity.qualification.ciRunId !== input.ciRunId ||
        identity.provenance.runId !== input.candidateRunId ||
        identity.qualification.sourceCommit !== input.sourceCommit
      )
        reject("Candidate identity does not match the selected preparation");
      return { identity, bundleBytes };
    });
    const version = saved.identity.qualification.version;
    const owner = fileContentOwner(join(directory, "content"));
    const bundle = yield* loadBundle(owner, saved.bundleBytes).pipe(
      Effect.mapError(() => releaseFailure("bundle")),
    );
    /** @type {import("@mannyc1/ts-release/bundle").ReadContent} */
    const readContent = (content) =>
      owner.read(content).pipe(Effect.mapError(() => releaseFailure("content")));
    const files = verifiedArtifacts({ bundle, readContent }, 32 * 1024 * 1024);
    /** @param {string} logicalName */
    const ownedFile = (logicalName) => {
      const file = bundle.artifacts.find((entry) => entry.logicalName === logicalName);
      return file?._tag === "OwnedFile" ? file : undefined;
    };
    const archives = new Map(
      packages.map((entry) => [entry.name, ownedFile(`${entry.name}-${version}.tgz`)]),
    );
    const proofs = new Map(
      packages.map((entry) => [
        entry.name,
        ownedFile(`${entry.name}-${version}.tgz.sigstore.json`),
      ]),
    );
    const identityFile = ownedFile("package-identity.json");
    const qualificationFile = ownedFile("qualification.json");
    if (
      bundle.artifacts.length !== packages.length * 2 + 2 ||
      [...archives.values(), ...proofs.values()].some((file) => file === undefined) ||
      identityFile === undefined ||
      qualificationFile === undefined
    )
      return yield* checked("content", () => reject("Unexpected candidate content set"));
    const identityBytes = yield* files.read(identityFile);
    const qualificationBytes = yield* files.read(qualificationFile);
    yield* checked("qualified-content", () => {
      const identity = validatePackageIdentity(JSON.parse(new TextDecoder().decode(identityBytes)));
      const qualification = Schema.decodeUnknownSync(Qualification)(
        JSON.parse(new TextDecoder().decode(qualificationBytes)),
      );
      if (
        !sameData(qualification, saved.identity.qualification) ||
        !qualificationMatches(qualification, identity) ||
        packages.some(
          (entry) =>
            identity.packages[entry.name].sha256 !== archives.get(entry.name)?.content.sha256,
        )
      )
        reject("Retained qualification does not identify these archives");
    });
    const noRead = () =>
      checked("admission-network", () => reject("Admission cannot contact the registry"));
    const providers = Npm.definitions({
      bundle,
      readContent,
      read: noRead,
      verifyProvenance: verify,
    });
    const planValue = yield* checked("plan", () =>
      JSON.parse(readBytes(join(directory, "plan.json")).toString()),
    );
    const plan = yield* loadPlan(planValue, providers);
    const intents = yield* checked("publication-policy", () => {
      if (
        plan.planId !== input.planId ||
        plan.bundleId !== input.bundleSha256 ||
        plan.journalId !== journalId(version) ||
        plan.operations.length !== packages.length
      )
        reject("Only the original workspace publication is allowed");
      /** @type {Map<string, { operation: import("@mannyc1/ts-release").Operation, intent: Npm.PublishIntent }>} */
      const byName = new Map();
      for (const operation of plan.operations) {
        if (operation.definitionId !== "npm.publish") reject("Only npm publications are allowed");
        const intent = Schema.decodeUnknownSync(Npm.PublishIntent)(operation.intent);
        if (byName.has(intent.name)) reject("Duplicate package publication");
        byName.set(intent.name, { operation, intent });
      }
      /** @type {Map<string, Npm.PublishIntent>} */
      const admitted = new Map();
      for (const entry of packages) {
        const selected = byName.get(entry.name) ?? reject(`Missing publication for ${entry.name}`);
        const { operation, intent } = selected;
        const dependencies = entry.dependsOn.map(
          (name) => byName.get(name)?.operation.operationId ?? reject("Missing dependency"),
        );
        if (
          JSON.stringify([...operation.dependsOn].sort()) !==
            JSON.stringify([...dependencies].sort()) ||
          intent.version !== version ||
          !sameData(intent.tarball, archives.get(entry.name)) ||
          intent.access !== "public" ||
          intent.initialTag !== (version.includes("-") ? "next" : "latest") ||
          !sameData(intent.authorization, authorization) ||
          intent.provenance._tag !== "GitHubActionsProvenance" ||
          !sameData(intent.provenance.bundle, proofs.get(entry.name)) ||
          !sameData(intent.provenance.source, saved.identity.provenance) ||
          intent.provenance.source.sourceCommit !== input.sourceCommit ||
          intent.provenance.source.sourceCommit !== input.applicationCommit ||
          intent.provenance.source.sourceRef !== workflowRef ||
          intent.provenance.source.eventName !== "workflow_dispatch" ||
          intent.provenance.source.runnerEnvironment !== "github-hosted"
        )
          reject("Publication destination, bytes, authentication or policy changed");
        admitted.set(entry.name, intent);
      }
      return admitted;
    });
    const provider =
      providers.find((entry) => entry.definitionId === "npm.publish") ??
      reject("Missing npm provider");
    for (const operation of plan.operations)
      yield* provider.prepare(operation, preparationContext(plan, operation));
    return { input, bundle, plan, intents, readContent };
  });

/** Keep read/observe credential-free and acquire npm OIDC only for an admitted package write.
 * @param {import("@mannyc1/ts-release/http").CredentialBinding} binding
 * @param {{ intents: ReadonlyMap<string, Npm.PublishIntent>, authorize: boolean, trusted: import("@mannyc1/ts-release/http").TrustedPublisherHost }} options */
export const npmCredentials = (binding, options) =>
  Effect.gen(function* () {
    const selected = yield* Npm.authorizationBinding(binding);
    const intent = options.intents.get(selected.packageName);
    yield* checked("credential-binding", () => {
      if (
        intent === undefined ||
        !sameData(selected.authorization, authorization) ||
        !sameData(selected.authorization, intent.authorization)
      )
        reject("Credential request is outside this release");
    });
    if (binding.method === "GET" || binding.method === "HEAD") return {};
    if (!options.authorize || binding.method !== "PUT" || !binding.bodyDigest)
      return yield* checked("authorization", () => reject("Publication was not authorized"));
    return yield* Npm.authorizeTrusted(
      { authorization, packageName: selected.packageName, binding },
      options.trusted,
    );
  });

/** @type {import("@mannyc1/ts-release/node").CreateApplication} */
export const createApplication = (raw) =>
  Effect.gen(function* () {
    const executionHostCommit = yield* checked("execution-host", currentExecutionHost);
    const { input, bundle, plan, intents, readContent } = yield* loadCandidate(raw);
    yield* checked("execution-host", () => {
      if (input.executionHostCommit !== executionHostCommit)
        reject("Application input names a different execution host");
    });
    const bounds = { timeoutMilliseconds: 30_000, maximumResponseBytes: 16 * 1024 * 1024 };
    /** @type {import("@mannyc1/ts-release/http").ResolveCredentials} */
    const credentials = (binding) =>
      npmCredentials(binding, {
        intents,
        authorize: input.authorize,
        trusted: makeGithubTrustedPublisherHost(bounds),
      });
    const providers = Npm.definitions({
      bundle,
      readContent,
      read: makeHttpRead({ ...bounds, credentials }),
      verifyProvenance,
    });
    const store = yield* openGitJournal({
      remote: journalRemote,
      cacheDirectory: resolve(".release/journal-cache"),
      // This application runs on the selected Ubuntu GitHub-hosted runner. Core
      // verifies the absolute executable and isolates Git configuration itself.
      gitExecutable: "/usr/bin/git",
      principal: "reactor-release-journal",
      scope: "npm-publication",
      timeoutMilliseconds: 30_000,
      maximumOutputBytes: 16 * 1024 * 1024,
      credentials: (coordinate) =>
        Effect.gen(function* () {
          yield* checked("journal-binding", () => {
            if (
              coordinate.remote !== journalRemote ||
              coordinate.principal !== "reactor-release-journal" ||
              coordinate.scope !== "npm-publication"
            )
              reject("Journal credential destination changed");
          });
          return { _tag: "Basic", username: "x-access-token", password: yield* secret("GH_TOKEN") };
        }),
    });
    return {
      bundle,
      options: { plan, authorize: input.authorize },
      host: {
        providers,
        store,
        transport: makeHttpTransport({ ...bounds, providers, credentials }),
        now: Date.now,
        uniqueId: randomUUID,
      },
    };
  });

/** @param {string} name */
const secret = (name) =>
  Config.Redacted(name).pipe(
    Effect.mapError(
      () =>
        new ReleaseError({
          code: "reactor-release-credential",
          message: "An explicitly selected release credential is unavailable",
        }),
    ),
  );
