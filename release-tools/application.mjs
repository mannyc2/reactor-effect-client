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
  packageName,
  readBytes,
  reject,
  releaseFailure,
  sha256,
  validatePackageIdentity,
} from "./model.mjs";
import { authorization, verifyProvenance, workflowRef } from "./provenance.mjs";

/** Admit saved bytes and one npm operation before acquiring any credentials or journal.
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
    const owner = fileContentOwner(join(directory, "content"));
    const bundle = yield* loadBundle(owner, saved.bundleBytes).pipe(
      Effect.mapError(() => releaseFailure("bundle")),
    );
    /** @type {import("@mannyc1/ts-release/bundle").ReadContent} */
    const readContent = (content) =>
      owner.read(content).pipe(Effect.mapError(() => releaseFailure("content")));
    const files = verifiedArtifacts({ bundle, readContent }, 32 * 1024 * 1024);
    const archive = bundle.artifacts.find(
      (file) => file.logicalName === `${packageName}-${saved.identity.qualification.version}.tgz`,
    );
    const identityFile = bundle.artifacts.find(
      (file) => file.logicalName === "package-identity.json",
    );
    const qualificationFile = bundle.artifacts.find(
      (file) => file.logicalName === "qualification.json",
    );
    const provenanceFile = bundle.artifacts.find(
      (file) =>
        file.logicalName ===
        `${packageName}-${saved.identity.qualification.version}.tgz.sigstore.json`,
    );
    if (
      bundle.artifacts.length !== 4 ||
      provenanceFile?._tag !== "OwnedFile" ||
      archive?._tag !== "OwnedFile" ||
      identityFile?._tag !== "OwnedFile" ||
      qualificationFile?._tag !== "OwnedFile"
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
        identity.sha256 !== archive.content.sha256 ||
        identity.sha256 !== qualification.sha256 ||
        identity.version !== qualification.version
      )
        reject("Retained qualification does not identify this archive");
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
    const { intent, operation } = yield* checked("publication-policy", () => {
      const operation = plan.operations[0] ?? reject("Missing npm operation");
      if (
        plan.planId !== input.planId ||
        plan.bundleId !== input.bundleSha256 ||
        plan.journalId !== journalId(saved.identity.qualification.version) ||
        plan.operations.length !== 1 ||
        operation?.definitionId !== "npm.publish" ||
        operation.dependsOn.length !== 0
      )
        reject("Only the original single-package publication is allowed");
      const intent = Schema.decodeUnknownSync(Npm.PublishIntent)(operation.intent);
      if (
        intent.name !== packageName ||
        intent.version !== saved.identity.qualification.version ||
        !sameData(intent.tarball, archive) ||
        intent.access !== "public" ||
        intent.initialTag !== (intent.version.includes("-") ? "next" : "latest") ||
        !sameData(intent.authorization, authorization) ||
        intent.provenance._tag !== "GitHubActionsProvenance" ||
        !sameData(intent.provenance.bundle, provenanceFile) ||
        !sameData(intent.provenance.source, saved.identity.provenance) ||
        intent.provenance.source.sourceCommit !== input.sourceCommit ||
        intent.provenance.source.sourceCommit !== input.applicationCommit ||
        intent.provenance.source.sourceRef !== workflowRef ||
        intent.provenance.source.eventName !== "workflow_dispatch" ||
        intent.provenance.source.runnerEnvironment !== "github-hosted"
      )
        reject("Publication destination, bytes, authentication or policy changed");
      return { intent, operation };
    });
    const provider =
      providers.find((entry) => entry.definitionId === "npm.publish") ??
      reject("Missing npm provider");
    yield* provider.prepare(operation, {
      own: { operation, receipts: [], observations: [] },
      dependencies: [],
    });
    return { input, bundle, plan, intent, readContent };
  });

/** Keep read/observe credential-free and acquire npm OIDC only for the admitted write.
 * @param {import("@mannyc1/ts-release/http").CredentialBinding} binding
 * @param {{ intent: Npm.PublishIntent, authorize: boolean, trusted: import("@mannyc1/ts-release/http").TrustedPublisherHost }} options */
export const npmCredentials = (binding, options) =>
  Effect.gen(function* () {
    const selected = yield* Npm.authorizationBinding(binding);
    yield* checked("credential-binding", () => {
      if (
        selected.packageName !== packageName ||
        !sameData(selected.authorization, authorization) ||
        !sameData(selected.authorization, options.intent.authorization)
      )
        reject("Credential request is outside this release");
    });
    if (binding.method === "GET" || binding.method === "HEAD") return {};
    if (!options.authorize || binding.method !== "PUT" || !binding.bodyDigest)
      return yield* checked("authorization", () => reject("Publication was not authorized"));
    return yield* Npm.authorizeTrusted({ authorization, packageName, binding }, options.trusted);
  });

/** @type {import("@mannyc1/ts-release/node").CreateApplication} */
export const createApplication = (raw) =>
  Effect.gen(function* () {
    const { input, bundle, plan, intent, readContent } = yield* loadCandidate(raw);
    const bounds = { timeoutMilliseconds: 30_000, maximumResponseBytes: 16 * 1024 * 1024 };
    /** @type {import("@mannyc1/ts-release/http").ResolveCredentials} */
    const credentials = (binding) =>
      npmCredentials(binding, {
        intent,
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
