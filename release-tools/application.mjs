// ts-release's host asks for a synchronous unique id for each journal event, and Effect's
// Crypto only offers one as an effect.
// @effect-diagnostics-next-line nodeBuiltinImport:off
import { randomUUID } from "node:crypto";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { Config, Effect, Path, Schema } from "effect";
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
  decodeJson,
  journalId,
  journalRemote,
  packages,
  preparationContext,
  qualificationMatches,
  readBytes,
  readJson,
  reject,
  releaseFailure,
  sameStrings,
  sha256,
  validatePackageIdentity,
} from "./model.mjs";
import { authorization, provenanceVerifier, workflowRef } from "./provenance.mjs";
import { currentExecutionHost } from "./host.mjs";

/** Whether two values are the same data. A value without a canonical form is rejected.
 * @param {unknown} left @param {unknown} right */
const same = (left, right) =>
  Effect.try({ try: () => sameData(left, right), catch: () => reject("Value is not canonical") });

/** Admit saved bytes and the workspace's npm operations before acquiring any credentials or journal. */
export const loadCandidate = Effect.fn("loadCandidate")(
  /** @param {unknown} raw @param {Npm.VerifyProvenance} verify */
  function* (raw, verify) {
    const path = yield* Path.Path;
    const input = yield* Schema.decodeUnknownEffect(ApplicationInput, {
      onExcessProperty: "error",
    })(raw).pipe(checked("input"));
    const directory = path.resolve(input.candidateDirectory);
    const saved = yield* Effect.gen(function* () {
      const identity = yield* readJson(path.join(directory, "identity.json")).pipe(
        Effect.flatMap(
          Schema.decodeUnknownEffect(CandidateIdentity, { onExcessProperty: "error" }),
        ),
      );
      const bundleBytes = yield* readBytes(path.join(directory, "bundle.json"));
      if (
        (yield* sha256(bundleBytes)) !== input.bundleSha256 ||
        identity.bundleSha256 !== input.bundleSha256 ||
        identity.planId !== input.planId ||
        identity.applicationCommit !== input.applicationCommit ||
        identity.qualification.ciRunId !== input.ciRunId ||
        identity.provenance.runId !== input.candidateRunId ||
        identity.qualification.sourceCommit !== input.sourceCommit
      )
        return yield* reject("Candidate identity does not match the selected preparation");
      return { identity, bundleBytes };
    }).pipe(checked("identity"));
    const version = saved.identity.qualification.version;
    const owner = fileContentOwner(path.join(directory, "content"));
    const bundle = yield* loadBundle(owner, saved.bundleBytes).pipe(checked("bundle"));
    /** @type {import("@mannyc1/ts-release/bundle").ReadContent} */
    const readContent = (content) => owner.read(content).pipe(checked("content"));
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
      return yield* releaseFailure("content");
    const identityBytes = yield* files.read(identityFile);
    const qualificationBytes = yield* files.read(qualificationFile);
    yield* Effect.gen(function* () {
      const identity = yield* decodeJson(new TextDecoder().decode(identityBytes)).pipe(
        Effect.flatMap(validatePackageIdentity),
      );
      const qualification = yield* decodeJson(new TextDecoder().decode(qualificationBytes)).pipe(
        Effect.flatMap(Schema.decodeUnknownEffect(Qualification)),
      );
      if (
        !(yield* same(qualification, saved.identity.qualification)) ||
        !qualificationMatches({ qualification, identity }) ||
        packages.some(
          (entry) =>
            identity.packages[entry.name].sha256 !== archives.get(entry.name)?.content.sha256,
        )
      )
        return yield* reject("Retained qualification does not identify these archives");
    }).pipe(checked("qualified-content"));
    const noRead = () => Effect.fail(releaseFailure("admission-network"));
    const providers = Npm.definitions({
      bundle,
      readContent,
      read: noRead,
      verifyProvenance: verify,
    });
    const planValue = yield* readJson(path.join(directory, "plan.json")).pipe(checked("plan"));
    const plan = yield* loadPlan(planValue, providers);
    const intents = yield* Effect.gen(function* () {
      if (
        plan.planId !== input.planId ||
        plan.bundleId !== input.bundleSha256 ||
        plan.journalId !== journalId(version) ||
        plan.operations.length !== packages.length
      )
        return yield* reject("Only the original workspace publication is allowed");
      /** @type {Map<string, { operation: import("@mannyc1/ts-release").Operation, intent: Npm.PublishIntent }>} */
      const byName = new Map();
      for (const operation of plan.operations) {
        if (operation.definitionId !== "npm.publish")
          return yield* reject("Only npm publications are allowed");
        const intent = yield* Schema.decodeUnknownEffect(Npm.PublishIntent)(operation.intent);
        if (byName.has(intent.name)) return yield* reject("Duplicate package publication");
        byName.set(intent.name, { operation, intent });
      }
      /** @type {Map<string, Npm.PublishIntent>} */
      const admitted = new Map();
      for (const entry of packages) {
        const selected = byName.get(entry.name);
        if (selected === undefined) return yield* reject(`Missing publication for ${entry.name}`);
        const { operation, intent } = selected;
        const dependencies = [];
        for (const name of entry.dependsOn) {
          const dependency = byName.get(name);
          if (dependency === undefined) return yield* reject("Missing dependency");
          dependencies.push(dependency.operation.operationId);
        }
        if (
          !sameStrings({ left: operation.dependsOn, right: dependencies }) ||
          intent.version !== version ||
          !(yield* same(intent.tarball, archives.get(entry.name))) ||
          intent.initialTag !== (version.includes("-") ? "next" : "latest") ||
          !(yield* same(intent.authorization, authorization)) ||
          intent.provenance._tag !== "GitHubActionsProvenance" ||
          !(yield* same(intent.provenance.bundle, proofs.get(entry.name))) ||
          !(yield* same(intent.provenance.source, saved.identity.provenance)) ||
          intent.provenance.source.sourceCommit !== input.sourceCommit ||
          intent.provenance.source.sourceCommit !== input.applicationCommit ||
          intent.provenance.source.sourceRef !== workflowRef ||
          intent.provenance.source.eventName !== "workflow_dispatch" ||
          intent.provenance.source.runnerEnvironment !== "github-hosted"
        )
          return yield* reject("Publication destination, bytes, authentication or policy changed");
        admitted.set(entry.name, intent);
      }
      return admitted;
    }).pipe(checked("publication-policy"));
    const provider = providers.find((entry) => entry.definitionId === "npm.publish");
    // The npm provider's definitions always include its publication.
    if (provider === undefined) return yield* Effect.die("Missing npm provider");
    for (const operation of plan.operations)
      yield* provider.prepare(operation, yield* preparationContext(plan, operation));
    return { input, bundle, plan, intents, readContent };
  },
);

/** Keep read/observe credential-free and acquire npm OIDC only for an admitted package write. */
export const npmCredentials = Effect.fn("npmCredentials")(
  /**
   * @param {import("@mannyc1/ts-release/http").CredentialBinding} binding
   * @param {{ intents: ReadonlyMap<string, Npm.PublishIntent>, authorize: boolean, trusted: import("@mannyc1/ts-release/http").TrustedPublisherHost }} options
   */
  function* (binding, options) {
    const selected = yield* Npm.authorizationBinding(binding);
    const intent = options.intents.get(selected.packageName);
    yield* Effect.gen(function* () {
      if (
        intent === undefined ||
        !(yield* same(selected.authorization, authorization)) ||
        !(yield* same(selected.authorization, intent.authorization))
      )
        return yield* reject("Credential request is outside this release");
    }).pipe(checked("credential-binding"));
    if (binding.method === "GET" || binding.method === "HEAD") return {};
    if (
      !options.authorize ||
      binding.method !== "PUT" ||
      binding.bodyDigest === undefined ||
      binding.bodyDigest === ""
    )
      return yield* releaseFailure("authorization");
    return yield* Npm.authorizeTrusted(
      { authorization, packageName: selected.packageName, binding },
      options.trusted,
    );
  },
);

/** @type {import("@mannyc1/ts-release/node").CreateApplication} */
export const createApplication = (raw) =>
  Effect.gen(function* () {
    const path = yield* Path.Path;
    const executionHostCommit = yield* currentExecutionHost.pipe(checked("execution-host"));
    const verifyProvenance = yield* provenanceVerifier;
    const { input, bundle, plan, intents, readContent } = yield* loadCandidate(
      raw,
      verifyProvenance,
    );
    if (input.executionHostCommit !== executionHostCommit)
      return yield* releaseFailure("execution-host");
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
      cacheDirectory: path.resolve(".release/journal-cache"),
      // This application runs on the selected Ubuntu GitHub-hosted runner. Core
      // verifies the absolute executable and isolates Git configuration itself.
      gitExecutable: "/usr/bin/git",
      principal: "reactor-release-journal",
      scope: "npm-publication",
      timeoutMilliseconds: 30_000,
      maximumOutputBytes: 16 * 1024 * 1024,
      credentials: (coordinate) =>
        Effect.gen(function* () {
          if (
            coordinate.remote !== journalRemote ||
            coordinate.principal !== "reactor-release-journal" ||
            coordinate.scope !== "npm-publication"
          )
            return yield* releaseFailure("journal-binding");
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
  }).pipe(
    // ts-release runs the application without services of its own, so this is its entry point.
    // @effect-diagnostics-next-line strictEffectProvide:off
    Effect.provide(NodeServices.layer),
  );

/** @param {string} name */
const secret = (name) =>
  Config.Redacted(name).pipe(
    Effect.mapError(() =>
      ReleaseError.make({
        code: "reactor-release-credential",
        message: "An explicitly selected release credential is unavailable",
      }),
    ),
  );
