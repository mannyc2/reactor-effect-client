import { Console, Effect, FileSystem, Path, Schema, Stdio } from "effect";
import { createPlan, loadPlan } from "@mannyc1/ts-release";
import { File, encodeBundle, finalize } from "@mannyc1/ts-release/bundle";
import { fileContentOwner } from "@mannyc1/ts-release/node";
import * as Npm from "@mannyc1/ts-release-npm";
import {
  CandidateIdentity,
  Qualification,
  checked,
  commit,
  decodeJson,
  journalId,
  jsonDocument,
  packages,
  preparationContext,
  qualificationMatches,
  qualifiedPackages,
  readBytes,
  readJson,
  reject,
  releaseFailure,
  sha256,
  validatePackageIdentity,
  validateRun,
} from "./model.mjs";
import {
  authorization,
  makeAttester,
  provenanceVerifier,
  sourceFromEnvironment,
  workflow,
  workflowRef,
} from "./provenance.mjs";
import { runCommand, variable } from "./host.mjs";

/** Adopt the qualified archives and retain signed provenance for each before npm publication. */
export const prepareCandidate = Effect.fn("prepareCandidate")(
  /**
   * @param {{ qualifiedDirectory: string, candidateDirectory: string, applicationCommit: string, run: unknown, ciRunId: string, source: Npm.ProvenanceSource }} options
   * @param {{ attest: Npm.Attest, verifyProvenance: Npm.VerifyProvenance }} signing
   */
  function* (options, signing) {
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const data = yield* Effect.gen(function* () {
      const run = yield* validateRun(options.run, options.ciRunId, "ci");
      yield* Schema.decodeEffect(commit)(options.applicationCommit);
      const source = yield* Schema.decodeEffect(Npm.ProvenanceSource)(options.source);
      if (
        options.applicationCommit !== run.head_sha ||
        source.sourceCommit !== run.head_sha ||
        source.repository !== authorization.repository ||
        source.workflow !== workflow ||
        source.workflowRef !== workflowRef ||
        source.sourceRef !== workflowRef ||
        source.eventName !== "workflow_dispatch" ||
        source.runnerEnvironment !== "github-hosted"
      )
        return yield* reject(
          "Provenance must sign the qualified main commit from this release workflow",
        );
      const identityBytes = yield* readBytes(
        path.join(options.qualifiedDirectory, "package-identity.json"),
      );
      const identity = yield* decodeJson(identityBytes.toString()).pipe(
        Effect.flatMap(validatePackageIdentity),
      );
      const qualificationBytes = yield* readBytes(
        path.join(options.qualifiedDirectory, "qualification.json"),
      );
      const qualification = yield* decodeJson(qualificationBytes.toString()).pipe(
        Effect.flatMap(Schema.decodeUnknownEffect(Qualification, { onExcessProperty: "error" })),
      );
      if (
        qualification.sourceCommit !== run.head_sha ||
        qualification.ciRunId !== String(run.id) ||
        qualification.ciRunAttempt !== String(run.run_attempt) ||
        !qualificationMatches({ qualification, identity })
      )
        return yield* reject("Qualified archives are not the selected successful CI input");
      const archives = [];
      for (const entry of qualifiedPackages(identity)) {
        const bytes = yield* readBytes(path.join(options.qualifiedDirectory, entry.tarball));
        if ((yield* sha256(bytes)) !== entry.sha256)
          return yield* reject("Qualified archive bytes changed");
        archives.push({ ...entry, bytes });
      }
      return { identityBytes, qualification, qualificationBytes, archives, source };
    }).pipe(checked("qualification"));
    const candidateDirectory = path.resolve(options.candidateDirectory);
    yield* fs
      .makeDirectory(candidateDirectory, { recursive: false, mode: 0o700 })
      .pipe(checked("destination"));
    const owner = fileContentOwner(path.join(candidateDirectory, "content"));
    /** @param {string} logicalName @param {Uint8Array} bytes @param {string} producer */
    const retainFile = (logicalName, bytes, producer) =>
      owner.putOwned(bytes).pipe(
        Effect.map((content) =>
          File.make({
            logicalName,
            content,
            deliveryMode: 0o644,
            executable: null,
            producedBy: { name: producer, version: "1" },
          }),
        ),
      );
    /** @type {File[]} */
    const files = [];
    /** @type {Map<string, File>} */
    const archiveFiles = new Map();
    for (const archive of data.archives) {
      const file = yield* retainFile(archive.tarball, archive.bytes, "reactor-qualified-ci");
      files.push(file);
      archiveFiles.set(archive.name, file);
    }
    files.push(
      yield* retainFile("package-identity.json", data.identityBytes, "reactor-qualified-ci"),
      yield* retainFile("qualification.json", data.qualificationBytes, "reactor-qualified-ci"),
    );
    let bundle = yield* finalize(files);
    /** @type {import("@mannyc1/ts-release/bundle").ArtifactAccess} */
    const access = {
      bundle,
      readContent: (content) => owner.read(content).pipe(checked("content")),
    };
    /** @type {Map<string, Npm.PackageMetadata>} */
    const metadata = new Map();
    for (const [name, file] of archiveFiles) {
      const inspected = yield* Npm.inspectTarball(file, access);
      if (
        inspected.private ||
        inspected.name !== name ||
        inspected.version !== data.qualification.version
      )
        return yield* releaseFailure("npm-metadata");
      metadata.set(name, inspected);
    }
    /** @type {Map<string, { file: File, mediaType: "application/vnd.dev.sigstore.bundle.v0.3+json" }>} */
    const provenance = new Map();
    for (const [name, file] of archiveFiles) {
      const attestation = yield* Npm.createProvenance(
        {
          authorize: true,
          name,
          version: data.qualification.version,
          tarball: file,
          source: data.source,
        },
        { ...access, attest: signing.attest },
      );
      const provenanceFile = yield* retainFile(
        `${file.logicalName}.sigstore.json`,
        attestation.bytes,
        "reactor-release-provenance",
      );
      files.push(provenanceFile);
      provenance.set(name, { file: provenanceFile, mediaType: attestation.mediaType });
    }
    bundle = yield* finalize(files);
    // One operation per package; each host publication depends on the client's.
    /** @type {Map<string, import("@mannyc1/ts-release").Operation>} */
    const operations = new Map();
    for (const entry of packages) {
      const file = archiveFiles.get(entry.name);
      const inspected = metadata.get(entry.name);
      const proof = provenance.get(entry.name);
      if (file === undefined || inspected === undefined || proof === undefined)
        return yield* releaseFailure("archive");
      const dependsOn = [];
      for (const name of entry.dependsOn) {
        const dependency = operations.get(name);
        // Publication order prepares every dependency first.
        if (dependency === undefined) return yield* Effect.die(`Unprepared dependency ${name}`);
        dependsOn.push(dependency.operationId);
      }
      const operation = yield* Npm.publish(
        Npm.PublishIntent.make({
          registry: "https://registry.npmjs.org/",
          name: entry.name,
          version: inspected.version,
          tarball: file,
          integrity: inspected.integrity,
          shasum: inspected.shasum,
          initialTag: inspected.version.includes("-") ? "next" : "latest",
          access: "public",
          authorization,
          provenance: Npm.GitHubActionsProvenance.make({
            source: data.source,
            bundle: proof.file,
            mediaType: proof.mediaType,
          }),
        }),
        dependsOn,
      );
      operations.set(entry.name, operation);
    }
    const bundleBytes = encodeBundle(bundle);
    const bundleSha256 = yield* sha256(bundleBytes);
    const plan = yield* createPlan(
      bundleSha256,
      [...operations.values()],
      journalId(data.qualification.version),
    );
    const noRead = () => Effect.fail(releaseFailure("preparation-network"));
    const providers = Npm.definitions({
      ...access,
      bundle,
      read: noRead,
      verifyProvenance: signing.verifyProvenance,
    });
    const loaded = yield* loadPlan(plan, providers);
    const provider = providers.find((entry) => entry.definitionId === "npm.publish");
    // The npm provider's definitions always include its publication.
    if (provider === undefined) return yield* Effect.die("Missing npm provider");
    // Exercise the provider's real native encoder for every publication, including
    // publishConfig policy, without acquiring credentials, retaining a dispatch permit
    // or sending bytes.
    for (const operation of loaded.operations)
      yield* provider.prepare(operation, yield* preparationContext(loaded, operation));
    const identity = yield* Schema.decodeEffect(CandidateIdentity)({
      format: "reactor-ts-release/v3",
      applicationCommit: options.applicationCommit,
      bundleSha256,
      planId: plan.planId,
      qualification: data.qualification,
      provenance: data.source,
    }).pipe(checked("identity"));
    yield* Effect.gen(function* () {
      const planJson = yield* jsonDocument(plan);
      const identityJson = yield* jsonDocument(identity);
      /** @type {Array<[string, Uint8Array]>} */
      const retained = [
        ["bundle.json", bundleBytes],
        ["plan.json", new TextEncoder().encode(`${planJson}\n`)],
        ["identity.json", new TextEncoder().encode(`${identityJson}\n`)],
      ];
      for (const [name, bytes] of retained)
        yield* fs.writeFile(path.join(candidateDirectory, name), bytes, {
          flag: "wx",
          mode: 0o600,
        });
    }).pipe(checked("retain"));
    return identity;
  },
);

if (import.meta.main)
  runCommand(
    Effect.gen(function* () {
      const [qualifiedDirectory, candidateDirectory, runFile] = yield* (yield* Stdio.Stdio).args;
      if (
        qualifiedDirectory === undefined ||
        qualifiedDirectory === "" ||
        candidateDirectory === undefined ||
        candidateDirectory === "" ||
        runFile === undefined ||
        runFile === ""
      )
        return yield* reject("usage: prepare.mjs qualified-dir new-candidate-dir ci-run.json");
      const source = yield* sourceFromEnvironment;
      const attest = yield* makeAttester(source);
      const verifyProvenance = yield* provenanceVerifier;
      const identity = yield* prepareCandidate(
        {
          qualifiedDirectory,
          candidateDirectory,
          applicationCommit: yield* variable("GITHUB_SHA"),
          ciRunId: yield* variable("CI_RUN_ID"),
          run: yield* readJson(runFile),
          source,
        },
        { attest, verifyProvenance },
      );
      // The workflow keeps standard output as the preparation report.
      yield* Console.log(yield* jsonDocument(identity));
    }),
  );
