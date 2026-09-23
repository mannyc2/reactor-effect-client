import { mkdirSync, writeFileSync } from "node:fs";
import { join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { Effect, Schema } from "effect";
import { createPlan, loadPlan } from "@mannyc1/ts-release";
import { File, encodeBundle, finalize } from "@mannyc1/ts-release/bundle";
import { fileContentOwner } from "@mannyc1/ts-release/node";
import * as Npm from "@mannyc1/ts-release-npm";
import {
  CandidateIdentity,
  Qualification,
  checked,
  commit,
  journalId,
  packages,
  preparationContext,
  qualificationMatches,
  qualifiedPackages,
  readBytes,
  reject,
  releaseFailure,
  sha256,
  validatePackageIdentity,
  validateRun,
} from "./model.mjs";
import {
  authorization,
  makeAttester,
  sourceFromEnvironment,
  verifyProvenance,
  workflow,
  workflowRef,
} from "./provenance.mjs";

/** Adopt the qualified archives and retain signed provenance for each before npm publication.
 * @param {{ qualifiedDirectory: string, candidateDirectory: string, applicationCommit: string, run: unknown, ciRunId: string, source: Npm.ProvenanceSource }} options
 * @param {{ attest: Npm.Attest, verifyProvenance: Npm.VerifyProvenance }} signing */
export const prepareCandidate = (options, signing) =>
  Effect.gen(function* () {
    const data = yield* checked("qualification", () => {
      const run = validateRun(options.run, options.ciRunId, "ci");
      Schema.decodeUnknownSync(commit)(options.applicationCommit);
      const source = Schema.decodeUnknownSync(Npm.ProvenanceSource)(options.source);
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
        reject("Provenance must sign the qualified main commit from this release workflow");
      const identityBytes = readBytes(join(options.qualifiedDirectory, "package-identity.json"));
      const identity = validatePackageIdentity(JSON.parse(identityBytes.toString()));
      const qualificationBytes = readBytes(join(options.qualifiedDirectory, "qualification.json"));
      const qualification = Schema.decodeUnknownSync(Qualification, { onExcessProperty: "error" })(
        JSON.parse(qualificationBytes.toString()),
      );
      if (
        qualification.sourceCommit !== run.head_sha ||
        qualification.ciRunId !== String(run.id) ||
        qualification.ciRunAttempt !== String(run.run_attempt) ||
        !qualificationMatches(qualification, identity)
      )
        reject("Qualified archives are not the selected successful CI input");
      const archives = qualifiedPackages(identity).map((entry) => {
        const bytes = readBytes(join(options.qualifiedDirectory, entry.tarball));
        if (sha256(bytes) !== entry.sha256) reject("Qualified archive bytes changed");
        return { ...entry, bytes };
      });
      return { identityBytes, qualification, qualificationBytes, archives, source };
    });
    const candidateDirectory = resolve(options.candidateDirectory);
    yield* checked("destination", () =>
      mkdirSync(candidateDirectory, { recursive: false, mode: 0o700 }),
    );
    const owner = fileContentOwner(join(candidateDirectory, "content"));
    /** @param {string} logicalName @param {Uint8Array} bytes @param {string} producer */
    const retainFile = (logicalName, bytes, producer) =>
      owner.putOwned(bytes).pipe(
        Effect.map(
          (content) =>
            new File({
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
      readContent: (content) =>
        owner.read(content).pipe(Effect.mapError(() => releaseFailure("content"))),
    };
    /** @type {Map<string, Npm.PackageMetadata>} */
    const metadata = new Map();
    for (const [name, file] of archiveFiles) {
      const inspected = yield* Npm.inspectTarball(file, access);
      yield* checked("npm-metadata", () => {
        if (
          inspected.private ||
          inspected.name !== name ||
          inspected.version !== data.qualification.version
        )
          reject("Native npm metadata differs from the qualified package");
      });
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
        return yield* checked("archive", () => reject(`Missing archive for ${entry.name}`));
      const dependsOn = entry.dependsOn.map(
        (name) => operations.get(name)?.operationId ?? reject(`Unprepared dependency ${name}`),
      );
      const operation = yield* Npm.publish(
        new Npm.PublishIntent({
          registry: "https://registry.npmjs.org/",
          name: entry.name,
          version: inspected.version,
          tarball: file,
          integrity: inspected.integrity,
          shasum: inspected.shasum,
          initialTag: inspected.version.includes("-") ? "next" : "latest",
          access: "public",
          authorization,
          provenance: new Npm.GitHubActionsProvenance({
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
    const bundleSha256 = sha256(bundleBytes);
    const plan = yield* createPlan(
      bundleSha256,
      [...operations.values()],
      journalId(data.qualification.version),
    );
    const noRead = () =>
      checked("preparation-network", () => reject("Preparation cannot contact a registry"));
    const providers = Npm.definitions({
      ...access,
      bundle,
      read: noRead,
      verifyProvenance: signing.verifyProvenance,
    });
    const loaded = yield* loadPlan(plan, providers);
    const provider =
      providers.find((entry) => entry.definitionId === "npm.publish") ??
      reject("Missing npm provider");
    // Exercise the provider's real native encoder for every publication, including
    // publishConfig policy, without acquiring credentials, retaining a dispatch permit
    // or sending bytes.
    for (const operation of loaded.operations)
      yield* provider.prepare(operation, preparationContext(loaded, operation));
    const identity = Schema.decodeUnknownSync(CandidateIdentity)({
      format: "reactor-ts-release/v3",
      applicationCommit: options.applicationCommit,
      bundleSha256,
      planId: plan.planId,
      qualification: data.qualification,
      provenance: data.source,
    });
    yield* checked("retain", () => {
      /** @type {Array<[string, Uint8Array | string]>} */
      const retained = [
        ["bundle.json", bundleBytes],
        ["plan.json", JSON.stringify(plan, null, 2) + "\n"],
        ["identity.json", JSON.stringify(identity, null, 2) + "\n"],
      ];
      for (const [name, bytes] of retained)
        writeFileSync(join(candidateDirectory, name), bytes, { flag: "wx", mode: 0o600 });
    });
    return identity;
  });

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  const [qualifiedDirectory, candidateDirectory, runFile] = process.argv.slice(2);
  if (!qualifiedDirectory || !candidateDirectory || !runFile)
    reject("usage: prepare.mjs qualified-dir new-candidate-dir ci-run.json");
  const source = sourceFromEnvironment(process.env);
  const attest = await Effect.runPromise(makeAttester(source));
  const result = await Effect.runPromise(
    prepareCandidate(
      {
        qualifiedDirectory,
        candidateDirectory,
        applicationCommit: process.env.GITHUB_SHA ?? "",
        ciRunId: process.env.CI_RUN_ID ?? "",
        run: JSON.parse(readBytes(runFile).toString()),
        source,
      },
      { attest, verifyProvenance },
    ),
  );
  console.log(JSON.stringify(result, null, 2));
}
