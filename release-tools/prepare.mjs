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
  packageName,
  principal,
  readBytes,
  reject,
  releaseFailure,
  sha256,
  validatePackageIdentity,
  validateRun,
} from "./model.mjs";

/** Adopt the qualified bytes. Preparation has no journal, HTTP client, credentials or publication authority.
 * @param {{ qualifiedDirectory: string, candidateDirectory: string, applicationCommit: string, run: unknown, ciRunId: string }} options */
export const prepareCandidate = (options) =>
  Effect.gen(function* () {
    const data = yield* checked("qualification", () => {
      const run = validateRun(options.run, options.ciRunId, "ci");
      Schema.decodeUnknownSync(commit)(options.applicationCommit);
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
        qualification.version !== identity.version ||
        qualification.sha256 !== identity.sha256
      )
        reject("Qualified archive is not the selected successful CI input");
      const bytes = readBytes(join(options.qualifiedDirectory, identity.tarball));
      if (sha256(bytes) !== identity.sha256) reject("Qualified archive bytes changed");
      return { identity, identityBytes, qualification, qualificationBytes, bytes };
    });
    const candidateDirectory = resolve(options.candidateDirectory);
    yield* checked("destination", () =>
      mkdirSync(candidateDirectory, { recursive: false, mode: 0o700 }),
    );
    const owner = fileContentOwner(join(candidateDirectory, "content"));
    /** @type {File[]} */
    const files = [];
    /** @type {Array<[string, Uint8Array]>} */
    const sources = [
      [data.identity.tarball, data.bytes],
      ["package-identity.json", data.identityBytes],
      ["qualification.json", data.qualificationBytes],
    ];
    for (const [logicalName, bytes] of sources) {
      files.push(
        new File({
          logicalName,
          content: yield* owner.putOwned(bytes),
          deliveryMode: 0o644,
          executable: null,
          producedBy: { name: "reactor-qualified-ci", version: "1" },
        }),
      );
    }
    const bundle = yield* finalize(files);
    const tarball = files[0];
    if (!tarball) return yield* checked("archive", () => reject("Missing tarball"));
    /** @type {import("@mannyc1/ts-release/bundle").ArtifactAccess} */
    const access = {
      bundle,
      readContent: (content) =>
        owner.read(content).pipe(Effect.mapError(() => releaseFailure("content"))),
    };
    const metadata = yield* Npm.inspectTarball(tarball, access);
    yield* checked("npm-metadata", () => {
      if (
        metadata.private ||
        metadata.name !== packageName ||
        metadata.version !== data.identity.version
      )
        reject("Native npm metadata differs from the qualified package");
    });
    const operation = yield* Npm.publish(
      new Npm.PublishIntent({
        registry: "https://registry.npmjs.org/",
        name: packageName,
        version: metadata.version,
        tarball,
        integrity: metadata.integrity,
        shasum: metadata.shasum,
        initialTag: metadata.version.includes("-") ? "next" : "latest",
        access: "public",
        authorization: new Npm.TokenAuthorization({ principal }),
        // The source repository is private and this is the initial-publication path.
        provenance: new Npm.NoProvenance({}),
      }),
    );
    const bundleBytes = encodeBundle(bundle);
    const bundleSha256 = sha256(bundleBytes);
    const plan = yield* createPlan(bundleSha256, [operation], journalId(metadata.version));
    const noRead = () =>
      checked("preparation-network", () => reject("Preparation cannot contact a registry"));
    const providers = Npm.definitions({ ...access, read: noRead });
    yield* loadPlan(plan, providers);
    const provider =
      providers.find((entry) => entry.definitionId === "npm.publish") ??
      reject("Missing npm provider");
    // Exercise the provider's real native encoder, including publishConfig policy,
    // without acquiring credentials, retaining a dispatch permit or sending bytes.
    yield* provider.prepare(operation, {
      own: { operation, receipts: [], observations: [] },
      dependencies: [],
    });
    const identity = Schema.decodeUnknownSync(CandidateIdentity)({
      format: "reactor-ts-release/v1",
      applicationCommit: options.applicationCommit,
      bundleSha256,
      planId: plan.planId,
      qualification: data.qualification,
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
  const result = await Effect.runPromise(
    prepareCandidate({
      qualifiedDirectory,
      candidateDirectory,
      applicationCommit: process.env.GITHUB_SHA ?? "",
      ciRunId: process.env.CI_RUN_ID ?? "",
      run: JSON.parse(readBytes(runFile).toString()),
    }),
  );
  console.log(JSON.stringify(result, null, 2));
}
