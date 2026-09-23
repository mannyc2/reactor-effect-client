import { appendFileSync, writeFileSync } from "node:fs";
import { join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { Schema } from "effect";
import { CandidateIdentity, confirmationFor, readBytes, reject, validateRun } from "./model.mjs";
import { currentExecutionHost, validateExecutionHost } from "./host.mjs";

/** @param {unknown} raw @param {{ ciRun: unknown, candidateRun: unknown, ciRunId: string, candidateRunId: string, executionHostCommit: string, environment: NodeJS.ProcessEnv, mode: string, confirmation: string, candidateDirectory: string }} options */
export const makeInput = (raw, options) => {
  const executionHostCommit = validateExecutionHost(
    options.environment,
    options.executionHostCommit,
  );
  const saved = Schema.decodeUnknownSync(CandidateIdentity, { onExcessProperty: "error" })(raw);
  const run = validateRun(options.ciRun, options.ciRunId, "ci");
  const preparation = validateRun(options.candidateRun, options.candidateRunId, "release");
  if (
    saved.qualification.ciRunId !== options.ciRunId ||
    saved.qualification.ciRunAttempt !== String(run.run_attempt) ||
    saved.provenance.runId !== options.candidateRunId ||
    saved.provenance.runAttempt !== String(preparation.run_attempt) ||
    saved.qualification.sourceCommit !== run.head_sha ||
    saved.applicationCommit !== preparation.head_sha ||
    saved.provenance.sourceCommit !== preparation.head_sha ||
    preparation.head_sha !== run.head_sha
  )
    reject("Selected candidate, preparation or CI identity changed");
  if (!["publish", "observe"].includes(options.mode))
    reject("Use publish or observe for a retained candidate");
  // The operator confirms every package coordinate the candidate will publish.
  if (options.mode === "publish" && options.confirmation !== confirmationFor(saved.qualification))
    reject("Explicit package/version publication confirmation is required");
  return {
    candidateDirectory: resolve(options.candidateDirectory),
    candidateRunId: options.candidateRunId,
    bundleSha256: saved.bundleSha256,
    planId: saved.planId,
    applicationCommit: saved.applicationCommit,
    executionHostCommit,
    ciRunId: saved.qualification.ciRunId,
    sourceCommit: saved.qualification.sourceCommit,
    authorize: options.mode === "publish",
  };
};

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  const [candidateDirectory, runFile, preparationFile, destination] = process.argv.slice(2);
  if (!candidateDirectory || !runFile || !preparationFile || !destination)
    reject("usage: input.mjs candidate-dir ci-run.json candidate-run.json input.json");
  const input = makeInput(
    JSON.parse(readBytes(join(candidateDirectory, "identity.json")).toString()),
    {
      ciRun: JSON.parse(readBytes(runFile).toString()),
      candidateRun: JSON.parse(readBytes(preparationFile).toString()),
      ciRunId: process.env.CI_RUN_ID ?? "",
      candidateRunId: process.env.CANDIDATE_RUN_ID ?? "",
      executionHostCommit: currentExecutionHost(),
      environment: process.env,
      mode: process.env.RELEASE_MODE ?? "",
      confirmation: process.env.CONFIRM ?? "",
      candidateDirectory,
    },
  );
  writeFileSync(destination, JSON.stringify(input, null, 2) + "\n", { flag: "wx", mode: 0o600 });
  if (process.env.GITHUB_OUTPUT)
    appendFileSync(process.env.GITHUB_OUTPUT, `application-input=${JSON.stringify(input)}\n`);
  console.log(
    JSON.stringify({
      planId: input.planId,
      applicationCommit: input.applicationCommit,
      executionHostCommit: input.executionHostCommit,
      authorize: input.authorize,
    }),
  );
}
