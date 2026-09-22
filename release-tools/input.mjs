import { appendFileSync, writeFileSync } from "node:fs";
import { join, resolve } from "node:path";
import { execFileSync } from "node:child_process";
import { fileURLToPath } from "node:url";
import { Schema } from "effect";
import { CandidateIdentity, readBytes, reject, repository, validateRun } from "./model.mjs";

/** @param {unknown} raw @param {{ ciRun: unknown, ciRunId: string, applicationCommit: string, mode: string, confirmation: string, candidateDirectory: string }} options */
export const makeInput = (raw, options) => {
  const saved = Schema.decodeUnknownSync(CandidateIdentity, { onExcessProperty: "error" })(raw);
  const run = validateRun(options.ciRun, options.ciRunId, "ci");
  if (
    saved.qualification.ciRunId !== options.ciRunId ||
    saved.qualification.sourceCommit !== run.head_sha ||
    saved.applicationCommit !== options.applicationCommit
  )
    reject("Selected candidate or release application changed");
  if (!["publish", "observe"].includes(options.mode))
    reject("Use publish or observe for a retained candidate");
  if (
    options.mode === "publish" &&
    options.confirmation !== `publish ${saved.qualification.name}@${saved.qualification.version}`
  )
    reject("Explicit package/version publication confirmation is required");
  return {
    candidateDirectory: resolve(options.candidateDirectory),
    bundleSha256: saved.bundleSha256,
    planId: saved.planId,
    applicationCommit: saved.applicationCommit,
    ciRunId: saved.qualification.ciRunId,
    sourceCommit: saved.qualification.sourceCommit,
    authorize: options.mode === "publish",
  };
};

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  const [candidateDirectory, runFile, destination] = process.argv.slice(2);
  if (!candidateDirectory || !runFile || !destination)
    reject("usage: input.mjs candidate-dir ci-run.json input.json");
  if (
    process.env.GITHUB_REPOSITORY !== repository ||
    process.env.GITHUB_EVENT_NAME !== "workflow_dispatch" ||
    process.env.GITHUB_REF !== "refs/heads/main"
  )
    reject("Publication is restricted to manual runs on this repository's main");
  const input = makeInput(
    JSON.parse(readBytes(join(candidateDirectory, "identity.json")).toString()),
    {
      ciRun: JSON.parse(readBytes(runFile).toString()),
      ciRunId: process.env.CI_RUN_ID ?? "",
      applicationCommit: execFileSync("git", ["rev-parse", "HEAD"], { encoding: "utf8" }).trim(),
      mode: process.env.RELEASE_MODE ?? "",
      confirmation: process.env.CONFIRM ?? "",
      candidateDirectory,
    },
  );
  if (input.authorize && !process.env.NPM_TOKEN)
    reject("Configure NPM_TOKEN before requesting publication");
  writeFileSync(destination, JSON.stringify(input, null, 2) + "\n", { flag: "wx", mode: 0o600 });
  if (process.env.GITHUB_OUTPUT)
    appendFileSync(process.env.GITHUB_OUTPUT, `application-input=${JSON.stringify(input)}\n`);
  console.log(JSON.stringify({ planId: input.planId, authorize: input.authorize }));
}
