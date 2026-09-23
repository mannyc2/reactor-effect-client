import { appendFileSync, mkdirSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { execFileSync } from "node:child_process";
import { Schema } from "effect";
import {
  Qualification,
  readBytes,
  reject,
  repository,
  sha256,
  validatePackageIdentity,
  validateRun,
} from "./model.mjs";

const [command, file, output] = process.argv.slice(2);
if (!file) reject("usage: ci.mjs select run.json | stamp package-identity.json qualification.json");
const env = process.env;
if (env.GITHUB_REPOSITORY !== repository || env.GITHUB_REF !== "refs/heads/main")
  reject("Release tooling requires this repository's main");
/** @param {string} key @param {string | undefined} value */
const emit = (key, value) => {
  if (!value || /[\r\n]/.test(value)) reject("Invalid workflow output");
  if (env.GITHUB_OUTPUT) appendFileSync(env.GITHUB_OUTPUT, `${key}=${value}\n`);
};
if (command === "select") {
  if (env.GITHUB_EVENT_NAME !== "workflow_dispatch") reject("Release must be manually dispatched");
  const mode = env.RELEASE_MODE;
  if (!["prepare", "publish", "observe"].includes(mode ?? "")) reject("Invalid release mode");
  const ci = validateRun(JSON.parse(readBytes(file).toString()), env.CI_RUN_ID ?? "", "ci");
  emit("source-commit", ci.head_sha);
  if (mode === "prepare") {
    if (ci.head_sha !== env.GITHUB_SHA) reject("Prepare requires CI for the current main commit");
    emit("application-commit", env.GITHUB_SHA);
  } else {
    if (!output) reject("Publish/observe requires the preparation run JSON");
    const prepared = validateRun(
      JSON.parse(readBytes(output).toString()),
      env.CANDIDATE_RUN_ID ?? "",
      "release",
    );
    emit("application-commit", prepared.head_sha);
  }
} else if (command === "stamp") {
  if (!output || !["push", "workflow_dispatch"].includes(env.GITHUB_EVENT_NAME ?? ""))
    reject("Only main CI can stamp a release artifact");
  const identity = validatePackageIdentity(JSON.parse(readBytes(file).toString()));
  const sourceCommit = execFileSync("git", ["rev-parse", "HEAD"], { encoding: "utf8" }).trim();
  const sourceTree = execFileSync("git", ["rev-parse", "HEAD^{tree}"], { encoding: "utf8" }).trim();
  execFileSync("git", ["diff", "--exit-code", "HEAD", "--"], { stdio: "pipe" });
  if (
    sourceCommit !== env.GITHUB_SHA ||
    sha256(readBytes(join(dirname(file), identity.tarball))) !== identity.sha256
  )
    reject("CI source or qualified tarball changed");
  const qualification = Schema.decodeUnknownSync(Qualification)({
    format: "reactor-qualified-ci/v1",
    repository,
    sourceCommit,
    sourceTree,
    ciRunId: env.GITHUB_RUN_ID,
    ciRunAttempt: env.GITHUB_RUN_ATTEMPT,
    name: identity.name,
    version: identity.version,
    sha256: identity.sha256,
  });
  mkdirSync(dirname(output), { recursive: true });
  writeFileSync(output, JSON.stringify(qualification, null, 2) + "\n", { flag: "wx" });
  emit("qualification", output);
} else reject("Unknown release CI command");
