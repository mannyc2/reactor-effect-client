import { Config, Effect, FileSystem, Path, Schema, Stdio } from "effect";
import {
  Qualification,
  jsonDocument,
  qualifiedPackages,
  readBytes,
  readJson,
  reject,
  releaseVersion,
  repository,
  sha256,
  validatePackageIdentity,
  validateRun,
} from "./model.mjs";
import { checkoutCommit, currentExecutionHost, git, runCommand, variable } from "./host.mjs";

const program = Effect.gen(function* () {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const [command, file, output] = yield* (yield* Stdio.Stdio).args;
  if (file === undefined || file === "")
    return yield* reject(
      "usage: ci.mjs validate package-identity.json | select run.json | stamp package-identity.json qualification.json",
    );
  if (command === "validate") {
    yield* readJson(file).pipe(Effect.flatMap(validatePackageIdentity));
    return;
  }
  const environment = yield* Config.all({
    repository: variable("GITHUB_REPOSITORY"),
    ref: variable("GITHUB_REF"),
    output: variable("GITHUB_OUTPUT"),
  });
  if (environment.repository !== repository || environment.ref !== "refs/heads/main")
    return yield* reject("Release tooling requires this repository's main");
  /** @param {string} key @param {string} value */
  const emit = (key, value) =>
    Effect.gen(function* () {
      if (value === "" || /[\r\n]/.test(value)) return yield* reject("Invalid workflow output");
      if (environment.output !== "")
        yield* fs.writeFileString(environment.output, `${key}=${value}\n`, { flag: "a" });
    });
  if (command === "select") {
    const executionHostCommit = yield* currentExecutionHost;
    const mode = yield* variable("RELEASE_MODE");
    if (!["prepare", "publish", "observe"].includes(mode))
      return yield* reject("Invalid release mode");
    const ciRunId = yield* variable("CI_RUN_ID");
    const ci = yield* readJson(file).pipe(Effect.flatMap((raw) => validateRun(raw, ciRunId, "ci")));
    yield* emit("source-commit", ci.head_sha);
    yield* emit("execution-host-commit", executionHostCommit);
    if (mode === "prepare") {
      if (ci.head_sha !== executionHostCommit)
        return yield* reject("Prepare requires CI for the current main commit");
    } else {
      if (output === undefined || output === "")
        return yield* reject("Publish/observe requires the preparation run JSON");
      const candidateRunId = yield* variable("CANDIDATE_RUN_ID");
      const prepared = yield* readJson(output).pipe(
        Effect.flatMap((raw) => validateRun(raw, candidateRunId, "release")),
      );
      if (prepared.head_sha !== ci.head_sha)
        return yield* reject("Preparation and selected CI sources differ");
    }
  } else if (command === "stamp") {
    const eventName = yield* variable("GITHUB_EVENT_NAME");
    if (output === undefined || output === "" || !["push", "workflow_dispatch"].includes(eventName))
      return yield* reject("Only main CI can stamp a release artifact");
    const identity = yield* readJson(file).pipe(Effect.flatMap(validatePackageIdentity));
    const sourceCommit = yield* checkoutCommit;
    const sourceTree = yield* git(["rev-parse", "HEAD^{tree}"]);
    yield* git(["diff", "--exit-code", "HEAD", "--"]);
    if (sourceCommit !== (yield* variable("GITHUB_SHA"))) return yield* reject("CI source changed");
    const archives = qualifiedPackages(identity);
    for (const entry of archives) {
      const bytes = yield* readBytes(path.join(path.dirname(file), entry.tarball));
      if ((yield* sha256(bytes)) !== entry.sha256)
        return yield* reject("A qualified tarball changed");
    }
    const qualification = yield* Schema.decodeEffect(Qualification)({
      format: "reactor-qualified-ci/v2",
      repository,
      sourceCommit,
      sourceTree,
      ciRunId: yield* variable("GITHUB_RUN_ID"),
      ciRunAttempt: yield* variable("GITHUB_RUN_ATTEMPT"),
      version: releaseVersion(identity),
      packages: archives,
    });
    yield* fs.makeDirectory(path.dirname(output), { recursive: true });
    yield* fs.writeFileString(output, `${yield* jsonDocument(qualification)}\n`, { flag: "wx" });
    yield* emit("qualification", output);
  } else return yield* reject("Unknown release CI command");
});

runCommand(program);
