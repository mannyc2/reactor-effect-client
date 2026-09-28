import { Console, Effect, FileSystem, Path, Schema, Stdio } from "effect";
import {
  CandidateIdentity,
  confirmationFor,
  jsonDocument,
  jsonLine,
  readJson,
  reject,
  validateRun,
} from "./model.mjs";
import { currentExecutionHost, runCommand, validateExecutionHost, variable } from "./host.mjs";

/** The application input for one retained candidate, bound to the operator's selection. */
export const makeInput = Effect.fnUntraced(
  /** @param {unknown} raw @param {{ ciRun: unknown, candidateRun: unknown, ciRunId: string, candidateRunId: string, executionHostCommit: string, mode: string, confirmation: string, candidateDirectory: string }} options */
  function* (raw, options) {
    const executionHostCommit = yield* validateExecutionHost(options.executionHostCommit);
    const saved = yield* Schema.decodeUnknownEffect(CandidateIdentity, {
      onExcessProperty: "error",
    })(raw);
    const run = yield* validateRun(options.ciRun, options.ciRunId, "ci");
    const preparation = yield* validateRun(options.candidateRun, options.candidateRunId, "release");
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
      return yield* reject("Selected candidate, preparation or CI identity changed");
    if (!["publish", "observe"].includes(options.mode))
      return yield* reject("Use publish or observe for a retained candidate");
    // The operator confirms every package coordinate the candidate will publish.
    if (options.mode === "publish" && options.confirmation !== confirmationFor(saved.qualification))
      return yield* reject("Explicit package/version publication confirmation is required");
    const path = yield* Path.Path;
    return {
      candidateDirectory: path.resolve(options.candidateDirectory),
      candidateRunId: options.candidateRunId,
      bundleSha256: saved.bundleSha256,
      planId: saved.planId,
      applicationCommit: saved.applicationCommit,
      executionHostCommit,
      ciRunId: saved.qualification.ciRunId,
      sourceCommit: saved.qualification.sourceCommit,
      authorize: options.mode === "publish",
    };
  },
);

if (import.meta.main)
  runCommand(
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const [candidateDirectory, runFile, preparationFile, destination] =
        yield* (yield* Stdio.Stdio).args;
      if (
        candidateDirectory === undefined ||
        candidateDirectory === "" ||
        runFile === undefined ||
        runFile === "" ||
        preparationFile === undefined ||
        preparationFile === "" ||
        destination === undefined ||
        destination === ""
      )
        return yield* reject(
          "usage: input.mjs candidate-dir ci-run.json candidate-run.json input.json",
        );
      const input = yield* makeInput(
        yield* readJson(path.join(candidateDirectory, "identity.json")),
        {
          ciRun: yield* readJson(runFile),
          candidateRun: yield* readJson(preparationFile),
          ciRunId: yield* variable("CI_RUN_ID"),
          candidateRunId: yield* variable("CANDIDATE_RUN_ID"),
          executionHostCommit: yield* currentExecutionHost,
          mode: yield* variable("RELEASE_MODE"),
          confirmation: yield* variable("CONFIRM"),
          candidateDirectory,
        },
      );
      yield* fs.writeFileString(destination, `${yield* jsonDocument(input)}\n`, {
        flag: "wx",
        mode: 0o600,
      });
      const output = yield* variable("GITHUB_OUTPUT");
      if (output !== "")
        yield* fs.writeFileString(output, `application-input=${yield* jsonLine(input)}\n`, {
          flag: "a",
        });
      yield* Console.log(
        yield* jsonLine({
          planId: input.planId,
          applicationCommit: input.applicationCommit,
          executionHostCommit: input.executionHostCommit,
          authorize: input.authorize,
        }),
      );
    }),
  );
