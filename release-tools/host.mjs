import { execFileSync } from "node:child_process";
import { Schema } from "effect";
import { commit, reject, repository } from "./model.mjs";

/** The execution host is the exact manually dispatched main commit. It is not
 * the source or preparation identity of a retained candidate.
 * @param {NodeJS.ProcessEnv} env @param {string} checkoutCommit */
export const validateExecutionHost = (env, checkoutCommit) => {
  if (
    env.GITHUB_REPOSITORY !== repository ||
    env.GITHUB_EVENT_NAME !== "workflow_dispatch" ||
    env.GITHUB_REF !== "refs/heads/main"
  )
    reject("Release execution requires a manual run on this repository's main");
  const selected = Schema.decodeUnknownSync(commit)(env.GITHUB_SHA);
  if (checkoutCommit !== selected)
    reject("Execution checkout differs from the dispatched main commit");
  return selected;
};

export const currentExecutionHost = () =>
  validateExecutionHost(
    process.env,
    execFileSync("git", ["rev-parse", "HEAD"], { encoding: "utf8" }).trim(),
  );
