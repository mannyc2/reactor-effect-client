import * as NodeRuntime from "@effect/platform-node/NodeRuntime";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { Cause, Config, Effect, Logger, Schema, Stream } from "effect";
import { ChildProcess, ChildProcessSpawner } from "effect/unstable/process";
import { commit, reject, repository } from "./model.mjs";

/** An environment variable, empty when it is unset.
 * @param {string} name */
export const variable = (name) => Config.String(name).pipe(Config.withDefault(""));

/** Run git in this checkout and return its trimmed standard output.
 * @param {readonly string[]} args */
export const git = (args) =>
  Effect.gen(function* () {
    const handle = yield* ChildProcess.make("git", args, { stderr: "inherit" });
    const [output, exitCode] = yield* Effect.all(
      [handle.stdout.pipe(Stream.decodeText(), Stream.mkString), handle.exitCode],
      { concurrency: "unbounded" },
    );
    if (exitCode !== ChildProcessSpawner.ExitCode(0))
      return yield* reject(`git ${args.join(" ")} exited ${exitCode}`);
    return output.trim();
  }).pipe(Effect.scoped);

/** The commit this checkout has. */
export const checkoutCommit = git(["rev-parse", "HEAD"]);

/** The execution host is the exact manually dispatched main commit. It is not
 * the source or preparation identity of a retained candidate. */
export const validateExecutionHost = Effect.fnUntraced(
  /** @param {string} checkout */
  function* (checkout) {
    const environment = yield* Config.all({
      repository: variable("GITHUB_REPOSITORY"),
      eventName: variable("GITHUB_EVENT_NAME"),
      ref: variable("GITHUB_REF"),
      sha: variable("GITHUB_SHA"),
    });
    if (
      environment.repository !== repository ||
      environment.eventName !== "workflow_dispatch" ||
      environment.ref !== "refs/heads/main"
    )
      return yield* reject("Release execution requires a manual run on this repository's main");
    const selected = yield* Schema.decodeEffect(commit)(environment.sha);
    if (checkout !== selected)
      return yield* reject("Execution checkout differs from the dispatched main commit");
    return selected;
  },
);

export const currentExecutionHost = checkoutCommit.pipe(Effect.flatMap(validateExecutionHost));

/** Run one release command on Node. Standard output carries only what the workflow reads,
 * so a failure is reported on standard error, and the process exits 1.
 * @template A, E
 * @param {Effect.Effect<A, E, NodeServices.NodeServices>} program */
export const runCommand = (program) =>
  program.pipe(
    Effect.tapCause((cause) =>
      Cause.hasInterruptsOnly(cause) ? Effect.void : Effect.logError(cause),
    ),
    Effect.provideService(Logger.LogToStderr, true),
    // A release command is its own entry point.
    // @effect-diagnostics-next-line strictEffectProvide:off
    Effect.provide(NodeServices.layer),
    NodeRuntime.runMain({ disableErrorReporting: true }),
  );
