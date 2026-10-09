/**
 * Runtime projects discover their own files; there is no test filename registry.
 *   portable    Vitest in packages/client, packages/browser and integration/hosted, each on Node
 *               and on Bun, and the scripts' own tests on Bun; concurrent suites divide the
 *               available workers, and hosts with fewer than four cores run one suite at a time
 *   native      Vitest in packages/native against the staged library, on Node then Bun
 *   integration Node/Vitest in integration, spawning the real browser/native runner
 *
 * Each run has 600 seconds, and the first that fails ends the project with its exit status. Two
 * client suites side by side on a four-core CI runner have taken up to 198 s.
 */
import { availableParallelism } from "node:os";
import * as NodeRuntime from "@effect/platform-node/NodeRuntime";
import * as NodeServices from "@effect/platform-node/NodeServices";
import type * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";
import * as Stdio from "effect/Stdio";
import { environment, runInherited, runStep, runtimes, type Step } from "./subprocess.js";

class TestError extends Schema.TaggedError<TestError>("reactor-effect/scripts/test/TestError")(
  "TestError",
  { message: Schema.String },
) {}

const limit: Duration.Input = "600 seconds";

const program = Effect.gen(function* () {
  const path = yield* Path.Path;
  const project = (yield* (yield* Stdio.Stdio).args)[0] ?? "portable";
  const root = yield* path.fromFileUrl(new URL("../", import.meta.url));
  const { node, bun } = yield* runtimes;
  const env = yield* environment;
  const vitest = (directory: string) => path.join(directory, "node_modules/vitest/vitest.mjs");
  const packages = path.join(root, "packages");
  const integration = path.join(root, "integration");
  /** A Vitest project runs on Node, which the engines declare, and on Bun. */
  const nodeAndBun = (
    name: string,
    directory: string,
    args: ReadonlyArray<string> = [],
  ): ReadonlyArray<Step> => [
    {
      label: `${name} on Node`,
      command: node,
      args: [vitest(directory), "run", ...args],
      cwd: directory,
      env,
      limit,
    },
    {
      label: `${name} on Bun`,
      command: bun,
      args: ["--bun", vitest(directory), "run", ...args],
      cwd: directory,
      env,
      limit,
    },
  ];
  /** Runs one at a time, each with this process's terminal. */
  const inTurn = (steps: ReadonlyArray<Step>) =>
    Effect.forEach(
      steps,
      (step) =>
        runInherited(step.command, step.args, { cwd: step.cwd, env: step.env }).pipe(
          Effect.timeoutOrElse({
            duration: limit,
            orElse: () =>
              Effect.fail(
                TestError.make({
                  message: `${step.command} ${step.args.join(" ")} did not finish within 600 seconds`,
                }),
              ),
          }),
        ),
      { discard: true },
    );
  switch (project) {
    case "portable": {
      // The suites share no port or file, so they run side by side, the longest first. The first
      // to fail stops the rest.
      const steps = [
        ...nodeAndBun("client", path.join(packages, "client")),
        ...nodeAndBun("browser", path.join(packages, "browser")),
        // The hosted qualification's gates, ledger and a rehearsal of every check.
        ...nodeAndBun("hosted", integration, ["--root", "hosted"]),
        // pack runs on Bun and reads bun.lock with Bun.JSONC, so its tests run there.
        {
          label: "scripts on Bun",
          command: bun,
          args: ["--bun", vitest(root), "run", "--root", "scripts"],
          cwd: root,
          env,
          limit,
        },
      ];
      const cores = availableParallelism();
      const concurrency = cores < 4 ? 1 : Math.min(steps.length, Math.floor(cores / 2));
      const workers = String(Math.max(1, Math.floor(cores / concurrency)));
      return yield* Effect.forEach(
        steps.map((step) => ({ ...step, args: [...step.args, "--maxWorkers", workers] })),
        runStep,
        { concurrency, discard: true },
      );
    }
    case "native":
      // The media load tests measure the bridge, so nothing runs beside them.
      return yield* inTurn(nodeAndBun("native", path.join(packages, "native")));
    case "integration":
      return yield* inTurn([
        {
          label: "integration on Node",
          command: node,
          args: [vitest(integration), "run"],
          cwd: integration,
          env,
        },
      ]);
    default:
      return yield* TestError.make({
        message: `unknown test project ${project}; use portable, native or integration`,
      });
  }
});

program.pipe(
  // The script's entry point.
  // @effect-diagnostics-next-line strictEffectProvide:off
  Effect.provide(NodeServices.layer),
  NodeRuntime.runMain,
);
