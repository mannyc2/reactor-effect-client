/**
 * Runs a verification profile's stages in order, and each stage's workspace commands side by side;
 * the first that fails ends the profile with its exit status. `--list` prints the stages without
 * running them.
 */
import * as NodeRuntime from "@effect/platform-node/NodeRuntime";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as Console from "effect/Console";
import * as Effect from "effect/Effect";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";
import * as Stdio from "effect/Stdio";
import { environment, runInherited, runSteps, runtimes, type Step } from "./subprocess.js";

class VerifyError extends Schema.TaggedError<VerifyError>(
  "reactor-effect/scripts/verify/VerifyError",
)("VerifyError", { message: Schema.String }) {}

// CI/release and local callers share exactly these workspace commands. Runtime
// test discovery lives in bunfig/Vitest projects, not in this orchestration.
// A stage's commands run side by side, and a stage starts once the one before it
// has passed. `build` comes first because dependent packages and examples
// resolve their workspace dependencies through the built declarations, and
// type-aware lint rules need those types too. `imports` is the Node and Bun
// portable runtime import guards, which load that build.
type Stages = ReadonlyArray<ReadonlyArray<string>>;
const portable: Stages = [
  ["generate:check", "format:check", "build"],
  ["imports", "lint", "typecheck", "check:examples", "test:portable"],
];
// The portable checks whose outcome can't depend on the host, which CI runs
// once; `runtime` runs the rest on each host.
const shared: Stages = [
  ["generate:check", "format:check", "build"],
  ["lint", "typecheck", "check:examples"],
];
// Native and integration tests import the built client package, and nothing
// runs beside them: the media load tests measure the bridge, not contention.
const native: Stages = [
  ["build"],
  ["imports"],
  ["native:build"],
  ["native:test"],
  ["test:integration"],
];
const runtime: Stages = [["build"], ["imports", "test:portable"]];
const packaging: Stages = [["test:pack"]];
const profiles: Readonly<Record<string, Stages>> = {
  portable,
  shared,
  runtime,
  native,
  package: packaging,
  release: [...portable, ...packaging],
  full: [...portable, ...native, ...packaging],
};

const program = Effect.gen(function* () {
  const path = yield* Path.Path;
  const args = yield* (yield* Stdio.Stdio).args;
  const profileAt = args.indexOf("--profile");
  const profile = profileAt < 0 ? "full" : args[profileAt + 1];
  const listOnly = args.includes("--list");
  for (let index = 0; index < args.length; index++) {
    const arg = args[index];
    if (arg === "--list") continue;
    if (arg === "--profile") {
      index++;
      continue;
    }
    return yield* VerifyError.make({ message: `unknown verification argument: ${arg}` });
  }
  const stages = profile === undefined ? undefined : profiles[profile];
  if (stages === undefined)
    return yield* VerifyError.make({
      message: `unknown verification profile ${profile}; use ${Object.keys(profiles).join(", ")}`,
    });
  const root = yield* path.fromFileUrl(new URL("../", import.meta.url));
  const { node, bun } = yield* runtimes;
  const env = { ...(yield* environment), BUN_BINARY: bun };
  const stepsOf = (command: string): ReadonlyArray<Step> =>
    command === "imports"
      ? [
          {
            label: "Node portable runtime imports",
            command: node,
            args: [
              "--experimental-loader",
              "./scripts/pack/resolution-guard.mjs",
              "scripts/portable-import.mjs",
            ],
            cwd: root,
            env: { ...env, PACK_CONSUMER_ROOT: root, PACK_DENY_NATIVE: "1" },
          },
          {
            label: "Bun portable runtime imports",
            command: bun,
            args: ["--no-env-file", "scripts/portable-import.mjs"],
            cwd: root,
            env,
          },
        ]
      : [
          {
            label: `bun run ${command}`,
            command: bun,
            args: ["--no-env-file", "run", command],
            cwd: root,
            env,
          },
        ];
  for (const stage of stages) {
    yield* Console.log(`verify ${profile}: ${stage.join(", ")}`);
    if (listOnly) continue;
    const steps = stage.flatMap(stepsOf);
    const [only] = steps;
    // A stage of one command keeps this terminal, so its output streams live.
    if (steps.length === 1 && only !== undefined)
      yield* runInherited(only.command, only.args, { cwd: only.cwd, env: only.env });
    else yield* runSteps(steps);
  }
});

program.pipe(
  // The script's entry point.
  // @effect-diagnostics-next-line strictEffectProvide:off
  Effect.provide(NodeServices.layer),
  NodeRuntime.runMain,
);
