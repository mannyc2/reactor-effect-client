/**
 * Runs a verification profile's workspace commands in order; the first that fails ends the
 * profile with its exit status. `--list` prints the commands without running them.
 */
import * as NodeRuntime from "@effect/platform-node/NodeRuntime";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as Console from "effect/Console";
import * as Effect from "effect/Effect";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";
import * as Stdio from "effect/Stdio";
import { environment, runInherited, runtimes } from "./subprocess.js";

class VerifyError extends Schema.TaggedError<VerifyError>(
  "reactor-effect/scripts/verify/VerifyError",
)("VerifyError", { message: Schema.String }) {}

// CI/release and local callers share exactly these workspace commands. Runtime
// test discovery lives in bunfig/Vitest projects, not in this orchestration.
// `build` precedes `lint` and `typecheck` because dependent packages and
// examples resolve their workspace dependencies through the built declarations,
// and type-aware lint rules need those types too.
const portable = [
  "generate:check",
  "format:check",
  "build",
  "lint",
  "typecheck",
  "check:examples",
  "test:portable",
];
// Native and integration tests import the built client package.
const native = ["build", "native:build", "native:test", "test:integration"];
const runtime = ["build", "test:portable"];
const packaging = ["test:pack"];
const profiles: Readonly<Record<string, ReadonlyArray<string>>> = {
  portable,
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
  const commands = profile === undefined ? undefined : profiles[profile];
  if (commands === undefined)
    return yield* VerifyError.make({
      message: `unknown verification profile ${profile}; use ${Object.keys(profiles).join(", ")}`,
    });
  const root = yield* path.fromFileUrl(new URL("../", import.meta.url));
  const { node, bun } = yield* runtimes;
  const env = { ...(yield* environment), BUN_BINARY: bun };
  const execute = (
    command: string,
    commandArgs: ReadonlyArray<string>,
    overrides: Readonly<Record<string, string>> = {},
  ) =>
    listOnly
      ? Effect.void
      : runInherited(command, commandArgs, { cwd: root, env: { ...env, ...overrides } });
  for (const command of commands) {
    yield* Console.log(`verify ${profile}: bun run ${command}`);
    yield* execute(bun, ["--no-env-file", "run", command]);
    if (command === "build") {
      yield* Console.log(`verify ${profile}: Node and Bun portable runtime imports`);
      yield* execute(
        node,
        [
          "--experimental-loader",
          "./scripts/pack/resolution-guard.mjs",
          "scripts/portable-import.mjs",
        ],
        { PACK_CONSUMER_ROOT: root, PACK_DENY_NATIVE: "1" },
      );
      yield* execute(bun, ["--no-env-file", "scripts/portable-import.mjs"]);
    }
  }
});

program.pipe(
  // The script's entry point.
  // @effect-diagnostics-next-line strictEffectProvide:off
  Effect.provide(NodeServices.layer),
  NodeRuntime.runMain,
);
