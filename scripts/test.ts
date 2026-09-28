/**
 * Runtime projects discover their own files; there is no test filename registry.
 *   portable    Vitest in packages/client, packages/browser and integration/hosted,
 *               each on Node then Bun, then the scripts' own tests on Bun
 *   native      Vitest in packages/native against the staged library, on Node then Bun
 *   integration Node/Vitest in integration, spawning the real browser/native runner
 *
 * Each run has 180 seconds, and the first that fails ends the project with its exit status.
 */
import * as NodeRuntime from "@effect/platform-node/NodeRuntime";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as Effect from "effect/Effect";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";
import * as Stdio from "effect/Stdio";
import { environment, runInherited, runtimes } from "./subprocess.js";

class TestError extends Schema.TaggedError<TestError>("reactor-effect/scripts/test/TestError")(
  "TestError",
  { message: Schema.String },
) {}

type Run = readonly [command: string, args: ReadonlyArray<string>, cwd: string];

const program = Effect.gen(function* () {
  const path = yield* Path.Path;
  const project = (yield* (yield* Stdio.Stdio).args)[0] ?? "portable";
  const root = yield* path.fromFileUrl(new URL("../", import.meta.url));
  const { node, bun } = yield* runtimes;
  const env = yield* environment;
  const vitest = (directory: string) => path.join(directory, "node_modules/vitest/vitest.mjs");
  /** A Vitest project runs on Node, which the engines declare, and then on Bun. */
  const nodeAndBun = (directory: string): ReadonlyArray<Run> => [
    [node, [vitest(directory), "run"], directory],
    [bun, ["--bun", vitest(directory), "run"], directory],
  ];
  const packages = path.join(root, "packages");
  const integration = path.join(root, "integration");
  const runsOf = (name: string): ReadonlyArray<Run> | undefined => {
    switch (name) {
      case "portable":
        return [
          ...nodeAndBun(path.join(packages, "client")),
          ...nodeAndBun(path.join(packages, "browser")),
          // The hosted qualification's gates, ledger and a rehearsal of every check.
          [node, [vitest(integration), "run", "--root", "hosted"], integration],
          [bun, ["--bun", vitest(integration), "run", "--root", "hosted"], integration],
          // pack runs on Bun and reads bun.lock with Bun.JSONC, so its tests run there.
          [bun, ["--bun", vitest(root), "run", "--root", "scripts"], root],
        ];
      case "native":
        return nodeAndBun(path.join(packages, "native"));
      case "integration":
        return [[node, [vitest(integration), "run"], integration]];
      default:
        return undefined;
    }
  };
  const runs = runsOf(project);
  if (runs === undefined)
    return yield* TestError.make({
      message: `unknown test project ${project}; use portable, native or integration`,
    });
  for (const [command, args, cwd] of runs)
    yield* runInherited(command, args, { cwd, env }).pipe(
      Effect.timeoutOrElse({
        duration: "180 seconds",
        orElse: () =>
          Effect.fail(
            TestError.make({
              message: `${command} ${args.join(" ")} did not finish within 180 seconds`,
            }),
          ),
      }),
    );
});

program.pipe(
  // The script's entry point.
  // @effect-diagnostics-next-line strictEffectProvide:off
  Effect.provide(NodeServices.layer),
  NodeRuntime.runMain,
);
