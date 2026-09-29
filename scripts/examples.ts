/**
 * Checks the examples past their typecheck, which `bun run typecheck` runs in
 * every example workspace: each example's offline tests under Node and then
 * Bun, and each browser bundle an example builds. Nothing here contacts
 * Reactor. Credential-like environment variables are removed, and the
 * examples that need a paid session are compiled, never run.
 *
 * Tests that drive a real ffmpeg skip themselves when it is not on PATH;
 * EXAMPLES_REQUIRE_FFMPEG=1, which CI sets, makes its absence a failure.
 */
import * as NodeRuntime from "@effect/platform-node/NodeRuntime";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as Config from "effect/Config";
import * as Console from "effect/Console";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";
import * as ChildProcess from "effect/unstable/process/ChildProcess";
import * as ChildProcessSpawner from "effect/unstable/process/ChildProcessSpawner";
import { environment, runInherited, runtimes } from "./subprocess.js";

class ExamplesError extends Schema.TaggedError<ExamplesError>(
  "reactor-effect/scripts/examples/ExamplesError",
)("ExamplesError", { message: Schema.String }) {}

/** What this check reads of an example workspace's manifest. */
const Manifest = Schema.fromJsonString(
  Schema.Struct({
    name: Schema.String,
    scripts: Schema.optionalKey(Schema.Record(Schema.String, Schema.String)),
  }),
);

const requireFfmpeg = Config.String("EXAMPLES_REQUIRE_FFMPEG").pipe(
  Config.map((value) => value === "1"),
  Config.withDefault(false),
);

const program = Effect.gen(function* () {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
  const root = yield* path.fromFileUrl(new URL("../", import.meta.url));
  const env = yield* environment;
  const { node, bun } = yield* runtimes;

  const run = Effect.fnUntraced(function* (
    command: string,
    args: ReadonlyArray<string>,
    cwd: string,
  ) {
    const where = `${command} ${args.join(" ")}`;
    yield* runInherited(command, args, { cwd, env }).pipe(
      Effect.timeoutOrElse({
        duration: "240 seconds",
        orElse: () =>
          Effect.fail(
            ExamplesError.make({
              message: `${where} did not finish within 240 seconds in ${path.relative(root, cwd)}`,
            }),
          ),
      }),
      Effect.catchTag("ChildFailed", (failed) =>
        Effect.fail(
          ExamplesError.make({
            message: `${where} exited ${failed.status} in ${path.relative(root, cwd)}`,
          }),
        ),
      ),
    );
  });

  /** Every example workspace: examples/<name> and packages/<package>/examples. */
  const candidates = [
    ...(yield* fs.readDirectory(path.join(root, "examples"))).map((name) =>
      path.join(root, "examples", name),
    ),
    ...(yield* fs.readDirectory(path.join(root, "packages"))).map((name) =>
      path.join(root, "packages", name, "examples"),
    ),
  ];
  // A file among them, such as a README, holds no manifest.
  const workspaces = yield* Effect.filter(candidates, (directory) =>
    fs.exists(path.join(directory, "package.json")).pipe(Effect.orElseSucceed(() => false)),
  );
  if (workspaces.length === 0)
    return yield* ExamplesError.make({ message: "no example workspaces found" });

  // An ffmpeg that can't start is as absent as one that isn't installed.
  const ffmpeg = yield* spawner
    .exitCode(
      ChildProcess.make("ffmpeg", ["-version"], {
        stdin: "ignore",
        stdout: "ignore",
        stderr: "ignore",
      }),
    )
    .pipe(
      Effect.map((status) => status === ChildProcessSpawner.ExitCode(0)),
      Effect.orElseSucceed(() => false),
    );
  if (!ffmpeg) {
    if (yield* requireFfmpeg)
      return yield* ExamplesError.make({
        message: "ffmpeg is required (EXAMPLES_REQUIRE_FFMPEG=1) and is not on PATH",
      });
    yield* Console.log(
      "examples-notice ffmpeg is not on PATH: the tests that encode video are skipped",
    );
  }

  for (const directory of workspaces) {
    const manifest = yield* Schema.decodeEffect(Manifest)(
      yield* fs.readFileString(path.join(directory, "package.json")),
    );
    if (manifest.scripts?.test !== undefined) {
      const vitest = path.join(directory, "node_modules/vitest/vitest.mjs");
      yield* run(node, [vitest, "run"], directory);
      yield* run(bun, ["--bun", vitest, "run"], directory);
      yield* Console.log(`examples-tested ${manifest.name} node bun`);
    }
    if (manifest.scripts?.build !== undefined) {
      const dist = path.join(directory, "dist");
      yield* fs.remove(dist, { recursive: true, force: true });
      yield* run(bun, ["--no-env-file", "run", "build"], directory);
      // A page's bundle must reach neither Node nor the native host.
      const bundles = (yield* fs.readDirectory(dist)).filter((name) => name.endsWith(".js"));
      for (const file of bundles) {
        const bundle = yield* fs.readFileString(path.join(dist, file));
        if (/reactor-effect-native|takeVideo|["']node:/.test(bundle))
          return yield* ExamplesError.make({
            message: `${manifest.name} bundle ${file} reaches Node or native code`,
          });
      }
      yield* Console.log(`examples-bundled ${manifest.name}`);
    }
  }
  yield* Console.log(`examples-ok ${workspaces.length} workspaces`);
});

program.pipe(
  // The script's entry point.
  // @effect-diagnostics-next-line strictEffectProvide:off
  Effect.provide(NodeServices.layer),
  NodeRuntime.runMain,
);
