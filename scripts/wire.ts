/**
 * Generates the Reactor wire codecs from the tracked protos with the client's pinned `buf` and
 * `protoc-gen-es`: the client's own, and the one the integration's browser far side plays
 * Reactor with. `--check` generates into a scratch directory and fails on any difference from
 * the committed code.
 *
 * protoc-gen-es opens every file with an `eslint-disable` directive. The generated code passes
 * the lint as it is, so the directive would only be reported as unused; it is removed here.
 */
import * as NodeRuntime from "@effect/platform-node/NodeRuntime";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as Console from "effect/Console";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";
import * as Stdio from "effect/Stdio";
import * as ChildProcess from "effect/process/ChildProcess";
import * as ChildProcessSpawner from "effect/process/ChildProcessSpawner";

class WireError extends Schema.TaggedError<WireError>("reactor-effect/scripts/wire/WireError")(
  "WireError",
  { message: Schema.String },
) {}

/** Where `packages/client/wire/buf.gen.yaml` puts each codec, from the repository root. */
const outputs = ["packages/client/src/internal/proto", "integration/browser/proto"];
const directive = "/* eslint-disable */\n";

const program = Effect.gen(function* () {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const args = yield* (yield* Stdio.Stdio).args;
  const check = args.includes("--check");
  for (const arg of args)
    if (arg !== "--check") return yield* WireError.make({ message: `unknown argument ${arg}` });
  const root = yield* path.fromFileUrl(new URL("../", import.meta.url));
  const client = path.join(root, "packages/client");
  const scratch = yield* fs.makeTempDirectoryScoped({ prefix: "reactor-wire-" });

  const exit = yield* ChildProcess.make(
    path.join(client, "node_modules/.bin/buf"),
    ["generate", "--template", "wire/buf.gen.yaml", "--output", scratch],
    { cwd: client, stdout: "inherit", stderr: "inherit" },
  ).pipe(
    Effect.flatMap((handle) => handle.exitCode),
    Effect.scoped,
  );
  if (exit !== ChildProcessSpawner.ExitCode(0))
    return yield* WireError.make({ message: `buf generate exited ${exit}` });

  const differences: string[] = [];
  for (const output of outputs) {
    const generated = path.join(scratch, output);
    const committed = path.join(root, output);
    const files = (yield* fs.readDirectory(generated, { recursive: true }))
      .filter((file) => file.endsWith(".ts"))
      .sort();
    for (const file of files) {
      const source = yield* fs.readFileString(path.join(generated, file));
      yield* fs.writeFileString(path.join(generated, file), source.replace(directive, ""));
    }
    if (!check) {
      yield* fs.remove(committed, { recursive: true, force: true });
      yield* fs.copy(generated, committed);
      yield* Console.log(`wire-generated ${output} ${files.length} files`);
      continue;
    }
    const present = (yield* fs.exists(committed))
      ? (yield* fs.readDirectory(committed, { recursive: true }))
          .filter((file) => file.endsWith(".ts"))
          .sort()
      : [];
    for (const file of new Set([...files, ...present])) {
      const expected = path.join(generated, file);
      const actual = path.join(committed, file);
      if (!(yield* fs.exists(expected)) || !(yield* fs.exists(actual))) {
        differences.push(path.join(output, file));
        continue;
      }
      if ((yield* fs.readFileString(expected)) !== (yield* fs.readFileString(actual)))
        differences.push(path.join(output, file));
    }
  }
  if (differences.length > 0) {
    for (const file of differences) yield* Console.error(`wire-differs ${file}`);
    return yield* WireError.make({
      message: "generated wire code differs; run bun run generate:wire",
    });
  }
  if (check) yield* Console.log(`wire-check-ok ${outputs.join(" ")}`);
}).pipe(Effect.scoped);

program.pipe(
  // The script's entry point.
  // @effect-diagnostics-next-line strictEffectProvide:off
  Effect.provide(NodeServices.layer),
  NodeRuntime.runMain,
);
