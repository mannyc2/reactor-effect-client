/**
 * The adoption check's paid owner runs under Node from this harness's
 * TypeScript sources. Only a paid run spawns it, and a rehearsal runs its owner
 * in process, so nothing else would notice a module Node cannot load.
 */
import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, layer } from "@effect/vitest";
import { Effect, Path, Stream } from "effect";
import { ChildProcess, ChildProcessSpawner } from "effect/unstable/process";
import { nodeOwnerArgs } from "../Target.js";

layer(NodeServices.layer)("the paid adoption owner", (it) => {
  it.effect(
    "starts under Node from the TypeScript sources and takes --isolated",
    () =>
      Effect.gen(function* () {
        const path = yield* Path.Path;
        const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
        const script = yield* path.fromFileUrl(new URL("../main.ts", import.meta.url));
        const { output, exitCode } = yield* Effect.scoped(
          Effect.gen(function* () {
            const handle = yield* spawner.spawn(
              ChildProcess.make("node", [...nodeOwnerArgs(script), "--help"]),
            );
            return yield* Effect.all(
              {
                output: handle.all.pipe(Stream.decodeText(), Stream.mkString),
                exitCode: handle.exitCode,
              },
              { concurrency: "unbounded" },
            );
          }),
        );
        assert.strictEqual(exitCode, 0, output);
        assert.include(output, "--isolated");
      }),
    30_000,
  );
});
