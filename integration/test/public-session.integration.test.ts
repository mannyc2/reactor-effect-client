import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, layer } from "@effect/vitest";
import * as Config from "effect/Config";
import * as Effect from "effect/Effect";
import * as Path from "effect/Path";
import * as Ref from "effect/Ref";
import * as Stdio from "effect/Stdio";
import * as Stream from "effect/Stream";
import * as ChildProcess from "effect/process/ChildProcess";

/** How much of the runner's output the assertions keep: its tail. */
const kept = 256 * 1024;

// The runner is a real subprocess with real Chrome, so its deadline runs on the live clock.
layer(NodeServices.layer, { excludeTestServices: true })((it) => {
  it.effect(
    "public Browser and Native owners exchange real local WebRTC media and join cleanup",
    () =>
      Effect.gen(function* () {
        const path = yield* Path.Path;
        const stdio = yield* Stdio.Stdio;
        const root = yield* path.fromFileUrl(new URL("../", import.meta.url));
        const bun = yield* Config.String("BUN_BINARY").pipe(Config.withDefault("bun"));
        const run = Effect.gen(function* () {
          // The runner, Chrome and their helpers share this process group, which closing the
          // scope terminates: TERM, then KILL two seconds later.
          const runner = yield* ChildProcess.make("sh", ["scripts/browser-native.sh"], {
            cwd: root,
            env: { NODE_BINARY: process.execPath, BUN_BINARY: bun },
            extendEnv: true,
            stdin: "ignore",
            detached: true,
            killSignal: "SIGTERM",
            forceKillAfter: "2 seconds",
          });
          const output = yield* Ref.make("");
          yield* runner.all.pipe(
            Stream.decodeText(),
            Stream.tap((text) => Ref.update(output, (seen) => `${seen}${text}`.slice(-kept))),
            Stream.run(stdio.stdout()),
          );
          return { exit: yield* runner.exitCode, output: yield* Ref.get(output) };
        }).pipe(
          Effect.scoped,
          Effect.timeoutOrElse({
            duration: "110 seconds",
            orElse: () =>
              Effect.sync(() =>
                assert.fail("public session subprocess exceeded its bounded lifetime"),
              ),
          }),
        );
        const { exit, output } = yield* run;
        assert.strictEqual(exit, 0, output);
        assert.include(output, "browser-native-ok");
        assert.include(output, "sameAttributedObjects");
        assert.include(output, "failureCleanup");
        assert.include(output, "native-artifact sha256=");
      }),
  );
});
