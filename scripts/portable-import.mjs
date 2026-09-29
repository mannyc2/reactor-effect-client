import * as NodeRuntime from "@effect/platform-node/NodeRuntime";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as Console from "effect/Console";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as Predicate from "effect/Predicate";
import * as Schema from "effect/Schema";

/**
 * Every public module of the built client and browser packages must load under
 * the current runtime and export something: the index and each top-level
 * `dist/<Module>.js` that the `"./*"` export reaches (`internal/` is not
 * exported). Run under the pack resolution guard, this also proves that no
 * portable module reaches the native package or its addon.
 */

// JavaScript can't pass a type argument in a call, so the error's own type is given here.
/** @type {typeof Schema.TaggedError<PortableImportError>} */
const TaggedError = Schema.TaggedError;

/** A built module is missing or exports nothing. */
class PortableImportError extends TaggedError(
  "reactor-effect/scripts/portable-import/PortableImportError",
)("PortableImportError", { message: Schema.String }) {}

const program = Effect.gen(function* () {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  for (const directory of ["client", "browser"]) {
    const dist = yield* path.fromFileUrl(
      new URL(`../packages/${directory}/dist/`, import.meta.url),
    );
    const modules = (yield* fs.readDirectory(dist)).filter((name) => name.endsWith(".js"));
    if (!modules.includes("index.js"))
      return yield* PortableImportError.make({ message: `${directory} has no built index` });
    for (const name of modules) {
      const url = yield* path.toFileUrl(path.join(dist, name));
      /** @type {unknown} */
      const module = yield* Effect.promise(() => import(url.href));
      if (!Predicate.isObject(module) || Object.keys(module).length === 0)
        return yield* PortableImportError.make({
          message: `${directory} module has no public exports: ${name}`,
        });
    }
  }
  const runtime = "bun" in process.versions ? "bun" : "node";
  yield* Console.log(`portable-runtime-import-ok ${runtime} ${process.version}`);
});

program.pipe(
  // The script's entry point.
  // @effect-diagnostics-next-line strictEffectProvide:off
  Effect.provide(NodeServices.layer),
  NodeRuntime.runMain,
);
