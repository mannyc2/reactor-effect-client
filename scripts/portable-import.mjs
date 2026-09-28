import { readdirSync } from "node:fs";

/**
 * Every public module of the built client and browser packages must load under
 * the current runtime and export something: the index and each top-level
 * `dist/<Module>.js` that the `"./*"` export reaches (`internal/` is not
 * exported). Run under the pack resolution guard, this also proves that no
 * portable module reaches the native package or its addon.
 */
for (const directory of ["client", "browser"]) {
  const dist = new URL(`../packages/${directory}/dist/`, import.meta.url);
  const modules = readdirSync(dist).filter((name) => name.endsWith(".js"));
  if (!modules.includes("index.js")) throw new Error(`${directory} has no built index`);
  for (const name of modules) {
    const module = await import(new URL(name, dist).href);
    if (Object.keys(module).length === 0)
      throw new Error(`${directory} module has no public exports: ${name}`);
  }
}
console.log(
  `portable-runtime-import-ok ${process.versions.bun === undefined ? "node" : "bun"} ${process.version}`,
);
