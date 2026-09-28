import { readFileSync } from "node:fs";

/**
 * Every portable entry of the built workspace packages must load under the
 * current runtime and export something. Run under the pack resolution guard,
 * this also proves that no portable entry reaches the native package or its addon.
 */
for (const directory of ["client", "browser"]) {
  const manifest = JSON.parse(
    readFileSync(new URL(`../packages/${directory}/package.json`, import.meta.url), "utf8"),
  );
  for (const entry of Object.keys(manifest.exports)) {
    const target = manifest.exports[entry]?.import;
    if (typeof target !== "string")
      throw new Error(`${manifest.name} entry has no import target: ${entry}`);
    const module = await import(
      new URL(`../packages/${directory}/${target}`, import.meta.url).href
    );
    if (Object.keys(module).length === 0)
      throw new Error(`${manifest.name} entry has no public exports: ${entry}`);
  }
}
console.log(
  `portable-runtime-import-ok ${process.versions.bun === undefined ? "node" : "bun"} ${process.version}`,
);
