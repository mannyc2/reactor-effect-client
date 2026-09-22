import { readFileSync } from "node:fs";

const manifest = JSON.parse(readFileSync(new URL("../package.json", import.meta.url), "utf8"));
for (const entry of [
  ".",
  "./browser",
  "./h3",
  "./orchestration",
  "./simulation",
  "./testing",
  "./wire",
]) {
  const target = manifest.exports[entry]?.import;
  if (typeof target !== "string") throw new Error(`portable entry has no import target: ${entry}`);
  const module = await import(new URL(`../${target}`, import.meta.url).href);
  if (Object.keys(module).length === 0)
    throw new Error(`portable entry has no public exports: ${entry}`);
}
console.log(
  `portable-runtime-import-ok ${process.versions.bun === undefined ? "node" : "bun"} ${process.version}`,
);
