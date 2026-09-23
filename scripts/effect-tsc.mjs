import { execFileSync } from "node:child_process";
import { createRequire } from "node:module";
import { dirname, join } from "node:path";

/**
 * The workspace `tsc` must be the @effect/tsgo build, which the root `prepare`
 * script patches in after every install. An unpatched compiler still type
 * checks, but silently without the Effect diagnostics this workspace enforces.
 */
const manifest = createRequire(import.meta.url).resolve("typescript/package.json");
const tsc = join(dirname(manifest), "bin", "tsc");
const version = execFileSync(process.execPath, [tsc, "--version"], { encoding: "utf8" }).trim();
if (!version.includes("+effect-tsgo"))
  throw new Error(
    `${version} is not the @effect/tsgo build; run bun install, or bunx effect-tsgo patch --typescript`,
  );
console.log(`effect-tsc-ok ${version}`);
