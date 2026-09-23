import { spawnSync } from "node:child_process";
import { existsSync, rmSync } from "node:fs";
import { join } from "node:path";
import { fileURLToPath } from "node:url";

/**
 * Emits real JavaScript for both host profiles into examples/dist and runs only
 * the offline simulation under Node and Bun. Live-session examples are never
 * executed; they need an application's own credentials.
 */
const examples = fileURLToPath(new URL("./", import.meta.url));
const root = join(examples, "..");
const tsc = join(root, "node_modules/typescript/bin/tsc");
const output = join(examples, "dist");
if (!existsSync(join(examples, "node_modules/reactor-effect-client/dist/index.d.ts")))
  throw new Error("compiled examples require built packages; run bun run build first");
rmSync(output, { recursive: true, force: true });
/** @param {string} command @param {string[]} args */
const run = (command, args) => {
  const result = spawnSync(command, args, { cwd: examples, stdio: "inherit", timeout: 60_000 });
  if (result.error !== undefined) throw result.error;
  if (result.status !== 0) throw new Error(`examples: ${command} exited ${result.status}`);
};

for (const profile of ["node", "browser"]) {
  run(process.execPath, [tsc, "-p", `${profile}/tsconfig.json`, "--outDir", join(output, profile)]);
  console.log(`examples-compiled ${profile}`);
}
for (const command of [process.execPath, process.env.BUN_BINARY ?? "bun"])
  run(command, ["check/simulation-smoke.mjs", join(output, "node/portable/simulation.mjs")]);
console.log(`examples-ok ${output}`);
