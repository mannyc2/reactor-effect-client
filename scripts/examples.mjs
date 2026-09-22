import { spawnSync } from "node:child_process";
import { existsSync, mkdirSync, mkdtempSync } from "node:fs";
import { join } from "node:path";
import { fileURLToPath } from "node:url";

const root = fileURLToPath(new URL("../", import.meta.url));
if (!existsSync(join(root, "dist/index.d.ts")))
  throw new Error("compiled examples require the package build; run bun run build first");
mkdirSync(join(root, ".check"), { recursive: true });
const output = mkdtempSync(join(root, ".check", "examples-"));
/** @param {string} command @param {string[]} args */
const run = (command, args) => {
  const result = spawnSync(command, args, { cwd: root, stdio: "inherit", timeout: 60_000 });
  if (result.error !== undefined) throw result.error;
  if (result.status !== 0) throw new Error(`examples: ${command} exited ${result.status}`);
};

for (const profile of ["node", "browser"]) {
  run(process.execPath, [
    "node_modules/typescript/bin/tsc",
    "-p",
    `examples/${profile}/tsconfig.json`,
    "--outDir",
    join(output, profile),
  ]);
  console.log(`examples-compiled ${profile}`);
}
// Only the offline simulation runs. Live-session examples are never evaluated.
for (const command of [process.execPath, process.env.BUN_BINARY ?? "bun"])
  run(command, [
    "test/fixtures/pack/example-smoke.mjs",
    join(output, "node/portable/simulation.mjs"),
  ]);
console.log(`examples-ok ${output}`);
