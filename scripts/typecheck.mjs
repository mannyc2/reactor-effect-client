import { spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";

const root = fileURLToPath(new URL("../", import.meta.url));
let failed = false;
for (const config of ["tsconfig.json", "tsconfig.tools.json", "tsconfig.integration.json"]) {
  console.log(`typecheck ${config}`);
  const result = spawnSync(
    process.execPath,
    ["node_modules/typescript/bin/tsc", "-p", config, "--noEmit"],
    {
      cwd: root,
      stdio: "inherit",
    },
  );
  if (result.error !== undefined) throw result.error;
  if (result.status !== 0) failed = true;
}
process.exitCode = failed ? 1 : 0;
