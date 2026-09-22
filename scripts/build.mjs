import { existsSync, readdirSync, rmSync } from "node:fs";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { spawnSync } from "node:child_process";

const root = fileURLToPath(new URL("../", import.meta.url));
const output = join(root, "dist");
// Retain only independently built platform binaries. Removed source modules
// must not survive in a later tarball as stale JavaScript or declarations.
if (existsSync(output)) {
  for (const entry of readdirSync(output)) {
    if (entry !== "native") rmSync(join(output, entry), { recursive: true, force: true });
  }
}
const built = spawnSync(process.execPath, [join(root, "node_modules/typescript/bin/tsc"), "-p", "tsconfig.build.json"], {
  cwd: root,
  stdio: "inherit",
});
if (built.error !== undefined) throw built.error;
process.exitCode = built.status ?? 1;
