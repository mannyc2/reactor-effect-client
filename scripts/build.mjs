import { existsSync, readdirSync, rmSync } from "node:fs";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { spawnSync } from "node:child_process";

const root = fileURLToPath(new URL("../", import.meta.url));
const output = join(root, "dist");
// Retain independently staged platform directories only. The native public
// entry now also lives under dist/native; retaining that entire directory
// would leave stale JS/declarations after its source modules were removed.
if (existsSync(output)) {
  for (const entry of readdirSync(output)) {
    if (entry !== "native") {
      rmSync(join(output, entry), { recursive: true, force: true });
      continue;
    }
    for (const nativeEntry of readdirSync(join(output, "native"), { withFileTypes: true })) {
      if (
        nativeEntry.isDirectory() &&
        /^(?:darwin|linux|win32)-(?:arm64|x64)$/.test(nativeEntry.name)
      )
        continue;
      rmSync(join(output, "native", nativeEntry.name), { recursive: true, force: true });
    }
  }
}
const built = spawnSync(
  process.execPath,
  [join(root, "node_modules/typescript/bin/tsc"), "-p", "tsconfig.build.json"],
  {
    cwd: root,
    stdio: "inherit",
  },
);
if (built.error !== undefined) throw built.error;
process.exitCode = built.status ?? 1;
