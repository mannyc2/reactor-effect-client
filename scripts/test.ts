import { spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";
import { join } from "node:path";

/** Runtime projects discover their own directories; no test filename registry. */
const project = process.argv[2] ?? "portable";
if (!["portable", "native", "integration"].includes(project)) {
  throw new Error(`unknown test project ${project}; use portable, native or integration`);
}
const root = fileURLToPath(new URL("../", import.meta.url));
const env = { ...process.env };
for (const name of Object.keys(env)) {
  if (/REACTOR.*(?:KEY|TOKEN|JWT)|OPENAI_API_KEY|OPENROUTER_API_KEY/.test(name)) delete env[name];
}
const command = project === "portable" ? process.execPath : (process.env.NODE_BINARY ?? "node");
const args =
  project === "portable"
    ? ["--no-env-file", "test"]
    : [join(root, "node_modules/vitest/vitest.mjs"), "run", "--project", project];
const result = spawnSync(command, args, { cwd: root, env, stdio: "inherit", timeout: 180_000 });
if (result.error !== undefined) throw result.error;
process.exitCode = result.status ?? 1;
