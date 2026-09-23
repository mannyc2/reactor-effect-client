import { spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";
import { join } from "node:path";

/**
 * Runtime projects discover their own files; there is no test filename registry.
 *   portable    Bun discovery from the workspace root (bunfig.toml excludes the others)
 *   native      Node/Vitest in packages/native against the staged library
 *   integration Node/Vitest in integration, spawning the real browser/native runner
 */
const project = process.argv[2] ?? "portable";
if (!["portable", "native", "integration"].includes(project)) {
  throw new Error(`unknown test project ${project}; use portable, native or integration`);
}
const root = fileURLToPath(new URL("../", import.meta.url));
const env = { ...process.env };
for (const name of Object.keys(env)) {
  if (/REACTOR.*(?:KEY|TOKEN|JWT)|OPENAI_API_KEY|OPENROUTER_API_KEY/.test(name)) delete env[name];
}
const directory = project === "native" ? join(root, "packages", "native") : join(root, project);
const [command, args, cwd] =
  project === "portable"
    ? [process.execPath, ["--no-env-file", "test"], root]
    : [
        process.env.NODE_BINARY ?? "node",
        [join(directory, "node_modules/vitest/vitest.mjs"), "run"],
        directory,
      ];
const result = spawnSync(command, args, { cwd, env, stdio: "inherit", timeout: 180_000 });
if (result.error !== undefined) throw result.error;
process.exitCode = result.status ?? 1;
