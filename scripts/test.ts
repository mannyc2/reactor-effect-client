import { spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";
import { join } from "node:path";

/**
 * Runtime projects discover their own files; there is no test filename registry.
 *   portable    Bun discovery from the workspace root (bunfig.toml excludes the others)
 *   native      Vitest in packages/native against the staged library, on Node then Bun
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
const vitest = join(directory, "node_modules/vitest/vitest.mjs");
const node = process.env.NODE_BINARY ?? "node";
const runs: readonly (readonly [string, readonly string[], string])[] =
  project === "portable"
    ? [[process.execPath, ["--no-env-file", "test"], root]]
    : project === "native"
      ? [
          [node, [vitest, "run"], directory],
          [process.env.BUN_BINARY ?? process.execPath, ["--bun", vitest, "run"], directory],
        ]
      : [[node, [vitest, "run"], directory]];
for (const [command, args, cwd] of runs) {
  const result = spawnSync(command, args, { cwd, env, stdio: "inherit", timeout: 180_000 });
  if (result.error !== undefined) throw result.error;
  process.exitCode = result.status ?? 1;
  if (process.exitCode !== 0) break;
}
