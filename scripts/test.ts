import { spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";
import { join } from "node:path";

/**
 * Runtime projects discover their own files; there is no test filename registry.
 *   portable    Vitest in packages/client and packages/browser, each on Node then Bun,
 *               then Bun discovery from the workspace root for the integration
 *               modeled-host and script tests (bunfig.toml excludes the others)
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
const node = process.env.NODE_BINARY ?? "node";
const bun = process.env.BUN_BINARY ?? process.execPath;
type Run = readonly [string, readonly string[], string];
const vitest = (directory: string) => join(directory, "node_modules/vitest/vitest.mjs");
/** A Vitest project runs on Node, which the engines declare, and then on Bun. */
const nodeAndBun = (directory: string): readonly Run[] => [
  [node, [vitest(directory), "run"], directory],
  [bun, ["--bun", vitest(directory), "run"], directory],
];
const packages = join(root, "packages");
const runs: readonly Run[] =
  project === "portable"
    ? [
        ...nodeAndBun(join(packages, "client")),
        ...nodeAndBun(join(packages, "browser")),
        [bun, ["--no-env-file", "test"], root],
      ]
    : project === "native"
      ? nodeAndBun(join(packages, "native"))
      : [[node, [vitest(join(root, "integration")), "run"], join(root, "integration")]];
for (const [command, args, cwd] of runs) {
  const result = spawnSync(command, args, { cwd, env, stdio: "inherit", timeout: 180_000 });
  if (result.error !== undefined) throw result.error;
  process.exitCode = result.status ?? 1;
  if (process.exitCode !== 0) break;
}
