import { spawnSync } from "node:child_process";
import { existsSync, readdirSync, readFileSync, rmSync } from "node:fs";
import { join, relative } from "node:path";
import { fileURLToPath } from "node:url";

/**
 * Checks the examples past their typecheck, which `bun run typecheck` runs in
 * every example workspace: each example's offline tests under Node and then
 * Bun, and each browser bundle an example builds. Nothing here contacts
 * Reactor. Credential-like environment variables are removed, and the
 * examples that need a paid session are compiled, never run.
 *
 * Tests that drive a real ffmpeg skip themselves when it is not on PATH;
 * EXAMPLES_REQUIRE_FFMPEG=1, which CI sets, makes its absence a failure.
 */
const root = fileURLToPath(new URL("../", import.meta.url));
const env: NodeJS.ProcessEnv = { ...process.env };
for (const name of Object.keys(env)) {
  if (/REACTOR.*(?:KEY|TOKEN|JWT)|OPENAI_API_KEY|OPENROUTER_API_KEY/.test(name)) delete env[name];
}
const node = process.env.NODE_BINARY ?? "node";
const bun = process.env.BUN_BINARY ?? process.execPath;

const fail = (message: string): never => {
  throw new Error(`examples: ${message}`);
};
const run = (command: string, args: readonly string[], cwd: string): void => {
  const result = spawnSync(command, [...args], { cwd, env, stdio: "inherit", timeout: 240_000 });
  if (result.error !== undefined) throw result.error;
  if (result.status !== 0)
    fail(`${command} ${args.join(" ")} exited ${result.status} in ${relative(root, cwd)}`);
};

interface Manifest {
  readonly name: string;
  readonly scripts?: Readonly<Record<string, string>>;
}
/** Every example workspace: examples/<name> and packages/<package>/examples. */
const workspaces = [
  ...readdirSync(join(root, "examples")).map((name) => join(root, "examples", name)),
  ...readdirSync(join(root, "packages")).map((name) => join(root, "packages", name, "examples")),
].filter((directory) => existsSync(join(directory, "package.json")));
if (workspaces.length === 0) fail("no example workspaces found");

if (spawnSync("ffmpeg", ["-version"]).status !== 0) {
  if (process.env.EXAMPLES_REQUIRE_FFMPEG === "1")
    fail("ffmpeg is required (EXAMPLES_REQUIRE_FFMPEG=1) and is not on PATH");
  console.log("examples-notice ffmpeg is not on PATH: the tests that encode video are skipped");
}

for (const directory of workspaces) {
  const manifest = JSON.parse(readFileSync(join(directory, "package.json"), "utf8")) as Manifest;
  if (manifest.scripts?.test !== undefined) {
    const vitest = join(directory, "node_modules/vitest/vitest.mjs");
    run(node, [vitest, "run"], directory);
    run(bun, ["--bun", vitest, "run"], directory);
    console.log(`examples-tested ${manifest.name} node bun`);
  }
  if (manifest.scripts?.build !== undefined) {
    const dist = join(directory, "dist");
    rmSync(dist, { recursive: true, force: true });
    run(bun, ["--no-env-file", "run", "build"], directory);
    // A page's bundle must reach neither Node nor the native host.
    for (const file of readdirSync(dist).filter((name) => name.endsWith(".js"))) {
      const bundle = readFileSync(join(dist, file), "utf8");
      if (/koffi|reactor_effect_peer_|["']node:/.test(bundle))
        fail(`${manifest.name} bundle ${file} reaches Node or native code`);
    }
    console.log(`examples-bundled ${manifest.name}`);
  }
}
console.log(`examples-ok ${workspaces.length} workspaces`);
