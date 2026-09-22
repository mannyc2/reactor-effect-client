import { readdirSync } from "node:fs";
import { join, resolve } from "node:path";
import { spawnSync } from "node:child_process";

// These source modules register cases with the retained archive harness; its
// Archive.test.ts wrapper owns their execution. They must not also be run as
// empty Bun test files, or counted as separate validation.
const archive = new Set([
  "wire.test.ts", "http.test.ts", "session.test.ts", "session-client.test.ts", "media.test.ts",
  "publication.test.ts", "stats.test.ts", "recording.test.ts", "native-audio.test.ts",
]);

const discover = (directory: string): string[] => readdirSync(directory, { withFileTypes: true })
  .flatMap((entry) => entry.isDirectory()
    ? discover(join(directory, entry.name))
    : entry.name.endsWith(".test.ts") ? [join(directory, entry.name)] : []);

const selected = discover("test").filter((path) => {
  const name = path.split(/[\\/]/).at(-1)!;
  return !/^Live\./i.test(name) && !(path === join("test", name) && archive.has(name));
});
const native = selected.filter((path) => /^native-(?:abi|parser|session)\.test\.ts$/.test(path.split(/[\\/]/).at(-1)!));
const unit = selected.filter((path) => !native.includes(path));
const env = { ...process.env };
for (const name of Object.keys(env)) {
  if (/REACTOR.*(?:KEY|TOKEN|JWT)|OPENAI_API_KEY|OPENROUTER_API_KEY/.test(name)) delete env[name];
}

const run = (executable: string, arguments_: string[]): void => {
  const result = spawnSync(executable, arguments_, { stdio: "inherit", env, timeout: 180_000 });
  if (result.error !== undefined) throw result.error;
  if (result.status !== 0) process.exit(result.status ?? 1);
};

if (unit.length === 0 || native.length === 0) throw new Error("SDK test discovery found an empty verification lane");
console.log(`SDK tests: ${unit.length} Bun files and ${native.length} native ABI Vitest files`);
run(process.execPath, ["--no-env-file", "test", ...unit]);
run(process.env.NODE_BINARY ?? "node", [resolve("node_modules/vitest/vitest.mjs"), "run", ...native]);
