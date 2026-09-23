import { spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";

const root = fileURLToPath(new URL("../", import.meta.url));
const args = process.argv.slice(2);
const profileAt = args.indexOf("--profile");
const profile = profileAt < 0 ? "full" : args[profileAt + 1];
const listOnly = args.includes("--list");
for (let index = 0; index < args.length; index++) {
  const arg = args[index];
  if (arg === "--list") continue;
  if (arg === "--profile") {
    index++;
    continue;
  }
  throw new Error(`unknown verification argument: ${arg}`);
}

// CI/release and local callers share exactly these workspace commands. Runtime
// test discovery lives in bunfig/Vitest projects, not in this orchestration.
// `build` precedes `typecheck` because dependent packages and examples resolve
// their workspace dependencies through the built declarations.
const portable = [
  "generate:check",
  "format:check",
  "lint",
  "build",
  "typecheck",
  "check:architecture",
  "check:examples",
  "test:portable",
];
// Native and integration tests import the built client package.
const native = ["build", "native:build", "native:test", "test:integration"];
const runtime = ["build", "test:portable"];
const packaging = ["test:pack"];
const profiles: Readonly<Record<string, readonly string[]>> = {
  portable,
  runtime,
  native,
  package: packaging,
  release: [...portable, ...packaging],
  full: [...portable, ...native, ...packaging],
};
const commands = profile === undefined ? undefined : profiles[profile];
if (commands === undefined)
  throw new Error(
    `unknown verification profile ${profile}; use ${Object.keys(profiles).join(", ")}`,
  );
const bun = process.env.BUN_BINARY ?? process.execPath;
const env: NodeJS.ProcessEnv = { ...process.env, BUN_BINARY: bun };
for (const name of Object.keys(env)) {
  if (/REACTOR.*(?:KEY|TOKEN|JWT)|OPENAI_API_KEY|OPENROUTER_API_KEY/.test(name)) delete env[name];
}
const execute = (
  command: string,
  args: readonly string[],
  overrides: NodeJS.ProcessEnv = {},
): boolean => {
  if (listOnly) return true;
  const result = spawnSync(command, [...args], {
    cwd: root,
    env: { ...env, ...overrides },
    stdio: "inherit",
  });
  if (result.error !== undefined) throw result.error;
  if (result.status === 0) return true;
  process.exitCode = result.status ?? 1;
  return false;
};
for (const command of commands) {
  console.log(`verify ${profile}: bun run ${command}`);
  if (!execute(bun, ["--no-env-file", "run", command])) break;
  if (command === "build") {
    console.log(`verify ${profile}: Node and Bun portable runtime imports`);
    if (
      !execute(
        process.env.NODE_BINARY ?? "node",
        [
          "--experimental-loader",
          "./scripts/pack/resolution-guard.mjs",
          "scripts/portable-import.mjs",
        ],
        {
          PACK_CONSUMER_ROOT: root,
          PACK_DENY_NATIVE: "1",
        },
      )
    )
      break;
    if (!execute(bun, ["--no-env-file", "scripts/portable-import.mjs"])) break;
  }
}
