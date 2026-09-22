import { spawn } from "node:child_process";
import { fileURLToPath } from "node:url";
import { expect, test } from "vitest";

test("public Browser and Native owners exchange real local WebRTC media and join cleanup", async () => {
  const root = fileURLToPath(new URL("../../", import.meta.url));
  const child = spawn("sh", ["scripts/browser-native.sh"], {
    cwd: root,
    env: {
      ...process.env,
      NODE_BINARY: process.execPath,
      BUN_BINARY: process.env.BUN_BINARY ?? "bun",
    },
    stdio: ["ignore", "pipe", "pipe"],
    detached: process.platform !== "win32",
  });
  let output = "";
  const capture = (chunk: Buffer): void => {
    output = `${output}${chunk.toString("utf8")}`.slice(-256 * 1024);
    process.stdout.write(chunk);
  };
  child.stdout.on("data", capture);
  child.stderr.on("data", capture);
  const signal = (name: NodeJS.Signals): void => {
    if (child.pid === undefined || child.exitCode !== null || child.signalCode !== null) return;
    try {
      if (process.platform === "win32") child.kill(name);
      else process.kill(-child.pid, name);
    } catch (cause) {
      if ((cause as NodeJS.ErrnoException).code !== "ESRCH") throw cause;
    }
  };
  let timedOut = false;
  let force: ReturnType<typeof setTimeout> | undefined;
  const timeout = setTimeout(() => {
    timedOut = true;
    signal("SIGTERM");
    force = setTimeout(() => signal("SIGKILL"), 2000);
  }, 110_000);
  let exit: number | null;
  try {
    exit = await new Promise<number | null>((resolve, reject) => {
      child.once("error", reject);
      child.once("exit", resolve);
    });
  } finally {
    clearTimeout(timeout);
    if (force !== undefined) clearTimeout(force);
  }
  expect(timedOut, "public session subprocess exceeded its bounded lifetime").toBe(false);
  expect(exit, output).toBe(0);
  expect(output).toContain("browser-native-ok");
  expect(output).toContain("sameAttributedObjects");
  expect(output).toContain("failureCleanup");
  expect(output).toContain("native-artifact sha256=");
});
