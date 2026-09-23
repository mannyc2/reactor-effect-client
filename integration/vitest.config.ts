import { defineConfig } from "vitest/config";

/** Integration runs under Node: it spawns the real browser/native runner and must not overlap with itself. */
export default defineConfig({
  test: {
    environment: "node",
    include: ["test/**/*.integration.test.ts"],
    pool: "forks",
    fileParallelism: false,
    testTimeout: 120_000,
  },
});
