import { defineConfig } from "vitest/config";

/**
 * Native tests load the staged library through Koffi in isolated processes,
 * under Node and under Bun. Files run one at a time so the media load tests
 * measure the bridge rather than contention with another suite.
 */
export default defineConfig({
  test: {
    environment: "node",
    include: ["test/**/*.test.ts"],
    pool: "forks",
    fileParallelism: false,
    testTimeout: 15_000,
  },
});
