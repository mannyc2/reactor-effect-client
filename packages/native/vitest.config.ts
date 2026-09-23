import { defineConfig } from "vitest/config";

/** Native tests run under Node and load the staged library through Koffi in isolated processes. */
export default defineConfig({
  test: {
    environment: "node",
    include: ["test/**/*.test.ts"],
    pool: "forks",
    testTimeout: 15_000,
  },
});
