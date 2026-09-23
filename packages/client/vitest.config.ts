import { defineConfig } from "vitest/config";

/**
 * The portable client suite runs offline against local fixtures, under Node
 * and under Bun, so the declared Node engine is exercised as well as Bun.
 */
export default defineConfig({
  test: {
    environment: "node",
    include: ["test/**/*.test.ts"],
  },
});
