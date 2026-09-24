import { defineConfig } from "vitest/config";

/** Offline: the simulation on the test clock. */
export default defineConfig({
  test: {
    environment: "node",
    include: ["test/**/*.test.ts"],
  },
});
