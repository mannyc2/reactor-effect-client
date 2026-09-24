import { defineConfig } from "vitest/config";

/** The channel's end-to-end test runs a real encoder in real time, one file at a time. */
export default defineConfig({
  test: {
    environment: "node",
    include: ["test/**/*.test.ts"],
    testTimeout: 60_000,
  },
});
