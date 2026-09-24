import { defineConfig } from "vitest/config";

/** The recorder against a real ffmpeg; no session and no native library. */
export default defineConfig({
  test: {
    environment: "node",
    include: ["test/**/*.test.ts"],
    testTimeout: 30_000,
  },
});
