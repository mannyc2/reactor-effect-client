import { defineConfig } from "vitest/config";

/**
 * Browser media policy tests fake their DOM and WebRTC hosts per test, so they
 * need no browser: they run in plain Node and Bun processes, as the client does.
 */
export default defineConfig({
  test: {
    environment: "node",
    include: ["test/**/*.test.ts"],
  },
});
