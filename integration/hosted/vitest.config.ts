import { defineConfig } from "vitest/config";

/** The hosted qualification's offline suite: gates, ledger and a rehearsal of every check. */
export default defineConfig({
  test: {
    environment: "node",
    include: ["test/**/*.test.ts"],
  },
});
