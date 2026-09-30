import { defineConfig } from "vitest/config";

/**
 * The scripts' own tests: pack's Effect stack selection, under Bun (it reads bun.lock with Bun.JSONC).
 * Some spawn tar, bun and npm, which take seconds on a machine busy with the other suites.
 */
export default defineConfig({
  test: { environment: "node", include: ["*.test.ts"], testTimeout: 30_000 },
});
