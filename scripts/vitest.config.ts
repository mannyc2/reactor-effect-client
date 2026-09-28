import { defineConfig } from "vitest/config";

/** The scripts' own tests: pack's Effect stack selection, under Bun (it reads bun.lock with Bun.JSONC). */
export default defineConfig({
  test: { environment: "node", include: ["*.test.ts"] },
});
