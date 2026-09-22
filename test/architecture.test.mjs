import { describe, expect, test } from "bun:test";
import { spawnSync } from "node:child_process";
import { mkdtempSync, mkdirSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { checkGraph, checkManifest, entries } from "../scripts/architecture.mjs";

const root = fileURLToPath(new URL("../", import.meta.url));
const manifest = JSON.parse(readFileSync(join(root, "package.json"), "utf8"));
/** @param {string} path @param {string[]} imports @param {boolean} [runtime] */
const module = (path, imports = [], runtime = true) => ({
  path,
  imports: imports.map((specifier) => ({ specifier, runtime, line: 1 })),
  problems: [],
});

describe("architecture contract", () => {
  test("keeps the exact public targets and Effect peer/development pins", () => {
    expect(checkManifest(manifest)).toEqual([]);
    expect(
      checkManifest({ ...manifest, exports: { ...manifest.exports, "./private": {} } }),
    ).toContain("package must retain exactly the eight public exports");
    expect(checkManifest({ ...manifest, peerDependencies: { effect: "latest" } })).toContain(
      "peerDependencies.effect must remain 4.0.0-rc.115",
    );
    expect(
      checkManifest({
        ...manifest,
        exports: { ...manifest.exports, ".": { import: "./dist/other.js" } },
      }),
    ).toContain("public export . must retain its types/import targets");
  });

  test("rejects upward policy and cross-host dependencies, including type-only imports", () => {
    const problems = checkGraph(
      [
        module("src/session.ts", ["./h3/types.js"], false),
        module("src/h3/types.ts", ["../orchestration/types.js"], false),
        module("src/orchestration/types.ts", ["../simulation/index.js"]),
        module("src/simulation/index.ts"),
        module("src/browser/index.ts", ["../native/index.js"]),
        module("src/native/index.ts"),
      ],
      manifest,
    );
    expect(problems.filter((problem) => problem.includes("forbidden"))).toHaveLength(4);
  });

  test("keeps host imports and undeclared dependencies out of portable source", () => {
    const problems = checkGraph(
      [
        module("src/h3/index.ts", [
          "node:fs",
          "fs/promises",
          "bun",
          "bun:ffi",
          "koffi",
          "@effect/platform-node",
          "reactor-effect-client",
          "file:///tmp/hidden.js",
        ]),
      ],
      manifest,
    );
    expect(problems).toHaveLength(8);
    expect(checkGraph([module("src/native/index.ts", ["node:module", "koffi"])], manifest)).toEqual(
      [],
    );
  });

  test("requires exact source closure without case-insensitive or workspace fallbacks", () => {
    const problems = checkGraph(
      [
        module("src/index.ts", ["./missing.js", "./Errors.js", "../test/fixture.js", "./errors"]),
        module("src/errors.ts"),
      ],
      manifest,
    );
    expect(problems).toHaveLength(4);
    expect(problems.every((problem) => problem.includes("source import"))).toBe(true);
  });

  test("rejects runtime cycles but permits non-initializing type backreferences", () => {
    const a = module("src/a.ts", ["./b.js"]);
    expect(checkGraph([a, module("src/b.ts", ["./a.js"])], manifest)).toEqual([
      "runtime import cycle: src/a.ts -> src/b.ts -> src/a.ts",
    ]);
    expect(checkGraph([a, module("src/b.ts", ["./a.js"], false)], manifest)).toEqual([]);
  });

  test("the compiler parser ignores comments/strings and catches dynamic and type-only edges", () => {
    mkdirSync(join(root, ".check"), { recursive: true });
    const temporary = mkdtempSync(join(root, ".check", "architecture-fixture-"));
    try {
      writeFileSync(join(temporary, "package.json"), JSON.stringify(manifest));
      writeFileSync(
        join(temporary, "tsconfig.build.json"),
        JSON.stringify({
          compilerOptions: {
            target: "ES2022",
            module: "NodeNext",
            moduleResolution: "NodeNext",
            types: [],
          },
          include: ["src/**/*.ts"],
        }),
      );
      for (const source of Object.values(entries)) {
        const path = join(temporary, "src", `${source}.ts`);
        mkdirSync(dirname(path), { recursive: true });
        writeFileSync(path, "export {};\n");
      }
      const entry = join(temporary, "src", "index.ts");
      writeFileSync(entry, '// import "node:fs";\nexport const prose = "from \'koffi\'";\n');
      const run = () =>
        spawnSync(
          process.env.NODE_BINARY ?? "node",
          [join(root, "scripts/architecture.mjs"), "--root", temporary],
          { cwd: root, encoding: "utf8", timeout: 30_000 },
        );
      const clean = run();
      expect(clean.error).toBeUndefined();
      expect(clean.status).toBe(0);
      expect(clean.stdout).toContain("architecture-ok");
      writeFileSync(
        entry,
        'import type { Hidden } from "./native/index.js";\nvoid import("node:fs");\nvoid import(globalThis.location.href);\n',
      );
      const broken = run();
      expect(broken.error).toBeUndefined();
      expect(broken.status).toBe(1);
      expect(broken.stderr).toContain("forbidden core -> native");
      expect(broken.stderr).toContain("Node builtin outside native boundary");
      expect(broken.stderr).toContain("module loads must have a literal specifier");
    } finally {
      // This test owns only its new tiny fixture, never an earlier delivery directory.
      rmSync(temporary, { recursive: true, force: true });
    }
  }, 60_000);
});
