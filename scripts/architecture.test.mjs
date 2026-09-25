import { describe, expect, test } from "bun:test";
import { spawnSync } from "node:child_process";
import { mkdtempSync, mkdirSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { checkGraph, checkManifest, checkWorkspace, rules } from "./architecture.mjs";

const root = fileURLToPath(new URL("../", import.meta.url));
/** @param {string} path */
const manifestOf = (path) => JSON.parse(readFileSync(join(root, path), "utf8"));
const workspace = manifestOf("package.json");
const client = manifestOf("packages/client/package.json");
const native = manifestOf("packages/native/package.json");
/** @param {string} name */
const ruleOf = (name) => {
  const rule = rules[name];
  if (rule === undefined) throw new Error(`no architecture rules for ${name}`);
  return rule;
};
const clientRule = ruleOf("reactor-effect-client");
const nativeRule = ruleOf("reactor-effect-native");
/** @param {string} path @param {string[]} imports @param {boolean} [runtime] */
const module = (path, imports = [], runtime = true) => ({
  path,
  imports: imports.map((specifier) => ({ specifier, runtime, line: 1 })),
  problems: [],
});
const entries = Object.values(clientRule.entries).map((source) => module(`src/${source}.ts`));

describe("architecture contract", () => {
  test("keeps the exact public targets and the catalog Effect pin", () => {
    expect(checkWorkspace(workspace)).toEqual([]);
    expect(
      checkWorkspace({ ...workspace, workspaces: { catalog: { effect: "latest" } } }),
    ).toContain("workspace catalog must pin effect to ^4.0.0-rc.117");
    expect(checkManifest(client, clientRule)).toEqual([]);
    expect(checkManifest(native, nativeRule)).toEqual([]);
    expect(
      checkManifest({ ...client, exports: { ...client.exports, "./private": {} } }, clientRule),
    ).toContain("reactor-effect-client must retain exactly its public exports");
    expect(
      checkManifest({ ...client, peerDependencies: { effect: "latest" } }, clientRule),
    ).toContain("peerDependencies.effect must be pinned through the workspace catalog");
    expect(
      checkManifest(
        { ...client, exports: { ...client.exports, ".": { import: "./dist/other.js" } } },
        clientRule,
      ),
    ).toContain("public export . must retain its types/import targets");
    expect(
      checkManifest(
        {
          ...native,
          peerDependencies: { ...native.peerDependencies, "reactor-effect-client": "0.2.0" },
        },
        nativeRule,
      ),
    ).toContain("peerDependencies.reactor-effect-client must use the workspace protocol");
  });

  test("rejects upward policy dependencies, including type-only imports", () => {
    const problems = checkGraph(
      [
        ...entries,
        module("src/session.ts", ["./h3/types.js"], false),
        module("src/h3/types.ts", ["../orchestration/types.js"], false),
        module("src/orchestration/types.ts", ["../simulation/index.js"]),
        module("src/simulation/faults.ts", ["../testing/index.js"]),
      ],
      client,
      clientRule,
    );
    expect(problems.filter((problem) => problem.includes("forbidden"))).toHaveLength(4);
  });

  test("keeps host imports and undeclared dependencies out of portable source", () => {
    const problems = checkGraph(
      [
        ...entries,
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
      client,
      clientRule,
    );
    expect(problems).toHaveLength(8);
    expect(
      checkGraph(
        [module("src/index.ts", ["node:module", "koffi", "reactor-effect-client/host"])],
        native,
        nativeRule,
      ),
    ).toEqual([]);
  });

  test("requires exact source closure without case-insensitive or workspace fallbacks", () => {
    const problems = checkGraph(
      [
        ...entries.filter((entry) => entry.path !== "src/index.ts"),
        module("src/index.ts", ["./missing.js", "./Errors.js", "../test/fixture.js", "./errors"]),
        module("src/errors.ts"),
      ],
      client,
      clientRule,
    );
    expect(problems).toHaveLength(4);
    expect(problems.every((problem) => problem.includes("source import"))).toBe(true);
  });

  test("rejects runtime cycles but permits non-initializing type backreferences", () => {
    const a = module("src/a.ts", ["./b.js"]);
    expect(checkGraph([...entries, a, module("src/b.ts", ["./a.js"])], client, clientRule)).toEqual(
      ["runtime import cycle: src/a.ts -> src/b.ts -> src/a.ts"],
    );
    expect(
      checkGraph([...entries, a, module("src/b.ts", ["./a.js"], false)], client, clientRule),
    ).toEqual([]);
  });

  test("the compiler parser ignores comments/strings and catches dynamic and type-only edges", () => {
    mkdirSync(join(root, ".check"), { recursive: true });
    const temporary = mkdtempSync(join(root, ".check", "architecture-fixture-"));
    try {
      writeFileSync(join(temporary, "package.json"), JSON.stringify(client));
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
      for (const source of Object.values(clientRule.entries)) {
        const path = join(temporary, "src", `${source}.ts`);
        mkdirSync(dirname(path), { recursive: true });
        writeFileSync(path, "export {};\n");
      }
      const entry = join(temporary, "src", "index.ts");
      writeFileSync(entry, '// import "node:fs";\nexport const prose = "from \'koffi\'";\n');
      const run = () =>
        spawnSync(
          process.env.NODE_BINARY ?? "node",
          [join(root, "scripts/architecture.mjs"), "--package", temporary],
          { cwd: root, encoding: "utf8", timeout: 30_000 },
        );
      const clean = run();
      expect(clean.error).toBeUndefined();
      expect(clean.status).toBe(0);
      expect(clean.stdout).toContain("architecture-ok");
      writeFileSync(
        entry,
        'import type { Hidden } from "./h3/index.js";\nvoid import("node:fs");\nvoid import(globalThis.location.href);\n',
      );
      const broken = run();
      expect(broken.error).toBeUndefined();
      expect(broken.status).toBe(1);
      expect(broken.stderr).toContain("forbidden core -> h3");
      expect(broken.stderr).toContain("Node builtin outside native boundary");
      expect(broken.stderr).toContain("module loads must have a literal specifier");
    } finally {
      // This test owns only its new tiny fixture, never an earlier delivery directory.
      rmSync(temporary, { recursive: true, force: true });
    }
  }, 60_000);
});
