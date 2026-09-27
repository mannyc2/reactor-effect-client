import { test, expect } from "bun:test";
import { createHash } from "node:crypto";
import { mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { spawnSync } from "node:child_process";
import { createRequire } from "node:module";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import {
  completeQualification,
  inspectConsumerTree,
  resolveWorkspaceStack,
  selectStack,
  verifiedArchiveRequirements,
  verifyInstalledArchive,
} from "./pack-effect-stack.js";

const baseline = "4.0.0-rc.117";
const later = "4.0.0-rc.118";
const requirement = `^${baseline}`;
const names = ["effect", "@effect/platform-node", "@effect/platform-node-shared"] as const;
const manifest = {
  workspaces: { catalog: { effect: requirement, "@effect/platform-node": requirement } },
  overrides: { "@effect/platform-node-shared": requirement },
};
// JSONC is the on-disk contract, including comments and trailing commas.
const lock = (version = baseline) => `{
  "lockfileVersion": 2,
  "catalog": ${JSON.stringify(manifest.workspaces.catalog)},
  "overrides": ${JSON.stringify(manifest.overrides)},
  "packages": {
    // A frozen selection, never a registry query.
    ${names.map((name) => `${JSON.stringify(name)}: ["${name}@${version}", "", {}, "sha512-fixture"],`).join("\n")}
  },
}`;
const select = (version = baseline) => selectStack(manifest, lock(version));
const nativeTree = (version = baseline) => ({
  dependencies: {
    effect: { version },
    "@effect/platform-node": {
      version,
      dependencies: {
        effect: { version },
        "@effect/platform-node-shared": { version, dependencies: { effect: { version } } },
      },
    },
  },
});
const put = (directory: string, value: unknown): string => {
  const path = join(directory, "package.json");
  mkdirSync(dirname(path), { recursive: true });
  writeFileSync(path, JSON.stringify(value));
  return path;
};
const withWorkspace = (body: (root: string) => void, version = baseline): void => {
  const root = mkdtempSync(join(tmpdir(), "pack-stack-"));
  try {
    put(root, { private: true });
    for (const owner of ["client", "browser", "native"]) {
      const directory = join(root, "packages", owner);
      put(directory, {
        name: `reactor-effect-${owner}`,
        devDependencies: {
          effect: "catalog:",
          ...(owner === "browser" ? {} : { "@effect/platform-node": "catalog:" }),
        },
      });
      for (const name of names) put(join(directory, "node_modules", name), { name, version });
    }
    body(root);
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
};

test("frozen JSONC selection keeps exact versions separate from compatibility ranges and hashes the bytes", () => {
  const selected = select();
  expect(selected.selected).toEqual({
    effect: baseline,
    nodePlatform: baseline,
    nodeShared: baseline,
  });
  expect(selected.requirements).toEqual({
    effect: requirement,
    nodePlatform: requirement,
    nodeSharedOverride: requirement,
  });
  expect(selected.lockfileSha256).toBe(createHash("sha256").update(lock()).digest("hex"));
});

test("a later aligned frozen RC above the range minimum qualifies", () => {
  const stack = select(later);
  expect(stack.selected.effect).toBe(later);
  withWorkspace((root) => {
    expect(resolveWorkspaceStack(root, stack)).toHaveLength(11);
    expect(inspectConsumerTree("native", "bun", nativeTree(later), stack).instances).toHaveLength(
      5,
    );
  }, later);
  // A registry advancing cannot change selection when these inputs stay frozen.
  expect(select().selected.effect).toBe(baseline);
});

test("hashes original lock bytes and rejects invalid UTF-8 even inside JSONC comments", () => {
  const bytes = new TextEncoder().encode(`${lock()}\n// frozen bytes\n`);
  expect(selectStack(manifest, bytes).lockfileSha256).toBe(
    createHash("sha256").update(bytes).digest("hex"),
  );
  bytes[bytes.length - 2] = 255;
  expect(() => selectStack(manifest, bytes)).toThrow(/JSONC/);
});

test("Bun archive metadata becomes npm-valid only after exact byte verification; wrong requirements still fail", () => {
  const root = mkdtempSync(join(tmpdir(), "pack-archive-requirement-"));
  try {
    const source = put(join(root, "source/package"), { name: "effect", version: baseline });
    const tarball = join(root, "effect.tgz");
    const consumer = join(root, "consumer");
    put(consumer, { private: true, type: "module" });
    const run = (command: string, args: string[]) =>
      spawnSync(command, args, { cwd: consumer, encoding: "utf8" });
    expect(run("tar", ["-czf", tarball, "-C", join(root, "source"), "package"]).status).toBe(0);
    // Refuse registry access even if a future installer tries resolving this fixture.
    const installed = run(process.execPath, [
      "--no-env-file",
      "add",
      "--ignore-scripts",
      "--exact",
      "--linker=hoisted",
      "--registry=http://127.0.0.1:9",
      tarball,
    ]);
    expect(installed.status).toBe(0);
    const inspect = () => run("npm", ["ls", "effect", "--all", "--json"]);
    const original = JSON.parse(readFileSync(join(consumer, "package.json"), "utf8")) as unknown;
    const diagnostic = inspect();
    expect(() =>
      inspectConsumerTree("portable-node", "bun", JSON.parse(diagnostic.stdout), select()),
    ).toThrow();
    const packagePath = join(consumer, "node_modules/effect/package.json");
    const bytes = readFileSync(packagePath);
    expect(bytes).toEqual(readFileSync(source));
    const verified = verifyInstalledArchive(consumer, {
      name: "effect",
      version: baseline,
      specifier: tarball,
      fileSha256: {
        "package.json": createHash("sha256").update(readFileSync(source)).digest("hex"),
      },
    });
    expect(() =>
      verifyInstalledArchive(consumer, {
        ...verified,
        fileSha256: { "package.json": "0".repeat(64) },
      }),
    ).toThrow(/differs from the exact archive/);
    const normalized = verifiedArchiveRequirements(original, [verified]);
    put(consumer, normalized);
    const checked = inspect();
    expect(checked.status).toBe(0);
    expect(
      inspectConsumerTree("portable-node", "bun", JSON.parse(checked.stdout), select()).instances,
    ).toHaveLength(1);
    expect(readFileSync(packagePath)).toEqual(bytes);
    expect(() =>
      verifiedArchiveRequirements(original, [
        { name: "effect", version: baseline, specifier: "wrong.tgz" },
      ]),
    ).toThrow(/not the installed archive/);
    put(consumer, { ...normalized, dependencies: { effect: later } });
    const wrong = inspect();
    expect(wrong.status).not.toBe(0);
    expect(() =>
      inspectConsumerTree("portable-node", "bun", JSON.parse(wrong.stdout), select()),
    ).toThrow();
    put(join(consumer, "node_modules/fixture-peer"), {
      name: "fixture-peer",
      version: "1.0.0",
      peerDependencies: { effect: "^3.0.0" },
    });
    put(consumer, { ...normalized, dependencies: { effect: baseline, "fixture-peer": "1.0.0" } });
    const invalidPeer = inspect();
    expect(invalidPeer.status).not.toBe(0);
    expect(() =>
      inspectConsumerTree("portable-node", "bun", JSON.parse(invalidPeer.stdout), select()),
    ).toThrow();
    rmSync(join(consumer, "node_modules/effect"), { recursive: true });
    const missingPeer = inspect();
    expect(missingPeer.status).not.toBe(0);
    expect(() =>
      inspectConsumerTree("portable-node", "bun", JSON.parse(missingPeer.stdout), select()),
    ).toThrow();
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});

for (const [label, text, reason] of [
  [
    "unsupported lock",
    lock().replace('"lockfileVersion": 2', '"lockfileVersion": 3'),
    /lockfileVersion/,
  ],
  ["invalid JSONC", "{ broken", /JSONC/],
  ["missing tuple", lock().replace('"effect": [', '"unrelated": ['), /missing.*effect/],
  ["range coordinate", lock(`^${baseline}`), /exact version/],
  ["partial coordinate", lock("4.0"), /exact version/],
  ["malformed coordinate", lock("garbage"), /exact version/],
  ["wrong tuple name", lock().replace(`"effect@${baseline}"`, `"other@${baseline}"`), /coordinate/],
  ["unsupported tuple", lock().replace('"", {}, "sha512-fixture"', '"", {}'), /tuple/],
  ["stale catalog", lock().replaceAll(requirement, "^4.0.0-rc.116"), /catalog.*differs/],
] as const)
  test(`rejects ${label}`, () => expect(() => selectStack(manifest, text)).toThrow(reason));

test("rejects mixed and incompatible duplicate lock coordinates", () => {
  const text = lock().replace(
    `"@effect/platform-node@${baseline}"`,
    `"@effect/platform-node@${later}"`,
  );
  expect(() => selectStack(manifest, text)).toThrow(/aligned/);
  const extra = lock().replace(
    '"packages": {',
    `"packages": { "nested/effect": ["effect@${later}", "", {}, "sha512-fixture"],`,
  );
  expect(() => selectStack(manifest, extra)).toThrow(/conflicting.*effect/);
  expect(() => selectStack(manifest, lock("3.0.0"))).toThrow(/satisfy/);
});

test("a scoped package named effect is not an Effect instance unless its coordinate is", () => {
  for (const key of ["@acme/effect", "foo/@acme/effect", "@x/foo/@acme/effect"]) {
    const withEntry = (coordinate: string) =>
      selectStack(
        manifest,
        lock().replace(
          '"packages": {',
          `"packages": { "${key}": ["${coordinate}", "", {}, "sha512-fixture"],`,
        ),
      );
    expect(withEntry("@acme/effect@1.0.0").selected.effect).toBe(baseline);
    expect(() => withEntry(`effect@${later}`)).toThrow(/conflicting.*effect/);
  }
  // A key that names a stack package is inspected even when its coordinate names
  // another package: an alias there would give that package the stack's import.
  const alias = lock().replace(
    '"packages": {',
    `"packages": { "foo/effect": ["@acme/other@1.0.0", "", {}, "sha512-fixture"],`,
  );
  expect(() => selectStack(manifest, alias)).toThrow(/wrong package coordinate @acme\/other/);
  const nested = lock().replace(
    '"packages": {',
    `"packages": { "foo/@effect/platform-node": ["@effect/platform-node@${later}", "", {}, "sha512-fixture"],`,
  );
  expect(() => selectStack(manifest, nested)).toThrow(/conflicting.*platform-node/);
});

test("owner-relative resolution succeeds without an undeclared root Effect", () =>
  withWorkspace((root) => {
    expect(() =>
      createRequire(join(root, "package.json")).resolve("effect/package.json"),
    ).toThrow();
    const edges = resolveWorkspaceStack(root, select());
    expect(edges).toHaveLength(11);
    expect(edges.every((edge) => edge.version === baseline && !edge.owner.includes(root))).toBe(
      true,
    );
  }));

for (const [name, value] of [
  ["stale", { name: "effect", version: later }],
  ["missing version", { name: "effect" }],
  ["malformed version", { name: "effect", version: `^${baseline}` }],
  ["wrong name", { name: "wrong", version: baseline }],
] as const)
  test(`rejects ${name} workspace metadata`, () =>
    withWorkspace((root) => {
      put(join(root, "packages/browser/node_modules/effect"), value);
      expect(() => resolveWorkspaceStack(root, select())).toThrow(/packages\/browser.*effect/);
    }));

for (const owner of ["@effect/platform-node", "@effect/platform-node-shared"])
  test(`rejects nested Effect resolved by ${owner}`, () =>
    withWorkspace((root) => {
      put(join(root, "packages/client/node_modules", owner, "node_modules/effect"), {
        name: "effect",
        version: later,
      });
      expect(() => resolveWorkspaceStack(root, select())).toThrow(/effect.*expected/);
    }));

test("rejects nested shared-platform", () =>
  withWorkspace((root) => {
    const nested = join(
      root,
      "packages/native/node_modules/@effect/platform-node/node_modules/@effect/platform-node-shared",
    );
    put(nested, { name: "@effect/platform-node-shared", version: later });
    expect(() => resolveWorkspaceStack(root, select())).toThrow(/platform-node-shared.*expected/);
  }));

test("rejects missing declared workspace dependency", () =>
  withWorkspace((root) => {
    put(join(root, "packages/browser"), { name: "reactor-effect-browser" });
    expect(() => resolveWorkspaceStack(root, select())).toThrow(/declare.*effect/);
  }));

test("consumer validation retains repeated aligned occurrences and rejects nested mismatches", () => {
  const tree = nativeTree();
  expect(inspectConsumerTree("native", "bun", tree, select()).instances).toHaveLength(5);
  tree.dependencies["@effect/platform-node"].dependencies.effect.version = later;
  expect(() => inspectConsumerTree("native", "bun", tree, select())).toThrow(
    /native.*effect.*expected/,
  );
});

for (const tree of [
  null,
  [],
  { dependencies: [] },
  { dependencies: { effect: {} } },
  { dependencies: { effect: { version: `^${baseline}` } } },
  { dependencies: { effect: { version: baseline, dependencies: null } } },
  { problems: ["invalid peer"], ...nativeTree() },
])
  test(`rejects malformed consumer tree ${JSON.stringify(tree)}`, () => {
    expect(() => inspectConsumerTree("native", "bun", tree, select())).toThrow(/native/);
  });

test("consumer kind requires its complete stack and selected exact version", () => {
  const portable = { dependencies: { effect: { version: baseline } } };
  expect(inspectConsumerTree("browser", "bun", portable, select()).instances).toHaveLength(1);
  expect(() => inspectConsumerTree("native", "bun", portable, select())).toThrow(
    /missing.*platform-node/,
  );
  expect(() => inspectConsumerTree("browser", "bun", {}, select())).toThrow(/missing.*effect/);
  expect(() => inspectConsumerTree("native", "bun", nativeTree(later), select())).toThrow(
    /expected/,
  );
});

test("complete evidence requires every successful consumer and preserves lock and resolution records", () =>
  withWorkspace((root) => {
    const stack = select();
    const workspace = resolveWorkspaceStack(root, stack);
    const portable = inspectConsumerTree(
      "portable-node",
      "bun",
      { dependencies: { effect: { version: baseline } } },
      stack,
    );
    const browser = { ...portable, name: "browser" as const };
    const native = inspectConsumerTree("native", "bun", nativeTree(), stack);
    expect(() => completeQualification(stack, workspace, [portable], "portable")).toThrow(
      /completed consumers/,
    );
    expect(() => completeQualification(stack, workspace, [portable, browser], "full")).toThrow(
      /completed consumers/,
    );
    expect(() => completeQualification(stack, [], [portable, browser, native], "full")).toThrow(
      /workspace/,
    );
    const completed = completeQualification(stack, workspace, [portable, browser, native], "full");
    expect(completed.format).toBe("reactor-effect-qualification-stack/v1");
    expect(completed.selection).toBe("frozen-workspace");
    expect(completed.lockfileSha256).toBe(stack.lockfileSha256);
    expect(completed.consumers).toEqual([portable, browser, native]);
    expect(completed.workspaceResolution).toEqual(workspace);
  }));
