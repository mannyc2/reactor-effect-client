import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, layer } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import type * as PlatformError from "effect/PlatformError";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as ChildProcess from "effect/unstable/process/ChildProcess";
import {
  ConsumerManifest,
  checkArchivePeers,
  completeQualification,
  inspectConsumerTree,
  resolveStackPackage,
  resolveWorkspaceStack,
  selectStack,
  verifiedArchiveRequirements,
  verifyInstalledArchive,
  type EffectStackError,
} from "./pack-effect-stack.js";

const baseline = "4.0.0-rc.117";
const later = "4.0.0-rc.118";
const names = ["effect", "@effect/platform-node", "@effect/platform-node-shared"] as const;
/** A root manifest whose catalog and override require the stack as `requirement`. */
const pinned = (requirement: string) => ({
  workspaces: {
    catalog: {
      effect: requirement,
      "@effect/platform-node": requirement,
      "@effect/platform-node-shared": requirement,
    },
  },
  overrides: { "@effect/platform-node-shared": requirement },
});
const manifest = pinned(baseline);
// JSONC is the on-disk contract, including comments and trailing commas.
const lock = (version = baseline, root = manifest) => `{
  "lockfileVersion": 2,
  "catalog": ${JSON.stringify(root.workspaces.catalog)},
  "overrides": ${JSON.stringify(root.overrides)},
  "packages": {
    // A frozen selection, never a registry query.
    ${names.map((name) => `${JSON.stringify(name)}: ["${name}@${version}", "", {}, "sha512-fixture"],`).join("\n")}
  },
}`;
const select = (version = baseline) => selectStack(manifest, lock(version));
/** SHA-256 by Bun's own hasher, independent of the Effect Crypto the stack hashes with. */
const bunSha256 = (bytes: string | Uint8Array) =>
  new Bun.CryptoHasher("sha256").update(bytes).digest("hex");
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
const Json = Schema.fromJsonString(Schema.Unknown);
/** A value as the JSON text `npm ls --json` or a manifest file holds. */
const json = Schema.encodeEffect(Json);

/** The message of the EffectStackError `effect` fails with. */
const rejection = Effect.fnUntraced(function* (
  effect: Effect.Effect<
    object,
    EffectStackError | PlatformError.PlatformError,
    NodeServices.NodeServices
  >,
) {
  const error = yield* Effect.flip(effect);
  assert.strictEqual(error._tag, "EffectStackError");
  return error.message;
});

const put = Effect.fnUntraced(function* (directory: string, value: unknown) {
  const fs = yield* FileSystem.FileSystem;
  const path = (yield* Path.Path).join(directory, "package.json");
  yield* fs.makeDirectory(directory, { recursive: true });
  yield* fs.writeFileString(path, yield* json(value));
  return path;
});

/** A disposable workspace whose owners hold the stack at `version`, and whose root has none. */
const workspace = Effect.fnUntraced(function* (version: string = baseline) {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const root = yield* fs.makeTempDirectoryScoped({ prefix: "pack-stack-" });
  yield* put(root, { private: true });
  for (const owner of ["client", "browser", "native"]) {
    const directory = path.join(root, "packages", owner);
    yield* put(directory, {
      name: `reactor-effect-${owner}`,
      devDependencies: {
        effect: "catalog:",
        ...(owner === "browser" ? {} : { "@effect/platform-node": "catalog:" }),
      },
    });
    for (const name of names)
      yield* put(path.join(directory, "node_modules", name), { name, version });
  }
  return root;
});

/** Runs a fixture command to completion, keeping its exit status and output. */
const execute = Effect.fnUntraced(function* (
  command: string,
  args: ReadonlyArray<string>,
  cwd: string,
) {
  const handle = yield* ChildProcess.make(command, args, {
    cwd,
    stdin: "ignore",
    stderr: "ignore",
  });
  const [stdout, status] = yield* Effect.all(
    [handle.stdout.pipe(Stream.decodeText(), Stream.mkString), handle.exitCode],
    { concurrency: "unbounded" },
  );
  return { stdout, status };
}, Effect.scoped);

layer(NodeServices.layer)("pack Effect stack", (it) => {
  it.effect("frozen JSONC selection keeps the exact requirements and hashes the bytes", () =>
    Effect.gen(function* () {
      const selected = yield* select();
      assert.deepStrictEqual(selected.selected, {
        effect: baseline,
        nodePlatform: baseline,
        nodeShared: baseline,
      });
      assert.deepStrictEqual(selected.requirements, {
        effect: baseline,
        nodePlatform: baseline,
        nodeSharedOverride: baseline,
      });
      assert.strictEqual(selected.lockfileSha256, bunSha256(lock()));
    }),
  );

  it.effect("refuses a range requirement, even one the frozen selection satisfies", () =>
    Effect.gen(function* () {
      const ranged = pinned(`^${baseline}`);
      assert.match(
        yield* rejection(selectStack(ranged, lock(baseline, ranged))),
        /effect requirement \^4\.0\.0-rc\.117 must be exactly the locked 4\.0\.0-rc\.117/,
      );
    }),
  );

  it.effect("refuses a shared-platform catalog entry its override and lock do not match", () =>
    Effect.gen(function* () {
      const drifted = {
        ...manifest,
        workspaces: {
          catalog: { ...manifest.workspaces.catalog, "@effect/platform-node-shared": later },
        },
      };
      assert.match(
        yield* rejection(selectStack(drifted, lock(baseline, drifted))),
        /shared-platform catalog entry 4\.0\.0-rc\.118 must match its override 4\.0\.0-rc\.117/,
      );
      assert.match(
        yield* rejection(selectStack(manifest, lock(baseline, drifted))),
        /lock catalog @effect\/platform-node-shared differs from manifest/,
      );
    }),
  );

  it.effect("a later aligned frozen RC qualifies only once the requirements name it", () =>
    Effect.gen(function* () {
      assert.match(yield* rejection(select(later)), /must be exactly the locked 4\.0\.0-rc\.118/);
      const stack = yield* selectStack(pinned(later), lock(later, pinned(later)));
      assert.strictEqual(stack.selected.effect, later);
      const root = yield* workspace(later);
      assert.lengthOf(yield* resolveWorkspaceStack(root, stack), 11);
      const tree = yield* json(nativeTree(later));
      assert.lengthOf((yield* inspectConsumerTree("native", "bun", tree, stack)).instances, 5);
      // A registry advancing cannot change selection when these inputs stay frozen.
      assert.strictEqual((yield* select()).selected.effect, baseline);
    }),
  );

  it.effect("hashes original lock bytes and rejects invalid UTF-8 even inside JSONC comments", () =>
    Effect.gen(function* () {
      const bytes = new TextEncoder().encode(`${lock()}\n// frozen bytes\n`);
      assert.strictEqual((yield* selectStack(manifest, bytes)).lockfileSha256, bunSha256(bytes));
      bytes[bytes.length - 2] = 255;
      assert.match(yield* rejection(selectStack(manifest, bytes)), /JSONC/);
    }),
  );

  it.effect(
    "Bun archive metadata becomes npm-valid only after exact byte verification; wrong requirements still fail",
    () =>
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        const stack = yield* select();
        const root = yield* fs.makeTempDirectoryScoped({ prefix: "pack-archive-requirement-" });
        const source = yield* put(path.join(root, "source/package"), {
          name: "effect",
          version: baseline,
        });
        const tarball = path.join(root, "effect.tgz");
        const consumer = path.join(root, "consumer");
        yield* put(consumer, { private: true, type: "module" });
        const packed = yield* execute(
          "tar",
          ["-czf", tarball, "-C", path.join(root, "source"), "package"],
          consumer,
        );
        assert.strictEqual(packed.status, 0);
        // Refuse registry access even if a future installer tries resolving this fixture.
        const installed = yield* execute(
          process.execPath,
          [
            "--no-env-file",
            "add",
            "--ignore-scripts",
            "--exact",
            "--linker=hoisted",
            "--registry=http://127.0.0.1:9",
            tarball,
          ],
          consumer,
        );
        assert.strictEqual(installed.status, 0);
        const inspect = execute("npm", ["ls", "effect", "--all", "--json"], consumer);
        const original = yield* Schema.decodeEffect(Schema.fromJsonString(ConsumerManifest))(
          yield* fs.readFileString(path.join(consumer, "package.json")),
        );
        const diagnostic = yield* inspect;
        yield* rejection(inspectConsumerTree("portable-node", "bun", diagnostic.stdout, stack));
        const packagePath = path.join(consumer, "node_modules/effect/package.json");
        const bytes = yield* fs.readFile(packagePath);
        assert.deepStrictEqual(bytes, yield* fs.readFile(source));
        const verified = yield* verifyInstalledArchive(consumer, {
          name: "effect",
          version: baseline,
          specifier: tarball,
          fileSha256: { "package.json": bunSha256(yield* fs.readFile(source)) },
        });
        assert.match(
          yield* rejection(
            verifyInstalledArchive(consumer, {
              ...verified,
              fileSha256: { "package.json": "0".repeat(64) },
            }),
          ),
          /differs from the exact archive/,
        );
        const normalized = yield* verifiedArchiveRequirements(original, [verified]);
        yield* put(consumer, normalized);
        const checked = yield* inspect;
        assert.strictEqual(checked.status, 0);
        assert.lengthOf(
          (yield* inspectConsumerTree("portable-node", "bun", checked.stdout, stack)).instances,
          1,
        );
        assert.deepStrictEqual(yield* fs.readFile(packagePath), bytes);
        assert.match(
          yield* rejection(
            verifiedArchiveRequirements(original, [
              { name: "effect", version: baseline, specifier: "wrong.tgz" },
            ]),
          ),
          /not the installed archive/,
        );
        yield* put(consumer, { ...normalized, dependencies: { effect: later } });
        const wrong = yield* inspect;
        assert.notStrictEqual(wrong.status, 0);
        yield* rejection(inspectConsumerTree("portable-node", "bun", wrong.stdout, stack));
        yield* put(path.join(consumer, "node_modules/fixture-peer"), {
          name: "fixture-peer",
          version: "1.0.0",
          peerDependencies: { effect: "^3.0.0" },
        });
        yield* put(consumer, {
          ...normalized,
          dependencies: { effect: baseline, "fixture-peer": "1.0.0" },
        });
        const invalidPeer = yield* inspect;
        assert.notStrictEqual(invalidPeer.status, 0);
        yield* rejection(inspectConsumerTree("portable-node", "bun", invalidPeer.stdout, stack));
        yield* fs.remove(path.join(consumer, "node_modules/effect"), { recursive: true });
        const missingPeer = yield* inspect;
        assert.notStrictEqual(missingPeer.status, 0);
        yield* rejection(inspectConsumerTree("portable-node", "bun", missingPeer.stdout, stack));
      }),
  );

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
    [
      "wrong tuple name",
      lock().replace(`"effect@${baseline}"`, `"other@${baseline}"`),
      /coordinate/,
    ],
    ["unsupported tuple", lock().replace('"", {}, "sha512-fixture"', '"", {}'), /tuple/],
    ["stale catalog", lock(baseline, pinned("4.0.0-rc.116")), /catalog.*differs/],
  ] as const)
    it.effect(`rejects ${label}`, () =>
      Effect.gen(function* () {
        assert.match(yield* rejection(selectStack(manifest, text)), reason);
      }),
    );

  it.effect("rejects mixed and incompatible duplicate lock coordinates", () =>
    Effect.gen(function* () {
      const text = lock().replace(
        `"@effect/platform-node@${baseline}"`,
        `"@effect/platform-node@${later}"`,
      );
      assert.match(yield* rejection(selectStack(manifest, text)), /aligned/);
      const extra = lock().replace(
        '"packages": {',
        `"packages": { "nested/effect": ["effect@${later}", "", {}, "sha512-fixture"],`,
      );
      assert.match(yield* rejection(selectStack(manifest, extra)), /conflicting.*effect/);
      assert.match(
        yield* rejection(selectStack(manifest, lock("3.0.0"))),
        /must be exactly the locked 3\.0\.0/,
      );
    }),
  );

  it.effect(
    "a scoped package named effect is not an Effect instance unless its coordinate is",
    () =>
      Effect.gen(function* () {
        for (const key of ["@acme/effect", "foo/@acme/effect", "@x/foo/@acme/effect"]) {
          const withEntry = (coordinate: string) =>
            lock().replace(
              '"packages": {',
              `"packages": { "${key}": ["${coordinate}", "", {}, "sha512-fixture"],`,
            );
          const scoped = yield* selectStack(manifest, withEntry("@acme/effect@1.0.0"));
          assert.strictEqual(scoped.selected.effect, baseline);
          assert.match(
            yield* rejection(selectStack(manifest, withEntry(`effect@${later}`))),
            /conflicting.*effect/,
          );
        }
        // A key that names a stack package is inspected even when its coordinate names
        // another package: an alias there would give that package the stack's import.
        const alias = lock().replace(
          '"packages": {',
          `"packages": { "foo/effect": ["@acme/other@1.0.0", "", {}, "sha512-fixture"],`,
        );
        assert.match(
          yield* rejection(selectStack(manifest, alias)),
          /wrong package coordinate @acme\/other/,
        );
        const nested = lock().replace(
          '"packages": {',
          `"packages": { "foo/@effect/platform-node": ["@effect/platform-node@${later}", "", {}, "sha512-fixture"],`,
        );
        assert.match(yield* rejection(selectStack(manifest, nested)), /conflicting.*platform-node/);
      }),
  );

  it.effect("owner-relative resolution succeeds without an undeclared root Effect", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const stack = yield* select();
      const root = yield* workspace();
      // The workspace root declares and holds no Effect; whatever lies above the temp directory
      // may, so each edge is checked to resolve inside the workspace, from its owner.
      assert.isFalse(yield* fs.exists(path.join(root, "node_modules")));
      const edges = yield* resolveWorkspaceStack(root, stack);
      assert.lengthOf(edges, 11);
      assert.isTrue(edges.every((edge) => edge.version === baseline));
      for (const directory of ["client", "browser", "native"]) {
        const inside = yield* fs.realPath(path.join(root, "packages", directory, "node_modules"));
        const owner = path.join(root, "packages", directory, "package.json");
        const effect = yield* resolveStackPackage(owner, "effect", stack);
        assert.isTrue(effect.path.startsWith(inside));
        if (directory === "browser") continue;
        const node = yield* resolveStackPackage(owner, "@effect/platform-node", stack);
        assert.isTrue(node.path.startsWith(inside));
        const nested = yield* resolveStackPackage(node.path, "effect", stack);
        assert.isTrue(nested.path.startsWith(inside));
      }
    }),
  );

  for (const [name, value] of [
    ["stale", { name: "effect", version: later }],
    ["missing version", { name: "effect" }],
    ["malformed version", { name: "effect", version: `^${baseline}` }],
    ["wrong name", { name: "wrong", version: baseline }],
  ] as const)
    it.effect(`rejects ${name} workspace metadata`, () =>
      Effect.gen(function* () {
        const path = yield* Path.Path;
        const stack = yield* select();
        const root = yield* workspace();
        yield* put(path.join(root, "packages/browser/node_modules/effect"), value);
        assert.match(
          yield* rejection(resolveWorkspaceStack(root, stack)),
          /packages\/browser.*effect/,
        );
      }),
    );

  for (const owner of ["@effect/platform-node", "@effect/platform-node-shared"])
    it.effect(`rejects nested Effect resolved by ${owner}`, () =>
      Effect.gen(function* () {
        const path = yield* Path.Path;
        const stack = yield* select();
        const root = yield* workspace();
        yield* put(path.join(root, "packages/client/node_modules", owner, "node_modules/effect"), {
          name: "effect",
          version: later,
        });
        assert.match(yield* rejection(resolveWorkspaceStack(root, stack)), /effect.*expected/);
      }),
    );

  it.effect("rejects nested shared-platform", () =>
    Effect.gen(function* () {
      const path = yield* Path.Path;
      const stack = yield* select();
      const root = yield* workspace();
      const nested = path.join(
        root,
        "packages/native/node_modules/@effect/platform-node/node_modules/@effect/platform-node-shared",
      );
      yield* put(nested, { name: "@effect/platform-node-shared", version: later });
      assert.match(
        yield* rejection(resolveWorkspaceStack(root, stack)),
        /platform-node-shared.*expected/,
      );
    }),
  );

  it.effect("rejects missing declared workspace dependency", () =>
    Effect.gen(function* () {
      const path = yield* Path.Path;
      const stack = yield* select();
      const root = yield* workspace();
      yield* put(path.join(root, "packages/browser"), { name: "reactor-effect-browser" });
      assert.match(yield* rejection(resolveWorkspaceStack(root, stack)), /declare.*effect/);
    }),
  );

  it.effect("rejects a workspace owner's range, even one the selection satisfies", () =>
    Effect.gen(function* () {
      const path = yield* Path.Path;
      const stack = yield* select();
      const root = yield* workspace();
      yield* put(path.join(root, "packages/browser"), {
        name: "reactor-effect-browser",
        devDependencies: { effect: `^${baseline}` },
      });
      assert.match(
        yield* rejection(resolveWorkspaceStack(root, stack)),
        /packages\/browser declared effect requirement differs from frozen selection/,
      );
    }),
  );

  it.effect("an archive peers on Effect and every stack package at exactly the selection", () =>
    Effect.gen(function* () {
      const stack = yield* select();
      const native = {
        effect: baseline,
        "@effect/platform-node": baseline,
        "@effect/platform-node-shared": baseline,
        "reactor-effect-client": "0.8.0",
      };
      yield* checkArchivePeers("reactor-effect-native", native, stack);
      const refused = (name: string, peers: Readonly<Record<string, string>>) =>
        Effect.flip(checkArchivePeers(name, peers, stack)).pipe(
          Effect.map((error) => error.message),
        );
      assert.match(
        yield* refused("reactor-effect-client", { effect: `^${baseline}` }),
        /reactor-effect-client must pin its effect peer to exactly 4\.0\.0-rc\.117, not \^4\.0\.0-rc\.117/,
      );
      assert.match(
        yield* refused("reactor-effect-native", {
          ...native,
          "@effect/platform-node": `^${baseline}`,
        }),
        /its @effect\/platform-node peer to exactly 4\.0\.0-rc\.117/,
      );
      assert.match(
        yield* refused("reactor-effect-browser", { "reactor-effect-client": "0.8.0" }),
        /reactor-effect-browser must declare its effect peer/,
      );
    }),
  );

  it.effect(
    "consumer validation retains repeated aligned occurrences and rejects nested mismatches",
    () =>
      Effect.gen(function* () {
        const stack = yield* select();
        const tree = nativeTree();
        const aligned = yield* inspectConsumerTree("native", "bun", yield* json(tree), stack);
        assert.lengthOf(aligned.instances, 5);
        tree.dependencies["@effect/platform-node"].dependencies.effect.version = later;
        assert.match(
          yield* rejection(inspectConsumerTree("native", "bun", yield* json(tree), stack)),
          /native.*effect.*expected/,
        );
      }),
  );

  for (const tree of [
    null,
    [],
    { dependencies: [] },
    { dependencies: { effect: {} } },
    { dependencies: { effect: { version: `^${baseline}` } } },
    { dependencies: { effect: { version: baseline, dependencies: null } } },
    { problems: ["invalid peer"], ...nativeTree() },
  ])
    it.effect(`rejects malformed consumer tree ${JSON.stringify(tree)}`, () =>
      Effect.gen(function* () {
        const stack = yield* select();
        assert.match(
          yield* rejection(inspectConsumerTree("native", "bun", yield* json(tree), stack)),
          /native/,
        );
      }),
    );

  it.effect("consumer kind requires its complete stack and selected exact version", () =>
    Effect.gen(function* () {
      const stack = yield* select();
      const portable = yield* json({ dependencies: { effect: { version: baseline } } });
      assert.lengthOf((yield* inspectConsumerTree("browser", "bun", portable, stack)).instances, 1);
      assert.match(
        yield* rejection(inspectConsumerTree("native", "bun", portable, stack)),
        /missing.*platform-node/,
      );
      assert.match(
        yield* rejection(inspectConsumerTree("browser", "bun", yield* json({}), stack)),
        /missing.*effect/,
      );
      assert.match(
        yield* rejection(
          inspectConsumerTree("native", "bun", yield* json(nativeTree(later)), stack),
        ),
        /expected/,
      );
    }),
  );

  it.effect(
    "complete evidence requires every successful consumer and preserves lock and resolution records",
    () =>
      Effect.gen(function* () {
        const stack = yield* select();
        const workspaceResolution = yield* resolveWorkspaceStack(yield* workspace(), stack);
        const portable = yield* inspectConsumerTree(
          "portable-node",
          "bun",
          yield* json({ dependencies: { effect: { version: baseline } } }),
          stack,
        );
        const browser = { ...portable, name: "browser" as const };
        const native = yield* inspectConsumerTree(
          "native",
          "bun",
          yield* json(nativeTree()),
          stack,
        );
        assert.match(
          yield* rejection(
            completeQualification(stack, workspaceResolution, [portable], "portable"),
          ),
          /completed consumers/,
        );
        assert.match(
          yield* rejection(
            completeQualification(stack, workspaceResolution, [portable, browser], "full"),
          ),
          /completed consumers/,
        );
        assert.match(
          yield* rejection(completeQualification(stack, [], [portable, browser, native], "full")),
          /workspace/,
        );
        const completed = yield* completeQualification(
          stack,
          workspaceResolution,
          [portable, browser, native],
          "full",
        );
        assert.strictEqual(completed.format, "reactor-effect-qualification-stack/v1");
        assert.strictEqual(completed.selection, "frozen-workspace");
        assert.strictEqual(completed.lockfileSha256, stack.lockfileSha256);
        assert.deepStrictEqual(completed.consumers, [portable, browser, native]);
        assert.deepStrictEqual(completed.workspaceResolution, workspaceResolution);
      }),
  );
});
