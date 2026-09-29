/**
 * Installed-package qualification for the public workspace packages.
 *
 * Every package is packed with `bun pm pack`, which rewrites `workspace:` and
 * `catalog:` protocols to exact versions. Each archive is validated on its own
 * (public exports, declaration/import closure, declared dependencies, native
 * identity), then installed into isolated consumers: a fresh Node consumer in
 * which npm takes Effect from the client's published peer, as an application's
 * own install does, a portable Node consumer without optional dependencies, a
 * browser consumer bundled without Node globals, and a native consumer that
 * verifies the installed addon's identity.
 * Each staged platform addon is packed as its own package. `--portable` packs
 * and checks only the client and browser packages, for hosts without a staged
 * addon; CI and release run the full gate.
 */
import { builtinModules } from "node:module";
import * as NodeRuntime from "@effect/platform-node/NodeRuntime";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as Config from "effect/Config";
import * as Console from "effect/Console";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import type * as PlatformError from "effect/PlatformError";
import * as Schema from "effect/Schema";
import * as Stdio from "effect/Stdio";
import * as Stream from "effect/Stream";
import * as ChildProcess from "effect/unstable/process/ChildProcess";
import * as ChildProcessSpawner from "effect/unstable/process/ChildProcessSpawner";
import {
  ConsumerManifest,
  QualificationStack,
  checkArchivePeers,
  completeQualification,
  inspectConsumerTree,
  resolveStackPackage,
  resolveWorkspaceStack,
  selectStack,
  sha256,
  verifiedArchiveRequirements,
  verifyInstalledArchive,
  type ConsumerResolution,
} from "./pack-effect-stack.js";
import { runtimes } from "./subprocess.js";

class PackError extends Schema.TaggedError<PackError>("reactor-effect/scripts/pack/PackError")(
  "PackError",
  { message: Schema.String },
) {}

const failure = (message: string) => PackError.make({ message });

const Dependencies = Schema.Record(Schema.String, Schema.String);

/** What pack reads of the root manifest: the catalog that pins the consumers' tools. */
const WorkspaceManifest = Schema.fromJsonString(
  Schema.Struct({
    workspaces: Schema.Struct({ catalog: Dependencies }),
    overrides: Schema.optionalKey(Dependencies),
  }),
);

/** What pack reads of a package's manifest; a platform package names its addon as `main`. */
const PackageManifest = Schema.fromJsonString(
  Schema.Struct({
    name: Schema.String,
    version: Schema.String,
    main: Schema.optionalKey(Schema.String),
    /** Absent from a platform package, which only carries its addon. */
    exports: Schema.optionalKey(Schema.Record(Schema.String, Schema.NullOr(Schema.String))),
    dependencies: Schema.optionalKey(Dependencies),
    peerDependencies: Schema.optionalKey(Dependencies),
    optionalDependencies: Schema.optionalKey(Dependencies),
    devDependencies: Schema.optionalKey(Dependencies),
  }),
);
type Manifest = typeof PackageManifest.Type;

/** The pinned TypeScript's manifest, which declares each host's compiler package. */
const CompilerManifest = Schema.fromJsonString(
  Schema.Struct({ version: Schema.String, optionalDependencies: Schema.optionalKey(Dependencies) }),
);

const JsonText = Schema.fromJsonString(Schema.Json);

/** What pack checks of the identity staging wrote beside a platform package's addon. */
const NativeIdentity = Schema.Struct({
  schemaVersion: Schema.Literal(2),
  /** The platform package's suffix, such as `linux-x64-gnu`. */
  platform: Schema.String,
  /** The addon file in that package. */
  file: Schema.String,
  sha256: Schema.String,
  build: Schema.Struct({
    sourceSha256: Schema.String,
    profile: Schema.Literal("release"),
    webrtcPrebuilt: Schema.String.check(Schema.isPattern(/^webrtc-\d+-[0-9a-f]{8}-p\d+$/)),
  }),
});

/** A consumer's manifest before its install. */
const NewConsumerManifest = Schema.fromJsonString(
  Schema.Struct({
    private: Schema.Literal(true),
    type: Schema.Literal("module"),
  }),
  { space: 2 },
);

const ConsumerManifestJson = Schema.fromJsonString(ConsumerManifest, { space: 2 });

/** The record of rewriting a Bun consumer's archive requirements to exact versions. */
const Normalization = Schema.fromJsonString(
  Schema.Struct({
    format: Schema.Literal("reactor-pack-archive-requirements/v1"),
    installer: Schema.Literal("bun"),
    reason: Schema.String,
    originalManifest: Schema.String,
    normalizedManifest: Schema.String,
    verification: Schema.Literal("complete-installed-archive-byte-identity"),
    archives: Schema.Array(
      Schema.Struct({
        name: Schema.String,
        version: Schema.String,
        installSpecifier: Schema.String,
        sha256: Schema.String,
        filesChecked: Schema.Int,
      }),
    ),
  }),
  { space: 2 },
);

const TypeScriptConfig = Schema.fromJsonString(
  Schema.Struct({
    compilerOptions: Schema.Record(Schema.String, Schema.Json),
    include: Schema.Array(Schema.String),
  }),
  { space: 2 },
);

/**
 * package-identity.json: every archive with its exports and file hashes, the Effect stack its
 * consumers qualified, and each platform addon's identity. CI and release read it back.
 */
const PackageIdentity = Schema.fromJsonString(
  Schema.Struct({
    profile: Schema.Literals(["portable", "full"]),
    installer: Schema.Literals(["npm", "bun"]),
    effect: Schema.String,
    qualificationStack: QualificationStack,
    packages: Schema.Record(
      Schema.String,
      Schema.Struct({
        version: Schema.String,
        tarball: Schema.String,
        sha256: Schema.String,
        exports: Schema.Array(Schema.String),
        files: Schema.Array(Schema.String),
        fileSha256: Schema.Record(Schema.String, Schema.String),
      }),
    ),
    nativeSourceSha256: Schema.NullOr(Schema.String),
    native: Schema.Record(Schema.String, Schema.Json),
  }),
  { space: 2 },
);

interface Archive {
  /** Workspace directory name under packages/. */
  readonly directory: string;
  readonly manifest: Manifest;
  readonly tarball: string;
  readonly installTarball: string;
  readonly sha256: string;
  readonly files: ReadonlySet<string>;
  readonly fileSha256: Readonly<Record<string, string>>;
}

/** Every public package's export map, as Effect's packages write theirs. */
const exportMap: Readonly<Record<string, string | null>> = {
  "./package.json": "./package.json",
  ".": "./dist/index.js",
  "./*": "./dist/*.js",
  "./internal/*": null,
  "./index": null,
};
const exportMapText = JSON.stringify(exportMap);
/** The same entries in the same order. */
const isExportMap = (exports: Readonly<Record<string, string | null>> | undefined): boolean => {
  const actual = Object.entries(exports ?? {});
  const expected = Object.entries(exportMap);
  return (
    actual.length === expected.length &&
    actual.every(
      ([key, value], index) => expected[index]?.[0] === key && expected[index][1] === value,
    )
  );
};
/** The modules `"./*"` reaches: each top-level `dist/<Module>.js`, `index` included. */
const publicModules = (files: ReadonlySet<string>): ReadonlyArray<string> =>
  [...files]
    .filter((path) => /^dist\/[^/]+\.js$/.test(path))
    .map((path) => path.slice("dist/".length, -".js".length))
    .sort();

/** Each published addon platform package, by the Node host it runs on. */
const addonPlatforms: Readonly<Record<string, string>> = {
  "darwin-arm64": "darwin-arm64",
  "linux-x64": "linux-x64-gnu",
};
const platformPrefix = "reactor-effect-native-";
const isPlatformPackage = (name: string): boolean => name.startsWith(platformPrefix);
const addonFile = (target: string): string => `reactor-effect-native.${target}.node`;
/** The public entries a release reviews: `.` and `./<Module>` for every other module. */
const publicEntries = (archive: Archive): ReadonlyArray<string> =>
  isPlatformPackage(archive.manifest.name)
    ? []
    : publicModules(archive.files).map((module) => (module === "index" ? "." : `./${module}`));
const builtins = new Set([...builtinModules, ...builtinModules.map((name) => `node:${name}`)]);
const packageName = (specifier: string): string =>
  specifier.startsWith("@")
    ? specifier.split("/").slice(0, 2).join("/")
    : (specifier.split("/", 1)[0] ?? specifier);
const specifiers = (source: string): ReadonlyArray<string> => {
  const found = new Set<string>();
  for (const expression of [
    /\bfrom\s+["']([^"']+)["']/g,
    /\bimport\s*\(\s*["']([^"']+)["']/g,
    /\bimport\s+["']([^"']+)["']/g,
    /\brequire\s*\(\s*["']([^"']+)["']/g,
  ]) {
    for (const match of source.matchAll(expression))
      if (match[1] !== undefined) found.add(match[1]);
  }
  return [...found];
};

/** The examples import `.ts` files, as Node's type stripping runs them; emit rewrites them. */
const exampleCompilerOptions = {
  allowImportingTsExtensions: true,
  rewriteRelativeImportExtensions: true,
  erasableSyntaxOnly: true,
  verbatimModuleSyntax: true,
};

const nodeCompilerOptions = {
  target: "ES2022",
  module: "NodeNext",
  moduleResolution: "NodeNext",
  lib: ["ES2023", "ESNext.Disposable"],
  strict: true,
  skipLibCheck: false,
  exactOptionalPropertyTypes: true,
  noUncheckedIndexedAccess: true,
  types: ["node"],
  typeRoots: ["./node_modules/@types"],
  ...exampleCompilerOptions,
};

/** The globals a fixture's host gives it: Node's, or a browser's and none of Node's. */
const fixtureHosts = {
  node: ["--types", "node", "--lib", "ES2023,DOM,ESNext.Disposable"],
  // An empty list, so that no installed type package stands in for the browser.
  browser: ["--types", "", "--lib", "ES2023,DOM,DOM.Iterable,ESNext.Disposable"],
} as const;

const exited = ChildProcessSpawner.ExitCode(0);
const text = (stream: Stream.Stream<Uint8Array, PlatformError.PlatformError>) =>
  stream.pipe(Stream.decodeText(), Stream.mkString);

/** Runs a tool to completion and keeps its output; `env` adds to this process's environment. */
const execute = Effect.fnUntraced(function* (
  command: string,
  args: ReadonlyArray<string>,
  cwd: string,
  env: Readonly<Record<string, string>> = {},
) {
  const handle = yield* ChildProcess.make(command, args, {
    cwd,
    env,
    extendEnv: true,
    stdin: "ignore",
  });
  const [stdout, stderr, status] = yield* Effect.all(
    [text(handle.stdout), text(handle.stderr), handle.exitCode],
    { concurrency: "unbounded" },
  );
  return { stdout, stderr, status };
}, Effect.scoped);

/** Runs a tool that must succeed, and returns its standard output. */
const run = Effect.fnUntraced(function* (
  command: string,
  args: ReadonlyArray<string>,
  cwd: string,
  env: Readonly<Record<string, string>> = {},
) {
  const { stdout, stderr, status } = yield* execute(command, args, cwd, env);
  if (status !== exited)
    return yield* failure(`${command} ${args.join(" ")} failed in ${cwd}\n${stdout}${stderr}`);
  return stdout;
});

const program = Effect.gen(function* () {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const root = yield* path.fromFileUrl(new URL("../", import.meta.url));
  const fixture = (name: string): string => path.join(root, "scripts", "pack", name);
  /**
   * The example sources each isolated consumer compiles, by what it installs.
   * Every consumer compiles the portable Rundown; the browser consumer adds the
   * page, and the native consumer, which installs @effect/platform-node, every
   * Node program. Example tests are left out: they need Vitest.
   */
  const exampleSources = {
    portable: ["packages/client/examples/src/Rundown.ts"],
    browser: [
      "packages/client/examples/src/Rundown.ts",
      "packages/browser/examples/src/Api.ts",
      "packages/browser/examples/src/WebCrypto.ts",
      "packages/browser/examples/src/app.ts",
    ],
    node: [
      "packages/client/examples/src/Rundown.ts",
      "packages/client/examples/src/main.ts",
      "packages/browser/examples/src/Api.ts",
      "packages/browser/examples/src/server.ts",
      "packages/native/examples/src/Recording.ts",
      "packages/native/examples/src/capture.ts",
      ...(yield* fs.readDirectory(path.join(root, "examples/livestream/src")))
        .filter((name) => name.endsWith(".ts"))
        .map((name) => `examples/livestream/src/${name}`),
    ],
  };
  const args = yield* (yield* Stdio.Stdio).args;
  const portableOnly = args.includes("--portable");
  const profile = portableOnly ? "portable" : "full";
  for (const arg of args)
    if (arg !== "--portable")
      return yield* failure(`unknown pack argument: ${arg}; use --portable`);
  yield* fs.makeDirectory(path.join(root, ".check"), { recursive: true });
  const packDirectory = yield* fs.makeTempDirectory({
    directory: path.join(root, ".check"),
    prefix: "pack-",
  });
  const workspace = yield* Schema.decodeEffect(WorkspaceManifest)(
    yield* fs.readFileString(path.join(root, "package.json")),
  );
  const catalog = workspace.workspaces.catalog;
  const typescriptVersion = catalog.typescript;
  if (typescriptVersion === undefined) return yield* failure("catalog must pin typescript");
  const nodeTypesVersion = catalog["@types/node"];
  if (nodeTypesVersion === undefined) return yield* failure("catalog must pin @types/node");
  // Release qualification selects frozen bytes, and the archives' Effect peers pin that selection.
  const stack = yield* selectStack(workspace, yield* fs.readFile(path.join(root, "bun.lock")));
  const { requirements, selected } = stack;
  const workspaceResolution = yield* resolveWorkspaceStack(root, stack);
  const consumers: Array<ConsumerResolution> = [];

  // TypeScript 7 ships its compiler as an optional platform package. Install that
  // exact tool explicitly so --omit=optional can still prove the SDK works without
  // the native addon. Every compiler and declaration remains inside the isolated consumer.
  const compilerManifest = yield* Schema.decodeEffect(CompilerManifest)(
    yield* fs.readFileString(path.join(root, "node_modules/typescript/package.json")),
  );
  if (compilerManifest.version !== typescriptVersion)
    return yield* failure("workspace TypeScript version differs from the pinned consumer compiler");
  const compilerPlatform = `@typescript/typescript-${process.platform}-${process.arch}`;
  const compilerPlatformVersion = compilerManifest.optionalDependencies?.[compilerPlatform];
  if (compilerPlatformVersion === undefined)
    return yield* failure(
      `pinned TypeScript does not declare its host compiler ${compilerPlatform}`,
    );
  const compilerPackages = [
    `typescript@${typescriptVersion}`,
    `${compilerPlatform}@${compilerPlatformVersion}`,
  ];

  // Resolve the selected Node once before entering deliberately stripped fixture
  // environments. An NVM installation must not rely on PATH surviving isolation.
  const { node: selectedNode, bun } = yield* runtimes;
  const node = yield* fs.realPath(
    (yield* run(selectedNode, ["-p", "process.execPath"], root)).trim(),
  );
  const installer = yield* Config.String("PACK_INSTALLER").pipe(Config.withDefault("npm"));
  if (installer !== "npm" && installer !== "bun")
    return yield* failure("PACK_INSTALLER must be npm or bun");
  const keep = yield* Config.String("KEEP_PACK_TMP").pipe(
    Config.map((value) => value === "1"),
    Config.withDefault(false),
  );
  yield* Console.log(`consumer-installer ${installer} profile ${profile}`);
  yield* run(bun, ["--no-env-file", "run", "build"], root);

  const hostAddon = addonPlatforms[`${process.platform}-${process.arch}`];
  /** Each platform addon's identity as staging wrote it, by platform. */
  const identities = new Map<string, Schema.Json>();
  /** Every build input's hash, as `rust/build.rs` embeds it; no Rust source ships. */
  const checkedOutSource = portableOnly
    ? undefined
    : (yield* run(node, ["packages/native/scripts/stage.mjs", "--source-hash"], root)).trim();

  const checkArchive = Effect.fnUntraced(function* (archive: Archive, unpacked: string) {
    const { files, manifest } = archive;
    // Only the native package runs on Node and loads its addon.
    const hostBuiltins = manifest.name === "reactor-effect-native";
    for (const required of ["package.json", "README.md", "LICENSE", "NOTICE"]) {
      if (!files.has(required))
        return yield* failure(`${manifest.name} tarball omitted ${required}`);
    }
    if (![...files].some((file) => file.startsWith("notices/")))
      return yield* failure(`${manifest.name} tarball omitted its third-party notices`);
    for (const group of [
      manifest.dependencies,
      manifest.peerDependencies,
      manifest.optionalDependencies,
      manifest.devDependencies,
    ])
      for (const [name, version] of Object.entries(group ?? {})) {
        if (/^(?:workspace|catalog):/.test(version))
          return yield* failure(
            `${manifest.name}: ${name} uses unpublished dependency protocol ${version}`,
          );
      }
    yield* checkArchivePeers(manifest.name, manifest.peerDependencies, stack);
    // Effect's shape: the index, one subpath per top-level module, internals and the
    // index's own path closed. TypeScript finds each module's declarations beside it.
    if (!isExportMap(manifest.exports))
      return yield* failure(`${manifest.name} export map differs from ${exportMapText}`);
    const modules = publicModules(files);
    if (!modules.includes("index"))
      return yield* failure(`${manifest.name} tarball omitted dist/index.js`);
    for (const module of modules)
      if (!files.has(`dist/${module}.d.ts`))
        return yield* failure(`${manifest.name} module ${module} has no declarations`);
    const dependencies = new Set([
      ...Object.keys(manifest.dependencies ?? {}),
      ...Object.keys(manifest.peerDependencies ?? {}),
      ...Object.keys(manifest.optionalDependencies ?? {}),
    ]);
    for (const file of files) {
      if (!file.startsWith("dist/") || (!file.endsWith(".js") && !file.endsWith(".d.ts"))) continue;
      const sourcePath = path.join(root, "packages", archive.directory, file);
      if (!(yield* fs.exists(sourcePath)))
        return yield* failure(`packed ${file} is absent from build output`);
      const source = yield* fs.readFileString(sourcePath);
      if (source !== (yield* fs.readFileString(path.join(unpacked, "package", file))))
        return yield* failure(`packed ${file} differs from the build inspected for import closure`);
      if (source.includes("/the-show") || source.includes("@the-show/"))
        return yield* failure(`${file} retains a workspace-specific import/path`);
      for (const specifier of specifiers(source)) {
        if (specifier.startsWith(".")) {
          // Archive entries are POSIX paths, as this host's are.
          const target = path.normalize(path.join(path.dirname(file), specifier));
          if (!files.has(target))
            return yield* failure(`${file} imports missing packaged file ${target}`);
          continue;
        }
        if (specifier.startsWith("/") || specifier.startsWith("file:"))
          return yield* failure(`${file} contains absolute import ${specifier}`);
        if (builtins.has(specifier)) {
          if (!hostBuiltins) return yield* failure(`${file} imports Node builtin ${specifier}`);
          continue;
        }
        if (!dependencies.has(packageName(specifier)))
          return yield* failure(`${file} imports undeclared external dependency ${specifier}`);
      }
    }
  });

  /** A platform package: its addon, the identity staging wrote for it, and notices. */
  const checkPlatformArchive = Effect.fnUntraced(function* (archive: Archive, unpacked: string) {
    const { files, manifest } = archive;
    const target = manifest.name.slice(platformPrefix.length);
    const addon = addonFile(target);
    const packaged = (file: string) => path.join(unpacked, "package", file);
    for (const required of ["package.json", "README.md", "LICENSE", "NOTICE", addon])
      if (!files.has(required))
        return yield* failure(`${manifest.name} tarball omitted ${required}`);
    if (![...files].some((file) => file.startsWith("notices/")))
      return yield* failure(`${manifest.name} tarball omitted its third-party notices`);
    const expectedFiles = [
      "package.json",
      "README.md",
      "LICENSE",
      "NOTICE",
      "native-identity.json",
    ];
    for (const file of files)
      if (!file.startsWith("notices/") && !expectedFiles.includes(file) && file !== addon)
        return yield* failure(`${manifest.name} tarball carries unexpected ${file}`);
    if (manifest.main !== addon) return yield* failure(`${manifest.name} main must be ${addon}`);
    const invalid = () => failure(`invalid native identity in ${manifest.name}`);
    const recorded = yield* Schema.decodeEffect(JsonText)(
      yield* fs.readFileString(packaged("native-identity.json")),
    ).pipe(Effect.mapError(invalid));
    const identity = yield* Schema.decodeUnknownEffect(NativeIdentity)(recorded).pipe(
      Effect.mapError(invalid),
    );
    if (identity.platform !== target || identity.file !== addon) return yield* invalid();
    if (
      !/^[a-f0-9]{64}$/.test(identity.sha256) ||
      !/^[a-f0-9]{64}$/.test(identity.build.sourceSha256)
    )
      return yield* failure(`invalid native hashes in ${manifest.name}`);
    if ((yield* sha256(yield* fs.readFile(packaged(addon)))) !== identity.sha256)
      return yield* failure(`native tarball hash differs from qualified stage: ${manifest.name}`);
    const staged = yield* fs.readFile(path.join(root, "packages", "native", "npm", target, addon));
    if ((yield* sha256(staged)) !== identity.sha256)
      return yield* failure(`native stage changed during packaging: ${manifest.name}`);
    if (identity.build.sourceSha256 !== checkedOutSource)
      return yield* failure(
        `${manifest.name} was built from sources other than the checked-out ones`,
      );
    identities.set(target, recorded);
  });

  /** The binding: no addon or Rust source, and every platform package pinned to its version. */
  const checkNativeArchive = Effect.fnUntraced(function* (archive: Archive) {
    const { files, manifest } = archive;
    for (const file of files)
      if (file.endsWith(".node") || /^(?:rust|npm|scripts)\//.test(file))
        return yield* failure(`${manifest.name} tarball carries ${file}`);
    const optional = manifest.optionalDependencies ?? {};
    const expected = Object.values(addonPlatforms)
      .map((target) => `${platformPrefix}${target}`)
      .sort();
    const listed = Object.keys(optional).sort();
    if (listed.length !== expected.length || listed.some((name, index) => name !== expected[index]))
      return yield* failure(
        `${manifest.name} must list exactly the platform packages ${expected.join(", ")}`,
      );
    for (const [name, version] of Object.entries(optional))
      if (version !== manifest.version)
        return yield* failure(
          `${manifest.name} must pin ${name} to exactly ${manifest.version}, not ${version}`,
        );
  });

  /** Packs one workspace package and validates the archive before any consumer sees it. */
  const packArchive = Effect.fnUntraced(function* (directory: string) {
    const packageRoot = path.join(root, "packages", directory);
    const source = yield* Schema.decodeEffect(PackageManifest)(
      yield* fs.readFileString(path.join(packageRoot, "package.json")),
    );
    const filename = `${source.name}-${source.version}.tgz`;
    yield* run(
      bun,
      [
        "--no-env-file",
        "pm",
        "pack",
        "--ignore-scripts",
        "--quiet",
        "--destination",
        packDirectory,
      ],
      packageRoot,
    );
    const tarball = path.join(packDirectory, filename);
    if (!(yield* fs.exists(tarball)))
      return yield* failure(`bun pm pack did not produce ${filename}`);
    yield* Console.log(`pack-created ${path.relative(root, tarball)}`);
    const files = new Set(
      (yield* run("tar", ["-tzf", tarball], root))
        .split("\n")
        .filter((entry) => entry.length > 0 && !entry.endsWith("/"))
        .map((entry) => (entry.startsWith("package/") ? entry.slice("package/".length) : entry)),
    );
    for (const file of files) {
      if (
        path.isAbsolute(file) ||
        file.split("/").some((part) => part === ".." || part.length === 0)
      )
        return yield* failure(`invalid tarball entry ${file}`);
    }
    const archiveSha256 = yield* sha256(yield* fs.readFile(tarball));
    // Stable archive names can retain stale Bun cache entries. A content-addressed
    // alias preserves the exact bytes without copying or pruning any cache.
    const installTarball =
      installer === "bun"
        ? path.join(packDirectory, `${source.name}-${archiveSha256}.tgz`)
        : tarball;
    if (installer === "bun") yield* fs.link(tarball, installTarball);
    const unpacked = path.join(packDirectory, "unpacked", directory);
    yield* fs.makeDirectory(unpacked, { recursive: true });
    yield* run("tar", ["-xzf", tarball, "-C", unpacked], root).pipe(
      // Preserve the archive and failure, but not a new partial extraction that
      // would make the next disk-constrained qualification attempt fail sooner.
      Effect.onError(() =>
        keep
          ? Effect.void
          : fs.remove(unpacked, { recursive: true, force: true }).pipe(Effect.orDie),
      ),
    );
    const packaged = (file: string) => path.join(unpacked, "package", file);
    const manifest = yield* Schema.decodeEffect(PackageManifest)(
      yield* fs.readFileString(packaged("package.json")),
    );
    const fileSha256: Record<string, string> = {};
    for (const file of [...files].sort())
      fileSha256[file] = yield* sha256(yield* fs.readFile(packaged(file)));
    const archive: Archive = {
      directory,
      manifest,
      tarball,
      installTarball,
      sha256: archiveSha256,
      files,
      fileSha256,
    };
    if (isPlatformPackage(manifest.name)) yield* checkPlatformArchive(archive, unpacked);
    else yield* checkArchive(archive, unpacked);
    if (manifest.name === "reactor-effect-native") yield* checkNativeArchive(archive);
    if (!keep) yield* fs.remove(unpacked, { recursive: true, force: true });
    return archive;
  });

  /** The platform addons to pack: the host's, those CI expects, and any other staged one. */
  const expectedAddons = (yield* Config.String("PACK_EXPECT_NATIVE_PLATFORMS").pipe(
    Config.withDefault(""),
  ))
    .split(",")
    .map((value) => value.trim())
    .filter((value) => value.length > 0);
  const stagedAddons = portableOnly
    ? []
    : yield* Effect.filter(
        Object.values(addonPlatforms),
        Effect.fnUntraced(function* (target) {
          const staged = yield* fs.exists(
            path.join(root, "packages", "native", "npm", target, addonFile(target)),
          );
          if (!staged && (target === hostAddon || expectedAddons.includes(target)))
            return yield* failure(`no staged addon for ${target}; run bun run native:build`);
          return staged;
        }),
      );
  const archives = new Map<string, Archive>();
  for (const directory of portableOnly
    ? ["client", "browser"]
    : ["client", "browser", "native", ...stagedAddons.map((target) => `native/npm/${target}`)])
    archives.set(directory, yield* packArchive(directory));
  const client = archives.get("client");
  if (client === undefined) return yield* failure("client archive was not produced");
  for (const archive of archives.values()) {
    if (archive.manifest.version !== client.manifest.version)
      return yield* failure(
        `${archive.manifest.name} version differs from ${client.manifest.name}`,
      );
    if (
      archive !== client &&
      !isPlatformPackage(archive.manifest.name) &&
      archive.manifest.peerDependencies?.["reactor-effect-client"] !== client.manifest.version
    )
      return yield* failure(
        `${archive.manifest.name} must pin its reactor-effect-client peer to ${client.manifest.version}`,
      );
  }

  const isolated = yield* Effect.acquireRelease(
    fs.makeTempDirectory({ prefix: "reactor-effect-pack-" }),
    (directory) =>
      keep
        ? Effect.void
        : fs.remove(directory, { recursive: true, force: true }).pipe(Effect.orDie),
  );

  // Compile the workspace's examples against the installed packages, never
  // against workspace source, keeping each example's own relative imports.
  const stageExamples = Effect.fnUntraced(function* (
    directory: string,
    sources: ReadonlyArray<string>,
  ) {
    const paths = [...sources].sort();
    for (const example of paths) {
      if (!(yield* fs.exists(path.join(root, example))))
        return yield* failure(`workspace lacks the example source ${example}`);
      const destination = path.join(directory, example);
      yield* fs.makeDirectory(path.dirname(destination), { recursive: true });
      yield* fs.copyFile(path.join(root, example), destination);
    }
    return paths;
  });

  // The complete checks remain independent, but need not retain their installed
  // trees concurrently. Only this run's successful consumer is removed.
  const releaseConsumer = (directory: string) =>
    keep ? Effect.void : fs.remove(directory, { recursive: true, force: true });

  const initConsumer = Effect.fnUntraced(function* (name: string) {
    const directory = path.join(isolated, name);
    yield* fs.makeDirectory(directory, { recursive: true });
    const manifest = yield* Schema.encodeEffect(NewConsumerManifest)({
      private: true,
      type: "module",
    });
    yield* fs.writeFileString(path.join(directory, "package.json"), manifest);
    yield* fs.copyFile(
      fixture("resolution-guard.mjs"),
      path.join(directory, "resolution-guard.mjs"),
    );
    return directory;
  });

  const install = Effect.fnUntraced(function* (
    directory: string,
    installed: ReadonlyArray<Archive>,
    packages: ReadonlyArray<string>,
    omitOptional: boolean,
  ) {
    const consumer = path.relative(isolated, directory);
    const command = installer === "bun" ? bun : "npm";
    const installArgs =
      installer === "bun"
        ? [
            "--no-env-file",
            "add",
            "--ignore-scripts",
            "--exact",
            "--linker=hoisted",
            `--backend=${process.platform === "darwin" ? "clonefile" : "hardlink"}`,
          ]
        : ["install", "--ignore-scripts", "--no-package-lock", "--no-audit", "--no-fund"];
    const result = yield* execute(
      command,
      [
        ...installArgs,
        ...(omitOptional ? ["--omit=optional"] : []),
        ...installed.map((archive) => archive.installTarball),
        ...packages,
      ],
      directory,
    );
    yield* fs.writeFileString(
      path.join(packDirectory, `install-${consumer}.log`),
      `${result.stdout}${result.stderr}`,
    );
    if (result.status !== exited)
      return yield* failure(
        `${installer} install failed in ${directory}\n${result.stdout}${result.stderr}`,
      );
    const verifiedArchives = [];
    for (const archive of installed) {
      verifiedArchives.push(
        yield* verifyInstalledArchive(directory, {
          name: archive.manifest.name,
          version: archive.manifest.version,
          specifier: archive.installTarball,
          fileSha256: archive.fileSha256,
        }),
      );
      yield* Console.log(
        `installed-package-identity ${consumer} ${archive.manifest.name} ${archive.files.size} files`,
      );
    }
    if (installer === "bun") {
      const manifestPath = path.join(directory, "package.json");
      const original = yield* fs.readFileString(manifestPath);
      const evidencePrefix = `install-${consumer}`;
      yield* fs.writeFileString(
        path.join(packDirectory, `${evidencePrefix}.manifest.json`),
        original,
      );
      const requirements = yield* verifiedArchiveRequirements(
        yield* Schema.decodeEffect(ConsumerManifestJson)(original),
        verifiedArchives,
      );
      const normalized = `${yield* Schema.encodeEffect(ConsumerManifestJson)(requirements)}\n`;
      yield* fs.writeFileString(manifestPath, normalized);
      yield* fs.writeFileString(
        path.join(packDirectory, `${evidencePrefix}.normalized-manifest.json`),
        normalized,
      );
      const normalization = yield* Schema.encodeEffect(Normalization)({
        format: "reactor-pack-archive-requirements/v1",
        installer,
        reason: "Bun omits npm local-tarball provenance metadata",
        originalManifest: `${evidencePrefix}.manifest.json`,
        normalizedManifest: `${evidencePrefix}.normalized-manifest.json`,
        verification: "complete-installed-archive-byte-identity",
        archives: installed.map((archive) => ({
          name: archive.manifest.name,
          version: archive.manifest.version,
          installSpecifier: archive.installTarball,
          sha256: archive.sha256,
          filesChecked: archive.files.size,
        })),
      });
      yield* fs.writeFileString(
        path.join(packDirectory, `${evidencePrefix}.normalization.json`),
        `${normalization}\n`,
      );
    }
    if (!(yield* fs.exists(path.join(directory, "node_modules", compilerPlatform, "package.json"))))
      return yield* failure(
        `isolated consumer is missing its compiler platform package ${compilerPlatform}`,
      );
    if (
      omitOptional &&
      (yield* fs.readDirectory(path.join(directory, "node_modules"))).some((name) =>
        isPlatformPackage(name),
      )
    )
      return yield* failure("an optional native addon was installed in a portable consumer");
    const compilerVersion = (yield* run(
      node,
      ["node_modules/typescript/bin/tsc", "--version"],
      directory,
    )).trim();
    if (compilerVersion !== `Version ${typescriptVersion}`)
      return yield* failure(
        `isolated consumer selected an unexpected compiler: ${compilerVersion}`,
      );
  });

  const assertTraceInside = Effect.fnUntraced(function* (trace: string, directory: string) {
    const rootReal = yield* fs.realPath(directory);
    const prefix = rootReal.endsWith(path.sep) ? rootReal : `${rootReal}${path.sep}`;
    const resolutions = new Set<string>();
    for (const match of trace.matchAll(/was successfully resolved to '([^']+)'/g))
      if (match[1] !== undefined) resolutions.add(match[1]);
    for (const raw of resolutions) {
      if (!path.isAbsolute(raw) || !(yield* fs.exists(raw))) continue;
      const resolvedPath = yield* fs.realPath(raw);
      if (resolvedPath !== rootReal && !resolvedPath.startsWith(prefix))
        return yield* failure(
          `TypeScript resolved outside isolated consumer ${directory}: ${resolvedPath}`,
        );
    }
  });

  const typecheck = Effect.fnUntraced(function* (
    directory: string,
    sourceName: string,
    compilerOptions: Readonly<Record<string, Schema.Json>>,
    allowEffectNodeGlobal: boolean,
    examples: ReadonlyArray<string>,
  ) {
    const consumer = path.relative(isolated, directory);
    const writeConfig = Effect.fnUntraced(function* (include: ReadonlyArray<string>) {
      const config = yield* Schema.encodeEffect(TypeScriptConfig)({ compilerOptions, include });
      yield* fs.writeFileString(path.join(directory, "tsconfig.json"), config);
    });
    yield* fs.copyFile(fixture(sourceName), path.join(directory, sourceName));
    const include = [sourceName, ...examples];
    yield* writeConfig(include);

    // Plain diagnostics, whatever FORCE_COLOR says, so the exception below can be recognized.
    const bare = yield* execute(
      node,
      ["node_modules/typescript/bin/tsc", "--noEmit", "--pretty", "false", "-p", "tsconfig.json"],
      directory,
    );
    if (bare.status !== exited) {
      const diagnostics = `${bare.stdout}${bare.stderr}`
        .trim()
        .split("\n")
        .filter((line) => line.length > 0);
      const knownEffect =
        diagnostics.length > 0 &&
        diagnostics.every((line) =>
          /node_modules[/\\]effect[/\\]dist[/\\]Channel\.d\.ts\(\d+,\d+\): error TS2304: Cannot find name 'TextDecoderOptions'\./.test(
            line,
          ),
        );
      if (!allowEffectNodeGlobal || !knownEffect)
        return yield* failure(
          `isolated type consumer failed in ${directory}\n${diagnostics.join("\n")}`,
        );
      yield* fs.copyFile(
        fixture("effect-node-globals.d.mts"),
        path.join(directory, "effect-node-globals.d.mts"),
      );
      yield* writeConfig([...include, "effect-node-globals.d.mts"]);
      yield* Console.log(`effect-no-dom-exception TextDecoderOptions (effect@${selected.effect})`);
    }

    yield* run(
      node,
      ["node_modules/typescript/bin/tsc", "--noEmit", "-p", "tsconfig.json"],
      directory,
    );
    const trace = yield* run(
      node,
      ["node_modules/typescript/bin/tsc", "--noEmit", "--traceResolution", "-p", "tsconfig.json"],
      directory,
    );
    yield* assertTraceInside(trace, directory);
    yield* fs.writeFileString(path.join(packDirectory, `types-${consumer}.trace.log`), trace);
    if (examples.length > 0) {
      yield* run(
        node,
        [
          "node_modules/typescript/bin/tsc",
          "-p",
          "tsconfig.json",
          "--rootDir",
          ".",
          "--outDir",
          "compiled-examples",
        ],
        directory,
      );
      for (const example of examples) {
        const emitted = path.join(directory, "compiled-examples", example.replace(/\.ts$/, ".js"));
        if (!(yield* fs.exists(emitted)))
          return yield* failure(`example was not emitted: ${example}`);
      }
      yield* Console.log(`installed-examples-compiled ${consumer} ${examples.length}`);
    }
  });

  const checkRuntimeFixtures = Effect.fnUntraced(function* (
    directory: string,
    names: ReadonlyArray<string>,
    host: keyof typeof fixtureHosts,
  ) {
    yield* run(
      node,
      [
        "node_modules/typescript/bin/tsc",
        "--ignoreConfig",
        "--allowJs",
        "--checkJs",
        "--noEmit",
        "--allowImportingTsExtensions",
        "--target",
        "ES2022",
        "--module",
        "NodeNext",
        "--strict",
        "--skipLibCheck",
        "false",
        "--exactOptionalPropertyTypes",
        "--noUncheckedIndexedAccess",
        ...fixtureHosts[host],
        ...names,
      ],
      directory,
    );
  });

  const guarded = (directory: string, denyNative: boolean) => ({
    PACK_CONSUMER_ROOT: directory,
    PACK_DENY_NATIVE: denyNative ? "1" : "0",
    NODE_PATH: "",
  });

  const checkConsumerStack = Effect.fnUntraced(function* (
    directory: string,
    name: ConsumerResolution["name"],
  ) {
    const effect = yield* resolveStackPackage(
      path.join(directory, "package.json"),
      "effect",
      stack,
    );
    const resolved = path.relative(yield* fs.realPath(directory), effect.path);
    if (path.isAbsolute(resolved) || resolved === ".." || resolved.startsWith(`..${path.sep}`))
      return yield* failure(`${name}: Effect resolved outside the isolated consumer`);
    // Keep npm's invalid-peer diagnostics even when Bun performed the install.
    const dependencies = yield* run(
      "npm",
      ["ls", "effect", "@effect/platform-node", "@effect/platform-node-shared", "--all", "--json"],
      directory,
    );
    yield* fs.writeFileString(
      path.join(packDirectory, `${name === "portable-node" ? "portable" : name}-dependencies.json`),
      dependencies,
    );
    return yield* inspectConsumerTree(name, installer, dependencies, stack);
  });

  const browserArchive = archives.get("browser");
  if (browserArchive === undefined) return yield* failure("browser archive was not produced");

  // What an application's own `npm install reactor-effect-client` resolves. The other consumers
  // install the selected Effect by name; this one names no Effect version and sets no override,
  // so npm takes Effect from the registry through the archive's peer, whatever PACK_INSTALLER
  // says, and every module of the client must then import on Node.
  const fresh = yield* initConsumer("fresh-node");
  const freshInstall = yield* execute(
    "npm",
    [
      "install",
      "--ignore-scripts",
      "--no-package-lock",
      "--no-audit",
      "--no-fund",
      "--prefer-online",
      client.tarball,
    ],
    fresh,
  );
  yield* fs.writeFileString(
    path.join(packDirectory, "install-fresh-node.log"),
    `${freshInstall.stdout}${freshInstall.stderr}`,
  );
  if (freshInstall.status !== exited)
    return yield* failure(
      `npm install failed in ${fresh}\n${freshInstall.stdout}${freshInstall.stderr}`,
    );
  yield* verifyInstalledArchive(fresh, {
    name: client.manifest.name,
    version: client.manifest.version,
    specifier: client.tarball,
    fileSha256: client.fileSha256,
  });
  yield* fs.copyFile(fixture("portable-import.mjs"), path.join(fresh, "portable-import.mjs"));
  const freshOutput = yield* run(
    node,
    ["--experimental-loader", "./resolution-guard.mjs", "portable-import.mjs"],
    fresh,
    guarded(fresh, true),
  );
  if (!freshOutput.includes("portable-import-ok"))
    return yield* failure("fresh install import smoke did not complete");
  // npm ls fails on a peer npm had to override to finish the install.
  yield* fs.writeFileString(
    path.join(packDirectory, "fresh-node-dependencies.json"),
    yield* run("npm", ["ls", "--all", "--json"], fresh),
  );
  const freshEffect = yield* resolveStackPackage(
    path.join(fresh, "node_modules", client.manifest.name, "package.json"),
    "effect",
    stack,
  );
  yield* Console.log(
    `fresh-install-ok ${client.manifest.name}@${client.manifest.version} effect@${freshEffect.version}`,
  );
  yield* releaseConsumer(fresh);

  const portable = yield* initConsumer("portable-node");
  yield* install(
    portable,
    [client],
    [`effect@${selected.effect}`, ...compilerPackages, `@types/node@${nodeTypesVersion}`],
    true,
  );
  const portableStack = yield* checkConsumerStack(portable, "portable-node");
  yield* fs.copyFile(fixture("portable-import.mjs"), path.join(portable, "portable-import.mjs"));
  const portableOutput = yield* run(
    node,
    ["--experimental-loader", "./resolution-guard.mjs", "portable-import.mjs"],
    portable,
    guarded(portable, true),
  );
  if (!portableOutput.includes("portable-import-ok"))
    return yield* failure("portable import smoke did not complete");
  yield* checkRuntimeFixtures(portable, ["portable-import.mjs", "resolution-guard.mjs"], "node");
  yield* typecheck(
    portable,
    "node-consumer.mts",
    nodeCompilerOptions,
    true,
    yield* stageExamples(portable, exampleSources.portable),
  );
  yield* fs.copyFile(fixture("rundown-smoke.mjs"), path.join(portable, "example-smoke.mjs"));
  const rundownExample = path.join(
    portable,
    "compiled-examples/packages/client/examples/src/Rundown.js",
  );
  for (const [runtime, command, runtimeArgs] of [
    ["Node", node, ["--experimental-loader", "./resolution-guard.mjs"]],
    ["Bun", bun, ["--no-env-file"]],
  ] as const) {
    const output = yield* run(
      command,
      [...runtimeArgs, "example-smoke.mjs", rundownExample],
      portable,
      guarded(portable, true),
    );
    if (!output.includes("compiled-example-ok"))
      return yield* failure(`${runtime} installed example did not complete`);
    yield* Console.log(`${runtime} ${output.trim()}`);
  }
  yield* checkRuntimeFixtures(portable, ["example-smoke.mjs"], "node");
  yield* fs.copyFile(
    fixture("browser-bundle-smoke.mjs"),
    path.join(portable, "browser-bundle-smoke.mjs"),
  );
  yield* checkRuntimeFixtures(portable, ["browser-bundle-smoke.mjs"], "node");
  consumers.push(portableStack);
  yield* releaseConsumer(portable);

  const browser = yield* initConsumer("browser");
  yield* install(
    browser,
    [client, browserArchive],
    [`effect@${selected.effect}`, ...compilerPackages],
    true,
  );
  const browserStack = yield* checkConsumerStack(browser, "browser");
  yield* fs.copyFile(fixture("browser-import.mjs"), path.join(browser, "browser-import.mjs"));
  const browserOutput = yield* run(
    node,
    ["--experimental-loader", "./resolution-guard.mjs", "browser-import.mjs"],
    browser,
    guarded(browser, true),
  );
  if (!browserOutput.includes("browser-import-ok"))
    return yield* failure("browser import smoke did not complete");
  yield* checkRuntimeFixtures(browser, ["browser-import.mjs"], "browser");
  yield* run(
    bun,
    [
      "--no-env-file",
      "build",
      "browser-import.mjs",
      "--target=browser",
      "--outfile=browser-bundle.js",
    ],
    browser,
  );
  const browserBundle = yield* fs.readFileString(path.join(browser, "browser-bundle.js"));
  if (/reactor-effect-native|takeVideo|buildIdentity/.test(browserBundle))
    return yield* failure("installed browser bundle includes a native implementation");
  yield* fs.copyFile(
    fixture("browser-bundle-smoke.mjs"),
    path.join(browser, "browser-bundle-smoke.mjs"),
  );
  const hostlessOutput = yield* run(
    node,
    ["browser-bundle-smoke.mjs", path.join(browser, "browser-bundle.js")],
    browser,
    { NODE_PATH: "" },
  );
  if (!hostlessOutput.includes("browser-import-ok"))
    return yield* failure("installed browser bundle requires Node Buffer or did not complete");
  yield* typecheck(
    browser,
    "browser-consumer.mts",
    {
      target: "ES2022",
      module: "NodeNext",
      moduleResolution: "NodeNext",
      lib: ["ES2023", "DOM", "DOM.Iterable", "ESNext.Disposable"],
      strict: true,
      skipLibCheck: false,
      exactOptionalPropertyTypes: true,
      noUncheckedIndexedAccess: true,
      types: [],
      ...exampleCompilerOptions,
    },
    false,
    yield* stageExamples(browser, exampleSources.browser),
  );
  consumers.push(browserStack);
  yield* releaseConsumer(browser);

  const nativeArchive = archives.get("native");
  if (nativeArchive !== undefined) {
    const addonArchive = archives.get(`native/npm/${hostAddon}`);
    if (addonArchive === undefined) return yield* failure("no addon archive for this host");
    const hostIdentity = identities.get(hostAddon ?? "");
    if (hostIdentity === undefined) return yield* failure("no native identity for this host");
    // Runs only in CI, where the addons are staged. Like the fresh consumer, it names no Effect
    // package and sets no override: the installer takes Effect, the Node platform and the shared
    // platform from the archives' exact peers, and the tree check requires the selection.
    const native = yield* initConsumer("native");
    yield* install(
      native,
      // The binding's optional dependency on this host's package resolves to its archive.
      [client, nativeArchive, addonArchive],
      [...compilerPackages, `@types/node@${nodeTypesVersion}`],
      false,
    );
    const nativeStack = yield* checkConsumerStack(native, "native");
    yield* Console.log(`installed-native-effect-stack ${selected.effect}`);
    yield* fs.copyFile(fixture("native-preflight.mjs"), path.join(native, "native-preflight.mjs"));
    const nativeOutput = yield* run(
      node,
      ["--experimental-loader", "./resolution-guard.mjs", "native-preflight.mjs"],
      native,
      {
        ...guarded(native, false),
        PACK_NATIVE_IDENTITY: yield* Schema.encodeEffect(JsonText)(hostIdentity),
      },
    );
    if (!nativeOutput.includes("native-preflight-ok"))
      return yield* failure("installed native preflight did not complete");
    yield* checkRuntimeFixtures(native, ["native-preflight.mjs", "resolution-guard.mjs"], "node");
    yield* Console.log(nativeOutput.trim());
    yield* typecheck(
      native,
      "native-consumer.mts",
      nodeCompilerOptions,
      true,
      yield* stageExamples(native, exampleSources.node),
    );
    consumers.push(nativeStack);
    yield* releaseConsumer(native);
  }

  const nativeSource = identities.size > 0 ? checkedOutSource : undefined;
  const identityPath = path.join(packDirectory, "package-identity.json");
  const identity = yield* Schema.encodeEffect(PackageIdentity)({
    profile,
    installer,
    effect: requirements.effect,
    qualificationStack: yield* completeQualification(
      stack,
      workspaceResolution,
      consumers,
      profile,
    ),
    packages: Object.fromEntries(
      [...archives.values()].map((archive) => [
        archive.manifest.name,
        {
          version: archive.manifest.version,
          tarball: path.relative(packDirectory, archive.tarball),
          sha256: archive.sha256,
          exports: [...publicEntries(archive)].sort(),
          files: [...archive.files].sort(),
          fileSha256: archive.fileSha256,
        },
      ]),
    ),
    nativeSourceSha256: nativeSource ?? null,
    native: Object.fromEntries(identities),
  });
  yield* fs.writeFileString(identityPath, `${identity}\n`);
  for (const archive of archives.values()) {
    yield* Console.log(
      `pack-smoke-ok ${archive.manifest.name}@${archive.manifest.version} ${path.relative(root, archive.tarball)} sha256=${archive.sha256}`,
    );
  }
  if (nativeSource !== undefined) yield* Console.log(`native-source ${nativeSource}`);
  const githubOutput = yield* Config.String("GITHUB_OUTPUT").pipe(Config.option);
  if (Option.isSome(githubOutput))
    yield* fs.writeFileString(
      githubOutput.value,
      `directory=${packDirectory}\nidentity=${identityPath}\n`,
      { flag: "a" },
    );
  yield* Console.log(`isolated ${keep ? isolated : "removed"}`);
}).pipe(Effect.scoped);

program.pipe(
  // The script's entry point.
  // @effect-diagnostics-next-line strictEffectProvide:off
  Effect.provide(NodeServices.layer),
  NodeRuntime.runMain,
);
