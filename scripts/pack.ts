import { builtinModules } from "node:module";
import {
  appendFileSync,
  copyFileSync,
  existsSync,
  linkSync,
  mkdirSync,
  mkdtempSync,
  readFileSync,
  readdirSync,
  realpathSync,
  rmSync,
  writeFileSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { dirname, isAbsolute, join, relative, sep } from "node:path";
import * as pathPosix from "node:path/posix";
import { fileURLToPath } from "node:url";
import { spawnSync } from "node:child_process";
import { createHash } from "node:crypto";
import {
  completeQualification,
  inspectConsumerTree,
  resolveStackPackage,
  resolveWorkspaceStack,
  selectStack,
  verifiedArchiveRequirements,
  verifyInstalledArchive,
  type ConsumerResolution,
} from "./pack-effect-stack.js";

/**
 * Installed-package qualification for the public workspace packages.
 *
 * Every package is packed with `bun pm pack`, which rewrites `workspace:` and
 * `catalog:` protocols to exact versions. Each archive is validated on its own
 * (public exports, declaration/import closure, declared dependencies, native
 * identity), then installed into isolated consumers: a portable Node consumer
 * without optional dependencies, a browser consumer bundled without Node
 * globals, and a native consumer that verifies the installed addon's identity.
 * Each staged platform addon is packed as its own package. `--portable` packs
 * and checks only the client and browser packages, for hosts without a staged
 * addon; CI and release run the full gate.
 */
interface Manifest {
  readonly name: string;
  readonly version: string;
  /** Absent from a platform package, which only carries its addon. */
  readonly exports: Readonly<Record<string, string | Readonly<Record<string, string>>>>;
  readonly dependencies?: Readonly<Record<string, string>>;
  readonly peerDependencies?: Readonly<Record<string, string>>;
  readonly optionalDependencies?: Readonly<Record<string, string>>;
  readonly devDependencies?: Readonly<Record<string, string>>;
}
interface RootManifest {
  readonly workspaces: { readonly catalog: Readonly<Record<string, string>> };
  readonly overrides?: Readonly<Record<string, string>>;
}
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
interface NativeIdentity {
  readonly schemaVersion: number;
  /** The platform package's suffix, such as `linux-x64-gnu`. */
  readonly platform: string;
  /** The addon file in that package. */
  readonly file: string;
  readonly sha256: string;
  readonly build: Readonly<{
    schemaVersion: number;
    sourceSha256: string;
    profile: string;
    target: string;
    webrtcPrebuilt: string;
  }>;
}

const root = fileURLToPath(new URL("../", import.meta.url));
const fixtureRoot = join(root, "scripts", "pack");
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
    ...readdirSync(join(root, "examples/livestream/src"))
      .filter((name) => name.endsWith(".ts"))
      .map((name) => `examples/livestream/src/${name}`),
  ],
} as const;
const portableOnly = process.argv.includes("--portable");
for (const arg of process.argv.slice(2))
  if (arg !== "--portable") throw new Error(`unknown pack argument: ${arg}; use --portable`);
mkdirSync(join(root, ".check"), { recursive: true });
const packDirectory = mkdtempSync(join(root, ".check", "pack-"));
const workspace = JSON.parse(readFileSync(join(root, "package.json"), "utf8")) as RootManifest;
const catalog = workspace.workspaces.catalog;

const fail = (message: string): never => {
  throw new Error(`pack smoke: ${message}`);
};
const sha256 = (bytes: Uint8Array): string => createHash("sha256").update(bytes).digest("hex");
const execute = (
  command: string,
  args: readonly string[],
  cwd: string,
  env: NodeJS.ProcessEnv = process.env,
) => spawnSync(command, [...args], { cwd, env, encoding: "utf8", maxBuffer: 64 * 1024 * 1024 });
const run = (
  command: string,
  args: readonly string[],
  cwd: string,
  env: NodeJS.ProcessEnv = process.env,
): string => {
  const result = execute(command, args, cwd, env);
  if (result.error !== undefined) throw result.error;
  if (result.status !== 0) {
    fail(`${command} ${args.join(" ")} failed in ${cwd}\n${result.stdout}${result.stderr}`);
  }
  return result.stdout;
};

const typescriptVersion = catalog.typescript ?? fail("catalog must pin typescript");
const nodeTypesVersion = catalog["@types/node"] ?? fail("catalog must pin @types/node");
// Release qualification selects frozen bytes; archive peers keep their ranges.
const stack = selectStack(workspace, readFileSync(join(root, "bun.lock")));
const { requirements, selected } = stack;
const workspaceResolution = resolveWorkspaceStack(root, stack);
const consumers: ConsumerResolution[] = [];

// TypeScript 7 ships its compiler as an optional platform package. Install that
// exact tool explicitly so --omit=optional can still prove the SDK works without
// the native addon. Every compiler and declaration remains inside the isolated consumer.
const compilerManifest = JSON.parse(
  readFileSync(join(root, "node_modules/typescript/package.json"), "utf8"),
) as Pick<Manifest, "version" | "optionalDependencies">;
if (compilerManifest.version !== typescriptVersion)
  fail("workspace TypeScript version differs from the pinned consumer compiler");
const compilerPlatform = `@typescript/typescript-${process.platform}-${process.arch}`;
const compilerPlatformVersion =
  compilerManifest.optionalDependencies?.[compilerPlatform] ??
  fail(`pinned TypeScript does not declare its host compiler ${compilerPlatform}`);
const compilerPackages = [
  `typescript@${typescriptVersion}`,
  `${compilerPlatform}@${compilerPlatformVersion}`,
];

// Resolve the selected Node once before entering deliberately stripped fixture
// environments. An NVM installation must not rely on PATH surviving isolation.
const node = realpathSync(
  run(process.env.NODE_BINARY ?? "node", ["-p", "process.execPath"], root).trim(),
);
const bun = process.env.BUN_BINARY ?? process.execPath;
const installer = process.env.PACK_INSTALLER ?? "npm";
if (installer !== "npm" && installer !== "bun")
  throw new Error("PACK_INSTALLER must be npm or bun");
const keep = process.env.KEEP_PACK_TMP === "1";
console.log(`consumer-installer ${installer} profile ${portableOnly ? "portable" : "full"}`);
run(bun, ["--no-env-file", "run", "build"], root);

/** Each published addon platform package, by the Node host it runs on. */
const addonPlatforms: Readonly<Record<string, string>> = {
  "darwin-arm64": "darwin-arm64",
  "linux-x64": "linux-x64-gnu",
};
const hostAddon = addonPlatforms[`${process.platform}-${process.arch}`];
const platformPrefix = "reactor-effect-native-";
const isPlatformPackage = (name: string): boolean => name.startsWith(platformPrefix);
const addonFile = (target: string): string => `reactor-effect-native.${target}.node`;
const builtins = new Set([...builtinModules, ...builtinModules.map((name) => `node:${name}`)]);
const packageName = (specifier: string): string =>
  specifier.startsWith("@")
    ? specifier.split("/").slice(0, 2).join("/")
    : (specifier.split("/", 1)[0] ?? specifier);
const specifiers = (source: string): readonly string[] => {
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

/** Packs one workspace package and validates the archive before any consumer sees it. */
const packArchive = (directory: string): Archive => {
  const packageRoot = join(root, "packages", directory);
  const source = JSON.parse(readFileSync(join(packageRoot, "package.json"), "utf8")) as Manifest;
  const filename = `${source.name}-${source.version}.tgz`;
  run(
    bun,
    ["--no-env-file", "pm", "pack", "--ignore-scripts", "--quiet", "--destination", packDirectory],
    packageRoot,
  );
  const tarball = join(packDirectory, filename);
  if (!existsSync(tarball)) fail(`bun pm pack did not produce ${filename}`);
  console.log(`pack-created ${relative(root, tarball)}`);
  const files = new Set(
    run("tar", ["-tzf", tarball], root)
      .split("\n")
      .filter((entry) => entry.length > 0 && !entry.endsWith("/"))
      .map((entry) => (entry.startsWith("package/") ? entry.slice("package/".length) : entry)),
  );
  for (const path of files) {
    if (isAbsolute(path) || path.split("/").some((part) => part === ".." || part.length === 0))
      fail(`invalid tarball entry ${path}`);
  }
  const archiveSha256 = sha256(readFileSync(tarball));
  let installTarball = tarball;
  if (installer === "bun") {
    // Stable archive names can retain stale Bun cache entries. A content-addressed
    // alias preserves the exact bytes without copying or pruning any cache.
    installTarball = join(packDirectory, `${source.name}-${archiveSha256}.tgz`);
    linkSync(tarball, installTarball);
  }
  const unpacked = join(packDirectory, "unpacked", directory);
  mkdirSync(unpacked, { recursive: true });
  try {
    run("tar", ["-xzf", tarball, "-C", unpacked], root);
  } catch (error) {
    // Preserve the archive and failure, but not a new partial extraction that
    // would make the next disk-constrained qualification attempt fail sooner.
    if (!keep) rmSync(unpacked, { recursive: true, force: true });
    throw error;
  }
  const packaged = (path: string): Buffer => readFileSync(join(unpacked, "package", path));
  const manifest = JSON.parse(packaged("package.json").toString("utf8")) as Manifest;
  const fileSha256 = Object.fromEntries(
    [...files].sort().map((path) => [path, sha256(packaged(path))]),
  );
  const archive: Archive = {
    directory,
    manifest,
    tarball,
    installTarball,
    sha256: archiveSha256,
    files,
    fileSha256,
  };
  if (isPlatformPackage(manifest.name)) checkPlatformArchive(archive, packaged);
  else checkArchive(archive, packaged);
  if (manifest.name === "reactor-effect-native") checkNativeArchive(archive);
  if (!keep) rmSync(unpacked, { recursive: true, force: true });
  return archive;
};

const checkArchive = (archive: Archive, packaged: (path: string) => Buffer): void => {
  const { files, manifest } = archive;
  // Only the native package runs on Node and loads its addon.
  const hostBuiltins = manifest.name === "reactor-effect-native";
  for (const required of ["package.json", "README.md", "LICENSE", "NOTICE"]) {
    if (!files.has(required)) fail(`${manifest.name} tarball omitted ${required}`);
  }
  if (![...files].some((path) => path.startsWith("notices/")))
    fail(`${manifest.name} tarball omitted its third-party notices`);
  for (const group of [
    manifest.dependencies,
    manifest.peerDependencies,
    manifest.optionalDependencies,
    manifest.devDependencies,
  ])
    for (const [name, version] of Object.entries(group ?? {})) {
      if (/^(?:workspace|catalog):/.test(version))
        fail(`${manifest.name}: ${name} uses unpublished dependency protocol ${version}`);
    }
  if (manifest.peerDependencies?.effect !== requirements.effect)
    fail(`${manifest.name} must declare the Effect peer range ${requirements.effect}`);
  for (const [name, value] of Object.entries(manifest.exports)) {
    if (name.includes("*")) fail(`package export is not explicit: ${name}`);
    const conditions =
      typeof value === "string"
        ? fail(`export ${name} must provide explicit types/import conditions`)
        : value;
    if (!("types" in conditions) || !("import" in conditions))
      fail(`export ${name} must provide both types and import targets`);
    for (const target of Object.values(conditions)) {
      if (!target.startsWith("./")) fail(`export ${name} has a non-package target: ${target}`);
      if (!files.has(target.slice(2)))
        fail(`export ${name} points at missing tarball file ${target}`);
    }
    if (!conditions.types?.endsWith(".d.ts"))
      fail(`export ${name} types target is not a declaration file: ${conditions.types}`);
  }
  const dependencies = new Set([
    ...Object.keys(manifest.dependencies ?? {}),
    ...Object.keys(manifest.peerDependencies ?? {}),
    ...Object.keys(manifest.optionalDependencies ?? {}),
  ]);
  for (const path of files) {
    if (!path.startsWith("dist/") || (!path.endsWith(".js") && !path.endsWith(".d.ts"))) continue;
    const sourcePath = join(root, "packages", archive.directory, path);
    if (!existsSync(sourcePath)) fail(`packed ${path} is absent from build output`);
    const source = readFileSync(sourcePath, "utf8");
    if (source !== packaged(path).toString("utf8"))
      fail(`packed ${path} differs from the build inspected for import closure`);
    if (source.includes("/the-show") || source.includes("@the-show/"))
      fail(`${path} retains a workspace-specific import/path`);
    for (const specifier of specifiers(source)) {
      if (specifier.startsWith(".")) {
        const target = pathPosix.normalize(pathPosix.join(pathPosix.dirname(path), specifier));
        if (!files.has(target)) fail(`${path} imports missing packaged file ${target}`);
        continue;
      }
      if (specifier.startsWith("/") || specifier.startsWith("file:"))
        fail(`${path} contains absolute import ${specifier}`);
      if (builtins.has(specifier)) {
        if (!hostBuiltins) fail(`${path} imports Node builtin ${specifier}`);
        continue;
      }
      const external = packageName(specifier);
      if (!dependencies.has(external))
        fail(`${path} imports undeclared external dependency ${specifier}`);
    }
  }
};

const identities = new Map<string, NativeIdentity>();
let nativeSource: string | undefined;
/** Every build input's hash, as `rust/build.rs` embeds it; no Rust source ships. */
const checkedOutSource = portableOnly
  ? undefined
  : run(node, ["packages/native/scripts/stage.mjs", "--source-hash"], root).trim();

/** A platform package: its addon, the identity staging wrote for it, and notices. */
const checkPlatformArchive = (archive: Archive, packaged: (path: string) => Buffer): void => {
  const { files, manifest } = archive;
  const target = manifest.name.slice(platformPrefix.length);
  const addon = addonFile(target);
  for (const required of ["package.json", "README.md", "LICENSE", "NOTICE", addon])
    if (!files.has(required)) fail(`${manifest.name} tarball omitted ${required}`);
  if (![...files].some((path) => path.startsWith("notices/")))
    fail(`${manifest.name} tarball omitted its third-party notices`);
  for (const path of files)
    if (
      !path.startsWith("notices/") &&
      !["package.json", "README.md", "LICENSE", "NOTICE", "native-identity.json", addon].includes(
        path,
      )
    )
      fail(`${manifest.name} tarball carries unexpected ${path}`);
  if ((manifest as { readonly main?: string }).main !== addon)
    fail(`${manifest.name} main must be ${addon}`);
  const identity = JSON.parse(packaged("native-identity.json").toString("utf8")) as NativeIdentity;
  if (
    identity.schemaVersion !== 2 ||
    identity.platform !== target ||
    identity.file !== addon ||
    identity.build.profile !== "release" ||
    !/^webrtc-\d+-[0-9a-f]{8}-p\d+$/.test(identity.build.webrtcPrebuilt)
  )
    fail(`invalid native identity in ${manifest.name}`);
  if (
    !/^[a-f0-9]{64}$/.test(identity.sha256) ||
    !/^[a-f0-9]{64}$/.test(identity.build.sourceSha256)
  )
    fail(`invalid native hashes in ${manifest.name}`);
  if (sha256(packaged(addon)) !== identity.sha256)
    fail(`native tarball hash differs from qualified stage: ${manifest.name}`);
  if (
    sha256(readFileSync(join(root, "packages", "native", "npm", target, addon))) !== identity.sha256
  )
    fail(`native stage changed during packaging: ${manifest.name}`);
  if (identity.build.sourceSha256 !== checkedOutSource)
    fail(`${manifest.name} was built from sources other than the checked-out ones`);
  nativeSource = identity.build.sourceSha256;
  identities.set(target, identity);
};

/** The binding: no addon or Rust source, and every platform package pinned to its version. */
const checkNativeArchive = (archive: Archive): void => {
  const { files, manifest } = archive;
  for (const path of files)
    if (path.endsWith(".node") || /^(?:rust|npm|scripts)\//.test(path))
      fail(`${manifest.name} tarball carries ${path}`);
  const optional = manifest.optionalDependencies ?? {};
  const expected = Object.values(addonPlatforms)
    .map((target) => `${platformPrefix}${target}`)
    .sort();
  if (JSON.stringify(Object.keys(optional).sort()) !== JSON.stringify(expected))
    fail(`${manifest.name} must list exactly the platform packages ${expected.join(", ")}`);
  for (const [name, version] of Object.entries(optional))
    if (version !== manifest.version)
      fail(`${manifest.name} must pin ${name} to exactly ${manifest.version}, not ${version}`);
};

/** The platform addons to pack: the host's, those CI expects, and any other staged one. */
const expectedAddons = (process.env.PACK_EXPECT_NATIVE_PLATFORMS ?? "")
  .split(",")
  .map((value) => value.trim())
  .filter(Boolean);
const addonDirectories = portableOnly
  ? []
  : Object.values(addonPlatforms)
      .filter((target) => {
        const staged = existsSync(
          join(root, "packages", "native", "npm", target, addonFile(target)),
        );
        if (!staged && (target === hostAddon || expectedAddons.includes(target)))
          fail(`no staged addon for ${target}; run bun run native:build`);
        return staged;
      })
      .map((target) => `native/npm/${target}`);
const archives = new Map<string, Archive>();
for (const directory of portableOnly
  ? ["client", "browser"]
  : ["client", "browser", "native", ...addonDirectories])
  archives.set(directory, packArchive(directory));
const client = archives.get("client") ?? fail("client archive was not produced");
for (const archive of archives.values()) {
  if (archive.manifest.version !== client.manifest.version)
    fail(`${archive.manifest.name} version differs from ${client.manifest.name}`);
  if (
    archive !== client &&
    !isPlatformPackage(archive.manifest.name) &&
    archive.manifest.peerDependencies?.["reactor-effect-client"] !== client.manifest.version
  )
    fail(
      `${archive.manifest.name} must pin its reactor-effect-client peer to ${client.manifest.version}`,
    );
}

const isolated = mkdtempSync(join(tmpdir(), "reactor-effect-pack-"));
const fixture = (name: string): string => join(fixtureRoot, name);

// Compile the workspace's examples against the installed packages, never
// against workspace source, keeping each example's own relative imports.
const stageExamples = (
  directory: string,
  profile: keyof typeof exampleSources,
): readonly string[] => {
  const paths = [...exampleSources[profile]].sort();
  for (const path of paths) {
    if (!existsSync(join(root, path))) fail(`workspace lacks the example source ${path}`);
    const destination = join(directory, path);
    mkdirSync(dirname(destination), { recursive: true });
    copyFileSync(join(root, path), destination);
  }
  return paths;
};

const releaseConsumer = (directory: string): void => {
  // The complete checks remain independent, but need not retain their installed
  // trees concurrently. Only this run's successful consumer is removed.
  if (!keep) rmSync(directory, { recursive: true, force: true });
};

const initConsumer = (name: string, overrides?: Readonly<Record<string, string>>): string => {
  const directory = join(isolated, name);
  mkdirSync(directory, { recursive: true });
  writeFileSync(
    join(directory, "package.json"),
    JSON.stringify(
      { private: true, type: "module", ...(overrides === undefined ? {} : { overrides }) },
      null,
      2,
    ),
  );
  copyFileSync(fixture("resolution-guard.mjs"), join(directory, "resolution-guard.mjs"));
  return directory;
};

const install = (
  directory: string,
  installed: readonly Archive[],
  packages: readonly string[],
  omitOptional: boolean,
): void => {
  const command = installer === "bun" ? bun : "npm";
  const args =
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
  if (omitOptional) args.push("--omit=optional");
  const result = execute(
    command,
    [
      ...args,
      `effect@${selected.effect}`,
      ...installed.map((archive) => archive.installTarball),
      ...packages,
    ],
    directory,
  );
  writeFileSync(
    join(packDirectory, `install-${relative(isolated, directory)}.log`),
    `${result.stdout ?? ""}${result.stderr ?? ""}`,
  );
  if (result.error !== undefined) throw result.error;
  if (result.status !== 0)
    fail(`${installer} install failed in ${directory}\n${result.stdout}${result.stderr}`);
  const verifiedArchives = installed.map((archive) => {
    const verified = verifyInstalledArchive(directory, {
      name: archive.manifest.name,
      version: archive.manifest.version,
      specifier: archive.installTarball,
      fileSha256: archive.fileSha256,
    });
    console.log(
      `installed-package-identity ${relative(isolated, directory)} ${archive.manifest.name} ${archive.files.size} files`,
    );
    return verified;
  });
  if (installer === "bun") {
    const manifestPath = join(directory, "package.json");
    const original = readFileSync(manifestPath, "utf8");
    const evidencePrefix = `install-${relative(isolated, directory)}`;
    writeFileSync(join(packDirectory, `${evidencePrefix}.manifest.json`), original);
    const normalized =
      JSON.stringify(verifiedArchiveRequirements(JSON.parse(original), verifiedArchives), null, 2) +
      "\n";
    writeFileSync(manifestPath, normalized);
    writeFileSync(join(packDirectory, `${evidencePrefix}.normalized-manifest.json`), normalized);
    writeFileSync(
      join(packDirectory, `${evidencePrefix}.normalization.json`),
      JSON.stringify(
        {
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
        },
        null,
        2,
      ) + "\n",
    );
  }
  if (!existsSync(join(directory, "node_modules", compilerPlatform, "package.json")))
    fail(`isolated consumer is missing its compiler platform package ${compilerPlatform}`);
  if (
    omitOptional &&
    readdirSync(join(directory, "node_modules")).some((name) => isPlatformPackage(name))
  )
    fail("an optional native addon was installed in a portable consumer");
  const compilerVersion = run(
    node,
    ["node_modules/typescript/bin/tsc", "--version"],
    directory,
  ).trim();
  if (compilerVersion !== `Version ${typescriptVersion}`)
    fail(`isolated consumer selected an unexpected compiler: ${compilerVersion}`);
};

const assertTraceInside = (trace: string, directory: string): void => {
  const rootReal = realpathSync(directory);
  const prefix = rootReal.endsWith(sep) ? rootReal : `${rootReal}${sep}`;
  for (const match of trace.matchAll(/was successfully resolved to '([^']+)'/g)) {
    const raw = match[1];
    if (raw === undefined || !isAbsolute(raw) || !existsSync(raw)) continue;
    const resolvedPath = realpathSync(raw);
    if (resolvedPath !== rootReal && !resolvedPath.startsWith(prefix)) {
      fail(`TypeScript resolved outside isolated consumer ${directory}: ${resolvedPath}`);
    }
  }
};

const typecheck = (
  directory: string,
  sourceName: string,
  compilerOptions: Record<string, unknown>,
  allowEffectNodeGlobal = false,
  examples: readonly string[] = [],
): void => {
  copyFileSync(fixture(sourceName), join(directory, sourceName));
  let include = [sourceName, ...examples];
  const writeConfig = () =>
    writeFileSync(
      join(directory, "tsconfig.json"),
      JSON.stringify({ compilerOptions, include }, null, 2),
    );
  writeConfig();

  // Plain diagnostics, whatever FORCE_COLOR says, so the exception below can be recognized.
  const bare = execute(
    node,
    ["node_modules/typescript/bin/tsc", "--noEmit", "--pretty", "false", "-p", "tsconfig.json"],
    directory,
  );
  if (bare.error !== undefined) throw bare.error;
  if (bare.status !== 0) {
    const diagnostics = `${bare.stdout}${bare.stderr}`.trim().split("\n").filter(Boolean);
    const knownEffect =
      diagnostics.length > 0 &&
      diagnostics.every((line) =>
        /node_modules[/\\]effect[/\\]dist[/\\]Channel\.d\.ts\(\d+,\d+\): error TS2304: Cannot find name 'TextDecoderOptions'\./.test(
          line,
        ),
      );
    if (!allowEffectNodeGlobal || !knownEffect) {
      fail(`isolated type consumer failed in ${directory}\n${diagnostics.join("\n")}`);
    }
    copyFileSync(
      fixture("effect-node-globals.d.mts"),
      join(directory, "effect-node-globals.d.mts"),
    );
    include = [...include, "effect-node-globals.d.mts"];
    writeConfig();
    console.log(`effect-no-dom-exception TextDecoderOptions (effect@${selected.effect})`);
  }

  run(node, ["node_modules/typescript/bin/tsc", "--noEmit", "-p", "tsconfig.json"], directory);
  const trace = run(
    node,
    ["node_modules/typescript/bin/tsc", "--noEmit", "--traceResolution", "-p", "tsconfig.json"],
    directory,
  );
  assertTraceInside(trace, directory);
  writeFileSync(join(packDirectory, `types-${relative(isolated, directory)}.trace.log`), trace);
  if (examples.length > 0) {
    run(
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
    for (const path of examples) {
      if (!existsSync(join(directory, "compiled-examples", path.replace(/\.ts$/, ".js"))))
        fail(`example was not emitted: ${path}`);
    }
    console.log(`installed-examples-compiled ${relative(isolated, directory)} ${examples.length}`);
  }
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

const checkRuntimeFixtures = (directory: string, names: readonly string[]): void => {
  run(
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
      "--types",
      "node",
      "--lib",
      "ES2023,DOM,ESNext.Disposable",
      ...names,
    ],
    directory,
  );
};

const guarded = (directory: string, denyNative: boolean): NodeJS.ProcessEnv => ({
  ...process.env,
  PACK_CONSUMER_ROOT: directory,
  PACK_DENY_NATIVE: denyNative ? "1" : "0",
  NODE_PATH: "",
});

const checkConsumerStack = (
  directory: string,
  name: ConsumerResolution["name"],
): ConsumerResolution => {
  const effect = resolveStackPackage(join(directory, "package.json"), "effect", stack);
  const resolved = relative(realpathSync(directory), effect.path);
  if (isAbsolute(resolved) || resolved === ".." || resolved.startsWith(`..${sep}`))
    fail(`${name}: Effect resolved outside the isolated consumer`);
  // Keep npm's invalid-peer diagnostics even when Bun performed the install.
  const dependencies = run(
    "npm",
    ["ls", "effect", "@effect/platform-node", "@effect/platform-node-shared", "--all", "--json"],
    directory,
  );
  writeFileSync(
    join(packDirectory, `${name === "portable-node" ? "portable" : name}-dependencies.json`),
    dependencies,
  );
  return inspectConsumerTree(name, installer, JSON.parse(dependencies), stack);
};

try {
  const browserArchive = archives.get("browser") ?? fail("browser archive was not produced");

  const portable = initConsumer("portable-node");
  install(portable, [client], [...compilerPackages, `@types/node@${nodeTypesVersion}`], true);
  const portableStack = checkConsumerStack(portable, "portable-node");
  copyFileSync(fixture("portable-import.mjs"), join(portable, "portable-import.mjs"));
  const portableOutput = run(
    node,
    ["--experimental-loader", "./resolution-guard.mjs", "portable-import.mjs"],
    portable,
    guarded(portable, true),
  );
  if (!portableOutput.includes("portable-import-ok"))
    fail("portable import smoke did not complete");
  copyFileSync(fixture("simulation-smoke.mjs"), join(portable, "simulation-smoke.mjs"));
  const simulationOutput = run(
    node,
    ["--experimental-loader", "./resolution-guard.mjs", "./simulation-smoke.mjs"],
    portable,
    guarded(portable, true),
  );
  if (!simulationOutput.includes("simulation-smoke-ok"))
    fail("installed production simulation did not complete");
  console.log(simulationOutput.trim());
  checkRuntimeFixtures(portable, [
    "portable-import.mjs",
    "simulation-smoke.mjs",
    "resolution-guard.mjs",
  ]);
  typecheck(
    portable,
    "node-consumer.mts",
    nodeCompilerOptions,
    true,
    stageExamples(portable, "portable"),
  );
  copyFileSync(fixture("rundown-smoke.mjs"), join(portable, "example-smoke.mjs"));
  const rundownExample = join(
    portable,
    "compiled-examples/packages/client/examples/src/Rundown.js",
  );
  for (const [runtime, command, args] of [
    ["Node", node, ["--experimental-loader", "./resolution-guard.mjs"]],
    ["Bun", bun, ["--no-env-file"]],
  ] as const) {
    const output = run(
      command,
      [...args, "example-smoke.mjs", rundownExample],
      portable,
      guarded(portable, true),
    );
    if (!output.includes("compiled-example-ok"))
      fail(`${runtime} installed example did not complete`);
    console.log(`${runtime} ${output.trim()}`);
  }
  checkRuntimeFixtures(portable, ["example-smoke.mjs"]);
  copyFileSync(fixture("browser-bundle-smoke.mjs"), join(portable, "browser-bundle-smoke.mjs"));
  checkRuntimeFixtures(portable, ["browser-bundle-smoke.mjs"]);
  consumers.push(portableStack);
  releaseConsumer(portable);

  const browser = initConsumer("browser");
  install(browser, [client, browserArchive], compilerPackages, true);
  const browserStack = checkConsumerStack(browser, "browser");
  copyFileSync(fixture("browser-import.mjs"), join(browser, "browser-import.mjs"));
  const browserOutput = run(
    node,
    ["--experimental-loader", "./resolution-guard.mjs", "browser-import.mjs"],
    browser,
    guarded(browser, true),
  );
  if (!browserOutput.includes("browser-import-ok")) fail("browser import smoke did not complete");
  run(
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
  const browserBundle = readFileSync(join(browser, "browser-bundle.js"), "utf8");
  if (/reactor-effect-native|takeVideo|buildIdentity/.test(browserBundle))
    fail("installed browser bundle includes a native implementation");
  copyFileSync(fixture("browser-bundle-smoke.mjs"), join(browser, "browser-bundle-smoke.mjs"));
  const hostlessOutput = run(
    node,
    ["browser-bundle-smoke.mjs", join(browser, "browser-bundle.js")],
    browser,
    { ...process.env, NODE_PATH: "" },
  );
  if (!hostlessOutput.includes("browser-import-ok"))
    fail("installed browser bundle requires Node Buffer or did not complete");
  typecheck(
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
    stageExamples(browser, "browser"),
  );
  consumers.push(browserStack);
  releaseConsumer(browser);

  const nativeArchive = archives.get("native");
  if (nativeArchive !== undefined) {
    const addonArchive =
      archives.get(`native/npm/${hostAddon}`) ?? fail(`no addon archive for this host`);
    // npm applies overrides only at the consumer root. The platform's prerelease
    // caret range otherwise admits a later shared platform and a second Effect.
    const native = initConsumer("native", { "@effect/platform-node-shared": selected.nodeShared });
    install(
      native,
      // The binding's optional dependency on this host's package resolves to its archive.
      [client, nativeArchive, addonArchive],
      [
        `@effect/platform-node@${selected.nodePlatform}`,
        ...compilerPackages,
        `@types/node@${nodeTypesVersion}`,
      ],
      false,
    );
    const nativeStack = checkConsumerStack(native, "native");
    console.log(`installed-native-effect-stack ${selected.effect}`);
    copyFileSync(fixture("native-preflight.mjs"), join(native, "native-preflight.mjs"));
    const nativeOutput = run(
      node,
      ["--experimental-loader", "./resolution-guard.mjs", "native-preflight.mjs"],
      native,
      {
        ...guarded(native, false),
        PACK_NATIVE_IDENTITY: JSON.stringify(identities.get(hostAddon ?? "")),
      },
    );
    if (!nativeOutput.includes("native-preflight-ok"))
      fail("installed native preflight did not complete");
    checkRuntimeFixtures(native, ["native-preflight.mjs", "resolution-guard.mjs"]);
    console.log(nativeOutput.trim());
    typecheck(
      native,
      "native-consumer.mts",
      nodeCompilerOptions,
      true,
      stageExamples(native, "node"),
    );
    consumers.push(nativeStack);
    releaseConsumer(native);
  }

  const identityPath = join(packDirectory, "package-identity.json");
  writeFileSync(
    identityPath,
    JSON.stringify(
      {
        profile: portableOnly ? "portable" : "full",
        installer,
        effect: requirements.effect,
        qualificationStack: completeQualification(
          stack,
          workspaceResolution,
          consumers,
          portableOnly ? "portable" : "full",
        ),
        packages: Object.fromEntries(
          [...archives.values()].map((archive) => [
            archive.manifest.name,
            {
              version: archive.manifest.version,
              tarball: relative(packDirectory, archive.tarball),
              sha256: archive.sha256,
              exports: Object.keys(archive.manifest.exports ?? {}).sort(),
              files: [...archive.files].sort(),
              fileSha256: archive.fileSha256,
            },
          ]),
        ),
        nativeSourceSha256: nativeSource ?? null,
        native: Object.fromEntries(identities),
      },
      null,
      2,
    ) + "\n",
  );
  for (const archive of archives.values()) {
    console.log(
      `pack-smoke-ok ${archive.manifest.name}@${archive.manifest.version} ${relative(root, archive.tarball)} sha256=${archive.sha256}`,
    );
  }
  if (nativeSource !== undefined) console.log(`native-source ${nativeSource}`);
  if (process.env.GITHUB_OUTPUT !== undefined) {
    appendFileSync(
      process.env.GITHUB_OUTPUT,
      `directory=${packDirectory}\nidentity=${identityPath}\n`,
    );
  }
  console.log(`isolated ${keep ? isolated : "removed"}`);
} finally {
  if (!keep) rmSync(isolated, { recursive: true, force: true });
}
