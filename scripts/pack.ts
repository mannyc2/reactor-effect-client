import { builtinModules } from "node:module";
import {
  appendFileSync,
  copyFileSync,
  existsSync,
  mkdirSync,
  mkdtempSync,
  readFileSync,
  realpathSync,
  rmSync,
  writeFileSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { isAbsolute, join, relative, sep } from "node:path";
import * as pathPosix from "node:path/posix";
import { fileURLToPath } from "node:url";
import { spawnSync } from "node:child_process";
import { createHash } from "node:crypto";

interface Manifest {
  readonly name: string;
  readonly version: string;
  readonly exports: Readonly<Record<string, string | Readonly<Record<string, string>>>>;
  readonly dependencies?: Readonly<Record<string, string>>;
  readonly peerDependencies?: Readonly<Record<string, string>>;
  readonly optionalDependencies?: Readonly<Record<string, string>>;
  readonly devDependencies?: Readonly<Record<string, string>>;
}

interface PackFile {
  readonly path: string;
}
interface PackResult {
  readonly filename: string;
  readonly files: readonly PackFile[];
}

const root = fileURLToPath(new URL("../", import.meta.url));
const fixtureRoot = join(root, "test", "fixtures", "pack");
mkdirSync(join(root, ".check"), { recursive: true });
const packDirectory = mkdtempSync(join(root, ".check", "pack-"));
const manifest = JSON.parse(readFileSync(join(root, "package.json"), "utf8")) as Manifest;
const typescriptVersion = manifest.devDependencies?.typescript;
const nodeTypesVersion = manifest.devDependencies?.["@types/node"];

const fail = (message: string): never => {
  throw new Error(`pack smoke: ${message}`);
};
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

if (typescriptVersion === undefined || nodeTypesVersion === undefined)
  fail("TypeScript/@types/node development versions are required for consumer smoke tests");

// TypeScript 7 ships its compiler as an optional platform package. Install that
// exact tool explicitly so --omit=optional can still prove the SDK works without
// Koffi. Every compiler and declaration remains inside the isolated consumer.
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

// Every run owns a new directory. Preserve previous delivery/evidence archives;
// the package below can only originate from this run's successful source build.
const node = process.env.NODE_BINARY ?? "node";
run(node, ["scripts/build.mjs"], root);

const packed = JSON.parse(
  run("npm", ["pack", "--ignore-scripts", "--json", "--pack-destination", packDirectory], root),
) as readonly PackResult[];
if (packed.length !== 1) fail("npm pack did not produce exactly one package");
const pack = packed[0] ?? fail("npm pack returned no package record");
const tarball = join(packDirectory, pack.filename);
const files = new Set(pack.files.map((entry) => entry.path));
if (!existsSync(tarball)) fail(`tarball does not exist: ${tarball}`);
for (const path of files) {
  if (isAbsolute(path) || path.split("/").some((part) => part === ".." || part.length === 0))
    fail(`invalid tarball entry ${path}`);
}
const unpacked = join(packDirectory, "unpacked");
mkdirSync(unpacked);
run("tar", ["-xzf", tarball, "-C", unpacked], root);
const packaged = (path: string): Buffer => readFileSync(join(unpacked, "package", path));

for (const required of [
  "package.json",
  "README.md",
  "CONTRIBUTING.md",
  "SECURITY.md",
  "LICENSE",
  "NOTICE",
]) {
  if (!files.has(required)) fail(`tarball omitted ${required}`);
}

const packedManifestText = packaged("package.json").toString("utf8");
const packedManifest = JSON.parse(packedManifestText) as Manifest;
const canonicalExports = [
  ".",
  "./browser",
  "./native",
  "./h3",
  "./orchestration",
  "./simulation",
  "./testing",
  "./wire",
].sort();
if (
  JSON.stringify(Object.keys(packedManifest.exports).sort()) !== JSON.stringify(canonicalExports)
) {
  fail("package must expose exactly the eight canonical public entry points");
}
const dependencyGroups = [
  packedManifest.dependencies,
  packedManifest.peerDependencies,
  packedManifest.optionalDependencies,
];
for (const group of dependencyGroups)
  for (const [name, version] of Object.entries(group ?? {})) {
    if (/^(?:workspace|catalog):/.test(version))
      fail(`${name} uses unpublished dependency protocol ${version}`);
  }

const exportTargets = new Set<string>();
for (const [name, value] of Object.entries(packedManifest.exports ?? {})) {
  if (name.includes("*")) fail(`package export is not explicit: ${name}`);
  const conditions =
    typeof value === "string"
      ? fail(`export ${name} must provide explicit types/import conditions`)
      : value;
  if (!("types" in conditions) || !("import" in conditions))
    fail(`export ${name} must provide both types and import targets`);
  const targets = Object.values(conditions);
  if (targets.length === 0) fail(`export ${name} has no targets`);
  for (const target of targets) {
    if (!target.startsWith("./")) fail(`export ${name} has a non-package target: ${target}`);
    const path = target.slice(2);
    exportTargets.add(path);
    if (!files.has(path)) fail(`export ${name} points at missing tarball file ${path}`);
  }
  const declaration = conditions.types ?? fail(`export ${name} omitted its types target`);
  if (!declaration.endsWith(".d.ts"))
    fail(`export ${name} types target is not a declaration file: ${declaration}`);
}

const dependencies = new Set([
  ...Object.keys(packedManifest.dependencies ?? {}),
  ...Object.keys(packedManifest.peerDependencies ?? {}),
  ...Object.keys(packedManifest.optionalDependencies ?? {}),
  packedManifest.name,
]);
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

for (const path of files) {
  if (!path.startsWith("dist/") || (!path.endsWith(".js") && !path.endsWith(".d.ts"))) continue;
  const sourcePath = join(root, path);
  if (!existsSync(sourcePath)) fail(`packed ${path} is absent from build output`);
  const source = readFileSync(sourcePath, "utf8");
  const packedSource = packaged(path).toString("utf8");
  if (source !== packedSource)
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
    if (builtins.has(specifier)) continue;
    const external = packageName(specifier);
    if (!dependencies.has(external))
      fail(`${path} imports undeclared external dependency ${specifier}`);
  }
}

const platform = `${process.platform}-${process.arch}`;
const nativeName =
  process.platform === "darwin"
    ? "libreactor_effect_native.dylib"
    : process.platform === "win32"
      ? "reactor_effect_native.dll"
      : "libreactor_effect_native.so";
const nativeArtifact = `dist/native/${platform}/${nativeName}`;
if (!files.has(nativeArtifact))
  fail(`tarball has no native artifact for current host (${nativeArtifact})`);
interface NativeIdentity {
  readonly schemaVersion: number;
  readonly platform: string;
  readonly library: string;
  readonly sha256: string;
  readonly build: Readonly<{ abiVersion: number; sourceSha256: string; profile: string }>;
}
const identities = new Map<string, NativeIdentity>();
let nativeSource: string | undefined;
for (const path of files) {
  if (!/^dist\/native\/[^/]+\/native-identity\.json$/.test(path)) continue;
  const identity = JSON.parse(packaged(path).toString("utf8")) as NativeIdentity;
  if (
    identity.schemaVersion !== 1 ||
    identity.build.abiVersion !== 2 ||
    identity.build.profile !== "release"
  )
    fail(`invalid native identity: ${path}`);
  if (
    !/^[a-f0-9]{64}$/.test(identity.sha256) ||
    !/^[a-f0-9]{64}$/.test(identity.build.sourceSha256)
  )
    fail(`invalid native hashes: ${path}`);
  const artifact = `dist/native/${identity.platform}/${identity.library}`;
  if (path !== `dist/native/${identity.platform}/native-identity.json` || !files.has(artifact))
    fail(`native identity refers to an absent/wrong platform: ${path}`);
  if (createHash("sha256").update(packaged(artifact)).digest("hex") !== identity.sha256)
    fail(`native tarball hash differs from qualified stage: ${artifact}`);
  if (
    createHash("sha256")
      .update(readFileSync(join(root, artifact)))
      .digest("hex") !== identity.sha256
  )
    fail(`native stage changed during packaging: ${artifact}`);
  if (nativeSource !== undefined && nativeSource !== identity.build.sourceSha256)
    fail("native platforms were built from different source identities");
  nativeSource = identity.build.sourceSha256;
  identities.set(identity.platform, identity);
}
if (!identities.has(platform)) fail("current host artifact has no qualified native identity");
if (!files.has("native/stage.mjs"))
  fail("source-build package omitted the sole native staging owner");
const sourceHash = createHash("sha256");
const nativeInputs = [
  "Cargo.toml",
  "Cargo.lock",
  "build.rs",
  ".cargo/config.toml",
  "include/reactor_effect_native.h",
  ...[...files]
    .filter((path) => path.startsWith("native/src/"))
    .map((path) => path.slice("native/".length)),
].sort();
for (const path of nativeInputs)
  sourceHash
    .update(path)
    .update("\0")
    .update(packaged(`native/${path}`))
    .update("\0");
if (sourceHash.digest("hex") !== nativeSource)
  fail("packaged native source differs from the source identity embedded in the tested artifacts");
for (const expected of (process.env.PACK_EXPECT_NATIVE_PLATFORMS ?? "")
  .split(",")
  .map((value) => value.trim())
  .filter(Boolean)) {
  const name = expected.startsWith("darwin-")
    ? "libreactor_effect_native.dylib"
    : expected.startsWith("win32-")
      ? "reactor_effect_native.dll"
      : "libreactor_effect_native.so";
  const artifact = `dist/native/${expected}/${name}`;
  if (!files.has(artifact) || !identities.has(expected))
    fail(`tarball omitted expected native artifact/identity ${artifact}`);
}

const isolated = mkdtempSync(join(tmpdir(), "reactor-effect-pack-"));
const keep = process.env.KEEP_PACK_TMP === "1";
const fixture = (name: string): string => join(fixtureRoot, name);

const initConsumer = (name: string): string => {
  const directory = join(isolated, name);
  mkdirSync(directory, { recursive: true });
  writeFileSync(
    join(directory, "package.json"),
    JSON.stringify({ private: true, type: "module" }, null, 2),
  );
  copyFileSync(fixture("resolution-guard.mjs"), join(directory, "resolution-guard.mjs"));
  return directory;
};

const install = (directory: string, packages: readonly string[], omitOptional: boolean): void => {
  const args = ["install", "--ignore-scripts", "--no-package-lock", "--no-audit", "--no-fund"];
  if (omitOptional) args.push("--omit=optional");
  run("npm", [...args, ...packages], directory);
  if (!existsSync(join(directory, "node_modules", compilerPlatform, "package.json")))
    fail(`isolated consumer is missing its compiler platform package ${compilerPlatform}`);
  if (omitOptional && existsSync(join(directory, "node_modules", "koffi")))
    fail("optional Koffi was installed in a portable consumer");
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
): void => {
  copyFileSync(fixture(sourceName), join(directory, sourceName));
  let include = [sourceName];
  const writeConfig = () =>
    writeFileSync(
      join(directory, "tsconfig.json"),
      JSON.stringify({ compilerOptions, include }, null, 2),
    );
  writeConfig();

  const bare = execute(
    "node",
    ["node_modules/typescript/bin/tsc", "--noEmit", "-p", "tsconfig.json"],
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
    include = [sourceName, "effect-node-globals.d.mts"];
    writeConfig();
    console.log("effect-no-dom-exception TextDecoderOptions (effect@4.0.0-rc.115)");
  }

  run("node", ["node_modules/typescript/bin/tsc", "--noEmit", "-p", "tsconfig.json"], directory);
  const trace = run(
    "node",
    ["node_modules/typescript/bin/tsc", "--noEmit", "--traceResolution", "-p", "tsconfig.json"],
    directory,
  );
  assertTraceInside(trace, directory);
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

try {
  const portable = initConsumer("portable-node");
  install(portable, [tarball, ...compilerPackages, `@types/node@${nodeTypesVersion}`], true);
  copyFileSync(fixture("portable-import.mjs"), join(portable, "portable-import.mjs"));
  const portableOutput = run(
    "node",
    ["--experimental-loader", "./resolution-guard.mjs", "portable-import.mjs"],
    portable,
    {
      ...process.env,
      PACK_CONSUMER_ROOT: portable,
      PACK_DENY_NATIVE: "1",
      NODE_PATH: "",
    },
  );
  if (!portableOutput.includes("portable-import-ok"))
    fail("portable import smoke did not complete");
  copyFileSync(fixture("simulation-smoke.mjs"), join(portable, "simulation-smoke.mjs"));
  const simulationOutput = run(
    node,
    ["--experimental-loader", "./resolution-guard.mjs", "./simulation-smoke.mjs"],
    portable,
    {
      PACK_CONSUMER_ROOT: portable,
      PACK_DENY_NATIVE: "1",
      NODE_PATH: "",
    },
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
    {
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
    },
    true,
  );

  const browser = initConsumer("browser");
  install(browser, [tarball, ...compilerPackages], true);
  copyFileSync(fixture("browser-import.mjs"), join(browser, "browser-import.mjs"));
  const browserOutput = run(
    "node",
    ["--experimental-loader", "./resolution-guard.mjs", "browser-import.mjs"],
    browser,
    {
      ...process.env,
      PACK_CONSUMER_ROOT: browser,
      PACK_DENY_NATIVE: "1",
      NODE_PATH: "",
    },
  );
  if (!browserOutput.includes("browser-import-ok")) fail("browser import smoke did not complete");
  const bun = process.env.BUN_BINARY ?? process.execPath;
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
  if (/koffi|reactor_effect_peer_|native-bridge|native-peer/.test(browserBundle))
    fail("installed browser bundle includes a native implementation");
  typecheck(browser, "browser-consumer.mts", {
    target: "ES2022",
    module: "NodeNext",
    moduleResolution: "NodeNext",
    lib: ["ES2023", "DOM", "DOM.Iterable", "ESNext.Disposable"],
    strict: true,
    skipLibCheck: false,
    exactOptionalPropertyTypes: true,
    noUncheckedIndexedAccess: true,
    types: [],
  });

  const native = initConsumer("native");
  const nodePlatformVersion =
    manifest.devDependencies?.["@effect/platform-node"] ??
    fail("native fixture needs the pinned Node Effect platform");
  install(
    native,
    [
      tarball,
      `@effect/platform-node@${nodePlatformVersion}`,
      ...compilerPackages,
      `@types/node@${nodeTypesVersion}`,
    ],
    false,
  );
  copyFileSync(fixture("native-preflight.mjs"), join(native, "native-preflight.mjs"));
  const nativeOutput = run(
    "node",
    ["--experimental-loader", "./resolution-guard.mjs", "native-preflight.mjs"],
    native,
    {
      ...process.env,
      PACK_CONSUMER_ROOT: native,
      PACK_DENY_NATIVE: "0",
      PACK_NATIVE_IDENTITY: JSON.stringify(identities.get(platform)),
      NODE_PATH: "",
    },
  );
  if (!nativeOutput.includes("native-preflight-ok"))
    fail("installed native preflight did not complete");
  checkRuntimeFixtures(native, ["native-preflight.mjs", "resolution-guard.mjs"]);
  console.log(nativeOutput.trim());

  console.log(`pack-smoke-ok ${packedManifest.name}@${packedManifest.version}`);
  console.log(`tarball ${relative(root, tarball)}`);
  console.log(`exports ${Object.keys(packedManifest.exports ?? {}).length}`);
  console.log(`native-source ${nativeSource}`);
  const tarballSha256 = createHash("sha256").update(readFileSync(tarball)).digest("hex");
  console.log(`tarball-sha256 ${tarballSha256}`);
  const identityPath = join(packDirectory, "package-identity.json");
  writeFileSync(
    identityPath,
    JSON.stringify(
      {
        name: packedManifest.name,
        version: packedManifest.version,
        tarball: pack.filename,
        sha256: tarballSha256,
        exports: canonicalExports,
        nativeSourceSha256: nativeSource,
        native: Object.fromEntries(identities),
        files: [...files].sort(),
      },
      null,
      2,
    ) + "\n",
  );
  if (process.env.GITHUB_OUTPUT !== undefined) {
    appendFileSync(process.env.GITHUB_OUTPUT, `tarball=${tarball}\nidentity=${identityPath}\n`);
  }
  console.log(`isolated ${keep ? isolated : "removed"}`);
} finally {
  if (!keep) rmSync(isolated, { recursive: true, force: true });
}
