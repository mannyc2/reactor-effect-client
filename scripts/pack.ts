import { builtinModules } from "node:module";
import { copyFileSync, existsSync, mkdirSync, mkdtempSync, readFileSync, realpathSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { isAbsolute, join, relative, sep } from "node:path";
import * as pathPosix from "node:path/posix";
import { fileURLToPath } from "node:url";
import { spawnSync } from "node:child_process";

interface Manifest {
  readonly name: string;
  readonly version: string;
  readonly exports: Readonly<Record<string, string | Readonly<Record<string, string>>>>;
  readonly dependencies?: Readonly<Record<string, string>>;
  readonly peerDependencies?: Readonly<Record<string, string>>;
  readonly optionalDependencies?: Readonly<Record<string, string>>;
  readonly devDependencies?: Readonly<Record<string, string>>;
}

interface PackFile { readonly path: string }
interface PackResult { readonly filename: string; readonly files: readonly PackFile[] }

const root = fileURLToPath(new URL("../", import.meta.url));
const fixtureRoot = join(root, "test", "fixtures", "pack");
const packDirectory = join(root, ".check", "pack");
const manifest = JSON.parse(readFileSync(join(root, "package.json"), "utf8")) as Manifest;
const typescriptVersion = manifest.devDependencies?.typescript;
const nodeTypesVersion = manifest.devDependencies?.["@types/node"];

const fail = (message: string): never => { throw new Error(`pack smoke: ${message}`); };
const execute = (command: string, args: readonly string[], cwd: string, env: NodeJS.ProcessEnv = process.env) =>
  spawnSync(command, [...args], { cwd, env, encoding: "utf8", maxBuffer: 64 * 1024 * 1024 });
const run = (command: string, args: readonly string[], cwd: string, env: NodeJS.ProcessEnv = process.env): string => {
  const result = execute(command, args, cwd, env);
  if (result.error !== undefined) throw result.error;
  if (result.status !== 0) {
    fail(`${command} ${args.join(" ")} failed in ${cwd}\n${result.stdout}${result.stderr}`);
  }
  return result.stdout;
};

if (typescriptVersion === undefined || nodeTypesVersion === undefined) fail("TypeScript/@types/node development versions are required for consumer smoke tests");

rmSync(packDirectory, { recursive: true, force: true });
mkdirSync(packDirectory, { recursive: true });

// Clear any prior tarball before building, then build inside the pack check so
// neither an old package artifact nor an old dist/ tree can be mistaken for
// current clean-consumer evidence after a source build failure.
run("node", ["scripts/build.mjs"], root);

const packed = JSON.parse(run("npm", ["pack", "--ignore-scripts", "--json", "--pack-destination", packDirectory], root)) as readonly PackResult[];
if (packed.length !== 1) fail("npm pack did not produce exactly one package");
const pack = packed[0] ?? fail("npm pack returned no package record");
const tarball = join(packDirectory, pack.filename);
const files = new Set(pack.files.map((entry) => entry.path));
if (!existsSync(tarball)) fail(`tarball does not exist: ${tarball}`);

for (const required of ["package.json", "README.md", "LICENSE", "NOTICE"]) {
  if (!files.has(required)) fail(`tarball omitted ${required}`);
}

const packedManifestText = run("tar", ["-xOf", tarball, "package/package.json"], root);
const packedManifest = JSON.parse(packedManifestText) as Manifest;
const dependencyGroups = [packedManifest.dependencies, packedManifest.peerDependencies, packedManifest.optionalDependencies];
for (const group of dependencyGroups) for (const [name, version] of Object.entries(group ?? {})) {
  if (/^(?:workspace|catalog):/.test(version)) fail(`${name} uses unpublished dependency protocol ${version}`);
}

const exportTargets = new Set<string>();
for (const [name, value] of Object.entries(packedManifest.exports ?? {})) {
  if (name.includes("*")) fail(`package export is not explicit: ${name}`);
  const conditions = typeof value === "string"
    ? fail(`export ${name} must provide explicit types/import conditions`)
    : value;
  if (!("types" in conditions) || !("import" in conditions)) fail(`export ${name} must provide both types and import targets`);
  const targets = Object.values(conditions);
  if (targets.length === 0) fail(`export ${name} has no targets`);
  for (const target of targets) {
    if (!target.startsWith("./")) fail(`export ${name} has a non-package target: ${target}`);
    const path = target.slice(2);
    exportTargets.add(path);
    if (!files.has(path)) fail(`export ${name} points at missing tarball file ${path}`);
  }
  const declaration = conditions.types ?? fail(`export ${name} omitted its types target`);
  if (!declaration.endsWith(".d.ts")) fail(`export ${name} types target is not a declaration file: ${declaration}`);
}

const dependencies = new Set([
  ...Object.keys(packedManifest.dependencies ?? {}),
  ...Object.keys(packedManifest.peerDependencies ?? {}),
  ...Object.keys(packedManifest.optionalDependencies ?? {}),
  packedManifest.name,
]);
const builtins = new Set([...builtinModules, ...builtinModules.map((name) => `node:${name}`)]);
const packageName = (specifier: string): string => specifier.startsWith("@")
  ? specifier.split("/").slice(0, 2).join("/")
  : specifier.split("/", 1)[0] ?? specifier;
const specifiers = (source: string): readonly string[] => {
  const found = new Set<string>();
  for (const expression of [
    /\bfrom\s+["']([^"']+)["']/g,
    /\bimport\s*\(\s*["']([^"']+)["']/g,
    /\bimport\s+["']([^"']+)["']/g,
    /\brequire\s*\(\s*["']([^"']+)["']/g,
  ]) {
    for (const match of source.matchAll(expression)) if (match[1] !== undefined) found.add(match[1]);
  }
  return [...found];
};

for (const path of files) {
  if (!path.startsWith("dist/") || (!path.endsWith(".js") && !path.endsWith(".d.ts"))) continue;
  const sourcePath = join(root, path);
  if (!existsSync(sourcePath)) fail(`packed ${path} is absent from build output`);
  const source = readFileSync(sourcePath, "utf8");
  if (source.includes("/the-show") || source.includes("@the-show/")) fail(`${path} retains a workspace-specific import/path`);
  for (const specifier of specifiers(source)) {
    if (specifier.startsWith(".")) {
      const target = pathPosix.normalize(pathPosix.join(pathPosix.dirname(path), specifier));
      if (!files.has(target)) fail(`${path} imports missing packaged file ${target}`);
      continue;
    }
    if (specifier.startsWith("/") || specifier.startsWith("file:")) fail(`${path} contains absolute import ${specifier}`);
    if (builtins.has(specifier)) continue;
    const external = packageName(specifier);
    if (!dependencies.has(external)) fail(`${path} imports undeclared external dependency ${specifier}`);
  }
}

const platform = `${process.platform}-${process.arch}`;
const nativeName = process.platform === "darwin" ? "libreactor_effect_native.dylib"
  : process.platform === "win32" ? "reactor_effect_native.dll" : "libreactor_effect_native.so";
const nativeArtifact = `dist/native/${platform}/${nativeName}`;
if (!files.has(nativeArtifact)) fail(`tarball has no native artifact for current host (${nativeArtifact})`);
for (const expected of (process.env.PACK_EXPECT_NATIVE_PLATFORMS ?? "").split(",").map((value) => value.trim()).filter(Boolean)) {
  const name = expected.startsWith("darwin-") ? "libreactor_effect_native.dylib"
    : expected.startsWith("win32-") ? "reactor_effect_native.dll" : "libreactor_effect_native.so";
  const artifact = `dist/native/${expected}/${name}`;
  if (!files.has(artifact)) fail(`tarball omitted expected native artifact ${artifact}`);
}

const isolated = mkdtempSync(join(tmpdir(), "reactor-effect-pack-"));
const keep = process.env.KEEP_PACK_TMP === "1";
const fixture = (name: string): string => join(fixtureRoot, name);

const initConsumer = (name: string): string => {
  const directory = join(isolated, name);
  mkdirSync(directory, { recursive: true });
  writeFileSync(join(directory, "package.json"), JSON.stringify({ private: true, type: "module" }, null, 2));
  copyFileSync(fixture("resolution-guard.mjs"), join(directory, "resolution-guard.mjs"));
  return directory;
};

const install = (directory: string, packages: readonly string[], omitOptional: boolean): void => {
  const args = ["install", "--ignore-scripts", "--no-package-lock", "--no-audit", "--no-fund"];
  if (omitOptional) args.push("--omit=optional");
  run("npm", [...args, ...packages], directory);
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

const typecheck = (directory: string, sourceName: string, compilerOptions: Record<string, unknown>, allowEffectNodeGlobal = false): void => {
  copyFileSync(fixture(sourceName), join(directory, sourceName));
  let include = [sourceName];
  const writeConfig = () => writeFileSync(join(directory, "tsconfig.json"), JSON.stringify({ compilerOptions, include }, null, 2));
  writeConfig();

  const bare = execute("node", ["node_modules/typescript/bin/tsc", "--noEmit", "-p", "tsconfig.json"], directory);
  if (bare.error !== undefined) throw bare.error;
  if (bare.status !== 0) {
    const diagnostics = `${bare.stdout}${bare.stderr}`.trim().split("\n").filter(Boolean);
    const knownEffect = diagnostics.length > 0 && diagnostics.every((line) =>
      /node_modules[/\\]effect[/\\]dist[/\\]Channel\.d\.ts\(\d+,\d+\): error TS2304: Cannot find name 'TextDecoderOptions'\./.test(line));
    if (!allowEffectNodeGlobal || !knownEffect) {
      fail(`isolated type consumer failed in ${directory}\n${diagnostics.join("\n")}`);
    }
    copyFileSync(fixture("effect-node-globals.d.mts"), join(directory, "effect-node-globals.d.mts"));
    include = [sourceName, "effect-node-globals.d.mts"];
    writeConfig();
    console.log("effect-no-dom-exception TextDecoderOptions (effect@4.0.0-rc.115)");
  }

  run("node", ["node_modules/typescript/bin/tsc", "--noEmit", "-p", "tsconfig.json"], directory);
  const trace = run("node", ["node_modules/typescript/bin/tsc", "--noEmit", "--traceResolution", "-p", "tsconfig.json"], directory);
  assertTraceInside(trace, directory);
};

try {
  const portable = initConsumer("portable-node");
  install(portable, [tarball, `typescript@${typescriptVersion}`, `@types/node@${nodeTypesVersion}`], false);
  copyFileSync(fixture("portable-import.mjs"), join(portable, "portable-import.mjs"));
  const portableOutput = run("node", ["--experimental-loader", "./resolution-guard.mjs", "portable-import.mjs"], portable, {
    ...process.env,
    PACK_CONSUMER_ROOT: portable,
    PACK_DENY_NATIVE: "1",
    NODE_PATH: "",
  });
  if (!portableOutput.includes("portable-import-ok")) fail("portable import smoke did not complete");
  typecheck(portable, "node-consumer.mts", {
    target: "ES2022", module: "NodeNext", moduleResolution: "NodeNext",
    lib: ["ES2023", "ESNext.Disposable"], strict: true, skipLibCheck: false,
    exactOptionalPropertyTypes: true, noUncheckedIndexedAccess: true,
    types: ["node"], typeRoots: ["./node_modules/@types"],
  }, true);

  const browser = initConsumer("browser");
  install(browser, [tarball, `typescript@${typescriptVersion}`], false);
  copyFileSync(fixture("browser-import.mjs"), join(browser, "browser-import.mjs"));
  const browserOutput = run("node", ["--experimental-loader", "./resolution-guard.mjs", "browser-import.mjs"], browser, {
    ...process.env,
    PACK_CONSUMER_ROOT: browser,
    PACK_DENY_NATIVE: "1",
    NODE_PATH: "",
  });
  if (!browserOutput.includes("browser-import-ok")) fail("browser import smoke did not complete");
  typecheck(browser, "browser-consumer.mts", {
    target: "ES2022", module: "NodeNext", moduleResolution: "NodeNext",
    lib: ["ES2023", "DOM", "DOM.Iterable", "ESNext.Disposable"], strict: true,
    skipLibCheck: false, exactOptionalPropertyTypes: true, noUncheckedIndexedAccess: true,
    types: [],
  });

  const native = initConsumer("native");
  install(native, [tarball], false);
  copyFileSync(fixture("native-preflight.mjs"), join(native, "native-preflight.mjs"));
  const nativeOutput = run("node", ["--experimental-loader", "./resolution-guard.mjs", "native-preflight.mjs"], native, {
    ...process.env,
    PACK_CONSUMER_ROOT: native,
    PACK_DENY_NATIVE: "0",
    NODE_PATH: "",
  });
  if (!nativeOutput.includes("native-preflight-ok")) fail("installed native preflight did not complete");

  console.log(`pack-smoke-ok ${packedManifest.name}@${packedManifest.version}`);
  console.log(`tarball ${relative(root, tarball)}`);
  console.log(`exports ${Object.keys(packedManifest.exports ?? {}).length}`);
  console.log(`isolated ${keep ? isolated : "removed"}`);
} finally {
  if (!keep) rmSync(isolated, { recursive: true, force: true });
}
