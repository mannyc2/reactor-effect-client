import { spawnSync } from "node:child_process";
import { mkdtempSync, readFileSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";

export const libraryName =
  process.platform === "darwin"
    ? "libreactor_effect_native.dylib"
    : process.platform === "win32"
      ? "reactor_effect_native.dll"
      : "libreactor_effect_native.so";
export const libraryPath = fileURLToPath(
  new URL(`../lib/${process.platform}-${process.arch}/${libraryName}`, import.meta.url),
);
const include = fileURLToPath(new URL("../rust/include", import.meta.url));
const fixtureSource = fileURLToPath(new URL("./session-fixture.c", import.meta.url));

/** Compile C source against the ABI header into a shared library in a fresh directory. */
export const compileLibrary = (
  source: string,
  name: string,
): { readonly directory: string; readonly path: string } => {
  const directory = mkdtempSync(join(tmpdir(), "reactor-native-"));
  const file = join(directory, `${name}.c`);
  const path = join(directory, `lib${name}.${process.platform === "darwin" ? "dylib" : "so"}`);
  writeFileSync(file, source);
  const compiler = process.env.CC ?? "cc";
  const flags = process.platform === "darwin" ? ["-dynamiclib"] : ["-shared", "-fPIC"];
  const result = spawnSync(
    compiler,
    ["-std=c11", "-D_DEFAULT_SOURCE", "-pthread", `-I${include}`, ...flags, file, "-o", path],
    { encoding: "utf8" },
  );
  if (result.error !== undefined) throw result.error;
  if (result.status !== 0) throw new Error(`${compiler} failed: ${result.stderr}`);
  return { directory, path };
};

/** The scripted ABI 3 fixture; `prefix` is prepended to its source. */
export const compileFixture = (
  prefix = "",
): { readonly directory: string; readonly path: string } =>
  compileLibrary(`${prefix}${readFileSync(fixtureSource, "utf8")}`, "fixture");

export const until = async (
  condition: () => boolean,
  message: string,
  timeoutMs = 5000,
): Promise<void> => {
  const deadline = performance.now() + timeoutMs;
  while (!condition()) {
    if (performance.now() >= deadline) throw new Error(message);
    await new Promise((resolve) => setTimeout(resolve, 5));
  }
};
