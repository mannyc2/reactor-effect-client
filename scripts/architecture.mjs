import { isBuiltin } from "node:module";
import { readFileSync, readdirSync, realpathSync } from "node:fs";
import { dirname, join, relative, resolve, sep } from "node:path";
import * as posix from "node:path/posix";
import { fileURLToPath } from "node:url";
import { API } from "typescript/unstable/sync";
import * as Ast from "typescript/unstable/ast";

/** @typedef {{ specifier: string, runtime: boolean, line: number }} Dependency */
/** @typedef {{ path: string, imports: readonly Dependency[], problems: readonly string[] }} Module */
/** @typedef {{ name: string, exports: Record<string, { types?: string, import?: string }>, peerDependencies?: Record<string, string>, devDependencies?: Record<string, string>, dependencies?: Record<string, string>, optionalDependencies?: Record<string, string> }} Manifest */
/** @typedef {{ workspaces?: { catalog?: Record<string, string> }, overrides?: Record<string, string> }} RootManifest */
/** @typedef {{ directory: string, entries: Readonly<Record<string, string>>, area: (path: string) => string, forbidden: Readonly<Record<string, readonly string[]>>, hostBuiltins: boolean }} Rule */

export const EFFECT_PIN = "^4.0.0-rc.117";

/** @param {string} path */
const clientArea = (path) => {
  if (path === "src/session.ts" || path === "src/SessionTypes.ts") return "session";
  if (path.startsWith("src/session/")) return "session";
  for (const name of ["coordinator", "h3", "orchestration", "simulation", "testing"])
    if (path.startsWith(`src/${name}/`)) return name;
  // Submission and Sequence are reusable bounded primitives, not orchestration
  // policy; index.ts and host.ts are projections of the layers beneath them.
  return "core";
};

/**
 * Public paths are a compatibility contract, not a directory-discovery rule.
 * Host boundaries between packages are enforced by their declared dependencies;
 * these rules cover the layering inside each package and host-only imports.
 * @type {Readonly<Record<string, Rule>>}
 */
export const rules = Object.freeze({
  "reactor-effect-client": {
    directory: "packages/client",
    entries: Object.freeze({
      ".": "index",
      "./h3": "h3/index",
      "./orchestration": "orchestration/index",
      "./simulation": "simulation/index",
      "./testing": "testing/index",
      "./wire": "wire",
      "./host": "host",
    }),
    area: clientArea,
    forbidden: Object.freeze({
      core: ["h3", "orchestration", "simulation", "testing"],
      session: ["h3", "orchestration", "simulation", "testing"],
      coordinator: ["session", "h3", "orchestration", "simulation", "testing"],
      h3: ["coordinator", "orchestration", "simulation", "testing"],
      orchestration: ["simulation", "testing"],
      simulation: ["testing"],
      testing: [],
    }),
    hostBuiltins: false,
  },
  "reactor-effect-browser": {
    directory: "packages/browser",
    entries: Object.freeze({ ".": "index" }),
    area: () => "browser",
    forbidden: Object.freeze({}),
    hostBuiltins: false,
  },
  "reactor-effect-native": {
    directory: "packages/native",
    entries: Object.freeze({ ".": "index" }),
    area: () => "native",
    forbidden: Object.freeze({}),
    hostBuiltins: true,
  },
});

/** @param {RootManifest} manifest */
export const checkWorkspace = (manifest) => {
  const problems = [];
  if (manifest.workspaces?.catalog?.effect !== EFFECT_PIN)
    problems.push(`workspace catalog must pin effect to ${EFFECT_PIN}`);
  if (manifest.overrides?.["@effect/platform-node-shared"] !== EFFECT_PIN)
    problems.push(`workspace override for @effect/platform-node-shared must remain ${EFFECT_PIN}`);
  return problems;
};

/** @param {Manifest} manifest @param {Rule} rule */
export const checkManifest = (manifest, rule) => {
  const problems = [];
  if (
    JSON.stringify(Object.keys(manifest.exports ?? {}).sort()) !==
    JSON.stringify(Object.keys(rule.entries).sort())
  )
    problems.push(`${manifest.name} must retain exactly its public exports`);
  for (const [name, source] of Object.entries(rule.entries)) {
    const target = manifest.exports?.[name];
    if (target?.types !== `./dist/${source}.d.ts` || target.import !== `./dist/${source}.js`)
      problems.push(`public export ${name} must retain its types/import targets`);
  }
  for (const [group, dependencies] of Object.entries({
    peerDependencies: manifest.peerDependencies,
    devDependencies: manifest.devDependencies,
  })) {
    if (dependencies?.effect !== "catalog:")
      problems.push(`${group}.effect must be pinned through the workspace catalog`);
  }
  for (const [group, dependencies] of Object.entries({
    dependencies: manifest.dependencies,
    peerDependencies: manifest.peerDependencies,
    devDependencies: manifest.devDependencies,
  }))
    for (const [name, version] of Object.entries(dependencies ?? {}))
      if (name in rules && version !== "workspace:*")
        problems.push(`${group}.${name} must use the workspace protocol`);
  return problems;
};

/** @param {readonly Module[]} modules @param {Manifest} manifest @param {Rule} rule */
export const checkGraph = (modules, manifest, rule) => {
  const problems = modules.flatMap((module) => module.problems);
  const paths = new Set(modules.map((module) => module.path));
  const dependencies = new Set([
    ...Object.keys(manifest.dependencies ?? {}),
    ...Object.keys(manifest.peerDependencies ?? {}),
    ...Object.keys(manifest.optionalDependencies ?? {}),
  ]);
  /** @type {Map<string, string[]>} */
  const runtime = new Map();
  for (const module of modules) {
    const owner = rule.area(module.path);
    const edges = [];
    for (const dependency of module.imports) {
      const { specifier } = dependency;
      const location = `${module.path}:${dependency.line}`;
      if (specifier.startsWith(".")) {
        const target = posix
          .normalize(posix.join(posix.dirname(module.path), specifier))
          .replace(/\.js$/, ".ts");
        if (!target.startsWith("src/") || !specifier.endsWith(".js") || !paths.has(target)) {
          problems.push(
            `${location}: missing, escaping, or non-explicit source import ${specifier}`,
          );
          continue;
        }
        if (rule.forbidden[owner]?.includes(rule.area(target)))
          problems.push(
            `${location}: forbidden ${owner} -> ${rule.area(target)} dependency (${target})`,
          );
        if (dependency.runtime) edges.push(target);
      } else if (isBuiltin(specifier)) {
        if (!rule.hostBuiltins)
          problems.push(`${location}: Node builtin outside native boundary: ${specifier}`);
      } else if (specifier === "bun" || specifier.startsWith("bun:")) {
        problems.push(`${location}: Bun host dependency in runtime source: ${specifier}`);
      } else {
        const name = specifier.startsWith("@")
          ? specifier.split("/").slice(0, 2).join("/")
          : (specifier.split("/")[0] ?? specifier);
        if (name === "koffi" && !rule.hostBuiltins)
          problems.push(`${location}: Koffi outside native boundary`);
        else if (name === manifest.name || !dependencies.has(name))
          problems.push(`${location}: undeclared or self dependency ${specifier}`);
      }
    }
    runtime.set(module.path, edges);
  }
  // Type-only backreferences do not initialize a module. Runtime cycles do.
  const complete = new Set();
  /** @type {string[]} */
  const active = [];
  /** @param {string} path */
  const visit = (path) => {
    const cycle = active.indexOf(path);
    if (cycle >= 0) {
      problems.push(`runtime import cycle: ${[...active.slice(cycle), path].join(" -> ")}`);
      return;
    }
    if (complete.has(path)) return;
    active.push(path);
    for (const target of runtime.get(path) ?? []) visit(target);
    active.pop();
    complete.add(path);
  };
  for (const path of [...paths].sort()) visit(path);
  for (const source of Object.values(rule.entries))
    if (!paths.has(`src/${source}.ts`))
      problems.push(`missing public source entry src/${source}.ts`);
  return problems;
};

/** @param {string} directory @returns {string[]} */
const sourceFiles = (directory) =>
  readdirSync(directory, { withFileTypes: true }).flatMap((entry) => {
    const path = join(directory, entry.name);
    return entry.isDirectory()
      ? sourceFiles(path)
      : entry.isFile() && path.endsWith(".ts")
        ? [path]
        : [];
  });

/**
 * Use the pinned compiler's syntax tree, not regexes over comments/strings.
 * TypeScript 7's synchronous API requires Node's child-process pipe handles;
 * the CLI therefore runs on Node even when invoked by Bun's package runner.
 * @param {string} root a package directory holding tsconfig.build.json and src/
 * @returns {Module[]}
 */
export const readGraph = (root) => {
  const api = new API({ cwd: root });
  try {
    const config = join(root, "tsconfig.build.json");
    const snapshot = api.updateSnapshot({ openProjects: [config] });
    try {
      const project = snapshot.getProject(config);
      if (project === undefined) throw new Error(`architecture: cannot load ${config}`);
      return sourceFiles(join(root, "src"))
        .sort()
        .map((path) => {
          const file = project.program.getSourceFile(path);
          if (file === undefined)
            throw new Error(`architecture: source omitted from build: ${path}`);
          const name = relative(root, path).split(sep).join("/");
          /** @type {Dependency[]} */
          const imports = [];
          /** @type {string[]} */
          const problems = [];
          if (project.program.getSyntacticDiagnostics(path).length > 0)
            problems.push(`${name}: syntax errors prevent reliable dependency analysis`);
          /** @param {Ast.Node | undefined} value @param {Ast.Node} node @param {boolean} runtime */
          const add = (value, node, runtime) => {
            const line = file.getLineAndCharacterOfPosition(node.getStart(file)).line + 1;
            if (
              value === undefined ||
              !(Ast.isStringLiteral(value) || Ast.isNoSubstitutionTemplateLiteral(value))
            ) {
              problems.push(`${name}:${line}: module loads must have a literal specifier`);
              return;
            }
            imports.push({ specifier: value.text, runtime, line });
          };
          /** @param {Ast.Node} node */
          const visit = (node) => {
            if (Ast.isImportDeclaration(node)) {
              const clause = node.importClause;
              const bindings = clause?.namedBindings;
              const onlyTypes =
                clause?.phaseModifier === Ast.SyntaxKind.TypeKeyword ||
                (clause?.name === undefined &&
                  bindings !== undefined &&
                  Ast.isNamedImports(bindings) &&
                  bindings.elements.length > 0 &&
                  bindings.elements.every((element) => element.isTypeOnly));
              add(node.moduleSpecifier, node, !onlyTypes);
            } else if (Ast.isExportDeclaration(node) && node.moduleSpecifier !== undefined) {
              const clause = node.exportClause;
              const onlyTypes =
                node.isTypeOnly ||
                (clause !== undefined &&
                  Ast.isNamedExports(clause) &&
                  clause.elements.length > 0 &&
                  clause.elements.every((element) => element.isTypeOnly));
              add(node.moduleSpecifier, node, !onlyTypes);
            } else if (Ast.isImportTypeNode(node)) {
              add(
                Ast.isLiteralTypeNode(node.argument) ? node.argument.literal : undefined,
                node,
                false,
              );
            } else if (
              Ast.isImportEqualsDeclaration(node) &&
              Ast.isExternalModuleReference(node.moduleReference)
            ) {
              add(node.moduleReference.expression, node, !node.isTypeOnly);
            } else if (
              Ast.isCallExpression(node) &&
              (node.expression.kind === Ast.SyntaxKind.ImportKeyword ||
                (Ast.isIdentifier(node.expression) && node.expression.text === "require"))
            ) {
              add(node.arguments[0], node, true);
            }
            node.forEachChild(visit);
          };
          visit(file);
          return { path: name, imports, problems };
        });
    } finally {
      snapshot.dispose();
    }
  } finally {
    api.close();
  }
};

/** @param {string} directory @returns {{ problems: string[], summary: string }} */
export const checkPackage = (directory) => {
  /** @type {Manifest} */
  const manifest = JSON.parse(readFileSync(join(directory, "package.json"), "utf8"));
  const rule = rules[manifest.name];
  if (rule === undefined) throw new Error(`architecture: no rules for package ${manifest.name}`);
  const graph = readGraph(directory);
  const problems = [...checkManifest(manifest, rule), ...checkGraph(graph, manifest, rule)];
  const exports = Object.keys(rule.entries).length;
  return {
    problems,
    summary: `${manifest.name}: ${graph.length} source modules; ${exports} exports; no runtime cycles`,
  };
};

if (process.argv[1] !== undefined && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  const args = process.argv.slice(2);
  if (args.length !== 0 && !(args.length === 2 && args[0] === "--package"))
    throw new Error("usage: node scripts/architecture.mjs [--package directory]");
  const problems = [];
  const summaries = [];
  if (args[1] !== undefined) {
    const result = checkPackage(realpathSync(args[1]));
    problems.push(...result.problems);
    summaries.push(result.summary);
  } else {
    const root = realpathSync(join(dirname(fileURLToPath(import.meta.url)), ".."));
    /** @type {RootManifest} */
    const workspace = JSON.parse(readFileSync(join(root, "package.json"), "utf8"));
    problems.push(...checkWorkspace(workspace));
    for (const rule of Object.values(rules)) {
      const result = checkPackage(join(root, rule.directory));
      problems.push(...result.problems);
      summaries.push(result.summary);
    }
  }
  if (problems.length > 0) {
    console.error(problems.join("\n"));
    process.exitCode = 1;
  } else console.log(`architecture-ok Effect ${EFFECT_PIN} via catalog\n${summaries.join("\n")}`);
}
