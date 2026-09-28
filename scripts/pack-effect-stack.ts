/**
 * Selects the Effect stack pack qualifies from the frozen bun.lock, and checks that the workspace
 * owners and every installed consumer resolve exactly that stack. It runs on Bun, which reads
 * bun.lock with Bun.JSONC and compares versions with Bun.semver.
 */
import { createRequire } from "node:module";
import * as Crypto from "effect/Crypto";
import * as Effect from "effect/Effect";
import * as Encoding from "effect/Encoding";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";

export class EffectStackError extends Schema.TaggedError<EffectStackError>(
  "reactor-effect/scripts/pack-effect-stack/EffectStackError",
)("EffectStackError", {
  message: Schema.String,
  cause: Schema.optionalKey(Schema.Defect()),
}) {}

const failure = (message: string) => EffectStackError.make({ message });

const stackKeys = ["effect", "nodePlatform", "nodeShared"] as const;
type StackKey = (typeof stackKeys)[number];
const packages = {
  effect: "effect",
  nodePlatform: "@effect/platform-node",
  nodeShared: "@effect/platform-node-shared",
} as const satisfies Record<StackKey, string>;
const StackPackage = Schema.Literals([packages.effect, packages.nodePlatform, packages.nodeShared]);
type StackPackage = typeof StackPackage.Type;
const keyFor = (name: string): StackKey | undefined =>
  stackKeys.find((key) => packages[key] === name);

const Versions = Schema.Struct({
  effect: Schema.String,
  nodePlatform: Schema.String,
  nodeShared: Schema.String,
});

/** The exact stack a frozen bun.lock selects, and the public ranges it must satisfy. */
const StackSelection = Schema.Struct({
  lockfileVersion: Schema.Literal(2),
  lockfileSha256: Schema.String,
  requirements: Schema.Struct({
    effect: Schema.String,
    nodePlatform: Schema.String,
    nodeSharedOverride: Schema.String,
  }),
  selected: Versions,
});
type StackSelection = typeof StackSelection.Type;

const WorkspaceResolution = Schema.Struct({
  owner: Schema.String,
  requested: StackPackage,
  name: StackPackage,
  version: Schema.String,
  lockCoordinate: Schema.String,
});
type WorkspaceResolution = typeof WorkspaceResolution.Type;

const Instance = Schema.Struct({ path: Schema.String, name: StackPackage, version: Schema.String });
type Instance = typeof Instance.Type;

/** The stack instances one consumer's installer placed, as npm ls reports them. */
const ConsumerResolution = Schema.Struct({
  name: Schema.Literals(["portable-node", "browser", "native"]),
  installer: Schema.Literals(["bun", "npm"]),
  instances: Schema.Array(Instance),
});
export type ConsumerResolution = typeof ConsumerResolution.Type;

/** What package-identity.json records of the stack once every consumer has passed. */
export const QualificationStack = Schema.Struct({
  format: Schema.Literal("reactor-effect-qualification-stack/v1"),
  selection: Schema.Literal("frozen-workspace"),
  ...StackSelection.fields,
  workspaceResolution: Schema.Array(WorkspaceResolution),
  consumers: Schema.Array(ConsumerResolution),
});
type QualificationStack = typeof QualificationStack.Type;

/** What selection reads of the root manifest: the Effect ranges its catalog and overrides pin. */
const StackManifest = Schema.Struct({
  workspaces: Schema.Struct({
    catalog: Schema.Struct({
      effect: Schema.NonEmptyString,
      "@effect/platform-node": Schema.NonEmptyString,
    }),
  }),
  overrides: Schema.Struct({ "@effect/platform-node-shared": Schema.NonEmptyString }),
});

/** A bun.lock package entry: a tuple led by its coordinate, `<name>@<version>`. */
const LockEntry = Schema.TupleWithRest(Schema.Tuple([Schema.String]), [Schema.Unknown]);
type LockEntry = typeof LockEntry.Type;

const Lockfile = Schema.Struct({
  lockfileVersion: Schema.Literal(2).annotate({
    message: "unsupported lockfileVersion; expected 2",
  }),
  catalog: Schema.Record(Schema.String, Schema.String),
  overrides: Schema.Record(Schema.String, Schema.String),
  packages: Schema.Record(Schema.String, LockEntry),
});

/** A package bun.lock resolved from a registry: coordinate, registry, metadata and integrity. */
const ResolvedPackage = Schema.Tuple([
  Schema.NonEmptyString,
  Schema.String,
  Schema.Record(Schema.String, Schema.Unknown),
  Schema.String,
]);

const Dependencies = Schema.Record(Schema.String, Schema.NonEmptyString);

/** A workspace owner's manifest, where it declares the stack packages it resolves. */
const OwnerManifest = Schema.fromJsonString(
  Schema.Struct({
    dependencies: Schema.optionalKey(Dependencies),
    devDependencies: Schema.optionalKey(Dependencies),
    peerDependencies: Schema.optionalKey(Dependencies),
  }),
);

/** An installed stack package's manifest. */
const InstalledManifest = Schema.fromJsonString(
  Schema.Struct({ name: Schema.String, version: Schema.optionalKey(Schema.String) }),
);

/**
 * A consumer's manifest: the private module pack writes, and the dependencies its install added
 * after it. The fields keep that order, and anything else the installer wrote follows them.
 */
export const ConsumerManifest = Schema.StructWithRest(
  Schema.Struct({
    private: Schema.Literal(true),
    type: Schema.Literal("module"),
    overrides: Schema.optionalKey(Schema.Record(Schema.String, Schema.String)),
    dependencies: Schema.Record(Schema.String, Schema.String),
  }),
  [Schema.Record(Schema.String, Schema.Json)],
);
type ConsumerManifest = typeof ConsumerManifest.Type;

/** A node of `npm ls --json` output. */
interface DependencyNode {
  readonly version?: string;
  readonly problems?: ReadonlyArray<unknown>;
  readonly invalid?: unknown;
  readonly dependencies?: { readonly [name: string]: DependencyNode };
}
const DependencyNode: Schema.Codec<DependencyNode> = Schema.Struct({
  version: Schema.optionalKey(Schema.String),
  problems: Schema.Unknown.pipe(Schema.Array, Schema.optionalKey),
  invalid: Schema.optionalKey(Schema.Unknown),
  dependencies: Schema.optionalKey(
    Schema.Record(
      Schema.String,
      Schema.suspend((): Schema.Codec<DependencyNode> => DependencyNode),
    ),
  ),
});
const DependencyTree = Schema.fromJsonString(DependencyNode);

/** Every node of a dependency tree, each before its own dependencies, with its path and name. */
const nodesOf = (
  node: DependencyNode,
  path: string,
  name?: string,
): ReadonlyArray<readonly [path: string, name: string | undefined, node: DependencyNode]> => [
  [path, name, node],
  ...Object.entries(node.dependencies ?? {}).flatMap(([dependency, child]) =>
    nodesOf(child, `${path}/dependencies/${dependency}`, dependency),
  ),
];

/** The lowercase hex SHA-256 of `bytes`, as pack records archives and their files. */
export const sha256 = Effect.fnUntraced(function* (bytes: Uint8Array) {
  const crypto = yield* Crypto.Crypto;
  return Encoding.encodeHex(yield* crypto.digest("SHA-256", bytes));
});

const exactVersionPattern =
  /^(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)(?:-(?:0|[1-9]\d*|\d*[a-zA-Z-][0-9a-zA-Z-]*)(?:\.(?:0|[1-9]\d*|\d*[a-zA-Z-][0-9a-zA-Z-]*))*)?(?:\+[0-9a-zA-Z-]+(?:\.[0-9a-zA-Z-]+)*)?$/;

const exactVersion = Effect.fnUntraced(function* (version: string | undefined, context: string) {
  if (version === undefined || version.length === 0)
    return yield* failure(`${context} exact version: expected a string`);
  // Bun's semver parser also accepts ranges and partial versions. Lock coordinates
  // and installed manifests must first be concrete, canonical SemVer strings.
  if (!exactVersionPattern.test(version))
    return yield* failure(`${context}: expected an exact version, got ${version}`);
  Bun.semver.order(version, version);
  return version;
});

const checkedVersion = Effect.fnUntraced(function* (
  name: StackPackage,
  value: string | undefined,
  stack: StackSelection,
  context: string,
) {
  const version = yield* exactVersion(value, `${context} ${name}`);
  const key = keyFor(name);
  if (key === undefined) return yield* failure(`unsupported package ${name}`);
  if (version !== stack.selected[key])
    return yield* failure(
      `${context} ${name}@${version}, expected ${stack.selected[key]}; run bun install --frozen-lockfile`,
    );
  return version;
});

interface VerifiedArchive {
  readonly name: string;
  readonly version: string;
  readonly specifier: string;
}

export const verifyInstalledArchive = Effect.fnUntraced(function* (
  directory: string,
  archive: VerifiedArchive & { readonly fileSha256: Readonly<Record<string, string>> },
) {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  for (const [file, expected] of Object.entries(archive.fileSha256)) {
    const installed = path.join(directory, "node_modules", archive.name, file);
    if (
      !(yield* fs.exists(installed)) ||
      (yield* sha256(yield* fs.readFile(installed))) !== expected
    )
      return yield* failure(`installed ${archive.name} differs from the exact archive: ${file}`);
  }
  const verified: VerifiedArchive = {
    name: archive.name,
    version: archive.version,
    specifier: archive.specifier,
  };
  return verified;
});

/** Call only after byte-checking the installed archives. Bun does not write npm's
 * local-tarball provenance metadata. Exact verified requirements let npm validate
 * the dependency graph without mutating an installed package or hiding errors. */
export const verifiedArchiveRequirements = Effect.fnUntraced(function* (
  manifest: ConsumerManifest,
  archives: ReadonlyArray<VerifiedArchive>,
) {
  const dependencies = { ...manifest.dependencies };
  for (const archive of archives) {
    if (
      dependencies[archive.name] !== archive.specifier &&
      dependencies[archive.name] !== `file:${archive.specifier}`
    )
      return yield* failure(`consumer ${archive.name}: dependency is not the installed archive`);
    dependencies[archive.name] = yield* exactVersion(
      archive.version,
      `verified archive ${archive.name}`,
    );
  }
  const normalized: ConsumerManifest = { ...manifest, dependencies };
  return normalized;
});

/** A key is `[<parent package>/]<package>`; the package is its last segment, or its last two
 * when the second-to-last is a scope, so `x/@acme/effect` is not Effect. */
const packageAt = (location: string): string => {
  const parts = location.split("/");
  const scope = parts.at(-2);
  return scope?.startsWith("@") === true ? `${scope}/${parts.at(-1)}` : (parts.at(-1) ?? "");
};

/** Selection comes only from frozen bytes; requirements remain public compatibility ranges. */
export const selectStack = Effect.fnUntraced(function* (
  manifest: unknown,
  lockBytes: string | Uint8Array,
) {
  const root = yield* Schema.decodeUnknownEffect(StackManifest)(manifest).pipe(
    Effect.mapError((error) => failure(`root manifest: ${error.message}`)),
  );
  const catalog = root.workspaces.catalog;
  const requirements = {
    effect: catalog.effect,
    nodePlatform: catalog[packages.nodePlatform],
    nodeSharedOverride: root.overrides[packages.nodeShared],
  };
  if (requirements.nodeSharedOverride !== requirements.effect)
    return yield* failure("the shared-platform override must match the Effect catalog requirement");
  const parsed = yield* Effect.try({
    try: () =>
      Bun.JSONC.parse(
        typeof lockBytes === "string"
          ? lockBytes
          : new TextDecoder("utf-8", { fatal: true }).decode(lockBytes),
      ),
    catch: (cause) => EffectStackError.make({ message: "unsupported bun.lock JSONC", cause }),
  });
  const lock = yield* Schema.decodeUnknownEffect(Lockfile)(parsed).pipe(
    Effect.mapError((error) => failure(`bun.lock: ${error.message}`)),
  );
  for (const name of [packages.effect, packages.nodePlatform])
    if (lock.catalog[name] !== catalog[name])
      return yield* failure(
        `lock catalog ${name} differs from manifest; run bun install --frozen-lockfile`,
      );
  if (lock.overrides[packages.nodeShared] !== requirements.nodeSharedOverride)
    return yield* failure(
      "lock shared-platform override differs from manifest; run bun install --frozen-lockfile",
    );
  const tupleVersion = Effect.fnUntraced(function* (
    entry: LockEntry,
    name: StackPackage,
    context: string,
  ) {
    const [coordinate] = yield* Schema.decodeUnknownEffect(ResolvedPackage)(entry).pipe(
      Effect.mapError(() => failure(`${context}: unsupported resolved package tuple`)),
    );
    if (!coordinate.startsWith(`${name}@`))
      return yield* failure(`${context}: wrong package coordinate ${coordinate}`);
    return yield* exactVersion(coordinate.slice(name.length + 1), context);
  });
  const fromTuple = Effect.fnUntraced(function* (key: StackKey) {
    const name = packages[key];
    const entry = lock.packages[name];
    if (entry === undefined) return yield* failure(`missing lock tuple for ${name}`);
    return yield* tupleVersion(entry, name, name);
  });
  const selected = {
    effect: yield* fromTuple("effect"),
    nodePlatform: yield* fromTuple("nodePlatform"),
    nodeShared: yield* fromTuple("nodeShared"),
  };
  for (const key of stackKeys) {
    const requirement = key === "nodeShared" ? requirements.nodeSharedOverride : requirements[key];
    if (!Bun.semver.satisfies(selected[key], requirement))
      return yield* failure(`${packages[key]}@${selected[key]} does not satisfy ${requirement}`);
    if (selected[key] !== selected.effect)
      return yield* failure("Effect/node/shared selections must be aligned");
  }
  // Bun may add qualified keys for a second instance. Inspect its coordinate as
  // well as its key so a nested conflicting version cannot hide behind an alias.
  for (const [location, entry] of Object.entries(lock.packages))
    for (const key of stackKeys) {
      const name = packages[key];
      if (packageAt(location) !== name && !entry[0].startsWith(`${name}@`)) continue;
      if ((yield* tupleVersion(entry, name, location)) !== selected[key])
        return yield* failure(`conflicting lock coordinate for ${name} at ${location}`);
    }
  const selection: StackSelection = {
    lockfileVersion: 2,
    lockfileSha256: yield* sha256(
      typeof lockBytes === "string" ? new TextEncoder().encode(lockBytes) : lockBytes,
    ),
    requirements,
    selected,
  };
  return selection;
});

/** This small filesystem seam is shared by production and disposable resolver fixtures. */
export const resolveStackPackage = Effect.fnUntraced(function* (
  ownerManifest: string,
  name: StackPackage,
  stack: StackSelection,
) {
  const fs = yield* FileSystem.FileSystem;
  const unresolved = (cause: unknown) =>
    EffectStackError.make({
      message: `${ownerManifest} cannot qualify ${name}; run bun install --frozen-lockfile`,
      cause,
    });
  const resolved = yield* Effect.try({
    try: () => createRequire(ownerManifest).resolve(`${name}/package.json`),
    catch: unresolved,
  });
  const path = yield* fs.realPath(resolved).pipe(Effect.mapError(unresolved));
  const text = yield* fs.readFileString(path).pipe(Effect.mapError(unresolved));
  const metadata = yield* Schema.decodeEffect(InstalledManifest)(text).pipe(
    Effect.mapError((error) => failure(`${ownerManifest} ${name}: ${error.message}`)),
  );
  if (metadata.name !== name)
    return yield* failure(`${ownerManifest}: resolved ${name} with wrong package name`);
  return { path, version: yield* checkedVersion(name, metadata.version, stack, ownerManifest) };
});

const workspaceEdges = ["client", "browser", "native"].flatMap(
  (directory): ReadonlyArray<{ readonly owner: string; readonly requested: StackPackage }> => {
    const owner = `packages/${directory}`;
    const nodeOwner = `${owner} -> ${packages.nodePlatform}`;
    const sharedOwner = `${nodeOwner} -> ${packages.nodeShared}`;
    return [
      { owner, requested: packages.effect },
      ...(directory === "browser"
        ? []
        : [
            { owner, requested: packages.nodePlatform },
            { owner: nodeOwner, requested: packages.effect },
            { owner: nodeOwner, requested: packages.nodeShared },
            { owner: sharedOwner, requested: packages.effect },
          ]),
    ];
  },
);

export const resolveWorkspaceStack = Effect.fnUntraced(function* (
  root: string,
  stack: StackSelection,
) {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const owners = new Map<string, string>();
  const resolutions: Array<WorkspaceResolution> = [];
  for (const { owner, requested } of workspaceEdges) {
    const ownerPath = owners.get(owner) ?? path.join(root, owner, "package.json");
    if (!owners.has(owner)) {
      const metadata = yield* Schema.decodeEffect(OwnerManifest)(
        yield* fs.readFileString(ownerPath),
      ).pipe(Effect.mapError((error) => failure(`${owner}: ${error.message}`)));
      const requirement = [
        metadata.dependencies,
        metadata.devDependencies,
        metadata.peerDependencies,
      ]
        .map((group) => group?.[requested])
        .find((value) => value !== undefined);
      if (requirement === undefined) return yield* failure(`${owner} must declare ${requested}`);
      const key = keyFor(requested);
      if (key === undefined) return yield* failure(`unsupported package ${requested}`);
      if (requirement !== "catalog:" && !Bun.semver.satisfies(stack.selected[key], requirement))
        return yield* failure(
          `${owner} declared ${requested} requirement differs from frozen selection`,
        );
    }
    const resolved = yield* resolveStackPackage(ownerPath, requested, stack);
    owners.set(`${owner} -> ${requested}`, resolved.path);
    resolutions.push({
      owner,
      requested,
      name: requested,
      version: resolved.version,
      lockCoordinate: `${requested}@${resolved.version}`,
    });
  }
  return resolutions;
});

/** npm ls remains a read-only diagnostic, including for Bun-installed consumers. */
export const inspectConsumerTree = Effect.fnUntraced(function* (
  name: ConsumerResolution["name"],
  installer: ConsumerResolution["installer"],
  output: string,
  stack: StackSelection,
) {
  const tree = yield* Schema.decodeEffect(DependencyTree)(output).pipe(
    Effect.mapError((error) => failure(`${name}: ${error.message}`)),
  );
  const instances: Array<Instance> = [];
  for (const [path, dependency, node] of nodesOf(tree, ".")) {
    const key = dependency === undefined ? undefined : keyFor(dependency);
    if (key !== undefined) {
      const packageName = packages[key];
      instances.push({
        path,
        name: packageName,
        version: yield* checkedVersion(packageName, node.version, stack, `${name} ${path}`),
      });
    }
    if (node.problems !== undefined && node.problems.length > 0)
      return yield* failure(`${name} ${path}: dependency problems`);
    if (node.invalid !== undefined && node.invalid !== false)
      return yield* failure(`${name} ${path}: invalid dependency`);
  }
  for (const required of name === "native" ? Object.values(packages) : [packages.effect])
    if (!instances.some((instance) => instance.name === required))
      return yield* failure(`${name}: missing required ${required}`);
  const resolution: ConsumerResolution = { name, installer, instances };
  return resolution;
});

/** Only completed consumer gates may enter the retained success identity. */
export const completeQualification = Effect.fnUntraced(function* (
  stack: StackSelection,
  workspace: ReadonlyArray<WorkspaceResolution>,
  consumers: ReadonlyArray<ConsumerResolution>,
  profile: "portable" | "full",
) {
  const workspaceResolution: Array<WorkspaceResolution> = [];
  for (const [index, edge] of workspace.entries()) {
    const expected = workspaceEdges[index];
    if (
      expected === undefined ||
      edge.owner !== expected.owner ||
      edge.requested !== expected.requested ||
      edge.name !== expected.requested
    )
      return yield* failure("workspace resolution differs from the required owner edges");
    const version = yield* checkedVersion(expected.requested, edge.version, stack, expected.owner);
    const lockCoordinate = `${expected.requested}@${version}`;
    if (edge.lockCoordinate !== lockCoordinate)
      return yield* failure("workspace lock coordinate differs");
    workspaceResolution.push({ ...expected, name: expected.requested, version, lockCoordinate });
  }
  if (workspaceResolution.length !== workspaceEdges.length)
    return yield* failure("incomplete workspace resolutions");
  const expectedConsumers =
    profile === "full" ? ["portable-node", "browser", "native"] : ["portable-node", "browser"];
  const completed: Array<ConsumerResolution> = [];
  for (const [index, consumer] of consumers.entries()) {
    if (consumer.name !== expectedConsumers[index])
      return yield* failure("unexpected completed consumers");
    const instances: Array<Instance> = [];
    for (const instance of consumer.instances) {
      const key = keyFor(instance.name);
      if (key === undefined) return yield* failure(`${consumer.name}: unexpected stack instance`);
      const packageName = packages[key];
      instances.push({
        path: instance.path,
        name: packageName,
        version: yield* checkedVersion(packageName, instance.version, stack, consumer.name),
      });
    }
    for (const required of consumer.name === "native" ? Object.values(packages) : [packages.effect])
      if (!instances.some((instance) => instance.name === required))
        return yield* failure(`${consumer.name}: missing required ${required}`);
    completed.push({ name: consumer.name, installer: consumer.installer, instances });
  }
  if (completed.length !== expectedConsumers.length)
    return yield* failure("missing completed consumers");
  if (new Set(completed.map((consumer) => consumer.installer)).size !== 1)
    return yield* failure("mixed consumer installers");
  const qualification: QualificationStack = {
    format: "reactor-effect-qualification-stack/v1",
    selection: "frozen-workspace",
    ...stack,
    workspaceResolution,
    consumers: completed,
  };
  return qualification;
});
