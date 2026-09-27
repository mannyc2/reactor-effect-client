import { createHash } from "node:crypto";
import { existsSync, readFileSync, realpathSync } from "node:fs";
import { createRequire } from "node:module";
import { join } from "node:path";

const packages = {
  effect: "effect",
  nodePlatform: "@effect/platform-node",
  nodeShared: "@effect/platform-node-shared",
} as const;
type StackKey = keyof typeof packages;
type PackageName = (typeof packages)[StackKey];
type Versions = Readonly<Record<StackKey, string>>;
interface Requirements {
  readonly effect: string;
  readonly nodePlatform: string;
  readonly nodeSharedOverride: string;
}
export interface StackSelection {
  readonly lockfileVersion: 2;
  readonly lockfileSha256: string;
  readonly requirements: Requirements;
  readonly selected: Versions;
}
interface WorkspaceResolution {
  readonly owner: string;
  readonly requested: PackageName;
  readonly name: PackageName;
  readonly version: string;
  readonly lockCoordinate: string;
}
interface Instance {
  readonly path: string;
  readonly name: PackageName;
  readonly version: string;
}
export interface ConsumerResolution {
  readonly name: "portable-node" | "browser" | "native";
  readonly installer: "bun" | "npm";
  readonly instances: readonly Instance[];
}
export interface QualificationStack extends StackSelection {
  readonly format: "reactor-effect-qualification-stack/v1";
  readonly selection: "frozen-workspace";
  readonly workspaceResolution: readonly WorkspaceResolution[];
  readonly consumers: readonly ConsumerResolution[];
}

const fail = (message: string): never => {
  throw new Error(`pack Effect qualification: ${message}`);
};
const record = (value: unknown, context: string): Record<string, unknown> => {
  if (typeof value !== "object" || value === null || Array.isArray(value))
    return fail(`${context}: expected an object`);
  return value as Record<string, unknown>;
};
const string = (value: unknown, context: string): string => {
  if (typeof value !== "string" || value.length === 0) return fail(`${context}: expected a string`);
  return value;
};
const array = (value: unknown, context: string): readonly unknown[] => {
  if (!Array.isArray(value)) return fail(`${context}: expected an array`);
  return value;
};
const keyFor = (name: string): StackKey | undefined => {
  for (const key of Object.keys(packages) as StackKey[]) if (packages[key] === name) return key;
  return undefined;
};
const exactVersion = (value: unknown, context: string): string => {
  const version = string(value, `${context} exact version`);
  // Bun's semver parser also accepts ranges and partial versions. Lock coordinates
  // and installed manifests must first be concrete, canonical SemVer strings.
  if (
    !/^(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)(?:-(?:0|[1-9]\d*|\d*[a-zA-Z-][0-9a-zA-Z-]*)(?:\.(?:0|[1-9]\d*|\d*[a-zA-Z-][0-9a-zA-Z-]*))*)?(?:\+[0-9a-zA-Z-]+(?:\.[0-9a-zA-Z-]+)*)?$/.test(
      version,
    )
  )
    return fail(`${context}: expected an exact version, got ${version}`);
  Bun.semver.order(version, version);
  return version;
};

interface VerifiedArchive {
  readonly name: string;
  readonly version: string;
  readonly specifier: string;
}

export const verifyInstalledArchive = (
  directory: string,
  archive: VerifiedArchive & { readonly fileSha256: Readonly<Record<string, string>> },
): VerifiedArchive => {
  for (const [path, expected] of Object.entries(archive.fileSha256)) {
    const installed = join(directory, "node_modules", archive.name, path);
    if (
      !existsSync(installed) ||
      createHash("sha256").update(readFileSync(installed)).digest("hex") !== expected
    )
      fail(`installed ${archive.name} differs from the exact archive: ${path}`);
  }
  return { name: archive.name, version: archive.version, specifier: archive.specifier };
};

/** Call only after byte-checking the installed archives. Bun does not write npm's
 * local-tarball provenance metadata. Exact verified requirements let npm validate
 * the dependency graph without mutating an installed package or hiding errors. */
export const verifiedArchiveRequirements = (
  value: unknown,
  archives: readonly VerifiedArchive[],
): Record<string, unknown> => {
  const manifest = record(value, "consumer manifest");
  const dependencies = { ...record(manifest.dependencies, "consumer dependencies") };
  for (const archive of archives) {
    if (
      dependencies[archive.name] !== archive.specifier &&
      dependencies[archive.name] !== `file:${archive.specifier}`
    )
      fail(`consumer ${archive.name}: dependency is not the installed archive`);
    dependencies[archive.name] = exactVersion(archive.version, `verified archive ${archive.name}`);
  }
  return { ...manifest, dependencies };
};

/** Selection comes only from frozen bytes; requirements remain public compatibility ranges. */
export const selectStack = (manifest: unknown, lockBytes: string | Uint8Array): StackSelection => {
  const root = record(manifest, "root manifest");
  const catalog = record(record(root.workspaces, "root workspaces").catalog, "root catalog");
  const overrides = record(root.overrides, "root overrides");
  const requirements: Requirements = {
    effect: string(catalog.effect, "catalog effect"),
    nodePlatform: string(catalog[packages.nodePlatform], "catalog platform-node"),
    nodeSharedOverride: string(overrides[packages.nodeShared], "shared-platform override"),
  };
  if (requirements.nodeSharedOverride !== requirements.effect)
    fail("the shared-platform override must match the Effect catalog requirement");
  let parsed: unknown;
  try {
    const lockText =
      typeof lockBytes === "string"
        ? lockBytes
        : new TextDecoder("utf-8", { fatal: true }).decode(lockBytes);
    parsed = Bun.JSONC.parse(lockText);
  } catch (cause) {
    throw new Error("pack Effect qualification: unsupported bun.lock JSONC", { cause });
  }
  const lock = record(parsed, "bun.lock");
  if (lock.lockfileVersion !== 2) fail("unsupported bun.lock lockfileVersion; expected 2");
  const lockedCatalog = record(lock.catalog, "lock catalog");
  const lockedOverrides = record(lock.overrides, "lock overrides");
  for (const name of [packages.effect, packages.nodePlatform])
    if (lockedCatalog[name] !== catalog[name])
      fail(`lock catalog ${name} differs from manifest; run bun install --frozen-lockfile`);
  if (lockedOverrides[packages.nodeShared] !== requirements.nodeSharedOverride)
    fail("lock shared-platform override differs from manifest; run bun install --frozen-lockfile");
  const tuples = record(lock.packages, "lock packages");
  const tupleVersion = (value: unknown, name: string, context: string): string => {
    const tuple = array(value, `${context} tuple`);
    if (tuple.length !== 4 || typeof tuple[1] !== "string" || typeof tuple[3] !== "string")
      return fail(`${context}: unsupported resolved package tuple`);
    record(tuple[2], `${context} tuple metadata`);
    const coordinate = string(tuple[0], `${context} coordinate`);
    if (!coordinate.startsWith(`${name}@`))
      return fail(`${context}: wrong package coordinate ${coordinate}`);
    return exactVersion(coordinate.slice(name.length + 1), context);
  };
  const fromTuple = (key: StackKey): string => {
    const name = packages[key];
    if (tuples[name] === undefined) return fail(`missing lock tuple for ${name}`);
    return tupleVersion(tuples[name], name, name);
  };
  const selected: Versions = {
    effect: fromTuple("effect"),
    nodePlatform: fromTuple("nodePlatform"),
    nodeShared: fromTuple("nodeShared"),
  };
  for (const key of Object.keys(packages) as StackKey[]) {
    const requirement = key === "nodeShared" ? requirements.nodeSharedOverride : requirements[key];
    if (!Bun.semver.satisfies(selected[key], requirement))
      fail(`${packages[key]}@${selected[key]} does not satisfy ${requirement}`);
    if (selected[key] !== selected.effect) fail("Effect/node/shared selections must be aligned");
  }
  // Bun may add qualified keys for a second instance. Inspect its coordinate as
  // well as its key so a nested conflicting version cannot hide behind an alias.
  // A nested key is `<parent package>/<name>`; `@scope/effect` is another package.
  const nestedUnder = (location: string, name: string): boolean =>
    location.endsWith(`/${name}`) && !/^@[^/]+$/.test(location.slice(0, -name.length - 1));
  for (const [location, value] of Object.entries(tuples)) {
    const tuple: readonly unknown[] = Array.isArray(value) ? value : [];
    for (const key of Object.keys(packages) as StackKey[]) {
      const name = packages[key];
      if (
        location === name ||
        nestedUnder(location, name) ||
        (typeof tuple[0] === "string" && tuple[0].startsWith(`${name}@`))
      ) {
        if (tupleVersion(value, name, location) !== selected[key])
          fail(`conflicting lock coordinate for ${name} at ${location}`);
      }
    }
  }
  return {
    lockfileVersion: 2,
    lockfileSha256: createHash("sha256").update(lockBytes).digest("hex"),
    requirements,
    selected,
  };
};

const checkedVersion = (
  name: PackageName,
  value: unknown,
  stack: StackSelection,
  context: string,
): string => {
  const version = exactVersion(value, `${context} ${name}`);
  const key = keyFor(name) ?? fail(`unsupported package ${name}`);
  if (version !== stack.selected[key])
    fail(
      `${context} ${name}@${version}, expected ${stack.selected[key]}; run bun install --frozen-lockfile`,
    );
  return version;
};

/** This small filesystem seam is shared by production and disposable resolver fixtures. */
export const resolveStackPackage = (
  ownerManifest: string,
  name: PackageName,
  stack: StackSelection,
) => {
  let path: string;
  let value: unknown;
  try {
    path = realpathSync(createRequire(ownerManifest).resolve(`${name}/package.json`));
    value = JSON.parse(readFileSync(path, "utf8"));
  } catch (cause) {
    throw new Error(
      `pack Effect qualification: ${ownerManifest} cannot qualify ${name}; run bun install --frozen-lockfile`,
      { cause },
    );
  }
  const metadata = record(value, `${ownerManifest} ${name}`);
  if (metadata.name !== name) fail(`${ownerManifest}: resolved ${name} with wrong package name`);
  return { path, version: checkedVersion(name, metadata.version, stack, ownerManifest) };
};

const workspaceEdges = ["client", "browser", "native"].flatMap((directory) => {
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
});

export const resolveWorkspaceStack = (
  root: string,
  stack: StackSelection,
): readonly WorkspaceResolution[] => {
  const owners = new Map<string, string>();
  const resolutions: WorkspaceResolution[] = [];
  for (const { owner, requested } of workspaceEdges) {
    const path = owners.get(owner) ?? join(root, owner, "package.json");
    if (!owners.has(owner)) {
      const metadata = record(JSON.parse(readFileSync(path, "utf8")), owner);
      const declared = [
        metadata.dependencies,
        metadata.devDependencies,
        metadata.peerDependencies,
      ].some((group) => {
        if (group === undefined) return false;
        const value = record(group, `${owner} dependencies`)[requested];
        if (value === undefined) return false;
        const requirement = string(value, `${owner} ${requested} requirement`);
        const key = keyFor(requested) ?? fail(`unsupported package ${requested}`);
        if (requirement !== "catalog:" && !Bun.semver.satisfies(stack.selected[key], requirement))
          fail(`${owner} declared ${requested} requirement differs from frozen selection`);
        return true;
      });
      if (!declared) fail(`${owner} must declare ${requested}`);
    }
    const resolved = resolveStackPackage(path, requested, stack);
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
};

/** npm ls remains a read-only diagnostic, including for Bun-installed consumers. */
export const inspectConsumerTree = (
  name: ConsumerResolution["name"],
  installer: ConsumerResolution["installer"],
  value: unknown,
  stack: StackSelection,
): ConsumerResolution => {
  const instances: Instance[] = [];
  const visit = (value: unknown, path: string): void => {
    const node = record(value, `${name} ${path}`);
    if (node.problems !== undefined && array(node.problems, `${name} ${path} problems`).length > 0)
      fail(`${name} ${path}: dependency problems`);
    if (node.invalid !== undefined && node.invalid !== false)
      fail(`${name} ${path}: invalid dependency`);
    if (node.dependencies === undefined) return;
    for (const [dependencyName, value] of Object.entries(
      record(node.dependencies, `${name} ${path} dependencies`),
    )) {
      const child = `${path}/dependencies/${dependencyName}`;
      const dependency = record(value, `${name} ${child}`);
      const key = keyFor(dependencyName);
      if (key !== undefined) {
        const packageName = packages[key];
        instances.push({
          path: child,
          name: packageName,
          version: checkedVersion(packageName, dependency.version, stack, `${name} ${child}`),
        });
      }
      visit(dependency, child);
    }
  };
  visit(value, ".");
  for (const required of name === "native" ? Object.values(packages) : [packages.effect])
    if (!instances.some((instance) => instance.name === required))
      fail(`${name}: missing required ${required}`);
  return { name, installer, instances };
};

/** Only completed consumer gates may enter the retained success identity. */
export const completeQualification = (
  stack: StackSelection,
  workspaceValue: unknown,
  consumersValue: unknown,
  profile: "portable" | "full",
): QualificationStack => {
  const workspaceResolution = array(workspaceValue, "workspace resolutions").map(
    (value, index): WorkspaceResolution => {
      const edge = record(value, "workspace resolution");
      const expected = workspaceEdges[index];
      if (
        expected === undefined ||
        edge.owner !== expected.owner ||
        edge.requested !== expected.requested ||
        edge.name !== expected.requested
      )
        return fail("workspace resolution differs from the required owner edges");
      const version = checkedVersion(expected.requested, edge.version, stack, expected.owner);
      if (edge.lockCoordinate !== `${expected.requested}@${version}`)
        fail("workspace lock coordinate differs");
      return {
        ...expected,
        name: expected.requested,
        version,
        lockCoordinate: `${expected.requested}@${version}`,
      };
    },
  );
  if (workspaceResolution.length !== workspaceEdges.length)
    fail("incomplete workspace resolutions");
  const expectedConsumers =
    profile === "full" ? ["portable-node", "browser", "native"] : ["portable-node", "browser"];
  const consumers = array(consumersValue, "completed consumers").map(
    (value, index): ConsumerResolution => {
      const consumer = record(value, "completed consumer");
      const name = consumer.name;
      if (
        (name !== "portable-node" && name !== "browser" && name !== "native") ||
        name !== expectedConsumers[index]
      )
        return fail("unexpected completed consumers");
      const installer = consumer.installer;
      if (installer !== "bun" && installer !== "npm") return fail(`${name}: invalid installer`);
      const instances = array(consumer.instances, `${name} instances`).map((value): Instance => {
        const instance = record(value, `${name} instance`);
        const key = keyFor(string(instance.name, `${name} instance name`));
        if (key === undefined) return fail(`${name}: unexpected stack instance`);
        const packageName = packages[key];
        return {
          path: string(instance.path, `${name} instance path`),
          name: packageName,
          version: checkedVersion(packageName, instance.version, stack, name),
        };
      });
      for (const required of name === "native" ? Object.values(packages) : [packages.effect])
        if (!instances.some((instance) => instance.name === required))
          fail(`${name}: missing required ${required}`);
      return { name, installer, instances };
    },
  );
  if (consumers.length !== expectedConsumers.length) fail("missing completed consumers");
  if (new Set(consumers.map((consumer) => consumer.installer)).size !== 1)
    fail("mixed consumer installers");
  return {
    format: "reactor-effect-qualification-stack/v1",
    selection: "frozen-workspace",
    ...stack,
    workspaceResolution,
    consumers,
  };
};
