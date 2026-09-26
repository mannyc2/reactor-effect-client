# Proposed exact Effect qualification contract for plan 005

Status: proposed implementation contract, not implemented. Read with [005](005-reproducible-effect-qualification.md). Baseline: `c47e50c784f6889b542cf8ee316067b688ca4737`. Only read-only metadata resolution was performed; no install, build or pack ran.

## Decision and evidence

Select the exact Effect stack already named by the frozen workspace lockfile and verify that the installed workspace resolves that stack. Install those exact versions into each clean package consumer. Public peer ranges and `package-identity.json.effect` remain unchanged. New compatibility testing against the newest range-satisfying version is outside this fix.

The baseline lock has `lockfileVersion: 2`, catalog/override ranges `^4.0.0-rc.117` at `bun.lock:147–159`, and exact package tuples for platform-node/shared at `:164–166` and Effect at `:426`. Relevant declared workspace dependencies are client `:77–91`, browser `:50–62`, and native `:107–125`. The root does not directly declare Effect/platform-node (`:5–18`), so root `require.resolve` is not a legitimate source of the selected stack.

Read-only `createRequire(<workspace>/package.json).resolve(<package>/package.json)` succeeded in the actual installed checkout for client/browser/native Effect and client/native platform-node. Resolving platform-node-shared from the resolved platform-node package also succeeded. All were rc.117, with the same Effect realpath under Bun's isolated store. These observations validate the resolver seam, not the state of any future checkout or tarball.

## Exact record and resolver

Use one concrete `QualificationStack` record, validated from unknown:

- `format: "reactor-effect-qualification-stack/v1"`.
- `selection: "frozen-workspace"`, `lockfileVersion: 2`, `lockfileSha256`.
- `requirements`: original catalog `effect`, `nodePlatform`, and root `nodeSharedOverride` strings, retained as ranges.
- `selected`: exact `effect`, `nodePlatform`, and `nodeShared` versions.
- `workspaceResolution`: package owner, requested name, exact name/version, and lock coordinate for each checked edge; use repository-relative owner names, not machine paths.
- `consumers`: completed consumer names and installer, plus the exact versions observed for every reachable instance of these three package names. Portable/browser consumers need only Effect; native must contain all three.

No timestamp, registry lookup or newest-version query participates in selection. Preserve per-run installation logs as existing diagnostics.

Implement the small I/O shell in `scripts/pack.ts` and pure validation in `scripts/pack-effect-stack.ts`:

1. Read root manifest and `bun.lock` bytes. Parse the lock with **`Bun.JSONC.parse`**, whose pinned Bun 1.4.2 types support comments/trailing commas and return unknown; do not use `JSON.parse` on the trailing-comma lock or regex-rewrite the file. Validate the narrow supported lock shape, version and required package tuples. Fail with a specific diagnostic on an unsupported lock format; do not attempt a general package-manager resolver.
2. Extract the exact coordinate suffix from each named package tuple, e.g. known prefix `effect@` followed by its exact version. This parses a resolved lock coordinate, not a version range. Validate the package name and a concrete version. Reject a missing tuple, range-like coordinate, malformed version, or an incompatible second relevant lock entry rather than choosing whichever entry is encountered first.
3. Use `createRequire` based at client/browser/native package manifests. Resolve their declared Effect packages; resolve platform-node from client and native; resolve shared-platform from the resolved platform-node package; resolve Effect again from platform-node and shared-platform owners. Read manifests through resolved package paths, validate name/version as unknown, canonicalize realpaths for internal comparison, and compare each reachable edge to the selected lock coordinate. No search of arbitrary global `node_modules`, no hard-coded Bun store directory and no undeclared root import.
4. Validate that each exact version satisfies its originating range using Bun's semver API, not prefix stripping. Pinned types expose `Bun.semver.satisfies` and `order`; `order(version, version)` can reject malformed concrete versions before range checks. Preserve the repository's current equality invariant across Effect/node/shared, which the existing check already asserts (`pack.ts:803–817`). An intentionally different version scheme requires a separately reviewed invariant change, not relaxing the check here.
5. Verify lock catalog/override requirements agree with the current root manifest before installation. A stale install, missing metadata or mismatch fails early with the frozen-install remedy; do not modify the lockfile automatically. Fresh implementation checkouts use `bun install --frozen-lockfile` as already required.
6. Feed exact selected Effect to every consumer installation, exact selected platform-node to native installation, and exact selected shared-platform to the native consumer root override. Keep archive peer validation comparing against the **range** (`pack.ts:281–282`) and leave public package manifests untouched. Naming separate variables `requirements` and `selected` makes accidental reuse visible.

The pure functions accept lock/manifest/tree records as unknown and return typed records or precise failures. Do not import Effect into root tooling; the root does not declare it. No new dependency, generic resolver framework, `any`, unsafe global module fallback or lockfile refresh is needed.

## Validate actual consumer resolutions

After each consumer installs, resolve its Effect package and check exact name/version. In native, retain `native-dependencies.json` and recursively validate every Effect/platform-node/shared node against `selected`; validate unknown npm-tree shape before trusting it. Report a missing required node, malformed node or mismatched nested instance. Retain current outside-consumer resolution and byte-identity checks.

Do not silently weaken existing duplicate-version rejection because top-level versions look correct. Identical versions at more than one path should be reported as occurrences, not automatically labeled incompatible; a new blanket physical-singleton rule is outside this fix. Mismatched versions anywhere in the reachable consumer tree fail.

Preserve the existing `npm ls` **read-only diagnostic** on a Bun-installed native consumer (`pack.ts:789–817`); it is not a package installation command. Its nonzero exit and invalid dependency diagnostics remain failures rather than being ignored. If Bun's layout causes a documented diagnostic incompatibility in a supported fixture, replace that diagnostic with an equivalently complete resolver walk in a separately identified change; never fall back automatically between installers.

## Durable evidence and release compatibility

Keep `package-identity.json.effect` exactly the catalog/public range. `release-tools/model.mjs:10` and `:94–104` require that literal. Changing it to the selected exact version breaks release preparation even if tarballs pass.

Add the separate metadata property **`qualificationStack`** to `package-identity.json`, containing the record above. This is the proposed small revision to 005's phrase “separate dependency evidence”: it is a separate field, not a change to `.effect`. A sidecar written only beside `native-dependencies.json` would not be durable release evidence: CI copies only tarballs and `package-identity.json` into its uploaded artifact (`.github/workflows/ci.yml:338–347`).

The current release decoder uses `Schema.decodeUnknownSync(PackageIdentity)` without excess-property rejection (`release-tools/model.mjs:162–163`); preparation retains the original raw identity bytes (`prepare.mjs:55–56`, `:104`). Therefore additive metadata can survive qualified artifact and candidate retention while the current release policy continues validating its original fields. Do not claim the old release schema independently validates the new record: pack owns validation for this patch. If release execution later needs to make decisions from decoded qualificationStack, explicitly extend its schema and tests then.

Add regression coverage to `release-tools/test/Candidate.test.mjs`: a qualified identity carrying a valid extra qualificationStack is accepted, the original range-valued `.effect` is still required, and the retained candidate's `package-identity.json` bytes contain the unchanged metadata. Existing fixtures without the additive field must still prepare. This needs a small explicit scope addition for that test file; no workflow or production release-tool change is necessary if these tests confirm current behavior.

Include fixture proof that the lock hash and selected/installed records are written only after successful consumer checks. A failed partial pack run must not emit a complete qualificationStack success record. The identity already emits at the end of pack (`pack.ts:843–866`); preserve that position.

## Regression matrix and test seams

Use real JSONC text fixtures in `scripts/pack-effect-stack.test.ts` plus tiny disposable resolver fixture directories when testing the filesystem seam. No registry access in these tests.

| Case | Expected result |
| --- | --- |
| Baseline lock, metadata and consumer trees all rc.117 | Pass; range requirements and exact selections remain separate. |
| Same caret requirement, lock and installed metadata advanced to a hypothetical later aligned RC | Pass and select that exact later RC; this is the regression against minimum stripping. |
| Later registry availability but unchanged frozen lock/metadata | Select rc.117; no registry lookup occurs. |
| Lock tuple missing, unsupported format, malformed version or mismatched catalog | Fail before consumer installation. |
| Installed version differs from lock, or client/browser/native resolve different versions | Fail before consumer installation. |
| Platform owner resolves a different nested Effect or shared-platform | Fail, even if root workspace Effect matches. |
| Consumer root matches but a nested dependency differs | Fail; recursively inspect all relevant occurrences. |
| Consumer missing Effect/node/shared required for its kind | Fail with package and consumer identity. |
| Peer archive range accidentally replaced with exact selected version | Archive validation fails; preserve public semantics. |
| Root Effect cannot resolve but workspace owners resolve correctly | Pass; root availability is not a prerequisite. |
| New qualificationStack beside old range-valued identity | Existing release preparation accepts and retains exact raw bytes. |

Do not make tests merely duplicate the helper's output. The later-RC fixture must be shown to fail the original minimum-derived equality; resolver fixtures must actually exercise owner-relative resolution and nested mismatches, not provide pre-normalized answers only.

## Installer matrix and commands

Development setup and all task-driven installs use Bun, respecting the user instruction. Leave the repository's existing default npm package-consumer CI lane unchanged; it is an established compatibility gate, not a reason to start using npm for workspace development.

1. `bun --no-env-file test scripts/pack-effect-stack.test.ts` for deterministic fixtures.
2. Root `bun run verify --profile portable`, including build before lint/typecheck; no unnecessary duplicate full reruns.
3. `PACK_INSTALLER=bun bun --no-env-file scripts/pack.ts --portable` for Bun-installed portable/browser consumers.
4. On a qualified native staging host, `PACK_INSTALLER=bun bun run test:pack` for the changed native override/tree path. Portable-only success cannot establish this fix.
5. Existing CI full `bun run verify --profile package` retains its default npm consumer installation and both staged native platforms. Require this unchanged CI lane to pass on the same final revision. Do not substitute one installer result for the other or add fallback behavior.
6. Install isolated release tooling with `bun install --cwd release-tools --frozen-lockfile --ignore-scripts` when needed, then `bun run check:release`, including the raw-identity-retention regression.

Installer selection remains recorded in each identity. Compare the selected locked stack between lanes; both must qualify the same archive identities and exact Effect selection. Their dependency layout need not have identical filesystem paths. No new paid or provider test is required.

## Scope and done contract

Production edits remain `scripts/pack.ts` plus the concrete adjacent helper if used. Tests: `scripts/pack-effect-stack.test.ts`, and the explicitly added `release-tools/test/Candidate.test.mjs`. Documentation: `scripts/README.md` and plan records. No package manifest, lockfile, workflow, native artifact or public peer-range changes.

Done means exact lock selection matches installed workspace and isolated consumer resolutions, later-aligned fixtures pass without accepting mixed stacks, archive peer semantics stay unchanged, qualificationStack reaches the retained release identity bytes, and Bun qualification plus existing npm CI and release-tool tests pass. The JSONC parser is intentionally narrow: a future unsupported lock format fails loudly for a reviewed update rather than guessing a version.
