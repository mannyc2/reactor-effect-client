# Plan 005: Qualify an explicitly selected Effect dependency stack

Planned at `c47e50c784f6889b542cf8ee316067b688ca4737`, 2026-09-26. P2; S–M; low fix risk; high confidence. Independent of scheduler changes; run alongside 001 and finish before using the next release's package qualification as evidence. The [exact-stack contract](005-effect-stack-contract.md) specifies selection, owner-relative resolution, durable evidence and regression fixtures.

## Parallel execution

Stream D starts immediately alongside 001, 003-E and 004-R; see the [parallel execution guide](PARALLEL-EXECUTION.md). Tooling and release-identity tests do not depend on runtime implementation. This stream owns `pack.ts`, not the public API files under `scripts/pack/`; those assertions are integrated through one owner. Final package evidence must be rerun on combined bytes, but that is no reason to postpone the resolver fix or block the harness. Use an isolated worktree for concurrent builds/pack.

## Problem and current state

The workspace intentionally accepts Effect caret ranges. `scripts/pack.ts:133–140` reads those ranges from the catalog/override, and the clean installer at lines 457–482 passes `effect@${effectVersion}`. Native platform packages are similarly ranged at lines 778–784. Validation then treats the range minimum as the expected installed version:

```ts
// scripts/pack.ts:802
const resolvedEffectVersion = nodeSharedVersion.replace(/^[\^~>=<]+/, "");
```

Lines 803–817 require every installed Effect/platform package to equal that string. A compatible later RC selected by a fresh install therefore fails this check. Exact-main CI currently passes; this is a latent qualification defect, not evidence of a current outage. `scripts/README.md:41` also still describes an rc.115 override while main uses `^4.0.0-rc.117`.

**Recommendation:** make release qualification reproducible using the exact stack resolved by the frozen workspace install. Preserve public peer ranges. Testing the newest compatible stack is a separate compatibility lane, not an incidental side effect of a release check. Never derive an exact version by stripping a range prefix. Select from `bun.lock` and verify owner-relative installed resolutions match it; this detects a stale install rather than silently qualifying it.

## Scope and conventions

Allowed files: `scripts/pack.ts`, a small adjacent concrete helper `scripts/pack-effect-stack.ts` and `scripts/pack-effect-stack.test.ts` if isolation is needed, `release-tools/test/Candidate.test.mjs`, `scripts/README.md`, and the plan/index. `scripts/tsconfig.json` already includes root `*.ts`. Existing root Bun discovery includes scripts tests. No public package manifests/ranges, lockfile refresh, release workflow, native binaries, or dependency upgrade is required.

Use Bun, no `any`, strict parsing from unknown, and comments explaining why selection is separate from compatibility. Read `CONTRIBUTING.md` and `scripts/README.md`. The requested `docs/Codex` convention files are absent at this SHA; read them if restored. Keep the helper specific to package qualification; don't create a dependency-resolution framework. `scripts/architecture.test.mjs` illustrates runner conventions, while the current typed `DependencyTree` near `pack.ts:795` is the input shape to validate rather than trust blindly.

## Steps

1. Branch `codex/pin-qualification-stack` from main. Run `git diff c47e50c..HEAD -- scripts/pack.ts scripts/README.md package.json bun.lock` and reconcile drift. If dependencies are absent, run `bun install --frozen-lockfile` (exit 0), then inspect the actual installed Effect and Node platform resolutions in the workspaces that declare them. Do not assume the isolated linker exposes them at the root.

2. Implement the concrete `QualificationStack` from the appendix: lock hash/version, requirements, exact selected versions, checked workspace edges and observed consumer versions. Parse the pinned JSONC lock with `Bun.JSONC.parse`, validate its narrow supported tuple shape, and use owner-relative `createRequire` from client/browser/native manifests. Resolve Effect through platform owners as well as workspaces; do not rely on undeclared root dependencies or hardcoded Bun store paths. Validate exact versions against requirements with Bun's semver API, confirm lock/manifest agreement and the existing Effect/node/shared equality invariant, then fail early on stale/missing/mixed resolutions. No registry selection or automatic lock refresh.

   Feed exact selected versions to portable/browser/native consumer installs and the native shared-platform override. Keep archive/public peer validation on its original ranges. Recursively validate all relevant instances in installed native trees, not just top-level metadata. **Verify:** `rg -n 'resolvedEffectVersion|effectVersion|nodePlatformVersion|nodeSharedVersion' scripts/pack.ts` → every install/check uses its intended selected version versus public range, with no range-prefix stripping used as resolution.

   Preserve `package-identity.json.effect` as the catalog range: `release-tools/model.mjs:97` requires that literal. Add validated **`qualificationStack`** metadata to that identity only after all consumers pass. A local dependency sidecar alone would be lost: current CI retains tarballs and identity, not arbitrary pack output. Existing release preparation accepts extra identity metadata and retains raw bytes; add a Candidate regression proving that compatibility and byte preservation. The old release schema does not independently validate the added field; pack does. No workflow or release-policy change is required if the regression confirms the inspected behavior.

3. Add meaningful fixture tests: the current aligned RC; a hypothetical later aligned RC selected by a changed frozen installation; mixed Effect versions; a missing/malformed package version; nested conflicting versions; and a selected stack differing from the consumer's installed stack. The later aligned fixture proves the resolver is not hardcoded to the range minimum; a release consumer still must equal its selected stack. Include actual owner-relative resolver fixtures, stale installed metadata versus lock, unsupported JSONC format, and a release candidate that retains the added metadata while still requiring range-valued `.effect`. Keep tests independent of the public registry. **Verify root:** `bun --no-env-file test scripts/pack-effect-stack.test.ts` → all fixtures pass. The regression must fail on the old minimum-derived check. If no helper is needed, record the equivalent fixture test filename in this scope before proceeding.

4. Update the stale rc.115 documentation to describe exact release qualification versus peer compatibility. **Verify root:** `bun run build`, `bun run typecheck`, `bun run lint` → exit 0; `bun run verify --profile portable` can serve as the final combined gate. Run `PACK_INSTALLER=bun bun --no-env-file scripts/pack.ts --portable` → isolated portable/browser consumers pass without workspace fallback. Full `PACK_INSTALLER=bun bun run test:pack` on a qualified native staging host or CI is required to prove the native branch; a portable-only pass does not cover the failing native check. Install isolated release-tool dependencies with `bun install --cwd release-tools --frozen-lockfile --ignore-scripts` if needed, then `bun run check:release` → exit 0, preserving release identity compatibility.

## Done criteria and maintenance

- Release consumers install the selected exact locked stack, while published peer ranges retain their existing semantics.
- Aligned newer fixture passes when selected; unexpected or conflicting installed versions fail.
- Evidence records the exact selected and installed stack in the retained identity bytes; the native pack gate passes on the final revision with Bun installation and the existing default npm compatibility CI lane. All task-driven installs use Bun; do not change or silently substitute the established CI lane.
- Root build/typecheck/lint and scoped fixture tests pass; `git diff --name-only` contains only scoped files.
- Index includes verification and implementing commit. Use conventional commits without AI attribution; do not push or publish without instruction.

Stop if resolution requires modifying dependency constraints, an undeclared root dependency, a lockfile refresh, or relaxing duplicate/version guards just to get a pass. If the actual platform packages legitimately use different version numbers, revisit the alignment contract explicitly instead of preserving a false equality check. Keep future floating compatibility tests separate from release-byte qualification.
