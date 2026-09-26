# 005 package-tooling handoff

The packet-stage evidence below is retained. Final combined portable, native, Chrome/native, release and full package qualification with Bun now pass; see the [final integration report](final-integration-handoff.md). Remote npm/OS CI, Linux execution and paid hosted qualification remain unrun.

Implementation: `3cb73b39b62570602fb03f14d474a9e99247c593`, branch `codex/pin-qualification-stack`, worktree `/Users/chriscarroll/.codex/worktrees/0d44/reactor-effect-client`. Input and freshly fetched remote main: `c47e50c784f6889b542cf8ee316067b688ca4737`; no drift. Shared original plans were read, not changed.

## Contract and scope

Exact Effect, Node platform and shared-platform versions come from validated frozen lock coordinates. Installed workspace owners, platform owners and all relevant isolated consumer occurrences must agree. Unsupported JSONC formats, nonconcrete versions, stale/mixed metadata and incomplete consumer evidence fail. Public peer requirements and identity `.effect` remain range-valued. The additive `qualificationStack` retains the raw lock hash, requirements, exact versions, eleven workspace edges and completed consumer observations; release preparation preserves the original identity bytes without independently decoding the new field.

Changed files:

- `scripts/pack.ts`
- `scripts/pack-effect-stack.ts`
- `scripts/pack-effect-stack.test.ts`
- `scripts/README.md`
- `release-tools/test/Candidate.test.mjs`

No manifests, locks, public API consumers, native source/artifacts, workflows or production release tools were changed. The helper's small owner-resolution and installed-byte I/O seams are shared by production and real filesystem fixtures; validation is independent of Effect and registry access.

Bun 1.4.2 leaves local-tarball dependencies invalid to npm 10.9.2 even with `file:` specs. A registry-disabled fixture reproduces this. Only after every installed archive file passes its digest check, the temporary Bun consumer's root archive requirements become the exact verified package versions. There is no reinstall or registry resolution, installed manifests remain untouched, and npm's strict diagnostic stays in use. Original/normalized manifests and an explicit normalization record remain beside the pack logs; npm validates the normalized dependency graph, not the original tarball spec. Wrong requirements, altered bytes, missing/invalid peers and nested stack conflicts remain failures. The npm installer path is unchanged.

## Checks actually run

Declared Bun 1.4.2 was already available at `/Users/chriscarroll/.local/share/vite-plus/package_manager/bun/1.4.2/bun/bin/bun`; commands used that directory on PATH and explicit `BUN_BINARY`. Node was 22.13.1 and npm diagnostic 10.9.2. Frozen workspace/release installs passed without lock changes. The compiler reported `7.0.2+effect-tsgo.0.45.0`.

- `bun --no-env-file test scripts/pack-effect-stack.test.ts`: 33 passed on final tooling; includes later aligned selection, raw-byte hashing, owner-relative nested/stale fixtures, consumer trees, partial evidence and the archive-metadata regression.
- `VITEST_MAX_WORKERS=1 bun run verify --profile portable`: passed build, lint, typecheck, architecture, generation/format checks, examples, client 757 tests on Node and 757 on Bun, browser 58 on each runtime, and root 131 tests including offline hosted rehearsals. The one-worker setting was confirmed from the actual child process. This gate preceded the narrow archive-metadata correction; final changed tooling then passed scoped tests/tsc/oxlint, root typecheck/lint/format, and both pack profiles. No runtime source changed in 005.
- `bun run check:release`: 71 passed, including additive metadata/raw-byte retention and all earlier timed-out cases with their original timeout budgets.
- `PACK_INSTALLER=bun bun --no-env-file scripts/pack.ts --portable`: passed, evidence `.check/pack-7jSJZq`.
- `PACK_INSTALLER=bun PACK_EXPECT_NATIVE_PLATFORMS=darwin-arm64,linux-x64 bun run test:pack`: passed, evidence `.check/pack-Nw1rG5`. Includes exact archive bytes, strict normalized dependency diagnostics, declaration/example compilation and macOS native preflight. Both native platforms are archived; Linux execution was not run locally.
- The full identity retains `.effect = ^4.0.0-rc.117`, exact selections `4.0.0-rc.117`, eleven workspace edges and portable/browser/native occurrence counts 2/3/8. Lock SHA-256 is `69e84a1e6327962dd318a5067a0280b9f1ec2e30f2bdbcc0853ea86674096819`.
- An earlier failed partial pack at `.check/pack-wjmdDf` emitted no success identity. Its invalid original archive metadata is preserved. Initial Bun 1.4.0 verification was partial; three release tests timed out during concurrent verification. Those failed results are not counted as passes; the serial required-runtime release retry passed.

Logs remain at `/tmp/reactor-d-005-portable.log`, `/tmp/reactor-d-005-portable-142.log`, `/tmp/reactor-d-005-release-142.log`, `/tmp/reactor-d-005-static.log`, `/tmp/reactor-d-005-pack-portable.log`, `/tmp/reactor-d-005-pack-portable-normalized.log`, and `/tmp/reactor-d-005-pack-full.log`.

## Reused native inputs and limits

The unmodified native source matches artifacts from successful baseline [CI run 36223885202](https://github.com/mannyc2/reactor-effect-client/actions/runs/36223885202). D downloaded both platforms into `.check/native-inputs`, checked library/sidecar hashes, ran the current `stage.mjs` for both, and confirmed generated sidecars equal the downloaded identities. Original downloaded inputs are read-only for peers to copy into their own staging directories. Retrieval metadata and stage receipts are retained there.

- Native source: `6b4ad6bf5c710345e46b3b375648b7981679a8570e2bf0b30cd073fccd984125`.
- Darwin library: `54312e0a85ea827d7a6af239034a79fd010f686cd2a053a4cd61cbc0aff350b6`.
- Linux library: `506f426b755f2baf52036555cbd07532a902bbf4806c9d89695cc5407b98288e`.

This is local tooling qualification, not final runtime integration or a new Linux/native-suite result. Existing default npm compatibility CI on the final combined revision remains pending; no push or CI dispatch is authorized. Final package evidence must be regenerated after A/B/C changes and shared documentation/API assertions are integrated. No paid provider sessions were run.

Next owner: D integrates shared public assertions/docs serially against each actual runtime packet and owns the final local join. A/B/C may continue their independent packets; 005 introduces no runtime API dependency.
