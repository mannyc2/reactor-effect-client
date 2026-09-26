# Final local integration handoff

All five implementation packets are integrated and the final combined local gates pass. Remote CI and paid hosted qualification remain separate, unrun gates.

- Branch: `codex/integrate-orchestration-packets`.
- Worktree: `/Users/chriscarroll/.codex/worktrees/0d44/reactor-effect-client`.
- Tested source commit: `a68d072640a700b1763e3e3ed8910a18031f848a`.
- Tested source tree: `dcfed85061da895e6ad6324863a42d99e67480a7`.
- Audited starting main: `c47e50c784f6889b542cf8ee316067b688ca4737`; the initial fresh fetch matched it.
- The final handoff commit changes only `plans/` documentation. It does not change the tested source or packed bytes. The original planning checkout and its specifications were not edited.

## Integrated packets

| Packet                   | Owner inputs                                 | Local integration and contract                                                                                                                                                                                                                                                                                                 |
| ------------------------ | -------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| 001 terminal settlement  | A `6bf95b2`, `9e02cfd`                       | `5670e9d`, `87e690d`; one terminal owner settles retained item/control waits and fences admission before publishing failure. Original abnormal Causes and prior decisive evidence survive competing closure.                                                                                                                   |
| 002 unknown recovery     | A `efea67a`, `7d3d129`                       | `fb88362`, `98db268`; optional `unknownRecoveryTimeout` defaults to 60 seconds and is positive, finite and at most 10 minutes. The original unknown age survives blocked control work; a truthful completed drain cancels its watchdog. Uncertain filler is bounded at 4096 identities, with refusal before the next dispatch. |
| 003-E switch evidence    | B `c34e95b`                                  | `8a69535`; optional public `Switched.handoff` keeps historical values assignable while preserving immutable eligibility facts and retiring/replacement identities.                                                                                                                                                             |
| 003-H/C qualification    | B `f5d9162`, `6dc2c74`, `fc1d742`, `bc3fd98` | `877c7b2`, `07d0e4e`, `1e11b37`, `91720a6`; real public legacy/continuous owners, source-attributed frames on one monotonic origin, accepted drain, canonical per-source cleanup and continuous-summary reconciliation. Paid selection is continuous, but no paid run occurred.                                                |
| 004 continuous renewal   | C `763052c`, `5cb2be8`, `9afac85`            | `23d03f8`, `4443f80`, `ed88be7`; opt-in `makeContinuous`, bounded physical reservations and history, private incarnation fencing and separate `CleanupSummary` with exact bigint counts. Unknown outcomes and contradictory evidence remain incomplete. Legacy construction/report shapes stay unchanged.                      |
| 005 frozen qualification | D `3cb73b3`                                  | Exact Effect/platform selection from frozen lock bytes, declaring-owner resolution, strict consumer dependency validation and additive raw identity metadata. Public peer ranges remain unchanged. See [005 details](005-package-tooling-handoff.md).                                                                          |
| Combined corrections     | A `cdb7a77`, `a790eb6`; B `4934b6e`          | `9dc0baf`, `a68d072`, `65ff491`; explicitly assert the new close-Cause contract, remove redundant scheduler source projection work and join every twin-owned socket on shutdown.                                                                                                                                               |

D authored the shared public-type and installed-consumer assertions against the actual packet APIs: `31fdb9c` (switch compatibility), `ce2e00a` (unknown deadline), and C-only `96551d9` integrated as `9841bda` (continuous constructor/summary). Both Node and browser consumers compile the continuous constructor, close result, engine/sequence surfaces and real JSON codec; the Node consumer also compiles the scheduler option and historical switch value. The final full pack compiles these assertions in isolated installations.

The separately authorized runner commit `56f6ebd` adds an optional aggregate timeout per subprocess. Only root Bun discovery receives 600 seconds; every Vitest subprocess remains at 180 seconds and individual test/CLI limits are unchanged. Both-constructor rehearsal estimates were 468.26 seconds including the existing root baseline. The final successful root run measured 465.77 seconds. No filename registry, skip or automatic fallback was added. The wrapper has no tighter limit and CI defines no shorter job timeout.

## Final verification

All gates below ran on `a68d072` with declared Bun 1.4.2, Node 22.13.1, patched compiler `7.0.2+effect-tsgo.0.45.0`, Python 3.13.5, one Vitest worker and ffmpeg required for examples. Native checks used pinned Rust 1.90.0, two Cargo build jobs and the supported `C` locale. No dependency manifest or lockfile changed.

| Gate                                                                                       | Result                                                                                                                                                                                                                                                                | Retained log                                         |
| ------------------------------------------------------------------------------------------ | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------- |
| `bun run verify --profile portable`                                                        | PASS; generation, format, build, runtime import guards, lint, all workspace typechecks, architecture, examples; 850 client and 58 browser tests on each Node/Bun runtime; 172 root tests, including all 26 renewal rehearsals. Root 465.77 s; whole profile 604.23 s. | `.check/final-integration/portable-passed.log`       |
| `bun run native:test`                                                                      | PASS; Rust format, 78 crate tests plus 7 far-peer tests, clippy and rustdoc; release far peer built; native Node 37 passed/1 platform skip, Bun 29 passed/9 platform skips. Existing isolated-host platform selection explains the skips; none were added.            | `.check/final-integration/native-passed.log`         |
| `bun run test:integration`                                                                 | PASS; real local Chrome/native public owners, ordered binary channels, decoded video/audio, attribution, media leases, failure cleanup and joined close. Direct connection; TURN was not requested.                                                                   | `.check/final-integration/browser-native-passed.log` |
| `bun run check:release`                                                                    | PASS; patched types, lint and all 71 release tests.                                                                                                                                                                                                                   | `.check/final-integration/release-passed.log`        |
| `PACK_INSTALLER=bun PACK_EXPECT_NATIVE_PLATFORMS=darwin-arm64,linux-x64 bun run test:pack` | PASS; all three archives, byte identity, export/declaration closure, installed public consumers/examples, browser bundle, strict dependency trees and Darwin native preflight.                                                                                        | `.check/final-integration/pack-passed.log`           |

The machine-readable local record is `.check/final-integration/receipt.json`. It records exact commands, source identity, test counts, versions, archive metadata and explicit unrun gates. It is local evidence, not a CI release stamp.

## Final archives and native provenance

Full package evidence: `.check/pack-pcZuov/package-identity.json`; its SHA-256 is `10f3adedf915f8876257132a155b9d447cdacf3862c55653b59b0a80f3cd8040`.

| Archive under `.check/pack-pcZuov/` | SHA-256                                                            |
| ----------------------------------- | ------------------------------------------------------------------ |
| `reactor-effect-client-0.5.0.tgz`   | `4bacfdf112fa88735d1a9340af9ccf1ce43ca9d9f3dbeca2cc660b708d64c4c5` |
| `reactor-effect-browser-0.5.0.tgz`  | `252fbf4d0c98c1e9bde2d359812c88bcba632f3ae0f2d0bf81c219ffcba00ce8` |
| `reactor-effect-native-0.5.0.tgz`   | `c368603de20b1f5c1c978e7098d98d1132c29be11b28431728b02eb84129da39` |

The full identity preserves `.effect = ^4.0.0-rc.117` while recording exact `4.0.0-rc.117` for Effect, Node platform and shared platform, eleven workspace resolution edges and 2/3/8 observed occurrences in the portable/browser/native consumers. Frozen lock SHA-256: `69e84a1e6327962dd318a5067a0280b9f1ec2e30f2bdbcc0853ea86674096819`. Original and normalized Bun consumer manifests, normalization receipts, archive hashes and install/resolution logs remain beside the identity. The successful consumer directories were removed by the existing runner.

The SDK native libraries were reused from successful baseline [CI run 36223885202](https://github.com/mannyc2/reactor-effect-client/actions/runs/36223885202), after canonical source, sidecar, embedded identity and library-hash checks. The current stage tool reproduced both original sidecars. Download receipts and immutable inputs are in `.check/native-inputs`; staged identities are `packages/native/lib/{darwin-arm64,linux-x64}/native-identity.json`. Local Rust checks and the far peer were built afresh; the staged SDK libraries were not replaced.

- Native source: `6b4ad6bf5c710345e46b3b375648b7981679a8570e2bf0b30cd073fccd984125`.
- Darwin library: `54312e0a85ea827d7a6af239034a79fd010f686cd2a053a4cd61cbc0aff350b6`.
- Linux library: `506f426b755f2baf52036555cbd07532a902bbf4806c9d89695cc5407b98288e`.

## Failures investigated before the successful gate

Earlier failures are preserved, not relabeled as passes:

1. B's first final matrix passed 25/26 cases. Continuous cleanup reservation correctly stopped a second-refusal path after two attempts; the copied legacy assertion expected three. `bc3fd98` pins the exact continuous exhausted/unknown-allocation summary while retaining the legacy expectation. The focused rerun passed and the final combined matrix passed all 26.
2. The first combined gate exposed the stronger C close-Cause contract and a slow 4096-entry stress case. `cdb7a77` explicitly owns and asserts both original close Exits. Its fixture-only performance improvement was insufficient: the third combined run still timed out. `a790eb6` removes repeated membership/cache/projection passes without using source-array identity as evidence. An in-place mutation/retirement/reappearance regression protects behavior. Identical profiles improved from 3462 to 1462 ms; full owner suites measured 2095 ms on Node and 1200 ms on Bun, with the same 4096 dispatches, 4097 live sources and five-second test limit. The final combined 850-test runtime suites pass.
3. The second combined gate passed all 26 renewal cases but an existing image-upload test timed out. Repetition reproduced it, and phase diagnostics proved upload and SDK cleanup had completed in about 256 ms. Twin HTTP close still owned one connection. A physical `100 Continue` request with an unfinished body reproduced the original five-second shutdown failure. `4934b6e` tracks/destroys existing and late twin-owned sockets and still joins the real server-close callback. The fix passed 200 repeated checks, the full twin file and the final combined run. No timer declared successful cleanup.

Logs for all three combined failures and the twin diagnosis/regressions are in `.check/final-integration/`. A's profiling and focused evidence remain in `/Users/chriscarroll/.codex/worktrees/1915/reactor-effect-client/.check/scheduler-stream-a/`; B's complete packet handoff is in `/Users/chriscarroll/.codex/worktrees/b8d3/reactor-effect-client/.check/qualification-stream-b/`; C's is in `/Users/chriscarroll/.codex/worktrees/437f/reactor-effect-client/.check/renewal-stream-c/`. Earlier workstream failures and 005's initial tarball-metadata failure are retained in their packet records.

## Remaining gates and next owner

Local implementation, shared assertions and combined local qualification are complete. The next owner is the maintainer/parent task for review and separately authorized remote work:

- Final npm-installer CI, the OS/Node matrix and Linux native execution on the combined revision remain unrun. Linux bytes are archived and validated, but local Darwin execution is not Linux qualification.
- No push, remote merge, CI dispatch, publication or paid provider session occurred. Paid `scheduler-renewal` remains unrun; any future request must first satisfy the missing CI gates and its explicit spend authorization on exact qualified bytes.
- Hosted timing, audio completeness, presented/encoded output and hosted TURN remain unqualified. Local decoded-frame evidence is limited accordingly.
- A characterized the existing renewal recovery-fiber supervision boundary: close retains its original defect while `engine.failure` may remain pending. Direct Engine/media all-cause supervision is a separately scoped follow-up; the scheduler's bounded service Timeout is a distinct failure.
- Continuous adapters must honor lifetime source/clip identity uniqueness. Historical reuse after bounded eviction cannot be fully detected. Stalled close/finalizers retain ownership; no global cleanup deadline is promised.

## Exact tested revision file inventory

The tested revision changes these 45 paths from the audited baseline. The final handoff commit additionally adds this report and updates the plan index/packet-status prose only.

- `CHANGELOG.md`
- `README.md`
- `integration/hosted/README.md`
- `integration/hosted/collect.ts`
- `integration/hosted/evidence.ts`
- `integration/hosted/gates.ts`
- `integration/hosted/qualify.ts`
- `integration/hosted/report.ts`
- `integration/hosted/twin/h3.ts`
- `integration/hosted/twin/peer.ts`
- `integration/hosted/twin/protocol.ts`
- `integration/hosted/twin/server.ts`
- `integration/test/hosted-gates.test.ts`
- `integration/test/hosted-rehearsal.test.ts`
- `integration/test/hosted-renewal.test.ts`
- `integration/test/hosted-twin.test.ts`
- `packages/client/README.md`
- `packages/client/src/orchestration/index.ts`
- `packages/client/src/orchestration/renewal-state.ts`
- `packages/client/src/orchestration/renewal.ts`
- `packages/client/src/orchestration/retention.ts`
- `packages/client/src/orchestration/scheduler-policy.ts`
- `packages/client/src/orchestration/scheduler.ts`
- `packages/client/src/orchestration/source-slot.ts`
- `packages/client/src/orchestration/types.ts`
- `packages/client/test/Evidence.test.ts`
- `packages/client/test/PublicTypes.test.ts`
- `packages/client/test/orchestration/Media.test.ts`
- `packages/client/test/orchestration/README.md`
- `packages/client/test/orchestration/RenewalRetention.test.ts`
- `packages/client/test/orchestration/RenewalState.test.ts`
- `packages/client/test/orchestration/Retention.test.ts`
- `packages/client/test/orchestration/SchedulerFates.test.ts`
- `packages/client/test/orchestration/SchedulerUnknownRecovery.test.ts`
- `plans/005-package-tooling-handoff.md`
- `plans/README.md`
- `plans/shared-integration-handoffs.md`
- `release-tools/test/Candidate.test.mjs`
- `scripts/README.md`
- `scripts/pack-effect-stack.test.ts`
- `scripts/pack-effect-stack.ts`
- `scripts/pack.ts`
- `scripts/pack/browser-consumer.mts`
- `scripts/pack/node-consumer.mts`
- `scripts/test.ts`
