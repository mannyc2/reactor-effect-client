# Review, direct fixes, and merge readiness

Independently review the completed reactor-effect-client work below, fix substantive findings directly, prepare reviewable PRs, and tell me exactly what is available to merge. Carry the work through verification and independent review. **Do not merge, enable auto-merge, enqueue a merge, close issues, publish, deploy, or start paid provider sessions. Stop with a concrete merge-readiness report so I can decide what to merge.**

Use judgment for routine fixes and reversible implementation decisions. Preserve unrelated work. Keep coordination quiet: parallel reviewers should return consolidated findings, not continual status messages. Escalate only a concrete decision or blocker requiring my input.

## Starting artifacts and identity

Repository: https://github.com/mannyc2/reactor-effect-client.

- Integrated branch: `codex/integrate-orchestration-packets`.
- Fetchable implementation ref: `origin/codex/integrate-orchestration-packets`.
- Implementation head: `dec33ea21cd8c00311d3cb100cbd5a7184420c05`; tree `b7f46a46aa73f38809fbe3f8d0c582a2e8f088da`.
- Source commit used for the final combined local checks: `a68d072640a700b1763e3e3ed8910a18031f848a`; tree `dcfed85061da895e6ad6324863a42d99e67480a7`.
- The difference from tested source to final head is four documentation files under `plans/`; verify this yourself. Package hashes were reported unchanged.
- Original baseline: `c47e50c784f6889b542cf8ee316067b688ca4737`. A remote read on September 26, 2026 still returned that main SHA, and GitHub returned no open PRs at prompt preparation. The implementation/provenance refs and this handoff have since been pushed for cross-machine review; that push did not run qualification or publish packages. All three referenced issues were open. Fetch again; these are observations, not assumptions about the state when you begin.

Start from a fresh checkout of the implementation branch. The separate `codex/orchestration-review-handoff` branch contains archived review material; do not merge that archive branch into the code PR. No access to the originating computer or its Codex tasks is required. Use the bootstrap in [README.md](README.md) to extract this directory under `.check/review-handoff` while keeping the implementation checkout at its original head. All links below resolve inside this handoff, and the original implementation handoffs are also tracked under `plans/` on the implementation branch.

The archive preserves historical text and raw receipts byte-for-byte, so they contain obsolete absolute paths. Use [ARTIFACTS.md](ARTIFACTS.md) and [manifest.json](manifest.json) to map those paths to archived files. Treat task IDs as provenance only. `source-refs.json` records the original owner branches, which are remotely fetchable. Generated package archives are reconstructed from source; native inputs are downloadable from the recorded CI artifact. Read the explicit binary availability limits before relying on an old archive hash.

Read these artifacts before acting:

- [Combined integration handoff](workstreams/D/final-integration-handoff.md), [package-tooling handoff](workstreams/D/005-package-tooling-handoff.md), and [shared integration record](workstreams/D/shared-integration-handoffs.md).
- [Machine-readable combined receipt](evidence/final-integration/receipt.json) and its adjacent passed and failed logs. These are local evidence, not a remote CI stamp.
- [Original plan index](original-plans/README.md), [parallel execution plan](original-plans/PARALLEL-EXECUTION.md), and [workstream registry](original-plans/WORKSTREAM-TASKS.md). Read all five numbered plans and their decision/contract appendices in that directory. Their original TODO tables are historical; the final implementation and handoffs determine current status.
- [A scheduler handoff](workstreams/A/handoff.md), including both combined-revision successors. Task `01a0df7a-9791-73b2-ba8a-f28679c77d15` on host `local`; final optimization input `a790eb613a7a8c77079abd83e41b165d5cb9777f`, branch `codex/optimize-scheduler-uncertainty`.
- [B qualification handoff](workstreams/B/handoff.md), [hosted-readiness candidate](workstreams/B/hosted-readiness.md), and [exact local identity](workstreams/B/final-local-identity.json). Task `01a0df7a-cca1-70b0-b5c7-503918eec678`; final socket correction `4934b6e08d58d9b61021cfe2307b43bbb97e7c32`, branch `codex/diagnose-hosted-upload`.
- [C continuous-renewal handoff](workstreams/C/handoff.md). Task `01a0df7b-09b1-7e70-8c87-c6db96f5e19c`; final C-only tested head `776c4f38b9e9b966dfe49fb03db4d9404373109c`, branch `codex/bound-renewal-retention`.
- Integration owner D: task `01a0df7b-42ce-7802-9985-53e3c34c3d67`, original host `local`, implementation branch above. Every implementation input and corrective successor is already integrated; do not cherry-pick duplicate owner commits.

The actual integration history, oldest first, is below. Inspect every commit and the combined diff; this is provenance, not a required 27-PR stack. Full owner-to-integration mappings are in the handoffs.

```text
3cb73b39b62570602fb03f14d474a9e99247c593  Frozen Effect-stack qualification
468b6652bfffa43d7ec6871bee457328a3e76859  Package qualification handoff
8a6953553257952c8c965bd97dbafb7ba14bae1a  Planned switch handoff evidence
31fdb9cae25b60028f62eb32f3c5a136586a270d  Switch evidence compatibility
5670e9d3419e1ef94b91e8132742dbd439fec68e  Settle handles before scheduler failure
da1101edf08579719b02bae06189a4767950a194  Terminal ordering documentation
15d2707ca6f777ea9688e88fed639f0cc689bdfb  Initial shared integration record
fb8836290a4b0dfde1fb302444c5d2bb06f82012  Bound unresolved admission
ce2e00a536d8f695b95e1b01e2d51b6872e7b023  Unknown recovery public contract
23d03f8d7326f8e1ba917f4067a539943a0bab05  Bounded cleanup retention owner
4443f80e9d62d0b571538039c677a011d9651ae3  Continuous renewal lifecycle
9841bda9807cd8b4ec2801bfd5e8c63f251fa9c8  Continuous cleanup public API
ed88be78d73032a368668eb10afa724a0477cb6b  Preserve contradictory cleanup evidence
56f6ebda9f98ca2944759b6bd7a9192792ed92ef  Root aggregate rehearsal budget
87e690daa114499bb7afe735435a65be48f57186  Competing terminal closer regressions
98db268cf9785ff152d402befb53d3b782a12304  Cancel watchdog after truthful drain
877c7b2776b47acb755714fea5cc93a8a17563a9  Public renewal rehearsal harness
bb58b40d0ed41a58a1bfa1634bd7b8b52f4831bc  Scheduler/continuous integration contracts
3e8267061fa82ecd03d3bcfc09fb0c542b247bb5  Historical hosted audio documentation
07d0e4e6f6de9075633db73744c2131c08cbac1a  Actual continuous harness and summary codec
b670fc1de990a21c674dfc52d16d0529ae9b7d27  Public qualification boundaries
1e11b375c66eb463c8aed6890f97f31cfdab94f8  Hosted documentation formatting
91720a64f19810b4fa1bc56025891832ef7ae072  Exact continuous refusal assertion
9dc0baf5df96524eaf4f916bae546c10e2c45fe9  Combined owner-scope Cause assertions
65ff491a4448c8211e9833953c2f8f883c2aaed7  Join all twin-owned sockets
a68d072640a700b1763e3e3ed8910a18031f848a  Shared scheduler source projection
dec33ea21cd8c00311d3cb100cbd5a7184420c05  Final documentation handoff
```

## Independent review and direct fixes

Read current repository policy, `CONTRIBUTING.md`, and relevant tooling documentation. Read `docs/Codex/comments.md` and `docs/Codex/file-structure.md` if present; they were absent at the planning baseline. Use Bun for development and frozen installs, no `any` or `as any`, and no AI attribution in commits. Use the repository's patched Effect compiler, not a substitute compiler that omits its diagnostics. Explain why in nontrivial comments and follow feature-local file/import rules.

Read the archived [Effect-development skill](guidance/effect-development/SKILL.md) and any current applicable guidance available on your host; read the installed Effect package's `AGENTS.md` completely, and consult its relevant guides and actual versioned implementation. Distinguish repository conventions from Effect's guarantees. The qualified local stack was Effect/platform `4.0.0-rc.117` with compiler `7.0.2+effect-tsgo.0.45.0`; confirm actual resolution. Do not assume a v3 idiom or newer documentation describes this version.

Parallelize independent review into scheduler, renewal/retention, harness/evidence, and package/release workstreams. Give each a bounded scope and consolidated findings. Use a single integration owner for overlapping production files, public assertions and documentation. Keep heavy runtime/native/pack gates serialized on this host; independent reading and focused checks can proceed in parallel. Obtain an independent review of the final fixes as well as the original changes. A passing implementation-owner report is evidence to investigate, not an approval.

Review at least these contracts and their interactions:

1. **Terminal ownership and Causes.** One terminal claim must settle retained item handles and actor controls, fence admission, and then notify failure. Check finite masked bookkeeping, child/parent scope order, concurrent closes, original typed failures/defects/interruption Causes, and completion barriers. In the installed Effect implementation, do not equate uninterruptibility with mutual exclusion or a closing scope flag with completed finalization. Prevent self-join and worker/finalizer deadlocks; preserve the original owner cleanup Exit.
2. **Unknown admission and truthful drains.** Canonical Renewal already requests replacement after an unknown command result. Verify real hook-to-replacement progress separately from generic Engine failure. The default 60-second monotonic watchdog, finite override capped at 10 minutes, original uncertainty age, blocked actor controls, replacement readiness, and watchdog cancellation after actual drain completion must agree. No timeout may replay a command, invent a rejected/played outcome, release uncertain capacity prematurely, or claim remote cleanup. Check the 4096-entry filler limit and refusal before dispatch 4097. Review the final source-projection optimization against mutable arrays/objects, retirement and identity reappearance, actor-local observation-token lifetime, and immutable earlier public rows.
3. **Continuous retention and cleanup.** Review the separate opt-in constructor and summary compatibility, legacy lifetime cap/full reports, reservation before acquisition, at most two physical resources, finite retained successes and unresolved/reserved budgets, and fail-closed exhaustion. Incomplete evidence must never be silently compacted. Check owned termination versus attached detachment, failed acquisitions, contradictory evidence, exact bigint codecs, actual finalizer completion, and repeated close with the original Cause. Required `retirement.unknownSubmissions` is nonnegative; a nonzero count remains incomplete even after confirmed termination. Accounting settled means bookkeeping finished. Check private incarnation/source fencing, no replay, per-source accepted/Started bounds, and the documented lifetime identity-uniqueness precondition after bounded eviction.
4. **Qualification evidence and fixture ownership.** Review optional immutable `Switched.handoff`, historical compatibility, final-clip/grace evidence, real public owner/openH3/scheduler/drain use, and attribution of unchanged frames on one monotonic origin. Verify both real constructors, original canonical per-source cleanup, continuous summary reconciliation and failure checkpoints. Review reservation-before-token budgeting, exactly two constrained grants, interruption, lost replies, refused allocation, stalled builds and closes. Offline decoded-frame observations establish neither hosted timing nor presented/encoded output or audio completeness. Check the final HTTP 100-Continue/socket regression and that shutdown actually joins the server-close callback.
5. **Exact dependency and release qualification.** Check frozen lock selection, declaring-owner resolution, recursive installed consumers, exact concrete versions versus unchanged peer ranges, raw identity-byte compatibility, and public installed type assertions. Review the Bun tarball accommodation carefully: only after archive bytes/name/version match, normalize the temporary consumer root manifest to verified exact versions; preserve original manifests/receipts, do not reinstall or edit installed package manifests, and do not suppress invalid/missing peer or nested-version failures. Check the aggregate runner change remains root Bun discovery only: 600 seconds there, 180 seconds for other subprocesses, existing per-case deadlines unchanged.

Fix substantive in-scope findings directly, add meaningful regressions that demonstrate them, resolve integration conflicts, and update behavior documentation where needed. Do not weaken assertions, skip failing cases, enlarge individual deadlines, or dismiss failures as contention merely to manufacture a pass. Preserve failed-run evidence and distinguish newly fixed behavior from pre-existing limitations. Track direct Engine/media all-cause supervision explicitly: A reproduced original defects retained by close while `engine.failure` can stay pending; the scheduler's elapsed Timeout is not a fix or qualification of that separate interface.

## Verification of the final combined revision

Reported local passes on `a68d072` include build/typecheck/lint and all portable guards, 850 client and 58 browser tests on each Node/Bun runtime, 172 root tests including all 26 renewal rehearsals, 85 Rust tests plus fmt/clippy/rustdoc, native Node 37 passes/1 existing platform skip and Bun 29 passes/9 existing platform skips, real local Chrome/native integration, 71 release-tool tests, and full Bun isolated-package validation. Verify the logs and provenance; do not substitute these reports for review or validation of your final changes.

Use the required build, typecheck and lint checks (build first), and run the final combined repository gates:

```sh
bun run verify --profile portable
bun run native:test
bun run test:integration
bun run check:release
PACK_INSTALLER=bun PACK_EXPECT_NATIVE_PLATFORMS=darwin-arm64,linux-x64 bun run test:pack
```

Follow current repository prerequisites and CI definitions, including the installed-package installer matrix. Bun remains the development package manager; the repository's npm-installer qualification job is a separate compatibility check, not permission to replace workspace tooling. Local qualification used Bun 1.4.2, Node 22.13.1, the patched compiler, one Vitest worker, Python 3.13.5, ffmpeg and Rust 1.90.0. Install or select the declared Bun version on your own host. Confirm versions instead of relying on paths from the originating computer.

Existing archive evidence is [package-identity.json](evidence/pack/package-identity.json), SHA-256 `10f3adedf915f8876257132a155b9d447cdacf3862c55653b59b0a80f3cd8040`. The original local client/browser/native archives had these hashes. Their bytes are not committed to Git; the metadata and native-input retrieval/reconstruction instructions are archived here:

- Client: `4bacfdf112fa88735d1a9340af9ccf1ce43ca9d9f3dbeca2cc660b708d64c4c5`.
- Browser: `252fbf4d0c98c1e9bde2d359812c88bcba632f3ae0f2d0bf81c219ffcba00ce8`.
- Native: `c368603de20b1f5c1c978e7098d98d1132c29be11b28431728b02eb84129da39`.

These are development archives still named 0.5.0, not newly published packages. Source-verified SDK native binaries came from [baseline CI run 36223885202](https://github.com/mannyc2/reactor-effect-client/actions/runs/36223885202); native provenance is in the receipt and `evidence/native-inputs`. Local Rust/far-peer checks and Darwin execution were fresh, but Linux execution was not. The native-input directory here contains provenance and sidecars; download the libraries using ARTIFACTS.md. Revalidate identities after changes; rebuild affected artifacts rather than inheriting stale qualification.

**Outstanding at handoff:** final npm-installer CI, the OS/Node CI matrix, and Linux execution on the combined revision. Complete applicable code CI on the exact final reviewed head through normal PR workflows; do not dispatch publication/deployment or paid workflows. Distinguish passed, failed, skipped, unrun, platform-limited and awaiting-review states. If an unavailable platform or required permission prevents a gate, report the exact blocker instead of claiming readiness. Reconcile newer main and rerun affected checks after any fix, rebase or conflict resolution; do not endlessly rerun unchanged passing gates without a reason.

Paid scheduler/renewal, outstanding resume/provider qualification, hosted TURN, audio completeness and presented/encoded output remain separate. The exact-byte hosted candidate is held for missing qualification, a named emergency operator/deadline, fresh pricing and ledger review, and explicit spend authorization. Do not ask for paid authorization merely to finish this merge-readiness task; identify whether an unrun check blocks a particular code PR or only a broader product claim.

## PR preparation, issues, and the stopping point

Fetch latest main and inspect existing PRs across relevant branches before creating anything. Reconcile duplicates or superseded PRs. Choose focused PRs with a clear dependency stack, or a coherent integration PR if splitting would make the tested changes harder to review. Explain the choice. Preserve meaningful fixes and all required final successors when rearranging commits. Conceptual dependencies are 001 before 002, switch evidence before continuous lifecycle, both harness and lifecycle before continuous harness qualification; 005 is independently reviewable. Shared assertions and final corrections must accompany the corresponding behavior.

Push the required review branches, create or update PRs with truthful scope and verification evidence, and attach every created or updated PR to the Codex task. Obtain independent review, fix actionable findings, and rerun affected gates. Read the current branch protections and required reviews/checks. Require successful applicable CI on the exact reviewed head and identify any pending formal approvals. Do not bypass protections or treat your own tests as an independent review. Recheck current main and mergeability before reporting readiness.

Read and reconcile these issues against the actual code, regressions and remaining scope:

- [#36 — A failing scheduler can leave its items unsettled](https://github.com/mannyc2/reactor-effect-client/issues/36): assess immediate failure-driven closure and complete local settlement, including competing closes and Causes.
- [#37 — A lost enqueue reply holds the only build slot until its source retires](https://github.com/mannyc2/reactor-effect-client/issues/37): assess canonical recovery, bounded generic service, strict uncertain capacity, no replay and truthful drains. Describe precisely what the new timeout does and does not guarantee.
- [#31 — Scheduling audit: source-aware order, fate recovery, and monotonic timing](https://github.com/mannyc2/reactor-effect-client/issues/31): much of the original delivery shipped in [PR #34](https://github.com/mannyc2/reactor-effect-client/pull/34) and 0.5.0. Reconcile remaining public scheduler/renewal and resume/provider qualification, and the-show migration/container evidence against their real status. Do not close the scope on the basis of offline tests or claim another repository's migration is verified without evidence. Record whether continuous mode and Engine/media supervision remain separate follow-ups.

Recommend issue dispositions with specific evidence and residuals. **Leave issues open; use neutral references rather than automatic closing directives.** No merging, auto-merge, merge-queue entry or issue closure is authorized by this prompt.

Finish with PR links and exact head SHAs; current main SHA; substantive findings and direct fixes; independent review results; final local/CI evidence and unrun limits; a per-PR ready/blocked assessment; recommended merge order and any review/main-reconciliation requirements between dependent PRs; and proposed issue dispositions with remaining work. Say explicitly that nothing was merged or closed. The result should let me decide which PRs can be merged now and what still prevents the others.
