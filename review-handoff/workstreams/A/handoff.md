# Stream A — scheduler handoff

Worktree: `/Users/chriscarroll/.codex/worktrees/1915/reactor-effect-client`.
Audited and freshly fetched starting main: `c47e50c784f6889b542cf8ee316067b688ca4737`; no baseline drift.
Final implementation branch: `codex/bound-unknown-admission`.

## Local commits

| Packet | Commit | Changes |
| --- | --- | --- |
| 001 | `6bf95b2a07135467711a708dbe3713275cec098b` | Single terminal owner, finite settlement before notification, private sequential scope, phase/Cause/startup/control regressions. Also retained on `codex/fix-scheduler-settlement`. |
| 002 | `efea67a3b2ed808220fb045c1b65377b41a316c9` | Finite unknown recovery deadline, independent scoped watchdog, bounded uncertain filler ledger and 34 regressions. |
| 001 regression supplement | `9e02cfdef60a6e30e0347dd16f047ff2709fe6de` | External sequential/parallel owner closure during terminal settlement joins the original claim. |
| 002 review correction | `7d3d129a9a29285ba20f833732560c93bc899cf9` | A truthful completed drain cancels its retained capacity watchdog; regression fails on the preceding implementation. |

## Exact changed files

- `packages/client/src/orchestration/scheduler.ts`
- `packages/client/src/orchestration/scheduler-policy.ts`
- `packages/client/test/orchestration/SchedulerFates.test.ts`
- `packages/client/test/orchestration/SchedulerUnknownRecovery.test.ts`

Shared docs and public consumer assertions are authored by stream D on its integration branch, not duplicated here. No renewal/source-slot/SourceFixture/dependency production edits.

## Contract

`Scheduler.failure` publishes after retained handles, actor-owned controls and `accepting = false` settle. Typed failure keeps Unknown honest and preserves earlier decisive evidence; abnormal Causes retain the original cause. A synchronous terminal claim owns masked finite local settlement; later closers join its completion. The scheduler-owned sequential child settles first, joins workers next, tears queues/events down last. Scope close does not claim bounded provider cleanup.

`SchedulerOptions.unknownRecoveryTimeout?: Duration.Input` defaults to 60 seconds and accepts a positive finite override up to 10 minutes. Accessor properties are rejected without invocation. The monotonic deadline begins when the command worker observes uncertainty, before actor delivery; it therefore also operates while `stopRenewal` holds up the actor. Timeout uses the existing Timeout reason, operation `scheduler.unknownRecovery`, and original uncertain command failures in diagnostic detail. No replay or timed capacity release. A Ready replacement clears the old capacity episode; a later drain uses the original age of retained uncertainty. Local retirement without a Ready replacement retains the active episode until recovery or truthful drain completion. Uncertain filler identities are independently retained/reconciled with a 4096-entry bound; service fails before a 4097th dispatch.

## Verification record

- Frozen install: Effect `4.0.0-rc.117`; compiler `7.0.2+effect-tsgo.0.45.0`.
- Final gates use existing Bun `1.4.2` at `/Users/chriscarroll/.local/share/vite-plus/package_manager/bun/1.4.2/bun/bin/bun`, task-local PATH/BUN_BINARY and `VITEST_MAX_WORKERS=1`. Node is `22.13.1`.
- Regression-first: 12 immediate failure-driven close/Cause cases failed against baseline on Node and Bun; initial three default-watchdog cases failed before 002, while both canonical recovery characterizations passed. Logs are retained alongside this handoff.
- Final 002 focused suites before the small review correction: Node and Bun each passed 87/87 in SchedulerFates/Core/Renewal/UnknownRecovery at default test timeouts. Patched client typecheck and type-aware scoped lint passed.
- Review correction: the completed-drain assertion failed on `efea67a`; corrected drain plus two competing-closer cases passed on Node and Bun, and patched client typecheck passed.
- Full portable on `efea67a`: PASS, including build/lint/typecheck/architecture/examples, 809 client tests and 58 browser tests on each runtime, plus 99 root portable/offline tests.
- Final portable on `7d3d129`: PASS (exit 0), including generation/format/build/lint/typecheck/architecture/examples, 811 client tests and 58 browser tests on each runtime, plus 99 root portable/offline tests. Log: `scheduler-final-portable.log`.
- Final portable package consumers on `7d3d129`: PASS (exit 0), installer Bun 1.4.2 with retained isolated consumers. Client/browser archive identity, portable Node simulation smoke, public consumers, browser bundle and installed examples passed. Both isolated consumers resolve Effect `4.0.0-rc.117` (verified from their installed package manifests). Log: `scheduler-final-pack.log`; archive evidence: `../pack-2l4rkr/package-identity.json`; consumers retained at `/var/folders/wm/3vd104sn2lx_4dy2y36j_8qh0000gn/T/reactor-effect-pack-yaVaXl`. This uses this branch's baseline pack tooling, not D's exact-stack improvement or shared assertion successors.

The initial 001 full portable run failed seven tests (six timeouts and one missed H3 observation) under the earlier Bun 1.4.0/multi-worker run. Logs are retained. The isolated retirement test and later declared-runtime single-worker full portable passed; failures were not dismissed or hidden. The 4096-entry stress case initially exposed excessive scanning/allocation and was improved without reducing its bound or inflating test timeouts.

## Integration and limits

D owns public option assertions, installed-consumer assertions, shared README/changelog/test documentation and the final combined revision. B has the full scheduler commit chain for its actual legacy/continuous constructor matrix; C has the recovery/cleanup findings. Their combined gates are separate from this worktree's evidence.

Canonical Renewal and actual H3/public Session fixture result hooks replace the source and permit a distinct later line without replay. Blocked real close retains its cleanup owner and returns its original report after release. The existing unsupervised Renewal recovery-fiber defect is reproduced: its original Die remains observable in the close owner while `engine.failure` stays pending. Direct Engine/media all-cause supervision remains a separately scoped follow-up; the scheduler's elapsed service Timeout is a distinct failure.

No hosted qualification, paid provider run, native/full-package combined qualification, remote push, merge or publication is claimed by stream A.

Final tracked working tree is clean. All four implementation commits are local; combined-branch qualification remains with D.

## Combined-revision regression successor

D's final combined `91720a6` portable Node gate failed two scheduler tests (847 passed, 2 failed). The saved report is `combined-91720a6-portable-failure.log`. The source-close characterization failed because C now correctly retains the original retirement defect through final owner cleanup. The 4096-entry fixture separately exceeded its unchanged default 5-second timeout. Neither failure was dismissed as contention.

Local successor `cdb7a773fa8f0583914b7a0f0c219d6c1431f245` on `codex/fix-combined-scheduler-regressions` starts from combined `91720a6` and changes only `packages/client/test/orchestration/SchedulerUnknownRecovery.test.ts`:

- Own Renewal in an explicit Scope and assert the original defect object in both `handle.close` and `Scope.close` Exits, while keeping `engine.failure` pending and checking the scheduler's independent Timeout.
- Run the operation-count capacity fixture with the live clock rather than sorting canceled virtual watchdog sleeps; the 10-minute recovery deadline remains unchanged. Put the preferred source first in the unordered snapshot to avoid irrelevant repeated full-snapshot lookup. All prior sources stay live; a new assertion requires all 4097 sources after exactly 4096 distinct uncertain dispatches. The Overflow/no-4097th-dispatch assertions and default 5-second timeout remain unchanged.

The live-clock change alone still timed out; source ordering was also necessary in the measured candidate. A trial scheduler scan optimization did not resolve the timeout and was reverted completely. No production source changes are in this successor.

Validation on the combined base plus successor: root build, typecheck and lint pass; Node and Bun each pass all 89 cases across SchedulerFates/Core/Renewal/UnknownRecovery. The full unknown suite passed under CPU profiling, with the capacity case measured at 3655 ms. Required static checks initially exposed an Exit union mismatch in the new assertion; mapping successful close output to void corrected it without changing Cause propagation. Final working tree is clean. The final combined full gate remains D's responsibility after cherry-picking this successor; this focused result does not replace it.

## Source projection performance successor

D's combined `65ff491` portable r3 gate again exceeded the ledger test's five-second timeout (5033 ms; Node 848/849). The prior fixture-only successor was insufficient; its passing focused checks did not establish reliable full-suite margin. The failed gate is retained in `scan-optimization/reactor-d-final-portable-r3.log`.

Local successor `a790eb613a7a8c77079abd83e41b165d5cb9777f` on `codex/optimize-scheduler-uncertainty`, based on `65ff491`, changes scheduler source projection and adds an in-place snapshot regression. Profiling identified repeated source-Set construction, cached-row retirement scanning and duplicate row lookup/public projection as the main cost. The scheduler now reads each source into a shared membership/public-row projection with a fresh observation token. Immutable empty rows are reused; cache pruning is needed only if retained rows exceed current snapshot size. The token-backed membership view stays inside the serialized actor and is consumed before its next projection. Mutable Engine arrays are read on every observation; array identity is never evidence. No new uncertainty index or API change was introduced.

The added regression changes the same source array and objects in place, verifies retirement settles Unknown honestly, reintroduces an old source ID, changes Ready rows in place, and checks that earlier public rows remain unchanged. The existing 4096 uncertain dispatch bound, all 4097 live sources, ten-minute watchdog and default five-second test timeout are unchanged.

Measurements under the same CPU profiling command: the complete unknown suite passed with the ledger test at 3462 ms before and 1462 ms after. Both worker profiles are retained alongside the logs. Required root build/typecheck/lint pass. The final full client suite passes 850/850 across 64 files on Node 22.13.1 and Bun 1.4.2, one Vitest worker. The ledger case measured 2095 ms inside the full Node suite and 1200 ms inside the full Bun suite. Final formatting/diff checks pass and tracked working tree is clean. D owns the next combined portable/native/package gate; these results do not claim that wider gate already passed.
