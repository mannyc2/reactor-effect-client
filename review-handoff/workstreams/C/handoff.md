# Stream C — bounded continuous renewal handoff

Worktree: `/Users/chriscarroll/.codex/worktrees/437f/reactor-effect-client`.
Branch: `codex/bound-renewal-retention`.
Audited and freshly fetched starting main: `c47e50c784f6889b542cf8ee316067b688ca4737`; no baseline drift.
Final tested HEAD: `776c4f38b9e9b966dfe49fb03db4d9404373109c`. Tracked working tree is clean.

## Local commits and integration order

| Packet | Local commit | Purpose |
| --- | --- | --- |
| 004-R | `763052cd82bf5f27278ea55e0e306d86889396c0` | Concrete retention reservations, complete/incomplete classifier and tests. Also retained on `codex/renewal-retention-core`. |
| B003-E dependency | `79740ae992437939be191f568d6c08c15ed26342` | Cherry-pick of B's `c34e95b8874d7cffaa2c400173e15c417289369a`, planned handoff evidence. |
| 004-L | `5cb2be80ed0061852367f2908adfba961e9373dd` | Continuous constructor, bounded ownership and history, private incarnation fencing, retirement and Schema tests. |
| 004 review correction | `9afac859a3af77ed8ece58d0ed898e2387e80a84` | Retain contradictory canonical evidence and verify actual scope-finalizer defects. |
| D005 dependency | `788ca45aae74b2e18adc7b3b86a763c20895cc06` | Cherry-pick of D's `3cb73b39b62570602fb03f14d474a9e99247c593`, exact frozen Effect-stack package qualification. |
| D C-only shared assertions/docs | `776c4f38b9e9b966dfe49fb03db4d9404373109c` | Cherry-pick of D's `96551d914a0b94b34b3027d380c2ff49f0691836`. |

For a join that already contains B003-E, consume C's `763052c` then `5cb2be8` then `9afac85`; do not duplicate `79740ae`. D has already consumed the source and shared inputs. C carries no A scheduler changes or B003-H/C rehearsal harness changes. D's root-only 600-second aggregate runner budget is reviewed but not required on this C-only branch.

## Exact C-owned files

- `packages/client/src/orchestration/index.ts`
- `packages/client/src/orchestration/renewal.ts`
- `packages/client/src/orchestration/retention.ts`
- `packages/client/src/orchestration/source-slot.ts`
- `packages/client/src/orchestration/types.ts`
- `packages/client/test/Evidence.test.ts`
- `packages/client/test/orchestration/Media.test.ts` (adapt B's direct SourceSlot fixture to its internal options)
- `packages/client/test/orchestration/RenewalRetention.test.ts`
- `packages/client/test/orchestration/Retention.test.ts`

Public docs/assertions are supplied by D in `CHANGELOG.md`, `packages/client/README.md`, `packages/client/test/PublicTypes.test.ts`, `packages/client/test/orchestration/README.md`, `scripts/pack/node-consumer.mts` and `scripts/pack/browser-consumer.mts`. D owns shared plan status and final integration notes. No `Sequence.ts`, `routing.ts`, scheduler production files, native sources, dependency manifests or lockfiles were changed by C.

## Contract

`makeContinuous`, `ContinuousOptions`, `ContinuousHandleShape` and the `CleanupSummary` Schema are opt-in exports. Legacy `make`, `layer`, `Handle` and complete `CleanupReport` retain their behavior, including the default 64-successful-open lifetime cap. Continuous renewal omits that cumulative cap unless `maxSessions` is explicitly supplied (same 1..4096 meaning).

Continuous adapters promise unique physical-source identities and no lifetime clip-ID reassignment. IDs must have 1..1024 UTF-16 code units. Private incarnation tokens fence selected preparations, source-fenced enqueue and sequence owners. Truly unbound preparations select current at first submit. Completed logical submissions retain only their original Exit after settlement, while live execution watchers are scoped to the source. Historical raw identity misuse after eviction is deliberately not detectable; evicted anchors may refuse as missing rather than SessionRetired.

There are at most two opening/active/recovering/retiring reservations. Retention capacity is reserved before open, including failed acquisitions. Successful detail retention defaults to 64 and accepts 0..4096; incomplete plus outstanding reservations default to 16 and accept 2..4096. Incomplete evidence is never evicted. Exhaustion fails closed. Accepted and distinct Started histories are separately bounded at 4096 per source, including pre-dispatch in-flight reservation. Repeated result hooks and Started observations are idempotent.

Retirement performs media close, canonical source close, budgeted local accounting, affinity retirement, owned-scope close and completion bookkeeping before pruning or releasing the resource. Independent stages still execute after a defect, and repeated close preserves the original Cause. A stalled finalizer remains pending and keeps its reservation; no whole-close deadline is invented. Completed failed close can expose factual cleanup without becoming a successful close Exit. The installed Effect rc.117 fork/scope implementation and public Scope.state regression verify that completed scoped fibers and closed child scopes detach their finalizer links.

The new report format is `reactor-orchestration-cleanup-summary/v1`, with no top-level `sessions`. Frozen summaries preserve canonical reports and count omitted proven-complete records separately as `noAllocation`, `ownedTerminated` and `attachedDetached`. An attached detach never counts as owned termination. Ordinals and totals are exact bigint values; JSON codecs use decimal strings and diagnostic errors. Unknown allocation, policy/local errors, unresolved publication, contradictory canonical response/identity facts, incomplete retirement and unknown committed outcomes all stay incomplete.

Reviewed plan refinement: `retained[].retirement.unknownSubmissions` is a required nonnegative bigint. Accounting `settled` means bookkeeping finished, not that every dispatch outcome became known. A nonzero count consumes the incomplete ledger even when remote termination is confirmed. Parent and B reviewed this refinement; canonical lease reports remain unchanged and no cleanup error is fabricated.

## Verification

Frozen installation uses Effect `4.0.0-rc.117`, compiler `7.0.2+effect-tsgo.0.45.0`, declared Bun `1.4.2` and Node `22.13.1`. Final gates use task-local PATH/BUN_BINARY pointing at `/Users/chriscarroll/.local/share/vite-plus/package_manager/bun/1.4.2/bun/bin/bun` and `VITEST_MAX_WORKERS=1`. Heavy validation was serialized after A and released directly to B when both gates completed. No per-test deadline was increased.

- Regression-first: the initial lifecycle cases failed for absent `makeContinuous`; contradictory evidence failed as wrongly complete before the correction. The new tests cover 80 renewals with two retained successes, legacy cap/explicit cap, identity fencing/reuse, unresolved ledger reservation, unknown outcomes, duplicate hooks, 4096-entry histories, repeated reconnect finalizer retention, defective/stalled close and parallel parent closure, exact summary codecs and malformed summaries.
- Focused Node and Bun orchestration plus Evidence: 27 files / 266 tests passed on the main lifecycle implementation before the two review cases (`node-focused.log`, `bun-focused.log`). Later targeted final core/lifecycle suites passed 30 tests on each runtime. The final full gate below includes all final cases.
- Final `bun run verify --profile portable`: **PASS, exit 0 on `776c4f3`**. Includes generated wire check, formatting, build, portable runtime import guards, lint, full workspace typecheck, architecture, examples, 63 client files / 793 tests on each Node and Bun, 6 browser files / 58 tests on each runtime, and 132 root offline tests across 9 files (120.92 seconds). Log: `final-portable.log`.
- Final `PACK_INSTALLER=bun bun --no-env-file scripts/pack.ts --portable`: **PASS, exit 0 on `776c4f3`**. Exact frozen Effect-stack validation, archive identity, public installed consumer compilation, portable simulation smoke, browser bundle, installed Node/Bun examples and browser example compilation passed. Log: `final-pack.log`. The two consumer manifests resolve Effect `4.0.0-rc.117`; temporary consumers were removed by the successful runner. Retained evidence is `../pack-gspy0t/package-identity.json` and adjacent normalized installation manifests.
- `git diff --check` passes and tracked working tree is clean after the gates.

Archive SHA-256:

- client `ece880d0deec8d195c633215239e883fac659d25be40281946298ef8018e8e75`
- browser `252fbf4d0c98c1e9bde2d359812c88bcba632f3ae0f2d0bf81c219ffcba00ce8`

The initial core full portable attempt on default Bun 1.4.0 with other worktrees testing concurrently failed six client cases: SchedulerCore higher-lane waiting, ElapsedTime wall-back renewal, and four SchedulerRenewal40s overlap scenarios (earlier-frame-loss, old-line-building, final-clip-frame-loss and final-clip-frame-loss-no-line). Each hit its existing five-second deadline; 59 files/762 tests passed and 3 files/6 tests failed. Its preceding static/build checks passed. The failure was recorded in task output and not treated as proven contention. The final declared-runtime, serialized single-worker full gate passes those tests and the new implementation without increasing test deadlines.

## Remaining integration ownership

C004-R and C004-L are complete and handed off. B next runs the actual two-constructor 26-case rehearsal matrix; D owns the joined source revision and final portable/native/package qualification. These joined checks are separate evidence from C's final gates. A characterized an existing unsupervised renewal recovery-fiber defect: close retains its original Die while engine.failure remains pending. Direct Engine/media all-cause supervision remains a separate follow-up; C does not silently broaden this packet.

No new hosted qualification, paid provider session, native/full-package combined qualification, remote push, merge or publication is claimed. All implementation commits remain local.
