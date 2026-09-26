# Plan 004: Support long-running renewal with explicit resource bounds

Planned at `c47e50c784f6889b542cf8ee316067b688ca4737`, 2026-09-26. P2; L; high fix risk; high confidence in the current limitation. The retention core starts independently; lifecycle integration follows only the small 003-E shared-source handoff, and can run alongside 002/003-H. The proposed design is complete enough for API review; implementation remains TODO. This is a follow-up to [#31's long-running-session note](https://github.com/mannyc2/reactor-effect-client/issues/31#issuecomment-5838220483), not a prerequisite for the urgent shutdown fix.

## Parallel execution

Split into **004-R** (API review, bounded retention owner and `Retention.test.ts`) and **004-L** (lifecycle integration and `RenewalRetention.test.ts`). 004-R starts immediately alongside 001/003-E/005. It owns the concrete `retention.ts` module and fixtures without editing live renewal owners. 004-L waits only for that core and the small 003-E handoff in shared source files; it then runs alongside 002 and the 003-H harness. It does not wait for all 003 offline work. See the [parallel execution guide](PARALLEL-EXECUTION.md) for file ownership, contract handoffs and integration gates. Coordinate any reproduced 002 renewal defect separately.

## Current state

`packages/client/src/orchestration/renewal.ts:181–185`:

```ts
const slots = new Map<string, Slot>();
const cleanups: SourceCleanup[] = [];
const seenCleanups = new Set<SourceCleanup["lease"]>();
const maxSessions = options.maxSessions ?? 64;
```

`acquire` refuses when `opened >= maxSessions` at line 486; each successful slot increments `opened` at line 538. This is a cumulative lifetime limit, not simultaneous source capacity. `recordCleanup` at lines 273–276 retains every lease object; closed slots remain in the map. Final close returns `Object.freeze({ sessions: Object.freeze([...cleanups]) })` at line 618.

`types.ts:316` makes the full sessions array the cleanup-report schema. `SourceCleanup.lease` at line 306 is the unmodified canonical `CloseReport`, including remote confirmation and errors. `source-slot.ts:124–136` retires through a close gate, closes the remote lease, joins committed work, retires affinity and closes the owned scope. Prune only after those responsibilities finish.

`Media.test.ts:285` uses `maxSessions: 1` as an intentional terminal-bound test. Removing or silently reinterpreting the option breaks a documented contract. With hourly lifetimes the default limit is roughly a few days, sooner with lead overlap/replacements; increasing it to 4096 postpones the ceiling and retains more objects.

## Recommended contract

The source-backed [retention decision](004-renewal-retention-decision.md) inventories every retained collection, defines the cleanup predicate, identity contract, report schema and regression matrix. Its concrete proposal is:

- Preserve `make`, `layer`, `HandleShape`, `CleanupReport` and default lifetime cap of 64. Add `makeContinuous` sharing the same renewal mechanism, with a distinct `ContinuousHandleShape` whose `close`/`cleanup` use a new `CleanupSummary`.
- Omitted `maxSessions` means no lifetime cap only in `makeContinuous`; an explicit value retains today's cumulative successful-opening meaning. Allow exactly two physical reservations including opening/live/recovering/retiring sources.
- Keep the newest **64** complete cleanup records by default (configurable 0..4096) and an unresolved ledger of **16** records/reservations (configurable 2..4096). Reserve an unresolved slot **before** every open attempt, including attempts that fail before a Slot exists. A completed report releases that reservation or converts it to an incomplete record. Exhaustion fails closed before a new allocation; existing owners already have space for their final reports.
- Summarize only proven complete retirements. Preserve every incomplete canonical report, accounting/scope/affinity failure, unresolved publication and unknown allocation. Attached sessions with successful local detach have no owned remote-termination obligation; count them separately from confirmed owned terminations. Never infer confirmation from elapsed expiry or source disappearance.
- Use a new format-discriminated summary with `retained`, exact omitted-complete counters, totals and exhaustion status, **no top-level `sessions` field**. An old decoder must reject it rather than read truncated history as complete. Keep canonical CloseReport values unmodified; add local retirement evidence beside them. Counts use bigint with JSON decimal encoding.
- Require globally unique physical source IDs and no reassignment of clip IDs for the continuous handle's lifetime. Validate live collisions and identity strings bounded at 1024 UTF-16 code units. Document that exact historical misuse detection after eviction is weaker than legacy `make`; arbitrary lifetime uniqueness cannot be fully checked with bounded memory. Add private incarnation tokens for already-selected preparation, sequence ownership and commit validation; public session facts stay physical IDs.
- Bound per-source accepted/Started ID sets at 4096 in continuous mode, including reservations before dispatch. Keep existing independent sequence limits and explicit acknowledgement/release. Do not imply continuous renewal permits unlimited unreleased sequence history or an infinite-lived source with infinite admissions.

The new API intentionally makes the identity precondition and summarized report a reviewable migration. If review rejects the uniqueness precondition, defer continuous mode and design public SourceRef/ClipRef controls separately; do not widen this retention PR into a scheduler API rewrite or add unbounded tombstones. This is a concrete recommended design, not an instruction to seek approval before ordinary planning work.

## Retirement and Effect ownership

`SourceSlot.closed` includes **Closing** (`renewal-state.ts:46–47`), and `recordCleanup` runs before committed joins, sequence retirement and owned scope close (`source-slot.ts:130–136`). Neither is an eviction barrier. Add one shared retirement completion Exit; only after accounting, scope/affinity work, loss aggregation and record classification finish can a Slot leave the live registry. Preserve original defects and idempotent joins. A stalled uninterruptible Source.close still owns its resource/reservation; neither a timeout nor another Scope.close call proves completion.

Effect rc.117 `forkIn` removes the scope finalizer when its fiber exits; a closed forked scope removes its parent link. A finalizer leak is **not** established by their presence. Verify these facts against the execution lock; never manipulate private Effect scope internals. Test the concrete retention owner through public lifecycle barriers.

A prepared logical handle that has never selected a source may still route to current on first submit. Once selected, commit must validate the captured incarnation. After commitment, retain its immutable outcome while releasing references to old Slot/media objects where possible. Caller-retained handles are outside the library's own count bound; no handle should retain the whole historical registry.

## Scope and conventions

This planning pass writes only `plans/`. Implementation scope: `packages/client/src/orchestration/{renewal,source-slot,types,routing,index}.ts`, the concrete `retention.ts` owner, `Sequence.ts` only if the identity/affinity decision requires a separately reviewed change, existing renewal/lifetime/trace/media/routing/scheduler integration tests plus `Retention.test.ts`, `RenewalRetention.test.ts` and `test/Evidence.test.ts`, `test/orchestration/README.md`, public type/pack consumers, package README, changelog and plan index. List final exact files in the decision; stop before touching files outside that list. No H3 protocol, browser/native transport, pricing, show restart-policy or scheduler-policy changes. Plan 003 owns adapting its new harness to the continuous constructor and summary; coordinate that integration after this API lands.

Use Bun, no `any`, feature-local imports and comments explaining the retention tradeoff. Read `CONTRIBUTING.md` and installed Effect scope/finalizer source. `docs/Codex/comments.md` and `file-structure.md` are absent at the baseline; read them if restored. Follow `RenewalTrace.test.ts`/`SourceFixture.lifecycle` for external lifecycle barriers, and the Schema cleanup codec pattern in `types.ts` for serialized evidence.

## Steps

1. Prepare the independent core on `codex/renewal-retention-core`; integrate it with 003-E on `codex/bound-renewal-retention`; run `git diff c47e50c..HEAD -- packages/client/src/orchestration packages/client/src/Sequence.ts packages/client/test/orchestration`. Reconcile 003-E and any separately proven 002 renewal patch; scheduler-only changes do not block core development. **Baseline from `packages/client`:** `bun run test test/orchestration/Renewal.test.ts test/orchestration/Media.test.ts test/orchestration/RenewalTrace.test.ts --testTimeout=6000` → exit 0.

2. Review the existing decision against the implementation checkout, then finalize the proposed signatures and exact file list. Preserve its named numeric bounds and semantics for: opening/live slots, completed success retention, incomplete cleanup retention and exhaustion, deduplication, reused source identity, retained prepared handles, sequence-affinity retirement, repeated close, report codec compatibility, and the `maxSessions` migration. Give the proposed public API and example report containing omitted-success counts or explicit truncation metadata. No apparent complete-history report may silently omit sessions. **Verify:** `rg -n '^## (Bounds|Cleanup predicate|Identity and handles|Report schema|Compatibility|Implementation files|Regression matrix)' plans/004-renewal-retention-decision.md` → all seven sections. Every mutable collection must have a stated bound/eviction rule or a justified independent existing bound.

3. Add accelerated long-run tests for more than 64 successful renewals with small lifetimes and flowing frames. Use small configured retention bounds so eviction occurs many times without creating a slow 4096-session test. Add close/replacement interruption, pending committed submission, live duplicate source IDs, selected-preparation incarnation fences after historical eviction, completed and unresolved sequences, incomplete cleanup and repeated-close scenarios. Assert public behavior and cleanup reports; if bounded internal retention needs direct inspection, test a concrete private retention owner rather than adding public debug counters. **Verify:** focused renewal/trace tests fail for the targeted old ceiling and pass for unchanged legacy-cap behavior.

4. Implement the chosen contract and bounded state in small ownership-preserving steps. Release closed slots only after local lifecycle/accounting finishes; retain small factual evidence, not live source/media objects. Keep finite live/acquisition bounds independent of history. Preserve unconfirmed cleanup, loss counters and old handles; an already-selected preparation must refuse rather than bind to a later incarnation; an Unbound logical handle retains current first-submit routing. **Verify:** renewal, lifetime-boundary, expiry, media and trace suites all pass on both Node and Bun. From `packages/client`, use `bun run test test/orchestration --testTimeout=6000`, then `node node_modules/vitest/vitest.mjs run test/orchestration` and `bun --bun node_modules/vitest/vitest.mjs run test/orchestration` for final runtime verification.

5. Update API/schema consumers and migration docs. Test encoding/decoding old evidence according to the decision, and idempotent repeated close with a compacted report. **Verify root:** `bun run verify --profile portable` and `PACK_INSTALLER=bun bun --no-env-file scripts/pack.ts --portable` → exit 0. Full native/pack CI remains required. **Final-join gate, not a prerequisite for the 004-L handoff:** after 003-C consumes the real continuous API, run 003's offline public-renewal scenario for both legacy and continuous constructors. 004-L completes its packet with its own lifecycle/codec regressions and 003-E evidence tests; it must not wait circularly for 003-C. Final separately authorized hosted qualification uses the continuous constructor with a two-allocation cap and records that choice; >64-renewal retention remains an offline test, not a claim from two live sources.

## Done criteria and stops

The decision appendix supplies the implementable retention predicate, numeric bounds and proposed compatibility policy; record API review acceptance separately from implementation. Implementation is done when accelerated renewal exceeds 64 acquisitions, all library-owned retained collections remain within their documented count limits, incomplete cleanup is reported honestly, exhaustion is explicit, old handles stay fenced, and legacy cap behavior is preserved or deliberately migrated.

`bun run build`, `bun run typecheck`, `bun run lint` must pass; portable verify includes them in the needed order. Dependencies use `bun install --frozen-lockfile`. No generated or out-of-scope changes; conventional commits without AI attribution; do not push or publish without instructions. Update index status and proof.

Stop if successful retirement cannot be distinguished from unresolved obligations, selected operations cannot be incarnation-fenced after eviction or the explicit unique-ID precondition is rejected, a new collection would grow without bound, or a schema change is being hidden as a patch-compatible detail. Never solve this by dropping failed cleanup records. Any decision affecting lifetime limits or report completeness needs explicit release notes and review.
