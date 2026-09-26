# Plan 002: Bound unresolved admission without inventing provider facts

Planned at `c47e50c784f6889b542cf8ee316067b688ca4737`, 2026-09-26. P1; M–L; high fix risk. Depends on 001 before production watchdog implementation; characterization can start earlier. Existing report: [#37](https://github.com/mannyc2/reactor-effect-client/issues/37). **The generic Engine starvation is established; lifetime starvation through canonical Renewal is not reproduced.** This revision corrects that distinction. The detailed source trace and proposed timeout contract are in [the capacity decision](002-unknown-capacity-decision.md).

## Parallel execution

Stream A follows 001 for production changes; characterization and test design may start earlier. Run in parallel with 003's harness and 004's retirement integration. Use `packages/client/test/orchestration/SchedulerUnknownRecovery.test.ts` for new cases when that avoids shared suite edits; existing suites remain required regression gates. `SchedulerRenewal.test.ts`, public-type/pack assertions and shared docs follow the single-writer handoff in the [parallel execution guide](PARALLEL-EXECUTION.md). No planned renewal production edit is required; coordinate a separately reproduced lower-owner defect with 004-L instead of competing for its files.

## Problem, existing recovery and recommendation

An enqueue with an unknown outcome occupies build capacity. At the default cap of one, another item and filler cannot dispatch on that source until keyed evidence or retirement. A local snapshot's absence does not prove rejection or free remote capacity. Preserve that conservative accounting.

Canonical Renewal already requests replacement after an unknown result. The result hook in `packages/client/src/orchestration/renewal.ts:750–787` does:

```ts
target.recordResult(result);
// ...sequence accounting...
target.finishAccounting();
if (Result.isFailure(result) && result.failure.context.outcome === "unknown") {
  yield* scheduleRecovery(target, result.failure, "replace");
}
```

`SourceSlot.recordResult` marks the slot indeterminate (`source-slot.ts:223–228`); `recover` joins committed accounting then chooses replacement (`renewal.ts:363–372`). `scheduleRecovery` is scope-owned and serialized by the commands permit (`:449–457`). The existing scripted Engine report does not run that path. **Do not add a duplicate public recovery capability or automatically replay the key.** First test the real hook-to-replacement path.

Recommended additional contract: a default **60-second unresolved-recovery deadline**, configurable as a finite positive duration up to **10 minutes**, terminalizes the scheduler if neither evidence nor usable replacement restores progress. It is a local service-availability policy, not a provider timing guarantee or permission to reuse capacity. The deadline applies to ordinary items and filler. It preserves terminal Unknown and uses 001's settle-before-notify boundary. Document this changed default behavior and its release compatibility implications.

The timeout does not promise that provider cleanup or the owning scope has completed. A source close can stall uninterruptibly; the scheduler must still make its own retained handles/control waiters terminal. No operation may claim confirmed remote shutdown without canonical cleanup evidence.

## Current boundaries to preserve

- `scheduler-policy.ts:255–266` counts Building, Unknown, active filler and uncertain filler against capacity; `:285–294` has an additional uncertain-filler guard. Do not remove these without provider proof.
- `scheduler.ts:569` holds one `unknownFiller`; `:1344–1349` writes it. Handle successive unknowns across replacement sources without overwriting still-relevant evidence or replaying an earlier key.
- `types.ts:154–163` offers a gap-free **local** state/subscription pair. Snapshot serials at `scheduler.ts:1766` are not a provider dispatch barrier.
- `scheduler.ts:1543–1550` settles unknown work on source disappearance. `renewal-state.ts:46–47` considers both Closing and Closed absent/closed, so this is routing retirement, not completion of cleanup. An episode must remain timed while there is no Ready preferred live replacement.
- `scheduler.ts:1715` awaits `engine.stopRenewal` inside the actor. That can wait for a permit held by recovery; a watchdog only delivered as an actor message can stall behind the very condition it should bound.
- `SourceSlot.close` masks its whole close path and awaits `source.close` (`source-slot.ts:123–136`). A timeout around that call is not proof of bounded cleanup. Keep source ownership and cleanup evidence intact.
- `replace` can disrupt accepted/playing clips and allocate a replacement (`renewal.ts:311–360`). Preserve that existing behavior and its failure evidence. Drain must not begin another allocation after renewal has been stopped; an already-open replacement remains usable.

## Proposed implementation contract

Add `SchedulerOptions.unknownRecoveryTimeout`; reuse the repository's duration parsing and finite-number checks. Timestamp every unknown outcome immediately. Arm the watchdog as soon as a preferred-source unknown exists, even with spare capacity or no queued demand; use the oldest relevant original observation plus the configured timeout. Snapshots, ticks, additional unknowns and source Closing must not restart the same blocked episode. End the capacity episode when authoritative keyed evidence removes its uncertainty, or a Ready live preferred replacement can accept work, even if the old uncertain source is still present. Keep old fate/timestamps separately. A later drain blocked by retained uncertainty uses its original deadline and fails immediately if overdue; entering drain cannot reset its age. A distinct later loss after recovery gets its own observation-based deadline. The decision appendix spells out edge cases, including multiple keys and filler.

Use one scoped watchdog and retained episode state, independent of the actor's command waits. At expiry recheck only retained episode identity and deadline, without awaiting Engine state, command permits or cleanup, before calling the shared terminalization owner from 001. Canceling/replacing a timer is not enough to fence its already-awakened continuation. No detached fibers, wall-clock arithmetic or one unbounded fiber per item. Replace scalar uncertain filler with a source/index ledger capped at 4096 entries; fail with the existing Overflow reason before another dispatch would exceed it, never evict unresolved identities. Defects remain causes; an elapsed recovery policy produces a typed Timeout through the existing error algebra, with an SDK-authored message and operation context. Do not encode a new failure category as an unexplained boolean or parse provider text.

This means a generic Engine with no retirement/recovery can fail explicitly at the bound; it cannot safely promise continuing playback. The canonical Renewal integration test must establish its normal replacement success separately. The report closes only when the default bound and honest outcomes are demonstrated, not when an opt-in path passes.

## Scope and repository/Effect standards

Implementation scope: `packages/client/src/orchestration/{scheduler,scheduler-policy}.ts`; `packages/client/test/orchestration/{SchedulerFates,SchedulerCore,SchedulerRenewal,SchedulerUnknownRecovery}.test.ts`; `packages/client/test/PublicTypes.test.ts`; `scripts/pack/node-consumer.mts`; package README, orchestration test README and changelog. `renewal.ts`, `source-slot.ts` and their existing tests may change **only for a reproduced defect in the existing recovery path**, recorded before editing; adding a recovery API is outside this plan. Reuse the existing Timeout reason; if its schema cannot express the decided semantics, review `errors.ts` and error codec tests as a deliberate scope change.

Read `CONTRIBUTING.md`, the decision appendix and installed Effect rc.117 source. Use Bun, no `any`, relative feature-local imports, comments explaining evidence/ownership and existing `runClock`/`SourceFixture` tests under Node and Bun. The requested `docs/Codex/comments.md` and `file-structure.md` are absent at the baseline; read them if restored. Use current `Context.Service` and `Scope.provide`; keep the patched Effect compiler and diagnostics enabled. Narrow uninterruptible masks protect finite local state; they do not serialize concurrent workers or bound external calls. Use actual monotonic Effect Clock timing, typed operational errors, original defects, owned fibers and the existing span boundaries with `captureStackTrace: false`.

## Execution and verification

1. Branch `codex/bound-unknown-admission` from current main with 001. Reconcile `git diff c47e50c..HEAD -- packages/client/src/orchestration packages/client/test/orchestration`. **Baseline from `packages/client`:** `bun run test test/orchestration/SchedulerFates.test.ts test/orchestration/SchedulerRenewal.test.ts --testTimeout=6000` → existing source-fence/no-replay tests pass.

2. Add characterization using the real Renewal and a Source fixture that invokes commit/result hooks. Lose L0's reply; submit L1 two seconds later at default capacity, with a source lifetime beyond its deadline. Prove whether existing recovery replaces A with B and dispatches L1 without replaying L0. Characterization may already pass: do not demand a failing test for a defect that this path does not have. Add canonical replacement failure, stalled `source.close`, and recovery-fiber defect cases; record precisely which owner surfaces each failure. **Verify:** the focused SchedulerRenewal suite establishes the hook-to-recovery trace, its allocation count, routing fence and cleanup obligations.

3. Add failing deadline regressions using the generic scripted Engine and a stalled canonical recovery. Use a small configured timeout under `runClock`; at one tick before expiry the reservation remains, and at expiry scheduler notification observes all 001 settlement invariants. Include a drain stuck in `stopRenewal` and a fixture that releases stalled cleanup after asserting the scheduler result, so the test itself can finish its owned scope. **Verify from `packages/client`:** `bun run test test/orchestration/SchedulerUnknownRecovery.test.ts --testTimeout=6000` if the dedicated packet test file is used. Old behavior fails for a still-pending scheduler/handle/control result, not because the test accidentally waited for an impossible cleanup bound.

4. Implement the option, bounded episode state and independently supervised timer. Reuse 001's private scope and first-terminal-claim rule. Preserve capacity, no-replay, keyed late observations and original started/first-decisive values. Ordinary scope close, source failure and timer expiry must compete through the same terminal owner. **Verify from `packages/client`:** `node node_modules/vitest/vitest.mjs run test/orchestration/SchedulerFates.test.ts test/orchestration/SchedulerCore.test.ts test/orchestration/SchedulerRenewal.test.ts test/orchestration/SchedulerUnknownRecovery.test.ts` and the same command with `bun --bun` replacing `node` → exit 0 on both runtimes; omit the dedicated filename only if its cases were kept in the existing suites under the agreed ownership schedule.

5. Cover: snapshot churn cannot extend the deadline; late Started removes the correct reservation without replay; local Closing without Ready replacement keeps the episode; Ready B ends A's capacity episode even while A is still present; late drain reuses A's original unknown deadline; stale timer A cannot fail recovered B; two unknown keys/filler cannot lose the oldest bound; later independent loss gets a new deadline; generic unsupported recovery fails truthfully; both drain modes admit no new allocation; caller-wait cancellation does not cancel owned recovery. Assert exact source/episode identity and finite retained state, not only elapsed completion.

6. Document default 60 seconds, valid overrides, terminal Unknown and the distinction between scheduler failure and cleanup completion. Update public type/installed-package consumers for the option. **Root gates:** `bun run verify --profile portable` and `PACK_INSTALLER=bun bun --no-env-file scripts/pack.ts --portable` → exit 0; retain full installed-package/native CI on the final integrated revision.

## Completion, limits and maintenance

Required root checks are `bun run build`, `bun run typecheck`, `bun run lint`; portable verify runs them in the needed order. Use `bun install --frozen-lockfile` only if needed. Conventional commits omit AI attribution; do not push/publish without instructions.

Done means the generic default is bounded, canonical recovery has a controlled integration trace, uncertainty is never replayed or converted to fabricated rejection, drain waiters settle even when the actor is blocked, and stalled cleanup remains honestly owned/reported. It does **not** mean every Engine can recover uninterrupted or every cleanup is bounded. Keep any reproduced lower-owner cleanup defect explicitly tracked. A recovery-fiber defect without an Engine terminal Cause is a separate all-cause supervision follow-up; this plan characterizes it and supplies the scheduler deadline fallback. Do not wrap a defect in a generic recoverable ReactorError to fit existing media-state unions.

Stop if implementation relies on undocumented provider ordering, proposes to release an unknown reservation on time alone, introduces paid allocation policy beyond existing Renewal, or leaves the timeout waiting behind a blocked actor. A watchdog defect cannot be swallowed. Do not run paid experiments during implementation; provider qualification belongs to 003 after the integrated offline gates.
