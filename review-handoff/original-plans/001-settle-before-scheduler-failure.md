# Plan 001: Settle handles before publishing scheduler failure

Planned at `c47e50c784f6889b542cf8ee316067b688ca4737`, 2026-09-26. P1; S–M; medium fix risk; high confidence. No dependencies. Existing report: [#36](https://github.com/mannyc2/reactor-effect-client/issues/36). Refined after a separate review of repository standards and the installed official Effect `4.0.0-rc.117` implementation. Ready for regression-first implementation; the fix itself has not been executed or tested.

## Parallel execution

Packet 001 is stream A's first task in the [parallel execution guide](PARALLEL-EXECUTION.md). Start it alongside 003-E, 004-R and 005 in an isolated worktree. Keep scheduler production ownership through the handoff to 002: supply the actual terminal entry point, private-scope order and regression results. Shared docs are applied serially by the integration owner; this does not delay independent code/tests. No dependency on renewal evidence, retention or tooling implementation.

## Intent and current state

`Scheduler.failure` is a natural supervisor boundary: consumers await it and close the owning scope. Every retained item's unresolved handle Deferreds and every actor-owned pending control reply must have a committed terminal result before that notification. Queued public calls not yet processed settle through their existing `closedCall` race when the terminal signal is published. This does not promise that all resumed caller fibers have already finished executing. Currently the notification races the bookkeeping it is meant to announce.

`packages/client/src/orchestration/scheduler.ts:664`:

```ts
const closeActor = (cause: ReactorFailure): Effect.Effect<void> =>
  Effect.gen(function* () {
    if (ended !== undefined) return;
    ended = cause;
    accepting = false;
    yield* Deferred.succeed(stopped, cause);
    for (const item of items.values()) {
```

The remainder settles items, withdrawals and drains and updates `stateRef`. Actor fibers are scope-owned at lines 1815–1817, and the scope finalizer calls `closeActor` again at line 1828. If the first call is interrupted after setting `ended`, the second returns without finishing. The defect path in `supervise` at lines 1796–1813 also publishes `stopped` before item failure propagation. Its `onExit` callback is already uninterruptible, but that does not give it exclusive ownership against another worker or the scope finalizer. Both paths must share the terminal ownership/completion rule while preserving their different error semantics.

`SchedulerFates.test.ts:67` is the controlled-scope test pattern. The test at line 511 creates an Unknown, sends `SessionFailed`, then advances 100 ms before reading the result. It does not test immediate failure-driven closure. Use `runClock`, fixture gates and controlled `Scope`, not a real-time sleep.

## Effect mechanics verified for this plan

General principles agree with Effect's [v4 resource-management guidance](https://effect.website/docs/v4/resource-management/introduction): cleanup runs for success, failure and interruption; `onExit` receives the full exit and protects its cleanup callback. Implementation details below were verified against the installed rc.117 source, which takes precedence over generic cookbook examples or another version's behavior.

Paths in this table are relative to `packages/client/node_modules/effect/src/` after a frozen install. Recheck them if the locked dependency changes.

| Primitive / source | Verified behavior | Consequence for this fix |
| --- | --- | --- |
| `Deferred.ts:1649–1660`; `internal/effect.ts:1163–1167` | Completion keeps the first result and resumes registered waiters; a suspended waiter's continuation can run immediately | Completing `stopped` is an observable scheduling boundary. Settle the owned state before publishing it; do not rely on the next line running first. Keep existing Started/firstDecisive results. |
| `internal/effect.ts:4484–4491` | `uninterruptible` changes interruptibility, not mutual exclusion | Mask only finite local terminal bookkeeping. A terminal claim and shared completion rule must separately prevent competing closers from overwriting or bypassing it. |
| `internal/effect.ts:4137–4162` | `onExit` masks its callback by default and preserves/combines failure causes | Preserve defects and unexpected interruption as causes. Adding another mask to `supervise` alone does not fix ordering or ownership. |
| `internal/effect.ts:5527–5596` | `forkScoped` delegates to scope-owned `forkIn`; scope cleanup interrupts and joins registered workers | Keep structured ownership. Do not use detached forks or close/join the entire worker scope from one of its own workers. |
| `internal/effect.ts:3914–3961`, `:4050` | Scope close marks the scope Closed before finalizers complete. Sequential finalizers run in reverse registration order; parallel scopes execute them concurrently | A second `Scope.close` is not the scheduler's completion barrier. Own the scheduler's cleanup order instead of assuming the caller supplied a sequential scope. |
| `PubSub.ts:995–1003`; `Queue.ts:1193–1229` | In rc.117, publication to a shut-down PubSub returns `false`; queue shutdown discards messages and interrupts pending takes | Retained handles are authoritative even if an as-run subscriber disappears. Settle/fence before teardown so queue interruption cannot win over the intended shutdown cause. Do not copy older-version assumptions about PubSub interruption. |

No new service/layer, STM conversion, dependency change, broad lifecycle abstraction, or public error algebra is needed. Use this repository's actual v4 names (`Context.Service`, `Scope.provide`, `forkIn`/`forkScoped`), rather than migrating code to stale skill examples.

## Concrete ownership and terminalization design

1. Give the scheduler a **private sequential child scope attached to its caller's scope**. Register its existing queue/event teardown finalizer, worker fibers and terminal-settlement finalizer in that child. Use `Scope.fork(parent, "sequential")` and provide that scope to the existing construction body, or explicitly register its operations there. Do not wrap construction in a short-lived `Effect.scoped` that closes before returning the scheduler.
2. Preserve the child cleanup order: terminal settlement first; interrupt/join workers next; queues/PubSub teardown last. Parent-scope parallelism must not reorder those scheduler-owned responsibilities. No new background fiber is required for shutdown.
3. Use one private terminal claim for typed failure/Closed versus abnormal `Cause`, and one joinable local settlement result. Claim the first terminal result synchronously before any publication or Deferred completion; later closers join that settlement instead of returning merely because shutdown started. A canonical `Exit` is a suitable representation because the public `stopped` Deferred succeeds with a `ReactorFailure` on typed shutdown and fails with the original cause on a defect. Keep the representation concrete and private, with no redundant state flags whose combinations can disagree.
4. The winning closer runs finite local settlement uninterruptibly: fence admission; settle item Deferreds and actor-owned controls; publish `state.accepting = false`; publish the terminal `stopped` result last. Handle startup's `initialized` waiter too when failure occurs before `makeScheduler` returns. Queued public calls continue to use `closedCall`; do not wait for consumers to run before completing shutdown.
5. Reuse the same ownership/completion path from `closeActor` and `supervise`. A later scope Closed must not overwrite an already-claimed provider failure or defect. The owner must never await its own settlement; a joiner must never wait for whole-scope closure. Keep command execution/provider waits interruptible and outside terminal settlement.
6. A defect during terminal settlement must not leave its completion latch permanently pending. An exit handler must release/join-fail completion and propagate the cause; it must not turn a partial failure into success or wait recursively on the unfinished latch. Test the normal worker-defect path without introducing a production-only test hook. If this requires a broader recovery framework, stop and narrow the design with review.

The first two points are a local ownership arrangement inside `scheduler.ts`, not a new public scheduler lifetime. They make the invariant independent of the caller's finalizer strategy. The implementation may use a simpler equivalent only if the same concurrent-close and parallel-parent tests establish the ordering and no-self-wait properties.

Do not implement single ownership by passing cleanup to `Deferred.complete`: in rc.117 its pre-check does not exclude another concurrent caller from also starting that cleanup (`Deferred.ts:331–335`). Do not use `completeWith` to store an unevaluated cleanup effect either, because awaiters can execute it separately (`:351–354`). The synchronous claim owns execution; Deferreds retain completed values/exits and let joiners observe them.

## Required outcome matrix

| Trigger / prior item state | Retained handle result | Scheduler/control result |
| --- | --- | --- |
| Typed terminal failure; interim Unknown | Every unresolved `started`, `outcome` and `firstDecisive` resolves to `{ _tag: "Unknown", terminal: true }` | `failure` yields the original failure object; pending controls refuse SessionClosed |
| Typed terminal failure; Accepted, Building or Ready | All unresolved handle effects resolve to `Failed` with Scheduler reason and the original cause | Same original failure; no invented provider rejection or playback |
| Typed terminal failure; previously Started | Existing `started` and `firstDecisive` remain the observed Started; unresolved `outcome` becomes `Failed{Scheduler}` | Do not fabricate Ended, duration or aired seconds |
| Already-terminal item or completed history handle | Every recorded result remains unchanged | Closing twice cannot rewrite evidence |
| Ordinary owner-scope close | Apply the same phase rules using the existing Closed failure | Pending/subsequent public commands refuse SessionClosed |
| Unexpected worker defect, or worker interruption before shutdown has been claimed | Unresolved waits fail with the original `Cause`; completed values remain unchanged | `Effect.exit(scheduler.failure)` exposes that cause, not a fabricated ReactorError or success |
| Worker interruption caused by the scheduler's orderly scope teardown | No new terminal winner; prior settlement remains authoritative | Scope closes after workers join |
| Caller cancels only its submit/withdraw/drain wait | Do not terminalize the whole scheduler or cancel library-owned work solely for that reason | Preserve existing owned-command/cancelable-wait contract |

The typed row describes a value in the success channel of `Scheduler.failure`, despite the property's name. Defects/interruption are observed through its `Exit/Cause`. The regression supervisor must capture `Effect.exit(scheduler.failure)` before closing the owner, or a defect can kill the supervisor before it exercises the race.

## Scope and conventions

Modify only `packages/client/src/orchestration/scheduler.ts`, `packages/client/test/orchestration/SchedulerFates.test.ts`, `packages/client/test/orchestration/README.md`, `packages/client/README.md`, `CHANGELOG.md` and this plan/index. No policy, renewal, provider, host, dependency or public union redesign.

Use Bun and no `any`. Read `CONTRIBUTING.md`; comments explain why ordering or masking is needed. The user-referenced `docs/Codex/comments.md` and `file-structure.md` are absent at this SHA; read them if present in the execution checkout. Existing narrow ownership commits in `Submission.ts:51–78` and queue accounting in `observation.ts:93–99` illustrate the repository's mask/restore discipline. Do not copy unrelated remote cleanup into this local settlement.

Repository standards to preserve:

- `CONTRIBUTING.md:53`: concrete modules; typed operational failures; defects remain defects. No broad `ignore`, `orDie`, generic ReactorError conversion or unsafe assertion just to satisfy types.
- `CONTRIBUTING.md:78`: existing spans stay at their actual operation boundaries, with `captureStackTrace: false` and identity/outcome only. Do not add spans to each item settlement, tick or observation. Preserve current caller/owned-work tracing rather than mechanically adding `Effect.fn` everywhere.
- `CONTRIBUTING.md:84–86`: Vitest plus the existing `runClock` fixture, portable under Node and Bun. TestClock controls time, not arbitrary fiber interleaving; use gates/controlled scheduling for this race.
- `tsconfig.base.json:82,89`: preserve Effect diagnostics and severity; no blanket suppressions, unsafe Effect casts or configuration weakening. Verify the actual compiler is the patched `+effect-tsgo` version before accepting typecheck evidence.

## Execution

1. Start an isolated branch `codex/fix-scheduler-settlement` from current remote main. Run `git diff c47e50c..HEAD -- packages/client/src/orchestration/scheduler.ts packages/client/test/orchestration/SchedulerFates.test.ts`; reconcile intervening changes before applying this plan. Use a conventional commit such as `fix(orchestration): settle items before scheduler failure`. Do not push or publish without an instruction to do so.

2. Add a regression fixture with a separate owner scope and a supervisor that awaits the exit of `scheduler.failure` and immediately closes that scope, without advancing time between those operations. Supervisor and retained-handle observers live in a surviving outer scope. Stage items across Unknown, Accepted, Building, Ready and Started using controlled gates (separate scenarios are preferable to an impossible single queue state). Keep an already-terminal handle too. Include pending withdrawal/drain and queued submit calls. Test both `Scope.make()` and `Scope.make("parallel")` as parent scopes. **Verify from `packages/client`:** `bun run test test/orchestration/SchedulerFates.test.ts --testTimeout=6000`. The new regression must fail against the old implementation specifically because a waiter remains pending, a cause changes, or accepting state is published too late; existing unrelated tests must not be broken by the fixture.

   If immediate continuation alone does not reliably expose the old race, configure a test-local small `Scheduler.MaxOpsBeforeYield` from the installed rc.117 source (`effect/src/Scheduler.ts:279`) or a controlled Scheduler dispatcher. Do not add production hooks, sleeps or stress-count retries as substitutes for an ordered trace. The existing test helper `settled` at `SchedulerFates.test.ts:57–62` treats a failed completed fiber as "waiting"; use direct `Exit`/`Cause` assertions for defect/interruption cases, and bound the test from outside the closing scope.

3. Implement the private scope and terminalization design above within `scheduler.ts`. Preserve every row of the outcome matrix. Review finalizer registration and startup-failure cleanup together, rather than merely moving the `Deferred.succeed` line. Ensure `CommandDone` arriving during/after closure only does permitted accounting and cannot revive admission or overwrite terminal evidence. **Verify:** rerun the focused command; all pass without a compensating sleep. A deliberately stalled provider command must still be interruptible during owner close; local settlement must not join it before publishing the item results.

4. Cover terminal observation failure, ordinary close, repeated/concurrent close, external interruption during local settlement, worker defect, caller-wait cancellation, startup failure and a late command result. After terminal notification, `state.accepting` must already be false and every returned handle must have its matrix result. Capture failures as Exits; assert the original cause. Verify scope teardown completes and no subsequent command is dispatched. Event delivery to a subscriber canceled by its own scope is not guaranteed—test the retained handles as the durable in-process contract. **Verify on both runtimes from `packages/client`:** `node node_modules/vitest/vitest.mjs run test/orchestration/SchedulerFates.test.ts` and `bun --bun node_modules/vitest/vitest.mjs run test/orchestration/SchedulerFates.test.ts`; both exit 0. Confirm the primary regression fails on the pre-fix scheduler under both runtimes.

5. Document the completed-settlement meaning of `failure` and the regression. Run final root gates below. Update the index with implementing commit, tests and issue linkage.

## Commands and done criteria

If dependencies are absent in the implementation checkout, `bun install --frozen-lockfile` must exit 0. Build precedes typecheck because workspaces resolve built declarations.

- `bun ./node_modules/typescript/bin/tsc --version` → version includes the `+effect-tsgo` build marker (locally verified: `7.0.2+effect-tsgo.0.45.0`). If not, repair the existing prepare step with `bun run prepare`, then check again. A stock-compiler pass is insufficient and is not a reason to relax diagnostics.
- `bun run build` → exit 0.
- `bun run typecheck` → exit 0, including Effect diagnostics.
- `bun run lint` → exit 0.
- `bun run verify --profile portable` → all listed checks pass; it includes the preceding build/lint/typecheck, so use it as the final combined gate rather than repeating them needlessly.
- `PACK_INSTALLER=bun bun --no-env-file scripts/pack.ts --portable` → isolated client/browser consumers pass. Full pack and native jobs remain CI gates.
- `git diff --name-only` → only scoped files; no temporary fixtures, credentials, dependency changes or generated source.
- New deterministic failure-driven-close cases pass on Node and Bun and fail on the pre-fix scheduler; no arbitrary delay was added.

## Stop conditions and maintenance

Stop and report if settlement needs an unbounded wait, a worker/scope self-join, new provider behavior, or mutation outside scope; if the regression cannot fail on the original source; or if a gate fails twice without an understood local cause. Reconcile source drift rather than applying stale excerpts. This plan does not require paid tests.

Review future handle fields and new control queues against this shutdown invariant. A new waiter must be included before terminal failure publication. Keep errors typed and defects unmodified; do not convert interruption into a successful cleanup claim.
