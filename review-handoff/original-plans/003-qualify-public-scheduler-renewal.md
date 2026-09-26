# Plan 003: Qualify the scheduler through its public renewal path

Planned at `c47e50c784f6889b542cf8ee316067b688ca4737`, 2026-09-26. P2; M; medium fix risk; high confidence. Evidence and harness development proceed independently of 001/002; verify their combined behavior at integration. Final integrated qualification follows 004 and 005; do not pay for intermediate evidence by default. The [qualification contract](003-qualification-contract.md) gives the concrete scenario, proposed bounds and schema. Existing umbrella: [#31](https://github.com/mannyc2/reactor-effect-client/issues/31).

## Parallel execution

Split this plan into **003-E** (SDK handoff evidence), **003-H** (legacy offline harness), and **003-C** (continuous integration). Follow the [parallel execution guide](PARALLEL-EXECUTION.md). 003-E starts alongside 001/004-R/005 and hands shared renewal files to 004-L as soon as its small evidence slice passes. 003-H then works only in hosted harness/tests while 004-L proceeds; schema/budget fixtures can start even earlier. 003-C consumes the real 004 API. Scheduler fixes are a final combined verification dependency, not a prerequisite for independent evidence/harness development. Keep partial work explicitly unqualified.

## Why and current state

The existing two-session hosted check collects valuable provider facts, but bypasses the behavior whose qualification is outstanding. `integration/hosted/qualify.ts:969` opens the second source directly, then lines 1036–1038 execute:

```ts
yield* recorded(run, provider.stop);
const oldStopMs = since(run.origin);
yield* recorded(run, newProvider.setAutoplay(true));
```

That is a manual provider transition, not `Orchestration.make` plus `makeScheduler`. Preserve it as provider calibration; do not label its result a scheduler-renewal pass.

Production renewal calls `canHandoff` using `current.finalClip()` at `renewal.ts:1101–1107`, then emits `Switched` with an aggregate tail at line 1123. Public `types.ts:253–258` has no separate switch reason, final-clip counts or elapsed grace. `source-slot.ts:314–317` already keeps final-clip received counts independently of whole-session counts. Add honest evidence rather than changing the handoff policy.

Per-frame and per-audio-block arrival arrays already exist in `collect.ts:221`, `:275` and `evidence.ts:45`, `:65`. The existing rehearsal in `integration/test/hosted-rehearsal.test.ts:17` launches the real qualification CLI in `rehearse` mode against loopback with a fresh ledger. Reuse it; do not create another paid harness.

## Scope and boundaries

Allowed implementation: `packages/client/src/orchestration/{types,source-slot,renewal,renewal-state}.ts`; `packages/client/test/orchestration/{Media,SchedulerRenewal,RenewalState}.test.ts`; `packages/client/test/orchestration/SourceFixture.ts` only for an indispensable fixture-only extension coordinated under the parallel ownership rule; `packages/client/test/PublicTypes.test.ts` and `scripts/pack/node-consumer.mts` if public type assertions need updating; `integration/hosted/{qualify,evidence,gates,collect,report}.ts`; existing `integration/hosted/twin/` files only for required scenario behavior; `integration/test/hosted-{rehearsal,evidence,gates,collect,twin}.test.ts`; a focused `integration/test/hosted-renewal.test.ts` if needed; root/package/hosted READMEs, changelog and this plan/index. New persisted fixture/evidence files belong under `integration/hosted/evidence/<tested-version>/` only after a real run, with its exact identity.

No change to native ABI, rendering, billing claims, show policy or scheduler decisions. No credential values, media samples or provider error text in evidence. Decoded arrivals do not establish encoded/viewer presentation. Preserve historical evidence schemas through optional additions or explicit versioning.

Use Bun and no `any`; follow the existing feature directories and imports. Read `CONTRIBUTING.md`, `integration/hosted/README.md` and the installed Effect source for APIs touched. The requested `docs/Codex` files are absent at this SHA; read them if present later.

## Concrete proposed contract

- Add check **`scheduler-renewal`**; retain `scheduler` as provider calibration. Preserve old v1 ledgers and add a versioned optional evidence subtree with strict complete-pass criteria. Initial/failed records remain encodable at every milestone.
- Add optional `Switched.handoff` for structural compatibility, always populated by the new implementation. Record the replacement ID, final observed clip/counts, count-complete versus grace-elapsed/no-observed-start decision, grace origin and monotonic elapsed wait. Capture the facts before asynchronous retirement. Existing `Switched.sessionId` remains the retiring source, and aggregate `tail` stays unchanged.
- Reuse existing collectors with an injected Effect monotonic elapsed clock for the new subtree. Do not subtract legacy wall-clock span/event offsets from those measurements. A harness-only WeakMap tags unchanged source frame objects before logical forwarding; test that identity seam. Reading current media state at dequeue is not source attribution.
- Use the public constructor, `openH3`, scheduler and accepted drain. Initial offline implementation covers `make`; after 004, cover both constructors and select `makeContinuous` for final hosted qualification. Configure two successful openings/two total open attempts, 50-second granted source caps, lead 40 seconds, grace 250 ms and two keyed five-second clips. Start drain only after the replacement is prepared and B admitted. The SDK controls autoplay and switching.
- Proposed setup 20 seconds, shared work 40 seconds from first allocation, cleanup observation 20 seconds. These are testable harness policies, not measured provider guarantees or bounds on arbitrary SDK finalizers. Persist incomplete evidence before cleanup; an external emergency stop preserves failure status and ownership uncertainty.
- Reserve both worst-case billed minutes before either grant at the current fetched rate, within existing $1.50/check and $3.75 total-ledger ceilings. These are repository policy constants, not verified current prices. Refuse before allocation when the rate/grants/ledger do not fit. No automatic retry or budget increase.
- Final continuous run uses retained-success limit 1 and unresolved reservation limit 2, exercising one successful compaction at close. Its cleanup subtree discriminates legacy report versus continuous summary; independently retained per-allocation canonical cleanup proves both actual leases terminated. A two-source run does not establish >64-renewal boundedness.

All constants above are **proposed and unmeasured**. The appendix gives the exact milestones, failure rehearsals, cross-field checks and clock/cleanup limits. If a source cap or build cannot fit the scenario, record a failed/unqualified result; do not quietly shorten a claimed provider lifetime or extend the spend.

## Steps and verification

1. Use separate `codex/qualify-switch-evidence` and `codex/hosted-renewal-harness` branches for 003-E/003-H; run `git diff c47e50c..HEAD -- packages/client/src/orchestration integration/hosted integration/test` and reconcile the expected scheduler fixes. Keep the evidence slice and harness independently reviewable; hand off evidence before waiting for the whole harness. **Baseline root command:** `bun --no-env-file test integration/test/hosted-gates.test.ts integration/test/hosted-evidence.test.ts integration/test/hosted-collect.test.ts` → exit 0 after dependencies/build prerequisites.

2. Add a typed switch-evidence field alongside the existing aggregate `tail`: final-clip observed counts, whether eligibility came from count completion or elapsed grace, the observed grace origin (Ended versus idle), and monotonic elapsed wait at the decision. Include an explicit unknown/not-started case rather than fabricated values. Capture evidence when the decision is made, before asynchronous retirement changes state. Do not claim counted frames were displayed or that audio is verified. Test early-session loss, final-clip loss, a last frame inside grace, and missing Ended. **Verify from `packages/client`:** `bun run test test/orchestration/Media.test.ts test/orchestration/RenewalState.test.ts test/orchestration/SchedulerRenewal.test.ts --testTimeout=6000` → existing gap bounds and new evidence cases pass. Run the same files under Node and Bun before merge.

3. Add the `scheduler-renewal` scenario to the existing CLI/rehearsal framework with its explicit discriminator. It first instantiates the legacy public renewing orchestration and scheduler; after 004 lands, parameterize the same internal scenario for continuous mode and add its summary validation. It must use keyed items and real renewal preparation, observe a `Switched`, and let the SDK perform autoplay/retirement. Read logical orchestration media as well as source-attributed evidence where exposed. Require keyed order, a decoded boundary on both sides and accepted-drain behavior. Exact TestClock timing bounds belong in step 2's simulation tests; the loopback twin uses real process timers, so qualify its observed transition with lifecycle barriers and a finite run deadline. Preserve the direct-provider scenario's other seven facts; do not cram all scenarios into a cap they cannot meet. **Verify root:** `bun --no-env-file test integration/test/hosted-rehearsal.test.ts` → loopback only, all scenarios pass; test evidence distinguishes provider calibration from public renewal.

4. Extend budget/evidence gates before adding any live command. The existing two-session check reserves its worst case before tokens are minted. Any added scenario needs its own explicit session count, cap, budget and ledger rules; never spend a previously assumed $1.50 twice. Keep finite waits and finalizers for every allocated source. Add failure rehearsals for acquisition, unknown command outcome, interruption, unavailable replacement and unconfirmed termination. A failed run must retain its evidence and stop; no automatic retry. **Verify:** the root hosted gates/evidence/rehearsal commands above pass with negative cases that refuse insufficient budget before allocation and reject incomplete/ambiguous evidence.

5. Integrate continuous-constructor coverage once 004 lands, then correct current qualification documentation using the already committed 0.3.1 audio summary. Keep resume and new scheduler qualification unqualified until they actually run. Explain that a handful of build samples are observations with a count, not a stable p95 estimate. Preserve old release-time statements as dated history where appropriate. **Verify:** `bun run verify --profile portable` → exit 0; `PACK_INSTALLER=bun bun --no-env-file scripts/pack.ts --portable` → consumers pass. Native CI and full installed-package smoke must pass for the final target revision.

6. After final combined 001–005 offline/package CI, prepare a reviewable hosted-run checklist containing exact tested revision/package hashes, native identity, constructor/retention settings, per-run and total reserved spend, scenario commands, time limits and external emergency intervention owner, required evidence and stop rules. Only then request the maintainer's explicit paid-session authorization. This planning task authorizes no paid run. After authorization, execute each approved scenario once, save schema-valid ledger/summary, and update qualification wording to the actual outcome. Leave unsuccessful or unrun gates visible. Treat resume as a separate outstanding check; do not imply this scheduler run covers it.

## Done criteria

- Root `bun run build`, `bun run typecheck`, `bun run lint` pass (included in portable verify); dependency setup, if needed, uses `bun install --frozen-lockfile`.
- Node and Bun handoff tests and the loopback rehearsal pass; the scenario reaches the public scheduler and renewing orchestration without manual provider stop/start controlling the switch.
- Aggregate tail is preserved; specific final-clip/grace evidence is verified; no duplicate arrival collector is added.
- Evidence schema rejects a purported public-renewal pass lacking `Switched`, item identities/order or termination evidence.
- Mark **implementation/offline verified** separately from **hosted qualified**. The latter requires an authorized real ledger tied to the exact tested bytes, continuous constructor for the combined path, and confirmed canonical cleanup for every allocated lease. Never mark it complete from simulation alone.
- `git diff --name-only` contains only scoped files. Use conventional commits, no AI attribution; do not push or publish without instructions.

## Stop conditions and maintenance

Stop if a meaningful scenario cannot fit its stated server-enforced caps, if the twin cannot establish the targeted event order, if a public evidence field would assert unobserved presentation, or if termination is unconfirmed. Do not raise budgets or repeat live checks to get a pass. Reconcile drift from 002/004 before final qualification.

Every later renewal-policy change must name which evidence it invalidates. Keep the-show's migration/container and encoded-output gates outside this SDK plan; verify those in its own repository before using them to close #31.
