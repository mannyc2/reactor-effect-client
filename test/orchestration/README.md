# Orchestration contract tests

Run the focused suite from the SDK root:

```sh
bun test test/orchestration --timeout 6000
node node_modules/typescript/bin/tsc -p test/orchestration/tsconfig.json
node_modules/.bin/oxlint -c .oxlintrc.json --deny-warnings test/orchestration
node test/orchestration/coverage-ledger.mjs
```

`SourceFixture.ts` implements a physical source without routing, renewal, or a provider reducer. Tests control its preparation, commitment, outcome, media generation, and lifecycle independently. `H3Source.test.ts` uses the existing canonical Session fixture with the real H3 provider and orchestration projection. `Simulation.test.ts` exercises the production simulation source and renderer hooks.

`Routing.test.ts` checks joint sequence/anchor/continuation ownership and exact positions. `OwnershipAnchor.test.ts` checks `sameSessionAs` for ready clips, conflicts, and commit-time revalidation; `H3Source.test.ts` verifies that this scheduling field never enters a provider command. `Renewal.test.ts` checks admission, commit-time revalidation, cancellation, source selection, sequence accounting, and joined cleanup. `Media.test.ts` checks frame-count handoff, unverified audio, queued media bounds, generation pressure, and failure publication. `Expiry.test.ts` keeps a committed member pending across its source deadline. `LifetimeBoundary.test.ts` checks stalled recovery at expiry and accumulated or unknown drop evidence across receiver generations. `RequestQueries.test.ts` checks detached input and factual playback horizons.

## Renewal decisions and ownership

`src/orchestration/renewal-state.ts` contains pure source-phase transitions and preparation, expiry, and handoff decisions. `source-slot.ts` owns a physical source's receiver scopes, registered committed submissions, acceptance accounting, and retirement. It closes the remote lease before waiting within a separate local cleanup budget; the original `Submission` still owns dispatch and its result. `media-buffer.ts` owns output admission, counters, and interrupt-safe single-item consumption. The existing limits remain 96 video frames and 192,000 interleaved audio samples. `renewal.ts` composes these responsibilities and retains admission serialization. A pending replacement is explicitly absent, opening, or ready.

`RenewalState.test.ts` checks boundary decisions and enumerates all 3,906 event traces through length five, reporting a shortest failing trace. `RenewalTrace.test.ts` exercises the real orchestration implementation through all six orderings of caller cancellation, remote reply, and expiry, for both acceptance and rejection. Its oracle depends on external event order, not the implementation's private phase. `MediaBuffer.test.ts` checks interrupted readers, retained output, and exact admission accounting. These tests use no real-time polling or provider sessions.

`Signals.ts` supplies retained fixture notifications with cancellation-safe waiters. `SourceFixture.lifecycle` exposes dispatch, known-result, accounting, reconnect, close, and finalization barriers; `RenewalFixture.awaitRenewal` awaits an observed policy event. Expiry and lifetime tests use these barriers with TestClock. The remaining general `until` helpers support older media-observation tests; they are not required by the new lifecycle traces. Late-activation regressions verify that completing reconnect, planned-switch autoplay, or replacement autoplay after explicit close cannot restore Ready. All three activation paths use one guarded Ready-state update.

The [coverage ledger](./coverage-ledger.md) maps every assertion from the four removed legacy suites. [The inventory](./legacy-assertions.json) preserves their exact expressions and locations against the frozen Git baseline. The generator rejects unaccounted assertions, stale replacement targets, or changed baseline totals. No test is skipped to complete the migration.

The provider and canonical ownership suites used by the ledger can be checked separately:

```sh
bun test test/h3/Provider.test.ts test/h3/ProviderReferences.test.ts test/Submission.test.ts test/Sequence.test.ts --timeout 6000
```

These tests use local fixtures and test clocks. They do not establish hosted generation or transport interoperability. The September 22 final orchestration run passed 114 tests with 664 assertions, including source affinity and acceptance accounting after remote expiry. `validation.bun.log` records that run; the earlier 14-failure result is preserved in `.check/canonical-closure/orchestration-before.log`. The complete portable profile passed 526 tests with 2,490 assertions in `.check/canonical-closure/verify-portable-qualified.log`. Earlier passing profiles remain alongside it. `validation.authority.log` records the separate provider, reference, submission, and sequence checks.
