# Qualification stream B handoff

Status: implementation and all final combined local gates pass, including all 26 renewal rehearsals, native checks, real Chrome/native integration, release tools and full isolated-package validation. Final npm/OS CI, Linux execution and paid hosted qualification remain unrun. No paid sessions, push, merge, CI dispatch or publication performed.

## Scope and commits

- Audited base: c47e50c784f6889b542cf8ee316067b688ca4737.
- 003-E SDK handoff evidence: c34e95b8874d7cffaa2c400173e15c417289369a. D integrated this independently; do not duplicate its cherry-pick.
- 003-H legacy harness: f5d9162. Hosted files/tests only.
- 003-C actual continuous API: 6dc2c74581c9eb62db05a24399ace12051d91f8f. Hosted files/tests only.
- Formatting-only successor: fc1d742.
- Test-only exact continuous-refusal assertion: bc3fd98eb3a5bcae526824550a0483433682b8d7.
- API dependencies consumed from C: 763052c, 5cb2be8, 9afac85.
- Scheduler dependencies consumed from A: 6bf95b2, efea67a, 9e02cfd, 7d3d129.
- D owns shared consumers, root/package docs, runner integration and final combined verification. Shared renewal SDK files were handed to C after 003-E.

## Evidence semantics

`Switched.handoff` is optional structurally and populated by the implementation, captured immutably before retirement waits. Its retiring `sessionId` and aggregate `tail` remain unchanged. Final-clip counts are local decoded observations; audio and presented/encoded output remain unverified.

The single `scheduler-renewal` CLI discriminator uses public openH3, owner, scheduler and accepted drain. Paid runs select actual makeContinuous; offline `--constructor=legacy` selects make. The scenario has two independent exact 50-second grants, a 110-second token duration, maxSessions 2, two grant-consuming open attempts, lead 40 seconds, grace 250 ms and two keyed five-second clips. Continuous retention settings are 1 successful and 2 unresolved records. Both billed minutes are reserved before either token; ceilings remain $1.50/check and $3.75/ledger.

The new versioned subtree uses a single Effect monotonic origin. WeakMap taps tag unchanged physical frame objects; existing logical media recorders reset accounting at source/generation boundaries. Untagged frames or mismatched source/logical arrivals fail. Original per-source canonical cleanup reports are retained independently of continuous compaction. Nominal summary requires two retirements, one retained complete owned termination, one omitted complete owned termination, no incomplete row and no exhaustion. Unknown submissions remain incomplete. Historical committed ledgers still decode and retain their reservations.

Setup is bounded to 20 seconds; work shares 40 seconds from first allocation; cleanup observation is 20 seconds. Durable failed checkpoints precede finalizers. The CLI has no supervisor; a stalled uninterruptible close still requires an external emergency owner. The rehearsal parent supplies that owner. An interrupted/failed paid run would retain its reservation.

## Completed verification

Runtime: Bun 1.4.2 at /Users/chriscarroll/.local/share/vite-plus/package_manager/bun/1.4.2/bun/bin/bun; patched Effect TypeScript compiler from frozen workspace.

- 003-E focused Node 44 / Bun 45 cases passed; root build, typecheck, lint passed.
- First full 003-E portable attempt failed an expect.any lint issue, fixed before commit. Later portable attempt passed build/typecheck/lint/architecture/examples and 758/761 client Node tests, but hit three unchanged SchedulerRenewal accepted-drain 5-second timeouts under concurrent heavy work. This is not a full portable pass. Log: /tmp/reactor-003e-portable.log. Heavy gates were then serialized; A's later final portable gate passed after its scheduler changes.
- Legacy public-renewal real-CLI matrix: 13/13, 114 assertions, 175.85 seconds wall. Log: /tmp/reactor-003h-matrix.log. Includes nominal, first/second allocation refusal, connect failure, second-token overgrant, slow/ignored DELETE, lost second enqueue reply, missing first/second video, stalled second build, allocation interruption and externally terminated stalled close.
- Final focused hosted evidence/gates/collect tests: 45/45, 191 assertions. Log: /tmp/reactor-003c-pure.log.
- Root build/typecheck/lint/format and architecture checks pass. Logs: /tmp/reactor-003c-{build,typecheck,lint,format,architecture}.log.
- Final matrix command: bun --no-env-file test integration/test/hosted-rehearsal.test.ts -t 'public renewal'. 26 tests preserve every case for both constructors; each child retains its 150-second emergency bound and per-case 180-second test deadline. Result: 25 passed / 1 failed assertion in 363.60 seconds, /tmp/reactor-003c-matrix.log. Continuous refusal correctly exhausted reserved cleanup capacity after two attempts; the test had expected legacy's third-open guard. The successor test asserts exact continuous exhaustion and incomplete unknown-allocation evidence while preserving legacy's three attempts. Narrow rerun: 1 pass, 13 assertions, 16.22 seconds, /tmp/reactor-003c-refusal-rerun.log. Integration typecheck and scoped lint pass after correction. No runtime change.

The real summarize CLI also decoded and rendered both nominal rehearsal ledgers successfully. Local rendered artifact: .check/qualification-stream-b/rehearsal-summary.md. These runs used the loopback twin at fc1d742 and are not hosted qualification.

## Integration coordination and hosted gate (prior to final verification)

The full 001–005 combined portable/native/local-WebRTC and package verification is owned by D. The new matrix alone is measured 363.60 seconds; D has a narrow root-Bun aggregate runner allowance of 600 seconds while retaining scenario/CLI/Vitest limits.

Only after final combined gates pass can a paid checklist be complete. It must bind the tested commit/tree, package archives and hashes, staged native identity, exact constructor settings, a fresh ledger and prior reservations, fresh provider pricing and both caps, commands, the 20/40/20-second timing design, a named maintainer/CI emergency intervention owner and deadline, required evidence and stop rules. No paid authorization is inferred from implementation or rehearsal. Resume remains a separate unrun hosted check. The committed 0.3.1 audio pass is historical evidence, not qualification of these new bytes.


## Combined-gate fixture shutdown correction

D's second combined portable run at 9dc0baf passed all 849 client / 58 browser tests on both runtimes and all 26 renewal rehearsals, then the pre-existing twin image test timed out. The root run finished 170/171 in 494.90 seconds; this was not an aggregate runner timeout. Full gate log: /tmp/reactor-d-final-portable-r2.log.

The unchanged image test reproduced at repetition 32. Phase diagnostics reproduced at repetition 6 and showed all upload/submit/owner cleanup completed by 256 ms, with the wait in twin-close. Server diagnostics reproduced again with listening=false, getConnections=1 and no mapped held response. A deterministic physical socket regression sends Expect:100-continue headers, withholds the body, then closes the twin: the original server failed its existing five-second test deadline on the first run.

Branch codex/diagnose-hosted-upload is based on D's exact 9dc0baf. The narrow fix explicitly tracks accepted sockets, destroys owned sockets during shutdown and late-delivered connections after shutdown begins, and still awaits the real server.close callback. Only integration/hosted/twin/server.ts and integration/test/hosted-twin.test.ts change. No SDK change or deadline increase; phase diagnostics contain only names/times and clean up their timer.

Verification: original image-only 100 repetitions pass after fix; unfinished-request regression plus image 100 repetitions each (200 pass, 500 assertions) pass; full twin file 18/18 (224 assertions, 37.70s) pass; root build/typecheck/lint and scoped format pass. Evidence: /tmp/reactor-b-socket-before.log, /tmp/reactor-b-socket-regressions.log, /tmp/reactor-b-twin-fixed.log. D owns the final combined rerun and remaining native/package checks.


## Final combined portable result

After the fixture socket fix and A's measured scheduler-source projection optimization, the final combined portable gate passed: 850 client tests and 58 browser tests on Node and Bun, plus all 172 root tests. All 26 renewal rehearsals passed. Root discovery took approximately 466 seconds inside the unchanged scenario limits and the root-only 600-second aggregate budget. Earlier failures remain recorded; the later native/package results are recorded below.


## Final combined local completion

D verified source commit `a68d072640a700b1763e3e3ed8910a18031f848a`, tree `dcfed85061da895e6ad6324863a42d99e67480a7`. Its final branch head is `dec33ea21cd8c00311d3cb100cbd5a7184420c05`, tree `b7f46a46aa73f38809fbe3f8d0c582a2e8f088da`; the difference is four plans-only files. B's twin fix is `4934b6e08d58d9b61021cfe2307b43bbb97e7c32`, integrated by D as `65ff491`.

All local gates passed: portable 850 client and 58 browser tests on each Node/Bun runtime plus 172 root tests including all 26 renewal cases; 85 Rust tests and format/clippy/rustdoc; native Node 37 and Bun 29 passes with 1/9 existing platform skips; real local Chrome/native integration; 71 release-tool tests; full Bun package validation with exact Effect/platform rc.117 and verified Darwin/Linux native bytes. Build, typecheck and lint are included in portable. The full pack's client/browser/native archive hashes and original native provenance were independently rehashed by B and match D's identity.

Full combined report: [D's final integration handoff](/Users/chriscarroll/.codex/worktrees/0d44/reactor-effect-client/plans/final-integration-handoff.md). Logs and machine-readable receipt: `/Users/chriscarroll/.codex/worktrees/0d44/reactor-effect-client/.check/final-integration/`. B's compact copy of exact-byte metadata is [final-local-identity.json](final-local-identity.json).

[Hosted readiness candidate](hosted-readiness.md) binds the bytes, proposed command, limits, required evidence and stop rules. It is held for final npm-installer/OS/Node CI and Linux execution, a named emergency operator, fresh pricing/ledger review and explicit spend authorization. No authorization request or paid run has been made. Resume remains separately unrun; historical audio qualification does not qualify these bytes. The original planning checkout/specifications were not modified.
