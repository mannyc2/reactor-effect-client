# Public scheduler-renewal hosted readiness candidate

Status: all combined local gates passed. This is a reviewable candidate, held for missing CI/platform gates and operator details. No paid authorization is requested or implied; no paid session was created.

## Exact candidate

- Tested source: `a68d072640a700b1763e3e3ed8910a18031f848a`; tree `dcfed85061da895e6ad6324863a42d99e67480a7`.
- Final integration branch: `codex/integrate-orchestration-packets`, head `dec33ea21cd8c00311d3cb100cbd5a7184420c05`, tree `b7f46a46aa73f38809fbe3f8d0c582a2e8f088da`. Its four-file difference from the tested revision is confined to `plans/` documentation.
- Checkout: `/Users/chriscarroll/.codex/worktrees/0d44/reactor-effect-client`. Future execution must revalidate this checkout and loaded built package files; a changed checkout or repack requires a new identity and applicable gates.
- Runtime: Bun 1.4.2; exact Effect, `@effect/platform-node` and `@effect/platform-node-shared` 4.0.0-rc.117. Frozen lock SHA-256: `69e84a1e6327962dd318a5067a0280b9f1ec2e30f2bdbcc0853ea86674096819`.
- Final package identity: `/Users/chriscarroll/.codex/worktrees/0d44/reactor-effect-client/.check/pack-pcZuov/package-identity.json`; SHA-256 `10f3adedf915f8876257132a155b9d447cdacf3862c55653b59b0a80f3cd8040`.

All archives below are in that identity's directory. B independently rehashed each archive and staged native binary after D's final handoff; every hash matched.

| Archive | SHA-256 |
| --- | --- |
| reactor-effect-client-0.5.0.tgz | `4bacfdf112fa88735d1a9340af9ccf1ce43ca9d9f3dbeca2cc660b708d64c4c5` |
| reactor-effect-browser-0.5.0.tgz | `252fbf4d0c98c1e9bde2d359812c88bcba632f3ae0f2d0bf81c219ffcba00ce8` |
| reactor-effect-native-0.5.0.tgz | `c368603de20b1f5c1c978e7098d98d1132c29be11b28431728b02eb84129da39` |

Native source SHA-256: `6b4ad6bf5c710345e46b3b375648b7981679a8570e2bf0b30cd073fccd984125`; ABI 4, libwebrtc `webrtc-7907-a5ddff60-p9`. The staged Darwin-arm64 dylib hash is `54312e0a85ea827d7a6af239034a79fd010f686cd2a053a4cd61cbc0aff350b6`; Linux-x64 so hash is `506f426b755f2baf52036555cbd07532a902bbf4806c9d89695cc5407b98288e`. Both were reused from baseline CI run 36223885202 after source/sidecar/embedded-identity verification. Local execution used Darwin; verified Linux bytes do not establish execution on this combined revision.

The command candidate uses the reviewed checkout's public built package exports, with their contents bound to the full pack identity. Archive installation/consumer checks are separate completed evidence. Do not silently substitute a published version or rebuild different package bytes for the hosted run.

## Passed and held prerequisites

- [x] Portable gate: build/typecheck/lint, format/generation/architecture/examples, 850 client and 58 browser tests on each Node/Bun runtime, 172 root tests including all 26 legacy/continuous renewal rehearsals. Root 465.77 seconds, whole profile 604.23 seconds.
- [x] Rust format, 85 tests, clippy, rustdoc; native Node 37 passed/1 existing platform skip and Bun 29 passed/9 existing platform skips.
- [x] Real local Chrome/native integration, decoded media, attribution, failure cleanup and joined close.
- [x] 71 release-tool tests and full Bun package validation with both native platforms and strict frozen-stack consumers.
- [ ] Final npm-installer CI, OS/Node matrix and Linux native execution on the combined revision. No push or CI dispatch was authorized or performed.
- [ ] Named operator and emergency intervention owner, actual reachable process/supervisor, approved deadline and network description. These fields remain unfilled; a candidate deadline alone is not supervision.
- [ ] Revalidated exact checkout/package/native identities; fresh pricing, ledger reservations and preflight result recorded immediately before any proposed paid run.
- [ ] Explicit maintainer authorization for one continuous `scheduler-renewal` attempt with its exact bytes, command and budget after the preceding gates pass.

Logs and receipt: `/Users/chriscarroll/.codex/worktrees/0d44/reactor-effect-client/.check/final-integration/`. See [compact identity](final-local-identity.json) and [complete B handoff](handoff.md) for provenance and preserved earlier failures.

## Proposed run, for later review only

Use one fresh ledger location for this candidate, seeded with every prior paid attempt/reservation in the same qualification budget if a previous attempt exists. Never reset budget by moving or omitting failed/unfinished records. One process holds the ledger lock. A new retry is a separately authorized run after its cause is investigated; nothing retries automatically.

The limits remain $1.50 for this two-session check and $3.75 for the entire ledger. The fresh rate must make two whole billed minutes fit both the per-check budget and remaining total before either token is minted. Historical $0.75/minute pricing is not a current pricing assertion. Each independent grant must allow exactly one 50-second session with a 110-second token. A nominal run opens two sources; no grant is reused. The dispenser refuses before a third allocation, including failed attempts.

Run these commands only after completing the held prerequisites. The preflight is free but contacts the provider, mints a token and loads native code; it has not been run for this final candidate. Its single-session rate/grant check does not replace the paid check's fresh two-session admission/reservation. The authorization flag below is a proposed command, not authorization to execute it.

```sh
cd /Users/chriscarroll/.codex/worktrees/0d44/reactor-effect-client
export PATH='/Users/chriscarroll/.local/share/vite-plus/package_manager/bun/1.4.2/bun/bin':"$PATH"
# Supply REACTOR_API_KEY through the operator's secret mechanism, not this document.
# Replace the network text and confirm the ledger path/reservation history before approval.
bun --no-env-file integration/hosted/qualify.ts preflight \
  --budget-usd=0.75 --total-budget-usd=3.75 \
  --ledger=.check/hosted-paid-0.5.0-a68d072

bun --no-env-file integration/hosted/qualify.ts scheduler-renewal \
  --budget-usd=1.50 --total-budget-usd=3.75 \
  --ledger=.check/hosted-paid-0.5.0-a68d072 \
  --network="REQUIRED: operator-recorded network and VPN/relay conditions" \
  --i-authorize-paid-sessions

bun --no-env-file integration/hosted/qualify.ts summarize \
  .check/hosted-paid-0.5.0-a68d072 \
  > .check/hosted-paid-0.5.0-a68d072/summary.md
```

Paid selection is actual `Orchestration.makeContinuous`; no constructor override is accepted. Owner settings are maxSessions 2, renewal lead 40 seconds, handoff grace 250 ms, retainedSuccessfulCleanups 1 and maxUnresolvedCleanups 2. Clip A and B each request five seconds with distinct keys. Submit A, observe Started/Ended, await Prepared with the preferred replacement, submit B on that source, then accept drain after B is admitted. The SDK controls autoplay, switching and retirement.

## Timing, intervention and stop rules

Setup has 20 seconds; scenario work shares 40 seconds from first allocation; cleanup observation has 20 seconds. Evidence uses one Effect monotonic origin; historical wall offsets and AsRunEvent timestamps are not interchangeable. Original allocation identities and failed/incomplete cleanup checkpoints are durably written before finalizers.

The CLI does not supervise its own process and cannot promise to end an uninterruptible finalizer. Proposed external bound for operator review: send SIGINT if still alive at process-start +140 seconds, then terminate the process at +150 seconds if it remains stuck. The owner and mechanism must be named and confirmed before approval. A kill is an incomplete run, not successful cleanup; it preserves the reservation. It must not trigger a concurrent raw source close or manufacture SDK cleanup evidence. The operator separately confirms remote sessions/costs in the provider dashboard using recorded identities and caps, retaining the original unconfirmed report.

Stop on failed admission, invalid/overgranted token, ambiguous allocation/submission, deadline, wrong source/order, missing media, retention exhaustion, cleanup failure or incomplete evidence. Do not start another attempt. Preserve original Causes, partial ledger and both grant reservations; investigate first. Even if an external observation later confirms termination, keep the SDK's original report unchanged and append the observation separately.

## Required passing evidence and limits

Passing requires independently reconciled evidence, not just seven named passing criteria:

1. Two distinct allocated source/session identities, exactly two open attempts and valid independent grants/caps, with allocation persisted before connection.
2. Keyed A then B accepted/finished in order; Prepared prefers the replacement before B admission; exactly one matching planned Switched, immutable SDK eligibility facts and final retiring clip A.
3. Nonempty logical video attributed to both physical sources/generations. Unchanged physical objects carry WeakMap attribution; logical source sums and recorded arrivals reconcile, and untagged frames fail.
4. Accepted drain completes without a later allocation; setup and shared work deadlines hold.
5. Both original canonical owned source-cleanup reports confirm local close and remote termination, with matching lease IDs, complete policy results, no local/remote errors and consistent DELETE/confirmation evidence.
6. Actual continuous CleanupSummary decodes through the SDK codec and reconciles exactly two total retirements, one retained complete owned termination, one omitted complete owned termination, no incomplete record/exhaustion and settled accounting. The retained report exactly matches source B; omission never substitutes for source A's original report. Unknown submissions remain incomplete.
7. Cleanup evidence completes inside its observation budget; the durable ledger passes full schema and independent cross-field validation. Retain per-source reports, switch/count facts, media timing, AsRun events, actual filler requests, drain, budget, runtime and native identity. Exclude credentials, raw media and provider payload/error text.

After the run, independently compare dashboard charges with conservative estimates and confirm any uncertain remote end. The source-to-source decoded-video gap is one observation, with no universal latency, encoded/presented-output or audio-completeness claim. Hosted TURN, resume, filler policies beyond this scenario and historical audio qualification remain separate. Rehearsals and the former scheduler calibration do not qualify the new continuous hosted path.
