# Shared integration handoffs

Branch: `codex/integrate-orchestration-packets`, worktree `/Users/chriscarroll/.codex/worktrees/0d44/reactor-effect-client`. D remains the only writer of the public consumer/type assertions and shared documentation. Runtime source changes are local cherry-picks from their designated owners; no remote merge or push occurred.

## 003-E switch evidence

Input B commit `c34e95b8874d7cffaa2c400173e15c417289369a`, integrated as `8a69535`. Shared compatibility/docs commit: `31fdb9cae25b60028f62eb32f3c5a136586a270d`.

Changed shared files: `packages/client/test/PublicTypes.test.ts`, `scripts/pack/node-consumer.mts`, `packages/client/README.md`, `packages/client/test/orchestration/README.md`, `CHANGELOG.md`.

The type assertions keep old `Switched` values without `handoff` assignable, pin the decision union, replacement ID, final clip ID and grace tags/origins, and query them through the existing public `Renewal` union. The installed consumer constructs a historical value without the field. Documentation names immutable eligibility capture before pressure/close waits, retiring versus replacement IDs, unchanged aggregate tail and local decoded-frame accounting; audio, presentation and hosted qualification remain unverified.

Verification on this packet: root build/typecheck/lint passed; PublicTypes, Media and RenewalState passed 37 tests on Node and 37 on Bun 1.4.2, with one worker. Bun portable installed-consumer qualification passed with evidence at `.check/pack-8bhOCW`. Logs: `/tmp/reactor-d-003e-{static,node,bun,pack}.log`. B's earlier full-portable failure under concurrent work is not relabeled as a pass; final combined verification remains pending.

Next owners: B may consume the public evidence contract in the hosted harness; C preserves these fields during retirement integration. D will revalidate the combined bytes.

## 001 terminal settlement

Input A commit `6bf95b2a07135467711a708dbe3713275cec098b`, integrated as `5670e9d`. Shared documentation commit: `da1101edf08579719b02bae06189a4767950a194`.

Changed shared files: `packages/client/README.md`, `packages/client/test/orchestration/README.md`, `CHANGELOG.md`. No public signature or consumer assertion changed in 001.

Documentation states that retained item waits, actor-owned controls and admission state settle before `scheduler.failure` publishes. Typed termination preserves terminal Unknown and earlier decisive evidence; unexpected defects/interruption preserve their Cause. Publication does not wait for resumed callers or establish remote cleanup completion. The test README records the immediate-close, phase, Cause, startup, stalled-control and cancelled-wait coverage supplied by A.

D reviewed the actual terminal-owner patch and formatted/checked the documentation. A's reported build/typecheck/lint/architecture checks and focused pre-fix/post-fix evidence remain packet evidence; its earlier full portable run failed and a required-runtime retry is pending with 002. The combined gate remains pending. Next owner: A completes 002 on its terminal owner; D will apply the actual option contract and verify the joined packets.

## 002 unknown-capacity deadline

A source `efea67a3b2ed808220fb045c1b65377b41a316c9` is integrated as `fb88362`; shared public assertions/docs are `ce2e00a536d8f695b95e1b01e2d51b6872e7b023`. Follow-ups `9e02cfdef60a6e30e0347dd16f047ff2709fe6de` (competing terminal closers) and `7d3d129a9a29285ba20f833732560c93bc899cf9` (cancel the watchdog after a truthful successful drain) are integrated as `87e690d` and `98db268`.

The optional `unknownRecoveryTimeout: Duration.Input` defaults to 60 seconds, must be positive and finite, and is capped at 10 minutes. An independent scoped watchdog retains the original unknown observation age through blocked control work and closing. Keyed proof or a usable replacement can restore service; a late drain preserves that original age until it truthfully completes. Timeout closes admission and settles waits without releasing uncertain capacity or replaying an item. The uncertain filler ledger admits at most 4096 entries and refuses before the next dispatch.

Shared files: `packages/client/test/PublicTypes.test.ts`, `scripts/pack/node-consumer.mts`, `packages/client/README.md`, `packages/client/test/orchestration/README.md`, `CHANGELOG.md`. Root build/typecheck/lint passed after the shared option assertions (`/tmp/reactor-d-002-static.log`). Installed-consumer assertions and final combined gates remain pending. A reported its first required-runtime full portable gate passed on `efea67a`, with 809 client and 58 browser tests on each runtime and 99 root tests; a final gate on its two reviewed follow-ups is in progress.

## 004 continuous renewal and cleanup retention

C inputs `763052cd82bf5f27278ea55e0e306d86889396c0`, `5cb2be80ed0061852367f2908adfba961e9373dd` and `9afac859a3af77ed8ece58d0ed898e2387e80a84` are integrated as `23d03f8`, `4443f80` and `ed88be7`. The shared API/docs packet was authored separately against C's actual API as `96551d914a0b94b34b3027d380c2ff49f0691836`, integrated as `9841bda` after reconciling only the changelog headings.

New exports are `makeContinuous`, `ContinuousOptions`, `ContinuousHandleShape` and `CleanupSummary`. Legacy construction and complete-history reports remain unchanged. Continuous renewal has at most two physical reservations, a bounded successful-cleanup tail and separately reserved incomplete-evidence capacity. Stable lifetime source/clip identities are an adapter precondition; live collisions are checked, captured work is fenced by private incarnations, and evicted historical identity misuse cannot be fully detected. Exact bigint retirement counts and omitted confirmed-cleanup categories accompany retained factual records. Unknown submission outcomes keep a record incomplete even when canonical remote termination is confirmed. One completion Exit owns retirement/close; stalled finalizers retain their reservation.

Shared files: `packages/client/test/PublicTypes.test.ts`, both `scripts/pack/{node,browser}-consumer.mts`, `packages/client/README.md`, `packages/client/test/orchestration/README.md`, `CHANGELOG.md`. The consumer fixtures use the actual options, constructor, close result, engine/sequence types and JSON codec. A clean C-only branch passed root build/typecheck/lint and public-type runtime checks (`/tmp/reactor-d-004-static.log`, `/tmp/reactor-d-004-public-types.log`). C's queued pack and final combined installed-consumer compilation remain pending.

## 003-H / 003-C and aggregate runner prerequisite

B's independently reviewable 003-H input `f5d9162` is integrated as `877c7b2`. Its actual continuous-constructor adaptation and final 26-case rehearsal are pending before the combined gate. B reported the original 13-case legacy public-renewal matrix passed in 175.85 seconds; final tightening and continuous coverage still require the final run.

Authorized runner commit `56f6ebd` changes only `scripts/test.ts` and `scripts/README.md`: an optional fourth run-tuple value gives root Bun discovery a 600-second aggregate timeout, retaining 180 seconds for every Vitest subprocess and every individual scenario/CLI deadline. The original estimate was 175.85 + 116.56 = 292.41 seconds. Both-constructor coverage revises it to 2 × 175.85 + 116.56 = 468.26 seconds, leaving about 132 seconds of bounded variance within the same authorized budget. `scripts/verify.ts` has no outer timeout and CI defines no shorter job timeout. No filename registry, skip or fallback was introduced. Final measured root duration is pending.
