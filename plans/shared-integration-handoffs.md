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
