# Active workstream tasks

Dispatched 2026-09-26 from remote-main baseline `c47e50c784f6889b542cf8ee316067b688ca4737`. All tasks run on host `local` in isolated worktrees. IDs below are actual task IDs, suitable for task messaging/status tools. The app's general task listing did not initially expose these new tasks; each identity is now confirmed by its task message and status snapshot.

| Stream | Task ID | Initial branch / worktree |
| --- | --- | --- |
| A — 001 then 002 scheduler | `01a0df7a-9791-73b2-ba8a-f28679c77d15` | `codex/fix-scheduler-settlement`; `/Users/chriscarroll/.codex/worktrees/1915/reactor-effect-client` |
| B — 003 evidence and harness | `01a0df7a-cca1-70b0-b5c7-503918eec678` | `codex/qualify-switch-evidence`, then `codex/hosted-renewal-harness`; `/Users/chriscarroll/.codex/worktrees/b8d3/reactor-effect-client` |
| C — 004 continuous renewal | `01a0df7b-09b1-7e70-8c87-c6db96f5e19c` | `codex/renewal-retention-core`; `/Users/chriscarroll/.codex/worktrees/437f/reactor-effect-client` |
| D — 005 and shared integration | `01a0df7b-42ce-7802-9985-53e3c34c3d67` | `codex/pin-qualification-stack`; `/Users/chriscarroll/.codex/worktrees/0d44/reactor-effect-client` |

Parent coordination task: `01a0df38-372b-7a12-80a1-d0510a9c4150`. Every task has received the complete peer directory. Follow [the parallel execution guide](PARALLEL-EXECUTION.md), using direct task handoffs rather than waiting for the parent to relay routine messages.

D is the designated single writer for shared public-type/pack-consumer assertions, shared README/changelog hunks and final combined verification. Source ownership passes from B to C after the small 003-E evidence handoff. A owns scheduler production files. Each task reports local commits and actual checks; pending checks remain pending.

First B-to-C handoff: `c34e95b8874d7cffaa2c400173e15c417289369a` (003-E). B reported build/typecheck/lint and Node focused tests passed when handing it off; its latest Bun/portable checks were still running. This registry records the handoff, not final validation. Obtain current status from B before marking that packet complete.

Implementation is now active. Final combined offline/package gates remain outstanding. No paid session, remote push, merge or publication is authorized by the fan-out request.

## Shared-host verification queue

C reported unchanged scheduler-suite timeouts during concurrent portable runs. These are failed runs, not established harmless contention. Keep their logs. Implementation/review/focused checks remain parallel, but new full portable, full-pack and native verification runs/retries use one shared-host lane coordinated by D. Once current runs finish, default order is A → B → C → D; skip an already-passed or unready packet and explicitly hand off the lane. Use `VITEST_MAX_WORKERS=1` for retries where verified supported/propagated, without raising timeouts or changing assertions. An uncontended failure still requires investigation.

Declared Bun 1.4.2 is already installed at `/Users/chriscarroll/.local/share/vite-plus/package_manager/bun/1.4.2/bun/bin/bun`; D discovered it and parent independently verified `--version`. Use task-local PATH/BUN_BINARY for subsequent gates, preserving the user's global installation.

## Native artifact reuse

Parent verified successful [baseline CI run 36223885202](https://github.com/mannyc2/reactor-effect-client/actions/runs/36223885202) at exact `c47e50c784f6889b542cf8ee316067b688ca4737`. As of discovery, its unexpired native artifacts were `native-darwin-arm64` (ID `10900520537`, expiry `2026-09-27T06:33:46Z`) and `native-linux-x64` (ID `10900067555`, expiry `2026-09-27T06:33:05Z`). D owns retrieval and canonical source/sidecar/library verification before reuse. Peers may copy verified inputs into independent staging; do not rebuild or mutate one shared native directory concurrently. Baseline CI is provenance for those native bytes, not qualification of new TypeScript or package archives. Final combined native/package evidence remains required.

D subsequently reported both artifact identities verified with current `stage.mjs`; regenerated sidecars matched downloaded identities. Immutable inputs: `/Users/chriscarroll/.codex/worktrees/0d44/reactor-effect-client/.check/native-inputs/{darwin-arm64,linux-x64}/`, with retrieval metadata and stage receipts adjacent. Verified native source SHA reported by D: `6b4ad6bf5c710345e46b3b375648b7981679a8570e2bf0b30cd073fccd984125`. B has these paths and must copy/validate into its own staging. Parent records D's verification result; it did not independently execute native tests.

## 005 handoff

D reported 005 READY FOR INTEGRATION: implementation `3cb73b39b62570602fb03f14d474a9e99247c593`, separate handoff/docs `468b6652bfffa43d7ec6871bee457328a3e76859`, branch `codex/pin-qualification-stack`. Detailed handoff: `/Users/chriscarroll/.codex/worktrees/0d44/reactor-effect-client/plans/005-package-tooling-handoff.md`. Reported passes: 33 stack fixtures, static checks, 71 release tests, portable gate, Bun portable/full pack and Darwin native preflight. The full portable gate preceded the narrow archive metadata correction; final scoped/static/pack checks cover that correction. New combined revision, npm CI and Linux/native-suite execution remain pending. D released the heavy slot and is moving to its dedicated local integration branch for serial shared compatibility/docs work. Peers must coordinate the next heavy run with D.

## Communication preference

The user requested less coordination chatter. Each task continues independently and keeps routine progress in its own task. Send dependency/API handoffs directly to affected peers; D manages heavy verification and shared integration directly. Do not copy routine status, acknowledgments, test counts or queue messages to the parent. Contact the parent only for an unresolved decision/blocker peers cannot resolve, or one consolidated completion report. Parent does not narrate routine peer traffic to the user.
