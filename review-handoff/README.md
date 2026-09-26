# Cross-machine orchestration review handoff

Start with [the review, direct-fix, and merge-readiness prompt](REVIEW-FIX-MERGE-PROMPT.md). It requires no access to the originating computer or its Codex tasks. Merging, auto-merge, issue closure, package publication, deployment, and paid provider sessions remain unauthorized.

The implementation is on `codex/integrate-orchestration-packets` at `dec33ea21cd8c00311d3cb100cbd5a7184420c05`. The locally tested source is its parent `a68d072640a700b1763e3e3ed8910a18031f848a`; the final implementation commit only updates four plan documents. This separate `codex/orchestration-review-handoff` branch adds archival context, not implementation changes. Do not merge this archive branch into a code PR.

For a new checkout:

```sh
git clone https://github.com/mannyc2/reactor-effect-client.git
cd reactor-effect-client
git fetch origin
git switch --create codex/review-orchestration origin/codex/integrate-orchestration-packets
mkdir -p .check
git archive origin/codex/orchestration-review-handoff review-handoff | tar -x -C .check
```

Read `.check/review-handoff/REVIEW-FIX-MERGE-PROMPT.md`. In an existing checkout, preserve unrelated work and use an isolated worktree. The example branch name can be changed if it already exists.

The copied artifacts are byte-preserved and inventoried in [manifest.json](manifest.json). Verify them before relying on them:

```sh
python3 - <<'PY'
from pathlib import Path
import hashlib, json
root = Path('.check/review-handoff')
manifest = json.loads((root / 'manifest.json').read_text())
for item in manifest['files']:
    data = (root / item['path']).read_bytes()
    assert len(data) == item['bytes'], item['path']
    assert hashlib.sha256(data).hexdigest() == item['sha256'], item['path']
print(f"Verified {len(manifest['files'])} archived artifacts")
PY
```

Included context:

- [Original plans and execution order](original-plans/README.md), including all five numbered plans and decision appendices.
- [Combined integration handoff](workstreams/D/final-integration-handoff.md), [package tooling](workstreams/D/005-package-tooling-handoff.md), and [shared integration record](workstreams/D/shared-integration-handoffs.md).
- Individual handoffs for [A](workstreams/A/handoff.md), [B](workstreams/B/handoff.md), and [C](workstreams/C/handoff.md), with their available focused logs and preserved failures.
- [Final receipt](evidence/final-integration/receipt.json), final gate logs, package identities, install/normalization receipts, dependency trees, type-resolution traces, native sidecars and provenance.
- [Hosted-readiness candidate](workstreams/B/hosted-readiness.md), explicitly held for missing qualification and separate spend authorization.
- [Effect guidance snapshot](guidance/effect-development/SKILL.md) with all linked reference guides. Version-sensitive truth remains the repository policy and frozen installed Effect source.
- [Original owner branch identities](source-refs.json). Those refs are published for provenance; their changes are already integrated. Do not cherry-pick them again.

[ARTIFACTS.md](ARTIFACTS.md) explains how old absolute paths map to this archive, which binary bytes are not stored in Git, and how another machine obtains the native inputs and produces fresh package archives. Original receipts are historical local evidence, not a CI pass or qualification of modified source. The archived local prompt is retained for provenance; use the portable prompt linked above.
