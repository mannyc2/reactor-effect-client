# Artifact availability and path mapping

The implementation and original owner branches are published on this repository. All files listed in `manifest.json` are copied byte-for-byte onto the handoff branch. They were previously local or ignored, except the combined D reports, which were committed locally but unpushed. File hashes in the manifest permit verification after a fresh clone.

The archived files contain historical machine paths. Those paths are evidence labels, not instructions to access the old computer. Map them as follows, relative to this directory:

| Original location | Portable location |
| --- | --- |
| Planning checkout `plans/` | `original-plans/` |
| A worktree `.check/scheduler-stream-a/` | `workstreams/A/` |
| B worktree `.check/qualification-stream-b/` | `workstreams/B/` |
| B's explicitly referenced available `/tmp/reactor-*.log` files | `workstreams/B/earlier-logs/` |
| B's two temporary nominal rehearsal JSON files | `workstreams/B/loopback-ledgers/` |
| C worktree `.check/renewal-stream-c/` | `workstreams/C/` |
| D worktree `plans/` handoff documents | `workstreams/D/` and implementation branch `plans/` |
| D worktree `.check/final-integration/` | `evidence/final-integration/` |
| D worktree `.check/pack-pcZuov/` text records | `evidence/pack/` |
| D worktree `.check/native-inputs/` JSON records | `evidence/native-inputs/` |
| Personal `effect-development` skill | `guidance/effect-development/` |

Earlier packet package identities, dependency reports, install/normalization records and compiler traces are in each owner's `earlier-pack-evidence/<original-pack-directory>/`. D's original 005 temporary logs are under `workstreams/D/earlier-logs/`. The two B raw ledgers are explicitly unpaid loopback rehearsals, not provider-session exports or hosted qualification. Their old absolute paths in `nominal-ledgers.json` map by filename to `workstreams/B/loopback-ledgers/`.

Codex task IDs and local executable paths identify provenance only. The final handoffs provide their completed results. Install the repository-declared tool versions on the new machine rather than trying to reuse the original machine's executable paths. Old temporary isolated package consumers were removed by their successful runner; their retained manifests, normalization receipts, dependency trees and compiler traces are available under `evidence/pack/`.

## Binary archives and native inputs

`CONTRIBUTING.md` excludes npm tarballs and native build output from Git. Consequently this archive includes their SHA-256 identities and qualification logs, but does not commit `.tgz`, `.so` or `.dylib` bytes. `manifest.json` records every excluded binary, including duplicate tarball filenames. The three original combined package tarballs remain local to the producing machine; do not claim those exact tarballs were uploaded, and do not confuse baseline CI packages with the new implementation archives.

The exact SDK native binaries used locally already exist in the repository's [baseline CI run 36223885202](https://github.com/mannyc2/reactor-effect-client/actions/runs/36223885202). The short-lived `native-darwin-arm64` and `native-linux-x64` artifacts expire September 27, 2026. Prefer the same libraries inside the `npm-package` artifact (ID `10900320864`), whose reported expiry is December 25, 2026. During this handoff export, that artifact was downloaded again and both libraries and sidecars were independently confirmed to match the preserved receipt exactly. GitHub artifact download may require repository access through `gh auth login` even for a public repository.

From the implementation checkout, with the handoff extracted under `.check/review-handoff`:

```sh
mkdir -p .check/baseline-native
gh run download 36223885202 --repo mannyc2/reactor-effect-client --name npm-package --dir .check/baseline-native
tar -xzf .check/baseline-native/reactor-effect-native-0.5.0.tgz -C .check/baseline-native
```

Verify the extracted libraries against the preserved receipt before staging:

```sh
python3 - <<'PY'
from pathlib import Path
import hashlib, json
receipt = json.loads(Path('.check/review-handoff/evidence/final-integration/receipt.json').read_text())
for platform, identity in receipt['nativeInputs'].items():
    root = Path('.check/baseline-native/package/lib') / platform
    data = (root / identity['library']).read_bytes()
    assert hashlib.sha256(data).hexdigest() == identity['sha256'], platform
    assert json.loads((root / 'native-identity.json').read_text()) == identity, platform
print('Both native libraries and sidecars match the preserved local evidence')
PY
node packages/native/scripts/stage.mjs .check/baseline-native/package/lib/darwin-arm64/libreactor_effect_native.dylib darwin-arm64
node packages/native/scripts/stage.mjs .check/baseline-native/package/lib/linux-x64/libreactor_effect_native.so linux-x64
```

The stage tool checks the source identity. A later native source change requires a new build; never bypass a mismatch. If the baseline artifacts have expired, follow the repository's documented native build procedure on supported hosts. Do not silently substitute an unverified library. Downloading or hashing Linux bytes does not establish Linux execution on the new TypeScript revision.

## Reproducing package evidence

Use the verified implementation head, declared runtimes, frozen install and patched compiler; stage verified native inputs above; then run the required gates in the portable prompt. The full local package gate is:

```sh
PACK_INSTALLER=bun PACK_EXPECT_NATIVE_PLATFORMS=darwin-arm64,linux-x64 bun run test:pack
```

This produces new archives and a new identity in the runner's `.check/pack-*` directory. Preserve the new receipt and hashes. A fresh successful run qualifies its own bytes; it does not retroactively verify the old local archives. A byte mismatch must be understood rather than relabeled as the old result. Do not publish these still-0.5.0 development packages.

The original full-pack identity SHA-256 is `10f3adedf915f8876257132a155b9d447cdacf3862c55653b59b0a80f3cd8040`. Original archive hashes are client `4bacfdf112fa88735d1a9340af9ccf1ce43ca9d9f3dbeca2cc660b708d64c4c5`, browser `252fbf4d0c98c1e9bde2d359812c88bcba632f3ae0f2d0bf81c219ffcba00ce8`, native `c368603de20b1f5c1c978e7098d98d1132c29be11b28431728b02eb84129da39`.

## Evidence limits

Text snapshots preserve original paths, versions and recorded failures; they have not been rewritten to imply a different host or a remote CI pass. Original plan TODOs and workstream-local pending checks may be superseded by D's final combined report. Runtime logs and test names can include synthetic token/credential terminology; no live paid provider session was part of these workstreams. Do not infer hosted qualification from loopback ledgers or local decoded-frame tests.

Remote npm-installer qualification, the final OS/Node matrix, and Linux execution remain unrun at handoff export. Pushing branches does not change those statuses. Nothing in this artifact transfer authorizes merging, issue closure, deployment, package publication or paid sessions.
