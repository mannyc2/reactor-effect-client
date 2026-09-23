# Maintainability milestones and qualification

Date: September 22, 2026. Base: `02f5ddf7995e747c87939be4d155eabff422d55c`.

**Resolved on September 22, 2026:** after free disk space became available, the
default npm package gate passed with all three isolated consumers. The exact
earlier archive was qualified without rebuilding either native library. The
ENOSPC records below describe the original attempt, not the current result.

The successful closeout is retained at
`.check/ts-release-setup/pack-closeout-l_hlpsbl/HANDOFF.md` and `pack.log`.
`.check/pack-rQiAUc/reactor-effect-client-0.2.0.tgz` has SHA-256
`b9c5a47ed3be9147ec5675563bdea01e6a2886cbeb1a0eb4fa945e07dd6614e7`, identical
to the earlier candidate. All 409 files matched in each installed consumer;
archived examples compiled in all three profiles and installed-native preflight
passed. Both native libraries, sidecars and all prior archives were unchanged.
No npm publication was performed. Subsequent release-metadata changes require
a separate package qualification, not a native rebuild.

## Completed implementation

The canonical Session now delegates generation/readiness fencing, remote
allocation evidence, and ordered cleanup to concrete internal modules.
`src/session.ts` fell from 1,520 to 1,275 lines. Wire decoding, correlation,
dispatch, cancellation accounting, publication and upload method bodies, media
generation projections, and public type exports were preserved. Thirteen new
deterministic tests cover lifecycle transitions, allocation interruption,
joined close, cleanup error accumulation, deadlines and old-generation fencing.

H3 now has one command/argument/reply authority projected from the existing
payload schemas, plus pure acceptance-evidence helpers. Correlation lifetime,
retirement, uncertainty bounds and request dispatch remain in the client.
Thirty-six independent regression tests cover the shared contracts and evidence
rules. Existing H3 tests, fixtures, payload schemas and public entry point are
unchanged. H3 0.5.5 prompt-only/image support and audio exclusion are preserved.

The compiler-backed architecture check rejects forbidden layer/host imports,
undeclared dependencies, missing or case-mismatched source targets, nonliteral
module loads and runtime cycles. Type-only imports still obey layer boundaries.
It also enforces the exact eight public export targets and Effect rc.115 pins.
Independent Node-without-DOM and browser-without-Node source projects join the
ordinary typecheck command.

These checks exposed two host leaks: native/Fetch source used DOM-only type names,
and the public PNG testing helper required Node zlib/Buffer. Native/Fetch now use
the existing portable types; their emitted JavaScript is byte-identical to the
previous retained package. The PNG helper now uses portable stored DEFLATE
blocks and Base64. Dimensions, black pixels and synchronous signatures remain
the fixture contract; compressed PNG bytes and sizes intentionally differ.
Seven PNG tests independently validate pixels, CRCs, Adler-32 and block bounds.

Seven checked-in public-package example modules cover canonical sessions, H3,
explicit renewal, offline simulation, Web Crypto and both host compositions.
Both host profiles emit real JavaScript. Only the offline simulation runs, under
Node and Bun; live-session examples are never executed by qualification.

Package checks now emit the actual archived examples inside independent
installed consumers, retain resolution traces and install diagnostics, and
hash-check every installed SDK file against the archive. The native fixture
explicitly provides Effect rc.115 and retains the existing shared-platform
rc.115 override at its own root. Its dependency-tree assertion rejects a mixed
Effect/platform stack. The default installer is still npm; explicit
`PACK_INSTALLER=bun` uses the same assertions, with content-addressed archive
names to prevent stale fixed-filename cache reuse. There is no automatic
fallback, outside-workspace resolution allowance, or relaxed type checking.

## Observed validation

All paths below are relative to this checkout. Logs are retained locally under
`.check/maintainability-final/` unless another directory is named.

| Check                                 | Observed result                                                                                                                                                         | Evidence                                                                             |
| ------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------ |
| Full portable profile                 | 612 tests, 3,439 expectations, zero failures; generator, formatting, five type projects, lint, architecture, build, Node/Bun import guards and compiled examples passed | `portable-qualified.log`                                                             |
| Staged native tests                   | 15 tests across four files passed                                                                                                                                       | `native-qualified.log`                                                               |
| Real local browser/native integration | One test passed; 40 changing decoded video frames, 15 audio packets, attributed replies, media retirement and joined cleanup                                            | `integration-qualified.log`                                                          |
| Session focused suite                 | 165 tests passed; original dispatch/correlation/projection bodies verified unchanged                                                                                    | `../maintainability-session-worker1-weuGHg/focused-final.log` and `preservation.log` |
| H3/source focused suite               | 138 tests, 1,151 expectations passed; old tests and public surface unchanged                                                                                            | `../maintainability-h3-freeze-w9dY5s/`                                               |
| Installed portable package            | All 409 SDK files byte-verified; strict Node declarations and five archived examples compiled; production simulation and compiled example ran under Node/Bun            | `pack-qualified-bun.log`                                                             |
| Installed browser package             | All 409 SDK files byte-verified; six archived examples compiled; seven browser-safe exports bundled and executed without Node Buffer                                    | `pack-qualified-bun.log`                                                             |
| Installed native package              | **Blocked at dependency installation by ENOSPC; preflight and installed-native example checks did not run**                                                             | `pack-qualified-v4.log`, `pack-qualified-v5.log`, `pack-qualified-bun.log`           |

The isolated Node declaration check retains the pre-existing, narrowly matched
Effect rc.115 `Channel.d.ts` / `TextDecoderOptions` exception. All other SDK,
example and dependency declaration diagnostics remain failures. The browser
consumer has no Node ambient types, and its bundle contains no native loader.

Actual host tools were Node 22.13.1, Bun 1.4.0, TypeScript 7.0.2 and Vitest 5.0.0.
The repository's Bun 1.4.2 pin was not changed; this run does not claim to have
executed that previously stalled host binary. npm additionally reported an
upstream Undici Node-engine warning in a failed native installation; this run
does not qualify every optional Node-platform module on Node 22.13.1.

## Retained package candidate

The full portable profile passed again after the final runner changes: 612 tests,
3,439 expectations, zero failures, with all five type projects, formatting,
lint, architecture, build and compiled examples green. The final source recheck
is retained at `.check/maintainability-final/portable-final-recheck.log`.

The candidate is a real npm archive, with 409 payload files and both unchanged
native platforms:

```text
.check/pack-CmQHNK/reactor-effect-client-0.2.0.tgz
SHA-256 b9c5a47ed3be9147ec5675563bdea01e6a2886cbeb1a0eb4fa945e07dd6614e7
```

The later archive at `.check/pack-1e7GUE/reactor-effect-client-0.2.0.tgz` is
byte-identical. Its extraction failed for lack of disk space; it is not a second
successful qualification. Candidate status and per-file hashes are recorded in
`.check/maintainability-final/candidate-identity.json`, explicitly marked partial.

Darwin native SHA-256:
`a72d3cccb1dd03590ed7dd1040386339a50ef1a96b6f6a093d0cadd1e712a669`.
Linux native SHA-256:
`e80e16bb755e6d918d6a2395baad656d7be3142b0a52fc669c1b80e432b0da70`.
Shared native source identity:
`d2584076f542ea12a964eecf8384c8b93b013672388351e1998571b0b3c69d37`, ABI 2.

All five pre-existing npm archives and both native binaries/sidecars match their
recorded hashes. The old renewal implementation, Submission, Sequence, wire/H3
payload schemas, lockfile, Rust inputs and `work/refactor-evidence` are unchanged.
Only disposable extraction/consumer directories created during this task were
removed. Byte-identical failed-run archives created during this task were
coalesced with hardlinks; every archive path and byte was preserved. No prior
delivery directory or global cache was pruned.

## Remaining gate and boundaries

The initial pack attempts exposed an NVM PATH dependency in a stripped fixture
environment and a Node lazy-Web-API initialization issue in the new no-Buffer
harness; both were corrected and their checks subsequently passed. Independent
native installations then exhausted host disk space under both npm and Bun.
The Node platform's prerelease range also admits later shared-platform/Effect
peers, so the fixture now explicitly retains the existing rc.115 pin and records
its dependency tree when installation succeeds. The final pinned native fixture
could not be exercised before disk exhaustion; no successful package identity
was written by the full pack runner.

After adequate free disk is available, run the complete unchanged-assertion gate
from this checkout; it will create a new archive and retain all earlier ones:

```sh
export PATH="$HOME/.bun/bin:$PATH"
export BUN_BINARY="$HOME/.bun/bin/bun"
PACK_EXPECT_NATIVE_PLATFORMS=darwin-arm64,linux-x64 bun --no-env-file scripts/pack.ts
# Explicit alternate installer, not a silent fallback:
PACK_INSTALLER=bun PACK_EXPECT_NATIVE_PLATFORMS=darwin-arm64,linux-x64 bun --no-env-file scripts/pack.ts
```

No native Rust source rebuild, Linux runtime-image rebuild, hosted provider
session, paid generation, publication or push was performed. The real-media
integration used local fixtures; TURN relay and Reactor custom frame metadata
were not exercised. Earlier Linux delivery evidence remains intact rather than
being relabeled as a new Linux execution.
