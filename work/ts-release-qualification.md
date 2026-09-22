# Main integration and ts-release setup

September 22, 2026. Maintenance commit `b65eaa6b11cc23d2d1159770aea440575e53868e`
was fast-forwarded onto `main` and pushed to the existing origin. No history was
rewritten. GitHub Actions was enabled; no repository visibility, billing,
protection rules or npm credentials were changed.

## Maintenance package gap closed

The previously blocked default-npm package gate now passes. Its retained archive
at `.check/pack-rQiAUc/reactor-effect-client-0.2.0.tgz` is byte-identical to the
earlier candidate, SHA-256
`b9c5a47ed3be9147ec5675563bdea01e6a2886cbeb1a0eb4fa945e07dd6614e7`.
All three isolated consumers verified 409 installed files, compiled the archived
examples and passed native preflight where applicable. The original native
libraries were reused without Rust, Docker or runtime-image rebuilds.

Evidence: `.check/ts-release-setup/pack-closeout-l_hlpsbl/HANDOFF.md` and `pack.log`.

## Release implementation

The private `release-tools/` workspace pins ts-release core and npm provider
0.4.1, Effect rc.115, and its own frozen lockfile. The published Action is pinned
to `3f64048fe4dd75fb3e853d6d3e048e4b635a1b1a`. CI uploads a flat package artifact
bound to the source commit/tree and successful main workflow run/attempt.

The release workflow requires separate manual preparation and publication.
Preparation retains the exact qualified archive in an immutable Bundle/Plan;
publication restores the original application commit and candidate, validates
exact approval, and uses ts-release's native npm provider and same-repository
Git CAS journal. There is no build, pack, Docker operation, direct `npm publish`,
automatic risk acceptance or silent candidate replacement in the release jobs.
Recovery observes the original candidate and checks current registry evidence
rather than treating a receipt as public visibility.

The repository remains private, so this pinned provider's initial publication
path uses an explicit npm token and NoProvenance. Root release metadata now
identifies the real repository and explicitly disables provenance. No runtime
dependency, eight-export contract, Effect pin, existing assertion or native
source changed. The tool does not claim OIDC trust or protected-environment
reviewer enforcement.

## Fresh release-metadata package

The metadata change was qualified as a new archive, not patched into an old one:

```text
.check/pack-Lf8n0D/reactor-effect-client-0.2.0.tgz
SHA-256 ac52fef85cbb3de5bc1fc544aab9cbb73fc2878ae60b7e88971eb6498b1ca5d5
```

The default npm gate passed all three isolated 409-file identities, strict
declaration/import checks, archived examples (5 portable, 6 browser, 6 native),
Node/Bun offline simulation and the installed Darwin ABI2 native preflight.
Both native platform artifacts retained their original hashes and source identity.
Logs and preservation records:
`.check/ts-release-setup/pack-release-lsgpfg6q/`.

The real qualified archive also passed pure `prepareCandidate` and `loadCandidate`
checks through the actual ts-release npm encoder. That check used explicitly
synthetic local coordinates, retained under `offline-fixture-NOT-FOR-PUBLICATION`.
It is not a hosted-CI attestation or a publishable workflow candidate. No journal
was opened and no network/publication attempt occurred. The original package
and package-identity bytes remained unchanged.

## Final local checks

| Check                     | Result                                                                                                                                          | Evidence under `.check/ts-release-setup/` |
| ------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------- |
| Portable verification     | 612 tests, 3,439 expectations, zero failures; generator, formatting, five type projects, lint, graph checks, build and compiled examples passed | `portable-qualified.log`                  |
| Release-tool verification | 41 offline tests, zero failures; full authored tooling typecheck passed                                                                         | `release-qualified.log`                   |
| Workflow validation       | actionlint 1.7.12 passed both workflows                                                                                                         | `actionlint-qualified.log`                |
| Frozen tool installation  | Successful `bun install --cwd release-tools --frozen-lockfile --ignore-scripts`                                                                 | `tools-frozen-install.log`                |
| SDK preservation          | Runtime source, old tests, native inputs and SDK lockfile unchanged from b65eaa6                                                                | `sdk-preservation.log`                    |

The offline tests cover tampered archives/Bundle/Plan/identities, wrong CI runs,
fork/PR rejection, exclusive candidate ownership, explicit package confirmation,
unauthorized zero-dispatch execution, ambiguous 503 followed by 404 with no
replay, journal identity retention, and actual observation versus receipt status.

Local tools were Bun 1.4.0 and Node 22.13.1. An upstream Undici engine warning in
the isolated install is retained; the required local tests passed. CI selects
Node 22.22.2/24.15.0 and Bun 1.4.2; the Action uses GitHub's node24 runtime.
Local results are not a claim that the new hosted CI or release workflow has run.

## External publication prerequisites

No npm package was published and no release workflow was dispatched during this
setup. A successful main CI artifact is required before manual preparation.
The first actual publish additionally requires the repository's `NPM_TOKEN`
Actions secret with appropriate npm publish/create authority, followed by the
explicit version-bound confirmation. No token was available or fabricated.
The operational procedure is in `release-tools/README.md`.
