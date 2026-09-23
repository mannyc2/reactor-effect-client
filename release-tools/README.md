# Release the qualified workspace packages with ts-release

The release workflow promotes already qualified npm archives: one for each of
`reactor-effect-client`, `reactor-effect-browser` and `reactor-effect-native`,
all carrying one release version. It does not compile the SDK, rebuild native
libraries, repack downloaded bytes, or create a Docker image. CI produces and
tests the three archives once; preparation and publication consume those exact
archives.

The private tooling workspace pins `@mannyc1/ts-release` and its npm provider to
`0.4.1`, with Effect and the Node platform at `4.0.0-rc.115`. Its separate lockfile
does not add release dependencies to the published packages or change their
public export maps. The Action is pinned to the published `v0.4.1` commit:

```text
mannyc2/ts-release/apps/action@3f64048fe4dd75fb3e853d6d3e048e4b635a1b1a
```

This integration uses ts-release's public npm provider, immutable Bundle/Plan,
owned content store, native HTTP transport and Git CAS journal. It does not call
`npm publish` behind the journal or delegate publication to a second owner.
It publishes npm only; it does not create a GitHub release or release tag.

## Setup and authentication

The repository and all three packages must be public. Configure the trusted
publisher of **each** package name (`reactor-effect-client`,
`reactor-effect-browser` and `reactor-effect-native`) for GitHub owner `mannyc2`,
repository `reactor-effect-client`, workflow filename `release.yml`, and **no
environment**. Enable direct `npm publish` permission: this provider does not use
npm's staged publishing flow. No npm token or repository Actions secret is used,
and token fallback is not supported.

npm requires a package to exist before its trusted publisher can be configured.
`reactor-effect-client` already exists on npm as a `0.0.0-reserved.0` placeholder
with this trusted publisher bound. `reactor-effect-browser` and
`reactor-effect-native` do not exist yet: create each namespace once through
interactive npm authentication with a placeholder version such as
`0.0.0-reserved.0` that carries no SDK code, then configure the same trusted
publisher for it. Complete this for every package before the first publish run:
the client is published first, so a host package whose trusted publisher is
missing fails its credential exchange only after the client publication has been
dispatched. Rerunning `publish` with the same candidate resumes only a host
publication whose dispatch was never recorded (a refused credential exchange
sends no bytes) or one the registry already shows at the exact version; a host
request that reached npm and was answered without acknowledgement stays
inconclusive and needs an explicit ts-release risk decision outside this
workflow, never a new preparation. This separate bootstrap does not qualify or
publish an SDK release. Subsequent SDK releases use only the retained candidates and OIDC
workflow described here.

GitHub supplies run-scoped `github.token` as `GH_TOKEN` for the fixed journal
remote. Only the publish/observe job receives `contents: write`. Both prepare and
publish receive `id-token: write`: prepare uses it to sign exact provenance for
each archive through Sigstore; publish exchanges its identity for a
package-scoped npm token as each publication is dispatched, and only for the
three package names admitted from the retained Plan. Observe mode never requests
npm credentials or dispatches publication. Dependency installs use the isolated
lockfile with `--ignore-scripts`.

Preparation calls Sigstore's production Fulcio/Rekor services and retains one
signed attestation per archive in the immutable Bundle. Its TUF root comes from
pinned `@sigstore/tuf@5.0.0`. Every restored candidate undergoes native signature
and exact source verification before registry access or journal credentials.
Preparation must run on the **same main commit as the selected successful CI
run**: GitHub's signed workload identity must match the source that produced the
archives. Every package manifest sets `publishConfig.provenance` to `true`.

See npm's [trusted-publishing documentation](https://docs.npmjs.com/trusted-publishers/).

No GitHub environment approval protection is assumed. The explicit manual
confirmation is an application check, not a substitute for branch protection,
reviewer enforcement, or GitHub account access controls. Repository visibility,
billing settings and branch protection are not changed by this tooling.

## Prepare, then publish

First obtain a **successful CI run on this repository's main branch**. CI runs
the portable, native and isolated-package checks, including the pack smoke for
all three packages. Its package job stamps `qualification.json` and uploads one
flat `npm-package` artifact containing:

```text
reactor-effect-client-<version>.tgz
reactor-effect-browser-<version>.tgz
reactor-effect-native-<version>.tgz
package-identity.json
qualification.json
```

Qualification binds all three archives to the exact main commit, source tree, CI
run and attempt. Pull-request and fork artifacts are ineligible.

Manually run **Release npm with ts-release** on `main` with `mode=prepare` and
that successful `ci_run_id`. Main must still point to its commit. This run
validates the qualification and every archive hash, checks that the three
packages share one version, that their export maps and file inventories are the
expected ones, that only the native package carries `lib/` files, and that both
native platform identities match; it signs provenance for each archive,
exercises the actual npm request encoder for each publication without sending
it, and retains `ts-release-candidate` containing:

```text
identity.json
bundle.json
plan.json
content/<sha256>  # three archives, identities, qualification, three signed provenance files
```

Review the recorded package/version, source and Plan identities. The step summary
of the preparation run prints the exact confirmation line the publish run
requires. Preparation signs provenance but does not publish and has no npm
credential. Then start a separate manual run with `mode=publish`, the **same**
`ci_run_id`, the successful preparation run's ID as `candidate_run_id`, and this
exact confirmation:

```text
publish reactor-effect-client@0.2.0 reactor-effect-browser@0.2.0 reactor-effect-native@0.2.0
```

Use the candidate's exact version for every package, in this order, as printed
by the preparation run. Stable versions target `latest`; prereleases target
`next`. The publication job restores the application and locked dependencies
from the preparation run's commit, not a newer main implementation. It admits
the original Bundle, Plan, qualification and all three archives before
credentials or journal access. The Plan holds exactly three permitted npm
publications to `https://registry.npmjs.org/`: `reactor-effect-client` first,
then `reactor-effect-browser` and `reactor-effect-native`, each depending on the
client's publication because both pin it as an exact peer. That order guarantees
the peer exists when a host package is published.

The final verifier refreshes actual registry observations for every package, not
just receipt acceptance. A publication receipt alone does not establish current
visibility. Visibility requires every package to be visible; conflicting content
for any package fails; unconfirmed visibility remains nonzero after bounded
observation-only retries. No retry in that verifier sends an npm PUT.

## Recovery and retained evidence

The journal ID is stable for this repository and version: one journal covers the
three package publications of a version. The fixed remote is this repository;
ts-release stores its CAS history beneath `refs/heads/ts-release-journal/`. CI
only responds to pushes on `main`, so journal updates do not trigger native
builds.

For interruption, timeouts, authentication problems or uncertain visibility,
select `mode=observe` with the **original successful preparation run ID** and
original CI run ID. Do not use the failed publication run as the candidate run.
Observe mode needs no npm token, never grants publication authority, and does
record observation evidence, per package, in the same durable Git journal.

After examining the journal, a later explicitly authorized publish run can
continue only where ts-release's evidence allows. Because the client is
published before the host packages, an interrupted run can leave the client
visible while a host package is not; a missing registry version for a package
after a lost response is not proof that its PUT failed. There is no automatic
risk acceptance, Plan supersession, candidate replacement, or blind resend.
Retain the same candidate, application commit, dependencies and remote journal.

Artifacts are retained for 90 days. Download and preserve the complete candidate
and reports before expiration when an unresolved release needs longer recovery.
Rebuilding an expired candidate is not recovery. A new preparation refuses an
existing local candidate directory; it never overwrites an interrupted one.

## Local development checks

```sh
bun install --cwd release-tools --frozen-lockfile --ignore-scripts
bun run check:release
```

The tests use local fake packages, owned temporary directories and in-memory
release hosts with explicitly untrusted structural attestation fixtures. They do
not publish, spend provider credits, or write this repository's remote journal.
Production preparation additionally requires the authenticated successful CI
metadata selected by the workflow. A local fixture test is not evidence that a
hosted package was released.

The workflow selects Node 24.15.0 for command steps and Bun 1.4.2 for installation.
The pinned Action itself uses GitHub's `node24` action runtime and resolves the
application's installed core. Offline checks do not establish that hosted Sigstore signing, npm OIDC exchange or
publication has succeeded; those require the actual hosted workflow.
