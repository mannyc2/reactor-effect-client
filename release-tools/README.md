# Release a qualified package with ts-release

The release workflow promotes an already qualified npm archive. It does not
compile the SDK, rebuild native libraries, repack downloaded bytes, or create a
Docker image. CI produces and tests the archive once; preparation and publication
consume that exact archive.

The private tooling workspace pins `@mannyc1/ts-release` and its npm provider to
`0.4.1`, with Effect and the Node platform at `4.0.0-rc.115`. Its separate lockfile
does not add release dependencies to the SDK or change the SDK's eight exports.
The Action is pinned to the published `v0.4.1` commit:

```text
mannyc2/ts-release/apps/action@3f64048fe4dd75fb3e853d6d3e048e4b635a1b1a
```

This integration uses ts-release's public npm provider, immutable Bundle/Plan,
owned content store, native HTTP transport and Git CAS journal. It does not call
`npm publish` behind the journal or delegate publication to a second owner.
It publishes npm only; it does not create a GitHub release or release tag.

## Setup and authentication

The repository and package must be public. Configure the npm package's trusted
publisher for GitHub owner `mannyc2`, repository `reactor-effect-client`, workflow
filename `release.yml`, and **no environment**. Enable direct `npm publish`
permission: this provider does not use npm's staged publishing flow. No npm token
or repository Actions secret is used, and token fallback is not supported.

npm requires the package to exist before its trusted publisher can be configured.
For a new name, create the namespace once through interactive npm authentication,
then configure its trusted publisher. This separate bootstrap does not qualify or
publish an SDK release. Subsequent SDK releases use only the retained candidates
and OIDC workflow described here.

GitHub supplies run-scoped `github.token` as `GH_TOKEN` for the fixed journal
remote. Only the publish/observe job receives `contents: write`. Both prepare and
publish receive `id-token: write`: prepare uses it to sign exact archive provenance
through Sigstore; publish exchanges its identity for a package-scoped npm token.
Observe mode never requests npm credentials or dispatches publication. Dependency
installs use the isolated lockfile with `--ignore-scripts`.

Preparation calls Sigstore's production Fulcio/Rekor services and retains the
signed attestation in the immutable Bundle. Its TUF root comes from pinned
`@sigstore/tuf@5.0.0`. Every restored candidate undergoes native signature and exact
source verification before registry access or journal credentials. Preparation
must run on the **same main commit as the selected successful CI run**: GitHub's
signed workload identity must match the source that produced the archive. The
package manifest sets `publishConfig.provenance` to `true`.

See npm's [trusted-publishing documentation](https://docs.npmjs.com/trusted-publishers/).

No GitHub environment approval protection is assumed. The explicit manual
confirmation is an application check, not a substitute for branch protection,
reviewer enforcement, or GitHub account access controls. Repository visibility,
billing settings and branch protection are not changed by this tooling.

## Prepare, then publish

First obtain a **successful CI run on this repository's main branch**. CI runs
the portable, native and isolated-package checks. It uploads one flat
`npm-package` artifact containing the archive, `package-identity.json` and
`qualification.json`. Qualification binds the archive to the exact main commit,
source tree, CI run and attempt. Pull-request and fork artifacts are ineligible.

Manually run **Release npm with ts-release** on `main` with `mode=prepare` and
that successful `ci_run_id`. Main must still point to its commit. This run validates the qualification, native
identities and archive hash, exercises the actual npm request encoder without
sending it, and retains `ts-release-candidate` containing:

```text
identity.json
bundle.json
plan.json
content/<sha256>  # archive, identities, qualification, signed provenance
```

Review the recorded package/version, source and Plan identities. Preparation
signs provenance but does not publish and has no npm credential. Then start a separate manual run
with `mode=publish`, the **same** `ci_run_id`, the successful preparation run's
ID as `candidate_run_id`, and this exact confirmation:

```text
publish reactor-effect-client@0.2.0
```

Use the candidate's exact version in the confirmation. Stable versions target
`latest`; prereleases target `next`. The publication job restores the application
and locked dependencies from the preparation run's commit, not a newer main
implementation. It admits the original Bundle, Plan, qualification and archive
before credentials or journal access. There is exactly one permitted npm
publication to `https://registry.npmjs.org/`.

The final verifier refreshes actual registry observations, not just receipt
acceptance. A publication receipt alone does not establish current visibility.
Conflicting content fails; unconfirmed visibility remains nonzero after bounded
observation-only retries. No retry in that verifier sends an npm PUT.

## Recovery and retained evidence

The journal ID is stable for this repository/package/version. The fixed remote
is this repository; ts-release stores its CAS history beneath
`refs/heads/ts-release-journal/`. CI only responds to pushes on `main`, so journal
updates do not trigger native builds.

For interruption, timeouts, authentication problems or uncertain visibility,
select `mode=observe` with the **original successful preparation run ID** and
original CI run ID. Do not use the failed publication run as the candidate run.
Observe mode needs no npm token, never grants publication authority, and does
record observation evidence in the same durable Git journal.

After examining the journal, a later explicitly authorized publish run can
continue only where ts-release's evidence allows. A missing registry version
after a lost response is not proof that the PUT failed. There is no automatic
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
release hosts with explicitly untrusted structural attestation fixtures. They do not publish, spend provider credits, or write this
repository's remote journal. Production preparation additionally requires the
authenticated successful CI metadata selected by the workflow. A local fixture
test is not evidence that a hosted package was released.

The workflow selects Node 24.15.0 for command steps and Bun 1.4.2 for installation.
The pinned Action itself uses GitHub's `node24` action runtime and resolves the
application's installed core. Offline checks do not establish that hosted Sigstore signing, npm OIDC exchange or
publication has succeeded; those require the actual hosted workflow.
