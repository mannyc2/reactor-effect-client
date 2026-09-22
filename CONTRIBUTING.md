# Contributing

Thanks for contributing to `reactor-effect-client`.

The project is intentionally small and explicit: one session authority, host transports beneath it, and application policy outside the SDK. Changes should preserve that ownership model and should be justified by a real reusable boundary or observed behavior.

## Development setup

Required for the TypeScript workspace:

- Bun 1.4.2, as declared by `packageManager`;
- Node.js 22 or newer for npm/package-consumer checks;
- the checked-in `bun.lock`.

Native development additionally requires Rust 1.90, clang/clang++, curl, tar with zstd support (or `zstd`), and a SHA-256 tool. `reactor-webrtc-sys` downloads a pinned libwebrtc prebuilt and verifies its published checksum.

```sh
bun install --frozen-lockfile
```

Do not commit credentials, JWTs, API keys, session exports, generated media, native build output, npm tarballs, or local environment files.

## Repository boundaries

- `src/Client.ts` and the protocol session own allocation, correlation, reconnect, cancellation accounting and cleanup semantics.
- `src/PeerFactory.ts` selects a transport capability through Effect dependency injection.
- `src/browser.ts` and `src/native*.ts` own host-specific WebRTC behavior.
- `src/engine/` owns reusable clip-engine behavior. Keep application scheduling, persona, pricing policy and show semantics outside it.
- `src/h3.ts`, `Clip.ts` and `ModelProfile.ts` adapt H3 model semantics without choosing a transport.
- `Submission.ts` and `Sequence.ts` are reusable ownership/correlation primitives. Preserve their bounded and explicit unknown-outcome behavior.
- `testing/` is the only public test-utility surface. Private fixtures stay under `test/`.

Prefer concrete modules over generic helper layers. Expected operational failures belong in typed Effect error channels; defects and impossible invariants should remain defects rather than being turned into generic recoverable errors.

## Validation

Use the smallest check that can invalidate your change while iterating, then run the full relevant gate once the inputs are final.

TypeScript/package gate:

```sh
bun run typecheck
bun run build
bun run lint
bun run native:build
bun run test
bun run test:pack
```

The pack smoke builds a real npm tarball, installs it into isolated consumers, checks export/declaration/import closure, compiles separate Node-without-DOM and browser-without-Node type consumers, verifies portable imports do not reach Koffi/native code, and runs native preflight from the installed tarball.

Native changes:

```sh
./scripts/native-test.sh
```

This runs the Rust tests, clippy with warnings denied, a release build and the JavaScript native ABI/parser/session-boundary tests. For Linux x64 use an explicit existing Docker context:

```sh
DOCKER_CONTEXT=my-context ./scripts/native-linux-x64.sh
```

Never start, stop, restart, or switch a developer's Docker daemon/context as part of a repository script.

Provider/live validation is separate evidence. Tests using local fakes can prove SDK ownership and scheduling semantics; they cannot prove hosted model behavior, provider billing, TURN relay behavior, or browser/native interoperability. Do not spend money or run paid generation for routine contributions.

## Native changes

The native bridge is transport/media only. Do not put Reactor session allocation, model commands or coordinator policy into Rust.

Callbacks from libwebrtc must not enter JavaScript directly. Copy into bounded native queues, fence event admission synchronously on close, and keep callback/userdata storage alive until shutdown proves quiescence.

The public native peer currently accepts at most one incoming video and one incoming audio track because pinned `reactor-webrtc` does not expose the remote callback's MID/identity. Do not widen this contract by assuming same-kind callback order; expose a formal upstream identity join first.

## Dependencies and notices

Avoid adding a dependency when a platform or standard-library primitive already owns the behavior. When a runtime dependency or copied/derived source is added:

1. verify its license is compatible with Apache-2.0 distribution;
2. retain required copyright, license, patent and NOTICE material;
3. update `NOTICE` or `notices/` where attribution is required;
4. make the dependency explicit in `package.json`/Cargo metadata rather than relying on a workspace parent;
5. extend pack/import-closure validation so a clean consumer cannot accidentally resolve a development dependency.

## Pull requests

Keep changes focused and reviewable. A useful description explains the concrete behavior before and after the change, the boundary it affects, and the validation that actually ran. Report unexecuted platform/provider checks plainly.

Do not rewrite unrelated dirty work. Do not add release publication, deployment, provider/model substitutions, or paid operations to a change unless those actions were separately authorized.

## Maintainer release configuration

`.github/workflows/release.yml` is the only npm publishing workflow. It is manual-only and must be dispatched from the exact `v<package.version>` tag with the matching version input and `confirm=publish`. It rebuilds/qualifies Linux x64 and macOS arm64 native artifacts, assembles them into one npm tarball, and reruns the isolated pack/install smoke before the publish job becomes eligible.

The publish job references the GitHub Environment named `npm`. Repository maintainers must configure that environment with required reviewers and should prevent self-review and restrict deployment tags to the intended `v*` release pattern. A workflow reference alone does not create those approval rules.

Configure npm Trusted Publishing for the real repository, workflow filename `release.yml`, and environment name `npm`, with direct `npm publish` allowed. The publish job uses OIDC (`id-token: write`) and no long-lived npm token. `package.json.repository` must match `github.repository`; the workflow fails before native builds if that metadata is absent or different.

The release workflow is configuration only until someone explicitly dispatches it and the protected `npm` environment approves the final publish job. Do not treat the presence of this workflow as evidence that any version has been published.
