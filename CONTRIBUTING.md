# Contributing

Thanks for contributing to `reactor-effect-client`.

The project is intentionally small and explicit: one session authority, host transports beneath it, and application policy outside the SDK. Changes should preserve that ownership model and should be justified by a real reusable boundary or observed behavior.

## Development setup

Required for the TypeScript workspace:

- Bun 1.4.2, as declared by `packageManager`;
- Node.js 22 or newer for npm/package-consumer checks;
- the checked-in `bun.lock`.

Native development additionally requires Rust 1.90, Clang 21 on Linux (the platform compiler on macOS), curl, tar with zstd support (or `zstd`), and a SHA-256 tool. `reactor-webrtc-sys` downloads a pinned libwebrtc prebuilt and verifies its published checksum. The explicit Linux dependency installer and supported distributions are documented in [native/README.md](./native/README.md); ordinary build commands never install system packages.

```sh
bun install --frozen-lockfile
```

Do not commit credentials, JWTs, API keys, session exports, generated media, native build output, npm tarballs, or local environment files.

## Repository boundaries

- `src/session/` exposes the canonical factory and session contract; `src/session.ts` owns protocol correlation, generations, cancellation accounting, and cleanup. `src/coordinator/` owns HTTP allocation/inspection/termination operations.
- `src/PeerFactory.ts` supplies the transport capability through Effect dependency injection. `src/browser/` and `src/native/` select host peers and expose media bound to one session generation.
- `src/h3/` consumes the canonical session and owns model schema validation, provider observations, controls, and acceptance evidence. It introduces no implicit playback, flush, reset, reconnect, filesystem, or path policy.
- `src/orchestration/` owns opt-in routing, scheduling, renewal, and recovering media. `src/Submission.ts` and `src/Sequence.ts` supply its bounded commit and affinity primitives. Keep persona, pricing, and show policy in the application.
- `src/simulation/` implements an unpaid source behind the production orchestration contract. `src/testing/` exports reusable test utilities; private fixtures remain under `test/`.

The package has exactly eight public exports: the root, `browser`, `native`, `h3`, `orchestration`, `simulation`, `testing`, and `wire`. Directory entry points use `src/<name>/index.ts`; the root and generated wire facade use `src/index.ts` and `src/wire.ts`. Internal file layout does not create additional supported deep imports. Update the corresponding isolated pack consumers whenever a public contract changes.

Prefer concrete modules over generic helper layers. Expected operational failures belong in typed Effect error channels; defects and impossible invariants should remain defects rather than being turned into generic recoverable errors.

## Validation

Use the smallest check that can invalidate your change while iterating, then run the full relevant gate once the inputs are final.

TypeScript/package gate:

```sh
bun run typecheck
bun run build
bun run lint
bun run test:portable
bun run native:build # Stage if missing or the native source identity changed.
bun run test:native
bun run test:integration
bun run test:pack
```

The pack smoke builds a real npm tarball, installs it into isolated consumers, checks the exact eight exports and declaration/import closure, compiles separate Node-without-DOM and browser-without-Node type consumers, verifies portable imports do not reach Koffi/native code, and exercises simulation plus native preflight from the installed tarball. Each run retains its exact tarball and package identity under `.check/pack-*`; preserve a qualified archive when handing off a release candidate.

The `native` and `integration` projects run through `scripts/test.ts` with Node/Vitest and use staged native libraries. For TypeScript or fixture changes, reuse those libraries when the embedded source identity, sidecar hash, and current native inputs still match. Native source changes require rebuilding and requalifying the artifact. `scripts/build.mjs`, browser integration, and pack validation mutate the shared `dist` directory; serialize them in a shared checkout.

Native changes:

```sh
./scripts/native-build.sh
./scripts/native-test.sh
```

The build script owns release staging. The test script checks Rust formatting, runs the Rust tests and clippy with warnings denied, then runs the JavaScript native ABI/parser/session-boundary tests against the staged artifact. It never restages a second release library. Finalizer type fixes must preserve cleanup failure: use a failing defect or an asserted cleanup result when an infallible finalizer cannot carry the typed error. Do not discard shutdown errors to make tests compile. For Linux x64 use an explicit existing Docker context:

```sh
DOCKER_CONTEXT=my-context ./scripts/native-linux-x64.sh
```

Never start, stop, restart, or switch a developer's Docker daemon/context as part of a repository script.

Provider/live validation is separate evidence. Tests using local fakes can prove SDK ownership and scheduling semantics; they cannot prove hosted model behavior, provider billing, TURN relay behavior, or browser/native interoperability. Do not spend money or run paid generation for routine contributions.

`./scripts/browser-native.sh` exercises a real local Chrome/native connection after the native library has been staged in `dist/native/`. It checks both binary channels, decoded changing video, PCM audio, browser import isolation, and resource cleanup. It does not allocate a Reactor session. `BUN_BINARY` and `BROWSER_EXECUTABLE` select explicitly managed test executables.

For a local TURN fixture, supply `BROWSER_NATIVE_FORCE_RELAY=1` plus `BROWSER_NATIVE_TURN_URL`, `BROWSER_NATIVE_TURN_USERNAME`, and `BROWSER_NATIVE_TURN_PASSWORD`. The test requires selected relay candidates and media delivery; candidate gathering alone cannot pass. Use a loopback-only test server, not hosted credentials. Standard Chrome output does not carry Reactor's custom frame metadata, so retain the native/native metadata test separately.

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
