# Contributing

Thanks for contributing to the reactor-effect workspace.

The project is intentionally small and explicit: one session authority, host transports beneath it, and application policy outside the SDK. Changes should preserve that ownership model and should be justified by a real reusable boundary or observed behavior.

## Development setup

Required for the TypeScript workspace:

- Bun 1.4.2, as declared by `packageManager`;
- Node.js 22 or newer for the Node/Vitest projects and npm package-consumer checks;
- CPython 3.13 for the wire generator;
- the checked-in `bun.lock`.

Native development additionally requires Rust 1.90 (under rustup, `packages/native/rust-toolchain.toml` selects it), Clang 21 on Linux (the platform compiler on macOS), curl, tar with zstd support (or `zstd`), and a SHA-256 tool. `reactor-webrtc-sys` downloads a pinned libwebrtc prebuilt and verifies its published checksum. The explicit Linux dependency installer and supported distributions are documented in [packages/native/README.md](./packages/native/README.md); ordinary build commands never install system packages.

```sh
bun install --frozen-lockfile
```

Do not commit credentials, JWTs, API keys, session exports, generated media, native build output, npm tarballs, or local environment files.

## Workspace layout

| Workspace           | Package                  | Role                                                                                       |
| ------------------- | ------------------------ | ------------------------------------------------------------------------------------------ |
| `packages/client`   | `reactor-effect-client`  | Portable core: session, coordinator, H3, orchestration, simulation, testing, wire, host    |
| `packages/browser`  | `reactor-effect-browser` | Browser peer and media over the client's `/host` surface                                   |
| `packages/native`   | `reactor-effect-native`  | Native peer, Rust crate, staged libraries, Vitest tests on Node and Bun                    |
| `packages/test-kit` | private                  | Runner-agnostic assertion helpers and `FakeTrack`; imports no workspace package            |
| `examples`          | private                  | Documentation examples compiled against the built packages for both hosts                  |
| `integration`       | private                  | Real Chrome/native qualification, its browser bundle sources, and their modeled-host tests |
| `scripts`           | root tooling             | `verify`, `test`, `architecture`, `pack` and the isolated-consumer fixtures                |
| `release-tools`     | private                  | ts-release application for npm publication; its own lockfile, outside the Bun workspace    |

Dependencies are declared per workspace. The root `package.json` catalog pins every version shared by more than one workspace (Effect, its platform packages, TypeScript, Vitest, types), and workspaces reference them as `catalog:`. Sibling packages are referenced as `workspace:*`; `bun pm pack` rewrites both protocols to exact versions when publishing, and the pack smoke rejects an archive that still carries either. `bun install` uses the isolated linker, so a workspace can import only what it declares.

Root scripts fan out with `bun run --filter`, which runs package scripts in dependency order. Any package script can also be run directly: `bun run --filter reactor-effect-client test`, or `cd packages/client && bun run test`.

## Repository boundaries

- `packages/client/src/session/` exposes the canonical factory and session contract; `src/session.ts` keeps wire correlation and dispatch/cancellation accounting. Its concrete `_internal/lifecycle.ts`, `remote.ts` and `cleanup.ts` modules own generation/phase admission, allocation evidence and ordered cleanup respectively. `src/coordinator/` owns HTTP allocation/inspection/termination operations.
- `src/PeerFactory.ts` supplies the transport capability through Effect dependency injection, and the root's `Peers` namespace is the public contract a third-party host implements. `src/host.ts` is the only module the host packages may reach into, and it holds only what a first-party host would otherwise duplicate: the peer contract types, media generation accessors, observations, the bounds helpers `finite`, `record` and `positiveLimit` for first-party IPC and host input, and the time normalizer `duration`. It is never application API, and a helper that only the client itself uses stays internal. Add to it deliberately; every export there is published and pinned by the host packages.
- `packages/browser` and `packages/native` select host peers and expose media bound to one session generation. They import `reactor-effect-client` and `reactor-effect-client/host` only, never each other. Their build configurations enforce the host: the browser package compiles with DOM types and no Node types, the native package with Node types and no DOM.
- `src/h3/` consumes the canonical session and owns model schema validation, provider observations, controls, and acceptance evidence. `_internal/contracts.ts` is the command/argument/reply authority and projects deployment shapes from the existing message schemas; `_internal/evidence.ts` owns pure evidence matching, not correlation lifetime. It introduces no implicit playback, flush, reset, reconnect, filesystem, or path policy.
- `src/orchestration/` owns opt-in routing, scheduling, renewal, and recovering media. `src/Submission.ts` and `src/Sequence.ts` supply its bounded commit and affinity primitives. Keep persona, pricing, and show policy in the application.
- `src/simulation/` implements an unpaid source behind the production orchestration contract. `src/testing/` exports reusable test utilities; private fixtures remain under each package's `test/`.

`reactor-effect-client` has exactly seven public exports: the root, `h3`, `orchestration`, `simulation`, `testing`, `wire` and `host`. The host packages export their root only. Directory entry points use `src/<name>/index.ts`; the root, wire and host modules use `src/index.ts`, `src/wire.ts` and `src/host.ts`. Internal file layout does not create additional supported deep imports. Update the corresponding isolated pack consumers under `scripts/pack/` whenever a public contract changes.

Prefer concrete modules over generic helper layers. Expected operational failures belong in typed Effect error channels; defects and impossible invariants should remain defects rather than being turned into generic recoverable errors. `packages/client/src/errors.ts` owns the error algebra and states its rules: a new failure category is a reason, not a flag on the context; a parser rejects with a `ReactorError` and runs through `parse` or `parsed`, so a bug in it stays a defect, and its caller chooses `InvalidInput` for caller input or `Protocol` for remote data; a passthrough re-raises a known failure through `isReactorFailure` or a class's `is`, never `instanceof ReactorError`; and provider text stays out of `message` and the cause chain. A known remote reply is a Schema (`src/contract.ts`, `src/h3/messages.ts`), cross-field rules included, decoded after the bounded JSON walker has copied it, and its `SchemaError`, which names the path but no input value, stays in `detail`; first-party IPC such as worklet packets and native snapshots keeps its hand-written bounds.

`bun run check:architecture` parses each package's source with the pinned TypeScript compiler. Inside `reactor-effect-client` it rejects upward policy dependencies; in every package it rejects undeclared dependencies, Node builtins outside the native package, Bun host imports, nonliteral loads, missing/case-mismatched source targets and runtime import cycles. Type-only edges still obey layer boundaries but do not create initialization cycles. The public export maps, the catalog Effect pin and the `workspace:` protocol between packages are checked as contracts, not inferred from directory discovery. Register a new package in `scripts/architecture.mjs` before adding it to the workspace.

Each package's `tsconfig.build.json` is its host closure; `packages/client/tsconfig.node.json` additionally checks the client without DOM types. Public `/testing` utilities must remain host-neutral. Its synchronous PNG fixture uses uncompressed stored blocks and portable Base64 rather than Node zlib/Buffer; image content, not compression bytes, is the fixture contract.

## Validation

Use the smallest check that can invalidate your change while iterating, then run the full relevant gate once the inputs are final. Dependent workspaces resolve their siblings through built declarations, so `bun run build` precedes `bun run typecheck`, `bun run lint`, the example and native checks.

```sh
bun run build
bun run typecheck
bun run lint
bun run check:architecture
bun run check:examples
bun run test:portable
bun run native:build # Stage if missing or the native source identity changed.
bun run test:native
bun run test:integration
bun run test:pack
```

`bun run verify --profile portable` is the CI portable gate; `runtime` builds and runs the portable tests only; `native`, `package`, `release` and `full` are described in [scripts/README.md](./scripts/README.md).

The workspace `tsc` is [@effect/tsgo](https://github.com/Effect-TS/tsgo): TypeScript 7 with the Effect language service built in. The root `prepare` script patches it into the installed `typescript` after every `bun install`. An install that skips lifecycle scripts leaves the stock compiler, which type-checks without the Effect rules and still passes; `tsc --version` ends in `+effect-tsgo` when the patch is in place, and `bunx effect-tsgo patch --typescript` applies it. Its rules are configured in `tsconfig.base.json`, and Effect errors and warnings fail the typecheck. Every rule the language service ships as a suggestion is raised to a warning, so the typecheck fails on each shape a rule matches: yield a yieldable error directly rather than through `Effect.fail`, use `Schema.Finite` for domain numbers, and expose a zero-argument operation as an `Effect` value (`session.stats`, `peer.shutdown`) rather than a function returning one. The rules match syntax, so they enforce less than their names suggest. `effectFnOpportunity`, in its default configuration, reports only a function with no return-type annotation whose body is `Effect.gen(...).pipe(Effect.withSpan(...))`; it says nothing about where spans belong. Further warnings keep platform globals out of Effect code (`globalDateInEffect`, `globalTimersInEffect` and the like), but only in code the rule sees as Effect code: a `setTimeout` in a plain `async` helper that an Effect awaits is not reported, so a deadline an Effect waits on belongs in Effect (`Effect.timeoutOrElse`), with a `TestClock` test as its guard. `anyUnknownInErrorContext` is a warning in `src/`. Construct Schema classes, errors included, from their fields and never override their constructors: decoding and `make` call the constructor with those fields. Give a derived form a named static factory instead, as `CommandFailure.from` and `PolicyFailure.refuse` do. Silence a finding on one line with `// @effect-diagnostics-next-line <rule>:off` under a comment that says why; a directive that no longer suppresses anything is itself reported. In VS Code, `.vscode/settings.json` points the TypeScript 7 server at the workspace compiler, so the same diagnostics and quick fixes appear in the editor. Each `@effect/tsgo` release supports specific TypeScript versions, so bump the two together.

`bun run lint` runs oxlint in type-aware mode: `oxlint-tsgolint` checks typescript-eslint's type-checked rules with the TypeScript 7 compiler. Its version names the TypeScript release it is built on (7.0.2001 for 7.0.2), so bump it with `typescript`. `.oxlintrc.json` enables typescript-eslint's recommended and strict type-checked rules as errors, among them `no-floating-promises`, `no-misused-promises` and the `no-unsafe-*` family, which applies to TypeScript files only because JavaScript has no declared types to check. `switch-exhaustiveness-check` requires a switch over a union to handle every member or to say, with a `default` clause, that it handles only some. A few rules are off because they fight this codebase's idioms: `consistent-return` and `no-unnecessary-condition` (Effect's `return yield*`, and the SDK's runtime checks on input from untyped callers), `no-misused-spread` (spreading a Schema class instance is how a changed copy is built) and `no-unnecessary-type-arguments` (`Redacted<string>` says what it holds). The Effect rules stay in the typecheck rather than also running in oxlint, so each finding is reported once. Silence a lint finding with `// oxlint-disable-next-line <rule>` under a comment that says why.

The pack smoke packs each public package with `bun pm pack`, checks the exact export map, declaration/import closure and declared dependencies of every archive, then installs the archives into isolated consumers: a portable Node consumer without optional dependencies, a browser consumer whose bundle runs without the Node `Buffer` global, and a native consumer that verifies the packaged library identity. The [examples](./examples/README.md) are compiled again inside those installations against the published declarations; only the offline simulation runs. Each run retains its archives, package identity and consumer resolution traces under `.check/pack-*`. `bun --no-env-file scripts/pack.ts --portable` runs the client and browser parts on a host without a staged native library. Successfully checked temporary consumer trees are released sequentially unless `KEEP_PACK_TMP=1`; previous delivery directories are never removed.

`bun run test:portable` runs the client and browser suites under Vitest, each package's `vitest.config.ts` selecting its `test/**/*.test.ts`, first on Node and then on Bun, so the Node engine the packages declare is tested as well as Bun. It then runs Bun's own runner from the workspace root over the tests that remain on `bun:test`, the integration modeled-host tests and `scripts/architecture.test.mjs`; `bunfig.toml` excludes every package's `test/` and the real-host `*.integration.test.ts` files. Inside a package, `node node_modules/vitest/vitest.mjs run` and `bun --bun node_modules/vitest/vitest.mjs run` run its suite on one runtime and take file filters. A portable test imports `test` and `expect` from `vitest` and uses only APIs both runtimes provide: a fixture server is a `node:http` server, not `Bun.serve`, and bytes a platform `FileSystem` returns, a `Buffer` on Node, are compared as a `Uint8Array`, since Vitest's `toEqual` tells the two apart. A test that runs Effects through its package's `test/harness.ts` takes the test context's `signal` and passes it to `run` and `failure`, so a test that times out interrupts its fiber and runs its finalizers; cleanup in a `finally` block runs without the signal, which is already aborted by then.

The native project runs under Vitest on Node and then on Bun, and the integration project under Node/Vitest, through `scripts/test.ts`; both use the staged native libraries under `packages/native/lib/`. The native media load tests also need the test far peer that `bun run native:test` builds (`cargo build --release --example far_peer`), or `REACTOR_NATIVE_FAR_PEER` naming one. For TypeScript or fixture changes, reuse those libraries when the embedded source identity, sidecar hash, and current native inputs still match. Native source changes require rebuilding and requalifying the artifact. Package builds, browser integration, and pack validation mutate the packages' `dist` directories; serialize them in a shared checkout.

Native changes:

```sh
bun run native:build
bun run native:test
```

The build script owns release staging. The test script checks Rust formatting, runs the Rust tests, clippy and rustdoc with warnings denied, builds the test far peer, then runs the JavaScript native suite, including the media load tests, against the staged artifact on Node and on Bun. It never restages a second release library. Finalizer type fixes must preserve cleanup failure: use a failing defect or an asserted cleanup result when an infallible finalizer cannot carry the typed error. Do not discard shutdown errors to make tests compile. For Linux x64 use an explicit existing Docker context:

```sh
DOCKER_CONTEXT=my-context bun run native:linux-x64
```

Never start, stop, restart, or switch a developer's Docker daemon/context as part of a repository script.

Provider/live validation is separate evidence. Tests using local fakes can prove SDK ownership and scheduling semantics; they cannot prove hosted model behavior, provider billing, TURN relay behavior, or browser/native interoperability. Do not spend money or run paid generation for routine contributions.

`bun run test:integration` exercises a real local Chrome/native connection after the native library has been staged. It builds the workspace packages, bundles `integration/browser/native-connectivity.ts` for the browser from the installed `reactor-effect-browser` package, checks both binary channels, decoded changing video, PCM audio, browser import isolation, and resource cleanup. It does not allocate a Reactor session. `BUN_BINARY`, `NODE_BINARY` and `BROWSER_EXECUTABLE` select explicitly managed test executables.

For a local TURN fixture, supply `BROWSER_NATIVE_FORCE_RELAY=1` plus `BROWSER_NATIVE_TURN_URL`, `BROWSER_NATIVE_TURN_USERNAME`, and `BROWSER_NATIVE_TURN_PASSWORD`. The test requires selected relay candidates and media delivery; candidate gathering alone cannot pass. Use a loopback-only test server, not hosted credentials. Standard Chrome output does not carry Reactor's custom frame metadata, so retain the native/native metadata test separately.

## Native changes

The native bridge is transport/media only. Do not put Reactor session allocation, model commands or coordinator policy into Rust.

Callbacks from libwebrtc must never enter or wait for JavaScript. Copy into bounded native queues and signal readiness; only a peer's notifier thread calls into JavaScript. Keep one libwebrtc factory per process. Fence event admission synchronously on close, and keep callback/userdata storage and the Koffi callback registration alive until shutdown has joined the notifier. A native failure carries one of the ABI's failure classes; add a class to the header, the Rust bridge and the host mapping together rather than matching on error text.

The public native peer currently accepts at most one incoming video and one incoming audio track because pinned `reactor-webrtc` does not expose the remote callback's MID/identity. Do not widen this contract by assuming same-kind callback order; expose a formal upstream identity join first.

Rust code follows the lint set in `packages/native/rust/Cargo.toml`, which `bun run native:test` enforces with warnings denied. On top of clippy's pedantic group it enables individual restriction lints, grouped by what they prevent. Outside tests there is no `unwrap`, `expect`, `panic!`, `unreachable!`, indexing or string slicing: reactor-webrtc runs callbacks behind `extern "C"` trampolines, where a panic aborts the host process, so library code returns errors instead. No error is silently discarded by `let _ =`, `.ok()` or `map_err(|_| ...)`, and no `match` absorbs new enum variants in a wildcard arm. `unsafe` is confined to `ffi`, which the `unsafe_code` lint enforces. There, every `unsafe` block has a `// SAFETY:` comment and does one unsafe operation, and a pointer's alignment and a length's bound are checked before any caller memory is touched. Nothing prints, since the host owns stdio. A few style lints hold conventions the code already follows, such as `Arc::clone(&x)` over `x.clone()`. A module with children is `name.rs` beside `name/`, never `name/mod.rs`. `clippy.toml` exempts tests from the panic lints. Suppress a lint only with `#[expect(lint, reason = "...")]` on the narrowest item; an `#[allow]` fails the lint check. Parse C arguments and JSON into typed values at the boundary (`abi.rs`, `ffi/memory.rs`, `protocol/`), state each exported function's pointer contract in its `# Safety` section, and keep a module's unit tests beside it. Tests check the Rust constants against the C header and each JSON shape against what the host parses, so a change to either fails until both sides agree.

## Dependencies and notices

Avoid adding a dependency when a platform or standard-library primitive already owns the behavior. When a runtime dependency or copied/derived source is added:

1. verify its license is compatible with Apache-2.0 distribution;
2. retain required copyright, license, patent and NOTICE material;
3. update the affected package's `NOTICE` and `notices/` where attribution is required, and the root `NOTICE` index;
4. declare the dependency in the package that imports it, through the catalog when another workspace shares the version, never through a workspace parent;
5. extend the pack/import-closure validation so a clean consumer cannot accidentally resolve a development dependency.

For the native crate, CI also checks every locked crate against `packages/native/rust/deny.toml` with cargo-deny 0.20.2: its license must be on the allow-list, its source must be crates.io or the pinned reactor-webrtc repository, and no RustSec advisory may apply to it. Run `cargo deny --manifest-path packages/native/rust/Cargo.toml --locked check` before changing `Cargo.lock`. Allowing a license there does not replace the notice steps above.

## Pull requests

Keep changes focused and reviewable. A useful description explains the concrete behavior before and after the change, the boundary it affects, and the validation that actually ran. Report unexecuted platform/provider checks plainly.

Do not rewrite unrelated dirty work. Do not add release publication, deployment, provider/model substitutions, or paid operations to a change unless those actions were separately authorized.

## Maintainer release configuration

`.github/workflows/release.yml` is the only npm publishing workflow. It uses the pinned ts-release 0.4.1 Action and public npm provider with npm trusted publishing and Sigstore provenance. Main CI qualifies the three workspace packages once, with both native platforms, and uploads one flat `npm-package` artifact holding `reactor-effect-client-<version>.tgz`, `reactor-effect-browser-<version>.tgz`, `reactor-effect-native-<version>.tgz`, `package-identity.json` and `qualification.json`; the manual release workflow only adopts and promotes those exact archives. Neither preparation nor publication rebuilds native code, recompiles the SDK, or repacks downloaded bytes. There are no release tags, GitHub releases or GitHub environments: a release is identified by the main CI run that qualified it and the preparation run that retained it.

Use a successful main CI run to create a candidate with `mode=prepare` while `main` still points at that run's commit. Review its immutable Bundle, Plan and package/source identities. A separate `mode=publish` run requires the same CI run ID, the successful preparation run's ID as `candidate_run_id`, and the exact confirmation line printed by the preparation run: `publish reactor-effect-client@<version> reactor-effect-browser@<version> reactor-effect-native@<version>`. All three packages carry that one version. The Plan publishes `reactor-effect-client` first; the browser and native publications depend on it because both pin it as an exact peer. `mode=observe` records fresh observations without publishing, and visibility requires every package to be visible. Retain the same candidate and Git CAS journal when recovering uncertain outcomes; one journal covers the three packages of a version.

Configure npm trusted publishing for each of the three package names against GitHub owner `mannyc2`, repository `reactor-effect-client`, workflow filename `release.yml` and no environment, with direct `npm publish` allowed. The publish job uses OIDC (`id-token: write`); there is no `NPM_TOKEN` or other long-lived npm secret. npm requires a package to exist before its trusted publisher can be configured, so each of the three was bootstrapped once through interactive npm authentication (a `0.0.0-reserved.0` placeholder carrying no SDK code) and then given this trusted-publisher configuration; 0.2.0 of all three was then published through `release.yml`. A new package name needs the same bootstrap before a publish run can succeed for it. The repository and packages must be public. Each package's `repository` metadata names this repository and its workspace directory, and each sets `publishConfig.provenance`. The explicit confirmation is an application check, not a substitute for branch protection, reviewer enforcement or account access controls.

The separate `release-tools/` directory pins its own lockfile and never enters the packages' runtime dependencies or export maps. Install it with `bun install --cwd release-tools --frozen-lockfile --ignore-scripts`, then run `bun run check:release`, which type-checks and lints it with those dependencies (`bun run lint` skips it, since its types need them) and runs its tests, which use fake packages offline. See [release-tools/README.md](./release-tools/README.md) for the exact preparation, publication and recovery procedure. Configuration and local tests are not evidence that any npm version has been published.
