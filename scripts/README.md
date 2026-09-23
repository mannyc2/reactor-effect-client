# Verification

`bun run verify` runs the same checks CI uses: wire regeneration, formatting, lint, the dependency-ordered package build, every workspace's typecheck, architecture, compiled examples, portable runtime imports and tests, native staging and tests, real local browser/native WebRTC, and clean installed-package validation. Any failure stops the profile and returns its nonzero exit status.

The profiles are `portable`, `runtime`, `native`, `package`, `release` and `full`. Print a profile without executing it with `bun run verify --profile full --list`. `portable` needs no native binary. `runtime` builds and runs only the portable tests and import guards; CI uses it for the OS/Node matrix. `native` builds the packages, then builds and stages the host binary before checking it and running the public session integration. `package` checks the already staged binaries without rebuilding them. `release` combines the portable and package checks; its native matrix must have qualified the staged artifacts first. `full` runs all three parts.

`test:portable` uses Bun discovery from the workspace root; `bunfig.toml` excludes the Node/Vitest files (`packages/native/test` and `*.integration.test.ts`) and disposable output. `test:native` runs Vitest in `packages/native` under Node and then Bun, and `test:integration` runs Vitest under Node in `integration`. There is no test filename registry: a new test in its workspace joins the corresponding project automatically.

## Files

| Path                    | Purpose                                                                                  |
| ----------------------- | ---------------------------------------------------------------------------------------- |
| `verify.ts`             | Profile orchestration; also runs the Node and Bun portable import guards after `build`   |
| `test.ts`               | Runs one test project with credential-like environment variables removed                 |
| `architecture.mjs`      | Compiler-backed layering, host, dependency, cycle and export-contract check per package  |
| `architecture.test.mjs` | Regression fixtures for the architecture check (Bun)                                     |
| `portable-import.mjs`   | Imports every portable entry of the built client and browser packages                    |
| `pack.ts`               | Packs, validates and installs the public packages into isolated consumers                |
| `pack/`                 | Consumer fixtures copied into those isolated installations                               |
| `tsconfig.json`         | Typechecks this directory (JavaScript included); `pack/` is checked inside the consumers |

## Pinned tools and generated code

The root manifest pins Bun 1.4.2 and, through its catalog, TypeScript, Vitest, the Effect packages and their types; Oxlint and Oxfmt are root development dependencies. The formatter configuration disables import and package-key sorting and keeps generated wire code, native source, notices and staged libraries outside JavaScript formatting. Native formatting is checked by `cargo fmt` with the pinned Rust 1.90.0 toolchain.

Each package builds from its `tsconfig.build.json`, which is also its host closure: the client and browser packages compile with DOM types and no Node types, the native package with Node types and no DOM. `packages/client/tsconfig.node.json` checks the client without DOM types. Each package's `tsconfig.json` adds its tests and test runner types; `scripts/tsconfig.json`, `examples/tsconfig.check.json` and `integration/tsconfig.json` check the JavaScript tooling with `checkJs`.

The architecture check uses the pinned compiler's syntax tree to check layer boundaries inside the client, host-only imports, declared dependencies, literal loading and runtime cycles for every package. Its regression fixtures distinguish actual imports from prose and type-only dependencies from initialization cycles. The synchronous TypeScript API runs on Node because it requires Node child-process pipe handles, even when Bun launches verification. `node scripts/architecture.mjs --package <directory>` checks one package directory; without arguments it checks the workspace.

After build, `check:examples` compiles the examples workspace for both hosts into `examples/dist` and executes only the offline simulation under Node and Bun. Live-session examples are never executed by the gate.

`packages/client/wire/generate.py` uses CPython 3.13 and only its standard library; CI pins 3.13.5. It reads every tracked Reactor proto and the tracked Google Struct descriptor. The parser rejects syntax outside its supported canonical schema subset, and the generator has no network or host-protoc dependency. `bun run generate:check` regenerates in memory and fails on any byte difference from `packages/client/src/wire.generated.ts`; `bun run generate:wire` writes the regenerated file. Schema changes must be reviewed alongside their generated diff.

## Installed-package and host evidence

`pack.ts` creates a fresh directory under `.check/pack-*` and preserves earlier archives. It packs each public package with `bun pm pack`, so `workspace:` and `catalog:` protocols are rewritten to exact versions; an archive that still carries either fails. For every archive it validates the exact public export map, all relative/declaration import closure, declared external dependencies, byte identity with the workspace build, the presence of README, LICENSE, NOTICE and `notices/`, the exact Effect peer and, for the host packages, the exact `reactor-effect-client` peer.

The portable and browser installations omit Koffi and use a resolver that rejects dependencies outside their isolated directories and any resolution into `reactor-effect-native`. Their type consumers compile without a workspace fallback; the Node declaration consumer has no DOM library. The browser consumer installs the client and browser archives, bundles every browser-safe entry from those installed packages, and runs the bundle without the Node `Buffer` global. The workspace examples are emitted in the isolated installations with strict dependency declaration checking, and the compiled offline simulation runs under both Node and Bun. TypeScript resolution traces remain alongside the archives. Temporary consumer trees are released after their checks by default to keep peak disk usage bounded. `KEEP_PACK_TMP=1` retains the extractions and all of this run's consumers. `--portable` skips the native archive and consumer.

The native consumer installs the client and native archives with `@effect/platform-node`, explicitly retains the workspace's shared-platform rc.115 override at its own root and verifies the installed Effect/platform dependency tree. npm does not inherit a dependency package's overrides, and the Node platform's prerelease caret range otherwise admits newer shared-platform peers. The qualified dependency tree is retained as `native-dependencies.json`.

`package-identity.json` is also the release input. On `main`, CI's package job passes it to `release-tools/ci.mjs stamp`, which re-hashes the three archives it names, writes `qualification.json` binding them to the exact commit, tree, run and attempt, and uploads the archives with both files as the flat `npm-package` artifact. The manual release workflow adopts that artifact without repacking; the ts-release application under `release-tools/` (its own lockfile, checked by `bun run check:release`) accepts only the `full` profile and rejects an identity whose packages disagree on version, whose export maps or file inventories changed, whose portable packages carry `lib/` files, or whose native platform identities differ. See [release-tools/README.md](../release-tools/README.md).

The default consumer installer remains npm. `PACK_INSTALLER=bun` selects the same assertions with Bun's hoisted clonefile/hardlink installation explicitly; there is no automatic fallback after an npm failure. A SHA-256-named hardlink prevents reuse of a stale fixed-filename Bun cache entry. Every installed file of every archive is byte-checked against that exact archive for either installer, and the selected installer is recorded in `package-identity.json`. No workspace links or outside resolutions are allowed. Install diagnostics are retained as `install-<consumer>.log`.

Each native sidecar under `lib/<platform>-<arch>/` must match its archived and staged binary SHA-256. All platforms must share the native source identity, reconstructed again from the native build inputs under `rust/` inside the archive. The installed native preflight verifies that same identity, fails before coordinator allocation for an invalid library, and closes its fixture allocation through the public owner. `PACK_EXPECT_NATIVE_PLATFORMS` lists the platforms the archive must carry.

`native:build` is the staging entry point. `native:test` checks that staged artifact and does not silently build a second copy. The `native` verification profile runs them in that order. The public integration uses real Chrome WebRTC with local provider and media fixtures. It exercises public Browser and Native owners, ACK versus model replies, generation attribution, owned decoded bytes and media leases, failure cleanup and joined shutdown. It contacts no live Reactor session. TURN relay qualification remains an explicit separate opt-in using the existing integration environment settings.
