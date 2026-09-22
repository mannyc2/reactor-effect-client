# Verification

`bun run verify` runs the same checks used by the CI and release jobs: wire
regeneration, formatting, all TypeScript projects, lint, build, portable runtime
imports and tests, native staging and tests, real local browser/native WebRTC,
and clean installed-package validation. Any failure stops the profile and
returns its nonzero exit status.

The profiles are `portable`, `native`, `package`, `release` and `full`. Print a
profile without executing it with `bun run verify --profile full --list`.
`portable` needs no native binary. `native` builds and stages the host binary
before checking it and running the public session integration. `package` checks
the already staged binaries without rebuilding them. `release` combines the
portable and package checks; its native matrix must have qualified the staged
artifacts first. `full` runs all three parts.

`test:portable` uses Bun discovery under `test/`, with the native, integration
and package-fixture directories excluded by `bunfig.toml`. `test:native` and
`test:integration` use the named Node/Vitest projects in `vitest.config.ts`.
There is no test filename registry. A new test in its runtime directory joins
the corresponding project automatically.

## Pinned tools and generated code

The package manifest pins Bun 1.4.2, TypeScript, Vitest, Oxlint and Oxfmt 0.70.0.
The formatter configuration disables import and package-key sorting and keeps
generated wire code and native source outside JavaScript formatting. Native
formatting is checked by `cargo fmt` with the pinned Rust 1.90.0 toolchain.
`tsconfig.tools.json` checks JavaScript scripts, the native staging script and
TypeScript tools; `tsconfig.integration.json` checks the browser integration
entry with its actual browser types.

`generate-wire.py` uses CPython 3.13 and only its standard library; CI pins
3.13.5. It reads every tracked Reactor proto and the tracked Google Struct
descriptor. The parser rejects syntax outside its supported canonical schema
subset, and the generator has no network or host-protoc dependency. The
descriptor reader, parser and TypeScript writer are versioned together as
`reactor-wire-ts/1`.

`bun run generate:check` regenerates in memory and fails on any byte difference
from `src/wire.generated.ts`. `bun run generate:wire` writes the regenerated
file. The wire version and attribution header are preserved; schema changes
must be reviewed alongside their generated diff. Do not format the generated
file or edit it independently of its canonical inputs.

## Installed-package and host evidence

`pack.ts` creates a fresh directory under `.check/pack-*` and preserves earlier
archives. It validates the actual tarball, exactly eight public exports, all
relative/declaration import closure, and declared external dependencies. The
portable and browser installations omit Koffi and use a resolver that rejects
dependencies outside their isolated directories. Their type consumers compile
without a workspace fallback; the Node declaration consumer has no DOM library.
The browser entry is also bundled from that installed package.

Each native sidecar must match its archived and staged binary SHA-256. All
platforms must share the native source identity, reconstructed again from the
native build inputs inside the tarball. The installed native preflight verifies
that same identity, fails before coordinator allocation for an invalid library,
and closes its fixture allocation through the public owner.

`native:build` is the staging entry point. `native:test` checks that staged
artifact and does not silently build a second copy. The `native` verification
profile runs them in that order. The public integration uses real Chrome
WebRTC with local provider and media fixtures. It exercises public Browser and
Native owners, ACK versus model replies, generation attribution, owned decoded
bytes and media leases, failure cleanup and joined shutdown. It contacts no
live Reactor session. TURN relay qualification remains an explicit separate
opt-in using the existing integration environment settings.
