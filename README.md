# reactor-effect

An independent Effect SDK for scoped Reactor sessions, H3 provider state, host media, and explicit orchestration, published as three packages from one Bun workspace.

| Package                                        | Purpose                                                                                                   | Runs in                |
| ---------------------------------------------- | --------------------------------------------------------------------------------------------------------- | ---------------------- |
| [`reactor-effect-client`](./packages/client)   | Canonical `Client`/`Session`, coordinator, H3 provider, orchestration, simulation, test fixtures, wire    | Node, Bun and browsers |
| [`reactor-effect-browser`](./packages/browser) | Built-in `RTCPeerConnection` host: generation-scoped tracks, media conversion, recording                  | Browsers               |
| [`reactor-effect-native`](./packages/native)   | Rust libwebrtc bridge over Koffi: decoded media generations, explicit file upload, staged native binaries | Node and Bun           |

One canonical `Session` owns each allocation or attachment, its commands, connection generations, and cleanup evidence. The host packages select transport capabilities beneath it. H3 consumes that same session and exposes provider state, and applications opt into orchestration and simulation when they need scheduling, sequence affinity, or renewal.

This is not an official Reactor SDK. Protocol material and native WebRTC dependencies are attributed in [NOTICE](./NOTICE) and in each package's `notices/` directory. Version 0.3.1 of all three packages is npm's `latest`, published with provenance by the [release workflow](./.github/workflows/release.yml). 0.3.0's sources passed the [hosted qualification](./integration/hosted) against hosted Reactor, and 0.3.1 adds H3 reference audio, whose own hosted check has not run yet. 0.3.0 changed the 0.2.0 public API incompatibly, so a `^0.2.0` range does not include it. [CHANGELOG.md](./CHANGELOG.md) lists what each release changes.

## Which package do I install?

- Every application installs `reactor-effect-client` and Effect `4.0.0-rc.115`. Its modules (`/h3`, `/orchestration`, `/simulation`, `/testing`, `/wire`) are subpaths of one package because they share exactly one dependency set and are portable; splitting them would add installs without isolating anything.
- A transport is a separate package because it changes what gets installed: `reactor-effect-browser` compiles against DOM types only, and `reactor-effect-native` carries the optional Koffi dependency, Node-only code and the staged shared libraries. Portable and browser consumers never download native binaries.
- The host packages pin `reactor-effect-client` as an exact peer, so an application always has one copy of the session contract. They reach the internals they need through the published `reactor-effect-client/host` module; applications never need it.

```sh
npm install reactor-effect-client effect@4.0.0-rc.115
npm install reactor-effect-browser                                      # browsers
npm install reactor-effect-native @effect/platform-node@4.0.0-rc.115    # Node
```

```ts
import { Effect, Layer } from "effect";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as Reactor from "reactor-effect-client";
import * as Native from "reactor-effect-native";

const clientLayer = Reactor.layer().pipe(
  Layer.provide(Layer.mergeAll(Reactor.FetchHttp.layer, NodeServices.layer, Native.layer())),
);
```

The package READMEs document the session contract, coordinator helpers, H3 provider, orchestration, browser media and the native bridge. The [examples](./examples/README.md) are four Effect applications, one for each shape an application takes: a server that broadcasts one renewing orchestration to many browsers, a page that runs its own session over WebRTC, a command line that captures one clip's decoded media to MP4, and an application service tested offline against the simulation.

## Workspace layout

```text
packages/client      reactor-effect-client   src/, test/ (Vitest on Node and Bun), wire/ (proto inputs + generator), notices/
packages/browser     reactor-effect-browser  src/, test/ (Vitest on Node and Bun)
packages/native      reactor-effect-native   src/, test/ (Vitest on Node and Bun), rust/ (crate), lib/ (staged binaries), scripts/
packages/test-kit    private                 runner-agnostic assertion helpers and host fakes shared by the suites
examples/livestream  private                 the live channel example: one orchestration broadcast to many browsers
packages/*/examples  private                 each package's own example
integration          private                 real Chrome/native WebRTC qualification (Node/Vitest) and its browser bundle
scripts              workspace tooling       verify profiles, test runner, architecture check, examples check, installed-package smoke
```

Every workspace declares exactly the dependencies it uses; `bun install` uses the isolated linker, so an undeclared import fails to resolve instead of leaning on a hoisted copy. Versions shared by several workspaces are pinned once in the root [`package.json`](./package.json) catalog, and sibling packages depend on each other with `workspace:*`; `bun pm pack` rewrites both to exact versions in the published manifests.

## Development

Bun 1.4.2 (declared by `packageManager`), Node.js 22 or newer and CPython 3.13 for the wire generator. The examples' video tests use `ffmpeg` when it is on `PATH` and skip without it; CI installs it. Native work additionally needs Rust 1.90 and the toolchain described in the [native README](./packages/native/README.md).

```sh
bun install --frozen-lockfile
bun run verify --profile portable   # generation, format, lint, build, typecheck, architecture, examples, portable tests
```

| Command                               | What it does                                                                                        |
| ------------------------------------- | --------------------------------------------------------------------------------------------------- |
| `bun run build`                       | Compiles every package in dependency order (`bun run --filter './packages/*' build`)                |
| `bun run typecheck`                   | Checks every workspace's tests and tooling against the built declarations                           |
| `bun run test`                        | Client and browser Vitest suites on Node and on Bun; the `integration` helpers and `scripts` on Bun |
| `bun run test:native`                 | Vitest tests in `packages/native` against the staged library, on Node and then on Bun               |
| `bun run test:integration`            | Real local browser/native session through the public packages                                       |
| `bun run test:pack`                   | Packs each package, validates the archives and installs them into isolated consumers                |
| `bun run check:architecture`          | Compiler-backed layering, host-boundary, dependency and cycle check for every package               |
| `bun run native:build`                | Builds the Rust bridge for the current host and stages it under `packages/native/lib/`              |
| `bun run --filter <package> <script>` | Any package script, for example `bun run --filter reactor-effect-client test`                       |

`bun run verify` runs the same profiles CI uses (`portable`, `runtime`, `native`, `package`, `release`, `full`); see [`scripts/README.md`](./scripts/README.md). Hosts without a staged native library can still validate packaging with `bun --no-env-file scripts/pack.ts --portable`.

## Continuous integration

The [CI workflow](./.github/workflows/ci.yml) runs the portable verification once, the portable runtime tests on an OS/Node matrix, and the native qualification per platform. On pull requests the native job keys a cache of the staged library on the native source identity that `packages/native/scripts/stage.mjs --source-hash` computes, together with the build recipe, toolchain and runner image; when none of those changed it restores the qualified library and the test far peer, runs only the JavaScript native tests (on Node and on Bun) and the integration tests, and skips the Rust toolchain entirely. A run on `main` always builds the library it qualifies. The package job then installs the three archives into isolated consumers and uploads the validated tarballs; on `main` it also stamps `qualification.json`, binding the three archives to that commit, tree, run and attempt, and uploads them together with `package-identity.json` as the flat `npm-package` artifact.

## Releases

Publication is manual and separate from CI. The [release workflow](./.github/workflows/release.yml) never builds the SDK: a `prepare` run adopts the three archives from a successful main CI run, signs Sigstore provenance for each and retains an immutable ts-release candidate; a separate `publish` run promotes that candidate only after an operator enters the exact `publish reactor-effect-client@<version> reactor-effect-browser@<version> reactor-effect-native@<version>` confirmation printed by the preparation. All three packages share one version, and `reactor-effect-client` is published first because the host packages pin it as an exact peer. npm trusted publishing (OIDC) replaces any token, and an `observe` run re-checks registry visibility without publishing. [release-tools/README.md](./release-tools/README.md) documents the npm prerequisites, the procedure and recovery.

## Support and limitations

The native media path was last measured with the ABI 3 bridge on September 23, 2026; ABI 4 changes only the frame and PCM headers, which now carry an admission sequence. A local libwebrtc far peer sent 1344x768 BGRA at 24 fps with its congestion controller held at 8 Mbps.

- On linux-x64, in a 4 vCPU container and on GitHub's runner, every session outside the stall test received 23–24 frames per second, and the bridge never held more than one frame. Audio arrived at about 100 blocks per second with none dropped, and control round trips stayed under 21 ms at p95. End-to-end latency, which the tests print but do not assert, was 19–95 ms at p95 outside the stalls.
- On GitHub's darwin-arm64 runner (3 cores, utility QoS), sessions received 21.6–24.3 frames per second, the bridge held at most one frame at p95 and no audio was dropped. End-to-end latency reached 332 ms at p95, and audio arrived at 50–65 blocks per second, because the far peer's pushes ran 20–60 ms late there. Neither delay is the bridge's.
- With one CPU-bound process per core, Node passed the whole suite. Bun passed throughput, stall and renewal, but its two-session readers fell just outside their bounds: 3 frames dropped where 2 were allowed, or 4 held at p95 where 2 were.

TURN relays with hosted credentials, physical hardware and native-to-browser media publication have not been exercised; a loopback Coturn relay and Chrome-to-native media have. Hosted Reactor was first exercised on September 24, 2026, by the maintainer-run, opt-in checks in `integration/hosted/qualify.ts` ([the record](./integration/hosted/evidence/0.3.0-rc.0/summary.md)). The published 0.3.0-rc.0 failed its vertical and takeover on three library causes. The fixes, released in 0.3.0, passed both from a checkout: 1344x768 video at 24 fps over a direct path, a clip accepted by its correlated reply, and an attach 2.7 s after the owner's death that received frames 0.2 s later. [The plan](./integration/hosted/README.md) says what each spending limit buys. At the published $0.75 a billed minute, $1.50 covers a one-session vertical check (correlated acceptance, lifecycle progression, changing non-black BGRA frames, audio when offered, the ICE pair carrying the media and confirmed termination) and a process-kill-and-attach takeover within 5 s of the owner's death; $2.25 adds hosted TURN, run only if neither selected a relay pair. Each check writes its evidence, never a token, to a new file, and CI rehearses every check against a local twin of hosted Reactor. [CHANGELOG.md](./CHANGELOG.md) keeps the dated qualification of each release, 0.3.0's hosted runs included.

## Contributing and security

See [CONTRIBUTING.md](./CONTRIBUTING.md) for the workspace workflow, validation expectations and third-party notice rules. See [SECURITY.md](./SECURITY.md) for private vulnerability reporting guidance and the project's security boundaries.

## License

The packages are licensed under Apache-2.0. Third-party and derived-source notices are retained in [NOTICE](./NOTICE) and in each package's `notices/` directory.
