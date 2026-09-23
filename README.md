# reactor-effect

An independent Effect SDK for scoped Reactor sessions, H3 provider state, host media, and explicit orchestration, published as three packages from one Bun workspace.

| Package                                        | Purpose                                                                                                   | Runs in                |
| ---------------------------------------------- | --------------------------------------------------------------------------------------------------------- | ---------------------- |
| [`reactor-effect-client`](./packages/client)   | Canonical `Client`/`Session`, coordinator, H3 provider, orchestration, simulation, test fixtures, wire    | Node, Bun and browsers |
| [`reactor-effect-browser`](./packages/browser) | Built-in `RTCPeerConnection` host: generation-scoped tracks, media conversion, recording                  | Browsers               |
| [`reactor-effect-native`](./packages/native)   | Rust libwebrtc bridge over Koffi: decoded media generations, explicit file upload, staged native binaries | Node and Bun           |

One canonical `Session` owns each allocation or attachment, its commands, connection generations, and cleanup evidence. The host packages select transport capabilities beneath it. H3 consumes that same session and exposes provider state, and applications opt into orchestration and simulation when they need scheduling, sequence affinity, or renewal.

This is not an official Reactor SDK. Protocol material and native WebRTC dependencies are attributed in [NOTICE](./NOTICE) and in each package's `notices/` directory. No SDK version has been published yet: `reactor-effect-client` on npm is a `0.0.0-reserved.0` placeholder, and the [release workflow](./.github/workflows/release.yml) is configuration until a maintainer dispatches it.

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

The package READMEs document the session contract, coordinator helpers, H3 provider, orchestration, browser media and the native bridge. The [compiled examples](./examples/README.md) cover canonical sessions, both host compositions, H3 acceptance evidence, explicit renewal and offline simulation.

## Workspace layout

```text
packages/client      reactor-effect-client   src/, test/ (Bun), wire/ (proto inputs + generator), notices/
packages/browser     reactor-effect-browser  src/, test/ (Bun)
packages/native      reactor-effect-native   src/, test/ (Node/Vitest), rust/ (crate), lib/ (staged binaries), scripts/
packages/test-kit    private                 runner-agnostic assertion helpers and host fakes shared by the suites
examples             private                 documentation examples compiled against the built packages
integration          private                 real Chrome/native WebRTC qualification (Node/Vitest) and its browser bundle
scripts              workspace tooling       verify profiles, test runner, architecture check, installed-package smoke
```

Every workspace declares exactly the dependencies it uses; `bun install` uses the isolated linker, so an undeclared import fails to resolve instead of leaning on a hoisted copy. Versions shared by several workspaces are pinned once in the root [`package.json`](./package.json) catalog, and sibling packages depend on each other with `workspace:*`; `bun pm pack` rewrites both to exact versions in the published manifests.

## Development

Bun 1.4.2 (declared by `packageManager`), Node.js 22 or newer and CPython 3.13 for the wire generator. Native work additionally needs Rust 1.90 and the toolchain described in the [native README](./packages/native/README.md).

```sh
bun install --frozen-lockfile
bun run verify --profile portable   # generation, format, lint, build, typecheck, architecture, examples, portable tests
```

| Command                               | What it does                                                                                        |
| ------------------------------------- | --------------------------------------------------------------------------------------------------- |
| `bun run build`                       | Compiles every package in dependency order (`bun run --filter './packages/*' build`)                |
| `bun run typecheck`                   | Checks every workspace's tests and tooling against the built declarations                           |
| `bun run test`                        | Portable suites under Bun (`packages/client`, `packages/browser`, `integration` helpers, `scripts`) |
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

## Qualification evidence

### ABI 3 media path

On September 23, 2026, in a Linux x64 container with 4 vCPUs, Rust 1.90.0, Clang 21.1.8, Node 24.15.0 and Bun 1.4.2, `bun run native:test` passed: Rust formatting, all 16 Rust tests including the real libwebrtc loopback, clippy with warnings denied, and all 19 JavaScript native tests on Node and again on Bun. Those include the media load tests against a local libwebrtc far peer sending 1344x768 BGRA at 24 fps, with its congestion controller held at 8 Mbps. On both runtimes each session outside the stall test received 23–24 frames per second, and the bridge never held more than one frame in any sample; audio reached JavaScript at about 100 blocks per second with none dropped, and control-channel round trips stayed under 21 ms at p95. The end-to-end latency the tests print, which they do not assert, was 19–95 ms at p95 outside the stalls. With one CPU-bound process per core and the tests at nice 5, Node passed the whole suite in both of two runs. Bun passed the throughput, stall and renewal tests, but its two-session readers fell just outside their bounds, dropping 3 frames where 2 were allowed in one run and holding 4 frames at p95 where 2 were allowed in the other. No observation queue overflowed.

The pull request's CI passed the same suite on GitHub's linux-x64 and darwin-arm64 runners, on Node and on Bun, together with the Chrome integration test on each. The macOS runner has 3 cores and runs job processes at utility QoS (priority 20). There sessions outside the stall test received between 21.6 and 24.3 frames per second, the bridge held at most one frame at p95, no audio was dropped, and control round trips stayed under 19 ms at p95. End-to-end latency, however, reached 332 ms at p95, and received audio arrived at 50–65 blocks per second. On that runner the far peer's pushes ran 20–60 ms behind their schedule, against under 8 ms on Linux, and reactor-webrtc's synthetic playout loop, which paces received audio, drops such lateness rather than repaying it. Neither delay is the bridge's, so the tests print both rather than assert them. Getting the load tests to pass there took three changes. `build.rs` now links the far-peer example against compiler-rt, because Cargo passes `rustc-link-lib` only to the library target. The far peer holds its congestion controller at a fixed rate; on the busy runner its estimate had fallen to about 600 kbps and its encoder sent 2–4 frames per second. And the native job now stops Spotlight, whose indexing of the build was using about one of the three cores.

Run against the previous ABI 2 bridge on Bun, an earlier version of these tests, which asserted delivery against the far peer's sent count and end-to-end latency, failed throughput, stall and renewal: 54 frames arrived where at least 220 were required, a 250 ms stall dropped 39 frames, and the renewal overlap dropped 710 audio blocks. On Node, ABI 2 failed the two-session test with 285 audio blocks dropped in 10 s. The CI portable profile, the release-tool tests, the pack/install smoke (whose installed native consumer preflights ABI 3) and the public browser/native integration project with Chromium also passed. In the container the browser runner needed `BROWSER_NATIVE_NO_SANDBOX=1` because it runs as root. TURN relays, physical hardware and hosted Reactor were not exercised.

### Canonical API checks

This record and the retained ones below predate the workspace split; their paths and script names refer to the previous single-package layout, and the API contracts they describe are unchanged.

On September 22, 2026, the focused TypeScript check passed and `scripts/native-test.sh` completed Rust formatting checks, all 12 Rust tests, clippy with warnings denied, and all 15 JavaScript native tests on macOS arm64. The Rust tests include the real libwebrtc media loopback. The JavaScript tests use the staged ABI 2 library and explicit C fixtures to cover the canonical session, ABI validation, source and byte identity, decoded-media parsing, interrupted foreign-call lifetime, and media failure/cleanup. The public browser/native integration project also passed through the canonical factories: both ordered binary channels, shared command return/event attribution, browser track leases and borrowed publication sources, failed-acquisition cleanup, 12 distinct decoded 160x96 BGRA frames, and 95 mono 48 kHz PCM packets were observed. Both owners joined cleanup without local errors and confirmed their fixture allocations closed.

That run verified current native sources and the embedded identities plus sidecar hashes of the staged macOS and Linux libraries against source SHA-256 `d2584076f542ea12a964eecf8384c8b93b013672388351e1998571b0b3c69d37`; it did not rebuild or replace the staged release libraries. It exercised the macOS library, with TURN disabled. Linux runtime execution, relay behavior, and Reactor custom browser frame metadata were not requalified by this check.

### Retained earlier qualification

The following retained September 22 records predate the canonical API migration; their historical test counts and names describe those runs. Changes to the public API, fixtures, and packaged declarations need their own focused validation, even when the native source and staged libraries are unchanged.

- macOS arm64 native qualification ran locally on September 22, 2026: 8 Rust native tests, clippy with warnings denied, a release build, and 8 JavaScript native boundary tests passed. The Rust loopback used real local libwebrtc ICE/DTLS/SCTP, ordered binary channels, real video encode/decode, PCM, metadata, stats, direction/bitrate controls, and callback quiescence. A separate compiled test ABI drove the JavaScript `SessionClient` media parser and cancellation bounds.
- Linux x64 qualification also ran on September 22, 2026, inside an isolated Debian 12 container in a task-owned Lima VM, using Rosetta for x86_64 execution on the arm64 Mac. Rust 1.90.0, Clang 21.1.8, all 8 native Rust tests, clippy with warnings denied, the release build, and all 8 JavaScript ABI/parser/session tests passed. The shared library loaded through the public native entry point in the pinned Node 24.14.1 Bookworm runtime and Bun 1.4.2 with glibc 2.36. This is Linux x64 userspace execution under translation, not qualification on physical x64 hardware.
- The Linux check reproduced a build-recipe failure with Bookworm's default Clang 14: it cannot compile the pinned WebRTC libc++ headers. The source-build recipes now explicitly select Clang 21. The opt-in installer verified the LLVM signing-key fingerprint and ran successfully in the private Debian container; ordinary native build scripts never install system packages.
- A real Chrome 153 browser loaded the browser bundle and exchanged exact binary messages over both channels with the SDK browser peer and the native bridge. Browser-generated media decoded natively as changing 160x96 BGRA frames and mono 48 kHz PCM. A separate run through a loopback-only Coturn 4.18.0 fixture proved selected relay-to-relay UDP connectivity, both binary channels, and decoded audio/video. These checks use the browser/native runner now under `integration/scripts/`; native cleanup failures fail the check.
- Local Mac iteration still used Bun 1.4.0 because the task-local Bun 1.4.2 binary stalled before JavaScript execution in that host harness. Linux runtime preflight did execute Bun 1.4.2 successfully. CI pins 1.4.2 explicitly.
- No paid or hosted Reactor generation was run. The local relay test does not prove hosted TURN credentials, Internet NAT/firewall traversal, or sustained production behavior. Chrome did not originate Reactor's custom frame metadata; that extension remains covered by the native/native loopback, not the browser test. Browser-to-native media was exercised; native-to-browser media publication was not.

## Contributing and security

See [CONTRIBUTING.md](./CONTRIBUTING.md) for the workspace workflow, validation expectations and third-party notice rules. See [SECURITY.md](./SECURITY.md) for private vulnerability reporting guidance and the project's security boundaries.

## License

The packages are licensed under Apache-2.0. Third-party and derived-source notices are retained in [NOTICE](./NOTICE) and in each package's `notices/` directory.
