# reactor-effect

An independent Effect SDK for Reactor's real-time video models: scoped sessions, the H3 provider, a playout that airs a keyed schedule across renewing sessions, and Reactor simulated in memory for tests. One Bun workspace publishes it as three packages, the native one with an addon package for each supported platform.

| Package                                        | Purpose                                                                                                                   | Runs in                |
| ---------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------- | ---------------------- |
| [`reactor-effect-client`](./packages/client)   | `Reactor` and `Session`, `Coordinator`, `H3`, `Playout` with `H3Source` and `LocalSource`, `ReactorTest`, the `Peer` port | Node, Bun and browsers |
| [`reactor-effect-browser`](./packages/browser) | `BrowserPeer` on the built-in `RTCPeerConnection`, and `BrowserMedia` for the session's DOM tracks and their playback     | Browsers               |
| [`reactor-effect-native`](./packages/native)   | `NativePeer` on a libwebrtc Node-API addon: decoded media, in process or in a child process; the addon ships per platform | Node and Bun           |

One `Session` owns each allocation or attachment: its commands, connection generations and cleanup evidence. A host package binds the session's `Peer` port and nothing more. `H3` reads that same session, and `Playout` gets its sessions from a source, `H3Source` for paid H3 or `LocalSource` for a local renderer, and never allocates around one. `ReactorTest` stands in for Reactor at the network edge, so the same application runs offline on the Effect clock.

This is not an official Reactor SDK. Protocol material and native WebRTC dependencies are attributed in [NOTICE](./NOTICE) and in each package's `notices/` directory. The [release workflow](./.github/workflows/release.yml) publishes every version to npm with provenance, and [CHANGELOG.md](./CHANGELOG.md) lists what each release changes and how to upgrade.

## Which package do I install?

- Every application installs `reactor-effect-client` and Effect `4.0.0-rc.117`. Its modules are flat: import one by its own subpath, such as `reactor-effect-client/Playout`, or all of them as namespaces from the root (`import { Playout, Reactor } from "reactor-effect-client"`). They are one package because they share exactly one dependency set and are all portable; splitting them would add installs without isolating anything.
- A transport is a separate package because it changes what gets installed: `reactor-effect-browser` compiles against DOM types only, and `reactor-effect-native` carries Node-only code and depends optionally on one package per platform, `reactor-effect-native-linux-x64-gnu` and `reactor-effect-native-darwin-arm64`, each holding only that platform's addon, so a host downloads only the addon it can run. Portable and browser consumers never download native binaries.
- The host packages pin `reactor-effect-client` as an exact peer, so an application always has one copy of the session contract. Each implements the client's `Peer` port (`reactor-effect-client/Peer`), which an application needs only to write a host of its own. Tests need no host: `ReactorTest.layer` provides the simulated coordinator and peers.

```sh
npm install reactor-effect-client effect@4.0.0-rc.117
npm install reactor-effect-browser                                      # browsers
npm install reactor-effect-native @effect/platform-node@4.0.0-rc.117    # Node
```

```ts
import { Layer } from "effect";
import * as FetchHttpClient from "effect/unstable/http/FetchHttpClient";
import * as Coordinator from "reactor-effect-client/Coordinator";
import * as Reactor from "reactor-effect-client/Reactor";
import { NativePeer } from "reactor-effect-native";

const reactorLayer = Reactor.layer().pipe(
  Layer.provide(Layer.mergeAll(Coordinator.layerConfig, NativePeer.layer())),
  Layer.provide(FetchHttpClient.layer),
);
```

The package READMEs document sessions and tokens, the H3 provider, the playout, `ReactorTest`, browser media and the native addon. The [examples](./examples/README.md) are four Effect applications, one for each shape an application takes: a server that broadcasts one renewing playout to many browsers, a page that runs its own session over WebRTC, a command line that captures one clip's decoded media to MP4, and an application service tested offline against `ReactorTest`.

## Workspace layout

```text
packages/client      reactor-effect-client   src/, test/ (Vitest on Node and Bun), wire/ (protocol sources), notices/
packages/browser     reactor-effect-browser  src/, test/ (Vitest on Node and Bun)
packages/native      reactor-effect-native   src/, test/ (Vitest on Node and Bun), rust/ (addon crate), npm/ (platform packages), scripts/
examples/livestream  private                 the live channel example: one playout broadcast to many browsers
packages/*/examples  private                 each package's own example
integration          private                 real Chrome/native WebRTC qualification, and the paid hosted checks in integration/hosted
scripts              workspace tooling       verify profiles, test runner, examples check, wire generation, installed-package smoke
release-tools        release application     release preparation, publication and recovery, on its own lockfile
```

Every workspace declares exactly the dependencies it uses; `bun install` uses the isolated linker, so an undeclared import fails to resolve instead of leaning on a hoisted copy. Versions shared by several workspaces are pinned once in the root [`package.json`](./package.json) catalog, and sibling packages depend on each other with `workspace:*`; `bun pm pack` rewrites both to exact versions in the published manifests.

## Development

Bun 1.4.2 (declared by `packageManager`) and Node.js 22 or newer; the examples and the integration runner run their TypeScript sources directly and need Node 22.18 or newer. `bun install` brings the pinned `buf` that generates the wire codec. The examples' video tests use `ffmpeg` when it is on `PATH` and skip without it; CI installs it. Native work additionally needs Rust 1.90 and the toolchain described in the [native README](./packages/native/README.md).

```sh
bun install --frozen-lockfile
bun run verify --profile portable   # wire check, format, build, lint, typecheck, examples, portable tests
```

| Command                               | What it does                                                                                           |
| ------------------------------------- | ------------------------------------------------------------------------------------------------------ |
| `bun run build`                       | Compiles every package in dependency order (`bun run --filter './packages/*' build`)                   |
| `bun run typecheck`                   | Checks every workspace's sources, tests and tooling against the built declarations                     |
| `bun run lint`                        | oxlint with the type-aware rules; unused suppressions fail                                             |
| `bun run test`                        | The client, browser and hosted-rehearsal Vitest suites on Node and on Bun, then the scripts' own tests |
| `bun run check:examples`              | Each example's offline tests on Node and Bun, and the browser bundles                                  |
| `bun run test:native`                 | Vitest in `packages/native` against the staged addon, on Node and then on Bun                          |
| `bun run test:integration`            | A real local browser/native session through the public packages                                        |
| `bun run test:pack`                   | Packs each package, validates the archives and installs them into isolated consumers                   |
| `bun run native:build`                | Builds the addon for the current host and stages it into its package under `packages/native/npm/`      |
| `bun run generate:wire`               | Regenerates the wire codec with `buf`; `generate:check` fails on any difference                        |
| `bun run --filter <package> <script>` | Any package script, for example `bun run --filter reactor-effect-client test`                          |

`bun run verify` runs the same profiles CI uses (`portable`, `runtime`, `native`, `package`, `release`, `full`); see [`scripts/README.md`](./scripts/README.md). Hosts without a staged addon can still validate packaging with `bun --no-env-file scripts/pack.ts --portable`.

## Continuous integration

The [CI workflow](./.github/workflows/ci.yml) runs the portable verification once, the portable runtime tests on an OS/Node matrix, and the native qualification per platform. On pull requests the native job keys a cache of the staged addon on the native source identity that `packages/native/scripts/stage.mjs --source-hash` computes, together with the build recipe, toolchain and runner image; when none of those changed it restores the qualified addon and the test far peer, restages it against the sources, runs only the JavaScript native tests (on Node and on Bun) and the integration tests, and skips the Rust toolchain entirely. A run on `main` always builds the addon it qualifies. The package job then installs the five archives, the two platform packages among them, into isolated consumers and uploads the validated tarballs; on `main` it also stamps `qualification.json`, binding the five archives to that commit, tree, run and attempt, and uploads them together with `package-identity.json` as the flat `npm-package` artifact.

## Releases

Publication is manual and separate from CI. The [release workflow](./.github/workflows/release.yml) never builds the SDK: a `prepare` run adopts the five archives from a successful main CI run, signs Sigstore provenance for each and retains an immutable ts-release candidate; a separate `publish` run promotes that candidate only after an operator enters the exact `publish reactor-effect-client@<version> reactor-effect-browser@<version> reactor-effect-native-linux-x64-gnu@<version> reactor-effect-native-darwin-arm64@<version> reactor-effect-native@<version>` confirmation printed by the preparation. All five packages share one version; `reactor-effect-client` is published first because the host packages pin it as an exact peer, and the platform packages before `reactor-effect-native`, which pins them exactly. npm trusted publishing (OIDC) replaces any token, and an `observe` run re-checks registry visibility without publishing. [release-tools/README.md](./release-tools/README.md) documents the npm prerequisites, the procedure and recovery.

## Support and limitations

The native media path was last measured on September 23, 2026, before the addon moved to napi-rs; the queues, owner thread and bounds it measured are unchanged, and the media load tests pass on the addon on linux-x64. A local libwebrtc far peer sent 1344x768 BGRA at 24 fps with its congestion controller held at 8 Mbps.

- On linux-x64, in a 4 vCPU container and on GitHub's runner, every session outside the stall test received 23–24 frames per second, and the bridge never held more than one frame. Audio arrived at about 100 blocks per second with none dropped, and control round trips stayed under 21 ms at p95. End-to-end latency, which the tests print but do not assert, was 19–95 ms at p95 outside the stalls.
- On GitHub's darwin-arm64 runner (3 cores, utility QoS), sessions received 21.6–24.3 frames per second, the bridge held at most one frame at p95 and no audio was dropped. End-to-end latency reached 332 ms at p95, and audio arrived at 50–65 blocks per second, because the far peer's pushes ran 20–60 ms late there. Neither delay is the bridge's.
- With one CPU-bound process per core, Node passed the whole suite. Bun passed throughput, stall and renewal, but its two-session readers fell just outside their bounds: 3 frames dropped where 2 were allowed, or 4 held at p95 where 2 were.

Hosted Reactor is exercised only by the maintainer-run, paid checks in [`integration/hosted`](./integration/hosted/README.md); CI rehearses every one of them on `ReactorTest` and never allocates a paid session. They first ran on September 24, 2026 ([record](./integration/hosted/evidence/0.3.0-rc.0/summary.md)): 1344x768 video at 24 fps over a direct path, a clip accepted by its correlated reply, and an attach 2.7 s after the owner's death that received frames 0.2 s later. Later runs measured H3's queue, seams, renewal handoff and cut ([0.6.0](./integration/hosted/evidence/0.6.0/summary.md), [0.7.0](./integration/hosted/evidence/0.7.0/summary.md)). The current API has run two of them from a checkout ([record](./integration/hosted/evidence/0.8.0-dev/summary.md)): `tokens`, which adopted a session with a bound token after the token that created it expired and carried a clip with reference audio on a refreshed token, and `cut`, whose fenced cut stopped one clip with no dark frame at the seam and whose flagged prompt showed moderation's verdict naming no request. The other checks have not run on it, and its published bytes have not been qualified against hosted Reactor. TURN relays with hosted credentials, physical hardware and native-to-browser media publication have not been exercised; a loopback Coturn relay and Chrome-to-native media have. A provider `Started` event is not proof of presented or encoded output.

## Contributing and security

See [CONTRIBUTING.md](./CONTRIBUTING.md) for the workspace workflow, validation expectations and third-party notice rules. See [SECURITY.md](./SECURITY.md) for private vulnerability reporting guidance and the project's security boundaries.

## License

The packages are licensed under Apache-2.0. Third-party and derived-source notices are retained in [NOTICE](./NOTICE) and in each package's `notices/` directory.
