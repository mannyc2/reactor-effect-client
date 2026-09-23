# reactor-effect-native

Native WebRTC host for [`reactor-effect-client`](https://www.npmjs.com/package/reactor-effect-client). `reactor-effect-native` is a small Rust `cdylib` over the pinned `reactor-webrtc` revision, loaded through the optional [Koffi](https://koffi.dev) FFI dependency. Reactor session ownership, HTTP, command correlation, cancellation, and model policy stay in `reactor-effect-client`; this package owns transport and decoded media only.

This is not an official Reactor SDK. Native WebRTC dependencies are attributed in [NOTICE](./NOTICE) and [`notices/`](./notices/).

## Install

```sh
npm install reactor-effect-client reactor-effect-native effect@4.0.0-rc.115 @effect/platform-node@4.0.0-rc.115
```

`reactor-effect-client` and Effect `4.0.0-rc.115` are exact peer dependencies. Koffi is an optional dependency; it and the shared library are loaded only when native preflight runs, so merely importing this module remains safe when the optional dependency is absent. `@effect/platform-node` supplies the Node services an application provides around its scoped operation; a consumer using that prerelease should retain the root override `"@effect/platform-node-shared": "4.0.0-rc.115"`, because the platform's caret range otherwise permits a later prerelease with a different Effect peer.

## Usage

```ts
import { Effect, Layer } from "effect";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as Reactor from "reactor-effect-client";
import * as Native from "reactor-effect-native";

const clientLayer = Reactor.layer().pipe(
  Layer.provide(Layer.mergeAll(Reactor.FetchHttp.layer, NodeServices.layer, Native.layer())),
);
```

`Native.make(configuration, nativeOptions)` constructs the canonical factory with the native peer already selected, and `Native.layer(nativeOptions)` supplies the `PeerFactory` for `Reactor.layer()`. Constructing a factory makes no allocation. `nativeOptions.libraryPath` can select a staged native artifact; its caller owns artifact provenance. The separate `Native.uploadFile` helper requires the host file services when used.

`Native.media(session)` obtains a decoded-media generation after the session connects. `media.video(name)` emits owned BGRA frames and `media.audio(name)` emits owned interleaved signed 16-bit PCM. Frame IDs and microsecond timestamps use `bigint`. Retaining a frame retains JavaScript-owned bytes, and native media has no browser track handles. Media values stay bound to their negotiated generation: a reconnect creates a new generation and existing readers end or fail with their source.

Native transport provides:

- STUN/TURN ICE configuration and offerer signaling;
- reliable ordered binary `control` and `data` channels;
- transceiver direction and sender bitrate controls;
- WebRTC statistics;
- owned decoded BGRA video with Reactor frame metadata;
- owned interleaved signed 16-bit PCM audio;
- immediate close fencing plus joined callback quiescence during shutdown.

The current public native peer accepts at most one incoming video track and one incoming audio track. Pinned `reactor-webrtc` delivers `RemoteTrack` callbacks without the transceiver MID/native identity needed to join multiple same-kind callbacks to SDP mappings without relying on arrival order. `NativePeer.prepare` therefore rejects an ambiguous declaration with `UnsupportedCapability` before native negotiation. Multiple outgoing tracks remain distinct by their declared transceivers.

## Package layout

| Path                                 | Contents                                                                                 |
| ------------------------------------ | ---------------------------------------------------------------------------------------- |
| `dist/`                              | The compiled TypeScript entry point and its declarations                                 |
| `lib/<platform>-<arch>/`             | The staged shared library and its `native-identity.json` sidecar for each shipped host   |
| `rust/`                              | The `reactor-effect-native` crate: Cargo manifest and lock, build script, header, source |
| `scripts/build.sh`                   | Builds the crate for the current host and stages it                                      |
| `scripts/stage.sh`, `stage.mjs`      | Stage an already built library; `stage.mjs` is the single staging owner                  |
| `scripts/install-linux-toolchain.sh` | Explicit root-only LLVM 21 installer for opted-in Debian/Ubuntu build environments       |

The bridge owns one libwebrtc peer on a Rust thread. libwebrtc callbacks copy their data into bounded Rust queues; they never invoke JavaScript. ABI 2 retains the exact packet selected by a size probe until it is copied or the queue is closed. Its bytes and item still count toward the queue limits. Producer pressure may evict other queued media; it cannot change the reader's packet.

`close()` fences new callback and host-call admission immediately. The host registers every operation before dispatching to Koffi and keeps that ownership until the foreign call actually returns, even when its Effect waiter is interrupted. `shutdown()` drains those active and queued foreign calls, joins the Rust owner and callback guards, then destroys the handle once. The C ABI requires other hosts to perform the same foreign-call drain before destruction. Joining the Rust owner alone cannot observe calls queued in a host executor.

## Build and stage

Native code requires Rust 1.90, Clang 21 on Linux (the platform compiler on macOS), curl, tar with zstd support (or `zstd`), and a SHA-256 tool. From the workspace root:

```sh
bun run native:build # sh packages/native/scripts/build.sh
```

The pinned `reactor-webrtc-sys` build downloads its matching libwebrtc prebuilt and verifies the published SHA-256 before linking. macOS prebuilts target macOS 13.0 or later. The script uses the checked-in Cargo configuration and stages the result under `lib/<platform>-<arch>/`. `scripts/stage.mjs` verifies the library's embedded ABI, target, release profile and source SHA-256 against the current native build inputs, then writes the binary and `native-identity.json` with its SHA-256 and build identity. The source identity covers the Cargo manifest/lock, build script, Cargo configuration, C header and Rust source; build identity also records compiler versions and relevant build flags. A stale binary is rejected before it can be staged. `node scripts/stage.mjs --source-hash` prints the identity of the current sources; pull-request CI keys its staged-artifact cache on it (with the build recipe, toolchain and runner image) and skips the Rust build when none of them changed, while a run on `main` always builds the library it qualifies.

Native tests and normal installed-package preflight use this staged artifact. Preflight verifies the binary hash and compares its loaded build identity with the sidecar. There is no fallback to `rust/target/debug` or `release`.

Linux native builds require LLVM/Clang 21. The pinned libwebrtc prebuilt ships the matching libc++ headers, and older distro Clang releases are not a supported compiler for this native bridge. `scripts/build.sh` and `scripts/test.sh` never install system packages: on Linux they use an explicit `CC`/`CXX` when supplied, otherwise they require `clang-21` and `clang++-21` on `PATH` and fail with a toolchain-specific error if unavailable. macOS compiler selection is unchanged.

For opted-in Debian 12 (bookworm) or Ubuntu 24.04 (noble) build environments, the package includes an explicit root-only installer:

```sh
sudo ./packages/native/scripts/install-linux-toolchain.sh
```

The installer uses the official apt.llvm.org LLVM 21 repository, verifies the repository signing key fingerprint `6084F3CF814B57C1CF12EFD515CF4D18AF4F7421` before adding the repository, and installs only the `clang-21` toolchain package after its bootstrap HTTPS/GPG requirements. The pinned libwebrtc artifact supplies the libc++/libc++abi headers and static archives used by the glue build, so system libc++ packages are intentionally not installed. The installer does not invoke `sudo` itself and is only called explicitly by Docker/CI recipes; ordinary local build/test scripts never modify the host package set.

## Qualification

Local qualification is credential-free:

```sh
bun run native:test # sh packages/native/scripts/test.sh
```

The script checks Rust formatting, runs the Rust tests and clippy with warnings denied, then runs the JavaScript ABI/parser/session-boundary tests (Node/Vitest in `packages/native/test`) against the staged artifact; it never restages a second release library. The loopback test negotiates two local peers and exercises real libwebrtc ICE/DTLS/SCTP, both binary channels, video encode/decode, PCM audio, per-frame metadata, stream stats, direction changes, bitrate controls, and callback quiescence. It does not contact Reactor or generate paid media.

Additional tests force differently sized media eviction between the size probe and copying calls, and block foreign calls while interrupting their Effect waiters. The lifetime fixture drains queued calls while one active call remains held, verifies that shutdown/destruction have not run, then releases the final call and verifies one destruction. Its tombstone reports unsafe ordering without intentionally dereferencing freed memory. These checks establish ownership and ordering; they do not claim an observed heap-corruption incident.

`scripts/stage.sh` stages an already-built shared library under the package runtime layout, `lib/<platform>-<arch>/`. For example:

```sh
./packages/native/scripts/stage.sh packages/native/rust/target/release/libreactor_effect_native.dylib darwin-arm64
```

Linux x64 has a reproducible container build that starts from the package source, installs Rust 1.90 plus the package-owned LLVM 21 recipe and auxiliary archive tools, runs the native Rust tests and clippy, and exports only the resulting shared library. It requires an explicit Docker context so the script never changes the caller's active context:

```sh
DOCKER_CONTEXT=default bun run native:linux-x64
```

That stages `lib/linux-x64/libreactor_effect_native.so`. The final runtime image only needs the packaged shared object and its normal glibc system dependencies; Cargo, clang and the libwebrtc archive are build-time inputs.

## License

Apache-2.0. See [NOTICE](./NOTICE) and [`notices/`](./notices/).
