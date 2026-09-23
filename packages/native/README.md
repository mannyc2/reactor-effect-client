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
- typed failure classes;
- immediate close fencing plus joined callback quiescence during shutdown.

The current public native peer accepts at most one incoming video track and one incoming audio track. Pinned `reactor-webrtc` delivers `RemoteTrack` callbacks without the transceiver MID/native identity needed to join multiple same-kind callbacks to SDP mappings without relying on arrival order. `NativePeer.prepare` therefore rejects an ambiguous declaration with `UnsupportedCapability` before native negotiation. Multiple outgoing tracks remain distinct by their declared transceivers.

## Package layout

| Path                                 | Contents                                                                                                     |
| ------------------------------------ | ------------------------------------------------------------------------------------------------------------ |
| `dist/`                              | The compiled TypeScript entry point and its declarations                                                     |
| `lib/<platform>-<arch>/`             | The staged shared library and its `native-identity.json` sidecar for each shipped host                       |
| `rust/`                              | The `reactor-effect-native` crate: Cargo manifest and lock, lint configuration, build script, header, source |
| `rust/examples/far_peer/`            | Test-only libwebrtc sender for the media load tests; not packaged or staged                                  |
| `rust-toolchain.toml`                | The Rust toolchain the native scripts use, the one CI pins                                                   |
| `scripts/build.sh`                   | Builds the crate for the current host and stages it                                                          |
| `scripts/stage.sh`, `stage.mjs`      | Stage an already built library; `stage.mjs` is the single staging owner                                      |
| `scripts/install-linux-toolchain.sh` | Explicit root-only LLVM 21 installer for opted-in Debian/Ubuntu build environments                           |

The crate's `src/` follows the peer's threads. `ffi.rs` exports the C ABI and `abi.rs` mirrors the header's constants, which a test checks against the header. `peer/` holds the peer handle, its owner thread (`owner.rs`), which owns every libwebrtc object, and the libwebrtc callbacks (`callbacks.rs`, `media.rs`) that copy into the state the threads share (`shared.rs`). `protocol/` is the JSON of requests, responses and event headers, and `sync/` holds the bounded queues, the callback gate and the notifier. Unit tests sit beside the code they test; a module with a larger suite keeps it in a `tests.rs` of its own.

## Media path (ABI 3)

The bridge owns one libwebrtc peer on a Rust thread. Every peer in a process shares one libwebrtc factory, created on the first `prepare` and never destroyed, because reactor-webrtc requires one factory per process.

libwebrtc callbacks copy each decoded frame once into a bounded, typed Rust queue and set a readiness bit. They never invoke or wait for JavaScript. The queues hold 8 video frames (333 ms at 24 fps), 256 PCM blocks (2.56 s of 10 ms blocks) and 1,024 transport events. One notifier thread per peer passes the readiness bits to a Koffi callback; it is the only native thread that ever waits on JavaScript. The callback wakes one pump fiber per queue. Each pump reads with synchronous, nonblocking takes that copy a frame straight into memory its consumer then owns, and yields between items so observers run at their own pace. No media call uses the libuv thread pool, and none is in flight when a reader is interrupted.

A full media queue evicts its oldest item and counts it, so `media.snapshot` reports what was dropped, delivered and still queued. A full event queue is not pruned: it retires the connection with an `Overflow` failure.

The C header, `rust/include/reactor_effect_native.h`, is the ABI contract. Readiness is a callback passed to `reactor_effect_peer_create`; `reactor_effect_peer_take_event`, `_take_video` and `_take_audio` report `BUFFER_TOO_SMALL` with the required size and keep the item queued until a take succeeds.

`close()` fences new callback and host-call admission immediately. The host registers every `call`, `send` and `shutdown` before dispatching it to Koffi, and keeps that ownership until the foreign call actually returns, even when its Effect waiter is interrupted. `shutdown()` drains those active and queued foreign calls, joins the Rust owner, the callback guards and the notifier thread, then destroys the handle and unregisters the Koffi callback once. The join is an asynchronous Koffi call because the notifier may be waiting for the JavaScript thread to run its callback. The C ABI requires other hosts to perform the same foreign-call drain before destruction, and to join from a thread other than the one that runs the callback. Joining the Rust owner alone cannot observe calls queued in a host executor.

## Failure classes

Every native failure is one of a closed set of classes, which the host maps to `ReactorError.code`:

| Status | `ReactorError.code` | Raised when                                                                 |
| ------ | ------------------- | --------------------------------------------------------------------------- |
| `-1`   | `InvalidInput`      | the bridge rejects a request or argument                                    |
| `-2`   | `Native`            | libwebrtc or the bridge fails in a way it cannot classify                   |
| `-3`   | `Overflow`          | a queue, buffer or message bound is exceeded                                |
| `-4`   | `Protocol`          | the remote peer breaks the negotiated contract, such as an undeclared track |
| `-5`   | `SdpRejected`       | libwebrtc refuses to create or apply an offer or answer                     |
| `-6`   | `ChannelClosed`     | a send targets a data channel that is not open                              |
| `3`    | `Closed`            | the peer is fenced or shut down                                             |

Pinned reactor-webrtc reports every libwebrtc error as a string, so the bridge classifies a failure by the operation that produced it. The diagnostic text stays in `context.detail`, never in the message, because a libwebrtc error can contain SDP.

When the connection state reaches `failed`, the host reads its statistics once. A candidate pair that succeeded or was nominated means ICE worked and the DTLS or SCTP transport above it failed: `TransportFailed`. Otherwise the failure is `IceFailed`, with the local candidate types tried. The session reports a data channel that closes as `ChannelClosed`, naming the channel. A `disconnected` state still fails the connection as `Disconnected`, and decode failures are not reported: reactor-webrtc surfaces neither ICE connection state nor decoder errors.

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

The script checks Rust formatting, then runs the Rust tests, clippy with the crate's lint set, and rustdoc, all with warnings denied, and builds the test far peer. It then runs the JavaScript suite in `packages/native/test` against the staged artifact twice, on Node and on Bun; it never restages a second release library. The Rust tests check the ABI's constants against the C header and each JSON shape against what the host parses, and drive every entry point through the C ABI, including null and undersized arguments, a caught panic and a closed peer. Loopback tests negotiate with a second local peer on the shared factory through the bridge's owner thread, and exercise real libwebrtc ICE/DTLS/SCTP, ordered binary messages on both channels, video encode/decode on two lanes with their per-frame metadata, PCM audio, stream stats, and the close fence. Other tests cover queue accounting under seeded random operations, direction and bitrate controls, readiness coalescing, callback quiescence, and a shutdown that must wait for a notifier still inside the host callback. None of it contacts Reactor or generates paid media.

The media load tests receive from `rust/examples/far_peer/`, a libwebrtc sender on the same pinned reactor-webrtc that sends 1344x768 BGRA at 24 fps with per-frame metadata, plus 48 kHz PCM, and echoes both channels. It holds its congestion controller at 8 Mbps: on loopback the estimate follows only how promptly the host schedules both processes, and on a busy runner it backs off until the encoder drops most frames.

The tests assert what the bridge owns: how many frames reach it, how many it drops, and how many it holds, queued natively or taken but not yet seen by the subscriber. End-to-end latency also carries the far peer's encoder and pacer and the receiver's jitter buffer, and received audio arrives at the pace of libwebrtc's playout clock; both follow how the host schedules the two processes, so each test prints them, with both processes' CPU use and the codec and pacing counters, without asserting them. Through Koffi and the staged library, on each runtime:

- one session must receive at least 20 frames per second for 10 s, drop at most 1% of them and hold at most two at p95; audio must keep reaching it at 20 blocks per second or more, none dropped, and control-channel round trips stay under 100 ms at p95;
- two concurrent sessions must each meet the same bounds for 10 s;
- a 250 ms event-loop stall may drop at most one frame; across a 2 s stall the native queue must evict what overflows its 8 frames, within two, lose no audio, and drain;
- a session is renewed three times while its predecessor streams; each replacement must keep receiving at least 15 frames per second while its predecessor shuts down in under 2 s, dropping at most one frame, with no 500 ms gap and no audio lost.

The C fixture in `test/session-fixture.c` implements the same header without libwebrtc, including a notifier thread. It drives the canonical session, blocks foreign calls while interrupting their Effect waiters, and checks that the Koffi callback is unregistered only after shutdown joined the notifier. The lifetime fixture drains queued calls while one active call remains held, verifies that shutdown/destruction have not run, then releases the final call and verifies one destruction. Its tombstone reports unsafe ordering without intentionally dereferencing freed memory. These checks establish ownership and ordering; they do not claim an observed heap-corruption incident.

`scripts/stage.sh` stages an already-built shared library under the package runtime layout, `lib/<platform>-<arch>/`. For example:

```sh
./packages/native/scripts/stage.sh packages/native/rust/target/release/libreactor_effect_native.dylib darwin-arm64
```

Linux x64 has a reproducible container build that starts from the package source, installs Rust 1.90 plus the package-owned LLVM 21 recipe and auxiliary archive tools, runs the native Rust tests, clippy and rustdoc, and exports only the resulting shared library. It requires an explicit Docker context so the script never changes the caller's active context:

```sh
DOCKER_CONTEXT=default bun run native:linux-x64
```

That stages `lib/linux-x64/libreactor_effect_native.so`. The final runtime image only needs the packaged shared object and its normal glibc system dependencies; Cargo, clang and the libwebrtc archive are build-time inputs.

## License

Apache-2.0. See [NOTICE](./NOTICE) and [`notices/`](./notices/).
