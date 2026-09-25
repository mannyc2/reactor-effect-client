# reactor-effect-native

Native WebRTC host for [`reactor-effect-client`](https://www.npmjs.com/package/reactor-effect-client). `reactor-effect-native` is a small Rust `cdylib` over the pinned `reactor-webrtc` revision, loaded through the optional [Koffi](https://koffi.dev) FFI dependency. Reactor session ownership, HTTP, command correlation, cancellation, and model policy stay in `reactor-effect-client`; this package owns transport and decoded media only.

This is not an official Reactor SDK. Native WebRTC dependencies are attributed in [NOTICE](./NOTICE) and [`notices/`](./notices/).

## Install

```sh
npm install reactor-effect-client reactor-effect-native effect@4.0.0-rc.117 @effect/platform-node@4.0.0-rc.117
```

`reactor-effect-client`, Effect `4.0.0-rc.117` and `@effect/platform-node` `4.0.0-rc.117` are peer dependencies; caret ranges (`^4.0.0-rc.117`) allow later rc releases without a forced SDK bump. Koffi is an optional dependency; it and the shared library are loaded only when `Native.layer()` is built, so merely importing this module remains safe when the optional dependency is absent. `@effect/platform-node` supplies the Node services an application provides around its scoped operation, and the [isolated host](#isolated-host) runs its child processes on it; this package loads it only when `Native.Isolated.layer()` is built. A consumer using the rc prerelease should retain the root override `"@effect/platform-node-shared": "^4.0.0-rc.117"` (matching their installed `@effect/platform-node`), because the platform's own caret range on its internal shared package can otherwise resolve to a different prerelease with a different Effect peer.

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

`Native.layer(nativeOptions)` supplies the `PeerFactory` for `Reactor.layer()`. Building it loads Koffi and the library and verifies the staged artifact once, so a missing or invalid library fails the layer with a `Native` error, and an invalid option with `InvalidInput`, before any `Client` exists to allocate a remote session. Constructing a factory makes no allocation. `nativeOptions.libraryPath` can select a staged native artifact; its caller owns artifact provenance. `nativeOptions.shutdownTimeout`, a `Duration.Input` of 10 seconds by default, bounds how long closing a connection waits for the native owner join (see [Media path](#media-path-abi-3)). The separate `Native.uploadFile` helper requires the host file services when used.

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

## Isolated host

`Native.Isolated.layer(options)` supplies the same `PeerFactory` with each connection generation's native peer in a child process of its own, driven over Effect RPC. Use it where a native failure must end one connection rather than the application: a crash inside libwebrtc, or an owner join that never completes, then takes down only that child, and the session fails or closes as it would for a lost transport. The in-process `Native.layer` stays the default and is unchanged.

```ts
const clientLayer = Reactor.layer().pipe(
  Layer.provide(
    Layer.mergeAll(Reactor.FetchHttp.layer, NodeServices.layer, Native.Isolated.layer()),
  ),
);
```

It takes the same `libraryPath` and `shutdownTimeout` options, and `Native.media(session)` reads its media as it does the in-process host's.

- **Preflight.** Building the layer forks a probe child that loads and verifies the library and opens a native peer, then shuts it down, so a missing or invalid library fails the layer with a `Native` error before any `Client` exists.
- **One child per peer.** `PeerFactory.make()` forks the peer's child at once, so it starts while the session allocates. The child is never respawned and nothing is replayed into it: when it dies, calls already handed to it fail with outcome `unknown`, later calls are refused as `not-submitted`, and the connection fails with `Native` ("native WebRTC child process exited"). A reconnect makes a new peer, and with it a new child. A child whose parent dies exits too.
- **Credentials stay in the parent.** Allocation, the session token, command correlation and termination never leave the parent process. The child sees ICE configuration, SDP, channel bytes and media, and starts with an empty environment and none of the parent's runtime flags.
- **Close.** `shutdown` asks the child to close and join its native peer and exit, within `shutdownTimeout` (10 seconds by default). On expiry the child is killed and the close reports `Shutdown` ("native child shutdown exceeded its deadline; child process killed"), which `Session.close` records in `localErrors` before it terminates the remote session. Nothing is retained, so unlike the in-process host a later peer is unaffected. `close()` retires the peer in the parent at once: no later event or frame from its child reaches the session.
- **Media.** Each track of a generation has one RPC stream from its child, opened by its first reader, and every reader of the track shares it with the same per-reader bounds as in-process. Each chunk carries one frame and the child sends the next only once the parent has acknowledged it, so each stream's credit is one frame. While the parent's channel is busy the child holds up to 8 video frames and 256 audio blocks, the native queues' depths, and evicts the oldest, counting it in `droppedVideo` or `droppedAudio` instead of `deliveredVideo` or `deliveredAudio`. Every frame keeps the admission `sequence` the native queue gave it, so an evicted frame is a gap that `recorder` reports, as it reports a frame the native queue dropped. `readerOverflows` counts the parent's readers and the child's.

It has costs the in-process host does not:

- starting a child takes about half a second (0.43–0.56 s from `make` to an open native peer on a 4-core Linux x64 runner), most of it loading Effect's RPC modules and verifying the staged library; it overlaps allocation, and the layer's probe pays it once more at build;
- every frame is copied across the IPC channel and then once more, because Node's advanced serialization delivers a message's typed arrays as views into one shared message buffer, and each frame's data must be the whole of an exact allocation of its own (a 1344x768 BGRA frame is about 4 MB);
- the parent must be Node: under Bun, building the layer fails with `UnsupportedCapability`, not submitted.

## Example

[`examples/`](https://github.com/mannyc2/reactor-effect-client/tree/main/packages/native/examples) is a command line that generates one H3 clip and writes its decoded frames and audio to an MP4, filling what the host dropped from `recorder`'s gaps; `--isolated` runs it on `Native.Isolated.layer()`. The repository's [live channel](https://github.com/mannyc2/reactor-effect-client/tree/main/examples/livestream) broadcasts an orchestration's decoded media to many browsers.

## Package layout

| Path                                 | Contents                                                                                                     |
| ------------------------------------ | ------------------------------------------------------------------------------------------------------------ |
| `dist/`                              | The compiled TypeScript entry point and its declarations, and the isolated host's child entry                |
| `lib/<platform>-<arch>/`             | The staged shared library and its `native-identity.json` sidecar for each shipped host                       |
| `rust/`                              | The `reactor-effect-native` crate: Cargo manifest and lock, lint configuration, build script, header, source |
| `rust/examples/far_peer/`            | Test-only libwebrtc sender for the media load tests; not packaged or staged                                  |
| `rust-toolchain.toml`                | The Rust toolchain the native scripts use, the one CI pins                                                   |
| `scripts/build.sh`                   | Builds the crate for the current host and stages it                                                          |
| `scripts/stage.sh`, `stage.mjs`      | Stage an already built library; `stage.mjs` is the single staging owner                                      |
| `scripts/install-linux-toolchain.sh` | Explicit root-only LLVM 21 installer for opted-in Debian/Ubuntu build environments                           |

The crate's `src/` follows the peer's threads. `ffi.rs` exports the C ABI and `abi.rs` mirrors the header's constants, which a test checks against the header. `peer/` holds the peer handle, its owner thread (`owner.rs`), which owns every libwebrtc object, and the libwebrtc callbacks (`callbacks.rs`, `media.rs`) that copy into the state the threads share (`shared.rs`). `protocol/` is the JSON of requests, responses and event headers, and `sync/` holds the bounded queues, the callback gate and the notifier. Unit tests sit beside the code they test; a module with a larger suite keeps it in a `tests.rs` of its own.

## Media path (ABI 4)

The bridge owns one libwebrtc peer on a Rust thread. Every peer in a process shares one libwebrtc factory, created on the first `prepare` and never destroyed, because reactor-webrtc requires one factory per process.

libwebrtc callbacks copy each decoded frame once into a bounded, typed Rust queue and set a readiness bit. They never invoke or wait for JavaScript. The queues hold 8 video frames (333 ms at 24 fps), 256 PCM blocks (2.56 s of 10 ms blocks) and 1,024 transport events. One notifier thread per peer passes the readiness bits to a Koffi callback; it is the only native thread that ever waits on JavaScript. The callback wakes one pump fiber per queue. Each pump reads with synchronous, nonblocking takes that copy a frame straight into memory its consumer then owns, and yields between items so observers run at their own pace. No media call uses the libuv thread pool, and none is in flight when a reader is interrupted.

Each decoded frame and PCM block is numbered on its track, from 0, as it enters its queue and before the queue can evict anything, and the number reaches JavaScript as the frame's `sequence`. A full media queue evicts its oldest item and counts it, so `media.snapshot` reports what was dropped, delivered and still queued, and each eviction is a gap in the sequences a reader receives, at its position: `recorder(stream)` from `reactor-effect-client` turns a track's frames into the frames plus a `Lost { after, count }` for each gap. The stall load test checks that those counts add up to the bridge's own drop count. A full event queue is not pruned: it retires the connection with an `Overflow` failure.

The C header, `rust/include/reactor_effect_native.h`, is the ABI contract. Readiness is a callback passed to `reactor_effect_peer_create`; `reactor_effect_peer_take_event`, `_take_video` and `_take_audio` report `BUFFER_TOO_SMALL` with the required size and keep the item queued until a take succeeds.

`close()` fences new callback and host-call admission immediately. The host registers every `call`, `send` and `shutdown` before dispatching it to Koffi, and keeps that ownership until the foreign call actually returns, even when its Effect waiter is interrupted. Running the peer's `shutdown` effect drains those active and queued foreign calls, joins the Rust owner, the callback guards and the notifier thread, then destroys the handle and unregisters the Koffi callback once. The join is an asynchronous Koffi call because the notifier may be waiting for the JavaScript thread to run its callback. The C ABI requires other hosts to perform the same foreign-call drain before destruction, and to join from a thread other than the one that runs the callback. Joining the Rust owner alone cannot observe calls queued in a host executor.

The host waits for that drain and join for at most `shutdownTimeout`; a healthy join took 13–62 ms in the media load tests on Node and Bun. On expiry the host stops waiting and the shutdown fails with `Shutdown` ("native owner join exceeded its deadline; handle retained"), which `Session.close` records in `localErrors` before it terminates the remote session. The join itself goes on: it keeps the handle and the registered Koffi callback, and destroys and unregisters them only if it completes. Until then the host holds the bridge, and because every peer of the library shares its one libwebrtc factory, preflight and peer creation fail with a `Native` error before any allocation rather than start another owner on a factory that may be wedged.

## Failure classes

Every native failure is one of a closed set of classes, which the host maps to the `ReactorError` reason's `_tag`:

| Status | `reason._tag`   | Raised when                                                                 |
| ------ | --------------- | --------------------------------------------------------------------------- |
| `-1`   | `InvalidInput`  | the bridge rejects a request or argument                                    |
| `-2`   | `Native`        | libwebrtc or the bridge fails in a way it cannot classify                   |
| `-3`   | `Overflow`      | a queue, buffer or message bound is exceeded                                |
| `-4`   | `Protocol`      | the remote peer breaks the negotiated contract, such as an undeclared track |
| `-5`   | `SdpRejected`   | libwebrtc refuses to create or apply an offer or answer                     |
| `-6`   | `ChannelClosed` | a send targets a data channel that is not open                              |
| `3`    | `Closed`        | the peer is fenced or shut down                                             |

Pinned reactor-webrtc reports every libwebrtc error as a string, so the bridge classifies a failure by the operation that produced it. That text can contain SDP, so it is never in the message and never in the error's cause chain, which exporters such as `OtlpTracer` render. It stays `Redacted` for explicit inspection: in the `Native` reason's `backendMessage` beside its `status`, or, for a classified failure, in `context.detail` as `{ status, backendMessage }`.

When the connection state reaches `failed`, the host reads its statistics once, in the pump that delivers events, so later events wait behind it and the connection scope owns the read. A read that fails, or outlasts its 2 s deadline on the fiber's `Clock`, reports `Disconnected`. A candidate pair that succeeded or was nominated means ICE worked and the DTLS or SCTP transport above it failed: `TransportFailed`. Otherwise the failure is `IceFailed`. Both reasons carry the number of candidate pairs the statistics listed, and `IceFailed` the local candidate types tried. The session reports a data channel that closes as `ChannelClosed`, naming the channel. A `disconnected` state still fails the connection as `Disconnected`, and decode failures are not reported: reactor-webrtc surfaces neither ICE connection state nor decoder errors.

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
- across a 250 ms event-loop stall the native queue may evict only what overflowed its 8 frames while JavaScript was blocked, within one, so at 24 fps at most one frame; across a 2 s stall it must evict what overflows, within two, lose no audio, and drain;
- a session is renewed three times while its predecessor streams; each replacement must keep receiving at least 15 frames per second over the 4 s from the start of its predecessor's shutdown, which must take under 2 s, dropping at most one frame, with no 500 ms gap and no audio lost;
- every session above closes cleanly: its native owner joins without a `Shutdown` failure, in under a fifth of the default `shutdownTimeout`, so a deadline that would fire on a slow but healthy join fails the suite;
- a canonical `Session` over the far peer, through a coordinator stand-in that relays its signaling, receives video and closes with `localClosed`, no `localErrors` and its remote termination confirmed, under the same bound;
- an answer whose candidates point at an unreachable documentation address fails ICE, and the events pump's classification reports `IceFailed` with its candidate-pair detail. libwebrtc reports the failure once it has pruned the last timed-out pair, so that detail lists no pairs.

The C fixture in `test/session-fixture.c` implements the same header without libwebrtc, including a notifier thread. It drives the canonical session, blocks foreign calls while interrupting their Effect waiters, and checks that the Koffi callback is unregistered only after shutdown joined the notifier. Holding a shutdown join open, it checks that `Session.close` returns at the deadline with `Shutdown` in `localErrors` and the remote session terminated, that the bridge is neither destroyed nor unregistered until the join completes, and that the process admits no new peer until then. Holding a statistics call open, it checks that a failed connection's classification ends on a `TestClock` with `Disconnected`. The lifetime fixture drains queued calls while one active call remains held, verifies that shutdown/destruction have not run, then releases the final call and verifies one destruction. Its tombstone reports unsafe ordering without intentionally dereferencing freed memory. These checks establish ownership and ordering; they do not claim an observed heap-corruption incident.

`test/frame-allocation.test.ts` scripts the video queue of a minimal ABI library. Each take must hand over the one exact-size buffer native code copied the frame into, with no JavaScript copy of its pixels except the trim of a frame smaller than its predecessor, and a public media generation's Stream consumer must receive that same buffer. The frame-ownership check it shares with the client and browser suites, `reactor-effect-test-kit/frames`, also runs on the frames of the canonical far-peer session.

`test/isolated.test.ts` runs the isolated host with real child processes, on Node; on Bun it checks only that the layer refuses to build. Over the C fixture it checks that successive generations each reach their own child through one parent, that a child killed mid-call fails that call as `unknown`, refuses later calls before dispatch and is never respawned, that a child exits when its parent dies with a native call in flight, that a shutdown held past its deadline kills the child and still terminates the remote session, that events and frames a retired, shut down or killed child sends late never reach it or a later peer, and that a cancelled wait never takes a later call's reply. Over the far peer it checks that a reader that stops reading fails alone with `Overflow` while each stream holds one unacknowledged one-frame chunk, and that a canonical session's frames arrive as exact allocations after the IPC hop and the session closes cleanly. The child entry is the built one, so the suite needs `bun run build` first.

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
