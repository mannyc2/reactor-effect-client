# reactor-effect-native

Native WebRTC host for [`reactor-effect-client`](https://www.npmjs.com/package/reactor-effect-client). `reactor-effect-native` drives a Node-API addon, built with [napi-rs](https://napi.rs) from a small Rust crate over the pinned `reactor-webrtc` revision. Each supported platform's addon ships in its own package, `reactor-effect-native-linux-x64-gnu` or `reactor-effect-native-darwin-arm64`, which this package lists as exact-version optional dependencies, so a package manager installs only the one the host can run. Reactor session ownership, HTTP, command correlation, cancellation, and model policy stay in `reactor-effect-client`; this package owns transport and decoded media only.

This is not an official Reactor SDK. Native WebRTC dependencies are attributed in [NOTICE](./NOTICE) and [`notices/`](./notices/).

## Install

```sh
npm install --save-exact reactor-effect-client reactor-effect-native effect@4.0.0-rc.117 @effect/platform-node@4.0.0-rc.117
```

`reactor-effect-client`, Effect `4.0.0-rc.117`, `@effect/platform-node` `4.0.0-rc.117` and `@effect/platform-node-shared` `4.0.0-rc.117` are exact peer dependencies: a later Effect rc needs a new SDK release. The addon is loaded only when `NativePeer.layer()` is built, so merely importing this module is safe on a host with no platform package, such as one installed with `--omit=optional`. `@effect/platform-node` supplies the Node services an application provides around its scoped operation, and the [isolated host](#isolated-host) runs its child processes on it; this package loads it only when `NativePeer.layerIsolated()` is built. This package never imports `@effect/platform-node-shared`, but the platform depends on it with a caret range, under which npm would install a later rc that fails to load on this Effect; as a peer, it is installed once, at `4.0.0-rc.117`, and the platform reuses it, so no override is needed. Install `@effect/platform-node` with `--save-exact`: under the caret range npm saves by default, a later install without a lockfile takes a later rc of it, and npm fails with `ERESOLVE`.

## Usage

```ts
import { Layer } from "effect";
import * as FetchHttpClient from "effect/unstable/http/FetchHttpClient";
import * as CoordinatorClient from "reactor-effect-client/CoordinatorClient";
import * as Reactor from "reactor-effect-client/Reactor";
import { NativePeer } from "reactor-effect-native";

const reactorLayer = Reactor.layer().pipe(
  Layer.provide(Layer.mergeAll(CoordinatorClient.layerConfig, NativePeer.layer())),
  Layer.provide(FetchHttpClient.layer),
);
```

`NativePeer.layer(options)` supplies the `PeerFactory` for `Reactor.layer()`. Building it loads this platform's addon, so before any session is allocated a host with no platform package fails the layer with `UnsupportedHost`, a platform package of another version or an addon that cannot load with `Native`, and an invalid option with `InvalidInput`. Constructing a factory makes no allocation. `options.addon` can name another addon file by absolute path; its caller owns its provenance. `options.shutdownTimeout`, a `Duration.Input` of 10 seconds by default, bounds how long closing a connection waits for the native owner join (see [Threads and media](#threads-and-media)).

`session.decoded` returns the connected session's decoded media. `media.video(name)` emits owned BGRA frames and `media.audio(name)` emits owned interleaved signed 16-bit PCM. Frame IDs and microsecond timestamps use `bigint`. Retaining a frame retains JavaScript-owned bytes, and native media has no browser track handles. Media values stay bound to their negotiated generation: a reconnect creates a new generation and existing readers end or fail with their source.

Native transport provides:

- STUN/TURN ICE configuration and offerer signaling;
- reliable ordered binary `control` and `data` channels;
- transceiver direction and sender bitrate controls;
- WebRTC statistics;
- owned decoded BGRA video with Reactor frame metadata;
- owned interleaved signed 16-bit PCM audio;
- typed failure classes;
- immediate close fencing plus joined callback quiescence during shutdown.

The current public native peer accepts at most one incoming video track and one incoming audio track. Pinned `reactor-webrtc` delivers `RemoteTrack` callbacks without the transceiver MID/native identity needed to join multiple same-kind callbacks to SDP mappings without relying on arrival order. `prepare` therefore rejects an ambiguous declaration with `UnsupportedCapability` before native negotiation. Multiple outgoing tracks remain distinct by their declared transceivers.

## Isolated host

`NativePeer.layerIsolated(options)` supplies the same `PeerFactory` with each connection generation's native peer in a child process of its own, driven over Effect RPC. Use it where a native failure must end one connection rather than the application: a crash inside libwebrtc, or an owner join that never completes, then takes down only that child, and the session fails or closes as it would for a lost transport. The in-process `NativePeer.layer` stays the default.

```ts
import { Layer } from "effect";
import * as FetchHttpClient from "effect/unstable/http/FetchHttpClient";
import * as CoordinatorClient from "reactor-effect-client/CoordinatorClient";
import * as Reactor from "reactor-effect-client/Reactor";
import { NativePeer } from "reactor-effect-native";

const reactorLayer = Reactor.layer().pipe(
  Layer.provide(Layer.mergeAll(CoordinatorClient.layerConfig, NativePeer.layerIsolated())),
  Layer.provide(FetchHttpClient.layer),
);
```

It takes the same `addon` and `shutdownTimeout` options, and `session.decoded` reads its media as it does over the in-process host. The child relays the addon and nothing else: the parent runs the same peer over it that runs in process, so frame naming, reader bounds and failure classification are shared.

- **Preflight.** Building the layer forks a probe child that loads the addon and opens a native peer, then shuts it down, so a host that cannot run it fails the layer before any session is allocated.
- **One child per peer.** `PeerFactory.make()` forks the peer's child at once, so it starts while the session allocates. The child is never respawned and nothing is replayed into it: when it dies, calls already handed to it fail with outcome `unknown`, later calls are refused as `not-submitted`, and the connection fails with `Native` ("native WebRTC child process exited"). A reconnect makes a new peer, and with it a new child. A child whose parent dies exits too.
- **Credentials stay in the parent.** Allocation, the session token, command correlation and termination never leave the parent process. The child sees ICE configuration, SDP, channel bytes and media, and starts with an empty environment and none of the parent's runtime flags.
- **Close.** `shutdown` asks the child to close and join its native peer and exit, within `shutdownTimeout` (10 seconds by default). On expiry the child is killed and the close reports `Shutdown` ("native child shutdown exceeded its deadline; child process killed"), which `Session.close` records in `localErrors` before it terminates the remote session. Nothing is retained, so unlike the in-process host a later peer is unaffected. `close()` retires the peer in the parent at once: no later event or frame from its child reaches the session.
- **Media.** Each of the addon's queues, events, video and audio, is one RPC stream from the child, opened as the peer is prepared. The child takes an item from the addon's queue only as the parent pulls one, and a media stream's credit is one frame, so while the parent is busy frames wait in the addon's own bounded queue, which evicts the oldest and counts it in `droppedVideo` or `droppedAudio`, exactly as in process. Every frame keeps the admission `sequence` the addon gave it, so an evicted frame is a gap that `recorder` reports. The parent fans each track out to its readers with the same per-reader bounds as in process.

It has costs the in-process host does not:

- starting a child takes about half a second, most of it loading Effect's RPC modules and the addon; it overlaps allocation, and the layer's probe pays it once more at build;
- every frame is copied across the IPC channel and then once more, because Node's advanced serialization delivers a message's typed arrays as views into one shared message buffer, and each frame's data must be the whole of an exact allocation of its own (a 1344x768 BGRA frame is about 4 MB);
- the parent must be Node: under Bun, building the layer fails with `UnsupportedCapability`, not submitted.

## Example

[`examples/`](https://github.com/mannyc2/reactor-effect-client/tree/main/packages/native/examples) is a command line that generates one H3 clip and writes its decoded frames and audio to an MP4, filling what the host dropped from `recorder`'s gaps; `--isolated` runs it on `NativePeer.layerIsolated()`. The repository's [live channel](https://github.com/mannyc2/reactor-effect-client/tree/main/examples/livestream) broadcasts a playout's decoded media to many browsers.

## Package layout

| Path                                 | Contents                                                                                                   |
| ------------------------------------ | ---------------------------------------------------------------------------------------------------------- |
| `dist/`                              | The compiled TypeScript entry point and its declarations, and the isolated host's child entry              |
| `npm/<platform>/`                    | Each platform package: its manifest and README; staging adds the addon, `native-identity.json` and notices |
| `rust/`                              | The `reactor-effect-native` crate: Cargo manifest and lock, lint configuration, build script, source       |
| `rust/examples/far_peer/`            | Test-only libwebrtc sender for the media load tests; not packaged or staged                                |
| `rust-toolchain.toml`                | The Rust toolchain the native scripts use, the one CI pins                                                 |
| `scripts/build.sh`                   | Builds the addon for the current host with `napi build` and stages it                                      |
| `scripts/linux-x64.sh`, `Dockerfile` | Builds, tests and stages the Linux x64 addon in a pinned container                                         |
| `scripts/stage.mjs`                  | Checks a built addon's identity and stages it into its platform package                                    |
| `scripts/install-linux-toolchain.sh` | Explicit root-only LLVM 21 installer for opted-in Debian/Ubuntu build environments                         |

Only `dist/`, the notices and the license files are in the `reactor-effect-native` package; no Rust source and no addon ship in it.

In `src/`, `NativePeer.ts` is the public layer. `internal/binding.ts` is the addon's surface as `napi build` declares it from `rust/src/binding.rs`, regenerated by every build; CI fails when the committed copy differs. `internal/addon.ts` loads the platform package; `internal/peer.ts` is the peer the client's `Peer` port sees, over a small handle on one addon peer that `internal/local.ts` implements in process and `internal/isolated/` in a child process.

The crate's `src/` follows the peer's threads. `binding.rs` is the whole Node-API surface: the `NativePeer` class, its typed replies and events, and conversions from the crate's own types. `peer/` holds the peer handle, its owner thread (`owner.rs`), which owns every libwebrtc object, and the libwebrtc callbacks (`callbacks.rs`, `media.rs`) that copy into the state the threads share (`shared.rs`). `protocol/` holds request validation, events and statistics, and `sync/` the bounded queues, the callback gate and the readiness bits. The crate has no `unsafe` code; the Node-API calls are napi-rs's generated glue. Unit tests sit beside the code they test; a module with a larger suite keeps it in a `tests.rs` of its own.

## Threads and media

The addon owns one libwebrtc peer on a Rust owner thread. Every peer in a process shares one libwebrtc factory, created on the first `prepare` and never destroyed, because reactor-webrtc requires one factory per process.

libwebrtc callbacks copy each decoded frame once into a bounded, typed Rust queue and set a readiness bit. They never invoke or wait for JavaScript. The queues hold 8 video frames (333 ms at 24 fps), 256 PCM blocks (2.56 s of 10 ms blocks) and 1,024 transport events. The first bit set since the host last looked queues one non-blocking call of a Node-API threadsafe function; later bits coalesce into it, and the call, on the JavaScript thread, takes the bits and only opens a latch per queue. Each queue is read by one stream that takes items synchronously, each frame in an `ArrayBuffer` of its own at its exact size, and yields between items so observers run at their own pace. No wake is lost: a bit set after the host took the bits wakes it again. The host fans each received track out to its readers, and each reader holds at most 24 video frames (a second at 24 fps, within 128 MiB) or 128 PCM blocks (within 4 MiB); a reader that falls further behind fails alone with `Overflow`, and `media.pressure` counts it in `readerOverflows`.

Calls that run on the owner thread, such as `prepare`, `answer`, `send` and `stats`, return a promise of a typed reply, resolved when the owner answers. At most 128 are admitted at once; `media.pressure` reports those not yet taken up as `pendingRequests`.

Each decoded frame and PCM block is numbered on its track, from 0, as it enters its queue and before the queue can evict anything, and the number reaches JavaScript as the frame's `sequence`. A full media queue evicts its oldest item and counts it, so `media.pressure` reports what was dropped, delivered and still queued, and each eviction is a gap in the sequences a reader receives, at its position: `recorder(stream)` from `reactor-effect-client` turns a track's frames into the frames plus a `Lost { after, count }` for each gap. The stall load test checks that those counts add up to the addon's own drop count. A full event queue is not pruned: it retires the connection with an `Overflow` failure.

`close()` fences callback admission and new calls at once and discards what is queued. `shutdown()` closes, then joins the owner thread and every admitted callback on a thread of its own, and settles its promise when the join completes, so the JavaScript thread never waits for it. Calls already admitted are answered or refused before the owner exits.

The host waits for that join for at most `shutdownTimeout`; a healthy join takes tens of milliseconds in the media load tests on Node and Bun. On expiry the host stops waiting and the shutdown fails with `Shutdown` ("native owner join exceeded its deadline; handle retained"), which `Session.close` records in `localErrors` before it terminates the remote session. The join itself goes on and keeps its peer, held by the layer that made it. Until it completes, because every peer shares the one libwebrtc factory, that layer's preflight and peer creation fail with a `Native` error before any allocation rather than start another owner on a factory that may be wedged. Releasing the layer stops waiting for the join.

## Failure classes

Every native failure is one of a closed set of classes, which the host maps to the `ReactorError` reason's `_tag`:

| Class           | Raised when                                                                 | Outcome of a call |
| --------------- | --------------------------------------------------------------------------- | ----------------- |
| `InvalidInput`  | the addon rejects a request or argument                                     | `not-submitted`   |
| `Overflow`      | a queue, buffer, message or call admission bound is exceeded                | `not-submitted`   |
| `Closed`        | the peer is fenced or shut down                                             | `not-submitted`   |
| `ChannelClosed` | a send targets a data channel that is not open                              | `not-submitted`   |
| `Native`        | libwebrtc or the addon fails in a way it cannot classify                    | `unknown`         |
| `Protocol`      | the remote peer breaks the negotiated contract, such as an undeclared track | `unknown`         |
| `SdpRejected`   | libwebrtc refuses to create or apply an offer or answer                     | `unknown`         |

The first four are refusals the addon makes before a call runs, so the call was not submitted; after any other class its side effect is unknown. The host decides by the class, never by the text. Pinned reactor-webrtc reports every libwebrtc error as a string, so the addon classifies a failure by the operation that produced it. That text can contain SDP, so it is never in the message and never in the error's cause chain, which exporters such as `OtlpTracer` render. It stays `Redacted` for explicit inspection: in the `Native` reason's `backendMessage`, or, for another class, in `context.detail` as `{ backendMessage }`.

The host reports the connection state as libwebrtc gives it, and the session classifies a `failed` connection from the peer's statistics, as it does over the browser host: a candidate pair that succeeded or was nominated means ICE worked and the DTLS or SCTP transport above it failed, `TransportFailed`; otherwise the failure is `IceFailed`. Both reasons carry the number of candidate pairs the statistics listed, and `IceFailed` the local candidate types tried. A statistics read that fails, or outlasts its 2 s deadline on the fiber's `Clock`, leaves the failure `Disconnected`. The session reports a data channel that closes as `ChannelClosed`, naming the channel. A `disconnected` state fails the connection as `Disconnected`, and decode failures are not reported: reactor-webrtc surfaces neither ICE connection state nor decoder errors.

## Build and stage

Native code requires Rust 1.90, Clang 21 on Linux (the platform compiler on macOS), curl, tar with zstd support (or `zstd`), and a SHA-256 tool. From the workspace root:

```sh
bun run native:build # sh packages/native/scripts/build.sh
```

The pinned `reactor-webrtc-sys` build downloads its matching libwebrtc prebuilt and verifies the published SHA-256 before linking. macOS prebuilts target macOS 13.0 or later. The script runs `napi build --platform --release` (`@napi-rs/cli` is pinned exactly) and stages the addon into its platform package under `npm/`. `scripts/stage.mjs` loads the addon and checks the build identity it embeds, its target, release profile and the SHA-256 of every native build input, against the current sources, and checks the linked libwebrtc prebuilt against the notices and SBOM; then it writes the addon, `native-identity.json` with its SHA-256 and build identity, and the license and notices into the platform package, and regenerates `src/internal/binding.ts`. A stale addon is rejected before it can be staged. `node scripts/stage.mjs --source-hash` prints the identity of the current sources; CI keys its staged-addon cache on it (with the build recipe, toolchain and runner image) and restores an accessible exact-match addon on pull requests and on `main`. Restaging validates it again; the native JavaScript and browser integration suites still run. On `main`, a cache miss can reuse the addon and far peer from a successful pull-request CI run in this repository with the same workflow, source identity, build recipe, toolchain and runner image. The existing artifacts carry the full SHA-256 of that cache key in their names. Expired or incomplete artifacts fall back to a fresh build and Rust checks; every restored addon is restaged and qualified again on Node, Bun and Chrome.

Linux x64 has a reproducible container build from the package source, with Rust 1.90, the package-owned LLVM 21 recipe and `@napi-rs/cli`. It runs the Rust formatting check, tests, clippy and rustdoc, and exports only the addon and its declarations, which the script then stages. It requires an explicit Docker context, so the script never changes the caller's active context:

```sh
DOCKER_CONTEXT=default bun run native:linux-x64
```

The addon needs only glibc at run time; Cargo, clang and the libwebrtc archive are build-time inputs. The darwin-arm64 addon is built and qualified on CI's macOS runner.

Linux native builds require LLVM/Clang 21. The pinned libwebrtc prebuilt ships the matching libc++ headers, and older distro Clang releases are not a supported compiler for this addon. `scripts/build.sh` and `scripts/test.sh` never install system packages: on Linux they use an explicit `CC`/`CXX` when supplied, otherwise they require `clang-21` and `clang++-21` on `PATH` and fail with a toolchain-specific error if unavailable.

For opted-in Debian 12 (bookworm) or Ubuntu 24.04 (noble) build environments, the package includes an explicit root-only installer:

```sh
sudo ./packages/native/scripts/install-linux-toolchain.sh
```

The installer uses the official apt.llvm.org LLVM 21 repository, verifies the repository signing key fingerprint `6084F3CF814B57C1CF12EFD515CF4D18AF4F7421` before adding the repository, and installs only the `clang-21` toolchain package after its bootstrap HTTPS/GPG requirements. The pinned libwebrtc artifact supplies the libc++/libc++abi headers and static archives used by the glue build, so system libc++ packages are intentionally not installed. The installer does not invoke `sudo` itself and is only called explicitly by Docker/CI recipes.

## Qualification

Local qualification is credential-free:

```sh
bun run native:test # sh packages/native/scripts/test.sh
```

The script checks Rust formatting, then runs the Rust tests, clippy with the crate's lint set, and rustdoc, all with warnings denied, and builds the test far peer. It then runs the JavaScript suite in `packages/native/test` against the staged addon twice, on Node and then on Bun. `sh scripts/test.sh rust` stops before the JavaScript suite: CI builds and checks each platform's addon once, then runs the suite on Node and on Bun in jobs of their own, each on its own runner, against that staged addon and far peer. `sh scripts/test.sh node` runs the same Rust checks and far-peer build, then the JavaScript suite on Node only. From the workspace root, `bun run verify --profile native-local` also builds, stages and runs browser integration for that local iteration tier. The full `native` profile keeps Node and Bun qualification and remains the required gate for native changes. Native addons and far-peer CI artifacts are retained for three days. The Rust tests drive the peer handle the binding wraps: loopback tests negotiate with a second local peer on the shared factory, and exercise real libwebrtc ICE/DTLS/SCTP, ordered binary messages on both channels, video encode/decode with per-frame metadata, PCM audio, statistics and the close fence. Other tests cover queue accounting under seeded random operations, direction and bitrate controls, readiness coalescing, call admission, callback quiescence and idempotent shutdown. None of it contacts Reactor or generates paid media.

The media load tests receive from `rust/examples/far_peer/`, a libwebrtc sender on the same pinned reactor-webrtc that sends 1344x768 BGRA at 24 fps with per-frame metadata, plus 48 kHz PCM, and echoes both channels. It holds its congestion controller at 8 Mbps: on loopback the estimate follows only how promptly the host schedules both processes, and on a busy runner it backs off until the encoder drops most frames.

The tests assert what the addon owns: how many frames reach it, how many it drops, and how many it holds, queued natively or taken but not yet seen by the subscriber. End-to-end latency also carries the far peer's encoder and pacer and the receiver's jitter buffer, and received audio arrives at the pace of libwebrtc's playout clock; both follow how the host schedules the two processes, so each test prints them without asserting them. Through the installed addon, on each runtime:

- one session must receive at least 20 frames per second for 10 s, drop at most 1% of them and hold at most two at p95; audio must keep reaching it at 20 blocks per second or more, none dropped, and control-channel round trips stay under 100 ms at p95;
- two concurrent sessions must each meet the same bounds for 10 s;
- across a 250 ms event-loop stall the native queue may evict only what overflowed its 8 frames while JavaScript was blocked, within one; across a 2 s stall it must evict what overflows, within two, lose no audio, and drain;
- a session is renewed three times while its predecessor streams; each replacement must keep receiving at least 15 frames per second over the 4 s from the start of its predecessor's shutdown, which must take under 2 s, dropping at most one frame, with no 500 ms gap and no audio lost;
- every session above closes cleanly, in under a fifth of the default `shutdownTimeout`;
- a canonical `Session` over the far peer, through a coordinator stand-in that relays its signaling, receives video whose frames are each the whole of their own buffer, and closes with `localClosed`, no `localErrors` and its remote termination confirmed;
- a canonical session whose answer points its candidates at an unreachable documentation address fails ICE, and its acquisition fails with `IceFailed`.

`test/fixtures/addon.mts` is a scripted fake of the addon without libwebrtc, which tests load in process or, by path, in a child. Over it, `test/peer.test.ts` checks that a source failure reaches existing and future readers, that shutdown ends readers, and that a failed connection is reported as its state with the peer left open to read. `test/session.test.ts` drives the canonical session over it: owned media, bounded submitted requests across caller cancellation, a failed connection classified from its statistics, or, when they never answer, reported `Disconnected` on a `TestClock`, and, holding a shutdown join open, that `Session.close` returns at the deadline with `Shutdown` in `localErrors` and the remote session terminated, and that no new peer is admitted until the join completes.

`test/isolated.test.ts` runs the isolated host with real child processes, on Node; on Bun it checks only that the layer refuses to build. Over the fake addon it checks that successive generations each reach their own child, that a child killed mid-call fails that call as `unknown`, refuses later calls before dispatch and is never respawned, that a child exits when its parent dies with a native call in flight, that a shutdown held past its deadline kills the child and still terminates the remote session, that events and frames a closed or killed child sends late never reach it or a later peer, and that a cancelled wait never takes a later call's reply. Over the far peer it checks that a reader that stops reading fails alone with `Overflow`, and that a canonical session's frames arrive as exact allocations after the IPC hop and the session closes cleanly. The child entry is the built one, so the suite needs `bun run build` first.

## License

Apache-2.0. See [NOTICE](./NOTICE) and [`notices/`](./notices/).
