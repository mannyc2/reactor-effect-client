# Native WebRTC transport

`reactor-effect-native` is the host transport used by the package's `/native`
entry point. It is a small Rust `cdylib` over the pinned `reactor-webrtc`
revision. Reactor session ownership, HTTP, command correlation, cancellation,
and model policy stay in the TypeScript SDK.

The bridge owns one libwebrtc peer on a Rust thread. libwebrtc callbacks copy
their data into bounded Rust queues; they never invoke JavaScript. `close()`
fences new callback admission immediately. `shutdown()` drops the native peer,
waits for in-flight callback guards to leave, joins the owner thread, and only
then permits the opaque handle to be freed.

The transport provides STUN/TURN ICE configuration, two reliable ordered
binary data channels (`control` and `data`), transceiver direction and bitrate
controls, WebRTC stats, decoded BGRA video with Reactor frame metadata, and
interleaved signed 16-bit PCM audio. Native decoded media is exposed as owned
sample data. Browser `MediaStreamTrack` leases and publication are not
available on this host.

The current public native peer accepts at most one incoming video track and one
incoming audio track. Pinned `reactor-webrtc` delivers `RemoteTrack` callbacks
without the transceiver MID/native identity needed to join multiple same-kind
callbacks to SDP mappings without relying on arrival order. `NativePeer.prepare`
therefore rejects an ambiguous declaration with `UnsupportedCapability` before
native negotiation. Multiple outgoing tracks remain distinct by their declared
transceivers.

Build and stage the current host artifact with:

```sh
./scripts/native-build.sh
```

The pinned `reactor-webrtc-sys` build downloads its matching libwebrtc prebuilt
and verifies the published SHA-256 before linking. macOS prebuilts target macOS
13.0 or later. The script uses the checked-in Cargo configuration and stages
the result under `dist/native/<platform>-<arch>/`.

Linux native builds require LLVM/Clang 21. The pinned libwebrtc prebuilt ships
the matching libc++ headers, and older distro Clang releases are not a supported
compiler for this native bridge. `scripts/native-build.sh` and
`scripts/native-test.sh` never install system packages: on Linux they use an
explicit `CC`/`CXX` when supplied, otherwise they require `clang-21` and
`clang++-21` on `PATH` and fail with a toolchain-specific error if unavailable.
macOS compiler selection is unchanged.

For opted-in Debian 12 (bookworm) or Ubuntu 24.04 (noble) build environments,
the package includes an explicit root-only installer:

```sh
sudo ./native/install-linux-toolchain.sh
```

The installer uses the official apt.llvm.org LLVM 21 repository, verifies the
repository signing key fingerprint
`6084F3CF814B57C1CF12EFD515CF4D18AF4F7421` before adding the repository, and
installs only the `clang-21` toolchain package after its bootstrap HTTPS/GPG
requirements. The pinned libwebrtc artifact supplies the libc++/libc++abi
headers and static archives used by the glue build, so system libc++ packages
are intentionally not installed. The installer does not invoke `sudo` itself
and is only called explicitly by Docker/CI recipes; ordinary local build/test
scripts never modify the host package set.

Local qualification is credential-free:

```sh
./scripts/native-test.sh
```

The loopback test negotiates two local peers and exercises real libwebrtc
ICE/DTLS/SCTP, both binary channels, video encode/decode, PCM audio,
per-frame metadata, stream stats, direction changes, bitrate controls, and
callback quiescence. It does not contact Reactor or generate paid media.

`scripts/native-package.sh` stages an already-built shared library under the
package runtime layout, `dist/native/<platform>-<arch>/`. For example:

```sh
./scripts/native-package.sh native/target/release/libreactor_effect_native.dylib darwin-arm64
```

Linux x64 has a reproducible container build that starts from the npm package
source, installs Rust 1.90 plus the package-owned LLVM 21 recipe and auxiliary
archive tools, runs the native Rust tests and clippy, and exports only the
resulting shared library. It requires an
explicit Docker context so the script never changes the caller's active context:

```sh
DOCKER_CONTEXT=default ./scripts/native-linux-x64.sh
```

That stages `dist/native/linux-x64/libreactor_effect_native.so`. The final
runtime image only needs the packaged shared object and its normal glibc system
dependencies; Cargo, clang and the libwebrtc archive are build-time inputs.
