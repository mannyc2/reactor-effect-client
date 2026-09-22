# reactor-effect-client

An independent Effect-native client and clip engine for Reactor sessions.

The package keeps one session authority in TypeScript and treats WebRTC as a transport/media capability beneath it. It provides portable coordinator/session APIs, explicit browser and native peer selections, a model-facing client used by the H3 adapter, and reusable clip/submission/sequence primitives.

This is not an official Reactor SDK. Protocol material and native WebRTC dependencies are attributed in [NOTICE](./NOTICE) and [`notices/`](./notices/).

## Install

The package name is `reactor-effect-client`. It has a peer dependency on Effect `4.0.0-rc.115` and an optional native FFI dependency on Koffi. This repository has not published a release; install the validated local tarball until a release exists.

```sh
npm install ./reactor-effect-client-0.1.0.tgz effect@4.0.0-rc.115
```

Importing the root, coordinator helpers, H3 adapter, or browser entry point does not select or load the native backend. Native transport is explicit through `reactor-effect-client/native`.

## Session ownership

`Client` is the portable scoped service. `Client.make` and `Client.layer` require Effect-provided HTTP and crypto services plus a `PeerFactory`. `create` allocates a remote session; `attach` identifies an existing session without taking ownership of its remote lifetime. Both return scope-owned local handles, and `session.connect` starts WebRTC. Releasing an owned session attempts and independently confirms remote termination; releasing an attached session closes only its local resources and publication claims.

```ts
import { Effect, Redacted } from "effect"
import * as Client from "reactor-effect-client/Client"

const useSession = Effect.gen(function*() {
  const client = yield* Client.Client
  const session = yield* client.create({
    model: "your-model",
    jwt: Redacted.make("session-token")
  })

  yield* session.connect
  return yield* session.current
})
```

The surrounding application supplies the `Client` layer and Effect platform services. This keeps credentials, HTTP policy, scopes, and host transport selection visible in composition instead of hiding them in global state.

For a Node application using the native transport, provide those dependencies around the entire scoped operation:

```ts
import { Effect, Layer } from "effect"
import * as NodeCrypto from "@effect/platform-node/NodeCrypto"
import * as Client from "reactor-effect-client/Client"
import * as FetchHttp from "reactor-effect-client/FetchHttp"
import * as Native from "reactor-effect-client/native"

const clientLayer = Client.layer().pipe(
  Layer.provide(Layer.mergeAll(FetchHttp.layer, NodeCrypto.layer, Native.layer()))
)

// useSession is the operation above. Acquiring a real session can be billable;
// the repository's ordinary verification uses controlled local peers instead.
const main = Effect.scoped(useSession).pipe(Effect.provide(clientLayer))
```

For model adapters that want decoded media and file upload on a server host, `Native.make(options)` creates the native peer factory and delegates to the common `SessionClient.make` boundary. It returns a scoped `Model.Client`; no connection is made until `client.connect()` runs.

## Runtime choices

### Browser

`reactor-effect-client/browser` uses built-in `RTCPeerConnection` and browser media tracks. It exposes the browser `PeerFactory` layer plus track-to-frame/audio helpers. Browser code does not import the native bridge.

### Native

`reactor-effect-client/native` uses the package's Rust `reactor-effect-native` cdylib over pinned `reactor-webrtc`. The bridge owns libwebrtc on a Rust thread and copies callbacks into bounded queues before JavaScript observes them.

Native transport provides:

- STUN/TURN ICE configuration and offerer signaling;
- reliable ordered binary `control` and `data` channels;
- transceiver direction and sender bitrate controls;
- WebRTC statistics;
- owned decoded BGRA video with Reactor frame metadata;
- owned interleaved signed 16-bit PCM audio;
- immediate close fencing plus joined callback quiescence during shutdown.

Native media is sample-oriented. It does not fabricate browser `MediaStreamTrack` handles. The current backend accepts at most one incoming video and one incoming audio track because pinned `reactor-webrtc` does not expose the remote track MID/identity needed to join several same-kind callbacks to SDP mappings without relying on arrival order. Ambiguous declarations fail before negotiation with `UnsupportedCapability`.

See [`native/README.md`](./native/README.md) for native build details.

## Coordinator helpers

`reactor-effect-client/Sessions` contains coordinator-only facts and operations such as pricing, bounded token minting, session inspection, and confirmed termination. These helpers use Effect HTTP services and have no WebRTC or native dependency.

## Clip engine and H3 adapter

`reactor-effect-client/engine` defines the reusable clip engine contract, session transport seam, reference handling, renewal support, and simulator. The engine distinguishes local validation, provider rejection, uncertain remote outcome, and terminal session failure instead of flattening all failures into one retryable error.

`reactor-effect-client/h3` composes the H3 Reference Turbo Realtime model profile with the engine and sequence primitives. It does not select a peer implementation and does not contain application scheduling or show policy.

`Submission` models an inert prepared operation. Preparation may be interrupted before commit; once execution commits, callers joining or abandoning the result do not replay the dispatch.

`Sequence` owns bounded sequence affinity and explicit member outcomes. Partial admission is represented member-by-member as accepted, rejected, or indeterminate, and a sequence remains bound to one owner until it is sealed/retired and explicitly released.

## Package exports

| Export | Purpose |
| --- | --- |
| `reactor-effect-client` | Portable `Client`, errors, session/HTTP/stat types, recording helpers |
| `/Client` | Scoped portable session factory service |
| `/PeerFactory` | Internal host transport capability service exposed for Effect composition |
| `/browser` | Browser WebRTC peer layer and browser media helpers |
| `/native` | Native peer layer and `Native.make` model-client convenience |
| `/Model` | Model-facing client, decoded frame, snapshot and error types |
| `/Sessions` | Coordinator pricing/token/inspection/termination helpers |
| `/Submission` | Submit-once scoped operation primitive |
| `/Sequence` | Bounded sequence affinity and partial-outcome tracking |
| `/Clip` | Clip request, canvas and reference schemas |
| `/ModelProfile` | Model profile and duration/frame alignment helpers |
| `/engine` | Clip engine/session/reference/renewal/simulator surface |
| `/engine/Engine` | Engine contract and lifecycle events |
| `/engine/Session` | Reactor transport adapter for the engine |
| `/engine/Renewing` | Renewal coordinator |
| `/engine/SimulatedTransport` | Deterministic local transport for tests/simulation |
| `/engine/References` | Reference upload/preparation helpers |
| `/h3` | H3 profile + engine + sequence adapter surface |
| `/http`, `/wire`, `/media` | Lower-level HTTP, protocol and media modules |
| `/FetchHttp` | Concrete Effect fetch HTTP implementation |
| `/testing`, `/testing/Faults`, `/testing/Png` | Explicit reusable test utilities |

All package exports are explicit. The pack smoke test verifies every export target and declaration exists in the tarball and that relative/external import closure is satisfiable from declared dependencies.

Effect `4.0.0-rc.115` itself references the global `TextDecoderOptions` type in its channel declarations. A strict Node project that deliberately omits DOM types may need to declare that standard interface. The pack check first proves this is the sole upstream diagnostic, then applies a test-only declaration; it does not enable `skipLibCheck` or hide SDK declaration errors.

## Current qualification evidence

The repository distinguishes tests that have actually run from build recipes and provider claims.

- macOS arm64 native qualification ran locally on September 22, 2026: 8 Rust native tests, clippy with warnings denied, a release build, and 8 JavaScript native boundary tests passed. The Rust loopback used real local libwebrtc ICE/DTLS/SCTP, ordered binary channels, real video encode/decode, PCM, metadata, stats, direction/bitrate controls, and callback quiescence. A separate compiled test ABI drove the JavaScript `SessionClient` media parser and cancellation bounds.
- The Linux x64 Docker build is reproducible and digest-pinned, but it was not executed locally because no existing Docker daemon/context was available. CI is intended to execute the Linux and macOS native matrix.
- Local iteration used Bun 1.4.0. The separately downloaded pinned Bun 1.4.2 binary stalled during startup in this Mac host harness, so that local environment did not establish Bun 1.4.2 runtime evidence. CI pins 1.4.2 explicitly.
- No paid generation, hosted Reactor media generation, TURN-relay interoperability, or browser-to-native wire interoperability was exercised by the local native tests. Those remain separate integration evidence.

## Build from source

The TypeScript workspace uses Bun and TypeScript. Native code requires Rust 1.90, clang, curl, tar/zstd, and the platform link dependencies described in [`native/README.md`](./native/README.md).

```sh
bun install --frozen-lockfile
bun run native:build
bun run typecheck
bun run build
bun run lint
bun run test
bun run test:pack
```

Native qualification on a supported host:

```sh
./scripts/native-test.sh
./scripts/native-build.sh
```

The isolated Linux x64 recipe requires an explicit Docker context and never changes the caller's active context:

```sh
DOCKER_CONTEXT=my-context ./scripts/native-linux-x64.sh
```

## Contributing and security

See [CONTRIBUTING.md](./CONTRIBUTING.md) for repository workflow, validation expectations and third-party notice rules. See [SECURITY.md](./SECURITY.md) for private vulnerability reporting guidance and the project's security boundaries.

## License

The package is licensed under Apache-2.0. Third-party and derived-source notices are retained in [NOTICE](./NOTICE) and [`notices/`](./notices/).
