# reactor-effect-client

Checked-in [compiled examples](./examples/README.md) cover canonical sessions,
Node and browser host composition, H3 acceptance evidence, explicit renewal and
offline simulation. `bun run verify --profile portable` checks architecture and
separate host source closures, compiles those examples, and executes only the
unpaid simulation. Final package qualification recompiles the archived examples
inside isolated installed consumers rather than resolving this workspace.

An independent Effect SDK for scoped Reactor sessions, H3 provider state, host media, and explicit orchestration.

One canonical `Session` owns each allocation or attachment, its commands, connection generations, and cleanup evidence. Browser and native entry points select host capabilities. H3 consumes that same session and exposes provider state. Applications opt into the separate orchestration and simulation surfaces when they need scheduling, sequence affinity, or renewal.

This is not an official Reactor SDK. Protocol material and native WebRTC dependencies are attributed in [NOTICE](./NOTICE) and [`notices/`](./notices/).

## Install

The package name is `reactor-effect-client`. It has a peer dependency on Effect `4.0.0-rc.115` and an optional native FFI dependency on Koffi. This repository has not published a release; install the validated local tarball until a release exists.

```sh
npm install ./reactor-effect-client-0.2.0.tgz effect@4.0.0-rc.115
```

The root, H3, orchestration, simulation, testing, wire, and browser imports are portable. Native transport is selected explicitly through `reactor-effect-client/native`; its optional Koffi dependency and shared library are loaded when native preflight runs.

## Session ownership

The root exports the `Client` service and its `make` and `layer` constructors. They require Effect HTTP and crypto services plus a `PeerFactory`. `create` allocates a remote session; `attach` identifies an existing session without taking ownership of its remote lifetime. Both return the same scope-owned `Session` contract, and `session.connect` starts WebRTC. `createConnected` and `attachConnected` combine those steps and clean up a partial acquisition before reporting failure.

Releasing an owned session attempts and independently confirms remote termination. Releasing an attached session closes its local resources and publication claims. `session.close` returns a `CloseReport` that retains local cleanup errors and remote termination uncertainty. Supervisors can inspect that evidence without allocating another owner.

```ts
import { Effect, Redacted } from "effect";
import * as Reactor from "reactor-effect-client";

const useSession = Effect.gen(function* () {
  const client = yield* Reactor.Client;
  const session = yield* client.create({
    model: "your-model",
    jwt: Redacted.make("session-token"),
  });

  yield* session.connect;
  return yield* session.current;
});
```

The surrounding application supplies the `Client` layer and Effect platform services. Credentials, HTTP policy, scopes, and host transport selection stay visible in composition.

For a Node application using the native transport, install `@effect/platform-node@4.0.0-rc.115` and provide those dependencies around the entire scoped operation:

```ts
import { Effect, Layer } from "effect";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as Reactor from "reactor-effect-client";
import * as Native from "reactor-effect-client/native";

const clientLayer = Reactor.layer().pipe(
  Layer.provide(Layer.mergeAll(Reactor.FetchHttp.layer, NodeServices.layer, Native.layer())),
);

// useSession is the operation above. Acquiring a real session can be billable;
// the repository's ordinary verification uses controlled local peers instead.
const main = Effect.scoped(useSession).pipe(Effect.provide(clientLayer));
```

`Native.make(configuration, nativeOptions)` and `Browser.make(configuration)` also construct the canonical factory with their host peer already selected. Constructing a factory makes no allocation. HTTP and crypto services remain explicit, and `nativeOptions.libraryPath` can select a staged native artifact. The root session constructor has no filesystem or path requirement; the separate `Native.uploadFile` helper requires the host file services when used.

## Runtime choices

### Browser

`reactor-effect-client/browser` uses built-in `RTCPeerConnection` and browser media tracks. `Browser.media(session)` obtains the negotiated generation; `media.track(name)` acquires a scoped `MediaStreamTrack`, and `media.publish(name, track)` publishes through that generation. The entry point also exports track-to-frame/audio and recording helpers.

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

`Native.media(session)` obtains a decoded-media generation after the session connects. `media.video(name)` emits owned BGRA frames and `media.audio(name)` emits owned interleaved signed 16-bit PCM. Frame IDs and microsecond timestamps use `bigint`. Retaining a frame retains JavaScript-owned bytes, and native media has no browser track handles.

Browser and native media values stay bound to their negotiated generation. A reconnect creates a new generation; existing readers end or fail with their source. Applications obtain the new media generation explicitly, or opt into orchestration's recovering media streams.

The current native backend accepts at most one incoming video and one incoming audio track because pinned `reactor-webrtc` does not expose the remote track MID/identity needed to join several same-kind callbacks to SDP mappings without relying on arrival order. Ambiguous declarations fail before negotiation with `UnsupportedCapability`.

See [`native/README.md`](./native/README.md) for native build details.

## Coordinator helpers

`Coordinator` is a namespace on the root export. `Coordinator.make(configuration)` provides pricing, bounded token minting, session inspection, and termination reports through Effect HTTP services. Constructing this client makes no network request and requires no peer implementation.

## H3 provider

`H3.make(session, options)` consumes an already connected canonical `Session`. It checks the deployment contract and exposes provider state, queue and clip messages, observations, command replies, and locally correlated acceptance evidence. Provider facts remain visible even when another client authored the clip. A command ACK establishes receipt; state changes require the corresponding model evidence.

```ts
import { Effect } from "effect";
import type { Session } from "reactor-effect-client";
import * as H3 from "reactor-effect-client/h3";

const enqueueClip = (session: Session) =>
  Effect.gen(function* () {
    const provider = yield* H3.make(session);
    return yield* provider.enqueue({
      prompt: "A slow camera move through a sunlit garden",
      seconds: 5,
    });
  });
```

Use a session created with `H3.modelName`. The adapter targets the documented `0.5.5` prompt-and-images subset of `reactor/h3-reference-to-video-turbo-realtime`. Prompt-only requests are supported. Image references accept owned bytes or explicit upload references; H3 itself requires no filesystem or path services. Reference-audio input is unsupported, and FastH3 `startingFrame`/`endingFrame` fields are absent from this contract.

The adapter exposes autoplay, flush, playback, reset, and other model controls as explicit operations. Creating it does not change those policies or initiate reconnect. Request `metadata` is a provider string; orchestration keeps its richer application annotation separately.

## Orchestration and simulation

`reactor-effect-client/orchestration` owns opt-in scheduling, sequence routing, renewal, and recovering media. `fromH3` adapts a provider into a physical source, and `make({ open, ...options })` returns a handle with `engine`, `media`, `mediaState`, sequence operations, and joined cleanup reports. The `open` effect supplies each source and its lifetime budget. Application scheduling, pricing, persona, and show policy stay outside the session and H3 layers.

`ClipRequest.sameSessionAs` targets the physical session that owns a known clip, including one already ready or playing, while leaving queue position unchanged. `before` requests insertion ahead of a clip still in the generation queue. Source affinity, insertion anchors, continuation, and sequence ownership must agree; missing, conflicting, retired, or recovering ownership fails locally with a `not-submitted` outcome. This keeps a dependent request on its required session during renewal without inventing an insertion point.

`Orchestration.Submission` models an inert prepared operation. Preparation may be interrupted before commit; once execution commits, callers joining or abandoning the result do not replay the dispatch. `CommandFailure.context` distinguishes `not-submitted`, `unknown`, and `replied` outcomes independently of the transport error category.

`Orchestration.Sequences` owns bounded sequence affinity and explicit member outcomes. Partial admission is represented member-by-member as accepted, rejected, or indeterminate, and a sequence remains bound to one owner until it is sealed/retired and explicitly released.

`reactor-effect-client/simulation` provides `make` and `layerSim` over the same orchestration engine and media contracts. Its source and renderer hooks support unpaid local execution and controlled failures. Reusable fault and image fixtures are available through the separate `reactor-effect-client/testing` entry point.

## Package exports

| Export                  | Purpose                                                                                        |
| ----------------------- | ---------------------------------------------------------------------------------------------- |
| `reactor-effect-client` | Canonical `Client`, `Session`, `make`/`layer`, errors, `Coordinator`, `Peers`, and `FetchHttp` |
| `/browser`              | Browser factory, generation-scoped tracks, media conversion, and recording                     |
| `/native`               | Native factory, decoded-media generations, and explicit file upload                            |
| `/h3`                   | H3 provider contract, state/queue/clip evidence, controls, and image validation                |
| `/orchestration`        | Scheduling, renewal, source adaptation, references, submissions, and sequences                 |
| `/simulation`           | Production simulation source, factory, and Effect service layer                                |
| `/testing`              | Reusable fault and PNG fixtures                                                                |
| `/wire`                 | Generated protocol messages and wire encoding/decoding                                         |

These eight paths are the complete public export map. The former module-per-file deep imports are removed. The pack smoke test checks the exact export set, every target and declaration, and relative/external import closure from declared dependencies. It installs the preserved tarball into isolated Node and browser consumers, exercises production simulation, and verifies native preflight against the packaged library identity.

Effect `4.0.0-rc.115` itself references the global `TextDecoderOptions` type in its channel declarations. A strict Node project that deliberately omits DOM types may need to declare that standard interface. The pack check first proves this is the sole upstream diagnostic, then applies a test-only declaration; it does not enable `skipLibCheck` or hide SDK declaration errors.

## Qualification evidence

The repository distinguishes tests that have actually run from build recipes and provider claims.

### Canonical API checks

On September 22, 2026, the focused TypeScript check passed and `scripts/native-test.sh` completed Rust formatting checks, all 12 Rust tests, clippy with warnings denied, and all 15 JavaScript native tests on macOS arm64. The Rust tests include the real libwebrtc media loopback. The JavaScript tests use the staged ABI 2 library and explicit C fixtures to cover the canonical session, ABI validation, source and byte identity, decoded-media parsing, interrupted foreign-call lifetime, and media failure/cleanup. The public browser/native integration project also passed through the canonical factories: both ordered binary channels, shared command return/event attribution, browser track leases and borrowed publication sources, failed-acquisition cleanup, 12 distinct decoded 160x96 BGRA frames, and 95 mono 48 kHz PCM packets were observed. Both owners joined cleanup without local errors and confirmed their fixture allocations closed.

That run verified current native sources and the embedded identities plus sidecar hashes of the staged macOS and Linux libraries against source SHA-256 `d2584076f542ea12a964eecf8384c8b93b013672388351e1998571b0b3c69d37`; it did not rebuild or replace the staged release libraries. It exercised the macOS library, with TURN disabled. Linux runtime execution, relay behavior, and Reactor custom browser frame metadata were not requalified by this check.

### Retained earlier qualification

The following retained September 22 records predate the canonical API migration; their historical test counts and names describe those runs. Changes to the public API, fixtures, and packaged declarations need their own focused validation, even when the native source and staged libraries are unchanged.

- macOS arm64 native qualification ran locally on September 22, 2026: 8 Rust native tests, clippy with warnings denied, a release build, and 8 JavaScript native boundary tests passed. The Rust loopback used real local libwebrtc ICE/DTLS/SCTP, ordered binary channels, real video encode/decode, PCM, metadata, stats, direction/bitrate controls, and callback quiescence. A separate compiled test ABI drove the JavaScript `SessionClient` media parser and cancellation bounds.
- Linux x64 qualification also ran on September 22, 2026, inside an isolated Debian 12 container in a task-owned Lima VM, using Rosetta for x86_64 execution on the arm64 Mac. Rust 1.90.0, Clang 21.1.8, all 8 native Rust tests, clippy with warnings denied, the release build, and all 8 JavaScript ABI/parser/session tests passed. The shared library loaded through the public native entry point in the pinned Node 24.14.1 Bookworm runtime and Bun 1.4.2 with glibc 2.36. This is Linux x64 userspace execution under translation, not qualification on physical x64 hardware.
- The Linux check reproduced a build-recipe failure with Bookworm's default Clang 14: it cannot compile the pinned WebRTC libc++ headers. The source-build recipes now explicitly select Clang 21. The opt-in installer verified the LLVM signing-key fingerprint and ran successfully in the private Debian container; ordinary native build scripts never install system packages.
- A real Chrome 153 browser loaded the browser bundle and exchanged exact binary messages over both channels with the SDK browser peer and the native bridge. Browser-generated media decoded natively as changing 160x96 BGRA frames and mono 48 kHz PCM. A separate run through a loopback-only Coturn 4.18.0 fixture proved selected relay-to-relay UDP connectivity, both binary channels, and decoded audio/video. These checks use `scripts/browser-native.sh`; native cleanup failures fail the check.
- Local Mac iteration still used Bun 1.4.0 because the task-local Bun 1.4.2 binary stalled before JavaScript execution in that host harness. Linux runtime preflight did execute Bun 1.4.2 successfully. CI pins 1.4.2 explicitly.
- No paid or hosted Reactor generation was run. The local relay test does not prove hosted TURN credentials, Internet NAT/firewall traversal, or sustained production behavior. Chrome did not originate Reactor's custom frame metadata; that extension remains covered by the native/native loopback, not the browser test. Browser-to-native media was exercised; native-to-browser media publication was not.

## Build from source

The TypeScript workspace uses Bun and TypeScript. Native code requires Rust 1.90, Clang 21 on Linux (the platform compiler on macOS), curl, tar/zstd, and the platform link dependencies described in [`native/README.md`](./native/README.md).

```sh
bun install --frozen-lockfile
bun run typecheck
bun run build
bun run lint
bun run test:portable
bun run native:build # Stage if missing or the native source identity changed.
bun run test:native
bun run test:integration
bun run test:pack
```

The native and integration projects require a staged, source-identified library in `dist/native/<platform>-<arch>`. Build it when absent or when native inputs change. For a native source change, qualify it on a supported host:

```sh
./scripts/native-build.sh
./scripts/native-test.sh
```

`./scripts/native-build.sh` owns release staging. `./scripts/native-test.sh` checks Rust formatting, tests, and clippy, then runs JavaScript tests against the staged library; it never restages that artifact. The isolated Linux x64 qualification recipe requires an explicit Docker context and never changes the caller's active context:

```sh
DOCKER_CONTEXT=my-context ./scripts/native-linux-x64.sh
```

Local browser/native qualification uses an existing Chrome/Chromium executable and the staged native library:

```sh
./scripts/browser-native.sh
```

Set `BUN_BINARY` or `BROWSER_EXECUTABLE` when the tested executables are not on the default path. The optional local relay fixture is explicit: set `BROWSER_NATIVE_FORCE_RELAY=1` together with `BROWSER_NATIVE_TURN_URL`, `BROWSER_NATIVE_TURN_USERNAME`, and `BROWSER_NATIVE_TURN_PASSWORD`. The runner verifies the selected relay candidates rather than treating candidate gathering as proof. Supply a local test TURN server; this command neither starts a TURN service nor contacts Reactor.

## Contributing and security

See [CONTRIBUTING.md](./CONTRIBUTING.md) for repository workflow, validation expectations and third-party notice rules. See [SECURITY.md](./SECURITY.md) for private vulnerability reporting guidance and the project's security boundaries.

## License

The package is licensed under Apache-2.0. Third-party and derived-source notices are retained in [NOTICE](./NOTICE) and [`notices/`](./notices/).
