# reactor-effect-client

An independent Effect SDK for scoped Reactor sessions, H3 provider state and explicit orchestration. This is the portable core of the [reactor-effect workspace](https://github.com/mannyc2/reactor-effect-client); browser and native transports are the separate `reactor-effect-browser` and `reactor-effect-native` packages.

One canonical `Session` owns each allocation or attachment, its commands, connection generations, and cleanup evidence. Host packages select transport capabilities. H3 consumes that same session and exposes provider state. Applications opt into the separate orchestration and simulation modules when they need scheduling, sequence affinity, or renewal.

This is not an official Reactor SDK. Protocol material is attributed in [NOTICE](./NOTICE) and [`notices/`](./notices/).

## Install

```sh
npm install reactor-effect-client effect@4.0.0-rc.115
```

Effect `4.0.0-rc.115` is an exact peer dependency. Every module in this package is portable: importing it selects no host and loads no native code. Add `reactor-effect-browser` or `reactor-effect-native` for a transport.

## Modules

| Import                                | Purpose                                                                                        |
| ------------------------------------- | ---------------------------------------------------------------------------------------------- |
| `reactor-effect-client`               | Canonical `Client`, `Session`, `make`/`layer`, errors, `Coordinator`, `Peers`, and `FetchHttp` |
| `reactor-effect-client/h3`            | H3 provider contract, state/queue/clip evidence, controls, and image validation                |
| `reactor-effect-client/orchestration` | Scheduling, renewal, source adaptation, references, submissions, and sequences                 |
| `reactor-effect-client/simulation`    | Production simulation source, factory, and Effect service layer                                |
| `reactor-effect-client/testing`       | Reusable fault and PNG fixtures                                                                |
| `reactor-effect-client/wire`          | Generated protocol messages and wire encoding/decoding                                         |
| `reactor-effect-client/host`          | Host transport extension surface used by the first-party host packages                         |

These seven paths are the complete public export map. Internal file layout does not create additional supported deep imports. `/host` is published for `reactor-effect-browser` and `reactor-effect-native`, which pin this exact version; applications compose hosts through those packages and never need it.

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

The surrounding application supplies the `Client` layer and Effect platform services. Credentials, HTTP policy, scopes, and host transport selection stay visible in composition. A host package supplies the `PeerFactory`:

```ts
import { Effect, Layer } from "effect";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as Reactor from "reactor-effect-client";
import * as Native from "reactor-effect-native";

const clientLayer = Reactor.layer().pipe(
  Layer.provide(Layer.mergeAll(Reactor.FetchHttp.layer, NodeServices.layer, Native.layer())),
);

// useSession is the operation above. Acquiring a real session can be billable;
// the repository's ordinary verification uses controlled local peers instead.
const main = Effect.scoped(useSession).pipe(Effect.provide(clientLayer));
```

`Native.make(configuration, nativeOptions)` and `Browser.make(configuration)` also construct the canonical factory with their host peer already selected. Constructing a factory makes no allocation. HTTP and crypto services remain explicit. The root session constructor has no filesystem or path requirement.

Browser and native media values stay bound to their negotiated generation. A reconnect creates a new generation; existing readers end or fail with their source. Applications obtain the new media generation explicitly, or opt into orchestration's recovering media streams.

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

## Wire module

`reactor-effect-client/wire` is generated from the tracked canonical protocol sources under [`wire/`](./wire/README.md) by `wire/generate.py` (CPython 3.13, standard library only). `bun run generate:check` fails on any byte difference; `bun run generate:wire` rewrites the module. Do not format the generated file or edit it independently of its inputs.

## Declarations without DOM types

Effect `4.0.0-rc.115` itself references the global `TextDecoderOptions` type in its channel declarations. A strict Node project that deliberately omits DOM types may need to declare that standard interface. The workspace's installed-package check first proves this is the sole upstream diagnostic, then applies a test-only declaration; it does not enable `skipLibCheck` or hide SDK declaration errors.

## Development

This package is built and tested from the workspace root; see the repository [CONTRIBUTING](https://github.com/mannyc2/reactor-effect-client/blob/main/CONTRIBUTING.md). Inside `packages/client`, `bun run test` runs the portable suite under Vitest and `bun run typecheck` checks the source closure both with browser types and with Node types.

## License

Apache-2.0. Third-party and derived-source notices are retained in [NOTICE](./NOTICE) and [`notices/`](./notices/).
