# reactor-effect-browser

Browser WebRTC host for [`reactor-effect-client`](https://www.npmjs.com/package/reactor-effect-client). It selects the built-in `RTCPeerConnection` and browser media tracks as the transport for the canonical `Session`, and exposes generation-scoped tracks, track-to-frame/audio conversion, and recording helpers.

This is not an official Reactor SDK.

## Install

```sh
npm install reactor-effect-client reactor-effect-browser effect@4.0.0-rc.115
```

`reactor-effect-client` and Effect `4.0.0-rc.115` are exact peer dependencies. The package needs a secure context with WebRTC and Web Crypto. It has no Node dependency: its declarations compile with DOM types and without `@types/node`, and the workspace's installed-package check bundles it for the browser and runs that bundle without the Node `Buffer` global.

## Usage

`Browser.layer` supplies the `PeerFactory` for `Reactor.layer()`. Constructing a factory makes no allocation. Effect HTTP and crypto services remain explicit.

```ts
import { Effect, Layer } from "effect";
import * as Reactor from "reactor-effect-client";
import * as Browser from "reactor-effect-browser";

const clientLayer = Reactor.layer({ apiUrl: "https://api.reactor.inc" }).pipe(
  Layer.provide(Layer.mergeAll(Reactor.FetchHttp.layer, Browser.layer)),
);

const useTrack = Effect.scoped(
  Effect.gen(function* () {
    const client = yield* Reactor.Client;
    const session = yield* client.createConnected({ model: "your-model" });
    const media = yield* Browser.media(session);
    return yield* media.track("main_video");
  }),
);
```

The application also provides Effect's `Crypto` service, backed by Web Crypto.

`Browser.media(session)` obtains the negotiated generation; `media.track(name)` acquires a scoped `MediaStreamTrack`, and `media.publish(name, track)` publishes through that generation. Media values stay bound to the generation that negotiated them. A reconnect creates a new generation; existing readers end or fail with their source, and applications obtain the new generation explicitly.

The entry point also exports `videoFrames`, `audioSamples`, `webAudioSamples`, `audioContext`, `play`, `nextPresentation` and the `Recording` namespace, whose `downloadClip(coordinator, clip, options)` transfers a prepared recording within a caller wall deadline through the portable `Coordinator.Client` from `Coordinator.make()`; it is the same operation as `coordinator.downloadClip(clip, options)`.

If the host lacks `RTCPeerConnection` or `MediaStream`, building `Browser.layer` fails with `UnsupportedHost`, before any coordinator request can be made.

## Example

[`examples/`](https://github.com/mannyc2/reactor-effect-client/tree/main/packages/browser/examples) is a page that runs its own session over the browser's WebRTC, with a server that only mints short tokens: `ManagedRuntime` behind ordinary DOM code, a session held in a scope until Stop, and a clip followed through its operation facts.

## Development

This package is built and tested from the workspace root; see the repository [CONTRIBUTING](https://github.com/mannyc2/reactor-effect-client/blob/main/CONTRIBUTING.md). Its tests are simulated-host policy tests: they do not establish browser WebRTC or codec support. Real Chrome interoperability with the native host is exercised by the workspace's `integration` project.

## License

Apache-2.0. See [NOTICE](./NOTICE) and [`notices/`](./notices/).
