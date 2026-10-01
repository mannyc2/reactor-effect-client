# reactor-effect-browser

Run [Reactor](https://reactor.inc) sessions from a web page with
[`reactor-effect-client`](https://www.npmjs.com/package/reactor-effect-client). `BrowserPeer` carries
the session on the browser's own `RTCPeerConnection`, and `BrowserMedia` plays the session's tracks
in your page's media elements. The page talks to Reactor directly for the lowest latency; your
server only mints its session tokens, so the API key never reaches the browser.

**[Documentation](https://mannyc2.github.io/reactor-effect-client/)** ·
[Sessions in the browser](https://mannyc2.github.io/reactor-effect-client/guides/browser/) ·
[Example](https://github.com/mannyc2/reactor-effect-client/tree/main/packages/browser/examples)

This is an independent project, not an official Reactor SDK.

## Install

```sh
npm install --save-exact reactor-effect-client reactor-effect-browser effect@4.0.0-rc.117
```

`reactor-effect-client` and Effect `4.0.0-rc.117` are peer dependencies, both exact: a later Effect rc needs a new SDK release. The package needs a browser with WebRTC. It has no Node dependency: its declarations compile with DOM types and without `@types/node`, and the workspace's installed-package check bundles it for the browser and runs that bundle without the Node `Buffer` global.

## Usage

`BrowserPeer.layer` supplies the `PeerFactory` for `Reactor.layer()`. Building it checks for `RTCPeerConnection` and `MediaStream`, and fails with `UnsupportedHost` without them, before any session is allocated. The HTTP client stays explicit. The page never holds the API key: its `CoordinatorClient.Tokens` ask the application's server for a token that creates the session and then, before each expires, for one bound to it.

```ts
import { Effect, Layer } from "effect";
import * as FetchHttpClient from "effect/unstable/http/FetchHttpClient";
import * as CoordinatorClient from "reactor-effect-client/CoordinatorClient";
import * as H3 from "reactor-effect-client/H3";
import * as Reactor from "reactor-effect-client/Reactor";
import { BrowserMedia, BrowserPeer } from "reactor-effect-browser";

const reactorLayer = Reactor.layer().pipe(
  Layer.provide(
    Layer.mergeAll(
      CoordinatorClient.layer({ apiUrl: "https://api.reactor.inc" }),
      BrowserPeer.layer,
    ),
  ),
  Layer.provide(FetchHttpClient.layer),
);

// The tokens come from the application's server, which holds the API key.
const watch = (element: HTMLVideoElement, tokens: CoordinatorClient.Tokens) =>
  Effect.gen(function* () {
    const reactor = yield* Reactor.Reactor;
    const session = yield* reactor.create({ model: H3.modelName, tokens });
    const tracks = yield* BrowserMedia.tracks(session);
    yield* BrowserMedia.play(yield* tracks.track("main_video"), element);
    return yield* Effect.never;
  }).pipe(Effect.scoped);
```

`BrowserMedia.tracks(session)` returns the session's current generation. `tracks.track(name)` acquires a clone of a received track that stops when its scope closes; `publish`, `unpublish`, `setTrackActive` and `setMaxBitrate` act on that generation, and a reconnect makes a new one that the application obtains again. The session reconnects a dropped connection on its own, so an application holding tracks follows `session.changes` to the next ready generation and obtains them again.

`BrowserMedia.play(track, element, { playTimeout })` plays a clone of `track` in `element` until the scope closes, which stops the clone and detaches it. It refuses a track that is not live and an element that already has a source, and fails when starting takes longer than `playTimeout`, 10 seconds by default. Starting playback does not establish that anyone saw it.

The peer opens the control channel before the data channel. It fails the connection with `Overflow` when a message exceeds the bound SCTP negotiated (at most 256 KiB), and refuses a `send`, before submitting it, while more than 1 MiB waits in the channel's buffer. Closing the peer closes both channels and the connection and stops every track it received or leased.

## Example

[`examples/`](https://github.com/mannyc2/reactor-effect-client/tree/main/packages/browser/examples) is a page that runs its own session over the browser's WebRTC, with a server that only mints short tokens: `ManagedRuntime` behind ordinary DOM code, a session held in a scope until Stop, and a clip followed through its operation facts.

## Development

This package is built and tested from the workspace root; see the repository [CONTRIBUTING](https://github.com/mannyc2/reactor-effect-client/blob/main/CONTRIBUTING.md). Its tests run the peer and playback against DOM fakes on Node and Bun: they check ordering, bounds and ownership, not browser WebRTC or codec support. Real Chrome interoperability with the native host is exercised by the workspace's `integration` project.

## License

Apache-2.0. See [NOTICE](./NOTICE) and [`notices/`](./notices/).
