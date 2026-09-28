# reactor-effect-browser

Browser WebRTC host for [`reactor-effect-client`](https://www.npmjs.com/package/reactor-effect-client). `BrowserPeer` binds the session's `Peer` port to the built-in `RTCPeerConnection`, and `BrowserMedia` reads a connected session's tracks as the browser's own `MediaStreamTrack`s and plays them.

This is not an official Reactor SDK.

## Install

```sh
npm install reactor-effect-client reactor-effect-browser effect@4.0.0-rc.117
```

`reactor-effect-client` and Effect `4.0.0-rc.117` are peer dependencies (`^4.0.0-rc.117`); later rc releases are accepted without a forced SDK bump. The package needs a browser with WebRTC. It has no Node dependency: its declarations compile with DOM types and without `@types/node`, and the workspace's installed-package check bundles it for the browser and runs that bundle without the Node `Buffer` global.

## Usage

`BrowserPeer.layer` supplies the `PeerFactory` for `Reactor.layer()`. Building it checks for `RTCPeerConnection` and `MediaStream`, and fails with `UnsupportedHost` without them, before any session is allocated. The HTTP client stays explicit.

```ts
import { Effect, Layer } from "effect";
import * as FetchHttpClient from "effect/unstable/http/FetchHttpClient";
import * as Coordinator from "reactor-effect-client/Coordinator";
import * as Reactor from "reactor-effect-client/Reactor";
import { BrowserMedia, BrowserPeer } from "reactor-effect-browser";

const reactorLayer = Reactor.layer().pipe(
  Layer.provide(
    Layer.mergeAll(Coordinator.layer({ apiUrl: "https://api.reactor.inc" }), BrowserPeer.layer),
  ),
  Layer.provide(FetchHttpClient.layer),
);

const watch = (element: HTMLVideoElement) =>
  Effect.gen(function* () {
    const reactor = yield* Reactor.Reactor;
    const session = yield* reactor.create({ model: "your-model" });
    const tracks = yield* BrowserMedia.tracks(session);
    yield* BrowserMedia.play(yield* tracks.track("main_video"), element);
    return yield* Effect.never;
  }).pipe(Effect.scoped);
```

`BrowserMedia.tracks(session)` returns the session's current generation. `tracks.track(name)` acquires a clone of a received track that stops when its scope closes; `publish`, `unpublish`, `setTrackActive` and `setMaxBitrate` act on that generation, and a reconnect makes a new one that the application obtains again.

`BrowserMedia.play(track, element, { playTimeout })` plays a clone of `track` in `element` until the scope closes, which stops the clone and detaches it. It refuses a track that is not live and an element that already has a source, and fails when starting takes longer than `playTimeout`, 10 seconds by default. Starting playback does not establish that anyone saw it.

The peer opens the control channel before the data channel. It fails the connection with `Overflow` when a message exceeds the bound SCTP negotiated (at most 256 KiB), and refuses a `send`, before submitting it, while more than 1 MiB waits in the channel's buffer. Closing the peer closes both channels and the connection and stops every track it received or leased.

## Example

[`examples/`](https://github.com/mannyc2/reactor-effect-client/tree/main/packages/browser/examples) is a page that runs its own session over the browser's WebRTC, with a server that only mints short tokens: `ManagedRuntime` behind ordinary DOM code, a session held in a scope until Stop, and a clip followed through its operation facts.

## Development

This package is built and tested from the workspace root; see the repository [CONTRIBUTING](https://github.com/mannyc2/reactor-effect-client/blob/main/CONTRIBUTING.md). Its tests run the peer and playback against DOM fakes on Node and Bun: they check ordering, bounds and ownership, not browser WebRTC or codec support. Real Chrome interoperability with the native host is exercised by the workspace's `integration` project.

## License

Apache-2.0. See [NOTICE](./NOTICE) and [`notices/`](./notices/).
