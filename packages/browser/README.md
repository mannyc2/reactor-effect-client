# reactor-effect-browser

Play streaming speech, animate a persistent character, and run [Reactor](https://reactor.inc)
sessions from a web page with
[`reactor-effect-client`](https://www.npmjs.com/package/reactor-effect-client). `BrowserAudio`
plays PCM and exposes an audio track; `BrowserCharacter` drives your renderer and captures a
canvas as a video track. `BrowserPeer` carries the session on the browser's own
`RTCPeerConnection`, and `BrowserMedia` plays the session's tracks in your page's media elements.
For Reactor sessions, the page talks to Reactor directly while your server mints session tokens,
so the API key never reaches the browser.

**[Documentation](https://mannyc2.github.io/reactor-effect-client/)** ·
[Sessions in the browser](https://mannyc2.github.io/reactor-effect-client/guides/browser/) ·
[Example](https://github.com/mannyc2/reactor-effect-client/tree/main/packages/browser/examples)

This is an independent project, not an official Reactor SDK.

## Install

```sh
npm install --save-exact reactor-effect-client reactor-effect-browser effect@4.0.0
```

`reactor-effect-client` is a peer dependency at exactly this package's version, and Effect at `~4.0.0`, any 4.0.x patch: Effect 4.1 needs a new SDK release. Audio playback needs Web Audio; Reactor sessions need WebRTC. The package has no Node dependency: its declarations compile with DOM types and without `@types/node`, and the workspace's installed-package check bundles it for the browser and runs that bundle without the Node `Buffer` global.

## Streaming audio and characters

`BrowserAudio.make` acquires one locally audible output in a scope. It accepts the client's
`Media.AudioFrame` stream from any speech provider or application decoder:

```ts
import { Effect } from "effect";
import * as SpeechModel from "reactor-effect-client/SpeechModel";
import * as BrowserAudio from "reactor-effect-browser/BrowserAudio";

const greeting = Effect.gen(function* () {
  const speech = yield* SpeechModel.SpeechModel;
  const playback = yield* BrowserAudio.make;
  yield* playback.play(speech.stream({ text: "Welcome to the programme." }));
}).pipe(Effect.scoped);
```

Provide an application `SpeechModel` layer at the entry point. For a browser application, keep
provider credentials on its server; the [character example](./examples#character-page) streams
audio from its server through the same interface. Start playback from a user gesture when the
browser requires one; starting fails with `Timeout` after ten seconds.

One output plays one stream at a time. A concurrent `play` fails with `Busy`; the application
chooses whether to interrupt the current utterance or queue another. Completion waits for the last
sample to finish local playback, and failure or interruption silences queued samples. The output
schedules at most one second ahead, including when one input block is larger; this does not bound
buffers in the source stream or network. `level` reads the RMS amplitude of audio actually playing.
PCM format failures, host failures and use after scope closure are typed `PlaybackError`s.

`BrowserCharacter.animate(renderer, playback.level)` advances a `Character.Renderer` on browser
animation frames and smooths its mouth opening from the output level. It runs until interrupted
or a renderer or level read fails, preserving that failure for the caller. Interruption cancels
the pending animation frame. The application loads and scopes its renderer, including any Live2D
SDK, model assets and textures; the library supplies no model or SDK. A background tab may suspend
animation.

`playback.track` is a real audio `MediaStreamTrack` borrowed from the playback scope.
`BrowserCharacter.capture(canvas, { frameRate: 30 })` acquires a video track in a scope, with 30 fps
as the default. Applications can combine them in `new MediaStream([playback.track, videoTrack])`
for `MediaRecorder`, play them through `BrowserMedia.play`, or publish them through
`BrowserMedia.tracks(session).publish`. Keep the owning scopes open while consuming the tracks;
closing them stops the tracks. Do not stop the borrowed audio track yourself. Capture and rendering
failures use `CharacterError`; neither a live track nor completed local playback proves delivery
to an audience.

These APIs handle streamed audio and persistent presentation. `Playout` continues to schedule
finite clips; recording, mixing, publishing and the choice of renderer belong to the application.

## Reactor sessions

`BrowserPeer.layer` supplies the `PeerFactory` for `Reactor.layer()`. Building it checks for `RTCPeerConnection` and `MediaStream`, and fails with `UnsupportedHost` without them, before any session is allocated. The HTTP client stays explicit. The page never holds the API key: its `CoordinatorClient.Tokens` ask the application's server for a token that creates the session and then, before each expires, for one bound to it.

```ts
import { Effect, Layer } from "effect";
import * as FetchHttpClient from "effect/http/FetchHttpClient";
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

## Examples

[H3 Studio](https://github.com/mannyc2/reactor-effect-client/tree/main/packages/browser/examples) is
a page that runs its own session: H3's queue live with its play, move, pop and stop controls,
reference images validated once and reused, each clip's lifecycle, and failures shown with their
dispatch outcome. With an API key on its server it is built to run on hosted H3 over `BrowserPeer`,
which has not yet run on a paid session; without one it runs on `ReactorTest` in the tab. The
offline build is the [playground](https://mannyc2.github.io/reactor-effect-client/playground/) on
the documentation site.

The same server's [character page](./examples#character-page) consumes `BrowserAudio` and
`BrowserCharacter` with OpenRouter speech or a local audio file. It can play audio alone or animate
the locally configured Live2D Hiyori model; the example owns its controls, server and Cubism adapter.

## Development

This package is built and tested from the workspace root; see the repository [CONTRIBUTING](https://github.com/mannyc2/reactor-effect-client/blob/main/CONTRIBUTING.md). Its tests run the peer and playback against DOM fakes on Node and Bun: they check ordering, bounds and ownership, not browser WebRTC or codec support. Real Chrome interoperability with the native host is exercised by the workspace's `integration` project.

## License

Apache-2.0. See [NOTICE](./NOTICE) and [`notices/`](./notices/).
