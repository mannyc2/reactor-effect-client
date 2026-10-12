# Browser examples

Two pages served by one local server: **H3 Studio**, an H3 session run from the browser, and the
**character page**, a voice heard on its own or spoken by a persistent Live2D character.

## H3 Studio

One page that runs an H3 Reference Turbo Realtime session from the browser and shows what the SDK knows about it: the session and its connection generations, H3's own queue as the model reports it, every clip followed from acceptance to its end, and every failure with its dispatch outcome. It runs offline with Reactor simulated in the tab by `ReactorTest`, and is built to run live on hosted Reactor from the same code; live mode has not run on a paid session yet.

### Run it

From the repository root, once:

```sh
bun install --frozen-lockfile && bun run build
cd packages/browser/examples
```

**Offline**, with `REACTOR_API_KEY` unset, so nothing is billed:

```sh
bun run start                         # http://127.0.0.1:3000
```

**Live**, on hosted Reactor:

```sh
REACTOR_API_KEY=rk_… bun run start    # http://127.0.0.1:3000
```

`start` bundles the page and starts the token server. With `REACTOR_API_KEY` the server mints a token for each session, capped at two minutes, and the page never sees the key. Reactor's pricing API states H3's rate per second, $0.035 a second or $2.10 a minute (`GET https://api.reactor.inc/pricing`, 2026-09-30; its billing page still says per session-minute), and the meter runs from ready until the session ends, so a capped session costs at most $4.20. Add `?offline` to the address to rehearse on the simulator with the key set. Without the key the server mints nothing and the page runs offline.

Closing the tab or leaving the page closes its session. The page sends the session's DELETE as a keepalive request, which the browser delivers after the page is gone: in Chrome 151, against a local stand-in for Reactor's API, it arrived after its CORS preflight once the tab had closed. A session whose page could not send it, after a crash or a lost connection, ends at the two-minute cap.

The page needs a browser with WebRTC and Web Crypto, on `localhost` or HTTPS. `HOST` and `PORT` change where the server listens. `REACTOR_API_URL` changes which coordinator the server mints tokens from; the page talks to `CoordinatorClient.defaultApiUrl`, so change both together.

**As a static playground**, offline, for any static host:

```sh
bun run build:playground              # writes dist/playground/
```

`dist/playground/` holds `index.html` and `app.js`, linked by relative paths, so it works from any base path. Module scripts do not load from `file://`; serve the directory over HTTP.

### Using the page

Start a session, write a prompt, set the clip length and send it. H3 is reference-to-video: drop PNG, JPEG or WebP images on the composer, pick them, or add the sample image the page draws. Each is validated once with `H3.validateReference` and every later clip reuses it. The length slider spans `H3.requestSeconds`, and the page shows the length H3 builds on its 24 fps frame grid.

The queue panel is H3's queue as the provider last reported it: the clip on air, the playout queue of ready clips, and the generation queue still to build. Each clip carries the facts its operation established, as chips: accepted (on the enqueue's reply, or on the clip's own metadata when the reply was lost), generated, started, and finished, stopped, popped or failed. Its controls are H3's own commands: play, move up and down, pop, and stop. A command H3's state does not list as valid, or the deployment does not offer, shows disabled.

The session panel shows the session's id, its state and connection generation, H3's availability, and two playback settings: autoplay, and holding each clip's last frame instead of flushing to black. Reconnect makes a new connection generation of the same session; the picture follows it. Stop closes the session and shows its close report: whether termination was confirmed by an independent read, the evidence, and whether the session may still bill.

A failure shows its tag, its reason and its dispatch outcome: `not-submitted` (nothing reached Reactor), `unknown` (it may have, so the SDK never sends it again) or `replied` (Reactor refused or failed it). Offline, the page can arm faults in the simulator to show them: a failed build, a lost enqueue reply that the clip's metadata then proves, a dropped enqueue whose outcome stays unknown, and a moderation verdict that ends the session.

Offline, `ReactorTest.Timing.hosted` draws every delay from ranges measured on paid hosted runs: small samples, not a replay. The simulator draws each clip as one flat colour, 336×192 here, and its soundtrack as a tone; the caption over the picture names the clip and frame each picture carries, as `ReactorTest.frameOf` reads them. Live mode is compiled and bundled by the checks and has not been run against hosted Reactor from this page.

### How it is built

The mode is a layer, chosen once as the page builds its `ManagedRuntime`. `app.ts` asks the server `GET /api/live` and picks `Live.layer` or `Offline.layer`; `playground.ts` always takes `Offline.layer`. Both provide the same services: `Reactor.Reactor`, the example's `Stage`, and `Crypto`. `Stage` holds what differs: how a session's media reaches the page, how the page describes the mode, and which faults it can arm. Everything in `Studio.ts` is the same code in both modes.

- **Live** (`Live.ts`): `Reactor.layer({ tokens })` over `CoordinatorClient.layer` and `BrowserPeer.layer`, with tokens from the server's `HttpApi` contract. Its `Stage` plays `BrowserMedia.tracks(session)` in a `<video>` and an `<audio>` with `BrowserMedia.play`. Building `BrowserPeer.layer` checks for WebRTC, so an unsupported browser fails before any session is paid for.
- **Offline** (`Offline.ts`): `ReactorTest.layer({ timing: ReactorTest.Timing.hosted, apiKey })` beneath `Reactor.layer({ tokens })` and `CoordinatorClient.layer({ apiKey })`, with tokens the simulated coordinator mints for a demo key. Its `Stage` reads `session.decoded`, keeps only the newest frame with `Stream.buffer({ capacity: 1, strategy: "sliding" })` and draws it on a `<canvas>`, converting BGRA or RGBA by each frame's `format`; sound plays the decoded PCM through Web Audio.

`Studio.ts` holds one session at a time in a `Scope` the page keeps until Stop: the session, its H3 provider, the observers and every clip's follower live in it, so one close releases everything. Button handlers are ordinary DOM code that run effects through the runtime. The picture and sound follow each ready connection generation with `Stream.switchMap` over `session.changes`. The queue panel renders `provider.changes` together with the Studio's clip cards, a `SubscriptionRef`. A clip is sent with `provider.prepare` and `submission.submit`, then followed through `provider.operation(submission)`: `accepted`, `reached("generated")`, `reached("started")` and `ended`. An enqueue whose outcome is `unknown` is still followed, since a clip that names it can prove it later. The event log reads `provider.observe()`, opened before the first command so it misses none of their replies.

| File                | What it is                                                                                                                    |
| ------------------- | ----------------------------------------------------------------------------------------------------------------------------- |
| `src/app.ts`        | The served page's entry: chooses live or offline as it builds the runtime                                                     |
| `src/playground.ts` | The static playground's entry: offline only                                                                                   |
| `src/Stage.ts`      | The service that holds what differs between the modes                                                                         |
| `src/Live.ts`       | Live layer: server tokens, `BrowserPeer`, media elements                                                                      |
| `src/Offline.ts`    | Offline layer: `ReactorTest`, decoded frames on a canvas, faults                                                              |
| `src/Studio.ts`     | The session, the queue's commands, each clip's operation, the page's handlers                                                 |
| `src/Page.ts`       | The DOM: elements, and how state is drawn into them                                                                           |
| `src/Api.ts`        | The server's contract, shared by the server and the pages' typed clients                                                      |
| `src/server.ts`     | Mints tokens, streams speech and serves both pages (Node, `@effect/platform-node`)                                            |
| `src/WebCrypto.ts`  | Effect's `Crypto` over Web Crypto, as `@effect/platform-browser`'s `BrowserCrypto.layer` provides it, without that dependency |
| `web/index.html`    | The page and its styles                                                                                                       |

The server's endpoint is for a local demo. A real deployment authenticates and rate-limits it, and binds only sessions the caller created: every token it hands out can start or act on a paid session.

`bun run check:examples` bundles both entries and fails if either reaches Node or native code, and `bun run typecheck` compiles the page with DOM types and no Node types.

## Character page

`/character` on the same server: one voice, heard on its own or spoken by a Live2D character.
Both modes use the same speech model and the library's `BrowserAudio` playback; the character adds
an application renderer driven by `BrowserCharacter`, following what is heard.

```sh
OPENROUTER_API_KEY=… OPENROUTER_SPEECH_MODEL=… OPENROUTER_SPEECH_VOICE=… bun run start
```

Choose a speech model and its supported voice from [OpenRouter's audio API](https://openrouter.ai/docs/api/api-reference/tts/create-speech). The example uses
`/api/v1/audio/speech` with PCM output; chat-only models cannot synthesize these lines.

Write a line and press Say. Say again, or Stop, interrupts the line playing. Without the OpenRouter
variables the server has no speech and the page says so; Play a local audio file sends any file the
browser can decode through the same playback, with no provider and no cost.

- **Speech.** The server runs `SpeechModel`, which `OpenRouterSpeech.layerConfig` provides when
  `OPENROUTER_API_KEY` is set, and streams each line to the page as server-sent events: PCM blocks as
  they are generated, then either the end or the provider's `SpeechError` with its reason and
  dispatch outcome, before or after audio. The page's `Speech.layer` is a `SpeechModel` over that
  endpoint, so the key stays on the server. A line has at most 500 characters. Saying one calls
  OpenRouter and uses the selected model's pricing. Interrupting a line releases its request on
  the server; that does not prove the provider stopped generating or billing it.
- **Playback.** `BrowserAudio.make` acquires the library's scoped Web Audio output. It schedules
  each block as it arrives, at most one second ahead of the output, so a line generated faster than
  real time waits in its stream (and in network buffers) rather than piling up in the audio graph.
  A line arriving slower than real time
  plays with gaps. A line ends when the output has played its last block, not when its response
  ends, and interrupting it silences everything it scheduled at once.
- **Character.** The model is the page's, not the line's: it loads once and keeps its idle motion,
  blinking, breathing and physics between lines. Its mouth follows the level of what the output is
  playing, read from an analyser on each animation frame, so it opens with the sound you hear rather
  than with the network, and closes when the sound stops, however the line ended. Audio only releases
  the character; leaving the page interrupts the line and releases both. A line has no known length
  and the character never ends, so neither is a `Playout` clip. `Avatar` adapts the loaded model to
  the client's `Character.Renderer`; `BrowserCharacter.animate` owns the animation cadence and
  mouth smoothing, and returns render or playback failures to the page.

The reusable contracts live in `reactor-effect-client` and their browser implementations in
`reactor-effect-browser`. This page owns its UI, local audio decoder, speech endpoint, credential
configuration and Cubism loader. Other applications can use the same library APIs with a different
speech provider or renderer. `BrowserAudio` also exposes its scoped audio track, and
`BrowserCharacter.capture` acquires a canvas video track, for application recording or publishing;
this page's controls demonstrate local playback.

### Live2D setup

Cubism Core and the model are obtained separately under Live2D's terms and are not bundled with
these packages. Without them the page names what is missing and plays the voice on its own.

1. Download the [Cubism SDK for Web](https://www.live2d.com/en/sdk/download/web/) and unzip it,
   retaining its licence files. The SDK includes the
   [Hiyori Momose sample](https://www.live2d.com/en/learn/sample/momose-hiyori/) used by this demo;
   the download and sample pages describe their terms.
2. Extract or link it here, where Git ignores it:
   `ln -s /path/to/CubismSdkForWeb-5-r.5 packages/browser/examples/cubism`.
3. In `packages/browser/examples`, `bun run build:live2d` typechecks the adapter and model renderer
   against the SDK's Framework sources and bundles them into `dist/live2d/adapter.js`.
4. Start the server with `LIVE2D_MODEL` naming the `.model3.json` of a model you may use, such as one
   of the SDK's samples under its own terms:
   `LIVE2D_MODEL=cubism/Samples/Resources/Hiyori/Hiyori.model3.json bun run start`. The server serves
   that model's directory, the SDK's Core script and the Framework's WebGL shaders.

The adapter draws with the Framework's own WebGL renderer, plays the model's `Idle` motions in
turn, blinks, breathes, and runs the model's physics and pose. Playback sets the parameters in
the model's `LipSync` group, or Cubism's standard `ParamMouthOpenY` when it names none, to the
measured mouth opening scaled by 0.8. This overrides idle mouth motions so silence closes the
mouth. It expects the SDK's layout (`Core/`
beside `Framework/`) and WebGL 2, and targets SDK 5-r.5. Core loads before the Framework is
initialised. Automated package checks do not download the SDK or a model; rendering can be
checked locally with Hiyori and the page's audio-file control.

| File                   | What it is                                                                                                |
| ---------------------- | --------------------------------------------------------------------------------------------------------- |
| `src/character.ts`     | The character page: its controls, the line playing, the mode, and the level meter                         |
| `src/Speech.ts`        | The page's `SpeechModel`, over the server's speech endpoint                                               |
| `src/Avatar.ts`        | Loads and scopes the Cubism model, adapts it to `Character.Renderer`, and runs `BrowserCharacter.animate` |
| `live2d/adapter.ts`    | Loads Cubism Core before importing the model renderer                                                     |
| `live2d/model.ts`      | The model, motions, textures and rendering over the Cubism SDK's Framework                                |
| `tsconfig.live2d.json` | Compiles the adapter with the Framework's looser settings                                                 |
| `web/character.html`   | The page and its styles                                                                                   |

`bun run check:examples` bundles the character page with the Studio and checks it reaches neither
Node nor native code. The adapter is outside the repository's typecheck and lint, since it needs the
SDK; `build:live2d` checks it where the SDK is linked.
