# H3 Studio

One page that runs an H3 Reference Turbo Realtime session from the browser and shows what the SDK knows about it: the session and its connection generations, H3's own queue as the model reports it, every clip followed from acceptance to its end, and every failure with its dispatch outcome. It runs offline with Reactor simulated in the tab by `ReactorTest`, and is built to run live on hosted Reactor from the same code; live mode has not run on a paid session yet.

## Run it

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

## Using the page

Start a session, write a prompt, set the clip length and send it. H3 is reference-to-video: drop PNG, JPEG or WebP images on the composer, pick them, or add the sample image the page draws. Each is validated once with `H3.validateReference` and every later clip reuses it. The length slider spans `H3.requestSeconds`, and the page shows the length H3 builds on its 24 fps frame grid.

The queue panel is H3's queue as the provider last reported it: the clip on air, the playout queue of ready clips, and the generation queue still to build. Each clip carries the facts its operation established, as chips: accepted (on the enqueue's reply, or on the clip's own metadata when the reply was lost), generated, started, and finished, stopped, popped or failed. Its controls are H3's own commands: play, move up and down, pop, and stop. A command H3's state does not list as valid, or the deployment does not offer, shows disabled.

The session panel shows the session's id, its state and connection generation, H3's availability, and two playback settings: autoplay, and holding each clip's last frame instead of flushing to black. Reconnect makes a new connection generation of the same session; the picture follows it. Stop closes the session and shows its close report: whether termination was confirmed by an independent read, the evidence, and whether the session may still bill.

A failure shows its tag, its reason and its dispatch outcome: `not-submitted` (nothing reached Reactor), `unknown` (it may have, so the SDK never sends it again) or `replied` (Reactor refused or failed it). Offline, the page can arm faults in the simulator to show them: a failed build, a lost enqueue reply that the clip's metadata then proves, a dropped enqueue whose outcome stays unknown, and a moderation verdict that ends the session.

Offline, `ReactorTest.Timing.hosted` draws every delay from ranges measured on paid hosted runs: small samples, not a replay. The simulator draws each clip as one flat colour, 336×192 here, and its soundtrack as a tone; the caption over the picture names the clip and frame each picture carries, as `ReactorTest.frameOf` reads them. Live mode is compiled and bundled by the checks and has not been run against hosted Reactor from this page.

## How it is built

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
| `src/Api.ts`        | The server's contract, shared by the server and the page's typed client                                                       |
| `src/server.ts`     | Mints tokens and serves the page (Node, `@effect/platform-node`)                                                              |
| `src/WebCrypto.ts`  | Effect's `Crypto` over Web Crypto, as `@effect/platform-browser`'s `BrowserCrypto.layer` provides it, without that dependency |
| `web/index.html`    | The page and its styles                                                                                                       |

The server's endpoint is for a local demo. A real deployment authenticates and rate-limits it, and binds only sessions the caller created: every token it hands out can start or act on a paid session.

`bun run check:examples` bundles both entries and fails if either reaches Node or native code, and `bun run typecheck` compiles the page with DOM types and no Node types.
