# In the browser

`reactor-effect-browser` runs a session over the browser's own WebRTC: the page allocates and connects the session, plays its tracks in media elements and sends prompts, with no media passing through your server. The server's one job is to hold the API key and mint short session tokens.

```sh
bun install && bun run build                 # from the repository root
cd packages/browser/examples
bun run build                                # bundles src/app.ts into dist/app.js
REACTOR_API_KEY=… node src/server.ts         # http://127.0.0.1:3000
```

Start opens a paid session of at most five minutes (the creating token's cap), Send enqueues a prompt, Sound on plays the audio track, and Stop closes the session. Needs a browser with WebRTC and Web Crypto, on `localhost` or HTTPS. `HOST`, `PORT` and `REACTOR_API_URL` change where the server listens and which coordinator it mints from.

## What it shows

- **Keys stay on the server.** `server.ts` serves one `HttpApi` endpoint, `POST /api/token`, over `Coordinator.tokens`: without a `session` it mints a token that may create one H3 session, capped at five minutes; with one, a token bound to that open session. The page gets tokens, never the key. A real deployment authenticates and rate-limits that endpoint, and binds only sessions the caller created: every token can start or act on a paid session.
- **One contract for both sides.** `Api.ts` defines the endpoint once; the server implements it with `HttpApiBuilder` and the page calls it through `HttpApiClient.make(Api)`.
- **Effect behind ordinary UI code.** The page builds one `ManagedRuntime` from the SDK's layers (`Reactor.layer()` over `Coordinator.layer`, `BrowserPeer.layer`, `FetchHttpClient.layer` and Web Crypto) and its button handlers are plain DOM code that call `runtime.runPromise`. Building `BrowserPeer.layer` checks for WebRTC, so an unsupported browser fails before any session is paid for.
- **A session that outlives its first token.** The page creates the session with `tokens: { create, bind }` over the endpoint, so before the creating token expires the session asks the server for one bound to itself and carries on with it.
- **A session held by the page.** Start creates a `Scope` that the page keeps; the session, its peer, its tracks and their playback (`BrowserMedia.play`) all live in it, so Stop closes the session, which terminates it and returns its `CloseReport` (whether termination was confirmed), then closes the scope, which releases everything else and reuses that report. Starting takes seconds, so the page tracks it: a second Start is ignored until the first settles (it would otherwise open a second paid session that nothing could stop), and a Stop pressed meanwhile closes the session as soon as it has connected. Leaving the page stops it too.
- **A clip followed to its end.** Send prepares a submission, submits it, and follows `provider.operation(submission)` through `generated`, `started` and `ended`.
- **Autoplay rules.** The video plays muted as soon as it connects; audio waits for its own click, because browsers refuse unmuted playback that starts seconds after the gesture that asked for it.

| File               | What it is                                                                             |
| ------------------ | -------------------------------------------------------------------------------------- |
| `src/Api.ts`       | The token endpoint's contract, shared by the server and the page                       |
| `src/server.ts`    | Mints tokens and serves the page (Node, `@effect/platform-node`)                       |
| `src/app.ts`       | The page's code: DOM types only, bundled by `bun build`                                |
| `src/WebCrypto.ts` | Effect's `Crypto` over Web Crypto; the pinned Effect stack has no browser layer for it |

It is compiled and bundled by the checks, never run by them: it needs a paid session. `bun run check:examples` fails if the bundle reaches Node or native code, and `bun run test:pack` compiles the page against the installed archives with DOM types and no Node types.
