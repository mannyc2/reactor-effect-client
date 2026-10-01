# Watch Reactor in your terminal

One H3 session plays a short playlist, and its video is drawn right in the terminal: each frame is decoded in this process and shown as 24-bit colour text, about twelve times a second. Every clip is followed through the facts H3 reports about it (accepted, generated, playing, ended), and when the playlist is over the session is closed and the program says whether Reactor confirmed its end.

It runs offline by default, on the SDK's simulated Reactor, so trying it costs nothing. With an API key only the layer beneath it changes: the program is written to run on hosted Reactor, and its pieces ran in paid checks under Bun, though it has not run there itself.

## Run it offline

Needs Node 22.18 or newer (it runs the TypeScript sources directly) or Bun, and a terminal with 24-bit colour. From the repository root:

```sh
bun install && bun run build
cd examples/terminal
node src/main.ts            # the built-in playlist; bun src/main.ts works the same
node src/main.ts "A paper boat on a rain-soaked street" "A red kite over a green hill"
node src/main.ts --help
```

Offline, Reactor is simulated in memory at the timing paid runs measured on hosted H3. Its frames are a flat colour per clip at 320x180, so the picture changes from clip to clip; the model's own video appears only live. On 2026-10-01 the simulated session connected about 5 seconds after launch, the first picture came at about 8 seconds, the screen was redrawn 11 to 12 times a second while clips played, and the four-clip playlist ended 41 seconds after launch, under Node and under Bun.

When standard output is not a terminal (a pipe, a CI log), nothing is drawn and each step is a line:

```text
offline: Reactor simulated in memory, priced as hosted H3; nothing is billed (set REACTOR_API_KEY to watch hosted Reactor)
at most 4.20 USD: the session is capped at 120 seconds, at 350 credits a second (10000 credits = 1 USD)
session sess_reactor_test_1 connected
clip 1 accepted
clip 2 accepted
clip 3 accepted
clip 4 accepted
clip 1 generated
video 320x180 BGRA
clip 1 playing: A lighthouse on a sea cliff at dusk, storm waves bursting white against the rocks, …
clip 2 generated
clip 3 generated
clip 1 ended
clip 2 playing: A neon-lit Tokyo alley at night in the rain, …
…
clip 4 ended
session sess_reactor_test_1 closed: termination confirmed
```

## Run it live

```sh
REACTOR_API_KEY=rk_… node src/main.ts
```

With `REACTOR_API_KEY` set, the program runs on hosted Reactor and the session is billed. It mints the session's token in this process with the key, and the token caps the session at two minutes: Reactor ends the session then, whatever becomes of this process. Before allocating anything it prints the most the run can cost, from the rate Reactor's pricing API states and in that rate's unit. On 2026-09-30 it priced H3 at 350 credits a second and 10,000 credits a dollar, which is $0.035 a second, so a run costs at most $4.20 (Reactor's billing page still says per session-minute). The meter runs from when the session is ready until it ends, and the program ends it as soon as the playlist has played: four clips of 8 seconds by default, or up to ten of your own.

Live mode needs the native addon for the machine: the platform package npm installs with `reactor-effect-native` (Linux x64 and macOS arm64), or `bun run native:build` in this repository. `REACTOR_API_URL` names another coordinator. `--reference image.png` starts every clip from an image (PNG, JPEG or WebP); an image H3 would refuse is refused before a session is allocated.

## What it shows

- **Decoded video in Node and Bun, with no browser.** `session.decoded` gives the connection's media, and `media.video("main_video")` is a stream of frames in this process's memory: BGRA or RGBA bytes, from the native peer live or from the simulation offline. The screen keeps only the newest frame (`Stream.buffer({ capacity: 1, strategy: "sliding" })`, the preview pattern `Media` documents) and draws it in upper-half blocks: each cell's colour is the pixel above and its background the pixel below.
- **One program, offline and live.** `watch` asks for `Reactor.Reactor` and `CoordinatorClient.CoordinatorClient` and nothing else. `Simulated` provides them over `ReactorTest.layer`, and `Hosted` over `FetchHttpClient` and `NativePeer.layer()`. Which one depends only on whether `REACTOR_API_KEY` is set.
- **A clip followed by its facts.** `h3.prepare` and `submission.submit` send a clip and return its acceptance; `h3.operation(submission)` then resolves `generated`, `started` and `ended` as H3 reports them. Autoplay is turned on first, since H3 plays nothing on its own, and clips are sent one after another, so H3's queue keeps the playlist's order. A clip whose build fails ends as `failed`, and the rest play on.
- **The cost bounded before anything starts.** `CoordinatorClient.modelRate` reads H3's rate from `coordinator.pricing`, and `coordinator.tokens` mints tokens with a `maxSessionDuration`, so the bound printed is the most Reactor can bill.
- **Cleanup on every exit path.** The screen and the session are scoped resources. However the show ends (the playlist done, a failure or Ctrl-C), the terminal is given back first, with the cursor shown and the last status left on the main screen, and then the session is closed and its close report says whether its termination was confirmed.

| File             | What it is                                                                           |
| ---------------- | ------------------------------------------------------------------------------------ |
| `src/main.ts`    | The program, the two layers it runs on, and the command line                         |
| `src/Screen.ts`  | The newest frame and the status on the alternate screen, or a line per step if piped |
| `src/Picture.ts` | A frame as 24-bit colour text, pure                                                  |

`bun run typecheck` compiles this workspace with the other examples; the checks never run it.

## Limits

- The picture follows the connection the session had when the show began. If the connection drops and the session reconnects, the clips and the status carry on and the picture keeps its last frame.
- Each redraw waits until the terminal has taken the one before, so a slow terminal shows fewer frames, never older ones. A 200-column picture of a 1344x768 frame is up to about 460 KiB of text a redraw.
- If Reactor ends the session before the playlist is over, at its cap or on a moderation verdict, the show ends with it: H3 then reports nothing more about any clip, so the program watches `session.changes` for a connection that will not come back, and fails with the reason.
