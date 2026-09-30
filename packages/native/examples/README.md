# Capture

`reactor-effect-native` gives server-side code the decoded media itself: BGRA frames and 16-bit PCM in JavaScript memory. This example is a command line that generates one H3 clip, follows it through its operation facts, and writes the clip's own frames and audio to an MP4.

```sh
bun install && bun run build          # from the repository root, with a staged native addon
cd packages/native/examples
REACTOR_API_KEY=… node src/capture.ts --prompt "A paper boat on a rain-soaked street" --out boat.mp4
node src/capture.ts --help
```

It prints the most the session can cost before it starts, at the rate Reactor's pricing states: the token caps the session at 90 seconds, and the bound counts each started minute whole, so two minutes. Needs `ffmpeg` on `PATH`, and the native addon for the host (`bun run native:build`, or an installed platform package). `--seconds` sets the clip's length (8 by default), `--reference image.png` starts the clip from an image, `--isolated` runs the native peer in a child process of its own (Node only), and `REACTOR_API_URL` names another coordinator.

## What it shows

- **A session from start to close in one scope.** The CLI mints a token with the key it holds, then `reactor.create` (which returns the session connected, on `CoordinatorClient.fixedTokens` since the token outlives the session), `H3.make` and `session.decoded`, all in one scope; closing it terminates the paid session. A finalizer registered as soon as the session exists prints whether termination was confirmed, however the capture ends: done, failed or interrupted.
- **A clip followed by its facts.** `provider.operation(submission)` resolves `generated`, then `started`, then `ended`. The recording keeps what arrives between the clip's start and its end. Both readers, video and audio, are started and subscribed before the clip is submitted (`Effect.forkScoped({ startImmediately: true })`), so neither misses the clip's opening.
- **Loss made visible.** `Media.recorder(stream)` turns each track into its frames plus a `Lost { after, count }` wherever the host dropped frames. The recorder fills each lost frame with the one before it and each lost audio block with silence, so the file keeps the source's timing, and reports how many it filled; `media.pressure` gives the host's own drop totals.
- **Crash containment on request.** `--isolated` swaps `NativePeer.layer()` for `NativePeer.layerIsolated()`: the same `PeerFactory`, with the native peer in a child process.
- **Effect's CLI and child processes.** Flags are declared with `effect/unstable/cli`, and ffmpeg runs through `ChildProcessSpawner`, with the video on its stdin and the audio on a third pipe.

| File                     | What it is                                                                        |
| ------------------------ | --------------------------------------------------------------------------------- |
| `src/capture.ts`         | The command: token, session, clip, recording, close                               |
| `src/Recording.ts`       | Writes a video track and an optional audio track to MP4, filling what was dropped |
| `test/Recording.test.ts` | Synthetic frames with gaps through a real ffmpeg, counted back with ffprobe       |

The recorder's test runs offline under Node and Bun in `bun run check:examples`, and skips without `ffmpeg` and `ffprobe`; the command itself is compiled, never run, by the checks.

## Limits

- The recording starts and stops on the operation's facts, which arrive on the control channel; the first and last frames of the clip are as close as those facts, not frame-exact.
- The recorder's pipe writers run detached: Effect's Node spawner leaves the child's input pipes with no error listener once their writer is interrupted, so a write still buffered when ffmpeg exits would surface as an uncaught `EPIPE`. Each writer ends its own pipe instead, and ffmpeg then exits on its own.
- Each track is read into a queue of two seconds (48 frames, 200 audio blocks); an encoder slower than real time holds up the readers, and a reader that falls behind the host's bound ends the recording early.
