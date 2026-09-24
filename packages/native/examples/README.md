# Capture

`reactor-effect-native` gives server-side code the decoded media itself: BGRA frames and 16-bit PCM in JavaScript memory. This example is a command line that generates one H3 clip, follows it through its operation facts, and writes the clip's own frames and audio to an MP4.

```sh
bun install && bun run build          # from the repository root, with a staged native library
cd packages/native/examples
REACTOR_API_KEY=… node src/capture.ts --prompt "A paper boat on a rain-soaked street" --out boat.mp4
node src/capture.ts --help
```

It prints the most the session can cost before it starts: the token caps the session at 90 seconds, and Reactor bills by the minute, so the bound counts two whole minutes. Needs `ffmpeg` on `PATH`, and a native library for the host (`bun run native:build`, or an installed package's staged one). `--reference image.png` starts the clip from an image; `--isolated` runs the native peer in a child process of its own (Node only).

## What it shows

- **A session from start to close in one scope.** The CLI mints a token with the key it holds, then `createConnected`, `H3.make` and `Native.media`, all in one scope; closing it terminates the paid session. A finalizer registered as soon as the session exists prints whether termination was confirmed, however the capture ends: done, failed or interrupted.
- **A clip followed by its facts.** `provider.operation(submission)` resolves `generated`, then `started`, then `ended`. The recording keeps what arrives between the clip's start and its end. Both readers, video and audio, are started and subscribed before the clip is submitted (`Effect.forkScoped({ startImmediately: true })`), so neither misses the clip's opening.
- **Loss made visible.** `recorder(stream)` turns each track into its frames plus a `Lost { after, count }` wherever the host dropped frames. The recorder fills each lost frame with the one before it and each lost audio block with silence, so the file keeps the source's timing, and reports how many it filled; `media.snapshot` gives the host's own totals.
- **Crash containment on request.** `--isolated` swaps `Native.layer()` for `Native.Isolated.layer()`: the same `PeerFactory`, with the native peer in a child process.
- **Effect's CLI and child processes.** Flags are declared with `effect/unstable/cli`, and ffmpeg runs through `ChildProcessSpawner`, with the video on its stdin and the audio on a third pipe.

| File                     | What it is                                                                        |
| ------------------------ | --------------------------------------------------------------------------------- |
| `src/capture.ts`         | The command: token, session, clip, recording, close                               |
| `src/Recording.ts`       | Writes a video track and an optional audio track to MP4, filling what was dropped |
| `test/Recording.test.ts` | Synthetic frames with gaps through a real ffmpeg, counted back with ffprobe       |

The recorder's test runs offline under Node and Bun in `bun run check:examples`; the command itself is compiled, never run, by the checks.

## Limits

- The recording starts and stops on the operation's facts, which arrive on the control channel; the first and last frames of the clip are as close as those facts, not frame-exact.
- The recorder's writers are detached for the same Effect 4.0.0-rc.115 spawner issue the live channel describes, and it takes the first frame from a queue because `Stream.peel` discards its sink's leftovers in that release.
- An encoder slower than real time holds up the readers; a reader that falls behind the host's bound ends the recording early.
