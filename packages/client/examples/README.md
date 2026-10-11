# Portable production examples

Two applications over the portable core: a rundown that schedules video clips on `Playout`, and
streaming speech that consumes `SpeechModel`. Their entry points select providers through Effect
Layers. The rundown runs offline on `ReactorTest`; the speech example calls Cartesia.

```sh
bun install && bun run build                     # from the repository root
node packages/client/examples/src/main.ts        # plays a short show offline, in real time
cd packages/client/examples && npx vitest run    # the tests, in about a second
```

## Rundown

- **Code written against a service, not a session.** `Rundown` asks for the application-owned `Channel` service. `main.ts` constructs it with `Playout.make`, `H3Source.model` and `H3Source.evidence`; its sessions come from `H3Source.open` over `Reactor.layer()`, with `ReactorTest.layer` beneath them in place of an HTTP client and a host. A paid deployment swaps `ReactorTest.layer` for `FetchHttpClient.layer` and a host's `PeerFactory`, and mints its tokens with its own API key; the Rundown does not change.
- **The playout owns the hard parts.** Order, build pacing, retries and never resending a request whose outcome is unknown are the playout's job. The Rundown submits each segment under its own key in one `show` lane and waits on each handle's `outcome`.
- **Outcomes as data.** Each segment ends `Played` (with its aired seconds), `Failed` (a failed build, a clip dropped or stopped before its end), `Refused` (the submission's error tag, before anything was sent) or `Unknown` (sent, but whether it aired is unknown), a `Schema` union read from the as-run status.
- **Testing without paying or waiting.** `ReactorTest` makes every delay an Effect sleep, so under `@effect/vitest`'s `layer` and `it.effect`, with `ReactorTest.flow` moving the test clock, the test runs on `ReactorTest.Timing.fixed` timing in milliseconds. A `FailBuild` fault stands in for a failed build. `main.ts` plays in real time on `ReactorTest.Timing.hosted`, the timing paid runs measured.

| File                   | What it is                                                                         |
| ---------------------- | ---------------------------------------------------------------------------------- |
| `src/Channel.ts`       | The application-owned playout service tag                                          |
| `src/Rundown.ts`       | The service; portable, it runs on Node, Bun and in browsers                        |
| `src/main.ts`          | Runs a show against the simulated Reactor with `NodeRuntime.runMain`               |
| `test/Rundown.test.ts` | Every segment played in order; a failed build reported as `Failed`, the rest aired |

`bun run test:pack` also compiles `Rundown.ts` inside clean installs of the packed archive, with and without DOM types, and runs it on the installed `ReactorTest` under Node and Bun.

## Streaming speech

`src/speech.ts` consumes `SpeechModel`, while the entry point selects `CartesiaSpeech` through
Effect's native model Layer. It needs an Effect HTTP client, with no Reactor session or native
addon. It writes each audio block to disk as it arrives and reports time to first audio.

From the repository root, after installing and building:

```sh
# Set CARTESIA_API_KEY, CARTESIA_MODEL and CARTESIA_VOICE_ID in your environment.
node packages/client/examples/src/speech.ts
# Listen to the completed raw PCM file:
ffplay -f s16le -ar 24000 -ac 1 .check/speech.pcm
```

Running the example calls Cartesia and can incur charges. Model and voice have no defaults.
`SPEECH_TEXT` changes the spoken sentence; `SPEECH_OUTPUT` changes the output path from
`.check/speech.pcm`. A failed or interrupted run leaves a `.partial` file; only a completed response
is renamed to the requested path. The entire stream has a two-minute client deadline.

The adapter uses the [Cartesia bytes API](https://docs.cartesia.ai/api-reference/tts/bytes), pinned
to `2026-08-14`, with a complete transcript and streamed mono 24 kHz PCM output. It does not pace
playback, mix tracks, or put an unknown-duration utterance into the clip scheduler. EOF is response
completion, without independent evidence that the whole utterance was generated or played.
Hosted generation quality and latency have not been qualified.
