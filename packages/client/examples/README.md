# Rundown: an application over a playout

`reactor-effect-client` is the portable core: sessions, the H3 provider, the `Playout` service and `ReactorTest`, a simulated Reactor, with no host of its own. This example is the part of an application that belongs there: a service that plays an ordered list of prompts on any `Playout`, and its tests, which run it against the simulated Reactor on Effect's test clock.

```sh
bun install && bun run build                     # from the repository root
node packages/client/examples/src/main.ts        # plays a short show offline, in real time
cd packages/client/examples && npx vitest run    # the tests, in about a second
```

## What it shows

- **Code written against a service, not a session.** `Rundown` asks for `Playout.Playout` and nothing else. `main.ts` provides a playout whose sessions come from `H3Source.open` over `Reactor.layer()`, with `ReactorTest.layer` beneath them in place of an HTTP client and a host. A paid deployment swaps `ReactorTest.layer` for `FetchHttpClient.layer` and a host's `PeerFactory`, and mints its tokens with its own API key; the Rundown does not change.
- **The playout owns the hard parts.** Order, build pacing, retries and never resending a request whose outcome is unknown are the playout's job. The Rundown submits each segment under its own key in one `show` lane and waits on each handle's `outcome`.
- **Outcomes as data.** Each segment ends `Played` (with its aired seconds), `Failed` (a failed build, a clip dropped or stopped before its end), `Refused` (the submission's error tag, before anything was sent) or `Unknown` (sent, but whether it aired is unknown), a `Schema` union read from the as-run status.
- **Testing without paying or waiting.** `ReactorTest` makes every delay an Effect sleep, so under `@effect/vitest`'s `layer` and `it.effect`, with `ReactorTest.flow` moving the test clock, the test runs on `ReactorTest.Timing.fixed` timing in milliseconds. A `FailBuild` fault stands in for a failed build. `main.ts` plays in real time on `ReactorTest.Timing.hosted`, the timing paid runs measured.

| File                   | What it is                                                                         |
| ---------------------- | ---------------------------------------------------------------------------------- |
| `src/Rundown.ts`       | The service; portable, it runs on Node, Bun and in browsers                        |
| `src/main.ts`          | Runs a show against the simulated Reactor with `NodeRuntime.runMain`               |
| `test/Rundown.test.ts` | Every segment played in order; a failed build reported as `Failed`, the rest aired |

`bun run test:pack` also compiles `Rundown.ts` inside clean installs of the packed archive, with and without DOM types, and runs it on the installed `ReactorTest` under Node and Bun.
