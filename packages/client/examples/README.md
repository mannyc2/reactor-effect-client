# Rundown: an application over the orchestration engine

`reactor-effect-client` is the portable core: sessions, the H3 provider, orchestration and an offline simulation, with no host of its own. This example is the part of an application that belongs there: a service that plays an ordered list of prompts on any orchestration `Engine`, and its tests, which run it against the simulation on Effect's test clock.

```sh
bun install && bun run build                     # from the repository root
node packages/client/examples/src/main.ts        # plays a short show offline, in real time
cd packages/client/examples && npx vitest run    # the tests, in about a second
```

## What it shows

- **Code written against a service, not a session.** `Rundown` asks for `Orchestration.Engine` and nothing else. `main.ts` provides the simulation (`Simulation.layerSim`); a paid deployment provides `Orchestration.layer({ open: Orchestration.openH3({ mint }) })` instead, and the Rundown does not change.
- **Gap-free observation.** `engine.observe()` subscribes before it reads the state, so no clip's start or end can fall between an enqueue and the events after it. The Rundown applies events on the same fiber that enqueues, in order.
- **Retrying only what is safe to retry.** A failure's `isRetryable` is true for backpressure (`QueueFull`, `SessionRecovering`, a local `Overflow`), a connection lost before sending, and a coordinator refusal that asks for a retry, and never when `context.outcome` is `unknown`. The Rundown retries the first with a `Schedule` (doubling from 250 ms, capped at 5 seconds with `Schedule.min`) and reports the second as `Unknown`: that clip may still play, so it is never resent. Events wait in the observation while an enqueue retries, so the Rundown observes with a larger buffer; an observation that still overflows fails the play.
- **Outcomes as data.** Each segment ends `Played`, `Failed`, `Refused` (with its reason's tag) or `Unknown`, a `Schema` union.
- **Testing without paying or waiting.** The simulation reads Effect's `Clock`, so under `@effect/vitest`'s `it.effect` the test clock drives it: five minutes of programme take milliseconds. Its faults stand in for a failed build (`buildFails`) and a session lost after an enqueue was sent (`sessionFails`).

| File                   | What it is                                                           |
| ---------------------- | -------------------------------------------------------------------- |
| `src/Rundown.ts`       | The service; portable, it runs on Node, Bun and in browsers          |
| `src/main.ts`          | Runs a show against the simulation with `NodeRuntime.runMain`        |
| `test/Rundown.test.ts` | In order; a full generation queue; a failed clip; an unknown outcome |

`bun run test:pack` also compiles `Rundown.ts` inside clean installs of the packed archive, with and without DOM types, and runs it on the simulation under Node and Bun.
