# reactor-effect-client

The portable core of [reactor-effect](https://github.com/mannyc2/reactor-effect-client), an
[Effect](https://effect.website) SDK for [Reactor](https://reactor.inc)'s real-time video models.
It opens and owns Reactor sessions, drives the H3 model, keeps a channel on air across session caps
with `Playout`, and simulates Reactor in memory with `ReactorTest`, so an application runs offline
before it spends anything. It loads no native code and runs on Node, Bun and browsers; a transport
comes from [`reactor-effect-browser`](https://www.npmjs.com/package/reactor-effect-browser) or
[`reactor-effect-native`](https://www.npmjs.com/package/reactor-effect-native).

**[Documentation](https://mannyc2.github.io/reactor-effect-client/)** ·
[Quickstart](https://mannyc2.github.io/reactor-effect-client/start/quickstart/) ·
[Examples](https://github.com/mannyc2/reactor-effect-client/tree/main/examples) ·
[llms-full.txt](https://mannyc2.github.io/reactor-effect-client/llms-full.txt)

This is an independent project, not an official Reactor SDK. Protocol material is attributed in
[NOTICE](./NOTICE) and [`notices/`](./notices/).

## Install

```sh
npm install --save-exact reactor-effect-client effect@4.0.0-rc.117
```

Effect is a peer dependency at exactly `4.0.0-rc.117`: its release candidates can move modules, so a
later one needs a new SDK release. Install any `@effect/*` package at the same version with
`--save-exact`. A project with `@effect/platform-node` also pins `@effect/platform-node-shared` to
`4.0.0-rc.117` with an override, since the platform's caret range otherwise installs a later
candidate: always under Bun and pnpm, and under npm unless `reactor-effect-native` is installed.
[Installation](https://mannyc2.github.io/reactor-effect-client/start/installation/) covers each
host.

## A first clip, offline

```ts
import * as NodeRuntime from "@effect/platform-node/NodeRuntime";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { Console, Effect, Layer, Redacted } from "effect";
import { CoordinatorClient, H3, Reactor, ReactorTest } from "reactor-effect-client";

const firstClip = Effect.gen(function* () {
  const coordinator = yield* CoordinatorClient.CoordinatorClient;
  const reactor = yield* Reactor.Reactor;
  const session = yield* reactor.create({
    model: H3.modelName,
    tokens: coordinator.tokens({ modelName: H3.modelName, maxSessionDuration: "2 minutes" }),
  });
  const h3 = yield* H3.make(session);
  yield* h3.setAutoplay(true);
  const submission = yield* h3.prepare({ prompt: "A paper boat on a rainy street", seconds: 5 });
  yield* submission.submit;
  const clip = yield* h3.operation(submission);
  yield* clip.ended;
  const report = yield* session.close;
  yield* Console.log(`termination confirmed: ${report.remote.confirmed}`);
}).pipe(Effect.scoped);

// Reactor simulated in memory at the timing paid runs measured: no key, nothing billed.
const Simulated = Reactor.layer().pipe(
  Layer.provideMerge(CoordinatorClient.layer({ apiKey: Redacted.make("demo") })),
  Layer.provideMerge(ReactorTest.layer({ timing: ReactorTest.Timing.hosted, apiKey: "demo" })),
);

firstClip.pipe(
  // The program's entry point, the one place a layer is provided.
  // @effect-diagnostics-next-line strictEffectProvide:off
  Effect.provide(Layer.mergeAll(Simulated, NodeServices.layer)),
  NodeRuntime.runMain,
);
```

For hosted H3, provide `Reactor.layer()` with `CoordinatorClient.layerConfig` (the API key from
`REACTOR_API_KEY`), a host's peer layer and an `HttpClient` instead; the program does not change.
[Going live](https://mannyc2.github.io/reactor-effect-client/start/going-live/) walks through it.

## Modules

Each module is its own subpath, `reactor-effect-client/<Module>`, and the root exports every one as
a namespace. Nothing under `internal/` is reachable.

| Module                                                                                    | What it is                                                                                         |
| ----------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------- |
| [`Reactor`](https://mannyc2.github.io/reactor-effect-client/concepts/sessions/)           | The service that acquires sessions: `create` allocates one this process owns, `attach` joins one   |
| [`Session`](https://mannyc2.github.io/reactor-effect-client/concepts/sessions/)           | One session: status, events, commands, uploads, recordings, media, reconnects and its close report |
| [`CoordinatorClient`](https://mannyc2.github.io/reactor-effect-client/concepts/sessions/) | Reactor's HTTP API: pricing, tokens (`tokens`, `fixedTokens`), inspection, termination, recordings |
| [`H3`](https://mannyc2.github.io/reactor-effect-client/concepts/h3/)                      | The H3 provider over a session: its state and queue, commands, acceptance and reference validation |
| [`Playout`](https://mannyc2.github.io/reactor-effect-client/concepts/playout/)            | Airs keyed items in priority lanes across sessions it renews, and reports what aired               |
| [`H3Source`](https://mannyc2.github.io/reactor-effect-client/concepts/sources/)           | A paid H3 session as a playout source: `open`, `resume` and the owner record `Allocation`          |
| [`LocalSource`](https://mannyc2.github.io/reactor-effect-client/concepts/sources/)        | A playout source rendered in this process by the application's hooks                               |
| [`Media`](https://mannyc2.github.io/reactor-effect-client/concepts/media/)                | Decoded frames and platform tracks of one connection generation, and `recorder`                    |
| [`Peer`](https://mannyc2.github.io/reactor-effect-client/reference/modules/)              | The transport port a host implements, and the `PeerFactory` service                                |
| [`ReactorError`](https://mannyc2.github.io/reactor-effect-client/concepts/errors/)        | Every failure the client raises, its tagged reason and its dispatch outcome                        |
| [`ReactorTest`](https://mannyc2.github.io/reactor-effect-client/guides/testing-offline/)  | Reactor simulated in memory: the coordinator, peers and an H3 model, driven by the Effect clock    |

Each module's doc comments state its options' defaults and bounds.

## What to know first

- **A session belongs to a scope.** Closing the scope closes the session; a session this process
  created is terminated and the end is confirmed with an independent read. `Session.mayStillBill`
  says whether a close report leaves anything that may still bill.
- **A session never sees the API key.** It runs on tokens minted for it and refreshes them before
  they expire. A creating token must state its session's cap with `maxSessionDuration`.
- **A dropped connection reconnects on the same session**, as a new connection generation that
  allocates nothing and replays no command.
- **Every failure is tagged** with a `reason`, and a failed command carries its dispatch outcome:
  `not-submitted`, `replied` or `unknown`. Nothing resends an `unknown` command.
- **H3 plays nothing until told**: call `setAutoplay(true)` or `play`, or let `Playout` drive it.
- **`Playout` keeps H3 on air**: it renews sessions before their cap and switches at a clip
  boundary, with lanes, filler, windows, cues, edits and placement. In paid runs of the 0.8.0
  library on hosted H3 (2026-09-28 and 09-29), seams measured 46–169 ms with no dark frame and a
  planned switch 420–432 ms. 0.9.0 has not run on hosted Reactor yet.
- **`ReactorTest` runs the same application offline**, at hosted timing, or on `TestClock` where
  an hour of programme with six renewals takes about 20 seconds, with faults to inject.
- **A provider `Started` fact is not proof** that a frame was presented or encoded.

## Example

[`examples/`](https://github.com/mannyc2/reactor-effect-client/tree/main/packages/client/examples)
is an application service written against `Playout`, run offline on `ReactorTest` and tested on
Effect's test clock. The repository's [other examples](https://github.com/mannyc2/reactor-effect-client/tree/main/examples)
include a terminal viewer and a 24/7 channel broadcast to many browsers.

## Development

This package is built and tested from the workspace root; see the repository's
[CONTRIBUTING](https://github.com/mannyc2/reactor-effect-client/blob/main/CONTRIBUTING.md). Inside
`packages/client`, `bun run test` runs the portable suite and `bun run typecheck` checks the source
closure with browser types and with Node types. The wire codec is generated from the protocol
sources in [`wire/`](./wire/README.md).

## License

Apache-2.0. Third-party and derived-source notices are retained in [NOTICE](./NOTICE) and
[`notices/`](./notices/).
