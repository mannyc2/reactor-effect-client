# reactor-effect-client

The portable core of [reactor-effect](https://github.com/mannyc2/reactor-effect-client), an
[Effect](https://effect.website) SDK for [Reactor](https://reactor.inc)'s real-time video models. It
opens and owns Reactor sessions, drives H3, FastH3 and Vidu S2-Avatar's talking character, keeps a
channel on air across session caps with `Playout`, and simulates Reactor in memory with
`ReactorTest`, so an application runs offline before it spends anything. It loads no native code and
runs on Node, Bun and browsers; a transport comes from
[`reactor-effect-browser`](https://www.npmjs.com/package/reactor-effect-browser) or
[`reactor-effect-native`](https://www.npmjs.com/package/reactor-effect-native).

**[Documentation](https://mannyc2.github.io/reactor-effect-client/)** ·
[Quickstart](https://mannyc2.github.io/reactor-effect-client/start/quickstart/) ·
[Examples](https://github.com/mannyc2/reactor-effect-client/tree/main/examples) ·
[llms-full.txt](https://mannyc2.github.io/reactor-effect-client/llms-full.txt)

This is an independent project, not an official Reactor SDK. Protocol material is attributed in
[NOTICE](./NOTICE) and [`notices/`](./notices/).

## Install

```sh
npm install --save-exact reactor-effect-client effect@4.0.0
```

Effect is a peer dependency at `~4.0.0`, any 4.0.x patch: the Effect modules the SDK builds on,
such as `effect/http` and `effect/rpc`, are marked unstable, and Effect may change those in a minor
release, so Effect 4.1 needs a new SDK release. Install any `@effect/*` package at the same version
as `effect`, with `--save-exact`. A project with `@effect/platform-node` also pins
`@effect/platform-node-shared` to that version with an override, since the platform's caret range
otherwise installs a later release.
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

## A talking character

`ViduS2Avatar` drives Vidu S2-Avatar (`reactor/vidu-s2-avatar`), which turns one photo of a person
into a character that talks with a caller in a live call:

```ts
const avatar = yield * ViduS2Avatar.make(session);
yield * avatar.createAvatar({ bytes, type: "image/jpeg" });
// It returns once the call is live, with the character's picture and voice resumed.
yield * avatar.startCall({ persona: "You are Tina, a museum guide.", greeting: "Say hello." });
yield * avatar.say("What lives in the deepest tank?");
const ended = yield * avatar.endCall;
```

A refused command fails with the `Refused` reason, carrying the model's own `code`. On Node and
Bun the caller speaks only through `say`: the native host can't publish a microphone track; a
browser publishes one on `mic`. Of two paid runs of the provider on hosted Reactor, one passed
every criterion; in the other, the native connection failed with a `Protocol` failure 21 ms after
the provider's first command, for a cause not yet known.
[A talking character](https://mannyc2.github.io/reactor-effect-client/guides/avatar/) and the
[avatar example](https://github.com/mannyc2/reactor-effect-client/tree/main/examples/avatar) cover
the rest.

## Modules

Each module is its own subpath, `reactor-effect-client/<Module>`, and the root exports every one as
a namespace. Nothing under `internal/` is reachable.

| Module                                                                                          | What it is                                                                                                                   |
| ----------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------- |
| [`Reactor`](https://mannyc2.github.io/reactor-effect-client/concepts/sessions/)                 | The service that acquires sessions: `create` allocates one this process owns, `attach` joins one                             |
| [`Session`](https://mannyc2.github.io/reactor-effect-client/concepts/sessions/)                 | One session: status, events, commands, uploads, recordings, media, reconnects and its close report                           |
| [`CoordinatorClient`](https://mannyc2.github.io/reactor-effect-client/concepts/sessions/)       | Reactor's HTTP API: pricing, tokens (`tokens`, `fixedTokens`), inspection, termination, recordings                           |
| [`H3`](https://mannyc2.github.io/reactor-effect-client/concepts/h3/)                            | The H3 provider over a session: its state and queue, commands, acceptance and reference validation                           |
| [`FastH3`](https://mannyc2.github.io/reactor-effect-client/concepts/h3/#fasth3)                 | The FastH3 provider over a session: frames or clip ids as opener and closer, commands and acceptance                         |
| [`ViduS2Avatar`](https://mannyc2.github.io/reactor-effect-client/guides/avatar/)                | The Vidu S2-Avatar provider over a session: an avatar from a photo, its calls and transcripts                                |
| [`References`](https://mannyc2.github.io/reactor-effect-client/concepts/h3/#loading-references) | Reference images and voice samples from a data URI, a file or a URL: bounded, validated, the location never logged           |
| [`Playout`](https://mannyc2.github.io/reactor-effect-client/concepts/playout/)                  | Airs keyed items in priority lanes across sessions it renews, and reports what aired                                         |
| [`H3Source`](https://mannyc2.github.io/reactor-effect-client/concepts/sources/)                 | A paid H3 session as a playout source: `open`, `resume`, `opener` and the owner record `Allocation`                          |
| [`FastH3Source`](https://mannyc2.github.io/reactor-effect-client/guides/fasth3/)                | A paid FastH3 session as a playout source: `open`, `resume`, `opener` and `model`; not yet qualified on hosted Reactor       |
| [`LocalSource`](https://mannyc2.github.io/reactor-effect-client/concepts/sources/)              | A playout source rendered in this process by the application's hooks                                                         |
| [`Ledger`](https://mannyc2.github.io/reactor-effect-client/guides/cost-control/)                | The paid sessions an application owns, in a store it provides: recorded before they connect, ended until Reactor confirms it |
| [`Media`](https://mannyc2.github.io/reactor-effect-client/concepts/media/)                      | Decoded frames and platform tracks of one connection generation, and `recorder`                                              |
| [`Peer`](https://mannyc2.github.io/reactor-effect-client/reference/modules/)                    | The transport port a host implements, and the `PeerFactory` service                                                          |
| [`ReactorError`](https://mannyc2.github.io/reactor-effect-client/concepts/errors/)              | Every failure the client raises, its tagged reason and its dispatch outcome                                                  |
| [`ReactorTest`](https://mannyc2.github.io/reactor-effect-client/guides/testing-offline/)        | Reactor simulated in memory: the coordinator, peers, H3, FastH3 and Vidu S2-Avatar, on the Effect clock                      |

Each module's doc comments state its options' defaults and bounds.

## What to know first

- **A session belongs to a scope.** Closing the scope closes the session; a session this process
  created is terminated and the end is confirmed with an independent read. `Session.mayStillBill`
  says whether a close report leaves anything that may still bill.
- **A `Ledger` records every paid session before it connects**, ends or resumes what a crashed
  process left before it allocates more, and ends each session until Reactor confirms it:
  `ledger.source(H3Source.opener({ tokens }))` is a playout's `open`.
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
  planned switch 420–432 ms. On 0.9.0 only the one-session `showreel` check has run there (seams of
  89–120 ms, 2026-10-01); its renewal and placement have run only on `ReactorTest`.
- **`ReactorTest` runs the same application offline**, at hosted timing, or on `TestClock` where
  an hour of programme with six renewals takes about 20 seconds, with faults to inject.
- **A provider `Started` fact is not proof** that a frame was presented or encoded.
- **Every operation a caller can cancel is traced** through Effect's `Tracer`, and queued playout work
  stays under the span that submitted it. Spans name identity and outcome, never credentials, inputs
  or provider text. [Tracing](https://mannyc2.github.io/reactor-effect-client/reference/tracing/)
  lists them.

## Example

[`examples/`](https://github.com/mannyc2/reactor-effect-client/tree/main/packages/client/examples)
is an application service written against `Playout`, run offline on `ReactorTest` and tested on
Effect's test clock. The repository's [other examples](https://github.com/mannyc2/reactor-effect-client/tree/main/examples)
include a terminal viewer, a call with a Vidu S2-Avatar character and a 24/7 channel broadcast to
many browsers.

## Development

This package is built and tested from the workspace root; see the repository's
[CONTRIBUTING](https://github.com/mannyc2/reactor-effect-client/blob/main/CONTRIBUTING.md). Inside
`packages/client`, `bun run test` runs the portable suite and `bun run typecheck` checks the source
closure with browser types and with Node types. The wire codec is generated from the protocol
sources in [`wire/`](./wire/README.md).

## License

Apache-2.0. Third-party and derived-source notices are retained in [NOTICE](./NOTICE) and
[`notices/`](./notices/).
