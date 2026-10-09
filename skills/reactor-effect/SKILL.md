---
name: reactor-effect
description: Build Reactor real-time video applications with reactor-effect, the Effect (effect-ts v4) SDK. Use when code imports `reactor-effect-client`, `reactor-effect-browser` or `reactor-effect-native`; when writing Effect code that opens Reactor sessions, drives H3, FastH3 or a Vidu S2-Avatar character, keeps a channel on air with `Playout`, reads decoded frames in Node or Bun, or tests against `ReactorTest`. SKIP for Reactor's official `@reactor-team/js-sdk` or Python `reactor-sdk` without Effect.
---

# reactor-effect

reactor-effect is an independent Effect SDK for [Reactor](https://reactor.inc)'s real-time video
models. A session is a scoped Effect resource; H3, FastH3 and Vidu S2-Avatar are providers over it;
`Playout` keeps H3 on air across sessions; `ReactorTest` is Reactor simulated in memory for offline runs and tests.

**Packages** (one version for all, published to npm):

- `reactor-effect-client`: `Reactor`, `Session`, `CoordinatorClient`, `H3`, `FastH3`, `ViduS2Avatar`,
  `References`, `H3Source`, `FastH3Source`, `LocalSource`, `Ledger`, `Playout`, `Media`, `Peer`, `ReactorError`, `ReactorTest`. Portable: Node, Bun and
  browsers.
- `reactor-effect-browser`: `BrowserPeer.layer` on `RTCPeerConnection`, `BrowserMedia` for tracks.
- `reactor-effect-native`: `NativePeer.layer()`, or `NativePeer.layerIsolated()` under Node, on a
  Node-API addon over Reactor's `reactor-webrtc` crate (libwebrtc), prebuilt for linux-x64-gnu and
  darwin-arm64: decoded BGRA frames and PCM.

**Full documentation for agents:** https://mannyc2.github.io/reactor-effect-client/llms-full.txt

## Before writing code

1. Effect is a peer at `~4.0.0`: any 4.0.x patch, never 4.1. Install `effect` and every
   `@effect/*` package with `--save-exact` at one 4.0.x release, such as `4.0.0`. With
   `@effect/platform-node`, also pin `@effect/platform-node-shared` to that release with an
   override (`"overrides"` for npm and Bun, `"pnpm": { "overrides" }` for pnpm). Set it before the
   first install.
2. Read Effect's own guide in the installed package: `node_modules/effect/AGENTS.md` (with Bun's
   isolated linker it sits under the package that depends on Effect). Effect 4 APIs differ from
   Effect 3: `Context.Service`, `Schema.TaggedError`, `Effect.fn`, `effect/*` modules.
3. Import modules by subpath (`reactor-effect-client/Playout`) or as namespaces from the root
   (`import { H3, Playout, Reactor } from "reactor-effect-client"`).
4. The coordinator client is `CoordinatorClient`; there is no `Coordinator` module.

## The layers

The application names no host; the entry point provides one of these once.

```ts
// Offline: Reactor simulated in memory at hosted timing. No key, nothing billed.
const Simulated = Reactor.layer().pipe(
  Layer.provideMerge(CoordinatorClient.layer({ apiKey: Redacted.make("demo") })),
  Layer.provideMerge(ReactorTest.layer({ timing: ReactorTest.Timing.hosted, apiKey: "demo" })),
);

// Hosted, on a server: REACTOR_API_KEY, frames decoded in this process.
const Hosted = Reactor.layer().pipe(
  Layer.provideMerge(Layer.mergeAll(CoordinatorClient.layerConfig, NativePeer.layer())),
  Layer.provide(FetchHttpClient.layer),
);
```

In a browser, use `BrowserPeer.layer` instead of `NativePeer.layer()`, and `CoordinatorClient.layer`
without an API key: the page gets session tokens from its own server, which mints them with
`coordinator.tokens({ apiKey, ... })`. Never ship the API key (`rk_...`) to a browser. `H3.make` needs
`Crypto`: `NodeServices.layer` on Node, and in browsers `BrowserCrypto.layer` from
`@effect/platform-browser` or a small layer of your own over Web Crypto.

## Core workflow

```ts
const program = Effect.gen(function* () {
  const coordinator = yield* CoordinatorClient.CoordinatorClient;
  const reactor = yield* Reactor.Reactor;
  const session = yield* reactor.create({
    model: H3.modelName,
    tokens: coordinator.tokens({ modelName: H3.modelName, maxSessionDuration: "2 minutes" }),
  });
  const h3 = yield* H3.make(session);
  yield* h3.setAutoplay(true);
  const submission = yield* h3.prepare({ prompt: "A lighthouse in a storm", seconds: 8 });
  yield* submission.submit;
  const clip = yield* h3.operation(submission);
  yield* clip.reached("started");
  yield* clip.ended;
  const report = yield* session.close;
}).pipe(Effect.scoped);
```

## Critical gotchas

- **H3 plays nothing on its own.** Call `setAutoplay(true)` or `play`, or let `Playout` drive it.
- **Every creating token must state its session's cap**: `maxSessionDuration` is required (whole
  seconds up to a day, or `"unlimited"`, never a default). Reactor's pricing API states H3's rate
  per second ($0.035/s on 2026-09-30; its billing page still says per session-minute), from `ready`
  until termination, idle time included.
- **A session belongs to a `Scope`.** Closing the scope terminates it. `session.close` returns a
  report; `Session.mayStillBill(report)` is true when termination was not confirmed.
- **A crash leaves its paid sessions billing until their caps** unless something ends them. Open
  them through a `Ledger` (`Ledger.layerFile(path)` over a `CoordinatorClient` holding the API key):
  it records each session before it connects, ends or resumes what a crashed process left before
  allocating more, and ends each session until Reactor confirms it. One store per process and
  account.
- **Load reference images and voice samples with `References`**, not a reader of your own:
  `References.image` and `References.audio` take a base64 `data:` URI, a `file://` URI or absolute
  path, or an http(s) URL, or `References.file(path)` for a relative path; they read at most 16 MiB
  (a session's upload limit, below H3's 25 MiB) within 10 s and validate as H3 does. Load them
  before allocating a session. Errors and spans never name the location.
- **Never resend a command whose failure's `context.outcome` is `"unknown"`**: it may have reached
  Reactor. `"not-submitted"` is safe to retry. The SDK itself never resends an unknown enqueue.
- **Errors are tagged**: handle them with `Effect.catchTag` and `Effect.catchReason` on the tagged
  `reason`. Provider text is `Redacted`; do not log it.
- **Decoded frames are shared and read-only.** Copy before changing them. A reader that falls behind
  fails with `Overflow`; for a preview keep the newest frame with
  `Stream.buffer({ capacity: 1, strategy: "sliding" })`. `Media.recorder` marks dropped runs as
  `Lost`.
- **A provider `Started` fact is not proof that a frame was presented or encoded.** Check media.
- **In Effect code, no `Date.now()`, `setTimeout` or `Math.random()`**: use `Clock`, `Effect.sleep`
  and `Random`, so `TestClock` drives them.

## Keeping a channel on air: `Playout`

For FastH3, use `Playout.make({ model: FastH3Source.model, open: ledger.source(FastH3Source.opener({ tokens })) })` with an application-owned `Playout.Service<FastH3.Request>` tag. Automatic continuations use only built, retained clips; hosted qualification is pending.

`Playout.layer({ open, lanes, filler, renewal })` where `open` is
`ledger.source(H3Source.opener({ tokens: coordinator.tokens({ ... }) }))`, with `ledger` the
`Ledger` service. Submit keyed items with
`playout.submit({ key: Playout.ItemKey.make("..."), lane, request: { prompt, seconds } })`; the
handle's `started` and `outcome` report what aired (`Ended`, `Dropped`, `Failed`, `Unobserved`, or a
terminal `Unknown`, which is never replayed). `renewal: { lead }` opens the next session `lead`
before the cap and switches at a clip boundary. Read
https://mannyc2.github.io/reactor-effect-client/concepts/playout/ before designing a schedule.

## A talking character: `ViduS2Avatar`

Vidu S2-Avatar (`ViduS2Avatar.modelName`, `reactor/vidu-s2-avatar`) makes a character from one photo
that talks with a caller in a live call. `ViduS2Avatar.make(session)`, then
`createAvatar({ bytes, type })` or `{ url }`, `startCall({ persona, greeting? })` (returns once live,
with the character's `main_video`/`main_audio` resumed), `say(text)`, `interrupt`, `updateCall`,
`endCall`. Commands go out one at a time; a refusal fails with reason `Refused` and the model's
`code`. Reactor bills the whole session, time between calls included. On Node and Bun the caller
speaks only through `say` (the native host can't publish a microphone); a browser publishes `mic`
with `BrowserMedia.tracks`. Its hosted runs used the raw `Session`; the provider has run on
`ReactorTest` only. Guide: https://mannyc2.github.io/reactor-effect-client/guides/avatar/

## Testing

Use `@effect/vitest` with the simulated layer and `TestClock`. Fork `ReactorTest.flow("20 millis")`
into the test's scope to keep virtual time moving, and assert on as-run outcomes. Faults, given to
`ReactorTest.layer({ faults })` or armed later with the `ReactorTest` service's `inject`, include
`FailBuild`, `DropReply`, `Disconnect`, `Moderate`, `Expire` and 18 more. Guide:
https://mannyc2.github.io/reactor-effect-client/guides/testing-offline/

## Examples to copy from

https://github.com/mannyc2/reactor-effect-client/tree/main/examples: `quickstart` (one clip,
offline or live), `terminal` (frames drawn in a terminal), `avatar` (a Vidu S2-Avatar call),
`livestream` (a 24/7 channel for many viewers), and the package examples:
`packages/browser/examples` (a page with its own session), `packages/native/examples` (an MP4
capture), `packages/client/examples` (an offline-tested service).
