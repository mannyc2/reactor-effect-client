---
name: reactor-effect
description: Build Reactor real-time video applications with reactor-effect, the Effect (effect-ts v4) SDK. Use when code imports `reactor-effect-client`, `reactor-effect-browser` or `reactor-effect-native`; when writing Effect code that opens Reactor sessions, drives the H3 model, keeps a channel on air with `Playout`, reads decoded frames in Node or Bun, or tests against `ReactorTest`. SKIP for Reactor's official `@reactor-team/js-sdk` or Python `reactor-sdk` without Effect.
---

# reactor-effect

reactor-effect is an independent Effect SDK for [Reactor](https://reactor.inc)'s real-time video
models. A session is a scoped Effect resource; the H3 model is a provider over it; `Playout` keeps
H3 on air across sessions; `ReactorTest` is Reactor simulated in memory for offline runs and tests.

**Packages** (one version for all, published to npm):

- `reactor-effect-client`: `Reactor`, `Session`, `CoordinatorClient`, `H3`, `H3Source`,
  `LocalSource`, `Playout`, `Media`, `Peer`, `ReactorError`, `ReactorTest`. Portable: Node, Bun and
  browsers.
- `reactor-effect-browser`: `BrowserPeer.layer` on `RTCPeerConnection`, `BrowserMedia` for tracks.
- `reactor-effect-native`: `NativePeer.layer()` / `NativePeer.layerIsolated()` on a libwebrtc
  Node-API addon, prebuilt for linux-x64-gnu and darwin-arm64: decoded BGRA frames and PCM.

**Full documentation for agents:** https://mannyc2.github.io/reactor-effect-client/llms-full.txt

## Before writing code

1. Effect is pinned **exactly** to `4.0.0-rc.117`, as a peer. Install every Effect package with
   `--save-exact` at that version. With `@effect/platform-node`, also pin
   `@effect/platform-node-shared` to `4.0.0-rc.117` with an override (`"overrides"` for npm and Bun,
   `"pnpm": { "overrides" }` for pnpm): always under Bun and pnpm, and under npm unless
   `reactor-effect-native` is installed. Set it before the first install.
2. Read Effect's own guide in the installed package: `node_modules/effect/AGENTS.md` (with Bun's
   isolated linker it sits under the package that depends on Effect). Effect 4 APIs differ from
   Effect 3: `Context.Service`, `Schema.TaggedError`, `Effect.fn`, `effect/unstable/*` modules.
3. Import modules by subpath (`reactor-effect-client/Playout`) or as namespaces from the root
   (`import { H3, Playout, Reactor } from "reactor-effect-client"`).
4. The coordinator client is `CoordinatorClient` from 0.9.0; in 0.8.x it was `Coordinator`.

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
`Crypto`: `NodeServices.layer` on Node, a Web Crypto layer in browsers.

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
- **Every creating token must cap its session**: `maxSessionDuration` is required (whole seconds up
  to a day, or `"unlimited"`). H3 bills per second from `ready` until termination ($0.035/s on
  2026-09-30), idle time included.
- **A session belongs to a `Scope`.** Closing the scope terminates it. `session.close` returns a
  report; `Session.mayStillBill(report)` is true when termination was not confirmed.
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

`Playout.layer({ open, lanes, filler, renewal })` where `open` is
`H3Source.open({ tokens: coordinator.tokens({ ... }) })`. Submit keyed items with
`playout.submit({ key: Playout.ItemKey.make("..."), lane, request: { prompt, seconds } })`; the
handle's `started` and `outcome` report what aired (`Ended`, `Dropped`, `Failed`, `Unobserved`, or a
terminal `Unknown`, which is never replayed). `renewal: { lead }` opens the next session `lead`
before the cap and switches at a clip boundary. Read
https://mannyc2.github.io/reactor-effect-client/concepts/playout/ before designing a schedule.

## Testing

Use `@effect/vitest` with the simulated layer and `TestClock`. Fork `ReactorTest.flow("20 millis")`
into the test's scope to keep virtual time moving, and assert on as-run outcomes. Faults, given to
`ReactorTest.layer({ faults })` or armed later with the `ReactorTest` service's `inject`, include
`FailBuild`, `DropReply`, `Disconnect`, `Moderate`, `Expire` and 18 more. Guide:
https://mannyc2.github.io/reactor-effect-client/guides/testing-offline/

## Examples to copy from

https://github.com/mannyc2/reactor-effect-client/tree/main/examples: `quickstart` (one clip,
offline or live), `terminal` (frames drawn in a terminal), `livestream` (a 24/7 channel for many
viewers), and the package examples: `packages/browser/examples` (a page with its own session),
`packages/native/examples` (an MP4 capture), `packages/client/examples` (an offline-tested service).
