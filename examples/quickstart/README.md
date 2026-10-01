# Quickstart

One H3 clip from prompt to its end, with its video decoded in this process. Without a key it runs on
`ReactorTest`, Reactor simulated in memory at the timing paid runs measured: nothing is billed and
nothing native is loaded. With `REACTOR_API_KEY` set, the same program runs on hosted H3. Only the
layer changes.

```sh
bun install && bun run build      # at the repository root
node examples/quickstart/src/main.ts
```

```text
session sess_reactor_test_1 connected
clip 00000000-0000-4000-8000-000000000001 accepted
frame 0: 320x180 BGRA
playing
frame 24: 320x180 BGRA
...
ended
session closed, termination confirmed: true
```

It takes about 12 seconds on Node or Bun: H3's build time, then the 5-second clip.

## On hosted H3

```sh
REACTOR_API_KEY=rk_... node examples/quickstart/src/main.ts
```

The session runs on a token minted for it, which caps it at two minutes. H3 bills per second from
`ready` until the session ends, at $0.035 a second on September 30, 2026, so the run costs about
$0.50 and can never cost more than $4.20. The frames arrive at 1344x768 through the libwebrtc addon.
An npm install of `reactor-effect-native` brings that addon prebuilt for Linux x64 (glibc) and macOS on
Apple silicon. In this repository, build it first with `bun run native:build`
([native README](../../packages/native/README.md)).

## What it shows

- `Reactor.create` allocates and connects a session on its own token.
- `H3.make` reads the model's state and queue; `prepare` and `submit` enqueue a clip; its operation
  reports when it is generated, started and ended.
- `session.decoded` streams owned BGRA frames into JavaScript, in Node and Bun, with no browser.
- `session.close` terminates the session and confirms it with an independent read.

Next: the [terminal viewer](../terminal) draws those frames in your terminal, and the
[live channel](../livestream) keeps H3 on air across sessions for many viewers.
