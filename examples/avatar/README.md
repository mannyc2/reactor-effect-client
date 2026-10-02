# Avatar

A Vidu S2-Avatar character made from one photo, and one call with it. The character greets you,
answers a line sent with `say`, and its picture is decoded in this process while its transcripts
print. Without a key it runs on `ReactorTest`, Reactor simulated in memory: nothing is billed and
nothing native is loaded. With `REACTOR_API_KEY` set, only the layer changes.

```sh
bun install && bun run build      # at the repository root
node examples/avatar/src/main.ts
```

```text
session sess_reactor_test_1 connected
making the avatar
avatar avatar_reactor_test_1 ready
call live
frame 0: 232x272 BGRA
frame 25: 232x272 BGRA
...
character: Greeting 1.
user: What lives in the deepest tank?
...
character: Answer 2 to: What lives in the deepest tank?
call ended: ended_by_client after 19 s
session closed, termination confirmed: true
```

It takes about 54 seconds on Node or Bun, at the slower of the paid runs' timings: 26.7 s to make an
avatar and 13.2 s to end a call. Offline, the picture is a flat colour and the character's lines
are placeholders, so what you see is the SDK at work, not the model.

## On hosted Vidu S2-Avatar

```sh
REACTOR_API_KEY=rk_... AVATAR_PHOTO=./tina.jpg node examples/avatar/src/main.ts
```

`AVATAR_PHOTO` is a photo of one person, full or half body, as PNG, JPEG, WebP or HEIC. Without it
the program sends a blank 64x64 stand-in meant for the simulation.

The session runs on a token minted for it, which caps it at two minutes. At the 70 credits a second
Reactor's pricing API stated for Vidu S2-Avatar on October 2, 2026 ($0.007 a second), a run can
cost at most $0.84. Reactor bills the whole session, time between calls included.

This program has not run on hosted Reactor. The paid `avatar` probe drove the same commands
through the raw `Session` under Bun on 2026-10-01 and 2026-10-02
([hosted evidence](https://mannyc2.github.io/reactor-effect-client/reference/hosted-evidence/)).
On the second run:

- the avatar was ready 2.1 s after `create_avatar`, against 26.7 s on the first, from the same
  photo;
- the call was live about 4 s after `start_call`;
- the picture came 0.20 s after the character's tracks were resumed at live, at 928x1088 and 24.9
  fps (640x360 before a call);
- `end_call` was answered in 6.9 s and 7.4 s.

The frames are decoded by the native addon. An npm install of `reactor-effect-native` brings it
prebuilt for Linux x64 (glibc) and macOS on Apple silicon; in this repository, build it with
`bun run native:build` ([native README](../../packages/native/README.md)).

## What it shows

- `ViduS2Avatar.make` reads the model's snapshot; `createAvatar` uploads the photo and returns once
  the avatar is ready.
- `startCall` returns once the call is live. The provider resumes the character's `main_video` and
  `main_audio` tracks then, as Reactor's tutorial does at every live. Without that resume, the
  first paid run got the character's voice and none of its picture.
- `say` is text the character answers as if the caller had said it. A Node client can't send
  microphone audio (the native host decodes what it receives and can't publish a track), so here
  the caller speaks only through `say`. In a browser, publish the `mic` track through `session.tracks`.
- `events()` streams snapshots, transcripts and the model's refusals; `endCall` returns the call's
  end once the model has released it.
- `session.close` terminates the session and confirms it with an independent read.
