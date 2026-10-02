# Examples

Seven Effect applications built on reactor-effect. Each is its own private workspace that declares
exactly what it uses and runs from its TypeScript sources on Node 22.18 or newer, or Bun. Most run
offline on `ReactorTest`, Reactor simulated in memory, with no API key. All but the rundown are
written to run on hosted Reactor with `REACTOR_API_KEY` set, where only the layer changes. No
example has run there yet, though paid checks ran most of the SDK they use
([hosted evidence](https://mannyc2.github.io/reactor-effect-client/reference/hosted-evidence/)).

```sh
bun install && bun run build      # once, at the repository root
```

| Example                                   | What it shows                                                                                                                                               | Without a key         |
| ----------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------- | --------------------- |
| [Quickstart](./quickstart)                | One H3 clip from prompt to its end, with its frames decoded in your process, in one file                                                                    | runs on `ReactorTest` |
| [Terminal viewer](./terminal)             | H3 drawn in your terminal as 24-bit colour text from decoded frames, with every clip followed to its end: no browser anywhere                               | runs on `ReactorTest` |
| [Avatar](./avatar)                        | A Vidu S2-Avatar character made from a photo: one call with a greeting and an answer to `say`, its picture decoded in your process, both sides' transcripts | runs on `ReactorTest` |
| [Live channel](./livestream)              | Your own 24/7 AI channel: viewers prompt it, a house rotation fills the gaps, sessions renew with no dark air, many browsers watch, RTMP restreaming        | runs on `ReactorTest` |
| [H3 Studio](../packages/browser/examples) | A page that runs its own session: H3's queue live, references, each clip's lifecycle, failures with their dispatch outcome                                  | runs in the browser   |
| [Capture](../packages/native/examples)    | A command line that writes one clip's decoded frames and audio to an MP4                                                                                    | live only             |
| [Rundown](../packages/client/examples)    | An application service over `Playout`, run and tested offline on the test clock                                                                             | runs on `ReactorTest` |

Offline, the simulated H3 sends a flat colour per clip, so what you see is the SDK at work rather
than the model's picture. H3 Studio's offline build runs in the
[playground](https://mannyc2.github.io/reactor-effect-client/playground/) on the documentation site.
The [examples page](https://mannyc2.github.io/reactor-effect-client/examples/) walks through each.

## Which shape fits

- **One session, many viewers**: the live channel. The server owns the session, its renewal and its
  cost; viewers get an ordinary video stream and never hold a token.
- **One session per user, lowest latency**: H3 Studio. Each page connects to Reactor itself and plays
  the WebRTC tracks; the server only mints the session's tokens.
- **Frames on a server, no viewer**: the terminal viewer and the capture. The native host hands your
  code decoded BGRA frames and PCM.
- **Logic you want to test without paying**: the rundown. Code written against `Playout` runs
  unchanged on `ReactorTest`, deterministically on the test clock.

## Live mode

A live example spends money. Reactor's pricing API states H3's rate per second, $0.035 a second on
September 30, 2026 (its billing page still says per session-minute), and the meter runs from
`ready` until the session ends. Every example caps its sessions with the token it mints, and the
terminal viewer and the capture print the most a run can cost before they allocate anything. The examples that
decode frames on the server need the native addon for the machine: npm installs it prebuilt with
`reactor-effect-native` (Linux x64 with glibc, macOS on Apple silicon), and in this repository
`bun run native:build` builds it.

## Using one outside this repository

Each `package.json` names its dependencies with this workspace's `catalog:` and `workspace:*`
protocols. In a project of your own, name the versions exactly: `effect` and the `@effect/*`
packages at one 4.0.x release, `4.0.0` here, and the three SDK packages at one version. With
`@effect/platform-node`, also pin `@effect/platform-node-shared` to that release with an override
([installation](https://mannyc2.github.io/reactor-effect-client/start/installation/)).

The examples import `@effect/platform-node` by module (`@effect/platform-node/NodeRuntime`). The
package's index also loads Effect's RPC modules, whose declarations name the DOM's `Transferable`,
so a Node project without DOM types that checks library declarations would need that type declared.

## What is checked

`bun run verify` typechecks every example workspace with the Effect language service's diagnostics,
and lints and formats them. `bun run check:examples` runs each example's offline tests under Node and
Bun (the live channel end to end on `ReactorTest`, the rundown, the capture's recorder) and builds
the browser bundles, which must reach neither Node nor native code. The tests that encode video need
`ffmpeg` and skip without it, except in CI. `bun run test:pack` compiles the examples again inside
clean installs of the packed archives and runs the rundown there on Node and Bun. Nothing in these
checks contacts Reactor or spends money: credential variables are removed, and the examples that
need a session are compiled but never run.
