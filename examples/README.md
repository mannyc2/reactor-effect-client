# Examples

Four programs, each showing what one part of the SDK is for, written as Effect applications: services and layers, schemas at their boundaries, `NodeRuntime.runMain`, and tests with `@effect/vitest`. Each is its own private workspace that declares exactly what it uses, runs from its TypeScript sources (Node 22.18 or newer, or Bun), and is checked by `bun run verify`.

| Example                                                  | Packages        | What it shows                                                                                                                                       | Paid?              |
| -------------------------------------------------------- | --------------- | --------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------ |
| [Live channel](./livestream/README.md)                   | client, native  | A server that runs one orchestration, renews its sessions, and broadcasts its decoded media to many browsers; prompts go through the server         | offline by default |
| [Rundown](../packages/client/examples/README.md)         | client          | An application service over the orchestration `Engine`, run and tested offline against the simulation on the test clock; error reasons and outcomes | no                 |
| [In the browser](../packages/browser/examples/README.md) | client, browser | A page that runs its own session over the browser's WebRTC, with a server that only mints tokens; `ManagedRuntime` from ordinary DOM code           | yes                |
| [Capture](../packages/native/examples/README.md)         | client, native  | A command line that generates one clip and writes its decoded frames and audio to an MP4, following the clip through its operation facts            | yes                |

## Which shape fits

- **One session, many viewers**: the live channel. The server owns the session, its renewal and its cost; viewers get an ordinary video stream and never hold a token.
- **One session per user, lowest latency**: the browser example. Each page connects to Reactor itself and plays the WebRTC tracks; the server's only job is to mint short tokens.
- **Frames on a server, no viewer**: the capture. The native host hands the decoded BGRA frames and PCM to your code.
- **Logic you want to test without paying**: the rundown. Code written against the orchestration's `Engine` runs unchanged on the simulation, deterministically on the test clock.

## Using one outside this repository

Each example's `package.json` names its dependencies with this workspace's `catalog:` and `workspace:*` protocols. In a project of your own, use the versions in the root [`package.json`](../package.json) (`effect` and the `@effect/*` packages at `4.0.0-rc.115`, the three SDK packages at the same version as each other), and keep the root override `"@effect/platform-node-shared": "4.0.0-rc.115"` in an npm project that depends on `@effect/platform-node`: the platform's caret range otherwise admits a later prerelease built against a different Effect. The examples import `@effect/platform-node` by module; see below.

## What is checked

`bun run verify` typechecks every example workspace (with the Effect language service's diagnostics) and lints and formats them. `bun run check:examples` then runs each example's offline tests under Node and Bun (the live channel end to end on the simulation, the rundown, the capture's recorder) and builds the browser bundle, which must reach neither Node nor native code. The tests that encode video need `ffmpeg` and skip without it, except in CI. `bun run test:pack` compiles the examples again inside clean installs of the packed archives and runs the rundown there on Node and Bun.

Nothing in these checks contacts Reactor or spends money: credential variables are removed, and the examples that need a session are compiled but never run.

## Found while writing them

- In Effect 4.0.0-rc.115, the Node child-process spawner leaves the child's stdin, and any input fd, with no error listener once its writer is interrupted, so a buffered write that fails when the child exits raises an uncaught `EPIPE`. The live channel and the capture keep their pipe writers running until they end their pipes themselves; see their notes.
- In Effect 4.0.0-rc.115, `Stream.peel` discards its sink's leftovers, so the rest of the chunk that completes the sink is lost. The capture's recorder takes its first frame from a queue instead.
- In the same release, the spawner's kill without `forceKillAfter` waits for the child to exit with no bound (its module documentation says one second), so a child that survives `SIGTERM` hangs whatever closes its scope. The live channel kills its encoder with `SIGKILL`.
- The examples import `@effect/platform-node` by module (`@effect/platform-node/NodeRuntime`), as this repository does, where Effect's own documentation imports the package's index. The index also loads Effect's RPC modules, whose rc.115 declarations name the DOM's `Transferable`. A Node project without DOM types that checks library declarations already needs a `TextDecoderOptions` declaration for Effect's `Channel` (as `bun run test:pack` supplies); importing the index would need `Transferable` too.
