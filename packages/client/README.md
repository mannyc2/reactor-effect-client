# reactor-effect-client

An independent Effect SDK for Reactor's real-time video models: scoped sessions, the H3 provider, a playout that airs a keyed schedule across renewing sessions, and Reactor simulated in memory for tests. This is the portable core of the [reactor-effect workspace](https://github.com/mannyc2/reactor-effect-client); the browser and native transports are the separate `reactor-effect-browser` and `reactor-effect-native` packages.

One `Session` owns each allocation or attachment: its commands, connection generations and cleanup evidence. `H3` reads that session and never allocates one. `Playout` gets its sessions from a source and never allocates around it. Application scheduling, pricing, personas and proof of presented output stay with the application.

This is not an official Reactor SDK. Protocol material is attributed in [NOTICE](./NOTICE) and [`notices/`](./notices/).

## Install

```sh
npm install reactor-effect-client effect@4.0.0-rc.117
```

Effect is a peer dependency (`^4.0.0-rc.117`); later rc releases are accepted without a forced SDK release. Every module in this package is portable: importing it selects no host and loads no native code. Add `reactor-effect-browser` or `reactor-effect-native` for a transport, or run on `ReactorTest` without one.

## Modules

Each module is its own subpath, `reactor-effect-client/<Module>`, and the root exports every one as a namespace: `import { Playout, Reactor } from "reactor-effect-client"`. Nothing under `internal/` is reachable.

| Module         | What it is                                                                                                      |
| -------------- | --------------------------------------------------------------------------------------------------------------- |
| `Reactor`      | The service that acquires sessions: `create` allocates one this process owns, `attach` joins one                |
| `Session`      | One session: its status, events, commands, uploads, recordings, media and close report                          |
| `Coordinator`  | Reactor's HTTP API: pricing, tokens (`Tokens`, `fixedTokens`), inspection, termination and recordings           |
| `H3`           | The H3 provider over a session: its state and queue, its commands, acceptance evidence and reference validation |
| `Playout`      | Airs keyed items in priority lanes across sessions it renews, and reports what aired                            |
| `H3Source`     | A paid H3 session as a playout source: `open`, `resume` and the owner record `Allocation`                       |
| `LocalSource`  | A playout source rendered in this process by the application's hooks                                            |
| `Media`        | Decoded frames and platform tracks of one connection generation, and `recorder`                                 |
| `Peer`         | The transport port a host implements, and the `PeerFactory` service                                             |
| `ReactorError` | Every failure the client raises, its tagged reason and its dispatch outcome                                     |
| `ReactorTest`  | Reactor simulated in memory: the coordinator, peers and an H3 model, driven by the Effect clock                 |

The modules' doc comments state each option's default and bound; this page is the map.

## Sessions

`Reactor.layer()` needs a `Coordinator` and a host's `PeerFactory`; `Coordinator.layer(options)` and `Coordinator.layerConfig` (`REACTOR_API_URL`, and the API key from `REACTOR_API_KEY`) need an Effect `HttpClient`. `reactor.create` returns a connected session in the caller's scope, and closing the scope closes it:

```ts
import * as NodeServices from "@effect/platform-node/NodeServices";
import { Effect, Layer } from "effect";
import * as FetchHttpClient from "effect/unstable/http/FetchHttpClient";
import { Coordinator, H3, Reactor } from "reactor-effect-client";
import { NativePeer } from "reactor-effect-native";

const firstClip = Effect.gen(function* () {
  const coordinator = yield* Coordinator.Coordinator;
  const reactor = yield* Reactor.Reactor;
  const session = yield* reactor.create({
    model: H3.modelName,
    tokens: coordinator.tokens({ modelName: H3.modelName, maxSessionDuration: "5 minutes" }),
  });
  const provider = yield* H3.make(session);
  return yield* provider.enqueue({ prompt: "A slow camera move through a sunlit garden" });
}).pipe(Effect.scoped);

const ReactorLive = Reactor.layer().pipe(
  Layer.provideMerge(Layer.mergeAll(Coordinator.layerConfig, NativePeer.layer())),
  Layer.provide(FetchHttpClient.layer),
);

// The application's entry point provides the layers once. Running it allocates a paid session.
export const main = firstClip.pipe(Effect.provide(Layer.mergeAll(ReactorLive, NodeServices.layer)));
```

Building a host layer is its preflight: `NativePeer.layer()` loads the addon and `BrowserPeer.layer` detects WebRTC, so a host that cannot run fails before anything is allocated, and the factory's `check` runs again before every allocation. Over `FetchHttpClient`, every coordinator request omits ambient credentials and refuses redirects.

**Ownership.** `create` allocates a session this process owns: closing it terminates the session and confirms the end with an independent read, since a `DELETE` response alone proves nothing. `onAllocated` runs after allocation and before connecting, so a supervisor can record the owner first; its failure closes the session. `attach` joins a session without its remote lifetime; with `adopt: true` it takes that lifetime over, as a process resuming a dead owner's session does. `session.close` returns a `CloseReport`, a Schema you can persist, with the termination verdict and every local cleanup error; a failed acquisition's `AcquisitionFailure` carries the same report.

**Tokens.** A session never sees the API key. It runs on `Tokens`: `create`, a token that may create one session, and `bind(sessionId)`, a fresh token bound to an open session. The session keeps one token for all its calls and mints the next with `bind` a minute before it expires, or a quarter of a shorter token's life, so it outlives any one token; Reactor keeps a token at most six hours. `coordinator.tokens({ modelName, maxSessionDuration })` mints both with the key the Coordinator holds. `maxSessionDuration` is required, a duration or `"unlimited"`, because an uncapped session bills until something ends it. `coordinator.mintToken` takes `bind` and `maxSessions` directly and refuses a grant Reactor echoes wider than asked. `Coordinator.fixedTokens(grant)` serves a session shorter than its token, and a browser gets its tokens from a server that holds the key (see the [browser example](https://github.com/mannyc2/reactor-effect-client/tree/main/packages/browser/examples)). A server holding the key can end any session of its account: `coordinator.terminate` sends the key when no `credential` is set.

**Lifecycle.** A session reads `INACTIVE` while its last connection is gone; it is still live and billed, and Reactor ends it 30 seconds later unless a connection returns. Only `CLOSED` is terminal (`Coordinator.isTerminal`). `session.reconnect` opens a new connection generation within `reconnectTimeout` (30 seconds) and never replays a command. Each generation's media and replies belong to it: a reconnect ends the old generation's readers, and a late reply is published labelled `stale-generation`. A `Moderation` event reports a content-moderation verdict; after `terminate` Reactor ends the session and it is not reconnected (`Moderated`). `session.snapshot`, `session.changes` and `session.observe` read the status; `observe` pairs a snapshot with every event after it, with no gap.

**Deadlines.** `Reactor.layer(options)` sets the session deadlines: `replyTimeout`, `uploadTimeout`, `connectTimeout` (3 minutes, the wait for a GPU included, which is not billed), `reconnectTimeout`, `readyTimeout` and `heartbeatInterval`. Every duration is a `Duration.Input`, and a bare number is milliseconds, so write the unit. One that is not a finite, non-negative duration fails with `InvalidInput`, not submitted. `replyTimeout` is the library's own deadline for a reply, not a caller's wait: after dispatch its expiry fails with `Timeout`, outcome `unknown`, and the request stays attributable. To stop waiting without abandoning a command, fork it and bound the join:

```ts
import { Effect, Fiber } from "effect";
import type { Session } from "reactor-effect-client";

export const seed = (session: Session.Session) =>
  Effect.gen(function* () {
    const fiber = yield* Effect.forkScoped(session.command("set_seed", { seed: 1 }));
    // Later, Fiber.await(fiber) still reads the command's own outcome.
    return yield* Fiber.join(fiber).pipe(Effect.timeout("2 seconds"));
  });
```

**Media.** `session.decoded` is the current generation's decoded media, from the native host or `ReactorTest`; `session.tracks` is its platform tracks, from the browser host. Every frame carries its admission `sequence` on its track, so `Media.recorder(stream)` yields each frame plus a `Lost { after, count }` wherever the host dropped frames. A reader that falls behind its bound fails alone with `Overflow` and is counted in `pressure`'s `readerOverflows`; a preview keeps only the newest frame with `Stream.buffer({ capacity: 1, strategy: "sliding" })`.

Uploads (`session.upload`), recordings (`requestRecordingClip`, `recording`, then `coordinator.downloadClip`) and the deployment's OpenAPI document (`session.schema`) are on the session too.

## Errors

Every failure is one of three classes, each with its own `_tag`: `ReactorError`, `CommandFailure` (a command, with the dispatch evidence its owner established) and `AcquisitionFailure` (a failed acquisition, with its cleanup report). `ReactorError.isReactorFailure` recognizes any of them. Each carries a tagged `reason`: route on it with `Effect.catchReason`, `catchReasons` or `unwrapReason`. A reason's tag is a code such as `Timeout`, `Disconnected`, `InvalidInput` or `Moderated`, or one of the reasons with fields of their own: `Http` (`status`, `retryAfter`, `body`), `Remote` and `RecorderDisabled` (`remoteCode`, `body`), `Native` (`backendMessage`), `IceFailed`, `TransportFailed` and `ClipEnded`.

`context.outcome` says whether the remote may have applied the request: `not-submitted`, `unknown` or `replied`. `isRetryable` is true for backpressure, a connection lost before dispatch, and an HTTP refusal for now (a 5xx, 408, 429 or a named `Retry-After`), and never when the outcome is `unknown`, since the remote may already have applied it. A request whose token could not be had in time was never sent, and a create answered with a 5xx may have allocated a session, so its allocation is `unknown`.

```ts
import { Effect } from "effect";
import type { H3 } from "reactor-effect-client";

export const enqueue = (provider: H3.Provider, request: H3.Request) =>
  provider.enqueue(request).pipe(
    Effect.map((acceptance) => acceptance.clip.clip_id),
    // Nothing was sent: this deployment takes no reference audio.
    Effect.catchReason("CommandFailure", "UnsupportedCapability", () => Effect.succeed(undefined)),
  );
```

`message` is written by the library and never holds provider or payload text, so spans and logs that record it stay payload-free. Provider and native text is kept only in `Redacted` fields for explicit inspection (`body`, `backendMessage`, `context.detail`) and never enters the cause chain that exporters render. Persist a failure as `ReactorError.FailureSummary`, never the error itself.

## Tracing

The client traces through Effect's `Tracer`, so any tracer the application provides, such as `OtlpTracer`, receives its spans. An operation a caller can cancel has a client span at the call: `Reactor.create` and `Reactor.attach`, `Session.connect` and `Session.reconnect` (with an event per phase, from `reactor.connect.described` to `reactor.connect.ready`), `Session.upload` and `Session.close`, and `Coordinator.mintToken`, `pricing`, `inspect` and `terminate`. A request the session owns past its caller's wait, a command or control request (`Session.command`, `Session.control`) or an H3 enqueue (`H3.enqueue`, with `H3.reconcile`), has its span on its own execution, so the span ends with the request's outcome even after the caller stopped waiting. `Playout.submit` and the playout's other edits have spans, and `H3Source` opens and resumes sessions in `reactor.playout.open` and `reactor.playout.resume`. Termination returns a verdict rather than failing, so `Coordinator.terminate` and `Session.close` carry it as `reactor.termination.attempted`, `confirmed` and `evidence`: a span that ended without error does not mean a paid session stopped. Spans name identity and outcome only, never a credential, command input, a reply, an upload's name or bytes, or provider text. Frames, streams and heartbeats are not traced.

## H3

`H3.make(session)` is the provider over a connected session created with `H3.modelName`. It targets the documented `0.5.5` schema of `reactor/h3-reference-to-video-turbo-realtime`: prompts with up to nine image references and three audio references, as owned bytes or earlier uploads. It reads the deployment's schema, requires only `enqueue`, `get_state` and `get_queue` to start, and fails any other command the deployment lacks as `UnsupportedCapability` before sending it (`provider.contract` says what it found). A request H3 would refuse is refused locally, `not-submitted`, before anything is uploaded; `H3.validateReference` and `H3.validateAudioReference` check a reference once for reuse, and the profile's constants (`requestSeconds`, `referenceLimits`, `audioReferenceLimits`, `canvases`) state the bounds.

An enqueue is accepted by its correlated reply, or by the clip's metadata when the reply is lost. One whose outcome stays `unknown` is never sent again, though later evidence within `reconcileWindow`, or on a later connection generation, can still prove its clip. `provider.operation(submission)` follows a committed clip through `accepted`, `reached("generated")`, `reached("started")` and `ended`, each naming the generation of its evidence. H3 replies to a command before it broadcasts the state the command changed, so a command that needs current facts waits for them; `snapshot`, `changes` and `observe` read them. A command's acknowledgement proves receipt, never a state change.

## Playout

`Playout.make(options)` (or `Playout.layer`) airs a schedule: the application submits keyed items into priority lanes, and one plan decides what to build, in what order, what to withdraw and when a replacement session takes over. A pure policy makes every decision; the service applies them one provider command at a time, wakes on a submission, a session's evidence or the plan's next deadline, and never polls. Sessions come from the `open` effect, called for the first session and for each replacement:

- `H3Source.open({ tokens, canvas, holdLastFrame, onAllocated })` mints a token, allocates, runs `onAllocated` with the owner record (`H3Source.Allocation`, without a token), connects and sets H3 up: autoplay off until the playout turns it on, the last frame held between clips unless `holdLastFrame: false` flushes to black, and the canvas set before the first enqueue. Its lifetime is the create token's cap; an uncapped session never expires and is replaced only when lost. A dropped connection gets `recovery` (20 seconds) to reconnect before the session counts as lost, and the playout sends the session nothing meanwhile.
- `H3Source.resume({ allocation, tokens })` adopts a session its dead owner recorded, with tokens bound to it; a session the owner already set up gets only reads.
- `LocalSource.open({ build, present, discard })` renders clips in this process, with H3's autoplay semantics, for locally rendered material and demos.

What an application can ask for:

- **Items** (`submit`) in lanes listed highest first. A lane queues (the default), replaces its waiting items make-before-break, or skips while busy (`LaneBusy`), and a `cut: true` lane stops a lower lane's playing clip once its own is Ready. An item starts `Follow` (its lane's next boundary), `Asap`, `Manual` (held until `release`) or `At` a wall-clock instant, within an optional `notBefore`/`startBy` window; `WouldMissDeadline` refuses one the plan cannot start in time. A request outside H3's documented limits, its references included, is refused with `InvalidItem`, naming each field and limit, and nothing is sent for it.
- **Edits**: `submitGroup` (parts that build in order and air back to back), `insert` before or after any item, `replace` a queued item make-before-break, `edit` for several edits applied together with the old clips as cover until the new ones are Ready, `withdraw` and `drain`. Keys are idempotent: the same spec returns the same handle, a changed one fails with `KeyMismatch`. Withdrawing a group key answers `withdrawn` if any part was.
- **Filler** in the bottom lane keeps a runway of Ready seconds between `floor` and `target`, sized to tile the gap before an `At` anchor. A tile asks for no less than its share, and H3 aligns it up to its frame grid, so the anchor may air up to 0.7 s late for each tile. `Playout.lineup(filler)` is one `line` lane above it.
- **Cues** fire at offsets from a clip's observed start or end, and `continuity: "previous"` builds a clip continuing from the one that airs before it.

The playout reports what happened, kept apart from what was asked. Each handle's `started` and `outcome`, and `asRun`, give an item's statuses: Accepted, Building, Ready, Started, Ended, Dropped, Failed, `Unobserved` (acknowledged, start never seen) or `Unknown` (sent, acknowledgement never seen). An `Unknown` is never sent again and no time is invented for it. `Failed` says why with a tagged `reason`: `Clip` (the provider failed the clip; its own words stay `Redacted` in `provider`, out of `message`, logs and spans), `Command` (a command for it failed, with its `CommandFailure`; an enqueue refused unsent because its session was not ready is sent again instead), `Lost` (its session was lost while it played, or before it was built twice), `Moderated` (with the verdict's `categories`) or `Closed`. `events` adds cues, session events (`Opened`, `Switched`, `Replaced`, `SetupFailed`, `Moderated`, and `Reconnecting` and `Reconnected { afterMillis }` around a dropped connection), each filler clip's start and end (`Filler`), and `Starved`. `state` gives the clip on air (`playing`: its key, `"filler"` or `"other"`, when its start was seen, and its length when known), the lanes, sessions, runway and the build estimates it learned, `video` and `audio` the on-air picture and sound across renewals, and `cleanup` the retired sessions' close reports.

**Renewal.** A capped session builds only what airs before its cap. The replacement opens `lead` (30 seconds) before the cap ends, takes new work, and takes the air at a boundary once the retiring session is idle and a `grace` (250 ms) after its last clip has passed. A session lost before a planned switch is closed, and the clips it never aired are rebuilt on the next one. An enqueue whose outcome stays unknown for `unknownTimeout` (60 seconds) makes its session indeterminate, so a replacement takes over. The playout fails after `maxSetupFailures` (3) consecutive failed setups, closing every session.

**Moderation.** Reactor terminates a session given flagged content, and the verdict names no clip, category or request; hosted H3 sent it about a second after answering the enqueue. The playout blames the item whose enqueue was sent there last, settles it `Failed` with reason `Moderated` and never builds it again, and fails after `maxModerations` (2) moderated sessions rather than keep opening paid ones. A verdict need not come, so a clip lost unbuilt with two sessions in a row fails too; a clip that was built passed screening and is rebuilt however often it is lost.

**On hosted H3.** `stop` names no clip and lands after its reply, so a cut is a step at a time: the playout turns autoplay off, stops the clip it cuts if that still plays, waits for H3 to report it ended, looks again, plays the cutter and restores autoplay. A cutter withdrawn meanwhile is removed instead of played. The fenced cut passed on hosted H3 with one `stop` and a 150 ms seam, with no dark frame. A continued build took 5.45 s against about 2.2 s for an independent 5 s clip, so the plan projects it and, when it would be late, continues from the clip that will be playing then. H3 falls back to an independent clip without saying so, so as-run never claims continuity. A `Started` event is a provider fact, not proof that a frame was presented or encoded.

## Testing with ReactorTest

`ReactorTest.layer({ timing })` provides the `HttpClient` and `PeerFactory` that `Coordinator.layer()` and `Reactor.layer()` need, backed by an in-memory coordinator and an H3 model that speaks the real wire protocol, so an application and every SDK layer above it run unchanged without a paid session. `layerCoordinator` is the HTTP API alone, with no host. Every delay is an `Effect.sleep` drawn from `timing`: `Timing.fixed` for a scenario that names the delays it depends on, `Timing.random({ seed })` for wide ranges that repeat for a seed, and `Timing.hosted` for the ranges paid runs measured, for demos. Under `TestClock`, fork `ReactorTest.flow()` into the test's scope and minutes of programme run in milliseconds; on the live clock it plays in real time.

`ReactorTest` keeps Reactor's documented rules and the facts paid runs observed: tokens bound to sessions, expiry and unbound tokens refused, the API key accepted as a bearer only to read and end sessions, several connections per session, `INACTIVE` for 30 seconds after the last one drops, five concurrent sessions and ten a minute refused with `429`, H3's request, reference and metadata limits, a `stop` that lands after its acknowledgement, moderation as observed and per-second billing. The `ReactorTest` service reports `sessions`, `billing` and a `log` of every command, message and request, and `inject` arms a `Fault`: a refused allocation or connect, a dropped or late reply, a stalled or failed build, a disconnect or expiry, a slow or ignored `DELETE`, black, frozen or absent video, no audio, an invalid image, an over-granting token, a late recording or a moderation verdict. `ReactorTest.pngBytes` and `wavBytes` make reference media H3's local checks accept, and `frameOf` names the clip a simulated frame belongs to.

It models what H3 does, not hosted performance, billing, TURN relays or interop: those need the [hosted checks](https://github.com/mannyc2/reactor-effect-client/tree/main/integration/hosted).

## Declarations without DOM types

Effect `4.0.0-rc.117` itself references the global `TextDecoderOptions` type in its channel declarations. A strict Node project that deliberately omits DOM types may need to declare that standard interface. The workspace's installed-package check first proves this is the sole upstream diagnostic, then applies a test-only declaration; it does not enable `skipLibCheck` or hide SDK declaration errors.

## Example

[`examples/`](https://github.com/mannyc2/reactor-effect-client/tree/main/packages/client/examples) is an application service written against `Playout`, run offline on `ReactorTest` and tested on Effect's test clock with faults standing in for a failed build and a lost reply. The repository's [other examples](https://github.com/mannyc2/reactor-effect-client/tree/main/examples) include a server that broadcasts one renewing playout to many browsers.

## Development

This package is built and tested from the workspace root; see the repository [CONTRIBUTING](https://github.com/mannyc2/reactor-effect-client/blob/main/CONTRIBUTING.md). Inside `packages/client`, `bun run test` runs the portable suite under Vitest and `bun run typecheck` checks the source closure both with browser types and with Node types. The wire codec is generated from the protocol sources in [`wire/`](./wire/README.md).

## License

Apache-2.0. Third-party and derived-source notices are retained in [NOTICE](./NOTICE) and [`notices/`](./notices/).
