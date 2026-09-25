# reactor-effect-client

An independent Effect SDK for scoped Reactor sessions, H3 provider state and explicit orchestration. This is the portable core of the [reactor-effect workspace](https://github.com/mannyc2/reactor-effect-client); browser and native transports are the separate `reactor-effect-browser` and `reactor-effect-native` packages.

One canonical `Session` owns each allocation or attachment, its commands, connection generations, and cleanup evidence. Host packages select transport capabilities. H3 consumes that same session and exposes provider state. Applications opt into the separate orchestration and simulation modules when they need scheduling, sequence affinity, or renewal.

This is not an official Reactor SDK. Protocol material is attributed in [NOTICE](./NOTICE) and [`notices/`](./notices/).

## Install

```sh
npm install reactor-effect-client effect@4.0.0-rc.117
```

Effect `4.0.0-rc.117` is a peer dependency (`^4.0.0-rc.117`); later rc releases are accepted without a forced SDK bump. Every module in this package is portable: importing it selects no host and loads no native code. Add `reactor-effect-browser` or `reactor-effect-native` for a transport.

## Modules

| Import                                | Purpose                                                                                        |
| ------------------------------------- | ---------------------------------------------------------------------------------------------- |
| `reactor-effect-client`               | Canonical `Client`, `Session`, `make`/`layer`, errors, `Coordinator`, `Peers`, and `FetchHttp` |
| `reactor-effect-client/h3`            | H3 provider contract, state/queue/clip evidence, controls, and reference validation            |
| `reactor-effect-client/orchestration` | Scheduling, renewal, source adaptation, references, submissions, and sequences                 |
| `reactor-effect-client/simulation`    | Production simulation source, factory, and Effect service layer                                |
| `reactor-effect-client/testing`       | Reusable fault, PNG and WAV fixtures                                                           |
| `reactor-effect-client/wire`          | Generated protocol messages and wire encoding/decoding                                         |
| `reactor-effect-client/host`          | Host transport extension surface used by the first-party host packages                         |

These seven paths are the complete public export map. Internal file layout does not create additional supported deep imports. `/host` is published for `reactor-effect-browser` and `reactor-effect-native`, which pin this exact version; applications compose hosts through those packages and never need it.

## Session ownership

The root exports the `Client` service and its `make` and `layer` constructors. They require Effect HTTP and crypto services plus a `PeerFactory`. `create` allocates a remote session; `attach` identifies an existing session without taking ownership of its remote lifetime, unless it is given `adopt: true`, which takes that lifetime over: the adopted session is `owned`, so closing it, or a failed `attachConnected`, terminates it, as a process resuming a session its dead owner recorded needs. Both return the same scope-owned `Session` contract, and `session.connect` starts WebRTC. `createConnected` and `attachConnected` combine those steps and clean up a partial acquisition before reporting failure.

Releasing an owned session attempts and independently confirms remote termination. Releasing an attached session closes its local resources and publication claims. `session.close` returns a `CloseReport` that retains local cleanup errors and remote termination uncertainty. Supervisors can inspect that evidence without allocating another owner.

```ts
import { Effect, Redacted } from "effect";
import * as Reactor from "reactor-effect-client";

const useSession = Effect.gen(function* () {
  const client = yield* Reactor.Client;
  const session = yield* client.create({
    model: "your-model",
    jwt: Redacted.make("session-token"),
  });

  yield* session.connect;
  return yield* session.current;
});
```

The surrounding application supplies the `Client` layer and Effect platform services. Credentials, HTTP policy, scopes, and host transport selection stay visible in composition. A host package supplies the `PeerFactory`:

```ts
import { Effect, Layer } from "effect";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as Reactor from "reactor-effect-client";
import * as Native from "reactor-effect-native";

const clientLayer = Reactor.layer().pipe(
  Layer.provide(Layer.mergeAll(Reactor.FetchHttp.layer, NodeServices.layer, Native.layer())),
);

// useSession is the operation above. Acquiring a real session can be billable;
// the repository's ordinary verification uses controlled local peers instead.
const main = Effect.scoped(useSession).pipe(Effect.provide(clientLayer));
```

Building a host layer is its preflight: `Native.layer()` loads and verifies the native library, and `Browser.layer` detects WebRTC, so an unsupported host fails while the layer is built, before any `Client` exists to allocate a remote session. Constructing a factory makes no allocation. HTTP and crypto services remain explicit. The root session constructor has no filesystem or path requirement. A custom host provides `PeerFactory` itself: `make` returns a fresh `Peer` for each connection generation, and an optional `check` fails when the host cannot create one right now; the factory runs it before every remote allocation.

Browser and native media values stay bound to their negotiated generation. A reconnect creates a new generation; existing readers end or fail with their source. Applications obtain the new media generation explicitly, or opt into orchestration's recovering media streams. Each `VideoFrame` declares its pixel `format` (`"BGRA"` from the native host); nothing converts between formats implicitly. A recorder reads a track directly and sees loss before admission as a rise in `pressure`'s `droppedVideo`/`droppedAudio`, and a reader that falls behind its bound fails with `Overflow` and counts in `readerOverflows`. Every frame carries its admission `sequence` on its track, so `recorder(stream)` yields each frame plus a `Lost { after, count }` wherever the host dropped frames, at their position. A preview keeps only the newest frame with `Stream.buffer({ capacity: 1, strategy: "sliding" })`.

## Errors

Every failure the client raises is one of four classes, each with its own `_tag`, so `Effect.catchTag` and a Schema union tell them apart:

- `ReactorError`: a session, coordinator or host failure;
- `CommandFailure`: a failed command, with the dispatch evidence its owner established;
- `AcquisitionFailure`: a failed acquisition, with the `cleanup` report of its partial lease, kept by reference;
- `PolicyFailure`, from `/orchestration`: a local refusal, which was never dispatched.

`isReactorFailure` recognizes any of them, and each class has an `is` guard. Re-raise a known failure through a guard rather than `instanceof ReactorError`, which none of the other three satisfies.

Each carries a tagged `reason`: route on `reason._tag`, with `Effect.catchReason`, `catchReasons` or `unwrapReason`. A `ReactorError`, `CommandFailure` or `AcquisitionFailure` reason is `Failure`, whose tag is a code such as `Timeout`, `Disconnected` or `InvalidInput`, or one of the reasons with fields of their own: `Http` (`status`, `retryAfter`, `body`), `Remote` and `RecorderDisabled` (`remoteCode`, `body`), `Native` (`status`, a Redacted `backendMessage`), `IceFailed` (`pairs`, `candidateTypes`) and `TransportFailed` (`pairs`). `ErrorCode` is the union of those tags. A `PolicyFailure` reason is a `Refusal` such as `QueueFull`, `SessionRecovering` or `Busy`, `Missing` with the request field whose clip has no known owner, or `Sequence` with the sequence's code.

`context.outcome` says whether the remote may have applied the request: `not-submitted`, `unknown` or `replied`. A `PolicyFailure`'s outcome is always `not-submitted`, so an `EngineError = CommandFailure | PolicyFailure` is read without narrowing. `isRetryable` is true for local backpressure, a connection lost before dispatch, an HTTP refusal that names a delay, and the `QueueFull` and `SessionRecovering` refusals, and never when the outcome is `unknown`; `retryAfter` is the delay an `Http` reason named.

```ts
const admission = (failure: EngineError) =>
  failure.context.outcome === "unknown" ? "reconcile" : failure.isRetryable ? "defer" : "reject";

const enqueued = engine.enqueue(request).pipe(
  Effect.catchReasons("PolicyFailure", {
    QueueFull: () => Effect.succeed("wait for a free generation slot"),
    SessionRecovering: () => Effect.succeed("wait for the session to recover"),
  }),
);
```

`message` is written by the library and never contains provider or payload text, so spans and logs that record it stay payload-free. Provider and backend text is kept only for explicit inspection: `Http.body`, `Remote.body`, the Redacted `Native.backendMessage` and `context.detail`. Diagnostic JSON leaves all of them out, and none of them is part of the cause chain that exporters render.

## Tracing

The client traces through Effect's `Tracer`, so any tracer the application provides, such as `OtlpTracer`, receives its spans. An operation a caller can cancel has a client span at the call: `reactor.session.create` and `reactor.session.attach`, `reactor.session.connect` and `reactor.session.reconnect` (with an event per phase, from `reactor.connect.described` to `reactor.connect.ready`), `reactor.session.upload` and `reactor.session.close`, and in orchestration `reactor.orchestration.resume` and `reactor.orchestration.renewal.open`, `recover`, `replace` and `switch`. A request the session owns past its caller's wait, a command or control request (`reactor.session.command`, `reactor.session.control`) or an H3 enqueue (`reactor.h3.enqueue`, with `reactor.h3.reconcile`), has its span on its own execution, so the span ends with the request's outcome even after the caller stopped waiting. Termination returns a verdict rather than failing, so `reactor.coordinator.terminate` and `reactor.session.close` carry it as `reactor.termination.attempted`, `confirmed` and `evidence`: a span that ended without error does not mean a paid session stopped. Spans name identity and outcome only: never a credential, command input, a reply, an upload's name or bytes, or provider text. The renewal tick, frames, streams and heartbeats are not traced.

## Time options

Every time option is an Effect `Duration.Input` named by its role:

- on the session, `connectTimeout`, `readyTimeout`, `heartbeatInterval`, `replyTimeout` and `uploadTimeout`;
- on the coordinator, `requestTimeout` and the poll's `initialDelay` and `maxDelay`, and on a token, `maxSessionDuration` and `expiresAfter`, which must be whole seconds;
- on H3, `replyTimeout`, `uploadTimeout`, `setupTimeout`, `reconcileWindow` and `resultHookTimeout`;
- in orchestration, `lead`, `reconnectTimeout` and the `lifetime` that `open` returns.

A bare number is milliseconds, as everywhere in Effect, so write the unit: `lead: "30 seconds"`, because `lead: 30` is 30 milliseconds. `"Infinity"` disables the heartbeat and marks a source that never expires. A value that is NaN, negative, zero where zero means nothing, or longer than the option's maximum fails with `InvalidInput`, not submitted. Measurements, instants and media lengths stay numbers in the unit their name carries: `requestRecordingClip(seconds)`, `ClipRequest.durationSeconds`, a renewal's `ageSeconds`.

`replyTimeout` is the session's own deadline for a reply. The session's default and a command's override share the key: `session.command(name, data, { uploads, replyTimeout })`. It is not a caller's wait. After dispatch it fails with `Timeout`, outcome `unknown`, and the command's `requestId`, and the request's slot stays held until a late reply or the connection generation retires. To stop waiting without abandoning the command, fork it and bound only the join:

```ts
const fiber = yield * Effect.forkScoped(session.command("set_seed", { seed: 1 }));
const reply = yield * Fiber.join(fiber).pipe(Effect.timeout("2 seconds"));
// Later, Fiber.await(fiber) still reads the command's own outcome.
```

When the command must be replayed rather than awaited, prepare it as an `Orchestration.Submission`.

## Coordinator helpers

`Coordinator` is a namespace on the root export. `Coordinator.make(configuration)` provides pricing, bounded token minting, session inspection, termination reports, and `downloadClip` for a prepared recording (`clip_ready`), which polls its HLS playlist and concatenates its segments within a caller wall deadline, through Effect HTTP services. Constructing this client makes no network request and requires no peer implementation. The root exports the Schemas the client decodes a session descriptor with, `SessionDescriptor`, `Capabilities`, `Track` and `Mapping`, and derives their types from them.

## H3 provider

`H3.make(session, options)` consumes an already connected canonical `Session`. It checks the deployment contract and exposes provider state, queue and clip messages, observations, command replies, and locally correlated acceptance evidence. Provider facts remain visible even when another client authored the clip. A command ACK establishes receipt; state changes require the corresponding model evidence.

```ts
import { Effect } from "effect";
import type { Session } from "reactor-effect-client";
import * as H3 from "reactor-effect-client/h3";

const enqueueClip = (session: Session) =>
  Effect.gen(function* () {
    const provider = yield* H3.make(session);
    return yield* provider.enqueue({
      prompt: "A slow camera move through a sunlit garden",
      seconds: 5,
    });
  });
```

Use a session created with `H3.modelName`. The adapter targets the documented `0.5.5` prompt-and-images subset of `reactor/h3-reference-to-video-turbo-realtime`, with reference audio. Prompt-only requests are supported. Image references accept owned bytes or explicit upload references; H3 itself requires no filesystem or path services. FastH3 `startingFrame`/`endingFrame` fields are absent from this contract.

`Request.audio` carries up to three audio references (`Audio 1`, `Audio 2`, ... in the prompt), sent as `reference_audios`: each 2–15 s of WAV, MP3, AAC/M4A, OGG/Opus, FLAC or WebM, mono or stereo, at most 25 MiB, as bytes or an existing upload. A clip with audio needs an image reference or `continueFrom`, and a continued clip takes at most two, since its continuation uses the third for the previous clip's soundtrack. The adapter identifies the container from the bytes and uploads it under that MIME type; it reads the length and channels of WAV and FLAC and refuses one outside the bounds, while H3 checks the other formats itself. Anything refused locally fails `not-submitted`, before an upload. `H3.validateAudioReference` validates one once for reuse, and `H3.audioReferenceLimits` lists the bounds. The `reference_audios` argument is optional in a deployment's schema: `provider.contract.referenceAudio` says whether this deployment declares it, and against one that does not, a request with audio fails `UnsupportedCapability` while every other request is unaffected. The accepted clip reports `has_reference_audio` and `reference_audio_count` when the deployment sends them. `ClipRequest.audio` takes `{ uri }` values for orchestration, loaded as image references are, and the simulation records them on the clip.

`provider.operation(submission)` keeps a committed clip's facts for as long as its scope holds them: `accepted`, `reached("generated" | "started")` and `ended` each resolve once, every fact naming the transport generation of its evidence, and a clip that fails or is popped first fails the phases it never reached with `ClipEnded`. Evidence arriving after the reconcile window, or in a later transport generation of the same session, still attributes an unknown enqueue. A commit reserves the operation's slot before it sends, so a table full of unresolved operations refuses new commits with `Overflow` instead of discarding evidence; releasing the scope frees the slot.

The adapter exposes autoplay, flush, playback, reset, and other model controls as explicit operations. Creating it does not change those policies or initiate reconnect. Request `metadata` is a provider string; orchestration keeps its richer application annotation separately.

## Orchestration and simulation

`reactor-effect-client/orchestration` owns opt-in scheduling, sequence routing, renewal, and recovering media. `fromH3Session(session, options)` builds a physical source from one connected session, deriving its H3 provider view (exposed as `source.provider`) and its decoded media from that session, so a source cannot pair one session's commands with another's media; it needs a host with decoded media (the native host), and `make({ open, ...options })` returns a handle with `engine`, `media`, `mediaState`, sequence operations, and joined cleanup reports. The `open` effect supplies each source and its lifetime budget. `openH3({ mint, onAllocated })` is that effect for paid H3 sessions: it mints the token, allocates the session, runs `onAllocated({ session, grant, allocation })` before connecting so the owner can be recorded durably, then connects and derives the source, with the granted length as its lifetime. The `allocation` is the record to persist (`Orchestration.Allocation`, without the token); its `endsAt` is when the granted length ends at the latest. `resumeH3({ allocation, jwt })` is the other half: after the owner died, it attaches to the recorded session with `adopt: true` and derives its source, sending only state and queue reads, never a canvas or other command a playing session refuses, with the time left until `endsAt` as its lifetime, so it is also a renewal `open`, and renewal terminates the session when it retires it. It refuses a record without `endsAt` or past it before anything is sent. `Orchestration.layer(options)` provides the handle's `Engine`, `Media` and `Handle` services from one orchestration; each build opens its own paid chain, so bind the layer to a `const`. `engine.observe(options)` is the engine's gap-free observation, as `session.observe` and `provider.observe` are for theirs: it subscribes before reading, returning the current `EngineState` with a stream of every later event (apply events idempotently by `clipId`; after an `Overflow`, observe again). `handle.observe(options)` extends it to the whole handle: the engine and media state, then engine events, renewals and media-state transitions in the one order they happened. `onRenewal` runs on its own reader and never holds the handle's command permit, and the orchestration logs through Effect's logger at debug level. Renewal measures elapsed time on Effect's monotonic clock: a source's age, its expiry, the lead, the retry cooldown and the recovery budget, as well as `BuildTiming`. A correction of the host's wall clock therefore moves no renewal and bends no build time. Recorded instants (every event's `at`, `startedAt`, `enqueuedAt` and `Allocation.endsAt`) stay epoch milliseconds of wall time. Renewal polls: each tick runs 100 ms after the previous one finished. Application scheduling, pricing, persona, and show policy stay outside the session and H3 layers.

`ClipRequest.sameSessionAs` targets the physical session that owns a known clip, including one already ready or playing, while leaving queue position unchanged. `before` requests insertion ahead of a clip still in the generation queue. Source affinity, insertion anchors, continuation, and sequence ownership must agree; missing, conflicting, retired, or recovering ownership fails locally with a `not-submitted` outcome. This keeps a dependent request on its required session during renewal without inventing an insertion point.

`Orchestration.makeScheduler(options)` is an opt-in clip scheduler over the `Engine`; `layerScheduler(options)` provides it as an Effect service. Apps submit stable keys into priority lanes, listed highest first. The `lineup(filler)` preset supplies one `line` lane above generated filler. Each lane is FIFO unless a deadline gives an earlier item priority for build admission. The scheduler admits one build at a time by default, keeps a floor and target of playable Ready seconds, and orders Ready clips within each physical session. Filler is generated by a pure `clip({ index, runwaySeconds, targetSeconds })` callback and is never owed. A replacement session has its own queue and runway during renewal; once an app clip is Ready there, the retiring session's Ready filler is withdrawn only when its final observed video has arrived and no old item is queued or building. The switch then follows its playing clip. Earlier frame loss and source drop totals remain on `Switched.tail`; they do not delay a later complete clip. Audio remains explicitly unverified. `EngineState.handoffReady` and the `HandoffReady` engine event expose this media condition; they do not establish presented output. A submitted request cannot set `position`, `before`, `sameSessionAs`, `continueFrom` or sequence ownership, because those can override the scheduler's order.

`submit({ key, lane, request, window, start })` returns a handle with `started`, `outcome` and `firstDecisive` effects. `started` and `firstDecisive` retain the first observed start or definitive disposition; `outcome` waits for the end or definitive disposition. None settles on interim `Unknown`. Repeating a retained key and spec returns the same handle; a changed spec fails `KeyMismatch`. Active keys are retained until settled. `maxHistory` bounds completed key retention (4096 by default, zero disables history); older completed keys can be submitted anew once evicted. Handles already held by callers keep their original results. Applications needing longer deduplication retain their own key ledger. `window.notBefore` and `window.startBy` are relative monotonic durations, with firm deadlines dropped on expiry and soft deadlines recorded late at start. `start` defaults to `Follow` (the next boundary); `At` names an epoch millisecond anchor and a late policy (`nextBoundary`, `skipIfLaterThan`, or `drop`). The scheduler fills toward an upcoming anchor and never cuts a playing clip. `WouldMissDeadline` rejects an item whose known queued material already makes its deadline impossible, including a negative `startBy` offset. A refused `At` removal is paced and retried; an uncertain removal waits for a fresh observation before retrying. Actual starts remain observable even when the provider could not hold the anchor. `withdraw(key)` waits for a provider-confirmed removal, including an in-flight build. `drain()` stops admission, withdraws waiting clips and waits for the current clip to end. `drain({ finish: "accepted" })` lets all accepted items reach their disposition, including those still waiting to build. Both stop filler admission and call `engine.stopRenewal` immediately, even while an enqueue is pending: new replacements cannot open, an opening replacement is cancelled and joined, and already acquired sources may finish. Expiry still retires a source and surfaces failure if no existing replacement can continue. The first drain call selects its mode. The owner closes its orchestration handle after drain completes.

`asRun` is an unbounded per-item lifecycle stream: Accepted, Building, Ready, Started, Ended, Dropped, Failed, Unobserved or Unknown. Each item also has an `outcome` wait. `Started.durationSeconds` is the provider-observed duration. `Failed.reason` distinguishes `Clip` failure, `Command` refusal and `Scheduler` termination, retaining the original typed cause where available. `scheduler.failure` exposes the original terminal failure; scope closure signals `Closed`, and pending or subsequent control calls fail with `SessionClosed`. `Started.at` and `Ended.at` are observed epoch times; elapsed airtime and deadline windows use Effect's monotonic clock. `Unobserved` means an acknowledged clip's start was not seen, including after observation overflow. `Unknown` means dispatch has no acknowledgment. A keyed clip observed later can still acquire its Ready or Started evidence. An absent queue snapshot cannot prove an unknown clip never played, so the scheduler never replays that key automatically. The renewal engine fences each enqueue to its selected physical source before dispatch; an unknown outcome becomes terminal when that source retires without keyed proof. With a custom engine that cannot fence its source, uncertainty stays open until the scheduler closes. A withdrawal of that key fails with the original uncertain outcome unless a provider clip is found and removed. The scheduler carries an item key in library-owned provider metadata and leaves the app's request metadata untouched. As-run records provider evidence; an app that needs proof of presented or encoded output must gather that separately.

`Orchestration.Submission` models an inert prepared operation. Preparation may be interrupted before commit; once execution commits, callers joining or abandoning the result do not replay the dispatch. `CommandFailure.context` distinguishes `not-submitted`, `unknown`, and `replied` outcomes independently of the transport error category, and an engine command fails with `EngineError`, a `CommandFailure` or a local `PolicyFailure`.

`Orchestration.Sequences` owns bounded sequence affinity and explicit member outcomes. Partial admission is represented member-by-member as accepted, rejected, or indeterminate, and a sequence remains bound to one owner until it is sealed/retired and explicitly released.

`reactor-effect-client/simulation` provides `make` and `layerSim` over the same orchestration engine and media contracts. Its source and renderer hooks support unpaid local execution and controlled failures. Reusable fault, image and audio fixtures (`pngBytes`, `wavBytes`) are available through the separate `reactor-effect-client/testing` entry point.

## Wire module

`reactor-effect-client/wire` is generated from the tracked canonical protocol sources under [`wire/`](./wire/README.md) by `wire/generate.py` (CPython 3.13, standard library only). `bun run generate:check` fails on any byte difference; `bun run generate:wire` rewrites the module. Do not format the generated file or edit it independently of its inputs.

## Declarations without DOM types

Effect `4.0.0-rc.117` itself references the global `TextDecoderOptions` type in its channel declarations. A strict Node project that deliberately omits DOM types may need to declare that standard interface. The workspace's installed-package check first proves this is the sole upstream diagnostic, then applies a test-only declaration; it does not enable `skipLibCheck` or hide SDK declaration errors.

## Example

[`examples/`](https://github.com/mannyc2/reactor-effect-client/tree/main/packages/client/examples) is an application service written against the orchestration `Engine`, run and tested offline against the simulation on Effect's test clock: gap-free observation, retrying only what `isRetryable` allows, and never resending an enqueue whose outcome is `unknown`. The repository's [other examples](https://github.com/mannyc2/reactor-effect-client/tree/main/examples) include a server that broadcasts one renewing orchestration to many browsers.

## Development

This package is built and tested from the workspace root; see the repository [CONTRIBUTING](https://github.com/mannyc2/reactor-effect-client/blob/main/CONTRIBUTING.md). Inside `packages/client`, `bun run test` runs the portable suite under Vitest and `bun run typecheck` checks the source closure both with browser types and with Node types.

## License

Apache-2.0. Third-party and derived-source notices are retained in [NOTICE](./NOTICE) and [`notices/`](./notices/).
