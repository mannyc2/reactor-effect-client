# Changelog

All notable changes to `reactor-effect-client`, `reactor-effect-browser` and `reactor-effect-native` are documented in this file. The three packages are always released together, with one version.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

The next release is 0.3.0, because `main` already breaks the 0.2.0 API. A `^0.2.0` range does not include 0.3.0, so applications move to it deliberately.

### Added

- Each dispatched model command and control request runs in a `reactor.session.command` or `reactor.session.control` client span on the session-owned execution. The span is a child of the caller's span and ends with the request's own outcome, even after the caller stopped waiting. It carries `reactor.operation`, `reactor.request.id` and `reactor.connection.generation`, and on exit `reactor.command.outcome` and, for a typed failure, `error.type` set to the error's code. It never carries the input, the reply or provider text. A request refused before dispatch opens no span, and heartbeats are not traced.

### Changed

- **Breaking:** `ReactorError` is built from its schema fields: `new ReactorError({ code, message, context })`, with `context` defaulting to empty. The positional `new ReactorError(code, message, context)` constructor is gone. `CommandFailure`, `AcquisitionFailure` and the orchestration `PolicyFailure` are built with `CommandFailure.from(error, context)`, `AcquisitionFailure.from(error, cleanup)` and `PolicyFailure.refuse(reason, message, operation?, cause?)`. Their `_tag` stays `ReactorError`; their `name` now names the subclass ([3f48e19]).
- **Breaking:** zero-argument operations are `Effect` values rather than functions returning one: `Coordinator.Client.pricing`, and `Peer.stats` and `Peer.shutdown` in the host peer contract (`Peers.Peer`). A custom `Peer` exposes `stats`, and `shutdown` when it has one, as values ([6291dc5]).
- **Breaking:** `ClipRequest`'s `durationSeconds`, `seed` and `position`, `Coordinator.Inspection`'s `observedAt` and `additional` values, and `ErrorContext`'s `status` and `retryAfterMs` are `Schema.Finite`, so NaN and infinities neither decode nor construct ([6291dc5]).
- **Breaking:** `reactor-effect-native` loads only an ABI 3 native library and rejects an ABI 2 library, including the 0.2.0 one passed as `libraryPath`. Decoded media now reaches JavaScript through bounded native queues drained with nonblocking takes, off the libuv thread pool, and every peer in a process shares one libwebrtc factory ([90f3b05]).
- **Breaking:** `ErrorCode` gains `SdpRejected`, `IceFailed`, `TransportFailed` and `ChannelClosed`, so an exhaustive match over it must handle them. A data channel that closes now fails the connection as `ChannelClosed` rather than `Disconnected`, and a failed native connection reports `IceFailed` or `TransportFailed` from its statistics ([90f3b05]).
- **Breaking (`/host` only):** `CoordinatorClient.create` resolves with the allocated session id and the reply, and the new `describe` decodes the session descriptor from it. `/host` serves the first-party host packages, which pin the exact client version.
- The three `ReactorError` messages that repeated provider text now carry a message written by the library: a data or control command's `Remote` error reads `remote command error <code>`, and a failed clip request reads `clip failed`. The provider's text moves to `context.body`, which diagnostic serialization excludes, so code that read it from `message` reads `context.body`, and routes on `code` and `context.remoteCode`. A disabled recorder is still reported as `RecorderDisabled`, recognized from the clip failure's text; it is the one documented exception to never classifying provider text, until the wire carries a code for it.
- `reactor-effect-browser`: a `play()` timeout's message reads `media play deadline` instead of `media read/copy/play deadline`; its code is still `Timeout`.
- `reactor-effect-native`: the Rust library no longer panics outside tests, where a panic in a libwebrtc callback would abort the host process, and checks every pointer's alignment and length bound before touching caller memory ([efd9e51]).

### Fixed

- A create reply that names its session but fails validation later, for example with an unknown track kind or malformed capabilities, no longer orphans the paid session. The session is recorded as owned from its id, the acquisition fails with a `Protocol` `AcquisitionFailure` whose `context.sessionId` names it, and cleanup deletes the session and confirms its termination (`cleanup.allocation` is `"known"`).
- JSON validation of caller input (command data, `extraArgs`, orchestration clip requests, H3 command data and `/wire`'s `structFromObject`) enforces its documented bounds. Object keys count toward the 4 MiB text budget; array holes and index accessors are rejected, and an accessor never runs; an array's length is charged against the node budget before any slot is read, so a sparse array with 100 million slots fails in milliseconds instead of passing after seconds. Each violation is still a typed failure that was not submitted. An array's non-index own keys are ignored, as `JSON.stringify` ignores them.
- An H3 refusal (`command_error`) keeps the provider's reason in `context.body` instead of dropping it.
- `reactor-effect-browser`: the deadlines for resuming and closing the `AudioContext` in `audioContext()` and for starting playback in `play()` run on Effect's `Clock` instead of a wall-clock `setTimeout`, so `TestClock` controls them. Under the live clock they behave as before.

## [0.2.0] - 2026-09-23

### Added

- First publication of the three packages, built from commit [eb0db13] and published to npm with SLSA provenance by the release workflow.
- `reactor-effect-client`: the canonical `Client` and scoped `Session` (allocation or attachment, commands, connection generations and cleanup evidence in `CloseReport`), coordinator operations (pricing, token minting, inspection, termination), the H3 provider (`/h3`), orchestration and renewal (`/orchestration`), an offline simulation (`/simulation`), test utilities (`/testing`), the generated wire codec (`/wire`) and the `/host` surface for host packages. It runs on Node 22 or newer, Bun and browsers, with Effect `4.0.0-rc.115`.
- `reactor-effect-browser`: an `RTCPeerConnection` host with generation-scoped tracks, media conversion and recording.
- `reactor-effect-native`: a libwebrtc bridge in Rust, loaded through Koffi (native ABI 2), with decoded media, file upload and staged libraries for linux-x64 and darwin-arm64, on Node and Bun.

[unreleased]: https://github.com/mannyc2/reactor-effect-client/compare/eb0db13b9b5f17b6f01e31b8ceca00dd787e9149...main
[0.2.0]: https://github.com/mannyc2/reactor-effect-client/tree/eb0db13b9b5f17b6f01e31b8ceca00dd787e9149
[eb0db13]: https://github.com/mannyc2/reactor-effect-client/commit/eb0db13b9b5f17b6f01e31b8ceca00dd787e9149
[3f48e19]: https://github.com/mannyc2/reactor-effect-client/commit/3f48e19a095eee8e9d538390fd4b6dfa8d5431e2
[6291dc5]: https://github.com/mannyc2/reactor-effect-client/commit/6291dc5ca27ea0111d76a4a22e40d04d64bc4498
[90f3b05]: https://github.com/mannyc2/reactor-effect-client/commit/90f3b05541277d9ca1ef290bed173e77bb2c48d8
[efd9e51]: https://github.com/mannyc2/reactor-effect-client/commit/efd9e514b702771be7fc40dc8447a5aecb09a1bd
