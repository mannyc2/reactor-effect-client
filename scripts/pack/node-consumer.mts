import type * as Effect from "effect/Effect";
import type * as Redacted from "effect/Redacted";
import type * as Crypto from "effect/Crypto";
import type * as Scope from "effect/Scope";
import type * as Http from "effect/unstable/http/HttpClient";
import * as Root from "reactor-effect-client";
import * as H3 from "reactor-effect-client/h3";
import * as Orchestration from "reactor-effect-client/orchestration";
import * as Simulation from "reactor-effect-client/simulation";
import * as Testing from "reactor-effect-client/testing";
import * as Wire from "reactor-effect-client/wire";

declare const factory: Root.Factory;
const owner = factory.create({ model: "fixture/installed-consumer" });
const attached = factory.attach({ sessionId: "sess_fixture_existing" });
const adopted = factory.attach({ sessionId: "sess_fixture_existing", adopt: true });
declare const record: Orchestration.Allocation;
declare const token: Redacted.Redacted<string>;
const endsAt: number | undefined = record.endsAt;
const resumed: Effect.Effect<Orchestration.OpenedH3, Root.AcquisitionFailure, unknown> =
  Orchestration.resumeH3({ allocation: record, jwt: token, source: { holdLastFrame: true } });
declare const session: Root.Session;
// H3 composes through the public canonical Session without introducing
// filesystem/path requirements into portable provider construction.
const provider: Effect.Effect<
  H3.Provider,
  Root.ReactorError | Root.CommandFailure,
  Crypto.Crypto | Scope.Scope
> = H3.make(session);
const coordinator: Effect.Effect<Root.Coordinator.Client, Root.ReactorError, Http.HttpClient> =
  Root.Coordinator.make();
declare const simulated: Effect.Success<ReturnType<typeof Simulation.make>>;
const engine: Orchestration.EngineShape = simulated.engine;
declare const priorClip: Orchestration.ClipId;
const sameOwner = new Orchestration.ClipRequest({
  prompt: "Keep a dependent clip on its physical session",
  references: [],
  durationSeconds: 5,
  metadata: {},
  sameSessionAs: priorClip,
});
void engine.prepare(sameOwner);
// Reference audio travels beside an image, from bytes the /testing fixture writes.
const voiced = new Orchestration.ClipRequest({
  prompt: "Audio 1 is the host's voice",
  references: [{ uri: "file:///host.png" }],
  audio: [{ uri: "file:///voice.wav" }],
  durationSeconds: 5,
  metadata: {},
});
void engine.prepare(voiced);
const spoken: H3.Request = {
  prompt: "Audio 1 is the host's voice",
  references: [{ _tag: "Bytes", bytes: Testing.pngBytes(2, 2) }],
  audio: [{ _tag: "Bytes", bytes: Testing.wavBytes(3) }],
};
const audioBounds: number = H3.audioReferenceLimits.maxAudio;
const validatedAudio: Effect.Effect<H3.ValidatedAudioReference, Root.ReactorError> =
  H3.validateAudioReference({ _tag: "Bytes", bytes: Testing.wavBytes(3) });
const media: Orchestration.MediaShape = simulated.media;
const encoded = Wire.ControlClientMessage.encode({
  request_id: "fixture",
  kind: 1,
  payload: { case: "ping", value: {} },
});
const fixtureBytes: Uint8Array = Testing.pngBytes(2, 2);
void [
  Root.make,
  H3,
  Orchestration,
  Simulation,
  owner,
  attached,
  adopted,
  endsAt,
  resumed,
  provider,
  coordinator,
  engine,
  media,
  encoded,
  fixtureBytes,
  spoken,
  audioBounds,
  validatedAudio,
];
