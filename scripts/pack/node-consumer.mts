import type * as Effect from "effect/Effect";
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
declare const session: Root.Session;
// H3 composes through the public canonical Session without introducing
// filesystem/path requirements into portable provider construction.
const provider: Effect.Effect<H3.Provider, Root.ReactorError, Crypto.Crypto | Scope.Scope> =
  H3.make(session);
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
  provider,
  coordinator,
  engine,
  media,
  encoded,
  fixtureBytes,
];
