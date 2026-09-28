import * as Effect from "effect/Effect";
import type * as Crypto from "effect/Crypto";
import type * as Layer from "effect/Layer";
import type * as Scope from "effect/Scope";
import * as Schema from "effect/Schema";
import * as Root from "reactor-effect-client";
import * as Browser from "reactor-effect-browser";
import * as H3 from "reactor-effect-client/H3";
import * as Orchestration from "reactor-effect-client/orchestration";
import * as Simulation from "reactor-effect-client/simulation";
import * as Testing from "reactor-effect-client/testing";
import * as Wire from "reactor-effect-client/wire";

declare const continuousOptions: Orchestration.ContinuousOptions;
const continuous: Effect.Effect<
  Orchestration.ContinuousHandleShape,
  Root.ReactorError | Root.AcquisitionFailure,
  Crypto.Crypto | Scope.Scope
> = Orchestration.makeContinuous(continuousOptions);
declare const continuousHandle: Orchestration.ContinuousHandleShape;
const continuousClose: Effect.Effect<Orchestration.CleanupSummary> = continuousHandle.close;
const continuousEngine: Orchestration.EngineShape = continuousHandle.engine;
const continuousSequences: Orchestration.HandleShape["sequences"] = continuousHandle.sequences;
const continuousCodec = Schema.toCodecJson(Orchestration.CleanupSummary);
void [continuous, continuousClose, continuousEngine, continuousSequences, continuousCodec];

declare const factory: Root.Factory;
const browserHost: Layer.Layer<Root.PeerFactory, Root.ReactorError> = Browser.layer;
declare const session: Root.Session;
const sameSession: Effect.Success<ReturnType<typeof factory.create>> = session;
const provider: Effect.Effect<
  H3.Provider,
  Root.ReactorError | Root.CommandFailure,
  Crypto.Crypto | Scope.Scope
> = H3.make(session);
const track = Effect.flatMap(Browser.media(session), (media) => media.track("video"));
const browserTrack: Effect.Effect<MediaStreamTrack, Root.ReactorError, Scope.Scope> = track;
void [Root.make, H3, Orchestration, Simulation, sameSession, provider, browserTrack, browserHost];
const fixtureBytes: Uint8Array = Testing.pngBytes(2, 2);
const fixtureUri: string = Testing.dataUri(fixtureBytes);
void [Wire.ControlClientMessage, fixtureUri];
