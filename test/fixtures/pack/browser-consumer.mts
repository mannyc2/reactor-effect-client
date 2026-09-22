import * as Effect from "effect/Effect";
import type * as Crypto from "effect/Crypto";
import type * as Scope from "effect/Scope";
import * as Root from "reactor-effect-client";
import * as Browser from "reactor-effect-client/browser";
import * as H3 from "reactor-effect-client/h3";
import * as Orchestration from "reactor-effect-client/orchestration";
import * as Simulation from "reactor-effect-client/simulation";

declare const factory: Effect.Success<ReturnType<typeof Browser.make>>;
declare const session: Root.Session;
const sameSession: Effect.Success<ReturnType<typeof factory.create>> = session;
const provider: Effect.Effect<H3.Provider, Root.ReactorError, Crypto.Crypto | Scope.Scope> =
  H3.make(session);
const track = Effect.flatMap(Browser.media(session), (media) => media.track("video"));
const browserTrack: Effect.Effect<MediaStreamTrack, Root.ReactorError, Scope.Scope> = track;
void [Root.make, H3, Orchestration, Simulation, sameSession, provider, browserTrack];
