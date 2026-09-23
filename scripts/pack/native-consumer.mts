import type * as Effect from "effect/Effect";
import type * as Crypto from "effect/Crypto";
import type * as Scope from "effect/Scope";
import type * as Http from "effect/unstable/http/HttpClient";
import * as Root from "reactor-effect-client";
import * as H3 from "reactor-effect-client/h3";
import * as Native from "reactor-effect-native";

type Factory = Effect.Success<ReturnType<typeof Native.make>>;
declare const factory: Factory;
const owner = factory.create({ model: "fixture/installed-consumer" });
const attached = factory.attach({ sessionId: "sess_fixture_existing" });
declare const session: Root.Session;
// The native host selects its peer while keeping the canonical Session contract
// published by reactor-effect-client; H3 composes over that same session.
const nativeFactory: Effect.Effect<Root.Factory, never, Http.HttpClient | Crypto.Crypto> =
  Native.make();
const provider: Effect.Effect<
  H3.Provider,
  Root.ReactorError | Root.CommandFailure,
  Crypto.Crypto | Scope.Scope
> = H3.make(session);
const nativeMedia: Effect.Effect<Native.MediaGeneration, Root.ReactorError> = Native.media(session);
declare const frame: Native.VideoFrame;
const nativeClock: bigint = frame.timestampMicros;
void [owner, attached, nativeFactory, provider, nativeMedia, nativeClock];
