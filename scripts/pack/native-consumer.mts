import type * as Effect from "effect/Effect";
import type * as Crypto from "effect/Crypto";
import type * as Scope from "effect/Scope";
import type * as Layer from "effect/Layer";
import * as Root from "reactor-effect-client";
import * as H3 from "reactor-effect-client/h3";
import * as Native from "reactor-effect-native";

declare const factory: Root.Factory;
const owner = factory.create({ model: "fixture/installed-consumer" });
const attached = factory.attach({ sessionId: "sess_fixture_existing" });
declare const session: Root.Session;
// The native host supplies the PeerFactory for the canonical Session contract
// published by reactor-effect-client, and building it is the host preflight;
// H3 composes over that same session.
const nativeHost: Layer.Layer<Root.PeerFactory, Root.ReactorError> = Native.layer();
const provider: Effect.Effect<
  H3.Provider,
  Root.ReactorError | Root.CommandFailure,
  Crypto.Crypto | Scope.Scope
> = H3.make(session);
const nativeMedia: Effect.Effect<Native.MediaGeneration, Root.ReactorError> = Native.media(session);
declare const frame: Native.VideoFrame;
const nativeClock: bigint = frame.timestampMicros;
const nativeFormat: Native.VideoFormat = frame.format;
void [owner, attached, nativeHost, provider, nativeMedia, nativeClock, nativeFormat];
