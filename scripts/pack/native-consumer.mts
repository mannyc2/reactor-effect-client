/** The native host's public API as a Node consumer sees it through the installed declarations. */
import type * as Layer from "effect/Layer";
import { Peer, ReactorError } from "reactor-effect-client";
import { NativePeer } from "reactor-effect-native";
import * as NativePeerModule from "reactor-effect-native/NativePeer";

const inProcess: Layer.Layer<Peer.PeerFactory, ReactorError.ReactorError> = NativePeer.layer();
const isolated: Layer.Layer<Peer.PeerFactory, ReactorError.ReactorError> = NativePeer.layerIsolated(
  { shutdownTimeout: "5 seconds" },
);
const sameModule: typeof NativePeer.layer = NativePeerModule.layer;
void [inProcess, isolated, sameModule];
