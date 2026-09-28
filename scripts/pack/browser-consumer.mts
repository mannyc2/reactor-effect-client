/** The browser host's public API as a DOM consumer sees it through the installed declarations. */
import type * as Effect from "effect/Effect";
import type * as Layer from "effect/Layer";
import type * as Scope from "effect/Scope";
import { Peer, ReactorError, Session } from "reactor-effect-client";
import { BrowserMedia, BrowserPeer } from "reactor-effect-browser";
import * as BrowserPeerModule from "reactor-effect-browser/BrowserPeer";

const host: Layer.Layer<Peer.PeerFactory, ReactorError.ReactorError> = BrowserPeer.layer;
declare const session: Session.Session;
const tracks: Effect.Effect<BrowserMedia.Tracks, ReactorError.ReactorError> =
  BrowserMedia.tracks(session);
declare const media: BrowserMedia.Tracks;
const video: Effect.Effect<MediaStreamTrack, ReactorError.ReactorError, Scope.Scope> =
  media.track("main_video");
declare const element: HTMLVideoElement;
declare const track: MediaStreamTrack;
const played: Effect.Effect<void, ReactorError.ReactorError, Scope.Scope> = BrowserMedia.play(
  track,
  element,
);
const sameModule: typeof BrowserPeer.layer = BrowserPeerModule.layer;
void [host, tracks, video, played, sameModule];
