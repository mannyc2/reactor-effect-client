/** The browser host's public API as a DOM consumer sees it through the installed declarations. */
import type * as Effect from "effect/Effect";
import type * as Layer from "effect/Layer";
import type * as Stream from "effect/Stream";
import type * as Scope from "effect/Scope";
import {
  AudioPlayback,
  Character,
  Media,
  Peer,
  ReactorError,
  Session,
} from "reactor-effect-client";
import { BrowserAudio, BrowserCharacter, BrowserMedia, BrowserPeer } from "reactor-effect-browser";
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

// Streaming generation and persistent characters compose without a Reactor session.
const audio: Effect.Effect<BrowserAudio.Playback, AudioPlayback.PlaybackError, Scope.Scope> =
  BrowserAudio.make;
declare const playback: BrowserAudio.Playback;
declare const renderer: Character.Renderer;
declare const canvas: HTMLCanvasElement;
const output: MediaStreamTrack = playback.track;
const animation: Effect.Effect<never, Character.CharacterError | AudioPlayback.PlaybackError> =
  BrowserCharacter.animate(renderer, playback.level);
const capture: Effect.Effect<MediaStreamTrack, Character.CharacterError, Scope.Scope> =
  BrowserCharacter.capture(canvas);
const playStream = <E, R>(
  frames: Stream.Stream<Media.AudioFrame, E, R>,
): Effect.Effect<void, E | AudioPlayback.PlaybackError, R> => playback.play(frames);
void [audio, output, animation, capture, playStream];
