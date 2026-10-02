/**
 * The Studio on hosted Reactor: a paid H3 session over the browser's own
 * WebRTC, on tokens the page's server mints, with its tracks played in media
 * elements.
 */
import { Effect, Layer, Redacted } from "effect";
import * as FetchHttpClient from "effect/http/FetchHttpClient";
import { HttpApiClient } from "effect/http-api";
import { BrowserMedia, BrowserPeer } from "reactor-effect-browser";
import * as CoordinatorClient from "reactor-effect-client/CoordinatorClient";
import * as Reactor from "reactor-effect-client/Reactor";
import { ReactorError } from "reactor-effect-client/ReactorError";
import type { Session } from "reactor-effect-client/Session";
import { Api } from "./Api.ts";
import { Stage } from "./Stage.ts";
import type { Services } from "./Stage.ts";
import { WebCrypto } from "./WebCrypto.ts";

/**
 * Tokens from the page's server, which holds the API key: one that may create
 * the session, then, before that expires, one bound to the session.
 */
const tokens = Effect.map(HttpApiClient.make(Api), (api): CoordinatorClient.Tokens => {
  const token = (session?: string) =>
    api.token({ payload: session === undefined ? {} : { session } }).pipe(
      Effect.map((reply) => ({
        jwt: Redacted.make(reply.jwt),
        expiresAt: reply.expiresAt,
        maxSessionSeconds: reply.maxSessionSeconds,
      })),
      Effect.mapError(() => ReactorError.fromCode("Http", "the server gave no session token")),
    );
  return { create: token(), bind: token };
});

const minutes = (seconds: number): string =>
  `${String(Math.floor(seconds / 60))}:${String(seconds % 60).padStart(2, "0")}`;

/** Plays one received track of the current generation in `element` until the generation ends. */
const present = Effect.fnUntraced(function* (
  session: Session,
  name: string,
  element: HTMLMediaElement,
) {
  const media = yield* BrowserMedia.tracks(session);
  yield* BrowserMedia.play(yield* media.track(name), element);
  return yield* media.retired;
});

const stage = (screen: HTMLElement, maxSessionSeconds: number) =>
  Layer.sync(Stage, () => {
    // The picture plays muted at once; sound waits for its own click, since
    // browsers refuse unmuted playback started long after the gesture that asked.
    const video = document.createElement("video");
    video.autoplay = true;
    // The attribute as well as the property: Safari reads the attribute for muted autoplay.
    video.defaultMuted = true;
    video.muted = true;
    video.playsInline = true;
    const audio = document.createElement("audio");
    screen.prepend(video, audio);
    return Stage.of({
      mode: "Live",
      about: `A real H3 session over this browser's WebRTC. Reactor bills it per second from ready until it ends, and the server's token caps it at ${minutes(maxSessionSeconds)}. The page never sees the API key.`,
      picture: (session) => present(session, "main_video", video),
      sound: (session) => present(session, "main_audio", audio),
      faults: [],
    });
  });

/**
 * The Studio's services on hosted Reactor. Building `BrowserPeer.layer` checks
 * for WebRTC, so an unsupported browser fails before any session is paid for.
 */
export const layer = (options: {
  readonly screen: HTMLElement;
  readonly maxSessionSeconds: number;
}): Layer.Layer<Services, ReactorError> =>
  Layer.mergeAll(
    Layer.unwrap(Effect.map(tokens, (tokens) => Reactor.layer({ tokens }))).pipe(
      Layer.provide(
        Layer.mergeAll(
          // The page talks to Reactor itself, with the tokens its server minted.
          CoordinatorClient.layer({ apiUrl: CoordinatorClient.defaultApiUrl }),
          BrowserPeer.layer,
        ),
      ),
    ),
    stage(options.screen, options.maxSessionSeconds),
    WebCrypto,
  ).pipe(Layer.provide(FetchHttpClient.layer));
