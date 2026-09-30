/**
 * The client's public API as a Node consumer without DOM types sees it through
 * the installed declarations: each module's main entry points, by type.
 */
import type * as Crypto from "effect/Crypto";
import type * as Effect from "effect/Effect";
import type * as Layer from "effect/Layer";
import type * as Scope from "effect/Scope";
import type * as Stream from "effect/Stream";
import type * as HttpClient from "effect/unstable/http/HttpClient";
import {
  CoordinatorClient,
  H3,
  H3Source,
  LocalSource,
  Media,
  Peer,
  Playout,
  Reactor,
  ReactorError,
  ReactorTest,
  Session,
} from "reactor-effect-client";
import * as PlayoutModule from "reactor-effect-client/Playout";

const coordinator: Layer.Layer<
  CoordinatorClient.CoordinatorClient,
  ReactorError.ReactorError,
  HttpClient.HttpClient
> = CoordinatorClient.layer();
const reactor: Layer.Layer<
  Reactor.Reactor,
  ReactorError.ReactorError,
  CoordinatorClient.CoordinatorClient | Peer.PeerFactory
> = Reactor.layer();
const simulated: Layer.Layer<ReactorTest.ReactorTest | HttpClient.HttpClient | Peer.PeerFactory> =
  ReactorTest.layer({ timing: ReactorTest.Timing.hosted });

declare const reactorService: Reactor.Reactor["Service"];
const created: Effect.Effect<Session.Session, ReactorError.AcquisitionFailure, Scope.Scope> =
  reactorService.create({ model: "helios" });
declare const session: Session.Session;
const closed: Effect.Effect<Session.CloseReport> = session.close;
const decoded: Effect.Effect<Media.DecodedMedia, ReactorError.ReactorError> = session.decoded;
const provider: Effect.Effect<
  H3.Provider,
  ReactorError.ReactorError | ReactorError.CommandFailure,
  Crypto.Crypto | Scope.Scope
> = H3.make(session);
const reference: Effect.Effect<H3.ValidatedAudioReference, ReactorError.ReactorError> =
  H3.validateAudioReference({ _tag: "Bytes", bytes: ReactorTest.wavBytes({ seconds: 3 }) });
const image: Uint8Array = ReactorTest.pngBytes({ width: 64, height: 64 });

declare const tokens: H3Source.OpenOptions["tokens"];
const source: Effect.Effect<
  Playout.Source,
  ReactorError.AcquisitionFailure,
  Reactor.Reactor | Crypto.Crypto | Scope.Scope
> = H3Source.open({ tokens });
declare const localOptions: LocalSource.Options;
const local = LocalSource.open(localOptions);
const playout: Layer.Layer<Playout.Playout, never, Reactor.Reactor | Crypto.Crypto> = Playout.layer(
  {
    open: H3Source.open({ tokens }),
    lanes: [{ name: "show" }, { name: "urgent", cut: true }],
  },
);
declare const service: Playout.Playout["Service"];
const handle: Effect.Effect<Playout.ItemHandle, Playout.SubmitError> = service.submit({
  key: Playout.ItemKey.make("opening"),
  lane: "show",
  request: { prompt: "A curtain rising on a painted forest", seconds: 5 },
});
const asRun: Stream.Stream<Playout.AsRunEvent> = service.asRun;
const video: Stream.Stream<Media.VideoFrame, ReactorError.ReactorError> = service.video;
const sameModule: typeof Playout.layer = PlayoutModule.layer;
void [coordinator, reactor, simulated, created, closed, decoded, provider, reference, image];
void [source, local, playout, handle, asRun, video, sameModule];
