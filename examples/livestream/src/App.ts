import { Effect, Layer } from "effect";
import { HttpRouter } from "effect/unstable/http";
import { Playout } from "reactor-effect-client";
import { Broadcast, ChannelMedia } from "./Broadcast.ts";
import * as Channel from "./Channel.ts";
import { Routes } from "./Http.ts";
import { Ledger } from "./Ledger.ts";
import { Programme } from "./Programme.ts";
import { Settings } from "./Settings.ts";

/** The broadcast reads the playout's continuous output. */
const Media = Layer.effect(
  ChannelMedia,
  Effect.gen(function* () {
    const playout = yield* Playout.Playout;
    return ChannelMedia.of({ video: playout.video, audio: playout.audio });
  }),
);

/**
 * The channel's services: the playout, the programme, the broadcast and the
 * evidence. What can fail without cost (settings, the evidence directory,
 * ffmpeg) is built before the playout, which in live mode opens a paid session.
 */
export const App = Layer.mergeAll(Programme.layer, Broadcast.layer.pipe(Layer.provide(Media))).pipe(
  Layer.provideMerge(Channel.layer),
  Layer.provideMerge(Layer.mergeAll(Ledger.layer, Settings.layer, Broadcast.preflight)),
);

/** The routes served over the app. The HTTP server itself is provided by the caller. */
export const Server = HttpRouter.serve(Routes).pipe(Layer.provide(App));
