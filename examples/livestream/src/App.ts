import { Effect, Layer } from "effect";
import { HttpRouter } from "effect/http";
import { Playout } from "reactor-effect-client";
import { Broadcast, ChannelMedia } from "./Broadcast.ts";
import * as Channel from "./Channel.ts";
import { Routes } from "./Http.ts";
import { Ledger } from "./Ledger.ts";
import { Monitor } from "./Monitor.ts";
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
 * The channel's services: the playout, the programme, the broadcast, the
 * monitor and the evidence. What can fail without cost (settings, the evidence
 * directory, ffmpeg and its ingest protocol) is built before the playout, which
 * in live mode opens a paid session.
 */
export const App = Monitor.layer.pipe(
  Layer.provideMerge(Layer.mergeAll(Programme.layer, Broadcast.layer.pipe(Layer.provide(Media)))),
  Layer.provideMerge(Channel.layer),
  Layer.provideMerge(
    Layer.mergeAll(Ledger.layer, Broadcast.preflight).pipe(Layer.provideMerge(Settings.layer)),
  ),
);

/** The routes served over the app. The HTTP server itself is provided by the caller. */
export const Server = HttpRouter.serve(Routes).pipe(Layer.provide(App));
