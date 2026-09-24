import { Effect, Layer } from "effect";
import { HttpRouter } from "effect/unstable/http";
import * as Reactor from "reactor-effect-client";
import * as Orchestration from "reactor-effect-client/orchestration";
import { Broadcast } from "./Broadcast.ts";
import * as Channel from "./Channel.ts";
import { Routes } from "./Http.ts";
import { Ledger } from "./Ledger.ts";
import { Programme } from "./Programme.ts";
import { Settings } from "./Settings.ts";

/**
 * Closes the orchestration first on shutdown. This layer is built after the
 * layers it depends on, so its finalizer runs before theirs: the sessions,
 * paid ones included, are terminated before the encoder, the drains and the
 * rest are torn down, and the cleanup report goes to the ledger. The
 * orchestration's own release then finds it closed and reuses that report.
 *
 * It also closes the orchestration as soon as it fails for good (a renewal
 * it could not open, past `CHANNEL_MAX_SESSIONS` for one): its last session
 * would otherwise keep running, and billing, off air until it expired.
 */
const CloseFirst = Layer.effectDiscard(
  Effect.gen(function* () {
    const handle = yield* Orchestration.Handle;
    const ledger = yield* Ledger;
    const close = yield* Effect.cached(handle.close.pipe(Effect.flatMap(ledger.closed)));
    yield* handle.engine.failure.pipe(
      Effect.flatMap((failure) =>
        Effect.logWarning("channel off air; closing its sessions", { reason: failure.message }),
      ),
      Effect.andThen(close),
      Effect.forkScoped,
    );
    yield* Effect.addFinalizer(() => close);
  }),
);

/**
 * The channel's services: sources, scheduling, broadcast and evidence. What
 * can fail without cost (settings, the evidence directory, ffmpeg) is built
 * before the orchestration, which in live mode opens a paid session.
 */
export const App = CloseFirst.pipe(
  Layer.provideMerge(Layer.mergeAll(Programme.layer, Broadcast.layer)),
  Layer.provideMerge(Channel.layer),
  Layer.provideMerge(Layer.mergeAll(Ledger.layer, Settings.layer, Broadcast.preflight)),
  Layer.provide(Reactor.FetchHttp.layer),
);

/** The routes served over the app. The HTTP server itself is provided by the caller. */
export const Server = HttpRouter.serve(Routes).pipe(Layer.provide(App));
