import { Effect, FileSystem, Layer, Path, Stream } from "effect";
import { HttpRouter, HttpServerResponse } from "effect/unstable/http";
import { HttpApiBuilder, HttpApiScalar } from "effect/unstable/httpapi";
import { Playout } from "reactor-effect-client";
import { Api, ChannelStatus, OffAir } from "./Api.ts";
import type { ChannelEvent, ClipSummary } from "./Api.ts";
import { Broadcast } from "./Broadcast.ts";
import { Programme } from "./Programme.ts";
import { Settings } from "./Settings.ts";

/** As-run statuses as the page shows them; anything that did not air reads as failed. */
const phases: Record<
  Playout.AsRunStatus["_tag"],
  Extract<ChannelEvent, { _tag: "Clip" }>["phase"]
> = {
  Accepted: "Queued",
  Building: "Building",
  Ready: "Ready",
  Started: "Started",
  Ended: "Ended",
  Dropped: "Failed",
  Failed: "Failed",
  Unobserved: "Failed",
  Unknown: "Failed",
};

/** Prompts in; status and the playout's events out. Failures are the contract's errors. */
const ChannelHandlers = HttpApiBuilder.group(
  Api,
  "channel",
  Effect.fn(function* (handlers) {
    const programme = yield* Programme;
    const playout = yield* Playout.Playout;
    const { mode } = yield* Settings;

    const summary = (key: string): Effect.Effect<ClipSummary> =>
      Effect.map(programme.prompt(key), (prompt) => ({ clipId: key, prompt: prompt ?? null }));

    const status = Effect.gen(function* () {
      const state = yield* playout.state;
      const onAir = state.sessions.find((session) => session.role === "on-air");
      return new ChannelStatus({
        mode,
        session: onAir?.sessionId ?? null,
        media: onAir === undefined ? "Recovering" : "Ready",
        playing: typeof state.playing === "string" ? yield* summary(state.playing) : null,
        upcoming: yield* Effect.forEach(
          state.lanes.flatMap((lane) => lane.keys),
          summary,
        ),
        loss: null,
      });
    });

    // Every event from now on. A clip failure carries no reason: an H3
    // `clip_failed` reason is provider text, which the channel does not repeat.
    const events = playout.events.pipe(
      Stream.mapEffect(
        Effect.fnUntraced(function* (
          event: Playout.Event,
        ): Effect.fn.Return<ReadonlyArray<ChannelEvent>> {
          switch (event._tag) {
            case "AsRun": {
              const { key, status: asRun } = event.event;
              return [
                {
                  _tag: "Clip",
                  phase: phases[asRun._tag],
                  ...(yield* summary(key)),
                } satisfies ChannelEvent,
              ];
            }
            case "Session": {
              const session = event.event;
              return [
                {
                  _tag: "Renewal",
                  phase: session._tag,
                  session:
                    session._tag === "Opened"
                      ? session.sessionId
                      : session._tag === "SetupFailed"
                        ? null
                        : session.from,
                  lostClips: session._tag === "Replaced" ? session.carried : null,
                } satisfies ChannelEvent,
              ];
            }
            case "Starved":
              return [{ _tag: "Starved" } satisfies ChannelEvent];
            case "Cue":
              return [];
          }
        }),
      ),
      Stream.flatMap((values) => Stream.fromIterable(values)),
    );

    return handlers.handleAll({
      submit: ({ payload }) => programme.submit(payload.prompt),
      status: () => status,
      events: () => Effect.succeed(events),
    });
  }),
);

/** Every viewer shares the one encoder; a response ends when its viewer leaves. */
const MediaHandlers = HttpApiBuilder.group(
  Api,
  "media",
  Effect.fn(function* (handlers) {
    const broadcast = yield* Broadcast;
    return handlers.handleAll({
      live: () =>
        broadcast.onAir.pipe(
          Effect.as(broadcast.viewer),
          Effect.mapError((error) => new OffAir({ message: error.message })),
        ),
    });
  }),
);

/** The page, the one route outside the API. */
const Page = HttpRouter.use(
  Effect.fn(function* (router) {
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const here = path.dirname(yield* path.fromFileUrl(new URL(import.meta.url)));
    const page = yield* fs.readFileString(path.join(here, "../web/index.html"));
    yield* router.add("GET", "/", HttpServerResponse.html(page));
  }),
);

export const Routes = Layer.mergeAll(
  HttpApiBuilder.layer(Api, { openapiPath: "/openapi.json" }).pipe(
    Layer.provide([ChannelHandlers, MediaHandlers]),
  ),
  HttpApiScalar.layer(Api, { path: "/docs" }),
  Page,
);
