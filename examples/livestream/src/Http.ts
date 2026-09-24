import { Effect, FileSystem, Layer, Option, Path, Stream } from "effect";
import { HttpRouter, HttpServerResponse } from "effect/unstable/http";
import { HttpApiBuilder, HttpApiScalar } from "effect/unstable/httpapi";
import * as Orchestration from "reactor-effect-client/orchestration";
import { Api, ChannelStatus, OffAir } from "./Api.ts";
import type { ChannelEvent, ClipSummary } from "./Api.ts";
import { Broadcast } from "./Broadcast.ts";
import { Programme } from "./Programme.ts";
import { Settings } from "./Settings.ts";

/** A clip as viewers see it: its identity and, when this server authored it, its prompt. */
const summary = (record: Orchestration.ClipRecord): ClipSummary => ({
  clipId: record.clipId,
  prompt: record.request?.prompt ?? null,
});

/** The clips that have not started, across every source the orchestration holds. */
const upcoming = (state: Orchestration.EngineState): readonly Orchestration.ClipRecord[] => [
  ...Option.match(state.building, { onNone: () => [], onSome: (building) => [building.record] }),
  ...state.queued,
  ...state.ready,
];

const playing = (state: Orchestration.EngineState): Option.Option<Orchestration.ClipRecord> =>
  Option.flatMap(state.playing, (value) => value.record);

const mediaEvent = (state: Orchestration.MediaState): ChannelEvent => ({
  _tag: "Media",
  state: state._tag,
  session: "sessionId" in state ? state.sessionId : null,
});

/**
 * A handle event as a channel event. A clip failure carries no reason: an H3
 * `clip_failed` reason is provider text, which the channel does not repeat.
 */
const toChannelEvent = Effect.fnUntraced(function* (
  engine: Orchestration.EngineShape,
  event: Orchestration.HandleEvent,
): Effect.fn.Return<ChannelEvent> {
  switch (event._tag) {
    case "Engine": {
      const inner = event.event;
      if (inner._tag === "Starved") return { _tag: "Starved" };
      if (inner._tag === "SessionFailed") return { _tag: "OffAir", reason: inner.failure.message };
      const state = yield* engine.state;
      const record = [...upcoming(state), ...Option.toArray(playing(state))].find(
        (value) => value.clipId === inner.clipId,
      );
      return {
        _tag: "Clip",
        clipId: inner.clipId,
        phase: inner._tag,
        prompt: record?.request?.prompt ?? null,
      };
    }
    case "Renewal": {
      const renewal = event.event;
      return {
        _tag: "Renewal",
        phase: renewal._tag,
        session: "sessionId" in renewal ? renewal.sessionId : null,
        lostClips: renewal._tag === "Replaced" ? renewal.lostClips : null,
      };
    }
    case "Media":
      return mediaEvent(event.state);
  }
});

/** Prompts in; status and the handle's events out. Failures are the contract's errors. */
const ChannelHandlers = HttpApiBuilder.group(
  Api,
  "channel",
  Effect.fn(function* (handlers) {
    const programme = yield* Programme;
    const handle = yield* Orchestration.Handle;
    const { mode } = yield* Settings;

    const status = Effect.gen(function* () {
      const state = yield* handle.engine.state;
      // Loss totals that cannot be read fail rather than read as zero.
      const pressure = yield* Effect.option(handle.media.pressure);
      return new ChannelStatus({
        mode,
        session: Option.getOrNull(yield* handle.sessionId),
        media: (yield* handle.mediaState)._tag,
        playing: Option.getOrNull(Option.map(playing(state), summary)),
        upcoming: upcoming(state).map(summary),
        loss: Option.getOrNull(
          Option.map(pressure, (value) => ({
            droppedVideo: Number(value.droppedVideo),
            droppedAudio: Number(value.droppedAudio),
            readerOverflows: Number(value.readerOverflows),
          })),
        ),
      });
    });

    // The handle's observation: its current state, then every change, with
    // nothing lost between them. An observer that falls behind ends its
    // stream; the browser's EventSource reconnects and observes again.
    const events = Stream.unwrap(
      Effect.gen(function* () {
        const observation = yield* handle.observe();
        const { engine, media } = observation.initial;
        const initial: readonly ChannelEvent[] = [
          mediaEvent(media),
          ...Option.toArray(playing(engine)).map((record): ChannelEvent => ({
            _tag: "Clip",
            phase: "Started",
            ...summary(record),
          })),
        ];
        return Stream.concat(
          Stream.fromIterable(initial),
          observation.events.pipe(
            Stream.mapEffect((event) => toChannelEvent(handle.engine, event)),
          ),
        );
      }),
    ).pipe(Stream.catch(() => Stream.empty));

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
