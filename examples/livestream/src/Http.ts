import { Effect, FileSystem, Layer, Path } from "effect";
import { HttpRouter, HttpServerResponse } from "effect/unstable/http";
import { HttpApiBuilder, HttpApiScalar } from "effect/unstable/httpapi";
import { Api, OffAir } from "./Api.ts";
import { Broadcast } from "./Broadcast.ts";
import { Monitor } from "./Monitor.ts";
import { Programme } from "./Programme.ts";

/** Prompts in; the channel's status out. Failures are the contract's errors. */
const ChannelHandlers = HttpApiBuilder.group(
  Api,
  "channel",
  Effect.fn(function* (handlers) {
    const programme = yield* Programme;
    const monitor = yield* Monitor;
    return handlers.handleAll({
      submit: ({ payload }) => programme.submit(payload.prompt),
      status: () => monitor.status,
      events: () => Effect.succeed(monitor.feed),
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
          Effect.mapError((error) => OffAir.make({ message: error.message })),
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
