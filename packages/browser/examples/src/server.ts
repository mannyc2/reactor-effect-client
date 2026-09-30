// NodeHttpServer serves on a server made by Node's own http module.
// @effect-diagnostics-next-line nodeBuiltinImport:off
import { createServer } from "node:http";
import * as NodeHttpServer from "@effect/platform-node/NodeHttpServer";
import * as NodeRuntime from "@effect/platform-node/NodeRuntime";
import { Config, Effect, FileSystem, Layer, Path, Redacted } from "effect";
import { HttpRouter, HttpServerResponse } from "effect/unstable/http";
import { HttpApiBuilder } from "effect/unstable/httpapi";
import * as CoordinatorClient from "reactor-effect-client/CoordinatorClient";
import * as H3 from "reactor-effect-client/H3";
import { Api, TokenUnavailable } from "./Api.ts";
import * as FetchHttpClient from "effect/unstable/http/FetchHttpClient";

/**
 * Mints one token per request. A token that creates a session caps it at five
 * minutes, so a page closed without cleanup leaves it running no longer than
 * that; Reactor bills it by the minute. A bound token lets that session go on
 * past its first token. A real deployment authenticates and rate-limits this
 * endpoint, and binds only sessions the caller created: every token it hands
 * out can start or act on a paid session.
 */
const SessionHandlers = HttpApiBuilder.group(
  Api,
  "session",
  Effect.fn(function* (handlers) {
    const apiKey = yield* Config.Redacted("REACTOR_API_KEY");
    const apiUrl = yield* Config.String("REACTOR_API_URL").pipe(
      Config.withDefault("https://api.reactor.inc"),
    );
    const coordinator = yield* CoordinatorClient.make({ apiUrl });
    const tokens = coordinator.tokens({
      apiKey,
      modelName: H3.modelName,
      maxSessionDuration: "5 minutes",
      expiresAfter: "6 minutes",
    });
    return handlers.handleAll({
      token: ({ payload }) =>
        (payload.session === undefined ? tokens.create : tokens.bind(payload.session)).pipe(
          Effect.map((grant) => ({
            jwt: Redacted.value(grant.jwt),
            expiresAt: grant.expiresAt,
            ...(grant.maxSessionSeconds === undefined
              ? {}
              : { maxSessionSeconds: grant.maxSessionSeconds }),
          })),
          Effect.tapError((error) =>
            Effect.logWarning("token refused", { reason: error.reason._tag }),
          ),
          Effect.mapError(() =>
            TokenUnavailable.make({ message: "No session token is available" }),
          ),
        ),
    });
  }),
);

/** The page and its bundle (`bun run build` writes dist/app.js). */
const Page = HttpRouter.use(
  Effect.fn(function* (router) {
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const root = path.join(path.dirname(yield* path.fromFileUrl(new URL(import.meta.url))), "..");
    yield* router.add(
      "GET",
      "/",
      HttpServerResponse.html(yield* fs.readFileString(path.join(root, "web/index.html"))),
    );
    yield* router.add(
      "GET",
      "/app.js",
      HttpServerResponse.file(path.join(root, "dist/app.js")).pipe(
        Effect.orElseSucceed(() =>
          HttpServerResponse.text("Run `bun run build` first.", { status: 404 }),
        ),
      ),
    );
  }),
);

HttpRouter.serve(
  Layer.mergeAll(HttpApiBuilder.layer(Api).pipe(Layer.provide(SessionHandlers)), Page),
).pipe(
  Layer.provide(FetchHttpClient.layer),
  // Local only: anyone who can reach this server can start paid sessions.
  Layer.provide(
    NodeHttpServer.layerConfig(createServer, {
      port: Config.Port("PORT").pipe(Config.withDefault(3000)),
      host: Config.String("HOST").pipe(Config.withDefault("127.0.0.1")),
    }),
  ),
  Layer.launch,
  NodeRuntime.runMain,
);
