import { createServer } from "node:http";
import * as NodeHttpServer from "@effect/platform-node/NodeHttpServer";
import * as NodeRuntime from "@effect/platform-node/NodeRuntime";
import { Config, Layer } from "effect";
import { Server } from "./App.ts";

/**
 * `node src/main.ts` serves the channel on HOST:PORT (127.0.0.1:3000 by
 * default), offline unless CHANNEL_MODE=live. Anyone who can reach the server
 * can watch and send prompts, so it listens locally unless HOST says
 * otherwise. runMain turns SIGINT and SIGTERM into interruption, so the
 * layers release in order and every session is closed.
 *
 * The video and the event feed never end on their own, so the server does
 * not wait for open responses before it stops: waiting would only keep paid
 * sessions running. They are cut, and the sessions closed, at once.
 */
Server.pipe(
  Layer.provide(
    NodeHttpServer.layerConfig(createServer, {
      port: Config.Port("PORT").pipe(Config.withDefault(3000)),
      host: Config.String("HOST").pipe(Config.withDefault("127.0.0.1")),
      disablePreemptiveShutdown: Config.succeed(true),
    }),
  ),
  Layer.launch,
  NodeRuntime.runMain,
);
