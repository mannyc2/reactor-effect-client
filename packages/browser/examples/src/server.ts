// NodeHttpServer serves on a server made by Node's own http module.
// @effect-diagnostics-next-line nodeBuiltinImport:off
import { createServer } from "node:http";
import * as NodeHttpServer from "@effect/platform-node/NodeHttpServer";
import * as NodeRuntime from "@effect/platform-node/NodeRuntime";
import {
  Config,
  Duration,
  Effect,
  FileSystem,
  Layer,
  Option,
  Path,
  Redacted,
  Stream,
} from "effect";
import * as Model from "effect/ai/Model";
import { HttpRouter, HttpServerResponse } from "effect/http";
import { HttpApiBuilder } from "effect/http-api";
import * as OpenRouterSpeech from "reactor-effect-client/OpenRouterSpeech";
import * as CoordinatorClient from "reactor-effect-client/CoordinatorClient";
import * as H3 from "reactor-effect-client/H3";
import type { AudioFrame } from "reactor-effect-client/Media";
import { SpeechError, SpeechModel } from "reactor-effect-client/SpeechModel";
import { Api, SpeechUnavailable, TokenUnavailable } from "./Api.ts";
import type { Character } from "./Api.ts";
import * as FetchHttpClient from "effect/http/FetchHttpClient";

/**
 * A token that creates a session caps it at two minutes, enough for a demo.
 * Reactor bills a session per second from ready until it ends, so a session
 * its page could not close, after a crash or a lost connection, bills no
 * longer than that.
 */
const sessionCap = Duration.minutes(2);

/**
 * Mints one token per request. A bound token lets a session go on past its
 * first token. A real deployment authenticates and rate-limits this endpoint,
 * and binds only sessions the caller created: every token it hands out can
 * start or act on a paid session. Without `REACTOR_API_KEY` it mints nothing,
 * and the page runs offline.
 */
const SessionHandlers = HttpApiBuilder.group(
  Api,
  "session",
  Effect.fn(function* (handlers) {
    const apiKey = yield* Config.Redacted("REACTOR_API_KEY").pipe(Config.option);
    const apiUrl = yield* Config.String("REACTOR_API_URL").pipe(
      Config.withDefault(CoordinatorClient.defaultApiUrl),
    );
    const unavailable = TokenUnavailable.make({ message: "No session token is available" });
    if (Option.isNone(apiKey)) {
      yield* Effect.logInfo("REACTOR_API_KEY is not set: the page runs offline");
      return handlers.handleAll({
        live: () => Effect.succeed({ _tag: "Unavailable" } as const),
        token: () => Effect.fail(unavailable),
      });
    }
    const coordinator = yield* CoordinatorClient.make({ apiUrl });
    const tokens = coordinator.tokens({
      apiKey: apiKey.value,
      modelName: H3.modelName,
      maxSessionDuration: sessionCap,
      expiresAfter: "6 minutes",
    });
    return handlers.handleAll({
      live: () =>
        Effect.succeed({
          _tag: "Available",
          maxSessionSeconds: Duration.toSeconds(sessionCap),
        } as const),
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
          Effect.mapError(() => unavailable),
        ),
    });
  }),
);

/**
 * Where the character page's Live2D files come from. None is in this
 * repository: the Cubism SDK for Web is linked at `cubism/` under its own
 * licence, `bun run build:live2d` builds the adapter against it, and
 * `LIVE2D_MODEL` names a `.model3.json` the person running it may use.
 */
const live2d = Effect.gen(function* () {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const root = path.join(path.dirname(yield* path.fromFileUrl(new URL(import.meta.url))), "..");
  const model = yield* Config.String("LIVE2D_MODEL").pipe(Config.option);
  const files = {
    core: path.join(root, "cubism/Core/live2dcubismcore.min.js"),
    shaders: path.join(root, "cubism/Framework/Shaders/WebGL"),
    adapter: path.join(root, "dist/live2d/adapter.js"),
    model: Option.map(model, (file) => path.resolve(file)),
  };
  const unavailable = (missing: string): Character["avatar"] => ({ _tag: "Unavailable", missing });
  /** The model's address, or the first piece the avatar still needs. */
  const avatar = Effect.gen(function* () {
    if (!(yield* fs.exists(files.core)))
      return unavailable("the Cubism SDK for Web, linked at packages/browser/examples/cubism");
    if (!(yield* fs.exists(files.adapter)))
      return unavailable("the Live2D adapter: run `bun run build:live2d`");
    if (Option.isNone(files.model)) return unavailable("LIVE2D_MODEL, naming a .model3.json");
    if (!(yield* fs.exists(files.model.value)))
      return unavailable(`the model LIVE2D_MODEL names, ${files.model.value}`);
    const name = encodeURIComponent(path.basename(files.model.value));
    return { _tag: "Available", model: `/live2d/model/${name}` } as const;
  }).pipe(Effect.orElseSucceed(() => unavailable("a readable Live2D setup")));
  return { files, avatar };
});

/** One block's samples as signed 16-bit little-endian bytes. */
const littleEndian = (frame: AudioFrame): Uint8Array => {
  const bytes = new Uint8Array(frame.samples.length * 2);
  const view = new DataView(bytes.buffer);
  for (const [index, sample] of frame.samples.entries()) view.setInt16(index * 2, sample, true);
  return bytes;
};

/**
 * The character page's speech and setup. Each request streams one utterance
 * from the speech model this server was given, so the provider's key never
 * leaves it. The page disconnecting interrupts the stream, which releases the
 * request here; it does not prove the provider stopped generating or billing.
 */
const CharacterHandlers = HttpApiBuilder.group(
  Api,
  "character",
  Effect.fn(function* (handlers) {
    const speech = Option.all({
      model: yield* Effect.serviceOption(SpeechModel),
      provider: yield* Effect.serviceOption(Model.ProviderName),
      name: yield* Effect.serviceOption(Model.ModelName),
    });
    if (Option.isNone(speech))
      yield* Effect.logInfo("OPENROUTER_API_KEY is not set: the character page has no speech");
    const { avatar } = yield* live2d;
    const unavailable = SpeechUnavailable.make({ message: "No speech provider is configured" });
    return handlers.handleAll({
      character: () =>
        Effect.map(avatar, (avatar) => ({
          speech: Option.match(speech, {
            onNone: () => ({ _tag: "Unavailable" }) as const,
            onSome: ({ provider, name }) => ({ _tag: "Available", provider, model: name }) as const,
          }),
          avatar,
        })),
      speak: ({ payload }) =>
        Option.match(speech, {
          onNone: () => Effect.fail(unavailable),
          onSome: ({ model }) =>
            Effect.succeed(
              model.stream({ text: payload.text }).pipe(
                Stream.map((frame) => ({
                  sampleRate: frame.sampleRate,
                  channels: frame.channels,
                  pcm: littleEndian(frame),
                })),
                Stream.tapError((error) =>
                  Effect.logWarning("speech failed", {
                    reason: error.reason._tag,
                    outcome: error.outcome,
                  }),
                ),
                // The provider's own diagnostics stay on this server.
                Stream.mapError((error) =>
                  SpeechError.make({ reason: error.reason, outcome: error.outcome }),
                ),
              ),
            ),
        }),
    });
  }),
);

/**
 * OpenRouter speaks when `OPENROUTER_API_KEY` is set, with an explicit speech
 * model and voice; without the key the character page plays local audio.
 */
const Speech = Layer.unwrap(
  Effect.map(Config.Redacted("OPENROUTER_API_KEY").pipe(Config.option), (key) =>
    Option.isSome(key) ? OpenRouterSpeech.layerConfig : Layer.empty,
  ),
);

/** The pages and their bundles (`bun run build` writes dist/). */
const Page = HttpRouter.use(
  Effect.fn(function* (router) {
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const root = path.join(path.dirname(yield* path.fromFileUrl(new URL(import.meta.url))), "..");
    const notFound = (message: string) => HttpServerResponse.text(message, { status: 404 });
    const file = (file: string, message = "Not found") =>
      HttpServerResponse.file(file).pipe(Effect.orElseSucceed(() => notFound(message)));
    /** A file under `directory`, named by the route's wildcard, and nothing outside it. */
    const within = (directory: string) =>
      Effect.flatMap(HttpRouter.params, (params) => {
        const target = path.resolve(directory, params["*"] ?? "");
        return path.relative(directory, target).startsWith("..")
          ? Effect.succeed(notFound("Not found"))
          : file(target);
      });
    yield* router.add(
      "GET",
      "/",
      HttpServerResponse.html(yield* fs.readFileString(path.join(root, "web/index.html"))),
    );
    yield* router.add(
      "GET",
      "/app.js",
      file(path.join(root, "dist/app.js"), "Run `bun run build` first."),
    );
    yield* router.add(
      "GET",
      "/character",
      HttpServerResponse.html(yield* fs.readFileString(path.join(root, "web/character.html"))),
    );
    yield* router.add(
      "GET",
      "/character.js",
      file(path.join(root, "dist/character.js"), "Run `bun run build` first."),
    );
    const { files } = yield* live2d;
    yield* router.add("GET", "/live2d/core.js", file(files.core));
    yield* router.add("GET", "/live2d/adapter.js", file(files.adapter));
    yield* router.add("GET", "/live2d/shaders/*", within(files.shaders));
    if (Option.isSome(files.model))
      yield* router.add("GET", "/live2d/model/*", within(path.dirname(files.model.value)));
  }),
);

HttpRouter.serve(
  Layer.mergeAll(
    HttpApiBuilder.layer(Api).pipe(
      Layer.provide([SessionHandlers, CharacterHandlers.pipe(Layer.provide(Speech))]),
    ),
    Page,
  ),
).pipe(
  Layer.provide(FetchHttpClient.layer),
  // Local only: anyone who can reach this server can start paid sessions and paid speech.
  Layer.provide(
    NodeHttpServer.layerConfig(createServer, {
      port: Config.Port("PORT").pipe(Config.withDefault(3000)),
      host: Config.String("HOST").pipe(Config.withDefault("127.0.0.1")),
    }),
  ),
  Layer.launch,
  NodeRuntime.runMain,
);
