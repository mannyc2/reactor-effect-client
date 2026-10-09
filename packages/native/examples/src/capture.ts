import * as NodeRuntime from "@effect/platform-node/NodeRuntime";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { Config, Console, Deferred, Effect, Fiber, Layer, Option, Stream } from "effect";
import { Command, Flag } from "effect/cli";
import * as Reactor from "reactor-effect-client/Reactor";
import * as CoordinatorClient from "reactor-effect-client/CoordinatorClient";
import * as H3 from "reactor-effect-client/H3";
import * as References from "reactor-effect-client/References";
import { NativePeer } from "reactor-effect-native";
import { toMp4 } from "./Recording.ts";
import * as FetchHttpClient from "effect/http/FetchHttpClient";

/** The session's cap: a token for 90 seconds bounds what one capture can cost. */
const sessionSeconds = 90;
/**
 * The seconds a capped session can be billed, counted in the unit Reactor's
 * pricing states the rate in: every second for a rate per second, each
 * started minute whole for a rate per minute.
 */
const billedSeconds = (rate: CoordinatorClient.Rate) =>
  rate.per === "second" ? sessionSeconds : Math.ceil(sessionSeconds / 60) * 60;

/**
 * One clip, captured on this machine: a paid H3 session over the native
 * host, the clip's own frames and audio decoded here, and an MP4 written
 * from them. The session is created, used and closed in one scope; closing
 * it terminates the paid session and reports whether that was confirmed.
 */
const record = Effect.fn("record")(function* (options: {
  readonly grant: CoordinatorClient.TokenGrant;
  readonly prompt: string;
  readonly seconds: number;
  readonly references: ReadonlyArray<H3.ValidatedReference>;
  readonly out: string;
}) {
  const reactor = yield* Reactor.Reactor;
  // The token outlives the 90-second session, so it is never refreshed.
  const session = yield* reactor.create({
    model: H3.modelName,
    tokens: CoordinatorClient.fixedTokens(options.grant),
  });
  yield* Console.log(`session ${session.id} connected`);
  // However the capture ends (done, failed or interrupted), close the session
  // and say whether its termination was confirmed. The scope's own release
  // then finds it closed and reuses the same report.
  yield* Effect.addFinalizer(() =>
    session.close.pipe(
      Effect.flatMap((report) =>
        Console.log(
          report.remote.confirmed
            ? "session terminated"
            : "session termination NOT confirmed: check it before it bills further",
        ),
      ),
    ),
  );
  const provider = yield* H3.make(session);
  // H3 changes no playback policy on its own: ask it to play the clip when it is ready.
  yield* provider.setAutoplay(true);
  const media = yield* session.decoded;
  const withAudio = media.tracks.some(
    (track) => track.name === "main_audio" && track.direction === "recvonly",
  );

  // The readers start now, before the clip can play, and keep what arrives
  // between the clip's start and its end, as its operation reports them.
  const started = yield* Deferred.make<void>();
  const ended = yield* Deferred.make<void>();
  const clipOnly = <A, E>(stream: Stream.Stream<A, E>) =>
    stream.pipe(
      Stream.dropUntilEffect(() => Deferred.isDone(started)),
      Stream.haltWhen(Deferred.await(ended)),
    );
  const writing = yield* toMp4({
    path: options.out,
    video: clipOnly(media.video("main_video")),
    audio: withAudio ? Option.some(clipOnly(media.audio("main_audio"))) : Option.none(),
  }).pipe(Effect.forkScoped({ startImmediately: true }));

  const submission = yield* provider.prepare({
    prompt: options.prompt,
    seconds: options.seconds,
    references: options.references,
  });
  const acceptance = yield* submission.submit;
  yield* Console.log(`clip ${acceptance.clip.clip_id} accepted`);
  const operation = yield* provider.operation(submission);
  yield* operation.reached("generated");
  yield* Console.log("generated");
  yield* operation.reached("started");
  yield* Deferred.succeed(started, undefined);
  yield* Console.log("playing; recording");
  yield* operation.ended;
  yield* Deferred.succeed(ended, undefined);

  const written = yield* Fiber.join(writing);
  const pressure = yield* media.pressure;
  yield* Console.log(
    `wrote ${options.out}: ${written.frames} frames, ${written.filledFrames} filled from ` +
      `dropped ones, ${written.filledBlocks} audio blocks filled with silence ` +
      `(host dropped ${pressure.droppedVideo} frames and ${pressure.droppedAudio} blocks in all)`,
  );
}, Effect.scoped);

const capture = Command.make(
  "capture",
  {
    prompt: Flag.String("prompt").pipe(Flag.withDescription("What the clip shows")),
    seconds: Flag.Finite("seconds").pipe(
      Flag.withDescription("The clip's length, 5 to 15 seconds"),
      Flag.withDefault(8),
    ),
    reference: Flag.File("reference", { mustExist: true }).pipe(
      Flag.withDescription("A reference image the clip starts from (PNG, JPEG or WebP)"),
      Flag.optional,
    ),
    out: Flag.String("out").pipe(
      Flag.withDescription("Where to write the MP4"),
      Flag.withDefault("clip.mp4"),
    ),
    isolated: Flag.Boolean("isolated").pipe(
      Flag.withDescription("Run the native peer in a child process of its own (Node only)"),
      Flag.withDefault(false),
    ),
  },
  Effect.fn(function* ({ prompt, seconds, reference, out }) {
    // An image H3 would refuse, or one larger than a session uploads, is refused here, before a
    // token is minted or a session allocated.
    const references = yield* Option.match(reference, {
      onNone: () => Effect.succeed([]),
      onSome: (path) => Effect.map(References.image(References.file(path)), (image) => [image]),
    });
    const apiKey = yield* Config.Redacted("REACTOR_API_KEY");
    const coordinator = yield* CoordinatorClient.CoordinatorClient;
    const rate = yield* CoordinatorClient.modelRate(yield* coordinator.pricing, H3.modelName);
    const billed = billedSeconds(rate);
    yield* Console.log(
      `at most ${((billed * rate.creditsPerSecond) / rate.creditsPerDollar).toFixed(2)} USD: ` +
        `the session is capped at ${sessionSeconds} seconds, billed by the ${rate.per}`,
    );
    // The API key stays here: the session runs on a token for one session.
    const grant = yield* coordinator.mintToken({
      apiKey,
      modelName: H3.modelName,
      maxSessionDuration: `${sessionSeconds} seconds`,
      expiresAfter: `${sessionSeconds + 60} seconds`,
    });
    yield* record({ grant, prompt, seconds, references, out });
  }),
).pipe(
  Command.provide(({ isolated }) =>
    Layer.unwrap(
      Effect.map(
        Config.String("REACTOR_API_URL").pipe(Config.withDefault("https://api.reactor.inc")),
        (apiUrl) =>
          Reactor.layer().pipe(
            Layer.provideMerge(CoordinatorClient.layer({ apiUrl })),
            Layer.provide(isolated ? NativePeer.layerIsolated() : NativePeer.layer()),
          ),
      ),
    ),
  ),
  Command.withDescription("Generate one clip with Reactor H3 and save it as an MP4"),
);

capture.pipe(
  Command.run({ version: "0.11.0" }),
  // The program's entry point.
  // @effect-diagnostics-next-line strictEffectProvide:off
  Effect.provide(Layer.mergeAll(NodeServices.layer, FetchHttpClient.layer)),
  NodeRuntime.runMain,
);
