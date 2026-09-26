import * as NodeRuntime from "@effect/platform-node/NodeRuntime";
import * as NodeServices from "@effect/platform-node/NodeServices";
import {
  Config,
  Console,
  Deferred,
  Effect,
  FileSystem,
  Fiber,
  Layer,
  Option,
  Stream,
} from "effect";
import { Command, Flag } from "effect/unstable/cli";
import * as Reactor from "reactor-effect-client";
import * as H3 from "reactor-effect-client/h3";
import * as Native from "reactor-effect-native";
import { toMp4 } from "./Recording.ts";

/** The session's cap: a token for 90 seconds bounds what one capture can cost. */
const sessionSeconds = 90;
/**
 * Reactor bills a session by the minute. Its documentation does not say how
 * a started minute rounds, so the bound counts it whole: two minutes.
 */
const billedSeconds = Math.ceil(sessionSeconds / 60) * 60;

/**
 * One clip, captured on this machine: a paid H3 session over the native
 * host, the clip's own frames and audio decoded here, and an MP4 written
 * from them. The session is created, used and closed in one scope; closing
 * it terminates the paid session and reports whether that was confirmed.
 */
const record = Effect.fn("record")(function* (options: {
  readonly grant: Reactor.Coordinator.TokenGrant;
  readonly prompt: string;
  readonly seconds: number;
  readonly references: ReadonlyArray<H3.Reference>;
  readonly out: string;
}) {
  const client = yield* Reactor.Client;
  const session = yield* client.createConnected({ model: H3.modelName, jwt: options.grant.jwt });
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
  const media = yield* Native.media(session);
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
  const pressure = yield* media.snapshot;
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
  Effect.fn(function* ({ prompt, seconds, reference, out, isolated }) {
    const apiKey = yield* Config.Redacted("REACTOR_API_KEY");
    const apiUrl = yield* Config.String("REACTOR_API_URL").pipe(
      Config.withDefault("https://api.reactor.inc"),
    );
    const coordinator = yield* Reactor.Coordinator.make({ apiUrl });
    const rate = yield* Reactor.Coordinator.modelRate(yield* coordinator.pricing, H3.modelName);
    yield* Console.log(
      `at most ${((billedSeconds * rate.creditsPerSecond) / rate.creditsPerDollar).toFixed(2)} USD: ` +
        `the session is capped at ${sessionSeconds} seconds, billed as ${billedSeconds / 60} minutes`,
    );
    // The API key stays here: the session runs on a token for one session.
    const grant = yield* coordinator.mintToken({
      apiKey,
      modelName: H3.modelName,
      maxSessionDuration: `${sessionSeconds} seconds`,
      expiresAfter: `${sessionSeconds + 60} seconds`,
    });
    const fs = yield* FileSystem.FileSystem;
    const references = yield* Option.match(reference, {
      onNone: () => Effect.succeed([]),
      onSome: (path) =>
        fs.readFile(path).pipe(Effect.map((bytes): H3.Reference[] => [{ _tag: "Bytes", bytes }])),
    });
    yield* record({ grant, prompt, seconds, references, out }).pipe(
      Effect.provide(
        Reactor.layer({ apiUrl }).pipe(
          Layer.provide(isolated ? Native.Isolated.layer() : Native.layer()),
        ),
      ),
    );
  }),
).pipe(Command.withDescription("Generate one clip with Reactor H3 and save it as an MP4"));

capture.pipe(
  Command.run({ version: "0.5.0" }),
  Effect.provide(Layer.mergeAll(NodeServices.layer, Reactor.FetchHttp.layer)),
  NodeRuntime.runMain,
);
