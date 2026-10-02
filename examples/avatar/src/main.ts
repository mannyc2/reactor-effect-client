import * as NodeRuntime from "@effect/platform-node/NodeRuntime";
import * as NodeServices from "@effect/platform-node/NodeServices";
import {
  Config,
  Console,
  Effect,
  FileSystem,
  Layer,
  Option,
  Path,
  Queue,
  Redacted,
  Schema,
  Stream,
} from "effect";
import * as FetchHttpClient from "effect/http/FetchHttpClient";
import { CoordinatorClient, Reactor, ReactorTest, ViduS2Avatar } from "reactor-effect-client";
import { NativePeer } from "reactor-effect-native";

const persona = "You are Tina, a guide at a sea-life museum. Answer in one short sentence.";

const imageTypes: Record<string, string> = {
  ".png": "image/png",
  ".jpg": "image/jpeg",
  ".jpeg": "image/jpeg",
  ".webp": "image/webp",
  ".heic": "image/heic",
};

/** The photo in AVATAR_PHOTO, or a blank stand-in, which only the simulation takes. */
const photo = Effect.gen(function* () {
  const file = yield* Config.option(Config.String("AVATAR_PHOTO"));
  if (Option.isNone(file))
    return { bytes: ReactorTest.pngBytes({ width: 64, height: 64 }), type: "image/png" } as const;
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const type = yield* Schema.decodeUnknownEffect(ViduS2Avatar.ImageType)(
    imageTypes[path.extname(file.value).toLowerCase()],
  );
  return { bytes: yield* fs.readFile(file.value), type, name: path.basename(file.value) };
});

/** A character made from a photo, one call with it, and the session closed. */
const conversation = Effect.gen(function* () {
  // Read before a session is allocated, so a bad path costs nothing.
  const image = yield* photo;
  const coordinator = yield* CoordinatorClient.CoordinatorClient;
  const reactor = yield* Reactor.Reactor;
  const session = yield* reactor.create({
    model: ViduS2Avatar.modelName,
    // Reactor bills the whole session, between calls too; this token caps it at two minutes.
    tokens: coordinator.tokens({
      modelName: ViduS2Avatar.modelName,
      maxSessionDuration: "2 minutes",
    }),
  });
  yield* Console.log(`session ${session.id} connected`);
  const avatar = yield* ViduS2Avatar.make(session);

  // Both sides' transcripts are printed; the character's finished lines are queued to wait on.
  const lines = yield* avatar.events().pipe(
    Stream.filter((event) => event._tag === "Transcript"),
    Stream.map((event) => event.transcript),
    Stream.tap((line) => (line.final ? Console.log(`${line.speaker}: ${line.text}`) : Effect.void)),
    Stream.filter((line) => line.final && line.speaker === "character"),
    Stream.toQueue({ capacity: "unbounded" }),
  );

  yield* Console.log("making the avatar");
  const ready = yield* avatar.createAvatar(image);
  yield* Console.log(`avatar ${ready.avatar_id} ready`);

  const media = yield* session.decoded;
  yield* media.video(ViduS2Avatar.tracks.video).pipe(
    Stream.filter((frame) => frame.sequence % 25n === 0n),
    Stream.runForEach((frame) =>
      Console.log(`frame ${frame.sequence}: ${frame.width}x${frame.height} ${frame.format}`),
    ),
    Effect.forkScoped,
  );

  // It returns once the call is live; the provider has asked Reactor for the character's tracks.
  yield* avatar.startCall({ persona, greeting: "Say hello and introduce yourself." });
  yield* Console.log("call live");
  yield* Queue.take(lines);
  // Text the character answers as if the caller had said it: a Node client sends no microphone.
  yield* avatar.say("What lives in the deepest tank?");
  yield* Queue.take(lines);

  const ended = yield* avatar.endCall;
  yield* Console.log(`call ended: ${ended.end_reason} after ${ended.duration_seconds} s`);
  const report = yield* session.close;
  yield* Console.log(`session closed, termination confirmed: ${report.remote.confirmed}`);
}).pipe(Effect.scoped);

/** Reactor simulated in memory, at the timing paid runs measured: no key, nothing billed. */
const Simulated = Reactor.layer().pipe(
  Layer.provideMerge(CoordinatorClient.layer({ apiKey: Redacted.make("demo") })),
  Layer.provideMerge(
    ReactorTest.layer({
      timing: ReactorTest.Timing.hosted,
      apiKey: "demo",
      width: 232,
      height: 272,
    }),
  ),
);

/** Hosted Reactor with the key in REACTOR_API_KEY, frames decoded by libwebrtc in this process. */
const Hosted = Reactor.layer().pipe(
  Layer.provideMerge(Layer.mergeAll(CoordinatorClient.layerConfig, NativePeer.layer())),
  Layer.provide(FetchHttpClient.layer),
);

/** The program is the same either way; only this choice differs. */
const Reactors = Layer.unwrap(
  Config.option(Config.Redacted("REACTOR_API_KEY")).pipe(
    Effect.map((key) => (Option.isSome(key) ? Hosted : Simulated)),
  ),
);

conversation.pipe(
  // The program's entry point, the one place a layer is provided.
  // @effect-diagnostics-next-line strictEffectProvide:off
  Effect.provide(Layer.mergeAll(Reactors, NodeServices.layer)),
  NodeRuntime.runMain,
);
